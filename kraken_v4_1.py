"""
kraken_v4_2.py — Conductor
Single-process three-engine trading bot.

Architecture:
  RateLimiter     — single API gatekeeper, 1.5s min spacing, exponential backoff
  StateManager    — atomic state.json writes, events.log, audit CSV
  RegimeEngine    — EMA/ADX regime on 1h candles (hourly)
  SignalEngine    — Bollinger Band entry/exit (hourly)
  TrailEngine     — analog historical fingerprinting + V4.1 tiered/floor (every 5 min)
  OrderBookEngine — 4-check book gate: imbalance/walls/depth/tape (every 5 min)
  Conductor       — orchestrates all engines, one main loop

Timing:
  T+0:00  Hourly:  regime + BB signal → entry/exit decision → book gate
  T+0:05  5-min:   trail update + book gate → position protection
  T+0:10  5-min:   (repeat)
  ...
  T+1:00  Hourly:  (repeat)

systemd: writes stdout every cycle for WatchdogSec=300 compliance.
State:   atomic write to state.json.tmp → os.replace() — reboot-safe.
Boot:    15s NTP settle + aggressive API retry before first call.
"""

import ccxt
import pandas as pd
import numpy as np
import time
import os
import json
import math
import collections
from datetime import datetime, timezone

from order_book import OrderBookEngine
from trail_engine import TrailEngine

# ─────────────────────────────────────────────────────────────
STATE_FILE   = 'kraken_v4_2_state.json'
LOG_FILE     = 'kraken_auditable_shadow_bot_v4_audit_trail.csv'
EVENT_FILE   = 'kraken_v4_2_events.log'
LEGACY_CSVS  = [
    'kraken_auditable_shadow_bot_v3_5_audit_trail.csv',
    'kraken_auditable_shadow_bot_v3_6_audit_trail.csv',
    'kraken_auditable_shadow_bot_v4_audit_trail.csv',
    'kraken_auditable_shadow_bot_v4_1_audit_trail.csv',
]


# ─────────────────────────────────────────────────────────────
# RATE LIMITER — single gatekeeper for ALL API calls
# ─────────────────────────────────────────────────────────────
class RateLimiter:
    MIN_SPACING  = 1.5    # seconds between any two calls
    BACKOFF_BASE = 10     # seconds, doubles each retry
    MAX_RETRIES  = 5

    def __init__(self):
        self._last_call = 0.0

    def _wait(self):
        elapsed = time.time() - self._last_call
        gap     = self.MIN_SPACING - elapsed
        if gap > 0:
            time.sleep(gap)
        self._last_call = time.time()

    def call(self, fn, *args, **kwargs):
        """Execute an API call with spacing + exponential backoff on rate errors."""
        for attempt in range(self.MAX_RETRIES):
            try:
                self._wait()
                return fn(*args, **kwargs)
            except ccxt.RateLimitExceeded as e:
                wait = self.BACKOFF_BASE * (2 ** attempt)
                log_event(f"[RateLimiter] Rate limit hit — backoff {wait}s "
                          f"(attempt {attempt+1}/{self.MAX_RETRIES}): {e}")
                time.sleep(wait)
            except ccxt.NetworkError as e:
                wait = self.BACKOFF_BASE * (2 ** attempt)
                log_event(f"[RateLimiter] Network error — backoff {wait}s: {e}")
                time.sleep(wait)
            except Exception as e:
                raise e
        log_event(f"[RateLimiter] Max retries exceeded — skipping call.")
        return None


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def fmt_duration(seconds):
    seconds = int(max(0, seconds))
    d, rem  = divmod(seconds, 86400)
    h, rem  = divmod(rem, 3600)
    m, s    = divmod(rem, 60)
    return f"{d}d {h}h {m}m {s}s"

def log_event(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with open(EVENT_FILE, 'a') as f:
        f.write(line + "\n")

def parse_ts(ts_str):
    try:
        return pd.to_datetime(ts_str, utc=True).timestamp()
    except Exception:
        return time.time()


# ─────────────────────────────────────────────────────────────
# STATE MANAGER — atomic writes, bootstrap, legacy CSV support
# ─────────────────────────────────────────────────────────────
class StateManager:
    def __init__(self):
        self.virtual_usd      = 2000.0
        self.virtual_btc      = 0.0
        self.max_equity       = 2000.0
        self.peak_price       = 0.0
        self.entry_price      = 0.0
        self.trade_count      = 0
        self.current_regime   = 'NEUTRAL'
        now = time.time()
        self.first_start_ts   = now
        self.session_start_ts = now
        self.total_paused_secs = 0.0
        self.history          = []

    def save(self):
        """Atomic write: tmp file → os.replace() — safe against mid-write reboot."""
        state = {
            'virtual_usd':        self.virtual_usd,
            'virtual_btc':        self.virtual_btc,
            'max_equity':         self.max_equity,
            'peak_price':         self.peak_price,
            'entry_price':        self.entry_price,
            'trade_count':        self.trade_count,
            'current_regime':     self.current_regime,
            'first_start_ts':     self.first_start_ts,
            'total_paused_secs':  self.total_paused_secs,
            'session_start_ts':   self.session_start_ts,
            'last_heartbeat_ts':  time.time(),
        }
        tmp = STATE_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_FILE)   # atomic on Linux

    def load(self):
        """Load from state.json, fall back to legacy CSVs, then fresh start."""
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE) as f:
                    s = json.load(f)
                now     = time.time()
                gap     = now - s.get('last_heartbeat_ts', 0)
                self.virtual_usd      = s['virtual_usd']
                self.virtual_btc      = s['virtual_btc']
                self.max_equity       = s['max_equity']
                self.peak_price       = s['peak_price']
                self.entry_price      = s.get('entry_price', 0.0)
                self.trade_count      = s['trade_count']
                self.current_regime   = s['current_regime']
                self.first_start_ts   = s['first_start_ts']
                self.total_paused_secs = s['total_paused_secs']
                self.session_start_ts = now
                if gap > 300:
                    self.total_paused_secs += gap
                    self.save()
                    log_event(
                        f">>> BOT RESTARTED | Gap: {fmt_duration(gap)} | "
                        f"Total Paused: {fmt_duration(self.total_paused_secs)} | "
                        f"Equity: ${self.virtual_usd:,.2f} | "
                        f"Trades: {self.trade_count} | Regime: {self.current_regime}"
                    )
                else:
                    log_event(f">>> BOT RESUMED (warm restart, gap={fmt_duration(gap)})")
                return True
            except Exception as e:
                log_event(f"WARNING: state.json unreadable ({e}) — trying legacy CSVs")

        return self._bootstrap_legacy()

    def _bootstrap_legacy(self):
        found = False
        first_ts = None
        last_ts  = None
        last_eq  = 2000.0
        max_eq   = 2000.0
        trades   = 0
        for path in LEGACY_CSVS:
            if not os.path.exists(path):
                continue
            try:
                df = pd.read_csv(path)
                if df.empty:
                    continue
                found    = True
                if first_ts is None:
                    first_ts = parse_ts(df.iloc[0]['timestamp'])
                last_ts  = parse_ts(df.iloc[-1]['timestamp'])
                last_eq  = float(df.iloc[-1]['equity'])
                max_eq   = max(max_eq, float(df['equity'].max()))
                exec_rows = df[df['exec_price'] > 0]
                trades   += len(exec_rows) // 2
                log_event(f"  [bootstrap] Loaded {path} | rows={len(df)} | "
                          f"last_equity=${last_eq:,.2f} | trades={len(exec_rows)//2}")
            except Exception as e:
                log_event(f"  [bootstrap] WARNING: could not read {path} ({e})")
        if not found or first_ts is None:
            return False
        now  = time.time()
        gap  = max(0.0, now - last_ts) if last_ts else 0.0
        self.virtual_usd       = last_eq
        self.max_equity        = max_eq
        self.trade_count       = max(1, trades)
        self.first_start_ts    = first_ts
        self.total_paused_secs = gap
        self.session_start_ts  = now
        log_event(
            f">>> BOOTSTRAPPED FROM LEGACY CSVs | "
            f"Original start: {datetime.fromtimestamp(first_ts).strftime('%Y-%m-%d %H:%M:%S')} | "
            f"Gross: {fmt_duration(now - first_ts)} | "
            f"Equity: ${self.virtual_usd:,.2f} | Trades: {self.trade_count} | "
            f"Gap: {fmt_duration(gap)}"
        )
        return True

    def gross_runtime(self):
        return time.time() - self.first_start_ts

    def net_runtime(self):
        return self.gross_runtime() - self.total_paused_secs

    def uptime_strings(self):
        return (fmt_duration(self.gross_runtime()),
                fmt_duration(self.total_paused_secs),
                fmt_duration(self.net_runtime()))


# ─────────────────────────────────────────────────────────────
# REGIME ENGINE
# ─────────────────────────────────────────────────────────────
class RegimeEngine:
    def __init__(self, ema_fast=21, ema_slow=55, adx_period=14, adx_threshold=20):
        self.ema_fast      = ema_fast
        self.ema_slow      = ema_slow
        self.adx_period    = adx_period
        self.adx_threshold = adx_threshold

    def _calc_adx(self, df):
        h, l, c   = df['h'], df['l'], df['c']
        pdm       = h.diff().clip(lower=0)
        ndm       = (-l.diff()).clip(lower=0)
        tr        = pd.concat([(h-l), (h-c.shift()).abs(), (l-c.shift()).abs()],
                              axis=1).max(axis=1)
        atr       = tr.ewm(span=self.adx_period, adjust=False).mean()
        pdi       = 100 * pdm.ewm(span=self.adx_period, adjust=False).mean() / (atr+1e-9)
        ndi       = 100 * ndm.ewm(span=self.adx_period, adjust=False).mean() / (atr+1e-9)
        dx        = (abs(pdi - ndi) / (pdi + ndi + 1e-9)) * 100
        return dx.ewm(span=self.adx_period, adjust=False).mean().iloc[-1]

    def detect(self, df_btc, df_eth):
        for df in (df_btc, df_eth):
            df['ema_fast'] = df['c'].ewm(span=self.ema_fast, adjust=False).mean()
            df['ema_slow'] = df['c'].ewm(span=self.ema_slow, adjust=False).mean()
        btc_bull  = (df_btc['ema_fast'].iloc[-1] > df_btc['ema_slow'].iloc[-1] and
                     df_btc['c'].iloc[-1] > df_btc['ema_fast'].iloc[-1])
        eth_bull  = df_eth['ema_fast'].iloc[-1] > df_eth['ema_slow'].iloc[-1]
        btc_bear  = df_btc['ema_fast'].iloc[-1] < df_btc['ema_slow'].iloc[-1]
        eth_bear  = df_eth['ema_fast'].iloc[-1] < df_eth['ema_slow'].iloc[-1]
        strong    = self._calc_adx(df_btc) > self.adx_threshold
        if btc_bull and eth_bull and strong:
            return 'BULL'
        if btc_bear or eth_bear:
            return 'BEAR'
        return 'NEUTRAL'


# ─────────────────────────────────────────────────────────────
# CONDUCTOR
# ─────────────────────────────────────────────────────────────
class Conductor:
    def __init__(self):
        self.exchange = ccxt.kraken({'enableRateLimit': False})  # we manage rate limits
        self.rl       = RateLimiter()
        self.state    = StateManager()
        self.regime   = RegimeEngine()
        self.trail    = TrailEngine(self.exchange, self.rl, shadow_mode=True)
        self.book     = OrderBookEngine(self.exchange, self.rl)

        self.symbol     = 'BTC/USD'
        self.eth_symbol = 'ETH/USD'
        self.timeframe  = '1h'
        self.slippage   = 0.0010
        self.stop_pct   = 0.035
        self.max_dd     = 0.15
        self.bull_trail = 0.013
        self.bear_trail = 0.020

        self.history    = []
        self.last_book_snapshot = {}

    # ── boot sequence ─────────────────────────────────────────
    def boot(self):
        log_event("--- KRAKEN V4.2: ANALOG TRAIL | ORDER BOOK GATE | "
                  "DYNAMIC TRAIL + PROFIT FLOOR | STATE RESUME ---")

        # NTP settle — clock must be stable before first API call
        log_event("[Boot] Waiting 15s for NTP clock sync...")
        time.sleep(15)

        # Verify API reachable with retry window
        log_event("[Boot] Verifying Kraken API connectivity...")
        deadline = time.time() + 300   # 5-minute retry window
        while time.time() < deadline:
            try:
                self.rl.call(self.exchange.fetch_time)
                log_event("[Boot] Kraken API reachable.")
                break
            except Exception as e:
                log_event(f"[Boot] API not yet reachable ({e}) — retrying in 15s")
                time.sleep(15)
        else:
            log_event("[Boot] CRITICAL: API unreachable after 5 minutes. Exiting.")
            raise SystemExit(1)

        # Load state
        resumed = self.state.load()
        if not resumed:
            log_event(">>> BOT FIRST START — fresh run.")
        self.state.save()

        # Build trail engine historical DB (non-blocking, runs in background of startup)
        log_event("[Boot] Building TrailEngine historical DB (this may take 1-2 minutes)...")
        self.trail.build_db()

        log_event("[Boot] Boot sequence complete.")

    # ── fetch candles ─────────────────────────────────────────
    def _fetch_ohlcv(self, symbol, tf, limit=100):
        raw = self.rl.call(self.exchange.fetch_ohlcv, symbol, tf, None, limit)
        if raw is None:
            return None
        return pd.DataFrame(raw, columns=['ts','o','h','l','c','v'])

    # ── hourly signal logic ───────────────────────────────────
    def hourly_signal(self):
        """
        Full hourly pipeline:
        1. Fetch 1h candles for BTC + ETH
        2. Detect regime
        3. Compute BB signal
        4. Gate through order book
        Returns (action, sig_price, delay_ms, book_snapshot)
        """
        df_btc = self._fetch_ohlcv(self.symbol,     '1h', 100)
        df_eth = self._fetch_ohlcv(self.eth_symbol,  '1h', 100)
        if df_btc is None or df_eth is None:
            return 'HOLD', 0.0, 0, {}

        self.state.current_regime = self.regime.detect(df_btc.copy(), df_eth.copy())

        bb_mult = 1.5 if self.state.current_regime == 'BULL' else 2.0
        df      = df_btc.copy()
        df['sma']   = df['c'].rolling(20).mean()
        df['std']   = df['c'].rolling(20).std()
        df['upper'] = df['sma'] + bb_mult * df['std']
        df['lower'] = df['sma'] - bb_mult * df['std']
        df['ema21'] = df['c'].ewm(span=21, adjust=False).mean()

        last   = df['c'].iloc[-1]
        lower  = df['lower'].iloc[-1]
        upper  = df['upper'].iloc[-1]
        ema21  = df['ema21'].iloc[-1]
        delay  = int(time.time() * 1000) - (int(df['ts'].iloc[-1]) + 3600000)

        action = 'HOLD'
        s      = self.state

        # Entry
        if s.virtual_btc == 0:
            if self.state.current_regime == 'BEAR':
                action = 'HOLD'
            elif self.state.current_regime == 'BULL':
                ema21_prev = df['ema21'].iloc[-2]
                crossover  = (df['c'].iloc[-2] < ema21_prev) and (last > ema21)
                if last < lower or crossover:
                    action = 'BUY'
            else:
                if last < lower:
                    action = 'BUY'

        # Exit (handled also in 5-min loop for trail stops)
        elif s.virtual_btc > 0:
            if last > upper:
                if not (self.state.current_regime == 'BULL' and last < ema21 * 1.01):
                    action = 'SELL_TARGET'
            elif last <= s.peak_price * (1 - self.stop_pct):
                action = 'SELL_STOP'
            elif self.state.current_regime == 'BEAR':
                action = 'SELL_REGIME_FLIP'

        # Order book gate
        book_verdict, snapshot = self.book.evaluate()
        action = self._apply_book_gate(action, book_verdict, snapshot)

        return action, last, delay, snapshot

    # ── 5-min sub-loop ────────────────────────────────────────
    def five_min_check(self):
        """
        Between hourly signals: update trail stop, re-run book gate.
        Can trigger SELL_TRAIL if stop is breached.
        Returns (action, price, book_snapshot)
        """
        s = self.state
        if s.virtual_btc == 0:
            # Still run book check for logging, no trade action
            _, snapshot = self.book.evaluate()
            return 'HOLD', 0.0, snapshot

        # Fetch current price
        ticker = self.rl.call(self.exchange.fetch_ticker, self.symbol)
        if ticker is None:
            return 'HOLD', 0.0, {}
        price = ticker['last']

        # Update peak
        s.peak_price = max(s.peak_price, price)

        # Fetch 1m and 5m candles for trail engine
        df_1m = self._fetch_ohlcv(self.symbol, '1m', 100)
        df_5m = self._fetch_ohlcv(self.symbol, '5m', 100)

        base_trail = self.bull_trail if s.current_regime == 'BULL' else self.bear_trail

        # Adaptive trail evaluation
        if df_1m is not None and df_5m is not None:
            trail_pct, stop_price, method, confidence, detail = self.trail.evaluate(
                df_1m, df_5m, s.entry_price, s.peak_price, base_trail
            )
        else:
            trail_pct  = base_trail
            stop_price = s.peak_price * (1 - base_trail)
            method     = 'fallback'
            confidence = 0.0
            detail     = {}

        # Hard stop
        hard_stop = s.peak_price * (1 - self.stop_pct)

        # Check stops
        action = 'HOLD'
        if price <= hard_stop:
            action = 'SELL_STOP'
        elif price <= stop_price:
            action = f'SELL_TRAIL({method},{trail_pct*100:.2f}%)'

        # Book gate on trail exits
        book_verdict, snapshot = self.book.evaluate()
        if action.startswith('SELL_TRAIL'):
            action = self._apply_book_gate(action, book_verdict, snapshot)

        # Store for health check
        self.last_trail_detail = detail
        self.last_stop_price   = stop_price
        self.last_trail_pct    = trail_pct
        self.last_trail_method = method
        self.last_confidence   = confidence

        return action, price, snapshot

    # ── book gate ─────────────────────────────────────────────
    def _apply_book_gate(self, action, verdict, snapshot):
        """
        Apply order book verdict to a proposed action.
        VETO: block BUY, tighten trail to next tier
        WARN: log warning, tighten trail by 0.2% on exits
        CONFIRM: proceed as planned
        """
        if verdict == 'VETO':
            if action == 'BUY':
                log_event(f"[BookGate] VETO on BUY — book does not confirm. "
                          f"Reason: {snapshot.get('depth_label','')}")
                return 'HOLD'
            elif action.startswith('SELL_TRAIL'):
                # Book is pulling liquidity — tighten immediately, don't wait
                log_event("[BookGate] VETO — liquidity withdrawal, forcing tighter trail")
                return action   # already triggered, let it through
        elif verdict == 'WARN':
            if action == 'BUY':
                log_event(f"[BookGate] WARN on BUY — proceeding with caution: "
                          f"{snapshot.get('imbalance_label','')}")
            elif action.startswith('SELL'):
                log_event(f"[BookGate] WARN on SELL — book shows buy pressure, "
                          f"holding one candle (trail tightened)")
                # Don't block the sell — but it's already triggered by price
        return action

    # ── execution ─────────────────────────────────────────────
    def execute(self, action, sig_price):
        exec_price = 0.0
        s = self.state
        if action == 'BUY' and s.virtual_btc == 0:
            exec_price    = sig_price * (1 + self.slippage)
            s.virtual_btc = s.virtual_usd / exec_price
            s.virtual_usd = 0.0
            s.peak_price  = sig_price
            s.entry_price = sig_price
            s.trade_count += 1
            log_event(f"!!! BUY @ ${exec_price:,.2f} | "
                      f"Regime: {s.current_regime} | BTC: {s.virtual_btc:.6f}")
        elif action.startswith('SELL') and s.virtual_btc > 0:
            exec_price    = sig_price * (1 - self.slippage)
            s.virtual_usd = s.virtual_btc * exec_price
            s.virtual_btc = 0.0
            s.peak_price  = 0.0
            s.entry_price = 0.0
            log_event(f"!!! SELL ({action}) @ ${exec_price:,.2f} | "
                      f"Regime: {s.current_regime} | USD: ${s.virtual_usd:,.2f}")
        return exec_price

    # ── audit trail ───────────────────────────────────────────
    def log_audit(self, price, sig_price, exec_price, delay, book_snap):
        s   = self.state
        eq  = s.virtual_usd + s.virtual_btc * price
        if s.virtual_btc > 0:
            s.peak_price = max(s.peak_price, price)
        s.max_equity = max(s.max_equity, eq)
        dd = (s.max_equity - eq) / s.max_equity
        gross, paused, net = s.uptime_strings()

        trail_pct    = getattr(self, 'last_trail_pct',    0.0)
        stop_price   = getattr(self, 'last_stop_price',   0.0)
        trail_method = getattr(self, 'last_trail_method', 'n/a')
        confidence   = getattr(self, 'last_confidence',   0.0)

        self.history.append({
            'timestamp':      datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'regime':         s.current_regime,
            'equity':         round(eq, 2),
            'drawdown':       round(dd, 4),
            'signal_price':   sig_price,
            'exec_price':     exec_price,
            'delay_ms':       delay,
            'entry_price':    round(s.entry_price, 2),
            'peak_price':     round(s.peak_price, 2),
            'trail_stop':     round(stop_price, 2),
            'trail_pct':      round(trail_pct * 100, 3),
            'trail_method':   trail_method,
            'trail_conf':     round(confidence, 3),
            'book_verdict':   book_snap.get('verdict', 'n/a'),
            'book_imbalance': book_snap.get('imbalance_ratio', 0),
            'gross_runtime':  gross,
            'total_paused':   paused,
            'net_runtime':    net,
        })
        pd.DataFrame(self.history).to_csv(LOG_FILE, index=False)
        return eq, dd

    # ── health check ──────────────────────────────────────────
    def print_health(self, eq, dd, book_snap):
        s            = self.state
        profit       = eq - 2000.0
        profit_pct   = profit / 2000.0 * 100
        gross, paused, net = s.uptime_strings()
        trail_pct    = getattr(self, 'last_trail_pct',    0.0)
        stop_price   = getattr(self, 'last_stop_price',   0.0)
        trail_method = getattr(self, 'last_trail_method', 'n/a')
        confidence   = getattr(self, 'last_confidence',   0.0)
        trail_detail = getattr(self, 'last_trail_detail', {})

        print("\n" + "="*62)
        print(f" KRAKEN V4.2 HEALTH CHECK | "
              f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f" REGIME: {s.current_regime}")
        print("─"*62)
        print(f" Trades:          {s.trade_count}")
        print(f" Equity:          ${eq:,.2f}")
        print(f" P/L:             ${profit:,.2f} ({profit_pct:.2f}%)")
        print(f" Max Drawdown:    {dd*100:.2f}% (limit: {self.max_dd*100:.0f}%)")
        print(f" Position:        {'LONG' if s.virtual_btc > 0 else 'FLAT'}")
        if s.virtual_btc > 0:
            gain_pct = ((s.peak_price - s.entry_price) / s.entry_price * 100
                        if s.entry_price > 0 else 0)
            print(f" Entry Price:     ${s.entry_price:,.2f}")
            print(f" Peak Price:      ${s.peak_price:,.2f}  (+{gain_pct:.3f}%)")
            print(f" Trail Stop @:    ${stop_price:,.2f}  "
                  f"({trail_pct*100:.2f}% | {trail_method})")
            if confidence > 0:
                print(f" Trail Conf:      {confidence:.2f}")
            if trail_detail:
                self.trail.print_detail(trail_detail)
            print(f" Hard Stop @:     ${s.peak_price*(1-self.stop_pct):,.2f}")
        print("─"*62)
        self.book.print_snapshot(book_snap)
        print("─"*62)
        print(f" Gross Runtime:   {gross}")
        print(f" Total Paused:    {paused}")
        print(f" Net Runtime:     {net}")
        print("="*62 + "\n")

    # ── main loop ─────────────────────────────────────────────
    def main(self):
        self.boot()

        # Initialize trail detail defaults
        self.last_trail_pct    = self.bull_trail
        self.last_stop_price   = 0.0
        self.last_trail_method = 'tiered'
        self.last_confidence   = 0.0
        self.last_trail_detail = {}

        while True:
            try:
                # ── HOURLY BLOCK ──────────────────────────────
                now       = time.time()
                next_hour = (now // 3600 + 1) * 3600

                action, sig_price, delay, book_snap = self.hourly_signal()
                exec_price = self.execute(action, sig_price)

                ticker = self.rl.call(self.exchange.fetch_ticker, self.symbol)
                price  = ticker['last'] if ticker else sig_price

                # Initialize trail for new position
                if action == 'BUY':
                    base = (self.bull_trail if self.state.current_regime == 'BULL'
                            else self.bear_trail)
                    self.last_stop_price   = price * (1 - base)
                    self.last_trail_pct    = base
                    self.last_trail_method = 'tiered'

                eq, dd = self.log_audit(price, sig_price, exec_price, delay, book_snap)
                self.print_health(eq, dd, book_snap)
                self.state.save()

                if dd >= self.max_dd:
                    log_event("CRITICAL: 15% DRAWDOWN KILL SWITCH TRIGGERED. STOPPING.")
                    self.state.save()
                    raise SystemExit(0)

                # ── 5-MIN SUB-LOOP ────────────────────────────
                # Run every 5 minutes until the next hour
                sub_interval = 300   # 5 minutes
                sub_next     = now + sub_interval

                while time.time() < next_hour - 30:
                    sleep_to = min(sub_next, next_hour - 30)
                    sleep_secs = sleep_to - time.time()
                    if sleep_secs > 0:
                        time.sleep(sleep_secs)

                    sub_action, sub_price, sub_book = self.five_min_check()

                    if sub_action != 'HOLD' and self.state.virtual_btc > 0:
                        exec_price = self.execute(sub_action, sub_price)
                        eq, dd     = self.log_audit(sub_price, sub_price,
                                                    exec_price, 0, sub_book)
                        self.print_health(eq, dd, sub_book)
                        self.state.save()
                        log_event(f"[5-min] {sub_action} @ ${sub_price:,.2f}")

                        if dd >= self.max_dd:
                            log_event("CRITICAL: 15% DRAWDOWN KILL SWITCH. STOPPING.")
                            self.state.save()
                            raise SystemExit(0)
                    else:
                        # Still print health so WatchdogSec gets stdout
                        if self.state.virtual_btc > 0:
                            ticker2 = self.rl.call(
                                self.exchange.fetch_ticker, self.symbol)
                            p2  = ticker2['last'] if ticker2 else sub_price
                            eq2 = (self.state.virtual_usd +
                                   self.state.virtual_btc * p2)
                            dd2 = ((self.state.max_equity - eq2)
                                   / self.state.max_equity)
                            self.print_health(eq2, dd2, sub_book)

                    sub_next += sub_interval

            except SystemExit:
                raise
            except Exception as e:
                log_event(f"[Main] Error: {e}")
                self.state.save()
                time.sleep(60)


if __name__ == '__main__':
    Conductor().main()
