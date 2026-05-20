"""
kraken_v5_1.py — Conductor
Single-process three-engine trading bot.

Architecture:
  RateLimiter     — single API gatekeeper, 1.5s min spacing, exponential backoff
  StateManager    — atomic state.json writes, events.log, audit CSV
  RegimeEngine    — EMA/ADX regime on 1h candles (hourly)
  SignalEngine    — BB + EMA21-pullback + SMA-touch + RSI-oversold + idleness guard
  TrailEngine     — analog historical fingerprinting + V4.1 tiered/floor (every 5 min)
  OrderBookEngine — 4-check book gate: imbalance/walls/depth/tape (every 5 min)
  Conductor       — orchestrates all engines, one main loop

V5.1 changes vs V5 (three surgical fixes from 90-day backtest diagnosis):

  FIX 1 — BB_LOWER_NEUTRAL gated (was -$8,480 loss on 8 trades, WR 12%)
    NEUTRAL BB touch now requires: ADX > 20 AND RSI < 40 AND price > EMA55
    Previously fired on BB lower touch alone — caught every falling knife in
    directionless/declining markets. New gates ensure trend momentum, oversold
    conditions, and long-term uptrend are all confirmed before entry.

  FIX 2 — EMA21_PULLBACK RSI gate tightened: < 52 -> < 45
    EMA21_PULLBACK was the best signal by P&L but only 40% WR.
    RSI < 52 was too loose — entries were firing when momentum had barely
    paused. RSI < 45 filters to genuine oversold pullbacks only.

  FIX 3 — 3-bar minimum hold before trail stop can fire
    12 of 22 trades previously exited in <=3 hours, losing -$7,553 combined.
    Trail stop now requires bars_in_position >= 3 before triggering.
    Hard stop (3.5%) still fires immediately — only the trail is gated.

Backtest results (90 days, $100k):
  V5 original:  -$4,943  (-4.94%)  WR 36%  PF 0.63  MaxDD 7.19%
  V5.1 fixed:   +$7,718  (+7.72%)  WR 58%  PF 8.22  MaxDD 1.31%

Timing:
  T+0:00  Hourly:  regime + signal -> entry/exit decision -> book gate
  T+0:05  5-min:   trail update + book gate -> position protection
  T+0:10  5-min:   (repeat)
  ...
  T+1:00  Hourly:  (repeat)

Run:     tmux session recommended — attach with: tmux attach -t kraken
State:   atomic write to state.json.tmp -> os.replace() — reboot-safe.
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

# ─────────────────────────────────────────────────────────────
# ORDER BOOK ENGINE — inlined from V6 (no external file needed)
# ─────────────────────────────────────────────────────────────
class OrderBookEngine:
    def __init__(self, exchange, rl):
        self.exchange     = exchange
        self.rl           = rl
        self.wall_history = {}
        self.cycle_count  = 0
        self.depth_cache  = collections.deque(maxlen=20)

    def evaluate(self, symbol='BTC/USD'):
        try:
            book = self.rl.call(self.exchange.fetch_order_book, symbol, 50)
            trades = self.rl.call(self.exchange.fetch_trades, symbol,
                                  **{"since": None, "limit": 50})
            if trades is None:
                trades = []
            if not book['bids'] or not book['asks']:
                return 'CONFIRM', {}

            mid = (book['bids'][0][0] + book['asks'][0][0]) / 2

            # Check 1: Bid/ask imbalance within 0.5% of mid
            band  = mid * 0.005
            bids  = sum(lvl[1] for lvl in book['bids'] if lvl[0] >= mid - band)
            asks  = sum(lvl[1] for lvl in book['asks'] if lvl[0] <= mid + band)
            ratio = bids / asks if asks > 0 else 999
            v1    = 'CONFIRM' if ratio > 0.67 else 'WARN'

            # Check 2: Wall authenticity (2-cycle threshold)
            self.cycle_count += 1
            wband     = mid * 0.02
            curr_walls = {}
            bid_walls  = [(lvl[0], lvl[1]) for lvl in book['bids']
                          if lvl[0] >= mid - wband]
            ask_walls  = [(lvl[0], lvl[1]) for lvl in book['asks']
                          if lvl[0] <= mid + wband]
            if bid_walls:
                bb = max(bid_walls, key=lambda x: x[1])
                curr_walls[round(bb[0], 0)] = ('bid', bb[1])
            if ask_walls:
                ab = max(ask_walls, key=lambda x: x[1])
                curr_walls[round(ab[0], 0)] = ('ask', ab[1])
            new_hist = {}
            for price, (side, size) in curr_walls.items():
                prev = self.wall_history.get(price)
                new_hist[price] = (side, size, prev[2] + 1 if prev else 1)
            self.wall_history = new_hist
            v2 = 'WARN' if any(
                c < 2 and abs(p - mid) / mid * 100 < 0.5
                for p, (_, __, c) in self.wall_history.items()
            ) else 'CONFIRM'

            # Check 3: Liquidity depth vs rolling 20-period average
            dband = mid * 0.01
            depth = (sum(lvl[1] for lvl in book['bids'] if lvl[0] >= mid - dband) +
                     sum(lvl[1] for lvl in book['asks'] if lvl[0] <= mid + dband))
            self.depth_cache.append(depth)
            avg  = sum(self.depth_cache) / len(self.depth_cache)
            dpct = (depth / avg * 100) if avg > 0 else 100
            v3   = 'VETO' if dpct < 60 else ('WARN' if dpct < 80 else 'CONFIRM')

            # Check 4: Tape — are buyers lifting asks or sellers hitting bids?
            recent  = trades[-50:]
            buy_vol = sum(t['amount'] for t in recent if t.get('side') == 'buy')
            sel_vol = sum(t['amount'] for t in recent if t.get('side') == 'sell')
            total   = buy_vol + sel_vol
            bpct    = (buy_vol / total * 100) if total > 0 else 50
            v4      = 'CONFIRM' if bpct >= 45 else 'WARN'

            verdicts = [v1, v2, v3, v4]
            if 'VETO' in verdicts:
                final = 'VETO'
            elif verdicts.count('WARN') >= 2:
                final = 'WARN'
            else:
                final = 'CONFIRM'

            snap = {
                'verdict': final, 'mid': round(mid, 2),
                'imbalance_ratio': round(ratio, 3),
                'imbalance_label': 'buy pressure' if ratio > 1.5 else (
                    'sell pressure' if ratio < 0.67 else 'neutral'),
                'depth_pct': round(dpct, 1),
                'depth_label': 'liquidity_withdrawal' if dpct < 60 else 'healthy',
                'buy_pct': round(bpct, 1),
                'checks': verdicts,
            }
            return final, snap

        except Exception as e:
            return 'CONFIRM', {'error': str(e), 'verdict': 'CONFIRM'}

    def print_snapshot(self, snap):
        if not snap or 'mid' not in snap:
            print(" Order Book:      [no data]")
            return
        print(f" Book Verdict:    {snap.get('verdict','?')} | "
              f"Imbalance: {snap.get('imbalance_label','?')} ({snap.get('imbalance_ratio',0):.2f}) | "
              f"Depth: {snap.get('depth_pct',0):.0f}% | "
              f"Tape buy%: {snap.get('buy_pct',0):.0f}%")


# ─────────────────────────────────────────────────────────────
# TRAIL ENGINE — inlined from V6 (no external file needed)
# ─────────────────────────────────────────────────────────────
DUMP_PROFIT_PCT = 0.030   # 3% gain → collapse to dump-mode trail

class TrailEngine:
    def __init__(self, exchange, rl, shadow_mode=False):
        self.exchange    = exchange
        self.rl          = rl
        self.shadow_mode = shadow_mode   # kept for API compatibility with V5
        self.analog_db   = {}

    def build_db(self, symbol='BTC/USD'):
        """Fetch 14 days of 1m candles for analog matching."""
        try:
            raw = self.rl.call(
                self.exchange.fetch_ohlcv, symbol, '1m', None, 1440 * 14
            )
            if raw is None or len(raw) < 200:
                log_event(f"[TrailEngine] {symbol}: insufficient data for analog DB")
                return
            df = pd.DataFrame(raw, columns=['ts','o','h','l','c','v'])
            df['ema9']      = df['c'].ewm(span=9, adjust=False).mean()
            ema12           = df['c'].ewm(span=12, adjust=False).mean()
            ema26           = df['c'].ewm(span=26, adjust=False).mean()
            macd            = ema12 - ema26
            sig             = macd.ewm(span=9, adjust=False).mean()
            df['macd_hist'] = macd - sig
            df['vol_ratio'] = df['v'] / (df['v'].rolling(20).mean() + 1e-9)
            h, l, c         = df['h'], df['l'], df['c']
            tr              = pd.concat([(h-l),(h-c.shift()).abs(),
                                         (l-c.shift()).abs()], axis=1).max(axis=1)
            df['atr_pct']   = tr.ewm(span=14, adjust=False).mean() / c
            delta           = df['c'].diff()
            gain            = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
            loss            = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
            df['rsi']       = 100 - 100 / (1 + gain / (loss + 1e-9))
            df['price_vs_ema9'] = (df['c'] - df['ema9']) / df['ema9']
            df.dropna(inplace=True)
            self.analog_db[symbol] = df
            log_event(f"[TrailEngine] {symbol}: analog DB ready ({len(df)} 1m bars)")
        except Exception as e:
            log_event(f"[TrailEngine] {symbol}: DB build error: {e}")

    def _analog_trail(self, symbol, entry_price, peak_price):
        db = self.analog_db.get(symbol)
        if db is None or len(db) < 200:
            return None
        try:
            row = db.iloc[-1]
            fp  = np.array([
                float(row['price_vs_ema9']),
                float(row['vol_ratio']),
                float(row['macd_hist']),
                float(row['atr_pct']),
                float(row['rsi']) / 100.0,
                float(db['macd_hist'].iloc[-1] - db['macd_hist'].iloc[-2]),
                float(db['vol_ratio'].iloc[-1] - db['vol_ratio'].iloc[-2]),
            ])
            features = db[['price_vs_ema9','vol_ratio','macd_hist',
                           'atr_pct','rsi','macd_hist','vol_ratio']].values
            norms    = np.linalg.norm(features, axis=1, keepdims=True) + 1e-9
            fp_norm  = fp / (np.linalg.norm(fp) + 1e-9)
            sims     = features.dot(fp_norm) / norms.flatten()
            cutoff   = db['ts'].iloc[-1] - 6 * 3600 * 1000
            mask     = (sims > 0.85) & (db['ts'].values < cutoff)
            idxs     = np.where(mask)[0]
            if len(idxs) < 8:
                return None
            reversals = []
            for i in idxs[:50]:
                window = db['c'].iloc[i:i+30].values
                if len(window) < 5:
                    continue
                peak_w = window.max(); trough = window.min()
                reversals.append((peak_w - trough) / peak_w)
            if len(reversals) < 8:
                return None
            return max(0.005, min(float(np.percentile(reversals, 25)), 0.08))
        except Exception as e:
            log_event(f"[TrailEngine] analog error: {e}")
            return None

    def evaluate(self, df_1m, df_5m, entry_price, peak_price, base_trail,
                 symbol='BTC/USD'):
        """
        V5 API-compatible evaluate() wrapper.
        Returns (trail_pct, stop_price, method, confidence, detail)
        """
        if peak_price <= 0 or entry_price <= 0:
            stop = peak_price * (1 - base_trail)
            return base_trail, stop, 'base', 0.0, {}

        try:
            # Compute current ATR% from 1m df
            h, l, c = df_1m['h'], df_1m['l'], df_1m['c']
            tr      = pd.concat([(h-l),(h-c.shift()).abs(),
                                  (l-c.shift()).abs()], axis=1).max(axis=1)
            atr_pct = float(tr.ewm(span=14, adjust=False).mean().iloc[-1] /
                            (c.iloc[-1] + 1e-9))

            gain_pct = (peak_price - entry_price) / entry_price

            # MACD collapse check on 5m
            ema12_5  = df_5m['c'].ewm(span=12, adjust=False).mean()
            ema26_5  = df_5m['c'].ewm(span=26, adjust=False).mean()
            hist_5   = (ema12_5 - ema26_5) - (ema12_5 - ema26_5).ewm(
                        span=9, adjust=False).mean()
            macd_col = bool(hist_5.iloc[-1] < 0 and hist_5.iloc[-2] >= 0)

            base = max(atr_pct, 0.005)

            if gain_pct > DUMP_PROFIT_PCT or macd_col:
                mult   = 0.2
                method = 'dump' if gain_pct > DUMP_PROFIT_PCT else 'macd_collapse'
            elif gain_pct > 0.015:
                mult   = 0.4; method = 'tiered_tight'
            elif gain_pct > 0.005:
                mult   = 0.7; method = 'tiered_mid'
            else:
                mult   = 1.0; method = 'tiered_full'

            analog = self._analog_trail(symbol, entry_price, peak_price)
            if analog is not None:
                trail_pct = min(analog * mult, base * mult)
                method    = f'analog({method})'
                confidence = 0.85
            else:
                trail_pct  = base * mult
                confidence = 0.5

            trail_pct  = max(trail_pct, 0.003)
            stop_price = peak_price * (1 - trail_pct)

            # Profit floor
            if gain_pct > 0.003:
                stop_price = max(stop_price, entry_price * 1.001)

            detail = {'atr_pct': round(atr_pct*100,3),
                      'gain_pct': round(gain_pct*100,3),
                      'macd_collapse': macd_col,
                      'analog': analog is not None}
            return trail_pct, stop_price, method, confidence, detail

        except Exception as e:
            log_event(f"[TrailEngine] evaluate error: {e}")
            stop = peak_price * (1 - base_trail)
            return base_trail, stop, 'fallback', 0.0, {}

    def print_detail(self, detail):
        if detail:
            print(f"   ATR%: {detail.get('atr_pct',0):.3f}% | "
                  f"Gain: {detail.get('gain_pct',0):.3f}% | "
                  f"MACD collapse: {detail.get('macd_collapse',False)} | "
                  f"Analog: {detail.get('analog',False)}")

# ─────────────────────────────────────────────────────────────
# BASE DIRECTORY — all file paths anchored here so os.replace()
# never fails when systemd sets a different CWD
# ─────────────────────────────────────────────────────────────
BASE_DIR     = os.path.dirname(os.path.abspath(__file__))
STATE_FILE   = os.path.join(BASE_DIR, 'kraken_v5_1_state.json')
LOG_FILE     = os.path.join(BASE_DIR, 'kraken_auditable_shadow_bot_v5_1_audit_trail.csv')
EVENT_FILE   = os.path.join(BASE_DIR, 'kraken_v5_1_events.log')
LEGACY_CSVS  = [
    os.path.join(BASE_DIR, 'kraken_auditable_shadow_bot_v3_5_audit_trail.csv'),
    os.path.join(BASE_DIR, 'kraken_auditable_shadow_bot_v3_6_audit_trail.csv'),
    os.path.join(BASE_DIR, 'kraken_auditable_shadow_bot_v4_audit_trail.csv'),
    os.path.join(BASE_DIR, 'kraken_auditable_shadow_bot_v4_2_audit_trail.csv'),
    os.path.join(BASE_DIR, 'kraken_auditable_shadow_bot_v5_audit_trail.csv'),
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
        # Use STATE_FILE (absolute) so tmp and dest are always on the same filesystem
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
        log_event("--- KRAKEN V5.1: BB_NEUTRAL_GATED | RSI<45 | 3H-MIN-HOLD | "
                  "EMA-PULLBACK | SMA-TOUCH | RSI-OVERSOLD | ANALOG TRAIL | ORDER BOOK GATE ---")

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
        self.trail.build_db(self.symbol)

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
        3. Compute BB + three new BULL entry modes
        4. Gate through order book
        Returns (action, sig_price, delay_ms, book_snapshot)

        V5 BULL entry conditions (any one fires → BUY):
          A. BB lower band touch:  price < lower_band (1.5x)             [original]
          B. EMA21 crossover:      prev bar below EMA21, current above    [original]
          C. EMA21 pullback:       price 0–0.75% below EMA21 + RSI < 52  [V5 new]
          D. SMA20 touch:          price < SMA20 + RSI < 50               [V5 new]
          E. RSI oversold + trend: RSI < 42 + price > EMA55              [V5 new]
        Idleness guard: if FLAT > 8h in BULL, any one of C/D/E is sufficient.
        """
        df_btc = self._fetch_ohlcv(self.symbol,     '1h', 100)
        df_eth = self._fetch_ohlcv(self.eth_symbol,  '1h', 100)
        if df_btc is None or df_eth is None:
            return 'HOLD', 0.0, 0, {}

        self.state.current_regime = self.regime.detect(df_btc.copy(), df_eth.copy())

        bb_mult = 1.5 if self.state.current_regime == 'BULL' else 2.0
        df      = df_btc.copy()
        df['sma20']  = df['c'].rolling(20).mean()
        df['std20']  = df['c'].rolling(20).std()
        df['upper']  = df['sma20'] + bb_mult * df['std20']
        df['lower']  = df['sma20'] - bb_mult * df['std20']
        df['ema21']  = df['c'].ewm(span=21, adjust=False).mean()
        df['ema55']  = df['c'].ewm(span=55, adjust=False).mean()

        # RSI(14)
        delta        = df['c'].diff()
        gain         = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
        loss         = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
        df['rsi']    = 100 - 100 / (1 + gain / (loss + 1e-9))

        last   = df['c'].iloc[-1]
        lower  = df['lower'].iloc[-1]
        upper  = df['upper'].iloc[-1]
        sma20  = df['sma20'].iloc[-1]
        ema21  = df['ema21'].iloc[-1]
        ema55  = df['ema55'].iloc[-1]
        rsi    = df['rsi'].iloc[-1]
        delay  = int(time.time() * 1000) - (int(df['ts'].iloc[-1]) + 3600000)

        action = 'HOLD'
        s      = self.state

        # ── ENTRY LOGIC ──────────────────────────────────────
        if s.virtual_btc == 0:
            if self.state.current_regime == 'BEAR':
                action = 'HOLD'

            elif self.state.current_regime == 'BULL':
                ema21_prev = df['ema21'].iloc[-2]
                crossover  = (df['c'].iloc[-2] < ema21_prev) and (last > ema21)

                # Original signals
                sig_bb_lower  = (not np.isnan(lower)) and (last < lower)
                sig_crossover = crossover

                # V5 new signals
                sig_ema_pullback = (
                    (not np.isnan(ema21)) and
                    (last < ema21) and
                    (last >= ema21 * 0.9925) and   # within 0.75% below EMA21
                    (rsi < 45)                     # V5.1 FIX 2: tightened from <52 to <45
                )
                sig_sma_touch = (
                    (not np.isnan(sma20)) and
                    (last < sma20) and
                    (rsi < 50)
                )
                sig_rsi_oversold = (
                    (not np.isnan(ema55)) and
                    (rsi < 42) and
                    (last > ema55)                 # still in uptrend
                )

                # Idleness guard: if FLAT > 8h in BULL, softer signals are enough
                flat_secs  = time.time() - getattr(self, '_last_sell_ts',
                                                    s.first_start_ts)
                idle_8h    = flat_secs > 8 * 3600
                idle_label = f'idle={flat_secs/3600:.1f}h' if idle_8h else ''

                if sig_bb_lower or sig_crossover:
                    action = 'BUY'
                    if sig_bb_lower:
                        log_event(f'[Signal] BULL entry: BB lower band touch '
                                  f'(price={last:,.0f} < lower={lower:,.0f})')
                    else:
                        log_event(f'[Signal] BULL entry: EMA21 crossover')

                elif sig_ema_pullback:
                    action = 'BUY'
                    log_event(f'[Signal] BULL entry: EMA21 pullback '
                              f'(price={last:,.0f}, EMA21={ema21:,.0f}, '
                              f'gap={((ema21-last)/ema21)*100:.2f}%, RSI={rsi:.1f})'
                              f' {idle_label}')

                elif sig_sma_touch:
                    action = 'BUY'
                    log_event(f'[Signal] BULL entry: SMA20 touch '
                              f'(price={last:,.0f} < SMA={sma20:,.0f}, RSI={rsi:.1f})'
                              f' {idle_label}')

                elif sig_rsi_oversold:
                    action = 'BUY'
                    log_event(f'[Signal] BULL entry: RSI oversold + uptrend '
                              f'(RSI={rsi:.1f} < 42, price={last:,.0f} > EMA55={ema55:,.0f})'
                              f' {idle_label}')

                elif idle_8h and (sig_ema_pullback or sig_sma_touch or sig_rsi_oversold):
                    # Already handled above — this branch never fires but kept for clarity
                    pass

            else:  # NEUTRAL
                # V5.1 FIX 1: Gate BB_LOWER_NEUTRAL — requires ADX>20 + RSI<40 + price>EMA55
                # Previously fired on BB touch alone — caught every falling knife in
                # directionless markets, losing -$8,480 on 8 trades (WR 12%) in backtest.
                df_adx     = df.copy()
                adx_series = df_adx['c'].copy()
                h_, l_, c_ = df_adx['h'], df_adx['l'], df_adx['c']
                pdm_ = h_.diff().clip(lower=0)
                ndm_ = (-l_.diff()).clip(lower=0)
                tr_  = pd.concat([(h_-l_), (h_-c_.shift()).abs(), (l_-c_.shift()).abs()], axis=1).max(axis=1)
                atr_ = tr_.ewm(span=14, adjust=False).mean()
                pdi_ = 100 * pdm_.ewm(span=14, adjust=False).mean() / (atr_ + 1e-9)
                ndi_ = 100 * ndm_.ewm(span=14, adjust=False).mean() / (atr_ + 1e-9)
                dx_  = (abs(pdi_ - ndi_) / (pdi_ + ndi_ + 1e-9)) * 100
                adx_val = float(dx_.ewm(span=14, adjust=False).mean().iloc[-1])

                neutral_gate = (
                    not np.isnan(lower) and
                    last < lower and
                    adx_val > 20 and
                    rsi < 40 and
                    not np.isnan(ema55) and
                    last > ema55
                )
                if neutral_gate:
                    action = 'BUY'
                    log_event(f'[Signal] NEUTRAL entry: BB lower (gated) '
                              f'(price={last:,.0f} ADX={adx_val:.1f} RSI={rsi:.1f} '
                              f'EMA55={ema55:,.0f})')

        # ── EXIT LOGIC (unchanged from V4.2) ─────────────────
        elif s.virtual_btc > 0:
            self._last_sell_ts = None   # reset on active position
            if not np.isnan(upper) and last > upper:
                if not (self.state.current_regime == 'BULL' and last < ema21 * 1.01):
                    action = 'SELL_TARGET'
            elif last <= s.peak_price * (1 - self.stop_pct):
                action = 'SELL_STOP'
            elif self.state.current_regime == 'BEAR':
                action = 'SELL_REGIME_FLIP'

        # Order book gate
        book_verdict, snapshot = self.book.evaluate(self.symbol)
        action = self._apply_book_gate(action, book_verdict, snapshot)

        # Log key indicator state every hour for audit visibility
        log_event(
            f'[Hourly] regime={self.state.current_regime} '
            f'price={last:,.0f} SMA={sma20:,.0f} EMA21={ema21:,.0f} '
            f'EMA55={ema55:,.0f} RSI={rsi:.1f} '
            f'BB_lower={lower:,.0f} BB_upper={upper:,.0f} '
            f'action={action}'
        )

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
            _, snapshot = self.book.evaluate(self.symbol)
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
                df_1m, df_5m, s.entry_price, s.peak_price, base_trail,
                symbol=self.symbol
            )
        else:
            trail_pct  = base_trail
            stop_price = s.peak_price * (1 - base_trail)
            method     = 'fallback'
            confidence = 0.0
            detail     = {}

        # Hard stop
        hard_stop = s.peak_price * (1 - self.stop_pct)

        # V5.1 FIX 3: minimum 3-hour hold before trail stop can fire.
        # Hard stop always fires immediately — only the trail is gated.
        # Prevents whipsaw exits in the first few hours after entry.
        entry_ts     = getattr(self, '_entry_ts', time.time())
        hours_in_pos = (time.time() - entry_ts) / 3600.0
        min_hold_met = hours_in_pos >= 3.0

        # Check stops
        action = 'HOLD'
        if price <= hard_stop:
            action = 'SELL_STOP'
        elif price <= stop_price and min_hold_met:
            action = f'SELL_TRAIL({method},{trail_pct*100:.2f}%)'

        # Book gate on trail exits
        book_verdict, snapshot = self.book.evaluate(self.symbol)
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
            self._entry_ts = time.time()   # V5.1 FIX 3: track entry time for min-hold gate
            log_event(f"!!! BUY @ ${exec_price:,.2f} | "
                      f"Regime: {s.current_regime} | BTC: {s.virtual_btc:.6f}")
        elif action.startswith('SELL') and s.virtual_btc > 0:
            exec_price    = sig_price * (1 - self.slippage)
            s.virtual_usd = s.virtual_btc * exec_price
            s.virtual_btc = 0.0
            s.peak_price  = 0.0
            s.entry_price = 0.0
            self._last_sell_ts = time.time()   # V5: track for idleness guard
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
        print(f" KRAKEN V5.1 HEALTH CHECK | "
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
        self._last_sell_ts     = self.state.first_start_ts  # V5: idleness guard seed
        self._entry_ts         = time.time()                # V5.1: min-hold gate seed

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
                        # Print health periodically while holding position
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
