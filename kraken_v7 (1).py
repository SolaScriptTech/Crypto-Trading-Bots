"""
kraken_v7.py — Multi-Asset Opportunity Scanner & Executor
$100,000 virtual capital | Shadow / paper mode | eu-west-1 Ireland

═══════════════════════════════════════════════════════════════════
ARCHITECTURE — THREE DECOUPLED THREADS
═══════════════════════════════════════════════════════════════════

  Thread 1 · EXIT LOOP  (every 20 seconds)
  ─────────────────────────────────────────
  · Fetches live ticker price for every open position
  · Evaluates stop ladder: hard stop → trailing stop → MACD exit
  · Trailing stop is anchored to 1h ATR (same timeframe as entry)
  · Tiers trail multiplier by unrealized profit level
  · Arms break-even floor once position reaches +0.8%
  · Does NOT scan for new entries — purely defensive
  · Writes audit CSV row on every exit
  · Registers closed symbol in CooldownRegistry

  Thread 2 · ENTRY SCAN  (every 5 minutes)
  ─────────────────────────────────────────
  · Scores all universe symbols using 1h OHLCV signal stack
  · Blocks entry on any symbol currently in CooldownRegistry
  · MACD entry gate: PINK only (2+ consecutive shrinking neg bars)
    — PINK_1 (single shrinking bar) is explicitly rejected
  · Conviction → position size mapping unchanged from V6
  · Registers new positions and deducts cash atomically under lock

  Thread 3 · HEARTBEAT  (every 5 minutes, offset by 30s)
  ─────────────────────────────────────────────────────
  · Prints portfolio summary to stdout
  · Saves state.json atomically
  · Checks portfolio kill-switch (15% max drawdown)

  CooldownRegistry
  ─────────────────────────────────────────
  · On any stop-out or forced exit: symbol locked for 2 × 3600s
    (two full 1h candle closes must pass before re-entry allowed)
  · Prevents the V6 "re-entry grinder" where bot immediately
    re-buys a just-stopped asset on the same PINK signal

═══════════════════════════════════════════════════════════════════
SIGNAL STACK (entry — ALL required)
═══════════════════════════════════════════════════════════════════
  1. MACD histogram: PINK state (2+ consecutive shrinking neg bars)
     — PINK_1 explicitly blocked (learned from V6)
  2. RSI(14) < 52
  3. Price at BB lower band OR EMA21 pullback (0-0.75%)
     OR SMA20 touch OR RSI < 42 above EMA55
  4. Order book verdict != VETO
  5. Asset regime: not BEAR
  6. Conviction score >= 40
  7. Symbol NOT in CooldownRegistry

═══════════════════════════════════════════════════════════════════
SIGNAL STACK (exit — priority ladder, first trigger wins)
═══════════════════════════════════════════════════════════════════
  A. Hard stop:     entry_price × (1 - 3.5%)
  B. Break-even:    once +0.8% gain, stop floor = entry_price
  C. Trailing stop: armed after +0.8% gain, 1h-ATR anchored
                    Tier by profit:
                      < +0.5%  → 1.0 × ATR_1h (full)
                      +0.5–1.5% → 0.7 × ATR_1h (mid)
                      +1.5–3.0% → 0.4 × ATR_1h (tight)
                      > +3.0%  → 0.2 × ATR_1h (dump mode)
  D. MACD LIGHT_GREEN (2+ consecutive shrinking pos bars)
     → collapse trail to 0.2 × ATR immediately
  E. Regime flips BEAR on held asset
  F. Time stop: 12h maximum hold

═══════════════════════════════════════════════════════════════════
POSITION SIZING (conviction → % of total equity)
═══════════════════════════════════════════════════════════════════
  Conviction 80-100 → 20%
  Conviction 60-79  → 12%
  Conviction 40-59  →  6%
  < 40              → no trade

Run:   tmux new -s kraken_v7  then  python3 kraken_v7.py
State: atomic write to .tmp → os.replace() anchored to BASE_DIR
Boot:  15s NTP settle + aggressive API retry
"""

import ccxt
import pandas as pd
import numpy as np
import threading
import time
import os
import json
import math
import collections
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────
# BASE DIR — anchor all paths to script location
# ─────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, 'kraken_v7_state.json')
LOG_FILE   = os.path.join(BASE_DIR, 'kraken_v7_audit_trail.csv')
EVENT_FILE = os.path.join(BASE_DIR, 'kraken_v7_events.log')

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
PAPER_MODE          = True
STARTING_CAPITAL    = 100_000.0
MAX_POSITIONS       = 5
DRY_POWDER_PCT      = 0.40       # keep 40% cash reserve
HARD_STOP_PCT       = 0.035      # 3.5% hard stop per position
MAX_DD_PCT          = 0.15       # 15% portfolio kill switch
SLIPPAGE            = 0.0010     # 10bps slippage model
MIN_VOL_24H_USD     = 5_000_000  # $5M minimum 24h volume
MIN_HISTORY_BARS    = 100        # need 100+ 1h bars for indicators
UNIVERSE_SIZE       = 30         # top N by 24h volume
EXIT_INTERVAL       = 20         # exit thread: seconds between checks
SCAN_INTERVAL       = 300        # entry scan: seconds between scans
HEARTBEAT_INTERVAL  = 300        # heartbeat: seconds between prints
UNIVERSE_REFRESH    = 3600       # universe rebuild: seconds
COOLDOWN_SECS       = 7200       # 2h cooldown after stop-out
BE_TRIGGER_PCT      = 0.008      # +0.8% → move stop to break-even
TRAIL_ARM_PCT       = 0.008      # +0.8% → arm trailing stop
DUMP_PROFIT_PCT     = 0.030      # +3.0% → collapse to dump-mode trail
MAX_HOLD_SECS       = 43200      # 12h maximum hold time


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def fmt_dur(seconds):
    seconds = int(max(0, seconds))
    d, r = divmod(seconds, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    return f"{d}d {h}h {m}m {s}s"

def log_event(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(EVENT_FILE, 'a') as f:
        f.write(line + "\n")

def now_ts():
    return time.time()

def safe_float(v, default=0.0):
    try:
        f = float(v)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return default


# ─────────────────────────────────────────────────────────────
# COOLDOWN REGISTRY
# ─────────────────────────────────────────────────────────────
class CooldownRegistry:
    """
    Per-symbol lockout after a stop-out.
    Two full 1h candles must pass before re-entry is allowed.
    Prevents the V6 re-entry grinder.
    """
    def __init__(self):
        self._lock    = threading.Lock()
        self._cooldowns = {}   # symbol → earliest_re_entry_ts

    def register(self, symbol: str, reason: str):
        until = now_ts() + COOLDOWN_SECS
        with self._lock:
            self._cooldowns[symbol] = until
        log_event(f"[Cooldown] {symbol} locked for {COOLDOWN_SECS//3600}h after {reason} "
                  f"(until {datetime.fromtimestamp(until).strftime('%H:%M:%S')})")

    def is_locked(self, symbol: str) -> bool:
        with self._lock:
            until = self._cooldowns.get(symbol, 0)
            if until and now_ts() < until:
                return True
            if symbol in self._cooldowns:
                del self._cooldowns[symbol]   # expired — clean up
            return False

    def status(self) -> dict:
        with self._lock:
            return {s: until for s, until in self._cooldowns.items()
                    if now_ts() < until}


# ─────────────────────────────────────────────────────────────
# RATE LIMITER
# ─────────────────────────────────────────────────────────────
class RateLimiter:
    MIN_SPACING  = 1.5
    BACKOFF_BASE = 10
    MAX_RETRIES  = 5

    def __init__(self):
        self._last = 0.0
        self._lock = threading.Lock()

    def _wait(self):
        with self._lock:
            gap = self.MIN_SPACING - (now_ts() - self._last)
            if gap > 0:
                time.sleep(gap)
            self._last = now_ts()

    def call(self, fn, *args, **kwargs):
        for attempt in range(self.MAX_RETRIES):
            try:
                self._wait()
                return fn(*args, **kwargs)
            except ccxt.RateLimitExceeded as e:
                wait = self.BACKOFF_BASE * (2 ** attempt)
                log_event(f"[RateLimiter] Rate limit — backoff {wait}s: {e}")
                time.sleep(wait)
            except ccxt.NetworkError as e:
                wait = self.BACKOFF_BASE * (2 ** attempt)
                log_event(f"[RateLimiter] Network error — backoff {wait}s: {e}")
                time.sleep(wait)
            except Exception as e:
                raise e
        log_event("[RateLimiter] Max retries exceeded — skipping.")
        return None


# ─────────────────────────────────────────────────────────────
# STATE MANAGER
# ─────────────────────────────────────────────────────────────
class StateManager:
    def __init__(self):
        self._lock             = threading.Lock()
        self.cash              = STARTING_CAPITAL
        self.max_equity        = STARTING_CAPITAL
        self.positions         = {}
        self.trade_count       = 0
        self.total_trades_won  = 0
        self.total_pnl_closed  = 0.0
        now = now_ts()
        self.first_start_ts    = now
        self.session_start_ts  = now
        self.total_paused_secs = 0.0
        self.last_heartbeat_ts = now
        self.audit_rows        = []

    def equity(self, prices: dict) -> float:
        with self._lock:
            pos_val = sum(
                p['size_usd'] * (prices.get(sym, p['entry_price']) / p['entry_price'])
                for sym, p in self.positions.items()
            )
            return self.cash + pos_val

    def save(self, prices: dict):
        with self._lock:
            positions_serial = {
                sym: {k: v for k, v in pos.items()}
                for sym, pos in self.positions.items()
            }
            state = {
                'cash':              self.cash,
                'max_equity':        self.max_equity,
                'positions':         positions_serial,
                'trade_count':       self.trade_count,
                'total_trades_won':  self.total_trades_won,
                'total_pnl_closed':  self.total_pnl_closed,
                'first_start_ts':    self.first_start_ts,
                'total_paused_secs': self.total_paused_secs,
                'session_start_ts':  self.session_start_ts,
                'last_heartbeat_ts': now_ts(),
            }
        tmp = STATE_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_FILE)

    def load(self):
        if not os.path.exists(STATE_FILE):
            log_event(">>> V7 FIRST START — fresh $100,000 virtual account.")
            return
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            gap = now_ts() - s.get('last_heartbeat_ts', now_ts())
            with self._lock:
                self.cash              = s['cash']
                self.max_equity        = s['max_equity']
                self.positions         = s.get('positions', {})
                self.trade_count       = s['trade_count']
                self.total_trades_won  = s.get('total_trades_won', 0)
                self.total_pnl_closed  = s.get('total_pnl_closed', 0.0)
                self.first_start_ts    = s['first_start_ts']
                self.total_paused_secs = s['total_paused_secs']
                self.session_start_ts  = now_ts()
            if gap > 300:
                self.total_paused_secs += gap
                log_event(
                    f">>> BOT RESTARTED | Gap: {fmt_dur(gap)} | "
                    f"Cash: ${self.cash:,.2f} | "
                    f"Positions: {len(self.positions)} | "
                    f"Trades: {self.trade_count}"
                )
            else:
                log_event(f">>> BOT RESUMED (warm restart, gap={fmt_dur(gap)})")
        except Exception as e:
            log_event(f"WARNING: state.json unreadable ({e}) — fresh start.")

    def gross_runtime(self): return now_ts() - self.first_start_ts
    def net_runtime(self):   return self.gross_runtime() - self.total_paused_secs

    def append_audit(self, row: dict):
        with self._lock:
            self.audit_rows.append(row)
            rows_copy = list(self.audit_rows)
        pd.DataFrame(rows_copy).to_csv(LOG_FILE, index=False)


# ─────────────────────────────────────────────────────────────
# UNIVERSE MANAGER
# ─────────────────────────────────────────────────────────────
class UniverseManager:
    def __init__(self, exchange, rl):
        self.exchange   = exchange
        self.rl         = rl
        self.universe   = []
        self.last_built = 0.0
        self._lock      = threading.Lock()

    def refresh(self, force=False):
        with self._lock:
            if not force and (now_ts() - self.last_built) < UNIVERSE_REFRESH:
                return list(self.universe)

        log_event("[Universe] Refreshing top-30 liquid pairs...")
        try:
            tickers = self.rl.call(self.exchange.fetch_tickers)
            if tickers is None:
                with self._lock:
                    return list(self.universe)

            candidates = []
            for symbol, t in tickers.items():
                if not symbol.endswith('/USD'):
                    continue
                if symbol in ('USDT/USD', 'USDC/USD', 'DAI/USD', 'BUSD/USD'):
                    continue
                vol_usd = (t.get('quoteVolume') or 0)
                if vol_usd < MIN_VOL_24H_USD:
                    continue
                candidates.append((symbol, vol_usd))

            candidates.sort(key=lambda x: x[1], reverse=True)
            universe = [s for s, _ in candidates[:UNIVERSE_SIZE]]
            with self._lock:
                self.universe   = universe
                self.last_built = now_ts()
            log_event(f"[Universe] {len(universe)} pairs: "
                      f"{', '.join(universe[:10])}{'...' if len(universe)>10 else ''}")
        except Exception as e:
            log_event(f"[Universe] Refresh error: {e}")

        with self._lock:
            return list(self.universe)


# ─────────────────────────────────────────────────────────────
# ORDER BOOK ENGINE
# ─────────────────────────────────────────────────────────────
class OrderBookEngine:
    def __init__(self, exchange, rl):
        self.exchange     = exchange
        self.rl           = rl
        self.wall_history = {}
        self.cycle_count  = 0
        self.depth_cache  = {}

    def evaluate(self, symbol):
        try:
            book   = self.rl.call(self.exchange.fetch_order_book, symbol, 50)
            trades = self.rl.call(self.exchange.fetch_trades, symbol,
                                  **{"since": None, "limit": 50})
            if trades is None:
                trades = []
            if not book['bids'] or not book['asks']:
                return 'CONFIRM', {}

            mid = (book['bids'][0][0] + book['asks'][0][0]) / 2

            # Check 1: Imbalance
            band  = mid * 0.005
            bids  = sum(lvl[1] for lvl in book['bids'] if lvl[0] >= mid - band)
            asks  = sum(lvl[1] for lvl in book['asks'] if lvl[0] <= mid + band)
            ratio = bids / asks if asks > 0 else 999
            v1    = 'CONFIRM' if ratio > 0.67 else 'WARN'

            # Check 2: Wall authenticity (2-cycle threshold)
            self.cycle_count += 1
            wband     = mid * 0.02
            curr_walls = {}
            bid_walls  = [(lvl[0], lvl[1]) for lvl in book['bids'] if lvl[0] >= mid - wband]
            ask_walls  = [(lvl[0], lvl[1]) for lvl in book['asks'] if lvl[0] <= mid + wband]
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

            # Check 3: Liquidity depth
            dband = mid * 0.01
            depth = (sum(lvl[1] for lvl in book['bids'] if lvl[0] >= mid - dband) +
                     sum(lvl[1] for lvl in book['asks'] if lvl[0] <= mid + dband))
            if symbol not in self.depth_cache:
                self.depth_cache[symbol] = collections.deque(maxlen=20)
            self.depth_cache[symbol].append(depth)
            avg  = sum(self.depth_cache[symbol]) / len(self.depth_cache[symbol])
            dpct = (depth / avg * 100) if avg > 0 else 100
            v3   = 'VETO' if dpct < 60 else ('WARN' if dpct < 80 else 'CONFIRM')

            # Check 4: Tape
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

            return final, {
                'mid': round(mid, 4), 'ratio': round(ratio, 3),
                'depth_pct': round(dpct, 1), 'buy_pct': round(bpct, 1),
                'checks': verdicts, 'verdict': final
            }
        except Exception as e:
            return 'CONFIRM', {'error': str(e)}


# ─────────────────────────────────────────────────────────────
# SCAN ENGINE — 1h signal scoring
# ─────────────────────────────────────────────────────────────
class ScanEngine:
    def __init__(self, exchange, rl):
        self.exchange = exchange
        self.rl       = rl

    def fetch_ohlcv(self, symbol, tf='1h', limit=150):
        raw = self.rl.call(self.exchange.fetch_ohlcv, symbol, tf, None, limit)
        if raw is None or len(raw) < MIN_HISTORY_BARS:
            return None
        df = pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        return df

    def _macd_state(self, df):
        """
        Returns (state, hist_values).
        States: DARK_RED | PINK | PINK_1 | DARK_GREEN | LIGHT_GREEN | LIGHT_GREEN_1 | FLAT

        V7 entry gate: PINK only (2+ consecutive shrinking neg bars).
        PINK_1 explicitly blocked — learned from V6 re-entry grinder.
        """
        ema12 = df['c'].ewm(span=12, adjust=False).mean()
        ema26 = df['c'].ewm(span=26, adjust=False).mean()
        macd  = ema12 - ema26
        sig   = macd.ewm(span=9, adjust=False).mean()
        hist  = macd - sig

        h     = hist.values
        curr  = h[-1]
        prev  = h[-2]
        prev2 = h[-3]

        if abs(curr) < 1e-8:
            return 'FLAT', h[-4:]

        if curr < 0:
            if abs(curr) < abs(prev) and abs(prev) < abs(prev2):
                return 'PINK', h[-4:]        # 2+ consecutive shrinking → entry
            elif abs(curr) < abs(prev):
                return 'PINK_1', h[-4:]      # 1 shrinking bar — BLOCKED in V7
            else:
                return 'DARK_RED', h[-4:]
        else:
            if curr < prev and prev < prev2:
                return 'LIGHT_GREEN', h[-4:]
            elif curr < prev:
                return 'LIGHT_GREEN_1', h[-4:]
            else:
                return 'DARK_GREEN', h[-4:]

    def _rsi(self, df, period=14):
        delta = df['c'].diff()
        gain  = delta.clip(lower=0).ewm(com=period-1, adjust=False).mean()
        loss  = (-delta.clip(upper=0)).ewm(com=period-1, adjust=False).mean()
        return (100 - 100 / (1 + gain / (loss + 1e-9))).iloc[-1]

    def _regime(self, df):
        df = df.copy()
        df['ema21'] = df['c'].ewm(span=21, adjust=False).mean()
        df['ema55'] = df['c'].ewm(span=55, adjust=False).mean()
        last = df.iloc[-1]
        if last['ema21'] > last['ema55'] and last['c'] > last['ema21']:
            return 'BULL'
        if last['ema21'] < last['ema55']:
            return 'BEAR'
        return 'NEUTRAL'

    def atr_pct_1h(self, symbol) -> float:
        """
        Fetch 1h OHLCV and compute ATR(14) as % of price.
        This is the canonical ATR for all trailing stop calculations in V7.
        Returns a float (e.g. 0.012 = 1.2%).
        """
        df = self.fetch_ohlcv(symbol, tf='1h', limit=50)
        if df is None:
            return 0.02   # fallback 2% if no data
        h, l, c = df['h'], df['l'], df['c']
        tr  = pd.concat([(h-l), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
        atr = tr.ewm(span=14, adjust=False).mean().iloc[-1]
        return safe_float(atr / c.iloc[-1], 0.02)

    def score(self, symbol):
        """
        Returns (conviction_score 0-100, detail_dict).
        Entry requires ALL of: PINK MACD + RSI<52 + price location + regime!=BEAR.
        PINK_1 is explicitly rejected in V7.
        """
        df = self.fetch_ohlcv(symbol)
        if df is None:
            return 0, {}

        regime = self._regime(df)
        if regime == 'BEAR':
            return 0, {'regime': regime, 'reason': 'BEAR — no entry'}

        rsi             = self._rsi(df)
        macd_state, hist_vals = self._macd_state(df)

        # ── V7 gate: PINK only, PINK_1 explicitly rejected ──
        if macd_state != 'PINK':
            return 0, {
                'regime': regime, 'rsi': round(rsi, 1),
                'macd_state': macd_state,
                'reason': f'MACD {macd_state} — PINK required (V7 strict gate)'
            }

        # BB / price location
        df['sma20'] = df['c'].rolling(20).mean()
        df['std20'] = df['c'].rolling(20).std()
        bb_mult     = 1.5 if regime == 'BULL' else 2.0
        df['lower'] = df['sma20'] - bb_mult * df['std20']
        df['ema21'] = df['c'].ewm(span=21, adjust=False).mean()
        df['ema55'] = df['c'].ewm(span=55, adjust=False).mean()

        last  = df['c'].iloc[-1]
        lower = df['lower'].iloc[-1]
        sma20 = df['sma20'].iloc[-1]
        ema21 = df['ema21'].iloc[-1]
        ema55 = df['ema55'].iloc[-1]

        sig_bb_lower     = (not np.isnan(lower)) and last < lower
        sig_ema_pullback = (not np.isnan(ema21)) and (last < ema21) and (last >= ema21 * 0.9925) and rsi < 52
        sig_sma_touch    = (not np.isnan(sma20)) and last < sma20 and rsi < 50
        sig_rsi_oversold = rsi < 42 and last > ema55
        price_signal     = sig_bb_lower or sig_ema_pullback or sig_sma_touch or sig_rsi_oversold

        if not (price_signal and rsi < 52):
            return 0, {
                'regime': regime, 'rsi': round(rsi, 1),
                'macd_state': macd_state, 'price_signal': price_signal,
                'reason': 'price/RSI conditions not met'
            }

        # ── Conviction scoring ──────────────────────────────
        score = 0

        # MACD: PINK = 25 (PINK_1 never reaches here in V7)
        score += 25

        # RSI depth
        if rsi < 30:   score += 25
        elif rsi < 38: score += 20
        elif rsi < 44: score += 15
        elif rsi < 48: score += 10
        else:          score += 5

        # Price location
        if sig_bb_lower:        score += 25
        elif sig_rsi_oversold:  score += 20
        elif sig_sma_touch:     score += 15
        elif sig_ema_pullback:  score += 10

        # Regime bonus
        if regime == 'BULL':    score += 15
        elif regime == 'NEUTRAL': score += 7

        # Volume
        vol_ratio = df['v'].iloc[-1] / (df['v'].rolling(20).mean().iloc[-1] + 1e-9)
        if vol_ratio > 1.5:   score += 10
        elif vol_ratio > 1.0: score += 5

        # Compute 1h ATR for position record
        atr_pct = self.atr_pct_1h(symbol)

        detail = {
            'regime':     regime,
            'rsi':        round(rsi, 1),
            'macd_state': macd_state,
            'macd_hist':  [round(float(x), 6) for x in hist_vals],
            'atr_pct':    round(atr_pct * 100, 3),   # stored as %, e.g. 1.2
            'sig_bb':     sig_bb_lower,
            'sig_ema':    sig_ema_pullback,
            'sig_sma':    sig_sma_touch,
            'sig_rsi':    sig_rsi_oversold,
            'vol_ratio':  round(float(vol_ratio), 2),
            'last_price': last,
            'ema21':      round(ema21, 4),
            'ema55':      round(ema55, 4),
            'sma20':      round(sma20, 4),
            'score':      score,
        }
        return score, detail

    def check_macd_exit(self, symbol) -> tuple:
        """
        Returns (should_exit: bool, reason: str, is_light_green: bool).
        Called by exit thread for MACD-based exit signals.
        """
        df = self.fetch_ohlcv(symbol)
        if df is None:
            return False, 'no data', False

        macd_state, _ = self._macd_state(df)
        regime        = self._regime(df)

        if regime == 'BEAR':
            return True, 'SELL_REGIME_FLIP', False
        if macd_state == 'LIGHT_GREEN':
            return True, 'SELL_MACD_LIGHTGREEN', True

        return False, 'HOLD', False


# ─────────────────────────────────────────────────────────────
# TRAIL ENGINE — 1h ATR anchored adaptive trailing stop
# ─────────────────────────────────────────────────────────────
class TrailEngine:
    """
    V7 redesign: all trail calculations are anchored to 1h ATR.

    The V6 bug was using 1m ATR from analog matching as the base,
    which created stops too tight to survive normal 1h candle noise.

    Here the trail is: ATR_1h × tier_multiplier, where the
    multiplier tightens as unrealized profit grows — exactly
    mirroring the backtester's simulate_dynamic_exit logic.
    """

    def compute_trail(self, entry_price: float, peak_price: float,
                      atr_pct: float, macd_collapse: bool = False) -> tuple:
        """
        Returns (trail_pct, stop_price, method_str).

        atr_pct: 1h ATR as decimal (e.g. 0.012 = 1.2%)
        """
        if peak_price <= 0 or entry_price <= 0:
            base = max(atr_pct, 0.01)
            return base, peak_price * (1 - base), 'atr_1h_base'

        gain_pct = (peak_price - entry_price) / entry_price

        # Base = 1h ATR, minimum 0.8% (wide enough to survive hourly noise)
        base = max(atr_pct, 0.008)

        if macd_collapse or gain_pct > DUMP_PROFIT_PCT:
            mult   = 0.2
            method = 'dump' if gain_pct > DUMP_PROFIT_PCT else 'macd_collapse'
        elif gain_pct > 0.015:
            mult   = 0.4
            method = 'tight'
        elif gain_pct > 0.005:
            mult   = 0.7
            method = 'mid'
        else:
            mult   = 1.0
            method = 'full'

        trail_pct  = base * mult
        trail_pct  = max(trail_pct, 0.005)   # absolute floor 0.5%
        stop_price = peak_price * (1 - trail_pct)

        # Break-even floor: once peak gain > 0.8%, stop never below entry
        if (peak_price - entry_price) / entry_price >= BE_TRIGGER_PCT:
            stop_price = max(stop_price, entry_price)
            if method == 'full':
                method = 'full_be'

        return trail_pct, stop_price, f'atr_1h_{method}'


# ─────────────────────────────────────────────────────────────
# PORTFOLIO MANAGER
# ─────────────────────────────────────────────────────────────
class PortfolioManager:
    def __init__(self, state: StateManager):
        self.state = state

    def _total_capital(self):
        return self.state.cash + sum(
            p['size_usd'] for p in self.state.positions.values()
        )

    def available_capital(self):
        total        = self._total_capital()
        max_deploy   = total * (1 - DRY_POWDER_PCT)
        deployed     = sum(p['size_usd'] for p in self.state.positions.values())
        return max(0.0, max_deploy - deployed)

    def position_size(self, conviction: int) -> float:
        if conviction >= 80:    pct = 0.20
        elif conviction >= 60:  pct = 0.12
        else:                   pct = 0.06
        size = self._total_capital() * pct
        return min(size, self.available_capital())

    def can_open(self) -> bool:
        with self.state._lock:
            return (len(self.state.positions) < MAX_POSITIONS and
                    self.available_capital() > 500)

    def open_position(self, symbol, conviction, entry_price, atr_pct, detail):
        with self.state._lock:
            if len(self.state.positions) >= MAX_POSITIONS:
                return False, "max positions"
            if self.available_capital() < 500:
                return False, "insufficient capital"

            size = self.position_size(conviction)
            if size < 100:
                return False, "position size too small"

            exec_price = entry_price * (1 + SLIPPAGE)
            self.state.positions[symbol] = {
                'entry_price':    exec_price,
                'signal_price':   entry_price,
                'peak_price':     exec_price,
                'size_usd':       size,
                'conviction':     conviction,
                'atr_pct':        atr_pct,   # 1h ATR as decimal
                'open_ts':        now_ts(),
                'regime':         detail.get('regime', 'NEUTRAL'),
                'macd_state':     detail.get('macd_state', ''),
                'trail_pct':      atr_pct,
                'stop_price':     exec_price * (1 - atr_pct),
                'trail_method':   'atr_1h_base',
                'macd_collapse':  False,
                'be_promoted':    False,    # break-even floor applied
                'trail_armed':    False,    # trailing stop armed
            }
            self.state.cash        -= size
            self.state.trade_count += 1

        log_event(
            f"!!! BUY {symbol} @ ${exec_price:,.4f} | "
            f"Size: ${size:,.2f} | Conviction: {conviction} | "
            f"ATR(1h): {atr_pct*100:.2f}% | Regime: {detail.get('regime')}"
        )
        return True, "opened"

    def close_position(self, symbol, current_price, reason):
        with self.state._lock:
            if symbol not in self.state.positions:
                return None
            pos        = self.state.positions.pop(symbol)
        exec_price = current_price * (1 - SLIPPAGE)
        pnl_pct    = (exec_price - pos['entry_price']) / pos['entry_price']
        pnl_usd    = pos['size_usd'] * pnl_pct
        close_val  = pos['size_usd'] * (1 + pnl_pct)

        with self.state._lock:
            self.state.cash             += close_val
            self.state.total_pnl_closed += pnl_usd
            if pnl_usd > 0:
                self.state.total_trades_won += 1

        log_event(
            f"!!! SELL {symbol} ({reason}) @ ${exec_price:,.4f} | "
            f"P&L: ${pnl_usd:+,.2f} ({pnl_pct*100:+.2f}%) | "
            f"Cash now: ${self.state.cash:,.2f}"
        )
        return pnl_usd, pnl_pct, pos


# ─────────────────────────────────────────────────────────────
# CONDUCTOR — orchestrates all threads
# ─────────────────────────────────────────────────────────────
class Conductor:
    def __init__(self):
        self.exchange  = ccxt.kraken({'enableRateLimit': False})
        self.rl        = RateLimiter()
        self.state     = StateManager()
        self.universe  = UniverseManager(self.exchange, self.rl)
        self.scanner   = ScanEngine(self.exchange, self.rl)
        self.trail_eng = TrailEngine()
        self.book      = OrderBookEngine(self.exchange, self.rl)
        self.portfolio = PortfolioManager(self.state)
        self.cooldown  = CooldownRegistry()
        self._prices   = {}
        self._prices_lock = threading.Lock()
        self._shutdown = threading.Event()

    # ── price fetch ───────────────────────────────────────────
    def _price(self, symbol) -> float:
        t = self.rl.call(self.exchange.fetch_ticker, symbol)
        if t:
            with self._prices_lock:
                self._prices[symbol] = t['last']
            return t['last']
        with self._prices_lock:
            return self._prices.get(symbol, 0.0)

    def _prices_snapshot(self) -> dict:
        with self._prices_lock:
            return dict(self._prices)

    # ── boot ──────────────────────────────────────────────────
    def boot(self):
        log_event(
            "--- KRAKEN V7: THREADED EXIT LOOP | 1h ATR TRAIL | "
            "PINK-ONLY ENTRY | COOLDOWN REGISTRY | $100K VIRTUAL | PAPER MODE ---"
        )
        log_event("[Boot] Waiting 15s for NTP clock sync...")
        time.sleep(15)

        log_event("[Boot] Verifying Kraken API connectivity...")
        deadline = now_ts() + 300
        while now_ts() < deadline:
            try:
                self.rl.call(self.exchange.fetch_time)
                log_event("[Boot] Kraken API reachable.")
                break
            except Exception as e:
                log_event(f"[Boot] Not reachable ({e}) — retry in 15s")
                time.sleep(15)
        else:
            log_event("[Boot] CRITICAL: API unreachable. Exiting.")
            raise SystemExit(1)

        self.state.load()
        self.state.save(self._prices_snapshot())
        self.universe.refresh(force=True)
        log_event("[Boot] Boot sequence complete.")

    # ─────────────────────────────────────────────────────────
    # THREAD 1: EXIT LOOP (every 20 seconds)
    # ─────────────────────────────────────────────────────────
    def exit_loop(self):
        """
        Runs every EXIT_INTERVAL seconds.
        Evaluates stop ladder for all open positions.
        This is the only thing that runs fast — no OHLCV fetches
        except for MACD exit check (1h, cached by scanner).

        Stop priority ladder (mirrors backtester's simulate_dynamic_exit):
          1. Hard stop
          2. Max hold time
          3. MACD LIGHT_GREEN / regime flip (1h signal)
          4. Trailing stop (1h ATR anchored, break-even promoted)
        """
        log_event("[ExitLoop] Thread started.")
        while not self._shutdown.is_set():
            try:
                with self.state._lock:
                    symbols = list(self.state.positions.keys())

                for symbol in symbols:
                    try:
                        self._evaluate_position(symbol)
                    except Exception as e:
                        log_event(f"[ExitLoop] {symbol} error: {e}")

            except Exception as e:
                log_event(f"[ExitLoop] Outer error: {e}")

            self._shutdown.wait(EXIT_INTERVAL)

    def _evaluate_position(self, symbol: str):
        with self.state._lock:
            pos = self.state.positions.get(symbol)
        if pos is None:
            return   # already closed by another check

        price = self._price(symbol)
        if price <= 0:
            return

        # Update peak price
        with self.state._lock:
            if symbol not in self.state.positions:
                return
            self.state.positions[symbol]['peak_price'] = max(
                self.state.positions[symbol]['peak_price'], price
            )
            pos = dict(self.state.positions[symbol])   # snapshot

        entry  = pos['entry_price']
        peak   = pos['peak_price']
        atr    = pos['atr_pct']
        open_ts = pos['open_ts']

        # ── 1. Hard stop ─────────────────────────────────────
        hard_stop = entry * (1 - HARD_STOP_PCT)
        if price <= hard_stop:
            result = self.portfolio.close_position(symbol, price, 'SELL_HARD_STOP')
            if result:
                pnl_usd, pnl_pct, closed_pos = result
                self._audit_exit(symbol, price, 'SELL_HARD_STOP', pnl_usd, pnl_pct)
                self.cooldown.register(symbol, 'SELL_HARD_STOP')
            return

        # ── 2. Max hold time ─────────────────────────────────
        if (now_ts() - open_ts) >= MAX_HOLD_SECS:
            result = self.portfolio.close_position(symbol, price, 'SELL_TIME_STOP')
            if result:
                pnl_usd, pnl_pct, _ = result
                self._audit_exit(symbol, price, 'SELL_TIME_STOP', pnl_usd, pnl_pct)
                # Time stop: shorter cooldown — thesis may be valid later
                self.cooldown.register(symbol, 'SELL_TIME_STOP')
            return

        # ── 3. MACD / regime exit (1h — checked every EXIT_INTERVAL) ──
        should_exit, exit_reason, is_light_green = self.scanner.check_macd_exit(symbol)

        if is_light_green:
            with self.state._lock:
                if symbol in self.state.positions:
                    self.state.positions[symbol]['macd_collapse'] = True
                    pos = dict(self.state.positions[symbol])

        # ── 4. Compute adaptive 1h ATR trail ─────────────────
        macd_collapse = pos.get('macd_collapse', False)
        trail_pct, stop_price, method = self.trail_eng.compute_trail(
            entry, peak, atr, macd_collapse
        )
        with self.state._lock:
            if symbol in self.state.positions:
                self.state.positions[symbol]['trail_pct']   = trail_pct
                self.state.positions[symbol]['stop_price']  = stop_price
                self.state.positions[symbol]['trail_method'] = method

                # Track BE promotion and trail arming for heartbeat display
                gain_pct = (peak - entry) / entry
                if gain_pct >= BE_TRIGGER_PCT:
                    self.state.positions[symbol]['be_promoted'] = True
                if gain_pct >= TRAIL_ARM_PCT:
                    self.state.positions[symbol]['trail_armed'] = True

        # Execute MACD exit now that trail is collapsed
        if should_exit:
            result = self.portfolio.close_position(symbol, price, exit_reason)
            if result:
                pnl_usd, pnl_pct, _ = result
                self._audit_exit(symbol, price, exit_reason, pnl_usd, pnl_pct)
                self.cooldown.register(symbol, exit_reason)
            return

        # ── 5. Trailing stop breach ───────────────────────────
        # Only enforce trail once armed (after BE_TRIGGER_PCT gain)
        trail_armed = pos.get('trail_armed', False)
        if trail_armed and price <= stop_price:
            result = self.portfolio.close_position(symbol, price, f'SELL_TRAIL({method})')
            if result:
                pnl_usd, pnl_pct, _ = result
                self._audit_exit(symbol, price, f'SELL_TRAIL({method})', pnl_usd, pnl_pct)
                self.cooldown.register(symbol, 'SELL_TRAIL')
            return

        # ── 6. Hard stop check again against live stop_price ─
        # (covers case where stop has been promoted above entry)
        if price <= stop_price and pos.get('be_promoted', False):
            result = self.portfolio.close_position(symbol, price, 'SELL_BE_STOP')
            if result:
                pnl_usd, pnl_pct, _ = result
                self._audit_exit(symbol, price, 'SELL_BE_STOP', pnl_usd, pnl_pct)
                self.cooldown.register(symbol, 'SELL_BE_STOP')

    # ─────────────────────────────────────────────────────────
    # THREAD 2: ENTRY SCAN (every 5 minutes)
    # ─────────────────────────────────────────────────────────
    def entry_scan_loop(self):
        """
        Runs every SCAN_INTERVAL seconds.
        Scores universe symbols, opens best available positions.
        Respects CooldownRegistry — will not enter any symbol
        that was recently stopped out.
        """
        log_event("[EntryLoop] Thread started.")
        # Stagger by 30s from boot so exit loop runs first
        self._shutdown.wait(30)

        while not self._shutdown.is_set():
            try:
                self._scan_entries()
            except Exception as e:
                log_event(f"[EntryLoop] Error: {e}")
            self._shutdown.wait(SCAN_INTERVAL)

    def _scan_entries(self):
        if not self.portfolio.can_open():
            return

        symbols = self.universe.refresh()
        scored  = []

        for symbol in symbols:
            with self.state._lock:
                already_held = symbol in self.state.positions
            if already_held:
                continue

            # CooldownRegistry gate — V7 key addition
            if self.cooldown.is_locked(symbol):
                continue

            try:
                conviction, detail = self.scanner.score(symbol)
                if conviction >= 40:
                    scored.append((conviction, symbol, detail))
            except Exception as e:
                log_event(f"[EntryLoop] {symbol} score error: {e}")

        scored.sort(key=lambda x: x[0], reverse=True)

        for conviction, symbol, detail in scored:
            if not self.portfolio.can_open():
                break

            book_verdict, book_snap = self.book.evaluate(symbol)
            if book_verdict == 'VETO':
                log_event(f"[BookGate] VETO on {symbol} (conviction={conviction}) — skipping")
                continue

            price = self._price(symbol)
            if price <= 0:
                continue

            # atr_pct is already 1h ATR in decimal form from scanner.score()
            atr_pct = detail.get('atr_pct', 2.0) / 100.0

            ok, msg = self.portfolio.open_position(
                symbol, conviction, price, atr_pct, detail
            )
            if ok:
                self._audit_entry(symbol, price, conviction, detail, book_snap)

    # ─────────────────────────────────────────────────────────
    # THREAD 3: HEARTBEAT (every 5 minutes)
    # ─────────────────────────────────────────────────────────
    def heartbeat_loop(self):
        """
        Prints portfolio status, saves state, checks kill switch.
        Offset 60s from boot so positions are stable before first print.
        """
        log_event("[Heartbeat] Thread started.")
        self._shutdown.wait(60)

        while not self._shutdown.is_set():
            try:
                prices = self._prices_snapshot()
                self._check_kill_switch(prices)
                self._print_heartbeat(prices)
                self.state.save(prices)
            except Exception as e:
                log_event(f"[Heartbeat] Error: {e}")
            self._shutdown.wait(HEARTBEAT_INTERVAL)

    def _check_kill_switch(self, prices):
        eq = self.state.equity(prices)
        with self.state._lock:
            mx = self.state.max_equity
        dd = (mx - eq) / mx if mx > 0 else 0
        if dd >= MAX_DD_PCT:
            log_event(f"CRITICAL: {MAX_DD_PCT*100:.0f}% DRAWDOWN KILL SWITCH — closing all.")
            with self.state._lock:
                symbols = list(self.state.positions.keys())
            for symbol in symbols:
                price  = self._price(symbol)
                result = self.portfolio.close_position(symbol, price, 'KILL_SWITCH')
                if result:
                    pnl_usd, pnl_pct, _ = result
                    self._audit_exit(symbol, price, 'KILL_SWITCH', pnl_usd, pnl_pct)
            self.state.save(self._prices_snapshot())
            self._shutdown.set()
            raise SystemExit(0)

    def _print_heartbeat(self, prices):
        s  = self.state
        eq = s.equity(prices)
        with s._lock:
            s.max_equity = max(s.max_equity, eq)
            mx = s.max_equity
        dd       = (mx - eq) / mx if mx > 0 else 0
        profit   = eq - STARTING_CAPITAL
        pp       = profit / STARTING_CAPITAL * 100
        win_rate = (s.total_trades_won / s.trade_count * 100
                    if s.trade_count > 0 else 0.0)

        print("\n" + "=" * 72)
        print(f" KRAKEN V7 HEARTBEAT | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("─" * 72)
        print(f" Portfolio Equity:   ${eq:>12,.2f}   (start: ${STARTING_CAPITAL:,.2f})")
        print(f" Total P&L:          ${profit:>+12,.2f}   ({pp:+.3f}%)")
        print(f" Closed P&L:         ${s.total_pnl_closed:>+12,.2f}")
        print(f" Cash Available:     ${s.cash:>12,.2f}")
        print(f" Max Drawdown:       {dd*100:>11.2f}%   (limit: {MAX_DD_PCT*100:.0f}%)")
        print(f" Trades: {s.trade_count}  |  Won: {s.total_trades_won}  |  Win Rate: {win_rate:.1f}%")
        print("─" * 72)

        with s._lock:
            positions = dict(s.positions)

        if positions:
            print(f" {'SYMBOL':<12} {'SIZE':>10} {'ENTRY':>10} {'CURR':>10} "
                  f"{'P&L':>10} {'TRAIL%':>8} {'BE':>4} {'METHOD':<20}")
            for sym, pos in positions.items():
                cur   = prices.get(sym, pos['entry_price'])
                g_pct = (cur - pos['entry_price']) / pos['entry_price'] * 100
                g_usd = pos['size_usd'] * g_pct / 100
                be    = '✓' if pos.get('be_promoted') else ' '
                print(f" {sym:<12} ${pos['size_usd']:>9,.0f} "
                      f"${pos['entry_price']:>9,.4f} "
                      f"${cur:>9,.4f} "
                      f"${g_usd:>+9,.2f} "
                      f"{pos['trail_pct']*100:>7.2f}% "
                      f"{be:>4} "
                      f"{pos['trail_method']:<20}")
        else:
            print(" Positions:          FLAT — scanning for entries")

        # Cooldown status
        cds = self.cooldown.status()
        if cds:
            print("─" * 72)
            print(f" Cooling down ({len(cds)}): " +
                  ", ".join(f"{s} until {datetime.fromtimestamp(t).strftime('%H:%M')}"
                            for s, t in cds.items()))

        print("─" * 72)
        uni = self.universe.universe
        print(f" Universe:           {len(uni)} pairs")
        print(f" Dry Powder:         {DRY_POWDER_PCT*100:.0f}% reserve | "
              f"Deployable: ${self.portfolio.available_capital():,.2f}")
        print("─" * 72)
        print(f" Gross Runtime:      {fmt_dur(s.gross_runtime())}")
        print(f" Total Paused:       {fmt_dur(s.total_paused_secs)}")
        print(f" Net Runtime:        {fmt_dur(s.net_runtime())}")
        print("=" * 72 + "\n")

    # ─────────────────────────────────────────────────────────
    # AUDIT TRAIL
    # ─────────────────────────────────────────────────────────
    def _audit_entry(self, symbol, price, conviction, detail, book_snap):
        prices = self._prices_snapshot()
        eq     = self.state.equity(prices)
        with self.state._lock:
            self.state.max_equity = max(self.state.max_equity, eq)
            mx = self.state.max_equity
        dd = (mx - eq) / mx if mx > 0 else 0
        self.state.append_audit({
            'timestamp':      datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'action':         'BUY',
            'symbol':         symbol,
            'price':          round(price, 6),
            'conviction':     conviction,
            'regime':         detail.get('regime'),
            'macd_state':     detail.get('macd_state'),
            'rsi':            detail.get('rsi'),
            'atr_pct_1h':     detail.get('atr_pct'),
            'vol_ratio':      detail.get('vol_ratio'),
            'book_verdict':   book_snap.get('verdict', 'n/a'),
            'equity':         round(eq, 2),
            'drawdown':       round(dd, 4),
            'cash':           round(self.state.cash, 2),
            'open_positions': len(self.state.positions),
            'cooldowns_active': len(self.cooldown.status()),
            'gross_runtime':  fmt_dur(self.state.gross_runtime()),
            'net_runtime':    fmt_dur(self.state.net_runtime()),
        })

    def _audit_exit(self, symbol, price, reason, pnl_usd, pnl_pct):
        prices = self._prices_snapshot()
        eq     = self.state.equity(prices)
        with self.state._lock:
            self.state.max_equity = max(self.state.max_equity, eq)
            mx = self.state.max_equity
        dd = (mx - eq) / mx if mx > 0 else 0
        self.state.append_audit({
            'timestamp':      datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'action':         'SELL',
            'symbol':         symbol,
            'price':          round(price, 6),
            'conviction':     0,
            'reason':         reason,
            'pnl_usd':        round(pnl_usd, 2),
            'pnl_pct':        round(pnl_pct * 100, 4),
            'equity':         round(eq, 2),
            'drawdown':       round(dd, 4),
            'cash':           round(self.state.cash, 2),
            'open_positions': len(self.state.positions),
            'cooldowns_active': len(self.cooldown.status()),
            'gross_runtime':  fmt_dur(self.state.gross_runtime()),
            'net_runtime':    fmt_dur(self.state.net_runtime()),
        })

    # ─────────────────────────────────────────────────────────
    # MAIN — launch all three threads
    # ─────────────────────────────────────────────────────────
    def main(self):
        self.boot()

        threads = [
            threading.Thread(target=self.exit_loop,       name='ExitLoop',   daemon=True),
            threading.Thread(target=self.entry_scan_loop, name='EntryLoop',  daemon=True),
            threading.Thread(target=self.heartbeat_loop,  name='Heartbeat',  daemon=True),
        ]

        for t in threads:
            t.start()
            log_event(f"[Main] Thread '{t.name}' started.")

        # Main thread just monitors — keeps process alive and
        # handles clean shutdown on KeyboardInterrupt
        try:
            while not self._shutdown.is_set():
                # Watchdog: log if any thread has died unexpectedly
                for t in threads:
                    if not t.is_alive():
                        log_event(f"[Main] WARNING: Thread '{t.name}' died — check logs.")
                self._shutdown.wait(60)
        except KeyboardInterrupt:
            log_event("[Main] KeyboardInterrupt received — shutting down gracefully.")
            self._shutdown.set()
            self.state.save(self._prices_snapshot())
            log_event("[Main] State saved. Goodbye.")


if __name__ == '__main__':
    Conductor().main()
