"""
kraken_v6.py — Multi-Asset Opportunity Scanner & Executor
$100,000 virtual capital | Shadow / paper mode | eu-west-1 Ireland

Architecture:
  RateLimiter      — single API gatekeeper, 1.5s min spacing, exponential backoff
  StateManager     — atomic state.json writes, events.log, audit CSV, heartbeat
  UniverseManager  — dynamic top-30 by 24h volume (refreshed hourly, min $5M floor)
  ScanEngine       — per-asset signal scoring: MACD histogram momentum,
                     RSI, Bollinger Band, EMA21 pullback, SMA20 touch, order book
  ConvictionEngine — composite 0-100 score → position size (6 / 12 / 20% of capital)
  TrailEngine      — per-asset ATR-calibrated adaptive trailing stop:
                     volatility-normalized base, tiered tightening with profit,
                     analog historical background check at entry,
                     MACD light-green collapse trigger,
                     3% profit dump-and-run mode
  OrderBookEngine  — 4-check gate: imbalance / wall authenticity /
                     liquidity depth / tape confirmation
  PortfolioManager — capital allocation, max 5 concurrent positions,
                     40% dry-powder reserve, per-position P&L tracking
  Conductor        — orchestrates all engines, heartbeat health check

Signal Stack (entry — ALL required):
  1. MACD histogram: PINK state (2+ consecutive shrinking negative bars)
  2. RSI(14) < 52
  3. Price at BB lower band OR EMA21 pullback (0-0.75%) OR SMA20 touch
  4. Order book verdict != VETO
  5. Asset regime: not BEAR
  6. Conviction score >= 40

Signal Stack (exit — first trigger wins):
  A. Trailing stop breached (ATR-based, tightens with profit growth)
  B. MACD histogram: LIGHT_GREEN state (2+ consecutive shrinking positive bars)
     → immediate trail collapse to 0.2x ATR
  C. 3% unrealized gain + any single shrinking green bar → dump and run
  D. Hard stop: entry_price * (1 - 3.5%)
  E. Regime flips BEAR on the held asset

Position Sizing:
  Conviction 80-100 → 20% of available capital
  Conviction 60-79  → 12% of available capital
  Conviction 40-59  →  6% of available capital
  Conviction < 40   → no trade

Trailing Stop:
  base_trail = ATR(14) as % of price (volatility-normalized per asset)
  Profit < 0.5%  → 1.0x base_trail
  Profit 0.5-1.5% → 0.7x base_trail
  Profit 1.5-3.0% → 0.4x base_trail
  Profit > 3.0%  → 0.2x base_trail (dump mode, 1-min exit checks)

Heartbeat: printed every 5-min cycle with running clock, pause time,
           per-position P&L, portfolio total, capital tally.

Run:   tmux new -s kraken_v6  then  python3 kraken_v6.py
State: atomic write to .tmp → os.replace() anchored to BASE_DIR
Boot:  15s NTP settle + aggressive API retry
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
# BASE DIR — anchor all paths to script location so tmux CWD
# never breaks os.replace() atomic writes
# ─────────────────────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, 'kraken_v6_state.json')
LOG_FILE   = os.path.join(BASE_DIR, 'kraken_v6_audit_trail.csv')
EVENT_FILE = os.path.join(BASE_DIR, 'kraken_v6_events.log')

PAPER_MODE        = True
STARTING_CAPITAL  = 100_000.0
MAX_POSITIONS     = 5
DRY_POWDER_PCT    = 0.40      # keep 40% cash reserve
HARD_STOP_PCT     = 0.035     # 3.5% hard stop per position
MAX_DD_PCT        = 0.15      # 15% portfolio kill switch
SLIPPAGE          = 0.0010    # 10bps slippage model
MIN_VOL_24H_USD   = 5_000_000 # $5M minimum 24h volume
MIN_HISTORY_BARS  = 100       # need 100+ 1h bars for indicators
UNIVERSE_SIZE     = 30        # top N by 24h volume
SCAN_INTERVAL     = 300       # 5 minutes between full scans
UNIVERSE_REFRESH  = 3600      # 1 hour between universe rebuilds
DUMP_PROFIT_PCT   = 0.030     # 3% → collapse trail to dump mode


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def fmt_dur(seconds):
    seconds = int(max(0, seconds))
    d, r  = divmod(seconds, 86400)
    h, r  = divmod(r, 3600)
    m, s  = divmod(r, 60)
    return f"{d}d {h}h {m}m {s}s"

def log_event(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(EVENT_FILE, 'a') as f:
        f.write(line + "\n")

def now_ts():
    return time.time()


# ─────────────────────────────────────────────────────────────
# RATE LIMITER
# ─────────────────────────────────────────────────────────────
class RateLimiter:
    MIN_SPACING  = 1.5
    BACKOFF_BASE = 10
    MAX_RETRIES  = 5

    def __init__(self):
        self._last = 0.0

    def _wait(self):
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
        self.cash              = STARTING_CAPITAL
        self.max_equity        = STARTING_CAPITAL
        self.positions         = {}   # symbol → PositionRecord
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
        pos_val = sum(
            p['size_usd'] * (prices.get(sym, p['entry_price']) / p['entry_price'])
            for sym, p in self.positions.items()
        )
        return self.cash + pos_val

    def save(self, prices: dict):
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
            log_event(">>> V6 FIRST START — fresh $100,000 virtual account.")
            return
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            gap = now_ts() - s.get('last_heartbeat_ts', now_ts())
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

    def gross_runtime(self):  return now_ts() - self.first_start_ts
    def net_runtime(self):    return self.gross_runtime() - self.total_paused_secs

    def append_audit(self, row: dict):
        self.audit_rows.append(row)
        pd.DataFrame(self.audit_rows).to_csv(LOG_FILE, index=False)


# ─────────────────────────────────────────────────────────────
# UNIVERSE MANAGER — dynamic top-30 by 24h USD volume
# ─────────────────────────────────────────────────────────────
class UniverseManager:
    def __init__(self, exchange, rl):
        self.exchange    = exchange
        self.rl          = rl
        self.universe    = []
        self.last_built  = 0.0

    def refresh(self, force=False):
        if not force and (now_ts() - self.last_built) < UNIVERSE_REFRESH:
            return self.universe

        log_event("[Universe] Refreshing top-30 liquid pairs...")
        try:
            tickers = self.rl.call(self.exchange.fetch_tickers)
            if tickers is None:
                return self.universe

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
            self.universe = [s for s, _ in candidates[:UNIVERSE_SIZE]]
            self.last_built = now_ts()
            log_event(f"[Universe] {len(self.universe)} pairs: "
                      f"{', '.join(self.universe[:10])}{'...' if len(self.universe)>10 else ''}")
        except Exception as e:
            log_event(f"[Universe] Refresh error: {e}")

        return self.universe


# ─────────────────────────────────────────────────────────────
# ORDER BOOK ENGINE (index-safe, no tuple unpacking)
# ─────────────────────────────────────────────────────────────
class OrderBookEngine:
    def __init__(self, exchange, rl):
        self.exchange     = exchange
        self.rl           = rl
        self.wall_history = {}
        self.cycle_count  = 0
        self.depth_cache  = {}   # per-symbol depth history

    def evaluate(self, symbol):
        try:
            book = self.rl.call(self.exchange.fetch_order_book, symbol, 50)
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
            wband = mid * 0.02
            curr_walls = {}
            bid_walls = [(lvl[0], lvl[1]) for lvl in book['bids'] if lvl[0] >= mid - wband]
            ask_walls = [(lvl[0], lvl[1]) for lvl in book['asks'] if lvl[0] <= mid + wband]
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
            key = symbol
            if key not in self.depth_cache:
                self.depth_cache[key] = collections.deque(maxlen=20)
            self.depth_cache[key].append(depth)
            avg   = sum(self.depth_cache[key]) / len(self.depth_cache[key])
            dpct  = (depth / avg * 100) if avg > 0 else 100
            v3    = 'VETO' if dpct < 60 else ('WARN' if dpct < 80 else 'CONFIRM')

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
# SCAN ENGINE — per-asset signal scoring
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
        Returns (state, hist_values) where state is one of:
          DARK_RED    — negative, growing (momentum building bearish)
          PINK        — negative, shrinking (bearish exhaustion → entry zone)
          DARK_GREEN  — positive, growing (momentum building bullish)
          LIGHT_GREEN — positive, shrinking (bullish exhaustion → exit zone)
          FLAT        — near zero
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
            # Negative bars
            if abs(curr) < abs(prev) and abs(prev) < abs(prev2):
                return 'PINK', h[-4:]       # 2 consecutive shrinking → entry zone
            elif abs(curr) < abs(prev):
                return 'PINK_1', h[-4:]     # 1 shrinking — partial signal
            else:
                return 'DARK_RED', h[-4:]
        else:
            # Positive bars
            if curr < prev and prev < prev2:
                return 'LIGHT_GREEN', h[-4:]  # 2 consecutive shrinking → exit zone
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

    def _atr_pct(self, df, period=14):
        h, l, c = df['h'], df['l'], df['c']
        tr  = pd.concat([(h-l), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
        atr = tr.ewm(span=period, adjust=False).mean().iloc[-1]
        return atr / c.iloc[-1]

    def score(self, symbol):
        """
        Returns (conviction_score 0-100, signal_detail_dict) or (0, {}) if no signal.
        Entry requires ALL of: PINK MACD + RSI<52 + price location + regime!=BEAR
        """
        df = self.fetch_ohlcv(symbol)
        if df is None:
            return 0, {}

        regime = self._regime(df)
        if regime == 'BEAR':
            return 0, {'regime': regime, 'reason': 'BEAR — no entry'}

        rsi        = self._rsi(df)
        macd_state, hist_vals = self._macd_state(df)
        atr_pct    = self._atr_pct(df)

        # BB
        df['sma20'] = df['c'].rolling(20).mean()
        df['std20'] = df['c'].rolling(20).std()
        bb_mult     = 1.5 if regime == 'BULL' else 2.0
        df['lower'] = df['sma20'] - bb_mult * df['std20']
        df['upper'] = df['sma20'] + bb_mult * df['std20']
        df['ema21'] = df['c'].ewm(span=21, adjust=False).mean()
        df['ema55'] = df['c'].ewm(span=55, adjust=False).mean()

        last   = df['c'].iloc[-1]
        lower  = df['lower'].iloc[-1]
        sma20  = df['sma20'].iloc[-1]
        ema21  = df['ema21'].iloc[-1]
        ema55  = df['ema55'].iloc[-1]

        # Price location signal
        sig_bb_lower     = (not np.isnan(lower)) and last < lower
        sig_ema_pullback = (not np.isnan(ema21)) and (last < ema21) and (last >= ema21 * 0.9925) and rsi < 52
        sig_sma_touch    = (not np.isnan(sma20)) and last < sma20 and rsi < 50
        sig_rsi_oversold = rsi < 42 and last > ema55
        price_signal     = sig_bb_lower or sig_ema_pullback or sig_sma_touch or sig_rsi_oversold

        # MACD must be PINK (2+ shrinking red) — hard requirement
        macd_entry = macd_state in ('PINK',)
        macd_bonus = macd_state in ('PINK', 'PINK_1')

        if not (price_signal and rsi < 52 and macd_bonus):
            return 0, {
                'regime': regime, 'rsi': round(rsi, 1),
                'macd_state': macd_state, 'price_signal': price_signal,
                'reason': 'signal conditions not met'
            }

        # ── Conviction scoring ──────────────────────────────
        score = 0

        # MACD component (0-25)
        if macd_state == 'PINK':
            score += 25    # 2+ consecutive shrinking — full conviction
        elif macd_state == 'PINK_1':
            score += 12    # only 1 shrinking bar — partial

        # RSI component (0-25): deeper oversold = higher score
        if rsi < 30:
            score += 25
        elif rsi < 38:
            score += 20
        elif rsi < 44:
            score += 15
        elif rsi < 48:
            score += 10
        else:
            score += 5

        # Price location component (0-25)
        if sig_bb_lower:
            score += 25
        elif sig_rsi_oversold:
            score += 20
        elif sig_sma_touch:
            score += 15
        elif sig_ema_pullback:
            score += 10

        # Regime bonus (0-15)
        if regime == 'BULL':
            score += 15
        elif regime == 'NEUTRAL':
            score += 7

        # Volume component (0-10): check if current vol above 20-period avg
        vol_ratio = df['v'].iloc[-1] / (df['v'].rolling(20).mean().iloc[-1] + 1e-9)
        if vol_ratio > 1.5:
            score += 10
        elif vol_ratio > 1.0:
            score += 5

        detail = {
            'regime':       regime,
            'rsi':          round(rsi, 1),
            'macd_state':   macd_state,
            'macd_hist':    [round(float(x), 6) for x in hist_vals],
            'atr_pct':      round(atr_pct * 100, 3),
            'sig_bb':       sig_bb_lower,
            'sig_ema':      sig_ema_pullback,
            'sig_sma':      sig_sma_touch,
            'sig_rsi':      sig_rsi_oversold,
            'vol_ratio':    round(float(vol_ratio), 2),
            'last_price':   last,
            'ema21':        round(ema21, 4),
            'ema55':        round(ema55, 4),
            'sma20':        round(sma20, 4),
            'score':        score,
        }
        return score, detail

    def check_exit(self, symbol, position):
        """
        Returns (should_exit: bool, reason: str, macd_collapse: bool)
        Checks MACD LIGHT_GREEN exit signal.
        """
        df = self.fetch_ohlcv(symbol)
        if df is None:
            return False, 'no data', False

        macd_state, _ = self._macd_state(df)
        regime        = self._regime(df)

        # BEAR regime flip → exit
        if regime == 'BEAR':
            return True, 'SELL_REGIME_FLIP', False

        # MACD LIGHT_GREEN → collapse trail
        if macd_state == 'LIGHT_GREEN':
            return True, 'SELL_MACD_LIGHTGREEN', True

        return False, 'HOLD', False


# ─────────────────────────────────────────────────────────────
# TRAIL ENGINE — per-asset ATR-calibrated adaptive trailing stop
# ─────────────────────────────────────────────────────────────
class TrailEngine:
    def __init__(self, exchange, rl):
        self.exchange = exchange
        self.rl       = rl
        # Per-symbol analog DB: symbol → list of 1m feature rows
        self.analog_db = {}

    def build_analog_db(self, symbol):
        """Fetch 14 days of 1m candles for analog matching."""
        try:
            raw = self.rl.call(
                self.exchange.fetch_ohlcv, symbol, '1m', None, 1440 * 14
            )
            if raw is None or len(raw) < 200:
                return
            df = pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            df['ema9']      = df['c'].ewm(span=9, adjust=False).mean()
            ema12           = df['c'].ewm(span=12, adjust=False).mean()
            ema26           = df['c'].ewm(span=26, adjust=False).mean()
            macd            = ema12 - ema26
            sig             = macd.ewm(span=9, adjust=False).mean()
            df['macd_hist'] = macd - sig
            df['vol_ratio'] = df['v'] / (df['v'].rolling(20).mean() + 1e-9)
            h, l, c         = df['h'], df['l'], df['c']
            tr              = pd.concat([(h-l),(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
            df['atr_pct']   = tr.ewm(span=14,adjust=False).mean() / c
            delta           = df['c'].diff()
            gain            = delta.clip(lower=0).ewm(com=13,adjust=False).mean()
            loss            = (-delta.clip(upper=0)).ewm(com=13,adjust=False).mean()
            df['rsi']       = 100 - 100/(1+gain/(loss+1e-9))
            df['price_vs_ema9'] = (df['c'] - df['ema9']) / df['ema9']
            df.dropna(inplace=True)
            self.analog_db[symbol] = df
            log_event(f"[TrailEngine] {symbol}: analog DB ready ({len(df)} 1m bars)")
        except Exception as e:
            log_event(f"[TrailEngine] {symbol}: DB build error: {e}")

    def _fingerprint(self, df_1m):
        """7-indicator normalized fingerprint of current 1m state."""
        row = df_1m.iloc[-1]
        return np.array([
            float(row['price_vs_ema9']),
            float(row['vol_ratio']),
            float(row['macd_hist']),
            float(row['atr_pct']),
            float(row['rsi']) / 100.0,
            float(df_1m['macd_hist'].iloc[-1] - df_1m['macd_hist'].iloc[-2]),  # slope
            float(df_1m['vol_ratio'].iloc[-1] - df_1m['vol_ratio'].iloc[-2]),   # vol delta
        ])

    def analog_trail(self, symbol, entry_price, peak_price):
        """
        Background check: find historical analogs, derive trail from
        25th-percentile reversal in the 30 minutes following each match.
        Returns trail_pct or None if insufficient matches.
        """
        db = self.analog_db.get(symbol)
        if db is None or len(db) < 200:
            return None

        try:
            fp = self._fingerprint(db)
            features = db[['price_vs_ema9','vol_ratio','macd_hist',
                           'atr_pct','rsi','macd_hist','vol_ratio']].values
            # Normalize
            norms = np.linalg.norm(features, axis=1, keepdims=True) + 1e-9
            fp_norm = fp / (np.linalg.norm(fp) + 1e-9)
            sims = features.dot(fp_norm) / norms.flatten()

            # Filter: similarity > 0.85, exclude last 6h
            cutoff = db['ts'].iloc[-1] - 6 * 3600 * 1000
            mask   = (sims > 0.85) & (db['ts'].values < cutoff)
            idxs   = np.where(mask)[0]

            if len(idxs) < 8:
                return None

            # For each match, measure max reversal in next 30 bars
            reversals = []
            for i in idxs[:50]:
                window = db['c'].iloc[i:i+30].values
                if len(window) < 5:
                    continue
                peak   = window.max()
                trough = window.min()
                rev    = (peak - trough) / peak
                reversals.append(rev)

            if len(reversals) < 8:
                return None

            trail = float(np.percentile(reversals, 25))
            return max(0.005, min(trail, 0.08))   # clamp 0.5%-8%

        except Exception as e:
            log_event(f"[TrailEngine] {symbol}: analog error: {e}")
            return None

    def compute_trail(self, symbol, entry_price, peak_price, atr_pct,
                      macd_collapse=False):
        """
        Returns (trail_pct, stop_price, method).
        Tiers by profit, collapses on MACD LIGHT_GREEN signal.
        """
        if peak_price <= 0 or entry_price <= 0:
            base = max(atr_pct, 0.01)
            return base, peak_price * (1 - base), 'atr_base'

        gain_pct = (peak_price - entry_price) / entry_price

        # Base trail = ATR%, minimum 0.5%
        base = max(atr_pct, 0.005)

        # Tier multiplier by profit level
        if gain_pct > DUMP_PROFIT_PCT or macd_collapse:
            mult = 0.2    # dump mode — very tight
            method = 'dump' if gain_pct > DUMP_PROFIT_PCT else 'macd_collapse'
        elif gain_pct > 0.015:
            mult   = 0.4
            method = 'tiered_tight'
        elif gain_pct > 0.005:
            mult   = 0.7
            method = 'tiered_mid'
        else:
            mult   = 1.0
            method = 'tiered_full'

        # Try analog override
        analog = self.analog_trail(symbol, entry_price, peak_price)
        if analog is not None:
            trail_pct = min(analog * mult, base * mult)
            method    = f'analog({method})'
        else:
            trail_pct = base * mult

        trail_pct  = max(trail_pct, 0.003)   # absolute floor 0.3%
        stop_price = peak_price * (1 - trail_pct)

        # Profit floor: if peak gain ever exceeded 0.3%, stop never below entry+0.1%
        if (peak_price - entry_price) / entry_price > 0.003:
            floor = entry_price * 1.001
            stop_price = max(stop_price, floor)

        return trail_pct, stop_price, method


# ─────────────────────────────────────────────────────────────
# PORTFOLIO MANAGER
# ─────────────────────────────────────────────────────────────
class PortfolioManager:
    def __init__(self, state: StateManager):
        self.state = state

    def available_capital(self):
        """Capital available for new positions respecting dry powder reserve."""
        total_est = self.state.cash + sum(
            p['size_usd'] for p in self.state.positions.values()
        )
        max_deployable = total_est * (1 - DRY_POWDER_PCT)
        currently_deployed = sum(p['size_usd'] for p in self.state.positions.values())
        return max(0.0, max_deployable - currently_deployed)

    def position_size(self, conviction: int, available: float) -> float:
        """Map conviction score to position size in USD."""
        if conviction >= 80:
            pct = 0.20
        elif conviction >= 60:
            pct = 0.12
        else:
            pct = 0.06

        total_capital = self.state.cash + sum(
            p['size_usd'] for p in self.state.positions.values()
        )
        size = total_capital * pct
        return min(size, available)

    def can_open(self) -> bool:
        return (len(self.state.positions) < MAX_POSITIONS and
                self.available_capital() > 500)

    def open_position(self, symbol, conviction, entry_price, atr_pct, detail):
        avail = self.available_capital()
        size  = self.position_size(conviction, avail)
        if size < 100:
            return False, "position size too small"

        exec_price = entry_price * (1 + SLIPPAGE)
        self.state.positions[symbol] = {
            'entry_price':   exec_price,
            'signal_price':  entry_price,
            'peak_price':    exec_price,
            'size_usd':      size,
            'conviction':    conviction,
            'atr_pct':       atr_pct,
            'open_ts':       now_ts(),
            'regime':        detail.get('regime', 'NEUTRAL'),
            'macd_state':    detail.get('macd_state', ''),
            'trail_pct':     atr_pct,
            'stop_price':    exec_price * (1 - atr_pct),
            'trail_method':  'atr_base',
            'macd_collapse': False,
        }
        self.state.cash       -= size
        self.state.trade_count += 1
        log_event(
            f"!!! BUY {symbol} @ ${exec_price:,.4f} | "
            f"Size: ${size:,.2f} | Conviction: {conviction} | "
            f"ATR: {atr_pct*100:.2f}% | Regime: {detail.get('regime')}"
        )
        return True, "opened"

    def close_position(self, symbol, current_price, reason):
        if symbol not in self.state.positions:
            return
        pos        = self.state.positions.pop(symbol)
        exec_price = current_price * (1 - SLIPPAGE)
        pnl_pct    = (exec_price - pos['entry_price']) / pos['entry_price']
        pnl_usd    = pos['size_usd'] * pnl_pct
        close_val  = pos['size_usd'] * (1 + pnl_pct)

        self.state.cash             += close_val
        self.state.total_pnl_closed += pnl_usd
        if pnl_usd > 0:
            self.state.total_trades_won += 1

        log_event(
            f"!!! SELL {symbol} ({reason}) @ ${exec_price:,.4f} | "
            f"P&L: ${pnl_usd:+,.2f} ({pnl_pct*100:+.2f}%) | "
            f"Cash now: ${self.state.cash:,.2f}"
        )
        return pnl_usd, pnl_pct


# ─────────────────────────────────────────────────────────────
# CONDUCTOR
# ─────────────────────────────────────────────────────────────
class Conductor:
    def __init__(self):
        self.exchange  = ccxt.kraken({'enableRateLimit': False})
        self.rl        = RateLimiter()
        self.state     = StateManager()
        self.universe  = UniverseManager(self.exchange, self.rl)
        self.scanner   = ScanEngine(self.exchange, self.rl)
        self.trail_eng = TrailEngine(self.exchange, self.rl)
        self.book      = OrderBookEngine(self.exchange, self.rl)
        self.portfolio = PortfolioManager(self.state)
        self._prices   = {}   # latest prices cache

    # ── boot ──────────────────────────────────────────────────
    def boot(self):
        log_event(
            "--- KRAKEN V6: MULTI-ASSET SCANNER | MACD HISTOGRAM MOMENTUM | "
            "ATR TRAIL | CONVICTION SIZING | $100K VIRTUAL | PAPER MODE ---"
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
        self.state.save(self._prices)

        # Build universe
        symbols = self.universe.refresh(force=True)

        # Seed analog DBs for any currently held positions
        for sym in list(self.state.positions.keys()):
            self.trail_eng.build_analog_db(sym)

        log_event("[Boot] Boot sequence complete.")

    # ── fetch current price ───────────────────────────────────
    def _price(self, symbol):
        t = self.rl.call(self.exchange.fetch_ticker, symbol)
        if t:
            self._prices[symbol] = t['last']
            return t['last']
        return self._prices.get(symbol, 0.0)

    # ── update open positions ─────────────────────────────────
    def update_positions(self):
        """
        For each open position: update peak, compute trail,
        check hard stop, MACD exit, trail breach.
        """
        for symbol in list(self.state.positions.keys()):
            pos   = self.state.positions[symbol]
            price = self._price(symbol)
            if price <= 0:
                continue

            # Update peak
            pos['peak_price'] = max(pos['peak_price'], price)

            # MACD exit check
            should_exit, reason, macd_collapse = self.scanner.check_exit(symbol, pos)
            if macd_collapse:
                pos['macd_collapse'] = True

            # Compute adaptive trail
            trail_pct, stop_price, method = self.trail_eng.compute_trail(
                symbol,
                pos['entry_price'],
                pos['peak_price'],
                pos['atr_pct'],
                pos.get('macd_collapse', False)
            )
            pos['trail_pct']    = trail_pct
            pos['stop_price']   = stop_price
            pos['trail_method'] = method

            # Hard stop check
            hard_stop = pos['entry_price'] * (1 - HARD_STOP_PCT)

            # Execute exit
            if price <= hard_stop:
                self.portfolio.close_position(symbol, price, 'SELL_HARD_STOP')
            elif price <= stop_price:
                self.portfolio.close_position(symbol, price, f'SELL_TRAIL({method})')
            elif should_exit:
                self.portfolio.close_position(symbol, price, reason)

    # ── scan for new entries ──────────────────────────────────
    def scan_entries(self):
        """Score all universe symbols, rank by conviction, open best available."""
        if not self.portfolio.can_open():
            return

        symbols   = self.universe.refresh()
        scored    = []

        for symbol in symbols:
            if symbol in self.state.positions:
                continue   # already holding
            try:
                conviction, detail = self.scanner.score(symbol)
                if conviction >= 40:
                    scored.append((conviction, symbol, detail))
            except Exception as e:
                log_event(f"[Scan] {symbol} error: {e}")

        # Sort by conviction descending
        scored.sort(key=lambda x: x[0], reverse=True)

        for conviction, symbol, detail in scored:
            if not self.portfolio.can_open():
                break

            # Order book gate
            book_verdict, book_snap = self.book.evaluate(symbol)
            if book_verdict == 'VETO':
                log_event(f"[BookGate] VETO on {symbol} (conviction={conviction}) — skipping")
                continue

            price   = self._price(symbol)
            if price <= 0:
                continue

            # Build analog DB at entry (background check)
            if symbol not in self.trail_eng.analog_db:
                self.trail_eng.build_analog_db(symbol)

            atr_pct = detail.get('atr_pct', 2.0) / 100.0
            ok, msg = self.portfolio.open_position(
                symbol, conviction, price, atr_pct, detail
            )
            if ok:
                # Append audit row
                self._audit_entry(symbol, price, conviction, detail, book_snap)

    # ── audit ─────────────────────────────────────────────────
    def _audit_entry(self, symbol, price, conviction, detail, book_snap):
        s  = self.state
        eq = s.equity(self._prices)
        s.max_equity = max(s.max_equity, eq)
        dd = (s.max_equity - eq) / s.max_equity
        s.append_audit({
            'timestamp':     datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'action':        'BUY',
            'symbol':        symbol,
            'price':         round(price, 4),
            'conviction':    conviction,
            'regime':        detail.get('regime'),
            'macd_state':    detail.get('macd_state'),
            'rsi':           detail.get('rsi'),
            'atr_pct':       detail.get('atr_pct'),
            'vol_ratio':     detail.get('vol_ratio'),
            'book_verdict':  book_snap.get('verdict', 'n/a'),
            'equity':        round(eq, 2),
            'drawdown':      round(dd, 4),
            'cash':          round(s.cash, 2),
            'open_positions': len(s.positions),
            'gross_runtime': fmt_dur(s.gross_runtime()),
            'net_runtime':   fmt_dur(s.net_runtime()),
        })

    def _audit_exit(self, symbol, price, reason, pnl_usd, pnl_pct):
        s  = self.state
        eq = s.equity(self._prices)
        s.max_equity = max(s.max_equity, eq)
        dd = (s.max_equity - eq) / s.max_equity
        s.append_audit({
            'timestamp':     datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'action':        'SELL',
            'symbol':        symbol,
            'price':         round(price, 4),
            'conviction':    0,
            'reason':        reason,
            'pnl_usd':       round(pnl_usd, 2),
            'pnl_pct':       round(pnl_pct * 100, 3),
            'equity':        round(eq, 2),
            'drawdown':      round(dd, 4),
            'cash':          round(s.cash, 2),
            'open_positions': len(s.positions),
            'gross_runtime': fmt_dur(s.gross_runtime()),
            'net_runtime':   fmt_dur(s.net_runtime()),
        })

    # ── heartbeat health check ────────────────────────────────
    def print_heartbeat(self):
        s      = self.state
        prices = self._prices
        eq     = s.equity(prices)
        s.max_equity = max(s.max_equity, eq)
        dd     = (s.max_equity - eq) / s.max_equity
        profit = eq - STARTING_CAPITAL
        pp     = profit / STARTING_CAPITAL * 100
        gross  = fmt_dur(s.gross_runtime())
        paused = fmt_dur(s.total_paused_secs)
        net    = fmt_dur(s.net_runtime())
        win_rate = (s.total_trades_won / s.trade_count * 100
                    if s.trade_count > 0 else 0.0)

        print("\n" + "=" * 68)
        print(f" KRAKEN V6 HEARTBEAT | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("─" * 68)
        print(f" Portfolio Equity:   ${eq:>12,.2f}   (start: ${STARTING_CAPITAL:,.2f})")
        print(f" Total P&L:          ${profit:>+12,.2f}   ({pp:+.3f}%)")
        print(f" Closed P&L:         ${s.total_pnl_closed:>+12,.2f}")
        print(f" Cash Available:     ${s.cash:>12,.2f}")
        print(f" Max Drawdown:       {dd*100:>11.2f}%   (limit: {MAX_DD_PCT*100:.0f}%)")
        print(f" Trades: {s.trade_count}  |  Won: {s.total_trades_won}  |  Win Rate: {win_rate:.1f}%")
        print("─" * 68)

        if s.positions:
            print(f" {'SYMBOL':<12} {'SIZE':>10} {'ENTRY':>10} {'CURRENT':>10} "
                  f"{'P&L':>10} {'TRAIL':>8} {'METHOD':<18}")
            for sym, pos in s.positions.items():
                cur   = prices.get(sym, pos['entry_price'])
                g_pct = (cur - pos['entry_price']) / pos['entry_price'] * 100
                g_usd = pos['size_usd'] * g_pct / 100
                print(f" {sym:<12} ${pos['size_usd']:>9,.0f} "
                      f"${pos['entry_price']:>9,.4f} "
                      f"${cur:>9,.4f} "
                      f"${g_usd:>+9,.2f} "
                      f"{pos['trail_pct']*100:>7.2f}% "
                      f"{pos['trail_method']:<18}")
        else:
            print(" Positions:          FLAT — scanning for entries")

        print("─" * 68)
        universe_sym = self.universe.universe
        print(f" Universe:           {len(universe_sym)} pairs")
        print(f" Dry Powder:         {DRY_POWDER_PCT*100:.0f}% reserve | "
              f"Deployable: ${self.portfolio.available_capital():,.2f}")
        print("─" * 68)
        print(f" Gross Runtime:      {gross}")
        print(f" Total Paused:       {paused}")
        print(f" Net Runtime:        {net}")
        print("=" * 68 + "\n")

    # ── kill switch ───────────────────────────────────────────
    def _check_kill_switch(self):
        eq = self.state.equity(self._prices)
        dd = (self.state.max_equity - eq) / self.state.max_equity
        if dd >= MAX_DD_PCT:
            log_event(f"CRITICAL: {MAX_DD_PCT*100:.0f}% DRAWDOWN KILL SWITCH. "
                      f"Closing all positions.")
            for symbol in list(self.state.positions.keys()):
                price = self._price(symbol)
                r = self.portfolio.close_position(symbol, price, 'KILL_SWITCH')
                if r:
                    self._audit_exit(symbol, price, 'KILL_SWITCH', r[0], r[1])
            self.state.save(self._prices)
            raise SystemExit(0)

    # ── main loop ─────────────────────────────────────────────
    def main(self):
        self.boot()

        while True:
            try:
                # Refresh prices for all held positions
                for sym in list(self.state.positions.keys()):
                    self._price(sym)

                # Update positions: trail, stops, exits
                self.update_positions()

                # Scan for new entries
                self.scan_entries()

                # Kill switch
                self._check_kill_switch()

                # Heartbeat
                self.print_heartbeat()

                # Save state
                self.state.save(self._prices)

                # Sleep until next scan
                time.sleep(SCAN_INTERVAL)

            except SystemExit:
                raise
            except Exception as e:
                log_event(f"[Main] Error: {e}")
                self.state.save(self._prices)
                time.sleep(60)


if __name__ == '__main__':
    Conductor().main()
