"""
kraken_v6.1 — Multi-Asset Opportunity Scanner & Executor
$100,000 virtual capital | Shadow / paper mode | eu-west-1 Ireland

Architecture: Single-loop, timestamp-gated (Matthew's design)
  - Fast tick (15s): price fetch + hard stop + trail stop only
  - Slow tick (5m):  MACD/regime exit check + entry scan + heartbeat
  - Cooldowns persisted in state.json — survive restarts

Fixes applied over the initial v6.1 draft:
  Fix 1 — MACD exit check moved to slow (5-min) gate.
           Was running every 15s tick → ~20 OHLCV API calls/min at
           5 positions → rate limit breach. Now runs every 5 minutes
           alongside the entry scan, which is the correct cadence since
           it reads 1h candles that only close once per hour anyway.

  Fix 2 — Trail floor raised from 0.3% → 0.5%, base minimum from
           0.5% → 0.8% of price. The 1h ATR on most liquid crypto
           pairs runs 0.8–1.5%. A 0.3% floor gets hit by a single
           adverse 1h candle before the thesis has had a chance to
           play out. New floor gives the position room to breathe
           without widening so much that losses become large.

  Fix 3 — DARK_GREEN removed from entry conditions entirely.
           sig_bull_momentum allowed entering extended moves with
           a tight mean-reversion stop — the worst combination.
           Entry is now PINK-only (2+ shrinking neg bars) which
           keeps the strategy coherent: we enter on exhaustion,
           not on acceleration.
"""

import ccxt
import pandas as pd
import numpy as np
import time
import os
import json
import collections
from datetime import datetime, timezone

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, 'kraken_v6_state.json')
LOG_FILE   = os.path.join(BASE_DIR, 'kraken_v6_audit_trail.csv')
EVENT_FILE = os.path.join(BASE_DIR, 'kraken_v6_events.log')

PAPER_MODE        = True
STARTING_CAPITAL  = 100_000.0
MAX_POSITIONS     = 5
DRY_POWDER_PCT    = 0.40
HARD_STOP_PCT     = 0.035
MAX_DD_PCT        = 0.15
SLIPPAGE          = 0.0010
MIN_VOL_24H_USD   = 5_000_000
MIN_HISTORY_BARS  = 100
UNIVERSE_SIZE     = 30
TICK_INTERVAL     = 15      # fast loop: price + hard/trail stop
SCAN_INTERVAL     = 300     # slow loop: MACD exit + entries + heartbeat
UNIVERSE_REFRESH  = 3600
DUMP_PROFIT_PCT   = 0.030
COOLDOWN_PERIOD   = 7200    # 2h cooldown after any stop-out
BE_TRIGGER_PCT    = 0.008   # +0.8% → move stop to break-even


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
        self.positions         = {}
        self.cooldowns         = {}   # symbol → timestamp of stop-out (persisted)
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
        state = {
            'cash':              self.cash,
            'max_equity':        self.max_equity,
            'positions':         self.positions,
            'cooldowns':         self.cooldowns,
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
            log_event(">>> V6.1 FIRST START — fresh $100,000 virtual account.")
            return
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            gap = now_ts() - s.get('last_heartbeat_ts', now_ts())
            self.cash              = s['cash']
            self.max_equity        = s['max_equity']
            self.positions         = s.get('positions', {})
            self.cooldowns         = s.get('cooldowns', {})
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
        self.audit_rows.append(row)
        pd.DataFrame(self.audit_rows).to_csv(LOG_FILE, index=False)


# ─────────────────────────────────────────────────────────────
# UNIVERSE MANAGER
# ─────────────────────────────────────────────────────────────
class UniverseManager:
    def __init__(self, exchange, rl):
        self.exchange   = exchange
        self.rl         = rl
        self.universe   = []
        self.last_built = 0.0

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
                vol_usd = t.get('quoteVolume') or 0
                if vol_usd < MIN_VOL_24H_USD:
                    continue
                candidates.append((symbol, vol_usd))
            candidates.sort(key=lambda x: x[1], reverse=True)
            self.universe   = [s for s, _ in candidates[:UNIVERSE_SIZE]]
            self.last_built = now_ts()
            log_event(f"[Universe] {len(self.universe)} pairs: "
                      f"{', '.join(self.universe[:10])}{'...' if len(self.universe)>10 else ''}")
        except Exception as e:
            log_event(f"[Universe] Refresh error: {e}")
        return self.universe


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

            mid  = (book['bids'][0][0] + book['asks'][0][0]) / 2
            band = mid * 0.005
            bids = sum(lvl[1] for lvl in book['bids'] if lvl[0] >= mid - band)
            asks = sum(lvl[1] for lvl in book['asks'] if lvl[0] <= mid + band)
            ratio = bids / asks if asks > 0 else 999
            v1    = 'CONFIRM' if ratio > 0.67 else 'WARN'

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

            dband = mid * 0.01
            depth = (sum(lvl[1] for lvl in book['bids'] if lvl[0] >= mid - dband) +
                     sum(lvl[1] for lvl in book['asks'] if lvl[0] <= mid + dband))
            if symbol not in self.depth_cache:
                self.depth_cache[symbol] = collections.deque(maxlen=20)
            self.depth_cache[symbol].append(depth)
            avg  = sum(self.depth_cache[symbol]) / len(self.depth_cache[symbol])
            dpct = (depth / avg * 100) if avg > 0 else 100
            v3   = 'VETO' if dpct < 60 else ('WARN' if dpct < 80 else 'CONFIRM')

            recent  = trades[-50:]
            buy_vol = sum(t['amount'] for t in recent if t.get('side') == 'buy')
            sel_vol = sum(t['amount'] for t in recent if t.get('side') == 'sell')
            total   = buy_vol + sel_vol
            bpct    = (buy_vol / total * 100) if total > 0 else 50
            v4      = 'CONFIRM' if bpct >= 45 else 'WARN'

            verdicts = [v1, v2, v3, v4]
            if 'VETO' in verdicts:         final = 'VETO'
            elif verdicts.count('WARN') >= 2: final = 'WARN'
            else:                          final = 'CONFIRM'

            return final, {
                'mid': round(mid, 4), 'ratio': round(ratio, 3),
                'depth_pct': round(dpct, 1), 'buy_pct': round(bpct, 1),
                'checks': verdicts, 'verdict': final
            }
        except Exception as e:
            return 'CONFIRM', {'error': str(e)}


# ─────────────────────────────────────────────────────────────
# SCAN ENGINE
# ─────────────────────────────────────────────────────────────
class ScanEngine:
    def __init__(self, exchange, rl):
        self.exchange = exchange
        self.rl       = rl

    def fetch_ohlcv(self, symbol, tf='1h', limit=150):
        raw = self.rl.call(self.exchange.fetch_ohlcv, symbol, tf, None, limit)
        if raw is None or len(raw) < MIN_HISTORY_BARS:
            return None
        return pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])

    def _macd_state(self, df):
        ema12 = df['c'].ewm(span=12, adjust=False).mean()
        ema26 = df['c'].ewm(span=26, adjust=False).mean()
        hist  = (ema12 - ema26) - (ema12 - ema26).ewm(span=9, adjust=False).mean()
        h     = hist.values
        curr, prev, prev2 = h[-1], h[-2], h[-3]

        if abs(curr) < 1e-8:
            return 'FLAT', h[-4:]
        if curr < 0:
            if abs(curr) < abs(prev) and abs(prev) < abs(prev2):
                return 'PINK', h[-4:]
            elif abs(curr) < abs(prev):
                return 'PINK_1', h[-4:]
            else:
                return 'DARK_RED', h[-4:]
        else:
            if curr > prev and prev > prev2:  return 'DARK_GREEN', h[-4:]
            elif curr < prev and prev < prev2: return 'LIGHT_GREEN', h[-4:]
            elif curr < prev:                  return 'LIGHT_GREEN_1', h[-4:]
            else:                              return 'DARK_GREEN_1', h[-4:]

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
        Entry requires ALL of:
          - MACD state == PINK (2+ consecutive shrinking neg bars)
            PINK_1 and DARK_GREEN are not accepted (Fix 3)
          - RSI(14) < 52
          - Price location: BB lower, EMA21 pullback, SMA20 touch, or RSI oversold
          - Regime != BEAR
          - Conviction >= 40
        """
        df = self.fetch_ohlcv(symbol)
        if df is None:
            return 0, {}

        regime = self._regime(df)
        if regime == 'BEAR':
            return 0, {'regime': regime, 'reason': 'BEAR — no entry'}

        rsi             = self._rsi(df)
        macd_state, hist_vals = self._macd_state(df)

        # FIX 3: PINK only — PINK_1 and DARK_GREEN explicitly rejected
        if macd_state != 'PINK':
            return 0, {
                'regime': regime, 'rsi': round(rsi, 1),
                'macd_state': macd_state,
                'reason': f'MACD {macd_state} — PINK required'
            }

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

        score = 25   # PINK always = 25 (no partial credit in this version)

        if rsi < 30:   score += 25
        elif rsi < 38: score += 20
        elif rsi < 44: score += 15
        elif rsi < 48: score += 10
        else:          score += 5

        if sig_bb_lower:        score += 25
        elif sig_rsi_oversold:  score += 20
        elif sig_sma_touch:     score += 15
        elif sig_ema_pullback:  score += 10

        if regime == 'BULL':    score += 15
        elif regime == 'NEUTRAL': score += 7

        atr_pct   = self._atr_pct(df)
        vol_ratio = df['v'].iloc[-1] / (df['v'].rolling(20).mean().iloc[-1] + 1e-9)
        if vol_ratio > 1.5:   score += 10
        elif vol_ratio > 1.0: score += 5

        detail = {
            'regime':     regime,
            'rsi':        round(rsi, 1),
            'macd_state': macd_state,
            'macd_hist':  [round(float(x), 6) for x in hist_vals],
            'atr_pct':    round(atr_pct * 100, 3),
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

    def check_exit(self, symbol):
        """
        FIX 1: Only called during the slow (5-min) scan cycle.
        Reads 1h OHLCV — no point polling faster than the candle closes.
        Returns (should_exit, reason, is_light_green).
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
# TRAIL ENGINE
# ─────────────────────────────────────────────────────────────
class TrailEngine:
    """
    FIX 2: Trail base minimum raised from 0.5% → 0.8%.
           Absolute floor raised from 0.3% → 0.5%.
           These values are calibrated to 1h ATR reality on
           liquid crypto pairs (typical range 0.8–1.5%).

           Break-even promotion added: once position reaches
           BE_TRIGGER_PCT gain, stop floor is raised to entry
           price so a winner can never become a full loser.
    """

    def compute_trail(self, entry_price, peak_price, atr_pct,
                      macd_collapse=False):
        """
        Returns (trail_pct, stop_price, method_str).
        atr_pct: 1h ATR as decimal (e.g. 0.012 = 1.2%)
        """
        if peak_price <= 0 or entry_price <= 0:
            base = max(atr_pct, 0.010)
            return base, peak_price * (1 - base), 'atr_base'

        gain_pct = (peak_price - entry_price) / entry_price

        # FIX 2: base minimum is 0.8% (was 0.5%)
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

        # FIX 2: absolute floor 0.5% (was 0.3%)
        trail_pct  = max(base * mult, 0.005)
        stop_price = peak_price * (1 - trail_pct)

        # Break-even floor: once peak gain >= 0.8%, stop never below entry
        if gain_pct >= BE_TRIGGER_PCT:
            stop_price = max(stop_price, entry_price)
            if method == 'full':
                method = 'full_be'

        return trail_pct, stop_price, method


# ─────────────────────────────────────────────────────────────
# PORTFOLIO MANAGER
# ─────────────────────────────────────────────────────────────
class PortfolioManager:
    def __init__(self, state: StateManager):
        self.state = state

    def available_capital(self):
        total      = self.state.cash + sum(p['size_usd'] for p in self.state.positions.values())
        max_deploy = total * (1 - DRY_POWDER_PCT)
        deployed   = sum(p['size_usd'] for p in self.state.positions.values())
        return max(0.0, max_deploy - deployed)

    def position_size(self, conviction: int) -> float:
        if conviction >= 80:    pct = 0.20
        elif conviction >= 60:  pct = 0.12
        else:                   pct = 0.06
        total = self.state.cash + sum(p['size_usd'] for p in self.state.positions.values())
        return min(total * pct, self.available_capital())

    def can_open(self) -> bool:
        return (len(self.state.positions) < MAX_POSITIONS and
                self.available_capital() > 500)

    def open_position(self, symbol, conviction, entry_price, atr_pct, detail):
        size = self.position_size(conviction)
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
            'be_promoted':   False,
        }
        self.state.cash        -= size
        self.state.trade_count += 1
        log_event(
            f"!!! BUY {symbol} @ ${exec_price:,.4f} | "
            f"Size: ${size:,.2f} | Conviction: {conviction} | "
            f"ATR: {atr_pct*100:.2f}% | Regime: {detail.get('regime')}"
        )
        return True, "opened"

    def close_position(self, symbol, current_price, reason):
        if symbol not in self.state.positions:
            return None
        pos        = self.state.positions.pop(symbol)
        exec_price = current_price * (1 - SLIPPAGE)
        pnl_pct    = (exec_price - pos['entry_price']) / pos['entry_price']
        pnl_usd    = pos['size_usd'] * pnl_pct
        close_val  = pos['size_usd'] * (1 + pnl_pct)

        self.state.cash             += close_val
        self.state.total_pnl_closed += pnl_usd
        if pnl_usd > 0:
            self.state.total_trades_won += 1

        # Register cooldown — persisted to state.json, survives restarts
        self.state.cooldowns[symbol] = now_ts()

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
        self.exchange   = ccxt.kraken({'enableRateLimit': False})
        self.rl         = RateLimiter()
        self.state      = StateManager()
        self.universe   = UniverseManager(self.exchange, self.rl)
        self.scanner    = ScanEngine(self.exchange, self.rl)
        self.trail_eng  = TrailEngine()
        self.book       = OrderBookEngine(self.exchange, self.rl)
        self.portfolio  = PortfolioManager(self.state)
        self._prices    = {}
        self.last_scan  = 0.0   # timestamp of last slow-cycle run

    def boot(self):
        log_event(
            "--- KRAKEN V6.1: SINGLE-LOOP ASYNC | PINK-ONLY ENTRY | "
            "1h-CADENCED MACD EXIT | COOLDOWNS | $100K VIRTUAL | PAPER MODE ---"
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
        self.universe.refresh(force=True)
        log_event("[Boot] Boot sequence complete.")

    def _price(self, symbol) -> float:
        t = self.rl.call(self.exchange.fetch_ticker, symbol)
        if t:
            self._prices[symbol] = t['last']
            return t['last']
        return self._prices.get(symbol, 0.0)

    # ── FAST TICK: price + hard stop + trail stop ─────────────
    def update_positions_fast(self):
        """
        Runs every TICK_INTERVAL (15s).
        Only evaluates price-based stops — no OHLCV fetches.
        MACD/regime exit is deliberately excluded here (Fix 1).
        """
        for symbol in list(self.state.positions.keys()):
            pos   = self.state.positions.get(symbol)
            if pos is None:
                continue
            price = self._price(symbol)
            if price <= 0:
                continue

            pos['peak_price'] = max(pos['peak_price'], price)

            trail_pct, stop_price, method = self.trail_eng.compute_trail(
                pos['entry_price'],
                pos['peak_price'],
                pos['atr_pct'],
                pos.get('macd_collapse', False)
            )
            pos['trail_pct']   = trail_pct
            pos['stop_price']  = stop_price
            pos['trail_method'] = method

            # Track break-even promotion for heartbeat display
            gain_pct = (pos['peak_price'] - pos['entry_price']) / pos['entry_price']
            if gain_pct >= BE_TRIGGER_PCT:
                pos['be_promoted'] = True

            hard_stop = pos['entry_price'] * (1 - HARD_STOP_PCT)

            if price <= hard_stop:
                result = self.portfolio.close_position(symbol, price, 'SELL_HARD_STOP')
                if result:
                    self._audit_exit(symbol, price, 'SELL_HARD_STOP', *result)
            elif price <= stop_price:
                result = self.portfolio.close_position(symbol, price, f'SELL_TRAIL({method})')
                if result:
                    self._audit_exit(symbol, price, f'SELL_TRAIL({method})', *result)

    # ── SLOW TICK: MACD exits + entry scan + heartbeat ────────
    def update_positions_slow(self):
        """
        FIX 1: MACD/regime exit check runs here — every 5 minutes,
        not every 15 seconds. 1h OHLCV fetching at 15s cadence with
        5 open positions would burn ~20 API calls/min on stale data.
        """
        for symbol in list(self.state.positions.keys()):
            pos = self.state.positions.get(symbol)
            if pos is None:
                continue
            price = self._price(symbol)
            if price <= 0:
                continue

            should_exit, reason, macd_collapse = self.scanner.check_exit(symbol)
            if macd_collapse:
                pos['macd_collapse'] = True
                # Recompute trail immediately with collapse flag
                trail_pct, stop_price, method = self.trail_eng.compute_trail(
                    pos['entry_price'], pos['peak_price'],
                    pos['atr_pct'], macd_collapse=True
                )
                pos['trail_pct']    = trail_pct
                pos['stop_price']   = stop_price
                pos['trail_method'] = method

            if should_exit:
                result = self.portfolio.close_position(symbol, price, reason)
                if result:
                    self._audit_exit(symbol, price, reason, *result)

    def scan_entries(self):
        if not self.portfolio.can_open():
            return
        symbols = self.universe.refresh()
        scored  = []

        for symbol in symbols:
            if symbol in self.state.positions:
                continue
            # Cooldown gate — persisted, survives restarts
            last_stop = self.state.cooldowns.get(symbol, 0)
            if now_ts() - last_stop < COOLDOWN_PERIOD:
                remaining = COOLDOWN_PERIOD - (now_ts() - last_stop)
                log_event(f"[Cooldown] {symbol} blocked — {fmt_dur(remaining)} remaining")
                continue
            try:
                conviction, detail = self.scanner.score(symbol)
                if conviction >= 40:
                    scored.append((conviction, symbol, detail))
            except Exception as e:
                log_event(f"[Scan] {symbol} error: {e}")

        scored.sort(key=lambda x: x[0], reverse=True)

        for conviction, symbol, detail in scored:
            if not self.portfolio.can_open():
                break
            book_verdict, book_snap = self.book.evaluate(symbol)
            if book_verdict == 'VETO':
                log_event(f"[BookGate] VETO on {symbol} — skipping")
                continue
            price = self._price(symbol)
            if price <= 0:
                continue
            atr_pct = detail.get('atr_pct', 2.0) / 100.0
            ok, msg = self.portfolio.open_position(symbol, conviction, price, atr_pct, detail)
            if ok:
                self._audit_entry(symbol, price, conviction, detail, book_snap)

    def _check_kill_switch(self):
        eq = self.state.equity(self._prices)
        self.state.max_equity = max(self.state.max_equity, eq)
        dd = (self.state.max_equity - eq) / self.state.max_equity
        if dd >= MAX_DD_PCT:
            log_event(f"CRITICAL: {MAX_DD_PCT*100:.0f}% DRAWDOWN KILL SWITCH — closing all.")
            for symbol in list(self.state.positions.keys()):
                price  = self._price(symbol)
                result = self.portfolio.close_position(symbol, price, 'KILL_SWITCH')
                if result:
                    self._audit_exit(symbol, price, 'KILL_SWITCH', *result)
            self.state.save(self._prices)
            raise SystemExit(0)

    def print_heartbeat(self):
        eq       = self.state.equity(self._prices)
        self.state.max_equity = max(self.state.max_equity, eq)
        dd       = (self.state.max_equity - eq) / self.state.max_equity
        profit   = eq - STARTING_CAPITAL
        pp       = profit / STARTING_CAPITAL * 100
        win_rate = (self.state.total_trades_won / self.state.trade_count * 100
                    if self.state.trade_count > 0 else 0.0)

        print("\n" + "=" * 72)
        print(f" KRAKEN V6.1 HEARTBEAT | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("─" * 72)
        print(f" Portfolio Equity:   ${eq:>12,.2f}   (start: ${STARTING_CAPITAL:,.2f})")
        print(f" Total P&L:          ${profit:>+12,.2f}   ({pp:+.3f}%)")
        print(f" Closed P&L:         ${self.state.total_pnl_closed:>+12,.2f}")
        print(f" Cash Available:     ${self.state.cash:>12,.2f}")
        print(f" Max Drawdown:       {dd*100:>11.2f}%   (limit: {MAX_DD_PCT*100:.0f}%)")
        print(f" Trades: {self.state.trade_count}  |  Won: {self.state.total_trades_won}  "
              f"|  Win Rate: {win_rate:.1f}%")
        print("─" * 72)

        if self.state.positions:
            print(f" {'SYMBOL':<12} {'SIZE':>10} {'ENTRY':>10} {'CURR':>10} "
                  f"{'P&L':>10} {'TRAIL%':>8} {'BE':>4} {'METHOD':<16}")
            for sym, pos in self.state.positions.items():
                cur   = self._prices.get(sym, pos['entry_price'])
                g_pct = (cur - pos['entry_price']) / pos['entry_price'] * 100
                g_usd = pos['size_usd'] * g_pct / 100
                be    = '✓' if pos.get('be_promoted') else ' '
                print(f" {sym:<12} ${pos['size_usd']:>9,.0f} "
                      f"${pos['entry_price']:>9,.4f} "
                      f"${cur:>9,.4f} "
                      f"${g_usd:>+9,.2f} "
                      f"{pos['trail_pct']*100:>7.2f}% "
                      f"{be:>4} "
                      f"{pos['trail_method']:<16}")
        else:
            print(" Positions:          FLAT — scanning for entries")

        # Active cooldowns
        active_cds = {s: t for s, t in self.state.cooldowns.items()
                      if now_ts() - t < COOLDOWN_PERIOD}
        if active_cds:
            print("─" * 72)
            print(f" Cooling down ({len(active_cds)}): " +
                  ", ".join(
                      f"{s} ({fmt_dur(COOLDOWN_PERIOD - (now_ts() - t))} left)"
                      for s, t in active_cds.items()
                  ))

        print("─" * 72)
        print(f" Universe:           {len(self.universe.universe)} pairs")
        print(f" Dry Powder:         {DRY_POWDER_PCT*100:.0f}% reserve | "
              f"Deployable: ${self.portfolio.available_capital():,.2f}")
        print("─" * 72)
        print(f" Gross Runtime:      {fmt_dur(self.state.gross_runtime())}")
        print(f" Total Paused:       {fmt_dur(self.state.total_paused_secs)}")
        print(f" Net Runtime:        {fmt_dur(self.state.net_runtime())}")
        print("=" * 72 + "\n")

    # ── AUDIT ─────────────────────────────────────────────────
    def _audit_entry(self, symbol, price, conviction, detail, book_snap):
        eq = self.state.equity(self._prices)
        self.state.max_equity = max(self.state.max_equity, eq)
        dd = (self.state.max_equity - eq) / self.state.max_equity
        self.state.append_audit({
            'timestamp':      datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'action':         'BUY',
            'symbol':         symbol,
            'price':          round(price, 6),
            'conviction':     conviction,
            'regime':         detail.get('regime'),
            'macd_state':     detail.get('macd_state'),
            'rsi':            detail.get('rsi'),
            'atr_pct':        detail.get('atr_pct'),
            'vol_ratio':      detail.get('vol_ratio'),
            'book_verdict':   book_snap.get('verdict', 'n/a'),
            'equity':         round(eq, 2),
            'drawdown':       round(dd, 4),
            'cash':           round(self.state.cash, 2),
            'open_positions': len(self.state.positions),
            'gross_runtime':  fmt_dur(self.state.gross_runtime()),
            'net_runtime':    fmt_dur(self.state.net_runtime()),
        })

    def _audit_exit(self, symbol, price, reason, pnl_usd, pnl_pct):
        eq = self.state.equity(self._prices)
        self.state.max_equity = max(self.state.max_equity, eq)
        dd = (self.state.max_equity - eq) / self.state.max_equity
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
            'gross_runtime':  fmt_dur(self.state.gross_runtime()),
            'net_runtime':    fmt_dur(self.state.net_runtime()),
        })

    # ── MAIN LOOP ─────────────────────────────────────────────
    def main(self):
        self.boot()

        while True:
            try:
                # ── FAST TICK (every 15s) ─────────────────────
                # Price fetch + hard stop + trail stop only.
                # No OHLCV, no entry scan.
                self.update_positions_fast()
                self._check_kill_switch()

                # ── SLOW TICK (every 5 min) ───────────────────
                # MACD/regime exits + entry scan + heartbeat.
                if now_ts() - self.last_scan >= SCAN_INTERVAL:
                    self.update_positions_slow()
                    self.scan_entries()
                    self.print_heartbeat()
                    self.state.save(self._prices)
                    self.last_scan = now_ts()

                time.sleep(TICK_INTERVAL)

            except SystemExit:
                raise
            except Exception as e:
                log_event(f"[Main] Error: {e}")
                self.state.save(self._prices)
                time.sleep(60)


if __name__ == '__main__':
    Conductor().main()
