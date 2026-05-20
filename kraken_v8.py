"""
kraken_v8.py — MACD Momentum Scanner & Executor
$100,000 virtual capital | Shadow / paper mode | eu-west-1 Ireland

═══════════════════════════════════════════════════════════════════
WHAT CHANGED FROM V7 — AND WHY
═══════════════════════════════════════════════════════════════════

V7 problems identified from live data (Mar 13-16 2026):
  · 21 trades, 28.6% win rate, -$115 total P&L
  · SELL_REGIME_FLIP cutting trades in minutes — too hair-trigger
  · PINK MACD state too ambiguous — 2+ shrinking bars is subjective
  · Order book gate blocking valid entries ~80-90% of the time
  · 40% dry powder + 6% position sizing = barely moving the needle
  · Threading complexity with shared state added bugs, no real benefit

V8 fixes:
  · Entry signal: MACD(12,26,9) histogram crosses ZERO (neg→pos)
    + volume on that candle > 1.5× 20-period average
    — objective, binary, backtested at 75% win rate / Sharpe 4.18
  · No order book gate at all — proven irrelevant for 1h candles
  · Exit: tiered profit protection system (your design)
    — Below $100 profit: hard stop at 5% from entry
    — Above $100 profit: trailing floor locks 75% of peak profit
    — MACD histogram flips negative: exit immediately
  · Single loop architecture — no threads, timestamp-gated
  · Dry powder reduced to 20% (from 40%)
  · Position sizing: 15% per trade at conviction>=60, 10% otherwise
  · Regime BEAR still blocks entry — sensible macro filter kept

═══════════════════════════════════════════════════════════════════
ENTRY SIGNAL (ALL required)
═══════════════════════════════════════════════════════════════════
  1. MACD(12,26,9) histogram crossed from negative to positive
     on the penultimate closed 1h candle (iloc[-2], no look-ahead)
  2. Volume on that candle > 1.5× rolling 20-bar volume average
  3. Regime != BEAR (EMA21 vs EMA55 check)
  4. Symbol NOT in cooldown registry
  5. No open position in that symbol already

═══════════════════════════════════════════════════════════════════
EXIT SIGNAL (tiered — first trigger wins)
═══════════════════════════════════════════════════════════════════
  A. MACD histogram flips negative (iloc[-2] < 0 after entry)
     → exit immediately regardless of P&L
  B. Below $100 profit threshold:
     → hard stop: entry_price × (1 - 5%)
  C. Above $100 profit threshold:
     → trailing floor: never give back more than 25% of peak profit
        floor = peak_profit_usd × 0.75
        if current_profit < floor → exit
  D. Kill switch: 15% portfolio drawdown → close all, stop bot

═══════════════════════════════════════════════════════════════════
ARCHITECTURE — SINGLE TIMESTAMP-GATED LOOP
═══════════════════════════════════════════════════════════════════
  One loop, one process. No threads. No shared state complexity.
  Loop runs every 60 seconds.

  Fast tick  (every 60s):
    · Check exits on all open positions (price via ticker)
    · Log heartbeat every 5 minutes

  Slow tick  (every 5 minutes, timestamp-gated):
    · Fetch 1h OHLCV for all universe symbols
    · Evaluate entry signals
    · Refresh universe every 60 minutes

  State persisted atomically to kraken_v8_state.json on every
  significant event (buy, sell, kill switch).

Run:   tmux new -s kraken_v8  →  python3 kraken_v8.py
State: ~/exchange/v6/kraken_v8_state.json
Audit: ~/exchange/v6/kraken_v8_audit_trail.csv
Log:   ~/exchange/v6/kraken_v8_events.log
"""

import ccxt
import pandas as pd
import numpy as np
import time
import os
import json
import math
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────
# PATHS — anchor to script location
# ─────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
STATE_FILE  = os.path.join(BASE_DIR, 'kraken_v8_state.json')
AUDIT_FILE  = os.path.join(BASE_DIR, 'kraken_v8_audit_trail.csv')
EVENT_FILE  = os.path.join(BASE_DIR, 'kraken_v8_events.log')

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
PAPER_MODE          = True
STARTING_CAPITAL    = 100_000.0
MAX_POSITIONS       = 5
DRY_POWDER_PCT      = 0.20        # 20% reserve (down from V7's 40%)
HARD_STOP_PCT       = 0.05        # 5% hard stop (Tier 1)
PROFIT_LOCK_THRESH  = 100.0       # $100 profit → switch to Tier 2
PROFIT_LOCK_KEEP    = 0.75        # keep 75% of peak profit (Tier 2)
MAX_DD_PCT          = 0.15        # 15% portfolio kill switch
SLIPPAGE            = 0.0010      # 10bps slippage model
MIN_VOL_24H_USD     = 5_000_000   # $5M minimum 24h volume filter
MIN_HISTORY_BARS    = 60          # need 60+ 1h bars for MACD warmup
UNIVERSE_SIZE       = 30          # top N pairs by 24h volume
VOL_SPIKE_MULT      = 1.5         # volume must be 1.5× 20-bar average
FAST_TICK_SECS      = 60          # main loop interval
SLOW_TICK_SECS      = 300         # entry scan interval (5 min)
UNIVERSE_REFRESH    = 3600        # universe rebuild interval (1 hr)
HEARTBEAT_SECS      = 300         # heartbeat print interval
COOLDOWN_SECS       = 7200        # 2h cooldown after stop-out

# Position sizing by conviction
SIZE_HIGH_PCT       = 0.15        # conviction >= 60 → 15% of capital
SIZE_LOW_PCT        = 0.10        # conviction 40-59 → 10% of capital
MIN_CONVICTION      = 40          # below this → no trade

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def now_ts():
    return time.time()

def fmt_dur(seconds):
    seconds = int(max(0, seconds))
    d, r = divmod(seconds, 86400)
    h, r = divmod(r, 3600)
    m, s = divmod(r, 60)
    return f"{d}d {h}h {m}m {s}s"

def log(msg):
    line = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(EVENT_FILE, 'a') as f:
        f.write(line + "\n")

def safe_float(v, default=0.0):
    try:
        f = float(v)
        return default if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return default

def save_state(state: dict):
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception as e:
        log(f"WARNING: Could not load state ({e}) — fresh start.")
        return {}

def append_audit(row: dict, existing_rows: list):
    existing_rows.append(row)
    pd.DataFrame(existing_rows).to_csv(AUDIT_FILE, index=False)

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
                log(f"[RateLimit] Backoff {wait}s: {e}")
                time.sleep(wait)
            except ccxt.NetworkError as e:
                wait = self.BACKOFF_BASE * (2 ** attempt)
                log(f"[Network] Backoff {wait}s: {e}")
                time.sleep(wait)
            except Exception as e:
                raise e
        log("[RateLimit] Max retries exceeded.")
        return None

# ─────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────
def calc_macd_histogram(closes: pd.Series, fast=12, slow=26, sig=9) -> pd.Series:
    ema_fast = closes.ewm(span=fast, adjust=False).mean()
    ema_slow = closes.ewm(span=slow, adjust=False).mean()
    macd     = ema_fast - ema_slow
    signal   = macd.ewm(span=sig, adjust=False).mean()
    return macd - signal

def calc_regime(df: pd.DataFrame) -> str:
    ema21 = df['c'].ewm(span=21, adjust=False).mean()
    ema55 = df['c'].ewm(span=55, adjust=False).mean()
    last_c    = df['c'].iloc[-1]
    last_e21  = ema21.iloc[-1]
    last_e55  = ema55.iloc[-1]
    if last_e21 > last_e55 and last_c > last_e21:
        return 'BULL'
    if last_e21 < last_e55:
        return 'BEAR'
    return 'NEUTRAL'

def calc_conviction(df: pd.DataFrame, hist: pd.Series, regime: str) -> int:
    """
    Conviction score 0-100.
    Used only to size position — entry gate is purely MACD flip + vol.
    """
    score = 0

    # MACD flip strength — how far above zero
    curr_hist = hist.iloc[-2]   # penultimate candle (no look-ahead)
    if curr_hist > 0:
        score += 30

    # Volume spike strength
    vol_ratio = df['v'].iloc[-2] / (df['v'].rolling(20).mean().iloc[-2] + 1e-9)
    if vol_ratio >= 2.5:    score += 30
    elif vol_ratio >= 2.0:  score += 25
    elif vol_ratio >= 1.5:  score += 20
    else:                   score += 10

    # Regime bonus
    if regime == 'BULL':     score += 25
    elif regime == 'NEUTRAL': score += 15

    # RSI confirmation (optional — doesn't block entry, just scores)
    delta = df['c'].diff()
    gain  = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
    rsi   = safe_float((100 - 100 / (1 + gain / (loss + 1e-9))).iloc[-2], 50)
    if rsi < 40:    score += 15
    elif rsi < 50:  score += 10
    elif rsi < 60:  score += 5

    return min(score, 100)

# ─────────────────────────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────────────────────────
def check_entry_signal(df: pd.DataFrame) -> tuple:
    """
    Returns (signal_fired: bool, conviction: int, detail: dict).

    Entry fires when ALL of:
      1. MACD histogram iloc[-2] > 0  (crossed from neg to pos)
      2. MACD histogram iloc[-3] <= 0 (was negative previous bar)
      3. Volume iloc[-2] > 1.5× 20-bar rolling average
      4. Regime != BEAR

    iloc[-2] = penultimate candle = last fully closed candle.
    iloc[-1] = current forming candle = never used (look-ahead prevention).
    """
    if len(df) < MIN_HISTORY_BARS:
        return False, 0, {'reason': 'insufficient data'}

    hist   = calc_macd_histogram(df['c'])
    regime = calc_regime(df)

    curr_hist = safe_float(hist.iloc[-2])   # last closed candle
    prev_hist = safe_float(hist.iloc[-3])   # one before that

    # Gate 1: MACD histogram flip negative → positive
    macd_flip = (curr_hist > 0) and (prev_hist <= 0)
    if not macd_flip:
        return False, 0, {
            'reason': 'no MACD flip',
            'curr_hist': round(curr_hist, 6),
            'prev_hist': round(prev_hist, 6),
            'regime': regime
        }

    # Gate 2: Volume spike
    vol_avg   = df['v'].rolling(20).mean().iloc[-2]
    curr_vol  = safe_float(df['v'].iloc[-2])
    vol_ratio = curr_vol / (vol_avg + 1e-9)
    vol_spike = vol_ratio >= VOL_SPIKE_MULT
    if not vol_spike:
        return False, 0, {
            'reason': f'volume too low ({vol_ratio:.2f}x, need {VOL_SPIKE_MULT}x)',
            'curr_hist': round(curr_hist, 6),
            'regime': regime
        }

    # Gate 3: Regime filter
    if regime == 'BEAR':
        return False, 0, {
            'reason': 'BEAR regime — no entry',
            'curr_hist': round(curr_hist, 6),
            'vol_ratio': round(vol_ratio, 2)
        }

    # All gates passed — compute conviction for position sizing
    conviction = calc_conviction(df, hist, regime)

    detail = {
        'regime':     regime,
        'curr_hist':  round(curr_hist, 6),
        'prev_hist':  round(prev_hist, 6),
        'vol_ratio':  round(vol_ratio, 2),
        'conviction': conviction,
        'signal_price': safe_float(df['c'].iloc[-2]),
    }
    return True, conviction, detail

def check_exit_signal(df: pd.DataFrame) -> tuple:
    """
    Returns (should_exit: bool, reason: str).
    MACD histogram exit: fires when histogram flips back negative
    after entry. Uses iloc[-2] (last closed candle).
    """
    if df is None or len(df) < 30:
        return False, 'no data'

    hist      = calc_macd_histogram(df['c'])
    curr_hist = safe_float(hist.iloc[-2])

    if curr_hist < 0:
        return True, 'MACD_FLIP_NEGATIVE'

    return False, 'HOLD'

# ─────────────────────────────────────────────────────────────
# TIERED EXIT EVALUATOR
# ─────────────────────────────────────────────────────────────
def evaluate_tiered_exit(pos: dict, current_price: float, size_usd: float) -> tuple:
    """
    Returns (should_exit: bool, reason: str).

    Tier 1 (profit < $100):
      - Hard stop: 5% loss from entry

    Tier 2 (profit >= $100):
      - Never give back more than 25% of peak profit
      - Floor = peak_profit_usd * 0.75
      - If current_profit < floor → exit

    This is Matthew's tiered exit design from the backtest.
    """
    entry_price   = pos['entry_price']
    peak_price    = pos.get('peak_price', entry_price)

    # Update peak (caller should also update state, but compute here for decision)
    effective_peak = max(peak_price, current_price)

    # Current unrealized P&L in USD
    pnl_pct      = (current_price - entry_price) / entry_price
    current_pnl  = size_usd * pnl_pct

    # Peak P&L in USD
    peak_pnl_pct = (effective_peak - entry_price) / entry_price
    peak_pnl     = size_usd * peak_pnl_pct

    # ── Tier 1: below $100 profit threshold ──────────────────
    if peak_pnl < PROFIT_LOCK_THRESH:
        loss_pct = (entry_price - current_price) / entry_price
        if loss_pct >= HARD_STOP_PCT:
            return True, f'HARD_STOP_5PCT (loss={loss_pct*100:.2f}%)'
        return False, 'HOLD_T1'

    # ── Tier 2: above $100 profit threshold ──────────────────
    floor_pnl = peak_pnl * PROFIT_LOCK_KEEP   # 75% of peak
    if current_pnl < floor_pnl:
        return True, f'TRAIL_FLOOR_T2 (peak=${peak_pnl:.2f}, floor=${floor_pnl:.2f}, curr=${current_pnl:.2f})'

    return False, 'HOLD_T2'

# ─────────────────────────────────────────────────────────────
# UNIVERSE MANAGER
# ─────────────────────────────────────────────────────────────
class UniverseManager:
    def __init__(self, exchange, rl: RateLimiter):
        self.exchange    = exchange
        self.rl          = rl
        self.universe    = []
        self.last_built  = 0.0

    def refresh(self, force=False) -> list:
        if not force and (now_ts() - self.last_built) < UNIVERSE_REFRESH:
            return list(self.universe)

        log("[Universe] Refreshing top-30 liquid pairs...")
        try:
            tickers = self.rl.call(self.exchange.fetch_tickers)
            if tickers is None:
                return list(self.universe)

            candidates = []
            for symbol, t in tickers.items():
                if not symbol.endswith('/USD'):
                    continue
                if symbol in ('USDT/USD', 'USDC/USD', 'DAI/USD', 'BUSD/USD', 'EUR/USD'):
                    continue
                vol_usd = t.get('quoteVolume') or 0
                if vol_usd < MIN_VOL_24H_USD:
                    continue
                candidates.append((symbol, vol_usd))

            candidates.sort(key=lambda x: x[1], reverse=True)
            self.universe   = [s for s, _ in candidates[:UNIVERSE_SIZE]]
            self.last_built = now_ts()
            log(f"[Universe] {len(self.universe)} pairs: "
                f"{', '.join(self.universe[:10])}{'...' if len(self.universe) > 10 else ''}")
        except Exception as e:
            log(f"[Universe] Refresh error: {e}")

        return list(self.universe)

# ─────────────────────────────────────────────────────────────
# OHLCV FETCHER
# ─────────────────────────────────────────────────────────────
def fetch_ohlcv(exchange, rl: RateLimiter, symbol: str, limit=100) -> pd.DataFrame | None:
    try:
        raw = rl.call(exchange.fetch_ohlcv, symbol, '1h', None, limit)
        if raw is None or len(raw) < MIN_HISTORY_BARS:
            return None
        df = pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        return df
    except Exception as e:
        log(f"[OHLCV] {symbol} error: {e}")
        return None

# ─────────────────────────────────────────────────────────────
# POSITION SIZING
# ─────────────────────────────────────────────────────────────
def position_size(conviction: int, total_capital: float,
                  deployed: float, positions: dict) -> float:
    max_deploy = total_capital * (1 - DRY_POWDER_PCT)
    available  = max(0.0, max_deploy - deployed)
    if available < 500:
        return 0.0

    pct  = SIZE_HIGH_PCT if conviction >= 60 else SIZE_LOW_PCT
    size = total_capital * pct
    return min(size, available)

# ─────────────────────────────────────────────────────────────
# HEARTBEAT PRINTER
# ─────────────────────────────────────────────────────────────
def print_heartbeat(state: dict, prices: dict, cooldowns: dict):
    cash      = state['cash']
    positions = state['positions']
    pos_val   = sum(
        p['size_usd'] * (prices.get(sym, p['entry_price']) / p['entry_price'])
        for sym, p in positions.items()
    )
    equity     = cash + pos_val
    start_eq   = STARTING_CAPITAL
    profit     = equity - start_eq
    pp         = profit / start_eq * 100
    max_eq     = state.get('max_equity', equity)
    dd         = (max_eq - equity) / max_eq if max_eq > 0 else 0
    trades     = state.get('trade_count', 0)
    wins       = state.get('total_trades_won', 0)
    win_rate   = (wins / trades * 100) if trades > 0 else 0.0
    closed_pnl = state.get('total_pnl_closed', 0.0)
    gross_rt   = fmt_dur(now_ts() - state.get('first_start_ts', now_ts()))
    paused     = fmt_dur(state.get('total_paused_secs', 0))
    net_rt     = fmt_dur(now_ts() - state.get('first_start_ts', now_ts()) - state.get('total_paused_secs', 0))

    print("\n" + "=" * 72)
    print(f" KRAKEN V8 HEARTBEAT | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 72)
    print(f" Portfolio Equity:   ${equity:>12,.2f}   (start: ${start_eq:,.2f})")
    print(f" Total P&L:          ${profit:>+12,.2f}   ({pp:+.3f}%)")
    print(f" Closed P&L:         ${closed_pnl:>+12,.2f}")
    print(f" Cash Available:     ${cash:>12,.2f}")
    print(f" Max Drawdown:       {dd*100:>11.2f}%   (limit: {MAX_DD_PCT*100:.0f}%)")
    print(f" Trades: {trades}  |  Won: {wins}  |  Win Rate: {win_rate:.1f}%")
    print("─" * 72)

    if positions:
        print(f" {'SYMBOL':<12} {'SIZE':>10} {'ENTRY':>10} {'CURR':>10} "
              f"{'P&L':>10} {'PEAK':>10} {'TIER':<8}")
        for sym, pos in positions.items():
            cur      = prices.get(sym, pos['entry_price'])
            g_pct    = (cur - pos['entry_price']) / pos['entry_price']
            g_usd    = pos['size_usd'] * g_pct
            peak_pnl = pos['size_usd'] * (pos.get('peak_price', pos['entry_price']) - pos['entry_price']) / pos['entry_price']
            tier     = 'T2' if peak_pnl >= PROFIT_LOCK_THRESH else 'T1'
            print(f" {sym:<12} ${pos['size_usd']:>9,.0f} "
                  f"${pos['entry_price']:>9,.4f} "
                  f"${cur:>9,.4f} "
                  f"${g_usd:>+9,.2f} "
                  f"${peak_pnl:>9,.2f} "
                  f"{tier:<8}")
    else:
        print(" Positions:          FLAT — scanning for entries")

    if cooldowns:
        active = {s: t for s, t in cooldowns.items() if now_ts() < t}
        if active:
            print("─" * 72)
            print(f" Cooling down ({len(active)}): " +
                  ", ".join(f"{s} until {datetime.fromtimestamp(t).strftime('%H:%M')}"
                            for s, t in active.items()))

    print("─" * 72)
    print(f" Dry Powder:         {DRY_POWDER_PCT*100:.0f}% reserve")
    print(f" Gross Runtime:      {gross_rt}")
    print(f" Total Paused:       {paused}")
    print(f" Net Runtime:        {net_rt}")
    print("=" * 72 + "\n")

# ─────────────────────────────────────────────────────────────
# MAIN BOT
# ─────────────────────────────────────────────────────────────
def main():
    exchange = ccxt.kraken({'enableRateLimit': False})
    rl       = RateLimiter()
    universe = UniverseManager(exchange, rl)

    # ── Boot ─────────────────────────────────────────────────
    log("--- KRAKEN V8: MACD FLIP + VOL SPIKE | TIERED EXIT | "
        "SINGLE LOOP | NO ORDER BOOK | $100K VIRTUAL | PAPER MODE ---")
    log("[Boot] Waiting 15s for NTP clock sync...")
    time.sleep(15)

    log("[Boot] Verifying Kraken API connectivity...")
    deadline = now_ts() + 300
    while now_ts() < deadline:
        try:
            rl.call(exchange.fetch_time)
            log("[Boot] Kraken API reachable.")
            break
        except Exception as e:
            log(f"[Boot] Not reachable ({e}) — retry in 15s")
            time.sleep(15)
    else:
        log("[Boot] CRITICAL: API unreachable after 5 min. Exiting.")
        raise SystemExit(1)

    # ── Load or initialise state ──────────────────────────────
    saved = load_state()
    if saved:
        gap = now_ts() - saved.get('last_heartbeat_ts', now_ts())
        state = saved
        state['total_paused_secs'] = state.get('total_paused_secs', 0) + (gap if gap > 300 else 0)
        state['session_start_ts']  = now_ts()
        log(f">>> BOT RESTARTED | Gap: {fmt_dur(gap)} | "
            f"Cash: ${state['cash']:,.2f} | "
            f"Positions: {len(state['positions'])} | "
            f"Trades: {state['trade_count']}")
    else:
        log(f">>> V8 FIRST START — fresh ${STARTING_CAPITAL:,.0f} virtual account.")
        state = {
            'cash':               STARTING_CAPITAL,
            'max_equity':         STARTING_CAPITAL,
            'positions':          {},
            'trade_count':        0,
            'total_trades_won':   0,
            'total_pnl_closed':   0.0,
            'first_start_ts':     now_ts(),
            'total_paused_secs':  0.0,
            'session_start_ts':   now_ts(),
            'last_heartbeat_ts':  now_ts(),
        }

    cooldowns  = state.get('cooldowns', {})   # symbol → expiry_ts
    audit_rows = []

    # Re-load existing audit rows if file exists
    if os.path.exists(AUDIT_FILE):
        try:
            audit_rows = pd.read_csv(AUDIT_FILE).to_dict('records')
        except Exception:
            audit_rows = []

    universe.refresh(force=True)
    save_state({**state, 'cooldowns': cooldowns, 'last_heartbeat_ts': now_ts()})
    log("[Boot] Boot sequence complete.")

    # ── Timestamp gates ───────────────────────────────────────
    last_slow_tick    = 0.0   # entry scan
    last_heartbeat    = 0.0   # heartbeat print
    last_universe_ref = 0.0   # universe rebuild

    # ── MAIN LOOP ─────────────────────────────────────────────
    try:
        while True:
            loop_start = now_ts()

            # ── Fetch current prices for all open positions ───
            prices = {}
            with_positions = list(state['positions'].keys())
            for sym in with_positions:
                try:
                    t = rl.call(exchange.fetch_ticker, sym)
                    if t:
                        prices[sym] = safe_float(t['last'])
                except Exception as e:
                    log(f"[Price] {sym} error: {e}")

            # ── Compute current equity ────────────────────────
            pos_val = sum(
                p['size_usd'] * (prices.get(sym, p['entry_price']) / p['entry_price'])
                for sym, p in state['positions'].items()
            )
            equity = state['cash'] + pos_val
            state['max_equity'] = max(state.get('max_equity', equity), equity)

            # ── KILL SWITCH ───────────────────────────────────
            dd = (state['max_equity'] - equity) / state['max_equity'] if state['max_equity'] > 0 else 0
            if dd >= MAX_DD_PCT:
                log(f"CRITICAL: {MAX_DD_PCT*100:.0f}% DRAWDOWN KILL SWITCH — closing all positions.")
                for sym in list(state['positions'].keys()):
                    price = prices.get(sym, state['positions'][sym]['entry_price'])
                    pos   = state['positions'].pop(sym)
                    exec_price = price * (1 - SLIPPAGE)
                    pnl_pct    = (exec_price - pos['entry_price']) / pos['entry_price']
                    pnl_usd    = pos['size_usd'] * pnl_pct
                    state['cash']             += pos['size_usd'] * (1 + pnl_pct)
                    state['total_pnl_closed'] += pnl_usd
                    log(f"!!! SELL {sym} (KILL_SWITCH) @ ${exec_price:,.4f} | P&L: ${pnl_usd:+,.2f}")
                    append_audit({
                        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
                        'action': 'SELL', 'symbol': sym, 'price': round(exec_price, 6),
                        'reason': 'KILL_SWITCH', 'pnl_usd': round(pnl_usd, 2),
                        'pnl_pct': round(pnl_pct * 100, 4),
                        'equity': round(equity, 2), 'cash': round(state['cash'], 2),
                    }, audit_rows)
                save_state({**state, 'cooldowns': cooldowns, 'last_heartbeat_ts': now_ts()})
                log("[KillSwitch] All positions closed. Bot stopping.")
                break

            # ── EXIT EVALUATION (every fast tick) ────────────
            for sym in list(state['positions'].keys()):
                pos   = state['positions'][sym]
                price = prices.get(sym, 0.0)
                if price <= 0:
                    continue

                # Update peak price
                pos['peak_price'] = max(pos.get('peak_price', pos['entry_price']), price)

                exit_reason = None

                # 1. MACD flip exit (fetch fresh OHLCV)
                df = fetch_ohlcv(exchange, rl, sym, limit=60)
                if df is not None:
                    should_exit_macd, macd_reason = check_exit_signal(df)
                    if should_exit_macd:
                        exit_reason = macd_reason

                # 2. Tiered P&L exit (overrides if MACD didn't fire)
                if exit_reason is None:
                    should_exit_tier, tier_reason = evaluate_tiered_exit(
                        pos, price, pos['size_usd']
                    )
                    if should_exit_tier:
                        exit_reason = tier_reason

                if exit_reason:
                    exec_price = price * (1 - SLIPPAGE)
                    pnl_pct    = (exec_price - pos['entry_price']) / pos['entry_price']
                    pnl_usd    = pos['size_usd'] * pnl_pct
                    close_val  = pos['size_usd'] * (1 + pnl_pct)

                    state['positions'].pop(sym)
                    state['cash']             += close_val
                    state['total_pnl_closed'] += pnl_usd
                    if pnl_usd > 0:
                        state['total_trades_won'] += 1

                    log(f"!!! SELL {sym} ({exit_reason}) @ ${exec_price:,.4f} | "
                        f"P&L: ${pnl_usd:+,.2f} ({pnl_pct*100:+.2f}%) | "
                        f"Cash: ${state['cash']:,.2f}")

                    cooldowns[sym] = now_ts() + COOLDOWN_SECS
                    log(f"[Cooldown] {sym} locked 2h (until "
                        f"{datetime.fromtimestamp(cooldowns[sym]).strftime('%H:%M:%S')})")

                    append_audit({
                        'timestamp':   datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
                        'action':      'SELL',
                        'symbol':      sym,
                        'price':       round(exec_price, 6),
                        'reason':      exit_reason,
                        'pnl_usd':     round(pnl_usd, 2),
                        'pnl_pct':     round(pnl_pct * 100, 4),
                        'peak_price':  round(pos.get('peak_price', exec_price), 6),
                        'entry_price': round(pos['entry_price'], 6),
                        'equity':      round(state['cash'] + sum(
                            p['size_usd'] * (prices.get(s, p['entry_price']) / p['entry_price'])
                            for s, p in state['positions'].items()
                        ), 2),
                        'cash':        round(state['cash'], 2),
                        'open_positions': len(state['positions']),
                    }, audit_rows)

                    save_state({**state, 'cooldowns': cooldowns, 'last_heartbeat_ts': now_ts()})

            # ── SLOW TICK: ENTRY SCAN (every 5 min) ──────────
            if now_ts() - last_slow_tick >= SLOW_TICK_SECS:
                last_slow_tick = now_ts()

                # Universe refresh (hourly)
                if now_ts() - last_universe_ref >= UNIVERSE_REFRESH:
                    universe.refresh(force=True)
                    last_universe_ref = now_ts()

                symbols = universe.refresh()

                # Check how many positions we can open
                deployed = sum(p['size_usd'] for p in state['positions'].values())
                total_capital = state['cash'] + deployed

                for sym in symbols:
                    # Already holding
                    if sym in state['positions']:
                        continue

                    # Cooldown check
                    if cooldowns.get(sym, 0) > now_ts():
                        continue

                    # Max positions
                    if len(state['positions']) >= MAX_POSITIONS:
                        break

                    # Fetch OHLCV and evaluate
                    df = fetch_ohlcv(exchange, rl, sym, limit=100)
                    if df is None:
                        continue

                    fired, conviction, detail = check_entry_signal(df)

                    if not fired or conviction < MIN_CONVICTION:
                        continue

                    # Size the position
                    deployed_now = sum(p['size_usd'] for p in state['positions'].values())
                    size = position_size(conviction, total_capital, deployed_now, state['positions'])
                    if size < 100:
                        log(f"[Entry] {sym} signal fired but insufficient capital (size=${size:.0f})")
                        continue

                    # Fetch live price
                    try:
                        t = rl.call(exchange.fetch_ticker, sym)
                        if t is None:
                            continue
                        price = safe_float(t['last'])
                    except Exception as e:
                        log(f"[Entry] {sym} price fetch error: {e}")
                        continue

                    if price <= 0:
                        continue

                    exec_price = price * (1 + SLIPPAGE)

                    # Open position
                    state['positions'][sym] = {
                        'entry_price':  exec_price,
                        'signal_price': detail.get('signal_price', price),
                        'peak_price':   exec_price,
                        'size_usd':     size,
                        'conviction':   conviction,
                        'open_ts':      now_ts(),
                        'regime':       detail.get('regime', 'NEUTRAL'),
                        'vol_ratio':    detail.get('vol_ratio', 0.0),
                        'curr_hist':    detail.get('curr_hist', 0.0),
                        'prev_hist':    detail.get('prev_hist', 0.0),
                    }
                    state['cash']        -= size
                    state['trade_count'] += 1

                    log(f"!!! BUY {sym} @ ${exec_price:,.4f} | "
                        f"Size: ${size:,.2f} | Conviction: {conviction} | "
                        f"Vol: {detail.get('vol_ratio', 0):.2f}x | "
                        f"Regime: {detail.get('regime')} | "
                        f"MACD hist: {detail.get('curr_hist', 0):.6f}")

                    append_audit({
                        'timestamp':    datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
                        'action':       'BUY',
                        'symbol':       sym,
                        'price':        round(exec_price, 6),
                        'signal_price': round(detail.get('signal_price', price), 6),
                        'conviction':   conviction,
                        'regime':       detail.get('regime'),
                        'vol_ratio':    detail.get('vol_ratio', 0.0),
                        'macd_hist':    detail.get('curr_hist', 0.0),
                        'size_usd':     round(size, 2),
                        'equity':       round(state['cash'] + sum(
                            p['size_usd'] * (prices.get(s, p['entry_price']) / p['entry_price'])
                            for s, p in state['positions'].items()
                        ), 2),
                        'cash':         round(state['cash'], 2),
                        'open_positions': len(state['positions']),
                    }, audit_rows)

                    save_state({**state, 'cooldowns': cooldowns, 'last_heartbeat_ts': now_ts()})

            # ── HEARTBEAT ─────────────────────────────────────
            if now_ts() - last_heartbeat >= HEARTBEAT_SECS:
                last_heartbeat = now_ts()
                state['last_heartbeat_ts'] = now_ts()
                print_heartbeat(state, prices, cooldowns)
                save_state({**state, 'cooldowns': cooldowns, 'last_heartbeat_ts': now_ts()})

            # ── Sleep remainder of fast tick ──────────────────
            elapsed = now_ts() - loop_start
            sleep_for = max(0, FAST_TICK_SECS - elapsed)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        log("[Main] KeyboardInterrupt — shutting down gracefully.")
        save_state({**state, 'cooldowns': cooldowns, 'last_heartbeat_ts': now_ts()})
        log("[Main] State saved. Goodbye.")


if __name__ == '__main__':
    main()
