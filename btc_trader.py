"""
btc_trader.py — BTC Momentum Trader
Single-asset BTC/USD | $2,000 virtual capital | Shadow/paper mode

═══════════════════════════════════════════════════════════════════
STRATEGY
═══════════════════════════════════════════════════════════════════

ENTRY — ALL required on the last fully closed 1h candle (iloc[-2]):
  1. MACD(12,26,9) histogram flipped from ≤0 → >0
  2. Volume > 1.5× 20-bar rolling average (prior bars only)
  3. Regime is not BEAR  (EMA21 < EMA55)

EXIT PRIORITY LADDER (first trigger wins):
  0a. Never-green pain threshold  — trade never went positive AND
      loss ≥ $150 → cut it. Failed signal, get out.
  0b. Chop detection — trade WAS green, now loss ≥ $150 → cut it.
      Move is dead.
  1.  Break-even floor — once trade is green, stop never goes
      below entry. You cannot lose on a winner.
  2.  Tiered trailing stop (profit-scaled):
        Peak profit < $100  → 5% hard stop from entry (Tier 1)
        Peak profit ≥ $100  → floor at 75% of peak profit (Tier 2)
  3.  MACD histogram flips negative on closed candle → exit
  4.  5% hard stop from entry — absolute floor, always active

═══════════════════════════════════════════════════════════════════
TIMING
═══════════════════════════════════════════════════════════════════
  Every 60s:  exit checks (price-based stops, break-even, pain)
  Every 5min: MACD candle exit check (uses closed 1h candle)
  Every 1h:   entry signal scan
  Every 1h:   universe/regime refresh

STATE FILES (same names as before — continuity preserved):
  btc_trader_state.json       — atomic state
  kraken_btc_trader_audit_trail.csv
  kraken_btc_trader_events.log

Run: tmux attach -t kraken → python3 btc_trader.py
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
# PATHS — same as old btc_trader, continuity preserved
# ─────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
STATE_FILE  = os.path.join(BASE_DIR, 'btc_trader_state.json')
AUDIT_FILE  = os.path.join(BASE_DIR, 'kraken_btc_trader_audit_trail.csv')
EVENT_FILE  = os.path.join(BASE_DIR, 'kraken_btc_trader_events.log')

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
STARTING_CAPITAL  = 2_000.0
SLIPPAGE          = 0.0010      # 10bps per side
HARD_STOP_PCT     = 0.05        # 5% from entry — absolute floor
MAX_DD_PCT        = 0.15        # 15% portfolio drawdown kill switch
VOL_SPIKE_MULT    = 1.5         # volume must be 1.5× 20-bar avg
PAIN_THRESHOLD    = 150.0       # never-green and chop cut threshold
TIER2_THRESH      = 100.0       # profit ($) that arms Tier 2 trail
TIER2_FLOOR_PCT   = 0.75        # Tier 2: keep 75% of peak profit

# Loop timings
EXIT_INTERVAL     = 60          # price-based exit check every 60s
MACD_EXIT_INTERVAL = 300        # MACD candle exit every 5min
ENTRY_INTERVAL    = 3600        # entry scan every 1h
HEARTBEAT_INTERVAL = 300        # status print every 5min

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

# ─────────────────────────────────────────────────────────────
# STATE — load/save with full continuity from old btc_trader
# ─────────────────────────────────────────────────────────────
def fresh_state():
    now = now_ts()
    return {
        'virtual_usd':        STARTING_CAPITAL,
        'virtual_btc':        0.0,
        'max_equity':         STARTING_CAPITAL,
        'peak_price':         0.0,
        'entry_price':        0.0,
        'trade_count':        0,
        'ever_green':         False,
        'peak_profit_usd':    0.0,
        'tier2_armed':        False,
        'first_start_ts':     now,
        'total_paused_secs':  0.0,
        'session_start_ts':   now,
        'last_heartbeat_ts':  now,
    }

def save_state(state):
    state['last_heartbeat_ts'] = now_ts()
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=2)
    os.replace(tmp, STATE_FILE)

def load_state():
    if not os.path.exists(STATE_FILE):
        return None
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        return s
    except Exception as e:
        log(f"WARNING: state load failed ({e}) — fresh start.")
        return None

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
                log(f"[RateLimit] Backoff {wait}s")
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
def calc_macd_histogram(closes):
    ema12  = closes.ewm(span=12, adjust=False).mean()
    ema26  = closes.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd - signal

def calc_regime(closes):
    ema21 = closes.ewm(span=21, adjust=False).mean()
    ema55 = closes.ewm(span=55, adjust=False).mean()
    if ema21.iloc[-1] < ema55.iloc[-1]:
        return 'BEAR'
    if ema21.iloc[-1] > ema55.iloc[-1] and closes.iloc[-1] > ema21.iloc[-1]:
        return 'BULL'
    return 'NEUTRAL'

def fetch_ohlcv(exchange, rl, limit=120):
    try:
        raw = rl.call(exchange.fetch_ohlcv, 'BTC/USD', '1h', None, limit)
        if raw is None or len(raw) < 60:
            return None
        return pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
    except Exception as e:
        log(f"[OHLCV] {e}")
        return None

# ─────────────────────────────────────────────────────────────
# ENTRY SIGNAL
# Uses iloc[-2] — last fully CLOSED 1h candle. No look-ahead.
# ─────────────────────────────────────────────────────────────
def check_entry(df):
    """
    Returns (fired, detail_dict)
    ALL three conditions must be true on iloc[-2]:
      1. MACD histogram flipped ≤0 → >0
      2. Volume > 1.5× prior 20-bar average
      3. Regime != BEAR
    """
    if df is None or len(df) < 60:
        return False, {'reason': 'insufficient data'}

    hist      = calc_macd_histogram(df['c'])
    curr_hist = safe_float(hist.iloc[-2])
    prev_hist = safe_float(hist.iloc[-3])
    regime    = calc_regime(df['c'])

    if regime == 'BEAR':
        return False, {'reason': 'BEAR regime'}

    if not (curr_hist > 0 and prev_hist <= 0):
        return False, {
            'reason': 'no MACD flip',
            'curr_hist': round(curr_hist, 6),
            'prev_hist': round(prev_hist, 6)
        }

    # Volume spike — compare iloc[-2] volume to average of PRIOR 20 bars
    vol_avg   = df['v'].shift(1).rolling(20).mean().iloc[-2]
    vol_curr  = safe_float(df['v'].iloc[-2])
    vol_ratio = vol_curr / (vol_avg + 1e-9)

    if vol_ratio < VOL_SPIKE_MULT:
        return False, {'reason': f'vol too low ({vol_ratio:.2f}x < {VOL_SPIKE_MULT}x)'}

    signal_price = safe_float(df['c'].iloc[-2])

    return True, {
        'regime':       regime,
        'curr_hist':    round(curr_hist, 6),
        'prev_hist':    round(prev_hist, 6),
        'vol_ratio':    round(vol_ratio, 2),
        'signal_price': signal_price,
    }

# ─────────────────────────────────────────────────────────────
# MACD EXIT — checks if histogram has flipped negative
# Uses iloc[-2] — last fully CLOSED 1h candle. No look-ahead.
# ─────────────────────────────────────────────────────────────
def check_macd_exit(df):
    if df is None or len(df) < 30:
        return False
    hist = calc_macd_histogram(df['c'])
    return safe_float(hist.iloc[-2]) < 0

# ─────────────────────────────────────────────────────────────
# EXIT EVALUATION — price-based checks
# Called every 60s with live price.
# ─────────────────────────────────────────────────────────────
def evaluate_exit(state, current_price):
    """
    Full exit ladder. Returns (should_exit, reason) or (False, 'HOLD').

    Priority:
      0a. Never-green pain threshold
      0b. Chop detection
      1.  Break-even floor
      2.  Tiered trail (Tier 1: hard stop | Tier 2: 75% of peak profit)
      3.  [MACD flip — checked separately by caller]
      4.  5% hard stop — always active
    """
    entry         = state['entry_price']
    peak          = state['peak_price']
    ever_green    = state.get('ever_green', False)
    peak_profit   = state.get('peak_profit_usd', 0.0)
    tier2_armed   = state.get('tier2_armed', False)

    # Update peak price — only ever goes UP
    if current_price > peak:
        state['peak_price'] = current_price
        peak = current_price

    pnl_usd = (current_price - entry) / entry * STARTING_CAPITAL

    # Track peak profit — only ever goes UP
    if pnl_usd > peak_profit:
        state['peak_profit_usd'] = pnl_usd
        peak_profit = pnl_usd

    # Arm Tier 2 once peak profit hits $100
    if peak_profit >= TIER2_THRESH:
        state['tier2_armed'] = True
        tier2_armed = True

    # Mark ever-green
    if pnl_usd > 0 and not ever_green:
        state['ever_green'] = True
        ever_green = True

    # ── 0a. Never-green pain threshold ───────────────────────
    if not ever_green and pnl_usd <= -PAIN_THRESHOLD:
        return True, (f'FAILED_SIGNAL_CUT '
                      f'(never green, loss=${abs(pnl_usd):.0f} >= ${PAIN_THRESHOLD:.0f})')

    # ── 0b. Chop detection ────────────────────────────────────
    if ever_green and pnl_usd <= -PAIN_THRESHOLD:
        return True, (f'CHOP_DETECTED '
                      f'(was green, now loss=${abs(pnl_usd):.0f} >= ${PAIN_THRESHOLD:.0f})')

    # ── 1. Break-even floor ───────────────────────────────────
    if ever_green and current_price < entry:
        return True, f'BREAK_EVEN_FLOOR (entry=${entry:,.2f})'

    # ── 4. Hard stop — always active ─────────────────────────
    hard_stop_price = entry * (1 - HARD_STOP_PCT)
    if current_price <= hard_stop_price:
        return True, f'HARD_STOP_5PCT (stop=${hard_stop_price:,.2f})'

    # ── 2. Tiered trail ──────────────────────────────────────
    if tier2_armed:
        # Tier 2: floor at 75% of peak profit
        floor_usd   = TIER2_FLOOR_PCT * peak_profit
        floor_price = entry * (1 + floor_usd / STARTING_CAPITAL)
        if current_price <= floor_price:
            return True, (f'TRAIL_FLOOR_T2 '
                          f'(peak=${peak_profit:.0f}, '
                          f'floor=${floor_usd:.0f} = {TIER2_FLOOR_PCT*100:.0f}%, '
                          f'stop=${floor_price:,.2f})')
    # Tier 1: just the hard stop (already checked above)

    return False, 'HOLD'

# ─────────────────────────────────────────────────────────────
# AUDIT
# ─────────────────────────────────────────────────────────────
_audit_rows = []

def log_audit(state, price, action, reason, exec_price=0.0):
    global _audit_rows
    equity = state['virtual_usd'] + state['virtual_btc'] * price
    state['max_equity'] = max(state.get('max_equity', equity), equity)
    dd = (state['max_equity'] - equity) / state['max_equity']

    _audit_rows.append({
        'timestamp':   datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
        'action':      action,
        'reason':      reason,
        'price':       round(price, 2),
        'exec_price':  round(exec_price, 2),
        'entry_price': round(state.get('entry_price', 0), 2),
        'peak_price':  round(state.get('peak_price', 0), 2),
        'equity':      round(equity, 2),
        'drawdown':    round(dd, 4),
        'trade_count': state['trade_count'],
        'ever_green':  state.get('ever_green', False),
        'peak_profit': round(state.get('peak_profit_usd', 0), 2),
        'tier2_armed': state.get('tier2_armed', False),
    })
    pd.DataFrame(_audit_rows).to_csv(AUDIT_FILE, index=False)
    return equity, dd

# ─────────────────────────────────────────────────────────────
# HEALTH PRINT
# ─────────────────────────────────────────────────────────────
def print_health(state, price):
    equity  = state['virtual_usd'] + state['virtual_btc'] * price
    profit  = equity - STARTING_CAPITAL
    pp      = profit / STARTING_CAPITAL * 100
    max_eq  = state.get('max_equity', equity)
    dd      = (max_eq - equity) / max_eq if max_eq > 0 else 0
    gross   = fmt_dur(now_ts() - state['first_start_ts'])
    paused  = fmt_dur(state.get('total_paused_secs', 0))
    net     = fmt_dur(now_ts() - state['first_start_ts'] - state.get('total_paused_secs', 0))

    print("\n" + "=" * 62)
    print(f" BTC_TRADER HEALTH CHECK | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─" * 62)
    print(f" Trades:          {state['trade_count']}")
    print(f" Equity:          ${equity:,.2f}")
    print(f" P/L:             ${profit:+,.2f} ({pp:+.2f}%)")
    print(f" Max Drawdown:    {dd*100:.2f}% (limit: {MAX_DD_PCT*100:.0f}%)")
    print(f" Position:        {'LONG' if state['virtual_btc'] > 0 else 'FLAT'}")

    if state['virtual_btc'] > 0:
        entry        = state['entry_price']
        peak         = state['peak_price']
        pnl_usd      = (price - entry) / entry * STARTING_CAPITAL
        peak_profit  = state.get('peak_profit_usd', 0.0)
        tier2_armed  = state.get('tier2_armed', False)
        ever_green   = state.get('ever_green', False)

        # Calculate current stop level for display
        hard_stop    = entry * (1 - HARD_STOP_PCT)
        if tier2_armed:
            floor_usd   = TIER2_FLOOR_PCT * peak_profit
            floor_price = entry * (1 + floor_usd / STARTING_CAPITAL)
            stop_display = max(hard_stop, floor_price)
            stop_label   = f"Tier 2 floor (${floor_usd:.0f} = {TIER2_FLOOR_PCT*100:.0f}% of peak)"
        else:
            stop_display = hard_stop
            stop_label   = "Tier 1 (5% hard stop)"

        print(f" Entry Price:     ${entry:,.2f}")
        print(f" Current Price:   ${price:,.2f}")
        print(f" Peak Price:      ${peak:,.2f}")
        print(f" Current P/L:     ${pnl_usd:+,.2f}")
        print(f" Peak Profit:     ${peak_profit:,.2f}")
        print(f" Tier 2 Armed:    {'YES' if tier2_armed else 'NO (need $' + str(int(TIER2_THRESH)) + ' peak profit)'}")
        print(f" Ever Green:      {'YES' if ever_green else 'NO'}")
        print(f" Stop Price:      ${stop_display:,.2f}  [{stop_label}]")
        print(f" Hard Stop:       ${hard_stop:,.2f}")
        print(f" Pain Threshold:  -${PAIN_THRESHOLD:.0f} {'(active — never green)' if not ever_green else '(active — chop)'}")

    print("─" * 62)
    print(f" Gross Runtime:   {gross}")
    print(f" Total Paused:    {paused}")
    print(f" Net Runtime:     {net}")
    print("=" * 62 + "\n")

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    exchange = ccxt.kraken({'enableRateLimit': False})
    rl       = RateLimiter()

    log("--- BTC_TRADER: MACD FLIP ENTRY | TIERED EXIT | "
        "BREAK-EVEN FLOOR | PAIN THRESHOLD | $2K VIRTUAL ---")

    # ── Boot ─────────────────────────────────────────────────
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
        log("[Boot] CRITICAL: API unreachable. Exiting.")
        raise SystemExit(1)

    # ── Load or initialise state ──────────────────────────────
    saved = load_state()
    if saved:
        gap   = now_ts() - saved.get('last_heartbeat_ts', now_ts())
        state = saved
        # Add new fields if upgrading from old btc_trader
        state.setdefault('ever_green',      False)
        state.setdefault('peak_profit_usd', 0.0)
        state.setdefault('tier2_armed',     False)
        state['total_paused_secs'] = state.get('total_paused_secs', 0) + (
            gap if gap > 300 else 0)
        state['session_start_ts'] = now_ts()
        log(f">>> BTC_TRADER RESTARTED | Gap: {fmt_dur(gap)} | "
            f"Equity base: ${state['virtual_usd']:,.2f} | "
            f"BTC: {state['virtual_btc']:.6f} | "
            f"Trades: {state['trade_count']}")
        if state['virtual_btc'] > 0:
            log(f">>> RESUMING LONG | Entry: ${state['entry_price']:,.2f} | "
                f"Peak: ${state['peak_price']:,.2f}")
    else:
        log(f">>> BTC_TRADER FIRST START — fresh ${STARTING_CAPITAL:,.0f} virtual account.")
        state = fresh_state()

    # Load existing audit rows
    global _audit_rows
    if os.path.exists(AUDIT_FILE):
        try:
            _audit_rows = pd.read_csv(AUDIT_FILE).to_dict('records')
        except Exception:
            _audit_rows = []

    save_state(state)
    log("[Boot] Boot complete. Entering main loop.")

    # ── Timestamp gates ───────────────────────────────────────
    last_exit_check  = 0.0
    last_macd_check  = 0.0
    last_entry_check = 0.0
    last_heartbeat   = 0.0

    # ── MAIN LOOP ─────────────────────────────────────────────
    try:
        while True:
            loop_start = now_ts()

            # ── Get current price ─────────────────────────────
            ticker = rl.call(exchange.fetch_ticker, 'BTC/USD')
            price  = safe_float(ticker['last']) if ticker else 0.0

            if price <= 0:
                time.sleep(10)
                continue

            # ── Equity + kill switch ──────────────────────────
            equity     = state['virtual_usd'] + state['virtual_btc'] * price
            state['max_equity'] = max(state.get('max_equity', equity), equity)
            dd = (state['max_equity'] - equity) / state['max_equity']

            if dd >= MAX_DD_PCT:
                log(f"CRITICAL: {MAX_DD_PCT*100:.0f}% DRAWDOWN KILL SWITCH.")
                if state['virtual_btc'] > 0:
                    ep = price * (1 - SLIPPAGE)
                    state['virtual_usd'] = state['virtual_btc'] * ep
                    state['virtual_btc'] = 0.0
                    log(f"!!! SELL BTC (KILL_SWITCH) @ ${ep:,.2f}")
                    log_audit(state, price, 'SELL', 'KILL_SWITCH', ep)
                save_state(state)
                raise SystemExit(0)

            # ── EXIT CHECKS (every 60s, price-based) ─────────
            if now_ts() - last_exit_check >= EXIT_INTERVAL:
                last_exit_check = now_ts()

                if state['virtual_btc'] > 0:
                    should_exit, reason = evaluate_exit(state, price)

                    if should_exit:
                        ep = price * (1 - SLIPPAGE)
                        pnl_pct = (ep - state['entry_price']) / state['entry_price']
                        pnl_usd = state['virtual_btc'] * ep - STARTING_CAPITAL * (state['entry_price'] / state['entry_price'])
                        # Simpler: calc pnl from capital deployed
                        pnl_usd = (ep - state['entry_price']) / state['entry_price'] * STARTING_CAPITAL

                        state['virtual_usd'] = state['virtual_btc'] * ep
                        state['virtual_btc'] = 0.0
                        state['peak_price']  = 0.0
                        state['entry_price'] = 0.0
                        state['ever_green']  = False
                        state['peak_profit_usd'] = 0.0
                        state['tier2_armed'] = False

                        log(f"!!! SELL BTC ({reason}) @ ${ep:,.2f} | "
                            f"P&L: ${pnl_usd:+,.2f} ({pnl_pct*100:+.2f}%) | "
                            f"Cash: ${state['virtual_usd']:,.2f}")

                        log_audit(state, price, 'SELL', reason, ep)
                        save_state(state)

            # ── MACD EXIT CHECK (every 5min, candle-based) ────
            if now_ts() - last_macd_check >= MACD_EXIT_INTERVAL:
                last_macd_check = now_ts()

                if state['virtual_btc'] > 0:
                    df = fetch_ohlcv(exchange, rl)
                    if df is not None and check_macd_exit(df):
                        ep      = price * (1 - SLIPPAGE)
                        pnl_pct = (ep - state['entry_price']) / state['entry_price']
                        pnl_usd = pnl_pct * STARTING_CAPITAL

                        state['virtual_usd'] = state['virtual_btc'] * ep
                        state['virtual_btc'] = 0.0
                        state['peak_price']  = 0.0
                        state['entry_price'] = 0.0
                        state['ever_green']  = False
                        state['peak_profit_usd'] = 0.0
                        state['tier2_armed'] = False

                        log(f"!!! SELL BTC (MACD_FLIP_NEGATIVE) @ ${ep:,.2f} | "
                            f"P&L: ${pnl_usd:+,.2f} ({pnl_pct*100:+.2f}%) | "
                            f"Cash: ${state['virtual_usd']:,.2f}")

                        log_audit(state, price, 'SELL', 'MACD_FLIP_NEGATIVE', ep)
                        save_state(state)

            # ── ENTRY CHECK (every 1h, candle-based) ──────────
            if now_ts() - last_entry_check >= ENTRY_INTERVAL:
                last_entry_check = now_ts()

                if state['virtual_btc'] == 0 and state['virtual_usd'] > 100:
                    df = fetch_ohlcv(exchange, rl)
                    fired, detail = check_entry(df)

                    if fired:
                        ep = price * (1 + SLIPPAGE)
                        btc_bought = state['virtual_usd'] / ep

                        state['virtual_btc']     = btc_bought
                        state['virtual_usd']     = 0.0
                        state['entry_price']     = ep
                        state['peak_price']      = ep
                        state['ever_green']      = False
                        state['peak_profit_usd'] = 0.0
                        state['tier2_armed']     = False
                        state['trade_count']    += 1

                        log(f"!!! BUY BTC @ ${ep:,.2f} | "
                            f"BTC: {btc_bought:.6f} | "
                            f"Regime: {detail.get('regime')} | "
                            f"MACD hist: {detail.get('curr_hist')} | "
                            f"Vol: {detail.get('vol_ratio')}x")

                        log_audit(state, price, 'BUY',
                                  f"MACD_FLIP vol={detail.get('vol_ratio')}x "
                                  f"regime={detail.get('regime')}", ep)
                        save_state(state)
                    else:
                        log(f"[Entry] No signal — {detail.get('reason','')}")

            # ── HEARTBEAT (every 5min) ────────────────────────
            if now_ts() - last_heartbeat >= HEARTBEAT_INTERVAL:
                last_heartbeat = now_ts()
                print_health(state, price)
                save_state(state)

            # ── Sleep ─────────────────────────────────────────
            elapsed   = now_ts() - loop_start
            sleep_for = max(1, EXIT_INTERVAL - elapsed)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        log("[Main] KeyboardInterrupt — shutting down gracefully.")
        save_state(state)
        log("[Main] State saved. Goodbye.")

if __name__ == '__main__':
    main()
