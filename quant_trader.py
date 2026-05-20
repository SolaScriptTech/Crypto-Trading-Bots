"""
quant_trader_v1.py — MACD Momentum Scanner (LIVE / REAL MONEY)
================================================================
Derived from kraken_v8_1.py (shadow/paper mode).
This version executes REAL orders on Kraken via the live API.

Capital is loaded from your actual Kraken USD balance on first boot.
All subsequent P&L tracking uses the state file — reconcile manually
if you add/remove funds from the account.

═══════════════════════════════════════════════════════════════════
STRATEGY  (unchanged from v8.1)
═══════════════════════════════════════════════════════════════════

ENTRY — ALL required on iloc[-2] (last fully closed 1h candle):
  1. MACD(12,26,9) histogram crossed negative → positive
  2. Volume > 1.5× 20-bar rolling average
  3. Regime != BEAR  (EMA21 vs EMA55)
  4. Not in 2h cooldown, not already held
  5. Conviction score >= 40

EXIT PRIORITY LADDER (first trigger wins):
  0a. Never-green pain threshold (failed signal cut)
  0b. Chop detection (was-green-now-deep-negative)
  1.  Break-even floor (stop never below entry once green)
  2.  Dynamic capital-scaled trail (pattern-engine modified)
  3.  MACD histogram flips negative (after 3-bar min hold)
  4.  5% hard stop — absolute, pattern engine cannot override
  5.  15% portfolio kill switch — closes all, stops bot

UNIVERSAL FAILSAFES:
  · 5% hard stop per position — always active
  · Break-even floor — once green, stop never below entry
  · Pain threshold cuts — never-green and chop detection
  · 15% portfolio drawdown kill switch
  · 2h re-entry cooldown after any exit

═══════════════════════════════════════════════════════════════════
REAL EXECUTION LAYER
═══════════════════════════════════════════════════════════════════

BUY:  create_market_order(symbol, 'buy', base_amount)
      base_amount = size_usd / signal_price
      Actual fill price + actual filled amount logged from order response.

SELL: create_market_order(symbol, 'sell', pos['base_amount'])
      Sells the exact base units purchased. No rounding error.
      Actual fill price logged from order response.

Slippage: NOT simulated — real fills capture real slippage.
          Audit logs signal_price vs exec_price for every trade.

Fees: Kraken taker fee ~0.26%. Included in real fills.
      Not separately modelled — exec_price absorbs it.

═══════════════════════════════════════════════════════════════════
CREDENTIALS
═══════════════════════════════════════════════════════════════════
Loaded from .env in the same directory as this file:
  KRAKEN_API_KEY=...
  KRAKEN_API_SECRET=...

═══════════════════════════════════════════════════════════════════
FILES
═══════════════════════════════════════════════════════════════════
State:   quant_trader_v1_state.json   (atomic writes)
Audit:   quant_trader_v1_audit.csv    (every trade + heartbeat)
Log:     quant_trader_v1_events.log   (all decisions + errors)
Pattern: pattern_lookup.json          (rebuilt every 6h)

Run:     tmux new -s quant → python3 quant_trader_v1.py
"""

import ccxt
import pandas as pd
import numpy as np
import time
import os
import json
import math
import subprocess
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
STATE_FILE     = os.path.join(BASE_DIR, 'quant_trader_v1_state.json')
AUDIT_FILE     = os.path.join(BASE_DIR, 'quant_trader_v1_audit.csv')
EVENT_FILE     = os.path.join(BASE_DIR, 'quant_trader_v1_events.log')
PATTERN_FILE   = os.path.join(BASE_DIR, 'pattern_lookup.json')
ENV_FILE       = os.path.join(BASE_DIR, '.env')

# ─────────────────────────────────────────────────────────────
# ENV LOADER — no dotenv dependency required
# ─────────────────────────────────────────────────────────────
def load_env(path=ENV_FILE):
    env = {}
    if not os.path.exists(path):
        return env
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#') or '=' not in line:
                continue
            k, v = line.split('=', 1)
            env[k.strip()] = v.strip()
    return env

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
MAX_POSITIONS      = 5
DRY_POWDER_PCT     = 0.20        # keep 20% of capital undeployed
HARD_STOP_PCT      = 0.05        # 5% hard stop per position
MAX_DD_PCT         = 0.15        # 15% portfolio drawdown kill switch
MIN_VOL_24H_USD    = 5_000_000   # min 24h volume for universe inclusion
MIN_HISTORY_BARS   = 60
UNIVERSE_SIZE      = 30
VOL_SPIKE_MULT     = 1.5
SIZE_HIGH_PCT      = 0.15        # high-conviction position: 15% of capital
SIZE_LOW_PCT       = 0.10        # low-conviction position:  10% of capital
MIN_CONVICTION     = 40
COOLDOWN_SECS      = 7200        # 2h cooldown after any exit
WATCHLIST_TTL_SECS = 7200        # max time on watchlist before stale
MIN_POSITION_USD   = 100         # don't open positions smaller than $100

# Loop timings
EXIT_INTERVAL      = 30          # exit check every 30s
WATCHLIST_INTERVAL = 60          # warm assets scanned every 60s
COLD_SCAN_INTERVAL = 300         # cold universe scanned every 5min
HEARTBEAT_INTERVAL = 300         # heartbeat every 5min
UNIVERSE_REFRESH   = 3600        # universe rebuilt every 1h
PATTERN_REBUILD    = 21600       # pattern engine rebuilt every 6h

# Pain thresholds — never-green cut and chop detection
PAIN_THRESHOLDS = [
    (50_000, 500),    # $50K+ position → cut at -$500
    (15_000, 250),    # $15K-$50K      → cut at -$250
    (5_000,  150),    # $5K-$15K       → cut at -$150
    (0,      100),    # Under $5K      → cut at -$100
]

PAIRS = [
    'BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'TAO/USD',
    'DOGE/USD', 'HYPE/USD', 'SUI/USD', 'ADA/USD', 'ZEC/USD',
]

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

def get_pain_threshold(size_usd):
    for min_size, threshold in PAIN_THRESHOLDS:
        if size_usd >= min_size:
            return threshold
    return 100

def load_pattern_lookup():
    if not os.path.exists(PATTERN_FILE):
        return {}
    try:
        with open(PATTERN_FILE) as f:
            return json.load(f)
    except Exception:
        return {}

def get_trail_modifier(symbol, pattern_lookup):
    entry = pattern_lookup.get(symbol, {})
    return safe_float(entry.get('trail_modifier', 1.0), 1.0)

# ─────────────────────────────────────────────────────────────
# STATE
# ─────────────────────────────────────────────────────────────
def save_state(state, cooldowns):
    payload = {**state, 'cooldowns': cooldowns, 'last_heartbeat_ts': now_ts()}
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(payload, f, indent=2)
    os.replace(tmp, STATE_FILE)

def load_state():
    if not os.path.exists(STATE_FILE):
        return {}, {}
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        cooldowns = s.pop('cooldowns', {})
        return s, cooldowns
    except Exception as e:
        log(f"WARNING: state load failed ({e}) — fresh start.")
        return {}, {}

def append_audit(row, audit_rows):
    audit_rows.append(row)
    pd.DataFrame(audit_rows).to_csv(AUDIT_FILE, index=False)

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
                log(f"[RateLimit] Backoff {wait}s (attempt {attempt+1})")
                time.sleep(wait)
            except ccxt.NetworkError as e:
                wait = self.BACKOFF_BASE * (2 ** attempt)
                log(f"[Network] Backoff {wait}s: {e}")
                time.sleep(wait)
            except Exception:
                raise
        log("[RateLimit] Max retries exceeded.")
        return None

# ─────────────────────────────────────────────────────────────
# REAL ORDER EXECUTION
# ─────────────────────────────────────────────────────────────
def execute_buy(exchange, rl, symbol, size_usd, signal_price):
    """
    Place a real market buy order on Kraken.

    Returns dict:
      fill_price   — actual weighted average fill price
      base_amount  — actual units of base asset received
      cost_usd     — actual USD spent (fill_price × base_amount)
      order_id     — Kraken order ID for records
      slippage_pct — (fill_price - signal_price) / signal_price × 100
    Raises on failure — caller handles.
    """
    # Calculate base amount from signal price
    raw_amount   = size_usd / signal_price
    base_amount  = safe_float(exchange.amount_to_precision(symbol, raw_amount))

    if base_amount <= 0:
        raise ValueError(f"Calculated base amount is zero for {symbol} "
                         f"(size=${size_usd:.2f}, price=${signal_price:.4f})")

    log(f"[Order] Placing MARKET BUY {symbol} | "
        f"Amount: {base_amount} | Signal price: ${signal_price:,.4f}")

    order = rl.call(exchange.create_market_order, symbol, 'buy', base_amount)

    if order is None:
        raise RuntimeError(f"create_market_order returned None for {symbol}")

    order_id = order.get('id', 'unknown')

    # Try to get fill from order response directly
    fill_price   = safe_float(order.get('average') or order.get('price'), 0.0)
    filled_amount = safe_float(order.get('filled') or order.get('amount'), 0.0)

    # If not immediately available, wait and fetch
    if fill_price <= 0 or filled_amount <= 0:
        log(f"[Order] Fetching fill details for order {order_id}...")
        time.sleep(3)
        try:
            fetched = rl.call(exchange.fetch_order, order_id, symbol)
            if fetched:
                fill_price    = safe_float(fetched.get('average') or
                                           fetched.get('price'), signal_price)
                filled_amount = safe_float(fetched.get('filled') or
                                           fetched.get('amount'), base_amount)
        except Exception as e:
            log(f"[Order] fetch_order failed ({e}) — using signal price as fallback")
            fill_price    = signal_price
            filled_amount = base_amount

    # Final fallbacks so accounting is never zero
    if fill_price <= 0:
        fill_price = signal_price
    if filled_amount <= 0:
        filled_amount = base_amount

    cost_usd     = fill_price * filled_amount
    slippage_pct = (fill_price - signal_price) / signal_price * 100

    log(f"[Order] BUY FILLED {symbol} | "
        f"Fill: ${fill_price:,.4f} | "
        f"Amount: {filled_amount} | "
        f"Cost: ${cost_usd:,.2f} | "
        f"Slippage: {slippage_pct:+.3f}% | "
        f"Order ID: {order_id}")

    return {
        'fill_price':    fill_price,
        'base_amount':   filled_amount,
        'cost_usd':      cost_usd,
        'order_id':      order_id,
        'slippage_pct':  slippage_pct,
    }


def execute_sell(exchange, rl, symbol, pos, signal_price):
    """
    Place a real market sell order on Kraken.

    Sells pos['base_amount'] — the exact units purchased.
    Returns dict:
      fill_price   — actual weighted average fill price
      proceeds_usd — actual USD received
      pnl_usd      — proceeds_usd - pos['cost_usd']
      pnl_pct      — pnl_usd / pos['cost_usd'] × 100
      order_id     — Kraken order ID for records
      slippage_pct — (signal_price - fill_price) / signal_price × 100
    Raises on failure — caller handles.
    """
    base_amount = safe_float(pos.get('base_amount', 0.0))

    if base_amount <= 0:
        raise ValueError(f"No base_amount in position for {symbol}: {pos}")

    # Respect exchange precision on the sell amount too
    base_amount = safe_float(exchange.amount_to_precision(symbol, base_amount))

    log(f"[Order] Placing MARKET SELL {symbol} | "
        f"Amount: {base_amount} | Signal price: ${signal_price:,.4f}")

    order = rl.call(exchange.create_market_order, symbol, 'sell', base_amount)

    if order is None:
        raise RuntimeError(f"create_market_order returned None for {symbol}")

    order_id = order.get('id', 'unknown')

    fill_price    = safe_float(order.get('average') or order.get('price'), 0.0)
    filled_amount = safe_float(order.get('filled') or order.get('amount'), 0.0)

    if fill_price <= 0 or filled_amount <= 0:
        log(f"[Order] Fetching fill details for order {order_id}...")
        time.sleep(3)
        try:
            fetched = rl.call(exchange.fetch_order, order_id, symbol)
            if fetched:
                fill_price    = safe_float(fetched.get('average') or
                                           fetched.get('price'), signal_price)
                filled_amount = safe_float(fetched.get('filled') or
                                           fetched.get('amount'), base_amount)
        except Exception as e:
            log(f"[Order] fetch_order failed ({e}) — using signal price as fallback")
            fill_price    = signal_price
            filled_amount = base_amount

    if fill_price <= 0:
        fill_price = signal_price
    if filled_amount <= 0:
        filled_amount = base_amount

    proceeds_usd = fill_price * filled_amount
    cost_usd     = safe_float(pos.get('cost_usd', pos['size_usd']))
    pnl_usd      = proceeds_usd - cost_usd
    pnl_pct      = (pnl_usd / cost_usd * 100) if cost_usd > 0 else 0.0
    slippage_pct = (signal_price - fill_price) / signal_price * 100

    log(f"[Order] SELL FILLED {symbol} | "
        f"Fill: ${fill_price:,.4f} | "
        f"Proceeds: ${proceeds_usd:,.2f} | "
        f"P&L: ${pnl_usd:+,.2f} ({pnl_pct:+.2f}%) | "
        f"Slippage: {slippage_pct:+.3f}% | "
        f"Order ID: {order_id}")

    return {
        'fill_price':    fill_price,
        'proceeds_usd':  proceeds_usd,
        'pnl_usd':       pnl_usd,
        'pnl_pct':       pnl_pct,
        'order_id':      order_id,
        'slippage_pct':  slippage_pct,
    }

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
    ema21  = closes.ewm(span=21, adjust=False).mean()
    ema55  = closes.ewm(span=55, adjust=False).mean()
    last_c = closes.iloc[-1]
    e21    = ema21.iloc[-1]
    e55    = ema55.iloc[-1]
    if e21 > e55 and last_c > e21:
        return 'BULL'
    if e21 < e55:
        return 'BEAR'
    return 'NEUTRAL'

def calc_rsi(closes, period=14):
    delta = closes.diff()
    gain  = delta.clip(lower=0).ewm(com=period - 1, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period - 1, adjust=False).mean()
    return safe_float((100 - 100 / (1 + gain / (loss + 1e-9))).iloc[-1], 50)

def calc_conviction(hist_val, vol_ratio, regime, rsi_val):
    score = 0
    if hist_val > 0:          score += 30
    if vol_ratio >= 2.5:      score += 30
    elif vol_ratio >= 2.0:    score += 25
    elif vol_ratio >= 1.5:    score += 20
    else:                     score += 10
    if regime == 'BULL':      score += 25
    elif regime == 'NEUTRAL': score += 15
    if rsi_val < 40:          score += 15
    elif rsi_val < 50:        score += 10
    elif rsi_val < 60:        score += 5
    return min(score, 100)

def fetch_ohlcv(exchange, rl, symbol, limit=100):
    try:
        raw = rl.call(exchange.fetch_ohlcv, symbol, '1h', None, limit)
        if raw is None or len(raw) < MIN_HISTORY_BARS:
            return None
        return pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
    except Exception as e:
        log(f"[OHLCV] {symbol}: {e}")
        return None

# ─────────────────────────────────────────────────────────────
# WATCHLIST MANAGER
# ─────────────────────────────────────────────────────────────
class WatchlistManager:
    """
    Maintains warm assets — those close to firing a MACD entry.
    Promoted when MACD histogram is negative but shrinking toward zero
    and volume is rising. Scanned every 60s vs cold universe every 5min.
    """
    def __init__(self):
        self._list = {}   # symbol → promoted_ts

    def update(self, symbol, df, positions):
        if symbol in positions:
            self._list.pop(symbol, None)
            return

        hist      = calc_macd_histogram(df['c'])
        curr      = safe_float(hist.iloc[-2])
        prev      = safe_float(hist.iloc[-3])
        vol_avg   = df['v'].rolling(20).mean().iloc[-2]
        vol_curr  = safe_float(df['v'].iloc[-2])
        vol_ratio = vol_curr / (vol_avg + 1e-9)
        regime    = calc_regime(df['c'])

        is_warm = (curr < 0 and abs(curr) < abs(prev) and
                   vol_ratio > 1.0 and regime != 'BEAR')

        if is_warm:
            if symbol not in self._list:
                self._list[symbol] = now_ts()
                log(f"[Watchlist] +{symbol} promoted "
                    f"(hist={curr:.6f}, vol={vol_ratio:.2f}x)")
        else:
            if symbol in self._list:
                log(f"[Watchlist] -{symbol} demoted")
            self._list.pop(symbol, None)

        # TTL expiry
        for sym in list(self._list.keys()):
            if now_ts() - self._list[sym] > WATCHLIST_TTL_SECS:
                log(f"[Watchlist] -{sym} expired (TTL)")
                self._list.pop(sym)

    def is_warm(self, symbol):
        return symbol in self._list

    def symbols(self):
        return list(self._list.keys())

    def remove(self, symbol):
        self._list.pop(symbol, None)

# ─────────────────────────────────────────────────────────────
# ENTRY SIGNAL
# ─────────────────────────────────────────────────────────────
def check_entry_signal(df):
    """
    Returns (fired, conviction, detail).
    Uses iloc[-2] — last fully closed 1h candle. No look-ahead.
    """
    if df is None or len(df) < MIN_HISTORY_BARS:
        return False, 0, {'reason': 'insufficient data'}

    hist      = calc_macd_histogram(df['c'])
    curr_hist = safe_float(hist.iloc[-2])
    prev_hist = safe_float(hist.iloc[-3])
    regime    = calc_regime(df['c'])

    if not (curr_hist > 0 and prev_hist <= 0):
        return False, 0, {
            'reason': 'no MACD flip',
            'curr': round(curr_hist, 6),
            'prev': round(prev_hist, 6),
        }

    vol_avg   = df['v'].rolling(20).mean().iloc[-2]
    vol_curr  = safe_float(df['v'].iloc[-2])
    vol_ratio = vol_curr / (vol_avg + 1e-9)

    if vol_ratio < VOL_SPIKE_MULT:
        return False, 0, {'reason': f'vol too low ({vol_ratio:.2f}x)'}

    if regime == 'BEAR':
        return False, 0, {'reason': 'BEAR regime'}

    rsi        = calc_rsi(df['c'])
    conviction = calc_conviction(curr_hist, vol_ratio, regime, rsi)

    return True, conviction, {
        'regime':       regime,
        'curr_hist':    round(curr_hist, 6),
        'prev_hist':    round(prev_hist, 6),
        'vol_ratio':    round(vol_ratio, 2),
        'rsi':          round(rsi, 1),
        'conviction':   conviction,
        'signal_price': safe_float(df['c'].iloc[-2]),
    }

def check_macd_exit(df):
    """Returns True if MACD histogram has flipped negative on iloc[-2]."""
    if df is None or len(df) < 30:
        return False
    hist = calc_macd_histogram(df['c'])
    return safe_float(hist.iloc[-2]) < 0

# ─────────────────────────────────────────────────────────────
# EXIT EVALUATION
# ─────────────────────────────────────────────────────────────
def evaluate_exit(pos, current_price, pattern_lookup):
    """
    Full exit ladder. Returns (should_exit, reason).

    Priority:
      0a. Never-green pain threshold
      0b. Chop detection
      1.  Break-even floor
      2.  Dynamic capital-scaled trail (pattern modified)
      3.  MACD flip (checked by caller on 5-min cadence)
      4.  5% hard stop — absolute
    """
    entry      = pos['entry_price']
    peak       = pos.get('peak_price', entry)
    size       = pos['size_usd']
    ever_green = pos.get('ever_green', False)

    # Update peak price
    peak = max(peak, current_price)
    pos['peak_price'] = peak

    pnl_pct     = (current_price - entry) / entry
    current_pnl = size * pnl_pct
    peak_pnl    = size * ((peak - entry) / entry)

    # Mark ever-green
    if current_pnl > 0 and not ever_green:
        pos['ever_green'] = True
        ever_green        = True

    pain = get_pain_threshold(size)

    # ── 0a. Never-green pain threshold ───────────────────────
    if not ever_green and current_pnl <= -pain:
        return True, (f'FAILED_SIGNAL_CUT '
                      f'(never green, loss=${abs(current_pnl):.0f} '
                      f'> threshold=${pain})')

    # ── 0b. Chop detection ────────────────────────────────────
    if ever_green and current_pnl <= -pain:
        return True, (f'CHOP_DETECTED '
                      f'(was green, now loss=${abs(current_pnl):.0f} '
                      f'> threshold=${pain})')

    # ── 1. Break-even floor ───────────────────────────────────
    if ever_green and current_price < entry:
        return True, f'BREAK_EVEN_FLOOR (stop=entry=${entry:.4f})'

    # ── 4. Hard stop — check before trail (no exceptions) ────
    loss_pct = (entry - current_price) / entry
    if loss_pct >= HARD_STOP_PCT:
        return True, f'HARD_STOP_5PCT (loss={loss_pct*100:.2f}%)'

    # ── 2. Dynamic capital-scaled trail ──────────────────────
    if peak_pnl <= 0:
        return False, 'HOLD_FLAT'

    one_pct_usd = size * 0.01

    if one_pct_usd >= 1000:   keep_pct, tier = 0.90, 'LARGE'
    elif one_pct_usd >= 600:  keep_pct, tier = 0.88, 'MID_HIGH'
    elif one_pct_usd >= 300:  keep_pct, tier = 0.85, 'MID'
    elif one_pct_usd >= 100:  keep_pct, tier = 0.75, 'SMALL'
    else:                     keep_pct, tier = 0.65, 'MICRO'

    sym      = pos.get('symbol', '')
    modifier = get_trail_modifier(sym, pattern_lookup)
    modifier = max(0.70, min(1.30, modifier))

    # modifier > 1 = "let it run" → keep less (lower floor)
    # modifier < 1 = "protect now" → keep more (higher floor)
    adjusted_keep = keep_pct / modifier
    adjusted_keep = max(0.50, min(0.95, adjusted_keep))

    floor_pnl = peak_pnl * adjusted_keep

    if current_pnl < floor_pnl:
        return True, (f'TRAIL_FLOOR_{tier} '
                      f'(1%=${one_pct_usd:.0f}, '
                      f'keep={adjusted_keep*100:.0f}%, '
                      f'modifier={modifier:.2f}, '
                      f'peak=${peak_pnl:.2f}, '
                      f'floor=${floor_pnl:.2f}, '
                      f'curr=${current_pnl:.2f})')

    return False, f'HOLD_{tier}'

# ─────────────────────────────────────────────────────────────
# POSITION SIZING
# ─────────────────────────────────────────────────────────────
def calc_position_size(conviction, total_capital, positions):
    deployed  = sum(p['size_usd'] for p in positions.values())
    max_deploy = total_capital * (1 - DRY_POWDER_PCT)
    available  = max(0.0, max_deploy - deployed)
    if available < MIN_POSITION_USD:
        return 0.0
    pct  = SIZE_HIGH_PCT if conviction >= 60 else SIZE_LOW_PCT
    size = total_capital * pct
    return min(size, available)

# ─────────────────────────────────────────────────────────────
# UNIVERSE MANAGER
# ─────────────────────────────────────────────────────────────
class UniverseManager:
    def __init__(self, exchange, rl):
        self.exchange   = exchange
        self.rl         = rl
        self.universe   = list(PAIRS)
        self.last_built = 0.0

    def refresh(self, force=False):
        if not force and (now_ts() - self.last_built) < UNIVERSE_REFRESH:
            return list(self.universe)
        log("[Universe] Refreshing top-30 liquid /USD pairs...")
        try:
            tickers = self.rl.call(self.exchange.fetch_tickers)
            if tickers is None:
                return list(self.universe)
            candidates = []
            for symbol, t in tickers.items():
                if not symbol.endswith('/USD'):
                    continue
                if symbol in ('USDT/USD', 'USDC/USD', 'DAI/USD',
                              'BUSD/USD', 'EUR/USD'):
                    continue
                vol = t.get('quoteVolume') or 0
                if vol >= MIN_VOL_24H_USD:
                    candidates.append((symbol, vol))
            candidates.sort(key=lambda x: x[1], reverse=True)
            self.universe   = [s for s, _ in candidates[:UNIVERSE_SIZE]]
            self.last_built = now_ts()
            log(f"[Universe] {len(self.universe)} pairs: "
                f"{', '.join(self.universe[:10])}"
                f"{'...' if len(self.universe) > 10 else ''}")
        except Exception as e:
            log(f"[Universe] Error: {e}")
        return list(self.universe)

# ─────────────────────────────────────────────────────────────
# PATTERN ENGINE RUNNER
# ─────────────────────────────────────────────────────────────
def run_pattern_engine_background(pairs=None):
    engine_path = os.path.join(BASE_DIR, 'pattern_engine.py')
    if not os.path.exists(engine_path):
        log("[PatternEngine] pattern_engine.py not found — skipping")
        return
    args = ['python3', engine_path]
    if pairs:
        args.extend(pairs)
    try:
        subprocess.Popen(args, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL, cwd=BASE_DIR)
        log("[PatternEngine] Launched background rebuild")
    except Exception as e:
        log(f"[PatternEngine] Launch failed: {e}")

# ─────────────────────────────────────────────────────────────
# HEARTBEAT
# ─────────────────────────────────────────────────────────────
def print_heartbeat(state, prices, cooldowns, watchlist_syms,
                    pattern_lookup, exchange, rl):
    cash      = state['cash']
    positions = state['positions']

    # Best-effort real balance check — non-fatal
    real_usd = None
    try:
        balance  = rl.call(exchange.fetch_balance)
        real_usd = safe_float(
            (balance or {}).get('USD', {}).get('free', None), None)
    except Exception:
        pass

    pos_val = sum(
        p['size_usd'] * (prices.get(s, p['entry_price']) / p['entry_price'])
        for s, p in positions.items()
    )
    equity   = cash + pos_val
    start_eq = state.get('starting_capital', cash)
    max_eq   = state.get('max_equity', equity)
    dd       = (max_eq - equity) / max_eq if max_eq > 0 else 0
    profit   = equity - start_eq
    pp       = profit / start_eq * 100 if start_eq > 0 else 0
    trades   = state.get('trade_count', 0)
    wins     = state.get('total_trades_won', 0)
    wr       = (wins / trades * 100) if trades > 0 else 0.0
    closed   = state.get('total_pnl_closed', 0.0)
    gross_rt = fmt_dur(now_ts() - state.get('first_start_ts', now_ts()))
    paused   = fmt_dur(state.get('total_paused_secs', 0))
    net_rt   = fmt_dur(now_ts() - state.get('first_start_ts', now_ts())
                       - state.get('total_paused_secs', 0))

    print("\n" + "=" * 72)
    print(f" QUANT_TRADER V1 HEARTBEAT | "
          f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | LIVE REAL MONEY")
    print("─" * 72)
    print(f" Portfolio Equity:   ${equity:>12,.2f}   "
          f"(start: ${start_eq:,.2f})")
    print(f" Total P&L:          ${profit:>+12,.2f}   ({pp:+.3f}%)")
    print(f" Closed P&L:         ${closed:>+12,.2f}")
    print(f" Cash (tracked):     ${cash:>12,.2f}")
    if real_usd is not None:
        diff = real_usd - cash
        print(f" Cash (exchange):    ${real_usd:>12,.2f}   "
              f"({'OK' if abs(diff) < 1 else f'DELTA ${diff:+.2f} — check manually'})")
    print(f" Max Drawdown:       {dd*100:>11.2f}%   (limit: {MAX_DD_PCT*100:.0f}%)")
    print(f" Trades: {trades}  |  Won: {wins}  |  Win Rate: {wr:.1f}%")
    print("─" * 72)

    if positions:
        print(f" {'SYMBOL':<12} {'SIZE':>9} {'ENTRY':>10} {'CURR':>10} "
              f"{'P&L$':>9} {'PEAK$':>9} {'EVER_G':>6} {'MOD':>6}")
        for sym, pos in positions.items():
            cur      = prices.get(sym, pos['entry_price'])
            g_usd    = pos['size_usd'] * (cur - pos['entry_price']) / pos['entry_price']
            peak_usd = pos['size_usd'] * (
                pos.get('peak_price', pos['entry_price']) - pos['entry_price']
            ) / pos['entry_price']
            eg       = 'Y' if pos.get('ever_green') else 'N'
            mod      = get_trail_modifier(sym, pattern_lookup)
            print(f" {sym:<12} ${pos['size_usd']:>8,.0f} "
                  f"${pos['entry_price']:>9,.4f} "
                  f"${cur:>9,.4f} "
                  f"${g_usd:>+8,.2f} "
                  f"${peak_usd:>8,.2f} "
                  f"{eg:>6} "
                  f"{mod:>5.2f}x")
    else:
        print(" Positions:          FLAT — scanning for entries")

    if watchlist_syms:
        print("─" * 72)
        print(f" Watchlist ({len(watchlist_syms)}): {', '.join(watchlist_syms)}")

    active_cds = {s: t for s, t in cooldowns.items() if now_ts() < t}
    if active_cds:
        print("─" * 72)
        print(f" Cooling down ({len(active_cds)}): " +
              ", ".join(
                  f"{s} until {datetime.fromtimestamp(t).strftime('%H:%M')}"
                  for s, t in active_cds.items()))

    print("─" * 72)
    print(f" Dry Powder:         {DRY_POWDER_PCT*100:.0f}% reserve  |  "
          f"Max positions: {MAX_POSITIONS}")
    print(f" Gross Runtime:      {gross_rt}")
    print(f" Total Paused:       {paused}")
    print(f" Net Runtime:        {net_rt}")
    print("=" * 72 + "\n")

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    # ── Load credentials from .env ────────────────────────────
    env = load_env()
    api_key    = env.get('KRAKEN_API_KEY', '')
    api_secret = env.get('KRAKEN_API_SECRET', '')

    if not api_key or not api_secret:
        raise SystemExit(
            "CRITICAL: KRAKEN_API_KEY or KRAKEN_API_SECRET missing from .env. "
            "Check the .env file and restart."
        )

    # ── Exchange init — real credentials ─────────────────────
    exchange = ccxt.kraken({
        'apiKey':          api_key,
        'secret':          api_secret,
        'enableRateLimit': False,   # we handle rate limiting ourselves
    })

    rl        = RateLimiter()
    universe  = UniverseManager(exchange, rl)
    watchlist = WatchlistManager()

    log("─" * 72)
    log("QUANT_TRADER V1 | MACD FLIP | DYNAMIC TRAIL | PATTERN ENGINE | "
        "PAIN THRESHOLD | BREAK-EVEN FLOOR | WATCHLIST | LIVE REAL MONEY")
    log("─" * 72)

    # ── NTP settle ────────────────────────────────────────────
    log("[Boot] Waiting 15s for NTP clock sync...")
    time.sleep(15)

    # ── Load markets (required for precision helpers) ─────────
    log("[Boot] Loading Kraken market specs...")
    try:
        rl.call(exchange.load_markets)
        log("[Boot] Markets loaded.")
    except Exception as e:
        log(f"[Boot] load_markets failed ({e}) — continuing anyway.")

    # ── Verify connectivity ───────────────────────────────────
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
        log("[Boot] CRITICAL: API unreachable after 5 minutes. Exiting.")
        raise SystemExit(1)

    # ── Load state ────────────────────────────────────────────
    saved, cooldowns = load_state()

    if saved:
        gap   = now_ts() - saved.get('last_heartbeat_ts', now_ts())
        state = saved
        state['total_paused_secs'] = state.get('total_paused_secs', 0) + (
            gap if gap > 300 else 0)
        state['session_start_ts'] = now_ts()
        log(f">>> RESTARTED | Gap: {fmt_dur(gap)} | "
            f"Cash: ${state['cash']:,.2f} | "
            f"Positions: {len(state['positions'])} | "
            f"Trades: {state['trade_count']}")
        for sym, pos in state['positions'].items():
            log(f">>> RESUMING POSITION {sym} | "
                f"Entry: ${pos['entry_price']:,.4f} | "
                f"Size: ${pos['size_usd']:,.2f} | "
                f"Base units: {pos.get('base_amount', '?')}")
    else:
        # ── First start — fetch real USD balance ──────────────
        log("[Boot] First start — fetching real Kraken USD balance...")
        starting_capital = 0.0
        try:
            balance          = rl.call(exchange.fetch_balance)
            starting_capital = safe_float(
                (balance or {}).get('USD', {}).get('free', 0.0), 0.0)
            log(f"[Boot] Real USD balance: ${starting_capital:,.2f}")
        except Exception as e:
            log(f"[Boot] fetch_balance failed ({e})")

        if starting_capital <= 0:
            raise SystemExit(
                "CRITICAL: Could not fetch USD balance or balance is zero. "
                "Check API key permissions (require: Query Funds, Create Orders)."
            )

        log(f">>> FIRST START — live account | "
            f"Starting capital: ${starting_capital:,.2f}")

        state = {
            'cash':              starting_capital,
            'starting_capital':  starting_capital,
            'max_equity':        starting_capital,
            'positions':         {},
            'trade_count':       0,
            'total_trades_won':  0,
            'total_pnl_closed':  0.0,
            'first_start_ts':    now_ts(),
            'total_paused_secs': 0.0,
            'session_start_ts':  now_ts(),
            'last_heartbeat_ts': now_ts(),
        }

    # Ensure starting_capital is always present in state
    if 'starting_capital' not in state:
        state['starting_capital'] = state['cash']

    # ── Load audit rows ───────────────────────────────────────
    audit_rows = []
    if os.path.exists(AUDIT_FILE):
        try:
            audit_rows = pd.read_csv(AUDIT_FILE).to_dict('records')
        except Exception:
            audit_rows = []

    universe.refresh(force=True)

    # ── Launch pattern engine ─────────────────────────────────
    run_pattern_engine_background(universe.universe)
    pattern_lookup   = load_pattern_lookup()
    last_pattern_run = now_ts()

    save_state(state, cooldowns)
    log("[Boot] Boot complete. Entering main loop.")

    # ── Timestamp gates ───────────────────────────────────────
    last_exit_check     = 0.0
    last_watchlist_scan = 0.0
    last_cold_scan      = 0.0
    last_heartbeat      = 0.0

    # ── MAIN LOOP ─────────────────────────────────────────────
    try:
        while True:
            loop_start = now_ts()

            # ── Fetch prices for open positions ───────────────
            prices = {}
            for sym in list(state['positions'].keys()):
                try:
                    t = rl.call(exchange.fetch_ticker, sym)
                    if t:
                        prices[sym] = safe_float(t['last'])
                except Exception as e:
                    log(f"[Price] {sym}: {e}")

            # ── Equity and kill switch ────────────────────────
            pos_val = sum(
                p['size_usd'] * (prices.get(s, p['entry_price']) / p['entry_price'])
                for s, p in state['positions'].items()
            )
            equity              = state['cash'] + pos_val
            state['max_equity'] = max(state.get('max_equity', equity), equity)
            dd = ((state['max_equity'] - equity) / state['max_equity']
                  if state['max_equity'] > 0 else 0)

            if dd >= MAX_DD_PCT:
                log(f"CRITICAL: {MAX_DD_PCT*100:.0f}% KILL SWITCH — closing all positions.")
                for sym in list(state['positions'].keys()):
                    price = prices.get(sym, state['positions'][sym]['entry_price'])
                    pos   = state['positions'].pop(sym)
                    try:
                        fill = execute_sell(exchange, rl, sym, pos, price)
                        pnl  = fill['pnl_usd']
                        ep   = fill['fill_price']
                        state['cash']             += fill['proceeds_usd']
                        state['total_pnl_closed'] += pnl
                        log(f"!!! SELL {sym} (KILL_SWITCH) | "
                            f"Fill: ${ep:,.4f} | P&L: ${pnl:+,.2f}")
                        append_audit({
                            'timestamp':  datetime.now(timezone.utc).strftime(
                                '%Y-%m-%d %H:%M:%S.%f'),
                            'action':     'SELL', 'symbol': sym,
                            'price':      round(ep, 6),
                            'reason':     'KILL_SWITCH',
                            'pnl_usd':    round(pnl, 2),
                            'order_id':   fill['order_id'],
                            'equity':     round(state['cash'], 2),
                            'cash':       round(state['cash'], 2),
                        }, audit_rows)
                    except Exception as e:
                        log(f"[KillSwitch] SELL FAILED for {sym}: {e} — "
                            f"MANUAL INTERVENTION REQUIRED")
                save_state(state, cooldowns)
                log("[KillSwitch] Bot stopping.")
                break

            # ── EXIT CHECK (every 30s) ────────────────────────
            if now_ts() - last_exit_check >= EXIT_INTERVAL:
                last_exit_check = now_ts()

                for sym in list(state['positions'].keys()):
                    pos   = state['positions'][sym]
                    price = prices.get(sym, 0.0)
                    if price <= 0:
                        continue

                    pos['symbol']     = sym
                    pos['peak_price'] = max(pos.get('peak_price',
                                                    pos['entry_price']), price)
                    if price > pos['entry_price'] and not pos.get('ever_green'):
                        pos['ever_green'] = True

                    exit_reason = None

                    # Tiered exit (pain threshold, break-even, dynamic trail)
                    should_exit, reason = evaluate_exit(pos, price, pattern_lookup)
                    if should_exit:
                        exit_reason = reason

                    # MACD flip (fetch fresh 1h candles)
                    if exit_reason is None:
                        df = fetch_ohlcv(exchange, rl, sym, limit=60)
                        if df is not None and check_macd_exit(df):
                            exit_reason = 'MACD_FLIP_NEGATIVE'

                    if exit_reason:
                        try:
                            fill = execute_sell(exchange, rl, sym, pos, price)
                        except Exception as e:
                            log(f"[Exit] SELL ORDER FAILED for {sym}: {e}. "
                                f"Will retry next cycle.")
                            continue

                        pnl_usd = fill['pnl_usd']
                        ep      = fill['fill_price']

                        state['positions'].pop(sym)
                        state['cash']             += fill['proceeds_usd']
                        state['total_pnl_closed'] += pnl_usd
                        if pnl_usd > 0:
                            state['total_trades_won'] += 1

                        log(f"!!! SELL {sym} ({exit_reason}) | "
                            f"Fill: ${ep:,.4f} | "
                            f"P&L: ${pnl_usd:+,.2f} ({fill['pnl_pct']:+.2f}%) | "
                            f"Cash: ${state['cash']:,.2f}")

                        cooldowns[sym] = now_ts() + COOLDOWN_SECS
                        watchlist.remove(sym)

                        append_audit({
                            'timestamp':        datetime.now(timezone.utc).strftime(
                                '%Y-%m-%d %H:%M:%S.%f'),
                            'action':           'SELL',
                            'symbol':           sym,
                            'signal_price':     round(price, 6),
                            'exec_price':       round(ep, 6),
                            'slippage_pct':     round(fill['slippage_pct'], 4),
                            'conviction':       pos.get('conviction', 0),
                            'regime':           pos.get('regime', ''),
                            'vol_ratio':        pos.get('vol_ratio', 0.0),
                            'macd_hist':        pos.get('curr_hist', 0.0),
                            'size_usd':         round(pos['size_usd'], 2),
                            'source':           pos.get('source', ''),
                            'reason':           exit_reason,
                            'pnl_usd':          round(pnl_usd, 2),
                            'pnl_pct':          round(fill['pnl_pct'], 4),
                            'peak_price':       round(pos.get('peak_price', ep), 6),
                            'ever_green':       pos.get('ever_green', False),
                            'order_id':         fill['order_id'],
                            'pattern_modifier': get_trail_modifier(sym, pattern_lookup),
                            'equity':           round(state['cash'] + sum(
                                p['size_usd'] * (prices.get(s, p['entry_price'])
                                                 / p['entry_price'])
                                for s, p in state['positions'].items()), 2),
                            'cash':             round(state['cash'], 2),
                            'open_positions':   len(state['positions']),
                        }, audit_rows)

                        save_state(state, cooldowns)

            # ── WATCHLIST SCAN (every 60s) ────────────────────
            if now_ts() - last_watchlist_scan >= WATCHLIST_INTERVAL:
                last_watchlist_scan = now_ts()

                for sym in watchlist.symbols():
                    if sym in state['positions']:
                        watchlist.remove(sym)
                        continue
                    if cooldowns.get(sym, 0) > now_ts():
                        continue
                    if len(state['positions']) >= MAX_POSITIONS:
                        break

                    df = fetch_ohlcv(exchange, rl, sym, limit=100)
                    if df is None:
                        continue

                    watchlist.update(sym, df, state['positions'])

                    fired, conviction, detail = check_entry_signal(df)
                    if not fired or conviction < MIN_CONVICTION:
                        continue

                    deployed      = sum(p['size_usd']
                                        for p in state['positions'].values())
                    total_capital = state['cash'] + deployed
                    size          = calc_position_size(
                        conviction, total_capital, state['positions'])
                    if size < MIN_POSITION_USD:
                        log(f"[Entry] {sym} skipped — insufficient capital "
                            f"(available size: ${size:.2f})")
                        continue

                    # Fetch live price for order
                    try:
                        t     = rl.call(exchange.fetch_ticker, sym)
                        price = safe_float(t['last']) if t else 0.0
                    except Exception as e:
                        log(f"[Entry] {sym} price fetch failed: {e}")
                        continue

                    if price <= 0:
                        continue

                    # Place real buy order
                    try:
                        fill = execute_buy(exchange, rl, sym, size, price)
                    except Exception as e:
                        log(f"[Entry] BUY ORDER FAILED for {sym}: {e}")
                        continue

                    ep = fill['fill_price']

                    state['positions'][sym] = {
                        'entry_price':  ep,
                        'signal_price': detail.get('signal_price', price),
                        'peak_price':   ep,
                        'size_usd':     size,
                        'base_amount':  fill['base_amount'],
                        'cost_usd':     fill['cost_usd'],
                        'conviction':   conviction,
                        'open_ts':      now_ts(),
                        'regime':       detail.get('regime', 'NEUTRAL'),
                        'vol_ratio':    detail.get('vol_ratio', 0.0),
                        'curr_hist':    detail.get('curr_hist', 0.0),
                        'ever_green':   False,
                        'symbol':       sym,
                        'source':       'WATCHLIST',
                    }
                    state['cash']        -= fill['cost_usd']
                    state['trade_count'] += 1

                    log(f"!!! BUY {sym} [WATCHLIST] | "
                        f"Fill: ${ep:,.4f} | "
                        f"Size: ${size:,.2f} | "
                        f"Units: {fill['base_amount']} | "
                        f"Conviction: {conviction} | "
                        f"Vol: {detail.get('vol_ratio', 0):.2f}x | "
                        f"Regime: {detail.get('regime')}")

                    watchlist.remove(sym)

                    append_audit({
                        'timestamp':      datetime.now(timezone.utc).strftime(
                            '%Y-%m-%d %H:%M:%S.%f'),
                        'action':         'BUY',
                        'symbol':         sym,
                        'signal_price':   round(detail.get('signal_price', price), 6),
                        'exec_price':     round(ep, 6),
                        'slippage_pct':   round(fill['slippage_pct'], 4),
                        'conviction':     conviction,
                        'regime':         detail.get('regime'),
                        'vol_ratio':      detail.get('vol_ratio', 0.0),
                        'macd_hist':      detail.get('curr_hist', 0.0),
                        'size_usd':       round(size, 2),
                        'base_amount':    fill['base_amount'],
                        'order_id':       fill['order_id'],
                        'source':         'WATCHLIST',
                        'pattern_modifier': get_trail_modifier(sym, pattern_lookup),
                        'equity':         round(state['cash'] + sum(
                            p['size_usd'] * (prices.get(s, p['entry_price'])
                                             / p['entry_price'])
                            for s, p in state['positions'].items()), 2),
                        'cash':           round(state['cash'], 2),
                        'open_positions': len(state['positions']),
                    }, audit_rows)

                    save_state(state, cooldowns)

            # ── COLD UNIVERSE SCAN (every 5min) ──────────────
            if now_ts() - last_cold_scan >= COLD_SCAN_INTERVAL:
                last_cold_scan = now_ts()
                symbols        = universe.refresh()

                for sym in symbols:
                    if sym in state['positions']:
                        continue
                    if watchlist.is_warm(sym):
                        continue
                    if cooldowns.get(sym, 0) > now_ts():
                        continue
                    if len(state['positions']) >= MAX_POSITIONS:
                        break

                    df = fetch_ohlcv(exchange, rl, sym, limit=100)
                    if df is None:
                        continue

                    watchlist.update(sym, df, state['positions'])

                    fired, conviction, detail = check_entry_signal(df)
                    if not fired or conviction < MIN_CONVICTION:
                        continue

                    deployed      = sum(p['size_usd']
                                        for p in state['positions'].values())
                    total_capital = state['cash'] + deployed
                    size          = calc_position_size(
                        conviction, total_capital, state['positions'])
                    if size < MIN_POSITION_USD:
                        log(f"[Entry] {sym} skipped — insufficient capital "
                            f"(available size: ${size:.2f})")
                        continue

                    try:
                        t     = rl.call(exchange.fetch_ticker, sym)
                        price = safe_float(t['last']) if t else 0.0
                    except Exception as e:
                        log(f"[Entry] {sym} price fetch failed: {e}")
                        continue

                    if price <= 0:
                        continue

                    try:
                        fill = execute_buy(exchange, rl, sym, size, price)
                    except Exception as e:
                        log(f"[Entry] BUY ORDER FAILED for {sym}: {e}")
                        continue

                    ep = fill['fill_price']

                    state['positions'][sym] = {
                        'entry_price':  ep,
                        'signal_price': detail.get('signal_price', price),
                        'peak_price':   ep,
                        'size_usd':     size,
                        'base_amount':  fill['base_amount'],
                        'cost_usd':     fill['cost_usd'],
                        'conviction':   conviction,
                        'open_ts':      now_ts(),
                        'regime':       detail.get('regime', 'NEUTRAL'),
                        'vol_ratio':    detail.get('vol_ratio', 0.0),
                        'curr_hist':    detail.get('curr_hist', 0.0),
                        'ever_green':   False,
                        'symbol':       sym,
                        'source':       'COLD_SCAN',
                    }
                    state['cash']        -= fill['cost_usd']
                    state['trade_count'] += 1

                    log(f"!!! BUY {sym} [COLD] | "
                        f"Fill: ${ep:,.4f} | "
                        f"Size: ${size:,.2f} | "
                        f"Units: {fill['base_amount']} | "
                        f"Conviction: {conviction} | "
                        f"Vol: {detail.get('vol_ratio', 0):.2f}x | "
                        f"Regime: {detail.get('regime')}")

                    append_audit({
                        'timestamp':      datetime.now(timezone.utc).strftime(
                            '%Y-%m-%d %H:%M:%S.%f'),
                        'action':         'BUY',
                        'symbol':         sym,
                        'signal_price':   round(detail.get('signal_price', price), 6),
                        'exec_price':     round(ep, 6),
                        'slippage_pct':   round(fill['slippage_pct'], 4),
                        'conviction':     conviction,
                        'regime':         detail.get('regime'),
                        'vol_ratio':      detail.get('vol_ratio', 0.0),
                        'macd_hist':      detail.get('curr_hist', 0.0),
                        'size_usd':       round(size, 2),
                        'base_amount':    fill['base_amount'],
                        'order_id':       fill['order_id'],
                        'source':         'COLD_SCAN',
                        'pattern_modifier': get_trail_modifier(sym, pattern_lookup),
                        'equity':         round(state['cash'] + sum(
                            p['size_usd'] * (prices.get(s, p['entry_price'])
                                             / p['entry_price'])
                            for s, p in state['positions'].items()), 2),
                        'cash':           round(state['cash'], 2),
                        'open_positions': len(state['positions']),
                    }, audit_rows)

                    save_state(state, cooldowns)

            # ── PATTERN ENGINE REBUILD (every 6h) ─────────────
            if now_ts() - last_pattern_run >= PATTERN_REBUILD:
                last_pattern_run = now_ts()
                run_pattern_engine_background(universe.universe)

            pattern_lookup = load_pattern_lookup()

            # ── HEARTBEAT (every 5min) ────────────────────────
            if now_ts() - last_heartbeat >= HEARTBEAT_INTERVAL:
                last_heartbeat             = now_ts()
                state['last_heartbeat_ts'] = now_ts()
                print_heartbeat(state, prices, cooldowns,
                                watchlist.symbols(), pattern_lookup,
                                exchange, rl)
                save_state(state, cooldowns)

            # ── Sleep remainder of loop ───────────────────────
            elapsed   = now_ts() - loop_start
            sleep_for = max(0, EXIT_INTERVAL - elapsed)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        log("[Main] KeyboardInterrupt — shutting down gracefully.")
        log("[Main] WARNING: Any open positions remain open on the exchange.")
        log("[Main] Check Kraken dashboard before restarting.")
        save_state(state, cooldowns)
        log("[Main] State saved. Goodbye.")


if __name__ == '__main__':
    main()