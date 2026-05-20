"""
kraken_v8_1.py — MACD Momentum Scanner V8.1
$100,000 virtual capital | Shadow/paper mode | eu-west-1 Ireland

═══════════════════════════════════════════════════════════════════
WHAT'S NEW IN V8.1 vs V8
═══════════════════════════════════════════════════════════════════

1. EXIT LOOP: 60s → 30s (faster position protection)

2. TWO-TIER ENTRY SCANNER:
   - Watchlist (warm assets — MACD shrinking toward zero):
     scanned every 60 seconds
   - Cold universe (everything else): scanned every 5 minutes
   - Assets promoted to watchlist when MACD histogram is negative
     but shrinking and volume is rising — close to signal

3. BREAK-EVEN FLOOR:
   The moment any trade shows positive P&L, hard stop rises to
   entry price. You can never lose money on a green trade. Ever.

4. NEVER-GREEN PAIN THRESHOLD (failed signal cut):
   If trade has never been green AND loss exceeds threshold → exit:
     $50K+ position  → cut at -$500
     $15K-$50K       → cut at -$250
     $5K-$15K        → cut at -$150
     Under $5K       → cut at -$100

5. CHOP DETECTION (was-green-now-deep-negative):
   Same thresholds. Even if it briefly touched profit, if it falls
   back through the pain threshold the move is dead. Cut it.

6. DYNAMIC CAPITAL-SCALED TRAILING STOP (from V8):
   Trail tightness scales with position size (dollar value of 1%):
     1% worth $1,000+ → keep 90% of peak profit
     1% worth $600+   → keep 88%
     1% worth $300+   → keep 85%
     1% worth $100+   → keep 75%
     1% worth <$100   → keep 65%

7. PATTERN ENGINE INTEGRATION:
   Reads pattern_lookup.json (built by pattern_engine.py at boot
   and every 6h). Trail modifier adjusts tightness based on how
   similar historical setups played out:
     70%+ went up  → trail_modifier = 1.20 (loosen, let it run)
     50-70%        → trail_modifier = 1.00 (standard)
     under 50%     → trail_modifier = 0.80 (tighten, protect)
   Failsafes always override pattern engine.

═══════════════════════════════════════════════════════════════════
ENTRY SIGNAL (ALL required on iloc[-2])
═══════════════════════════════════════════════════════════════════
  1. MACD(12,26,9) histogram crossed negative → positive
  2. Volume > 1.5× 20-bar rolling average
  3. Regime != BEAR
  4. Not in cooldown, not already held
  5. Conviction score >= 40

═══════════════════════════════════════════════════════════════════
EXIT PRIORITY LADDER (first trigger wins)
═══════════════════════════════════════════════════════════════════
  0a. Never-green pain threshold (failed signal)
  0b. Chop detection (was green, gave it all back)
  1.  Break-even floor (stop never below entry once green)
  2.  Dynamic capital-scaled trail (pattern-engine modified)
  3.  MACD histogram flips negative
  4.  5% hard stop (absolute, pattern engine cannot override)
  5.  15% portfolio kill switch

═══════════════════════════════════════════════════════════════════
UNIVERSAL FAILSAFES (pattern engine cannot override)
═══════════════════════════════════════════════════════════════════
  · 5% hard stop per position — always active
  · Break-even floor — once green, stop never below entry
  · Pain threshold cuts — never-green and chop detection
  · 15% portfolio drawdown kill switch — closes all, stops bot
  · 2h cooldown after any exit

Run:   tmux new -s kraken_v8 → python3 kraken_v8_1.py
State: kraken_v8_1_state.json (atomic writes)
Audit: kraken_v8_1_audit_trail.csv
Log:   kraken_v8_1_events.log
"""

import ccxt
import pandas as pd
import numpy as np
import time
import os
import json
import math
import subprocess
import threading
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────────────────────
BASE_DIR       = os.path.dirname(os.path.abspath(__file__))
STATE_FILE     = os.path.join(BASE_DIR, 'kraken_v8_1_state.json')
AUDIT_FILE     = os.path.join(BASE_DIR, 'kraken_v8_1_audit_trail.csv')
EVENT_FILE     = os.path.join(BASE_DIR, 'kraken_v8_1_events.log')
PATTERN_FILE   = os.path.join(BASE_DIR, 'pattern_lookup.json')
WATCHLIST_FILE = os.path.join(BASE_DIR, 'watchlist.json')

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
PAPER_MODE           = True
STARTING_CAPITAL     = 100_000.0
MAX_POSITIONS        = 5
DRY_POWDER_PCT       = 0.20
HARD_STOP_PCT        = 0.05
MAX_DD_PCT           = 0.15
SLIPPAGE             = 0.0010
MIN_VOL_24H_USD      = 5_000_000
MIN_HISTORY_BARS     = 60
UNIVERSE_SIZE        = 30
VOL_SPIKE_MULT       = 1.5
SIZE_HIGH_PCT        = 0.15
SIZE_LOW_PCT         = 0.10
MIN_CONVICTION       = 40
COOLDOWN_SECS        = 7200
WATCHLIST_TTL_SECS   = 7200       # 2h max on watchlist before stale

# Loop timings
EXIT_INTERVAL        = 30         # exit check every 30s
WATCHLIST_INTERVAL   = 60         # warm assets scanned every 60s
COLD_SCAN_INTERVAL   = 300        # cold universe scanned every 5min
HEARTBEAT_INTERVAL   = 300        # heartbeat every 5min
UNIVERSE_REFRESH     = 3600       # universe rebuilt every 1h
PATTERN_REBUILD      = 21600      # pattern engine rebuilt every 6h

# Pain thresholds — never-green cut and chop detection
PAIN_THRESHOLDS = [
    (50_000, 500),    # $50K+ position → cut at -$500
    (15_000, 250),    # $15K-$50K      → cut at -$250
    (5_000,  150),    # $5K-$15K       → cut at -$150
    (0,      100),    # Under $5K      → cut at -$100
]

PAIRS = [
    'BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'TAO/USD',
    'DOGE/USD', 'HYPE/USD', 'SUI/USD', 'ADA/USD', 'ZEC/USD'
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

def save_state(state, cooldowns, watchlist):
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump({**state,
                   'cooldowns': cooldowns,
                   'last_heartbeat_ts': now_ts()}, f, indent=2)
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

def get_pain_threshold(size_usd):
    for min_size, threshold in PAIN_THRESHOLDS:
        if size_usd >= min_size:
            return threshold
    return 100

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
    last_c, last_e21, last_e55 = closes.iloc[-1], ema21.iloc[-1], ema55.iloc[-1]
    if last_e21 > last_e55 and last_c > last_e21:
        return 'BULL'
    if last_e21 < last_e55:
        return 'BEAR'
    return 'NEUTRAL'

def calc_rsi(closes, period=14):
    delta = closes.diff()
    gain  = delta.clip(lower=0).ewm(com=period-1, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period-1, adjust=False).mean()
    return safe_float((100 - 100 / (1 + gain / (loss + 1e-9))).iloc[-1], 50)

def calc_conviction(hist_val, vol_ratio, regime, rsi_val):
    score = 0
    if hist_val > 0:           score += 30
    if vol_ratio >= 2.5:       score += 30
    elif vol_ratio >= 2.0:     score += 25
    elif vol_ratio >= 1.5:     score += 20
    else:                      score += 10
    if regime == 'BULL':       score += 25
    elif regime == 'NEUTRAL':  score += 15
    if rsi_val < 40:           score += 15
    elif rsi_val < 50:         score += 10
    elif rsi_val < 60:         score += 5
    return min(score, 100)

def fetch_ohlcv(exchange, rl, symbol, limit=100):
    try:
        raw = rl.call(exchange.fetch_ohlcv, symbol, '1h', None, limit)
        if raw is None or len(raw) < MIN_HISTORY_BARS:
            return None
        df = pd.DataFrame(raw, columns=['ts','o','h','l','c','v'])
        return df
    except Exception as e:
        log(f"[OHLCV] {symbol}: {e}")
        return None

# ─────────────────────────────────────────────────────────────
# WATCHLIST MANAGER
# ─────────────────────────────────────────────────────────────
class WatchlistManager:
    """
    Maintains a set of warm assets — those close to firing.
    Promoted when MACD histogram is negative but shrinking toward zero
    and volume is rising. Demoted when signal fires, MACD expands
    negative, or TTL expires.
    """
    def __init__(self):
        self._list  = {}   # symbol → promoted_ts

    def update(self, symbol, df, positions):
        if symbol in positions:
            self._list.pop(symbol, None)
            return

        hist     = calc_macd_histogram(df['c'])
        curr     = safe_float(hist.iloc[-2])
        prev     = safe_float(hist.iloc[-3])
        vol_avg  = df['v'].rolling(20).mean().iloc[-2]
        vol_curr = safe_float(df['v'].iloc[-2])
        vol_ratio = vol_curr / (vol_avg + 1e-9)
        regime    = calc_regime(df['c'])

        # Promotion condition
        is_warm = (
            curr < 0 and              # histogram still negative
            abs(curr) < abs(prev) and  # but shrinking toward zero
            vol_ratio > 1.0 and        # volume picking up
            regime != 'BEAR'
        )

        if is_warm:
            if symbol not in self._list:
                self._list[symbol] = now_ts()
                log(f"[Watchlist] +{symbol} promoted (hist={curr:.6f}, vol={vol_ratio:.2f}x)")
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
    Uses iloc[-2] — last fully closed 1h candle (no look-ahead).
    """
    if len(df) < MIN_HISTORY_BARS:
        return False, 0, {'reason': 'insufficient data'}

    hist      = calc_macd_histogram(df['c'])
    curr_hist = safe_float(hist.iloc[-2])
    prev_hist = safe_float(hist.iloc[-3])
    regime    = calc_regime(df['c'])

    if not (curr_hist > 0 and prev_hist <= 0):
        return False, 0, {'reason': 'no MACD flip',
                          'curr': round(curr_hist,6), 'prev': round(prev_hist,6)}

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

def is_watchlist_candidate(df):
    """Returns True if asset should be on watchlist — MACD shrinking toward zero."""
    if df is None or len(df) < 30:
        return False
    hist     = calc_macd_histogram(df['c'])
    curr     = safe_float(hist.iloc[-2])
    prev     = safe_float(hist.iloc[-3])
    vol_avg  = df['v'].rolling(20).mean().iloc[-2]
    vol_curr = safe_float(df['v'].iloc[-2])
    vol_ratio = vol_curr / (vol_avg + 1e-9)
    regime    = calc_regime(df['c'])
    return (curr < 0 and abs(curr) < abs(prev) and
            vol_ratio > 1.0 and regime != 'BEAR')

# ─────────────────────────────────────────────────────────────
# EXIT EVALUATION
# ─────────────────────────────────────────────────────────────
def evaluate_exit(pos, current_price, pattern_lookup):
    """
    Full exit ladder. Returns (should_exit, reason).

    Priority:
      0a. Never-green pain threshold
      0b. Chop detection (was-green-now-deep-negative)
      1.  Break-even floor
      2.  Dynamic capital-scaled trail (pattern modified)
      3.  MACD flip (checked by caller)
      4.  5% hard stop
    """
    entry      = pos['entry_price']
    peak       = pos.get('peak_price', entry)
    size       = pos['size_usd']
    ever_green = pos.get('ever_green', False)

    peak = max(peak, current_price)   # update peak

    pnl_pct     = (current_price - entry) / entry
    current_pnl = size * pnl_pct
    peak_pnl    = size * ((peak - entry) / entry)

    # Mark ever-green
    if current_pnl > 0 and not ever_green:
        pos['ever_green'] = True
        ever_green        = True

    pain_threshold = get_pain_threshold(size)

    # ── 0a. Never-green pain threshold ───────────────────────
    if not ever_green and current_pnl <= -pain_threshold:
        return True, (f'FAILED_SIGNAL_CUT '
                      f'(never green, loss=${abs(current_pnl):.0f} '
                      f'> threshold=${pain_threshold})')

    # ── 0b. Chop detection ────────────────────────────────────
    if ever_green and current_pnl <= -pain_threshold:
        return True, (f'CHOP_DETECTED '
                      f'(was green, now loss=${abs(current_pnl):.0f} '
                      f'> threshold=${pain_threshold})')

    # ── 1. Break-even floor ───────────────────────────────────
    if ever_green and current_price < entry:
        return True, f'BREAK_EVEN_FLOOR (stop=entry=${entry:.4f})'

    # ── 4. Hard stop (check before trail — no exceptions) ────
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

    # Apply pattern engine modifier
    sym      = pos.get('symbol', '')
    modifier = get_trail_modifier(sym, pattern_lookup)
    modifier = max(0.70, min(1.30, modifier))   # clamp to ±30%

    # Modifier loosens trail by increasing keep_pct, tightens by decreasing
    # modifier > 1 means "let it run more" → keep less (lower floor)
    # modifier < 1 means "protect now"     → keep more (higher floor)
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
    deployed   = sum(p['size_usd'] for p in positions.values())
    max_deploy = total_capital * (1 - DRY_POWDER_PCT)
    available  = max(0.0, max_deploy - deployed)
    if available < 500:
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
        log("[Universe] Refreshing top-30 liquid pairs...")
        try:
            tickers = self.rl.call(self.exchange.fetch_tickers)
            if tickers is None:
                return list(self.universe)
            candidates = []
            for symbol, t in tickers.items():
                if not symbol.endswith('/USD'):
                    continue
                if symbol in ('USDT/USD','USDC/USD','DAI/USD','BUSD/USD','EUR/USD'):
                    continue
                vol = t.get('quoteVolume') or 0
                if vol >= MIN_VOL_24H_USD:
                    candidates.append((symbol, vol))
            candidates.sort(key=lambda x: x[1], reverse=True)
            self.universe   = [s for s,_ in candidates[:UNIVERSE_SIZE]]
            self.last_built = now_ts()
            log(f"[Universe] {len(self.universe)} pairs: "
                f"{', '.join(self.universe[:10])}{'...' if len(self.universe)>10 else ''}")
        except Exception as e:
            log(f"[Universe] Error: {e}")
        return list(self.universe)

# ─────────────────────────────────────────────────────────────
# PATTERN ENGINE RUNNER
# ─────────────────────────────────────────────────────────────
def run_pattern_engine_background(pairs=None):
    """Launch pattern_engine.py as a background subprocess."""
    engine_path = os.path.join(BASE_DIR, 'pattern_engine.py')
    if not os.path.exists(engine_path):
        log("[PatternEngine] pattern_engine.py not found — skipping")
        return
    args = ['python3', engine_path]
    if pairs:
        args.extend(pairs)
    try:
        subprocess.Popen(args, stdout=subprocess.DEVNULL,
                         stderr=subprocess.DEVNULL,
                         cwd=BASE_DIR)
        log(f"[PatternEngine] Launched background rebuild")
    except Exception as e:
        log(f"[PatternEngine] Launch failed: {e}")

# ─────────────────────────────────────────────────────────────
# HEARTBEAT
# ─────────────────────────────────────────────────────────────
def print_heartbeat(state, prices, cooldowns, watchlist_syms, pattern_lookup):
    cash      = state['cash']
    positions = state['positions']
    pos_val   = sum(
        p['size_usd'] * (prices.get(s, p['entry_price']) / p['entry_price'])
        for s, p in positions.items()
    )
    equity    = cash + pos_val
    max_eq    = state.get('max_equity', equity)
    dd        = (max_eq - equity) / max_eq if max_eq > 0 else 0
    profit    = equity - STARTING_CAPITAL
    pp        = profit / STARTING_CAPITAL * 100
    trades    = state.get('trade_count', 0)
    wins      = state.get('total_trades_won', 0)
    wr        = (wins/trades*100) if trades > 0 else 0.0
    closed    = state.get('total_pnl_closed', 0.0)
    gross_rt  = fmt_dur(now_ts() - state.get('first_start_ts', now_ts()))
    paused    = fmt_dur(state.get('total_paused_secs', 0))
    net_rt    = fmt_dur(now_ts() - state.get('first_start_ts', now_ts())
                        - state.get('total_paused_secs', 0))

    print("\n" + "="*72)
    print(f" KRAKEN V8.1 HEARTBEAT | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("─"*72)
    print(f" Portfolio Equity:   ${equity:>12,.2f}   (start: ${STARTING_CAPITAL:,.2f})")
    print(f" Total P&L:          ${profit:>+12,.2f}   ({pp:+.3f}%)")
    print(f" Closed P&L:         ${closed:>+12,.2f}")
    print(f" Cash Available:     ${cash:>12,.2f}")
    print(f" Max Drawdown:       {dd*100:>11.2f}%   (limit: {MAX_DD_PCT*100:.0f}%)")
    print(f" Trades: {trades}  |  Won: {wins}  |  Win Rate: {wr:.1f}%")
    print("─"*72)

    if positions:
        print(f" {'SYMBOL':<12} {'SIZE':>9} {'ENTRY':>10} {'CURR':>10} "
              f"{'P&L$':>9} {'PEAK$':>9} {'EVER_G':>6} {'MODIFIER':>9}")
        for sym, pos in positions.items():
            cur      = prices.get(sym, pos['entry_price'])
            g_usd    = pos['size_usd'] * (cur - pos['entry_price']) / pos['entry_price']
            peak_usd = pos['size_usd'] * (pos.get('peak_price', pos['entry_price'])
                                          - pos['entry_price']) / pos['entry_price']
            eg       = 'Y' if pos.get('ever_green') else 'N'
            mod      = get_trail_modifier(sym, pattern_lookup)
            print(f" {sym:<12} ${pos['size_usd']:>8,.0f} "
                  f"${pos['entry_price']:>9,.4f} "
                  f"${cur:>9,.4f} "
                  f"${g_usd:>+8,.2f} "
                  f"${peak_usd:>8,.2f} "
                  f"{eg:>6} "
                  f"{mod:>9.2f}x")
    else:
        print(" Positions:          FLAT — scanning for entries")

    if watchlist_syms:
        print("─"*72)
        print(f" Watchlist ({len(watchlist_syms)}): {', '.join(watchlist_syms)}")

    active_cds = {s:t for s,t in cooldowns.items() if now_ts() < t}
    if active_cds:
        print("─"*72)
        print(f" Cooling down ({len(active_cds)}): " +
              ", ".join(f"{s} until {datetime.fromtimestamp(t).strftime('%H:%M')}"
                        for s,t in active_cds.items()))

    print("─"*72)
    print(f" Dry Powder:         {DRY_POWDER_PCT*100:.0f}% reserve")
    print(f" Gross Runtime:      {gross_rt}")
    print(f" Total Paused:       {paused}")
    print(f" Net Runtime:        {net_rt}")
    print("="*72 + "\n")

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    exchange  = ccxt.kraken({'enableRateLimit': False})
    rl        = RateLimiter()
    universe  = UniverseManager(exchange, rl)
    watchlist = WatchlistManager()

    # ── Boot ─────────────────────────────────────────────────
    log("--- KRAKEN V8.1: MACD FLIP | DYNAMIC TRAIL | PATTERN ENGINE | "
        "PAIN THRESHOLD | BREAK-EVEN FLOOR | WATCHLIST | $100K VIRTUAL ---")
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

    # ── Load state ────────────────────────────────────────────
    saved, cooldowns = load_state()
    if saved:
        gap = now_ts() - saved.get('last_heartbeat_ts', now_ts())
        state = saved
        state['total_paused_secs'] = state.get('total_paused_secs', 0) + (
            gap if gap > 300 else 0)
        state['session_start_ts'] = now_ts()
        log(f">>> V8.1 RESTARTED | Gap: {fmt_dur(gap)} | "
            f"Cash: ${state['cash']:,.2f} | "
            f"Positions: {len(state['positions'])} | "
            f"Trades: {state['trade_count']}")
    else:
        log(f">>> V8.1 FIRST START — fresh ${STARTING_CAPITAL:,.0f} virtual account.")
        state = {
            'cash':              STARTING_CAPITAL,
            'max_equity':        STARTING_CAPITAL,
            'positions':         {},
            'trade_count':       0,
            'total_trades_won':  0,
            'total_pnl_closed':  0.0,
            'first_start_ts':    now_ts(),
            'total_paused_secs': 0.0,
            'session_start_ts':  now_ts(),
            'last_heartbeat_ts': now_ts(),
        }

    audit_rows = []
    if os.path.exists(AUDIT_FILE):
        try:
            audit_rows = pd.read_csv(AUDIT_FILE).to_dict('records')
        except Exception:
            audit_rows = []

    universe.refresh(force=True)

    # ── Launch pattern engine at boot ────────────────────────
    run_pattern_engine_background(universe.universe)
    pattern_lookup    = load_pattern_lookup()
    last_pattern_run  = now_ts()

    save_state(state, cooldowns, watchlist.symbols())
    log("[Boot] Boot sequence complete.")

    # ── Timestamp gates ───────────────────────────────────────
    last_exit_check    = 0.0
    last_watchlist_scan = 0.0
    last_cold_scan     = 0.0
    last_heartbeat     = 0.0

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
            equity             = state['cash'] + pos_val
            state['max_equity'] = max(state.get('max_equity', equity), equity)
            dd = ((state['max_equity'] - equity) / state['max_equity']
                  if state['max_equity'] > 0 else 0)

            if dd >= MAX_DD_PCT:
                log(f"CRITICAL: {MAX_DD_PCT*100:.0f}% KILL SWITCH — closing all.")
                for sym in list(state['positions'].keys()):
                    price = prices.get(sym, state['positions'][sym]['entry_price'])
                    pos   = state['positions'].pop(sym)
                    ep    = price * (1 - SLIPPAGE)
                    pnl   = pos['size_usd'] * ((ep - pos['entry_price']) / pos['entry_price'])
                    state['cash']             += pos['size_usd'] * (1 + (ep - pos['entry_price']) / pos['entry_price'])
                    state['total_pnl_closed'] += pnl
                    log(f"!!! SELL {sym} (KILL_SWITCH) @ ${ep:,.4f} | P&L: ${pnl:+,.2f}")
                    append_audit({
                        'timestamp': datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
                        'action': 'SELL', 'symbol': sym, 'price': round(ep,6),
                        'reason': 'KILL_SWITCH', 'pnl_usd': round(pnl,2),
                        'equity': round(equity,2), 'cash': round(state['cash'],2),
                    }, audit_rows)
                save_state(state, cooldowns, watchlist.symbols())
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

                    pos['symbol']    = sym
                    pos['peak_price'] = max(pos.get('peak_price', pos['entry_price']), price)
                    if price > pos['entry_price'] and not pos.get('ever_green'):
                        pos['ever_green'] = True

                    exit_reason = None

                    # Tiered exit (pain threshold, BE floor, dynamic trail)
                    should_exit, reason = evaluate_exit(pos, price, pattern_lookup)
                    if should_exit:
                        exit_reason = reason

                    # MACD flip exit (fetch fresh 1h data)
                    if exit_reason is None:
                        df = fetch_ohlcv(exchange, rl, sym, limit=60)
                        if df is not None and check_macd_exit(df):
                            exit_reason = 'MACD_FLIP_NEGATIVE'

                    if exit_reason:
                        ep     = price * (1 - SLIPPAGE)
                        pnl_pct = (ep - pos['entry_price']) / pos['entry_price']
                        pnl_usd = pos['size_usd'] * pnl_pct
                        close_v = pos['size_usd'] * (1 + pnl_pct)

                        state['positions'].pop(sym)
                        state['cash']             += close_v
                        state['total_pnl_closed'] += pnl_usd
                        if pnl_usd > 0:
                            state['total_trades_won'] += 1

                        log(f"!!! SELL {sym} ({exit_reason}) @ ${ep:,.4f} | "
                            f"P&L: ${pnl_usd:+,.2f} ({pnl_pct*100:+.2f}%) | "
                            f"Cash: ${state['cash']:,.2f}")

                        cooldowns[sym] = now_ts() + COOLDOWN_SECS
                        log(f"[Cooldown] {sym} locked 2h")

                        watchlist.remove(sym)

                        append_audit({
                            'timestamp':    datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
                            'action':       'SELL',
                            'symbol':       sym,
                            'price':        round(ep, 6),
                            'reason':       exit_reason,
                            'pnl_usd':      round(pnl_usd, 2),
                            'pnl_pct':      round(pnl_pct*100, 4),
                            'peak_price':   round(pos.get('peak_price', ep), 6),
                            'ever_green':   pos.get('ever_green', False),
                            'equity':       round(state['cash'] + sum(
                                p['size_usd']*(prices.get(s,p['entry_price'])/p['entry_price'])
                                for s,p in state['positions'].items()), 2),
                            'cash':         round(state['cash'], 2),
                            'open_positions': len(state['positions']),
                            'pattern_modifier': get_trail_modifier(sym, pattern_lookup),
                        }, audit_rows)

                        save_state(state, cooldowns, watchlist.symbols())

            # ── WATCHLIST SCAN (every 60s) ────────────────────
            if now_ts() - last_watchlist_scan >= WATCHLIST_INTERVAL:
                last_watchlist_scan = now_ts()

                warm_symbols = watchlist.symbols()
                for sym in warm_symbols:
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

                    # Update watchlist status
                    watchlist.update(sym, df, state['positions'])

                    fired, conviction, detail = check_entry_signal(df)
                    if not fired or conviction < MIN_CONVICTION:
                        continue

                    deployed      = sum(p['size_usd'] for p in state['positions'].values())
                    total_capital = state['cash'] + deployed
                    size          = calc_position_size(conviction, total_capital, state['positions'])
                    if size < 100:
                        continue

                    try:
                        t = rl.call(exchange.fetch_ticker, sym)
                        if t is None:
                            continue
                        price = safe_float(t['last'])
                    except Exception as e:
                        log(f"[Entry] {sym} price error: {e}")
                        continue

                    if price <= 0:
                        continue

                    ep = price * (1 + SLIPPAGE)
                    state['positions'][sym] = {
                        'entry_price':  ep,
                        'signal_price': detail.get('signal_price', price),
                        'peak_price':   ep,
                        'size_usd':     size,
                        'conviction':   conviction,
                        'open_ts':      now_ts(),
                        'regime':       detail.get('regime','NEUTRAL'),
                        'vol_ratio':    detail.get('vol_ratio', 0.0),
                        'curr_hist':    detail.get('curr_hist', 0.0),
                        'ever_green':   False,
                        'symbol':       sym,
                    }
                    state['cash']        -= size
                    state['trade_count'] += 1

                    log(f"!!! BUY {sym} [WATCHLIST] @ ${ep:,.4f} | "
                        f"Size: ${size:,.2f} | Conviction: {conviction} | "
                        f"Vol: {detail.get('vol_ratio',0):.2f}x | "
                        f"Regime: {detail.get('regime')}")

                    watchlist.remove(sym)

                    pattern_mod = get_trail_modifier(sym, pattern_lookup)
                    append_audit({
                        'timestamp':      datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
                        'action':         'BUY',
                        'symbol':         sym,
                        'price':          round(ep, 6),
                        'signal_price':   round(detail.get('signal_price', price), 6),
                        'conviction':     conviction,
                        'regime':         detail.get('regime'),
                        'vol_ratio':      detail.get('vol_ratio', 0.0),
                        'macd_hist':      detail.get('curr_hist', 0.0),
                        'size_usd':       round(size, 2),
                        'source':         'WATCHLIST',
                        'pattern_modifier': pattern_mod,
                        'equity':         round(state['cash'] + sum(
                            p['size_usd']*(prices.get(s,p['entry_price'])/p['entry_price'])
                            for s,p in state['positions'].items()), 2),
                        'cash':           round(state['cash'], 2),
                        'open_positions': len(state['positions']),
                    }, audit_rows)

                    save_state(state, cooldowns, watchlist.symbols())

            # ── COLD UNIVERSE SCAN (every 5min) ──────────────
            if now_ts() - last_cold_scan >= COLD_SCAN_INTERVAL:
                last_cold_scan = now_ts()

                symbols = universe.refresh()

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

                    # Check if this should be on watchlist
                    watchlist.update(sym, df, state['positions'])

                    # Check for immediate entry signal
                    fired, conviction, detail = check_entry_signal(df)
                    if not fired or conviction < MIN_CONVICTION:
                        continue

                    deployed      = sum(p['size_usd'] for p in state['positions'].values())
                    total_capital = state['cash'] + deployed
                    size          = calc_position_size(conviction, total_capital, state['positions'])
                    if size < 100:
                        continue

                    try:
                        t = rl.call(exchange.fetch_ticker, sym)
                        if t is None:
                            continue
                        price = safe_float(t['last'])
                    except Exception as e:
                        log(f"[Entry] {sym} price error: {e}")
                        continue

                    if price <= 0:
                        continue

                    ep = price * (1 + SLIPPAGE)
                    state['positions'][sym] = {
                        'entry_price':  ep,
                        'signal_price': detail.get('signal_price', price),
                        'peak_price':   ep,
                        'size_usd':     size,
                        'conviction':   conviction,
                        'open_ts':      now_ts(),
                        'regime':       detail.get('regime','NEUTRAL'),
                        'vol_ratio':    detail.get('vol_ratio', 0.0),
                        'curr_hist':    detail.get('curr_hist', 0.0),
                        'ever_green':   False,
                        'symbol':       sym,
                    }
                    state['cash']        -= size
                    state['trade_count'] += 1

                    log(f"!!! BUY {sym} [COLD] @ ${ep:,.4f} | "
                        f"Size: ${size:,.2f} | Conviction: {conviction} | "
                        f"Vol: {detail.get('vol_ratio',0):.2f}x | "
                        f"Regime: {detail.get('regime')}")

                    pattern_mod = get_trail_modifier(sym, pattern_lookup)
                    append_audit({
                        'timestamp':      datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
                        'action':         'BUY',
                        'symbol':         sym,
                        'price':          round(ep, 6),
                        'signal_price':   round(detail.get('signal_price', price), 6),
                        'conviction':     conviction,
                        'regime':         detail.get('regime'),
                        'vol_ratio':      detail.get('vol_ratio', 0.0),
                        'macd_hist':      detail.get('curr_hist', 0.0),
                        'size_usd':       round(size, 2),
                        'source':         'COLD_SCAN',
                        'pattern_modifier': pattern_mod,
                        'equity':         round(state['cash'] + sum(
                            p['size_usd']*(prices.get(s,p['entry_price'])/p['entry_price'])
                            for s,p in state['positions'].items()), 2),
                        'cash':           round(state['cash'], 2),
                        'open_positions': len(state['positions']),
                    }, audit_rows)

                    save_state(state, cooldowns, watchlist.symbols())

            # ── PATTERN ENGINE REBUILD (every 6h) ─────────────
            if now_ts() - last_pattern_run >= PATTERN_REBUILD:
                last_pattern_run = now_ts()
                run_pattern_engine_background(universe.universe)

            # Reload pattern lookup (picks up any fresh rebuild)
            pattern_lookup = load_pattern_lookup()

            # ── HEARTBEAT (every 5min) ────────────────────────
            if now_ts() - last_heartbeat >= HEARTBEAT_INTERVAL:
                last_heartbeat             = now_ts()
                state['last_heartbeat_ts'] = now_ts()
                print_heartbeat(state, prices, cooldowns,
                                watchlist.symbols(), pattern_lookup)
                save_state(state, cooldowns, watchlist.symbols())

            # ── Sleep remainder of loop ───────────────────────
            elapsed   = now_ts() - loop_start
            sleep_for = max(0, EXIT_INTERVAL - elapsed)
            time.sleep(sleep_for)

    except KeyboardInterrupt:
        log("[Main] KeyboardInterrupt — shutting down gracefully.")
        save_state(state, cooldowns, watchlist.symbols())
        log("[Main] State saved. Goodbye.")


if __name__ == '__main__':
    main()
