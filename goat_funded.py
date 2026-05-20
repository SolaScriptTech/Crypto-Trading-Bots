"""
goat_funded.py — GOAT Funded BTC/USD Live Bot
==============================================
The exact strategy that produced the backtest results in
backtest_v5_fixed_trades.csv / backtest_v5_fixed_equity.csv.

$100,000 virtual capital | BTC/USD | 1h candles | eu-west-1 Ireland

═══════════════════════════════════════════════════════════════════
REGIME  (hourly, on BTC + ETH 1h candles)
═══════════════════════════════════════════════════════════════════
  BULL    = btc_bull AND eth_bull AND ADX(14) > 20
            btc_bull = EMA21 > EMA55 AND price > EMA21
            eth_bull = EMA21 > EMA55
  BEAR    = btc EMA21 < EMA55  OR  eth EMA21 < EMA55
  NEUTRAL = everything else

═══════════════════════════════════════════════════════════════════
ENTRY  (flat position only, evaluated on last CLOSED candle iloc[-2])
═══════════════════════════════════════════════════════════════════
  BEAR   → no entry under any condition

  BULL   → any ONE of the following fires a BUY:
    A. BB_LOWER    : close < lower_band  (BB multiplier 1.5 in BULL)
    B. EMA21_CROSS : prev_close < prev_EMA21 AND close > EMA21
    C. EMA21_PULL  : close < EMA21 AND close >= EMA21*0.9925 AND RSI < 52
    D. SMA20_TOUCH : close < SMA20 AND RSI < 50
    E. RSI_OVERSOLD: RSI < 42 AND close > EMA55

    Idleness guard: flat > 8h in BULL → any of C/D/E fires regardless

  NEUTRAL → only:
    A. BB_LOWER (BB multiplier 2.0 in NEUTRAL)

═══════════════════════════════════════════════════════════════════
EXIT LADDER  (5-min sub-loop: trail + book; hourly: BB upper + regime)
═══════════════════════════════════════════════════════════════════
  Priority (first trigger wins):

  1. SELL_REGIME_FLIP  : regime flips to BEAR → immediate exit
  2. SELL_STOP         : price <= peak_price * (1 - 3.5%)  [hard stop]
  3. SELL_TRAIL        : price <= tiered trail stop (see below)
  4. SELL_TARGET       : price > BB upper band
                         (skipped if BULL and price < EMA21 * 1.01)

  Tiered trailing stop (V4.1 — proven in backtest):
    Peak gain < 0.3%   → 1.3% trail from peak
    Peak gain 0.3–0.7% → 0.8% trail from peak
    Peak gain 0.7–1.2% → 0.5% trail from peak
    Peak gain > 1.2%   → 0.3% trail from peak

  Profit floor (active once peak gain ever exceeds 0.3%):
    Stop price can NEVER drop below entry_price × 1.001
    A trade that went green cannot exit below breakeven.

═══════════════════════════════════════════════════════════════════
INFRASTRUCTURE
═══════════════════════════════════════════════════════════════════
  - Single process, timestamp-gated loop (no threads)
  - Atomic state.json writes (tmp → os.replace)
  - 15% portfolio kill switch
  - 10bps slippage model both sides
  - All API calls through RateLimiter (1.5s min gap, exp backoff)
  - NTP settle wait on boot
  - Legacy CSV bootstrap for equity continuity

Run:   tmux new -s goat → python3 goat_funded.py
State: goat_funded_state.json
Audit: goat_funded_audit.csv
Log:   goat_funded_events.log
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
# PATHS
# ─────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
STATE_FILE  = os.path.join(BASE_DIR, 'goat_funded_state.json')
AUDIT_FILE  = os.path.join(BASE_DIR, 'goat_funded_audit.csv')
EVENT_FILE  = os.path.join(BASE_DIR, 'goat_funded_events.log')

# Legacy CSVs to bootstrap equity continuity from prior runs
LEGACY_CSVS = [
    os.path.join(BASE_DIR, 'backtest_v5_fixed_equity.csv'),
]

# ─────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────
STARTING_CAPITAL  = 100_000.0
SLIPPAGE          = 0.0010       # 10bps per side
STOP_PCT          = 0.035        # 3.5% hard stop from peak
MAX_DD_PCT        = 0.15         # 15% portfolio kill switch

BB_MULT_BULL      = 1.5          # Bollinger Band multiplier in BULL
BB_MULT_NEUTRAL   = 2.0          # Bollinger Band multiplier in NEUTRAL

ADX_THRESHOLD     = 20           # minimum ADX to confirm BULL regime
IDLE_HOURS        = 8            # hours flat in BULL before idle guard fires

# Tiered trailing stop thresholds (peak gain → trail distance)
TRAIL_TIERS = [
    (0.012, 0.003),   # gain > 1.2% → 0.3% trail
    (0.007, 0.005),   # gain > 0.7% → 0.5% trail
    (0.003, 0.008),   # gain > 0.3% → 0.8% trail
    (0.000, 0.013),   # gain < 0.3% → 1.3% trail
]
PROFIT_FLOOR_TRIGGER = 0.003     # 0.3% peak gain arms the profit floor
PROFIT_FLOOR_LEVEL   = 0.001     # floor = entry * 1.001

# Loop timing
HOURLY_INTERVAL   = 3600
SUBLOOP_INTERVAL  = 300          # 5-min sub-loop

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def sf(v, d=0.0):
    try:
        f = float(v)
        return d if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return d

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
        f.write(line + '\n')

def parse_ts(ts_str):
    try:
        return pd.to_datetime(ts_str, utc=True).timestamp()
    except Exception:
        return time.time()

# ─────────────────────────────────────────────────────────────
# RATE LIMITER — single gatekeeper for ALL Kraken API calls
# ─────────────────────────────────────────────────────────────
class RateLimiter:
    MIN_SPACING  = 1.5
    BACKOFF_BASE = 10
    MAX_RETRIES  = 5

    def __init__(self):
        self._last = 0.0

    def _wait(self):
        gap = self.MIN_SPACING - (time.time() - self._last)
        if gap > 0:
            time.sleep(gap)
        self._last = time.time()

    def call(self, fn, *args, **kwargs):
        for attempt in range(self.MAX_RETRIES):
            try:
                self._wait()
                return fn(*args, **kwargs)
            except ccxt.RateLimitExceeded as e:
                wait = self.BACKOFF_BASE * (2 ** attempt)
                log_event(f"[RateLimiter] Rate limit — backoff {wait}s (attempt {attempt+1})")
                time.sleep(wait)
            except ccxt.NetworkError as e:
                wait = self.BACKOFF_BASE * (2 ** attempt)
                log_event(f"[RateLimiter] Network error — backoff {wait}s: {e}")
                time.sleep(wait)
            except Exception as e:
                raise
        log_event("[RateLimiter] Max retries exceeded — skipping call.")
        return None

# ─────────────────────────────────────────────────────────────
# STATE MANAGER — atomic writes, legacy CSV bootstrap
# ─────────────────────────────────────────────────────────────
class StateManager:
    def __init__(self):
        now = time.time()
        self.virtual_usd       = STARTING_CAPITAL
        self.virtual_btc       = 0.0
        self.max_equity        = STARTING_CAPITAL
        self.peak_price        = 0.0
        self.entry_price       = 0.0
        self.trade_count       = 0
        self.current_regime    = 'NEUTRAL'
        self.first_start_ts    = now
        self.session_start_ts  = now
        self.total_paused_secs = 0.0
        self.ever_floor        = False   # profit floor armed flag

    def equity(self, price):
        return self.virtual_usd + self.virtual_btc * price

    def uptime(self):
        gross  = time.time() - self.first_start_ts
        paused = self.total_paused_secs
        return fmt_dur(gross), fmt_dur(paused), fmt_dur(gross - paused)

    def save(self):
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
            'ever_floor':         self.ever_floor,
            'last_heartbeat_ts':  time.time(),
        }
        tmp = STATE_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, STATE_FILE)

    def load(self):
        """Load from state.json → legacy CSV → fresh start."""
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE) as f:
                    s = json.load(f)
                now = time.time()
                gap = now - s.get('last_heartbeat_ts', now)
                self.virtual_usd       = s['virtual_usd']
                self.virtual_btc       = s['virtual_btc']
                self.max_equity        = s['max_equity']
                self.peak_price        = s['peak_price']
                self.entry_price       = s.get('entry_price', 0.0)
                self.trade_count       = s['trade_count']
                self.current_regime    = s.get('current_regime', 'NEUTRAL')
                self.first_start_ts    = s['first_start_ts']
                self.total_paused_secs = s['total_paused_secs']
                self.ever_floor        = s.get('ever_floor', False)
                self.session_start_ts  = now
                if gap > 300:
                    self.total_paused_secs += gap
                    log_event(f">>> RESTARTED | Gap: {fmt_dur(gap)} | "
                              f"Equity: ${self.virtual_usd:,.2f} | "
                              f"Trades: {self.trade_count} | "
                              f"Paused total: {fmt_dur(self.total_paused_secs)}")
                else:
                    log_event(f">>> WARM RESTART (gap {fmt_dur(gap)})")
                return True
            except Exception as e:
                log_event(f"WARNING: state.json unreadable ({e}) — trying legacy CSVs")

        # Bootstrap from legacy CSV (equity continuity)
        for path in LEGACY_CSVS:
            if not os.path.exists(path):
                continue
            try:
                df = pd.read_csv(path)
                if df.empty:
                    continue
                eq_col = 'equity' if 'equity' in df.columns else df.columns[1]
                ts_col = df.columns[0]
                last_eq = float(df[eq_col].iloc[-1])
                first_ts = parse_ts(str(df[ts_col].iloc[0]))
                self.virtual_usd    = last_eq
                self.max_equity     = float(df[eq_col].max())
                self.first_start_ts = first_ts
                log_event(f">>> BOOTSTRAPPED from {os.path.basename(path)} | "
                          f"Equity: ${last_eq:,.2f} | "
                          f"Start: {datetime.fromtimestamp(first_ts).strftime('%Y-%m-%d %H:%M:%S')}")
                return True
            except Exception as e:
                log_event(f"WARNING: bootstrap failed ({e})")

        return False

# ─────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────
def calc_adx(df, period=14):
    h, l, c = df['h'], df['l'], df['c']
    pdm = h.diff().clip(lower=0)
    ndm = (-l.diff()).clip(lower=0)
    tr  = pd.concat([(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()],
                    axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    pdi = 100 * pdm.ewm(span=period, adjust=False).mean() / (atr + 1e-9)
    ndi = 100 * ndm.ewm(span=period, adjust=False).mean() / (atr + 1e-9)
    dx  = (pdi - ndi).abs() / (pdi + ndi + 1e-9) * 100
    return dx.ewm(span=period, adjust=False).mean()

def compute_signals(df_btc, df_eth, bb_mult):
    """
    Compute all indicators on the provided dataframes.
    All signals read from iloc[-2] (last CLOSED candle — no look-ahead).
    Returns a dict of all indicator values and signal flags.
    """
    df = df_btc.copy()
    c  = df['c']

    # Bollinger Bands
    df['sma20'] = c.rolling(20).mean()
    df['std20'] = c.rolling(20).std()
    df['upper'] = df['sma20'] + bb_mult * df['std20']
    df['lower'] = df['sma20'] - bb_mult * df['std20']

    # EMAs
    df['ema21'] = c.ewm(span=21, adjust=False).mean()
    df['ema55'] = c.ewm(span=55, adjust=False).mean()

    # RSI(14)
    delta     = c.diff()
    gain      = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    loss      = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
    df['rsi'] = 100 - 100 / (1 + gain / (loss + 1e-9))

    # ADX on BTC
    df['adx'] = calc_adx(df_btc)

    # ETH regime EMAs
    df_eth = df_eth.copy()
    df_eth['ema21'] = df_eth['c'].ewm(span=21, adjust=False).mean()
    df_eth['ema55'] = df_eth['c'].ewm(span=55, adjust=False).mean()

    # Read from iloc[-2] — last fully closed candle
    I = -2

    close    = sf(c.iloc[I])
    prev_c   = sf(c.iloc[I - 1])
    upper    = sf(df['upper'].iloc[I])
    lower    = sf(df['lower'].iloc[I])
    sma20    = sf(df['sma20'].iloc[I])
    ema21    = sf(df['ema21'].iloc[I])
    ema55    = sf(df['ema55'].iloc[I])
    prev_e21 = sf(df['ema21'].iloc[I - 1])
    rsi      = sf(df['rsi'].iloc[I], 50.0)
    adx_val  = sf(df['adx'].iloc[I])

    # Current (live) candle for exit checks — iloc[-1]
    live_close  = sf(c.iloc[-1])
    live_upper  = sf(df['upper'].iloc[-1])
    live_ema21  = sf(df['ema21'].iloc[-1])

    # Regime components
    btc_bull = (sf(df['ema21'].iloc[-1]) > sf(df['ema55'].iloc[-1]) and
                live_close > sf(df['ema21'].iloc[-1]))
    eth_bull = sf(df_eth['ema21'].iloc[-1]) > sf(df_eth['ema55'].iloc[-1])
    btc_bear = sf(df['ema21'].iloc[-1]) < sf(df['ema55'].iloc[-1])
    eth_bear = sf(df_eth['ema21'].iloc[-1]) < sf(df_eth['ema55'].iloc[-1])
    strong   = adx_val > ADX_THRESHOLD

    if btc_bull and eth_bull and strong:
        regime = 'BULL'
    elif btc_bear or eth_bear:
        regime = 'BEAR'
    else:
        regime = 'NEUTRAL'

    # Entry signals (all from last CLOSED candle)
    sig_bb_lower  = (not np.isnan(lower)) and (close < lower)
    sig_crossover = (prev_c < prev_e21) and (close > ema21)
    sig_ema_pull  = (not np.isnan(ema21) and
                     close < ema21 and
                     close >= ema21 * 0.9925 and
                     rsi < 52)
    sig_sma_touch = (not np.isnan(sma20) and close < sma20 and rsi < 50)
    sig_rsi_os    = (not np.isnan(ema55) and rsi < 42 and close > ema55)

    # BB upper exit check — uses live candle for exit decisions
    bb_upper_exit = (not np.isnan(live_upper) and live_close > live_upper)

    return {
        'regime':        regime,
        'close':         close,
        'live_close':    live_close,
        'live_ema21':    live_ema21,
        'live_upper':    live_upper,
        'upper':         upper,
        'lower':         lower,
        'sma20':         sma20,
        'ema21':         ema21,
        'ema55':         ema55,
        'rsi':           rsi,
        'adx':           adx_val,
        'sig_bb_lower':  sig_bb_lower,
        'sig_crossover': sig_crossover,
        'sig_ema_pull':  sig_ema_pull,
        'sig_sma_touch': sig_sma_touch,
        'sig_rsi_os':    sig_rsi_os,
        'bb_upper_exit': bb_upper_exit,
    }

# ─────────────────────────────────────────────────────────────
# TRAIL STOP — V4.1 tiered + profit floor
# ─────────────────────────────────────────────────────────────
def calc_trail_stop(entry_price, peak_price, ever_floor):
    """
    Returns (stop_price, trail_pct, trail_label, updated_ever_floor).
    Tiered trail from peak + profit floor once peak gain > 0.3%.
    """
    gain_pct  = (peak_price - entry_price) / entry_price
    trail_pct = TRAIL_TIERS[-1][1]    # default 1.3%
    for threshold, pct in TRAIL_TIERS:
        if gain_pct >= threshold:
            trail_pct = pct
            break

    trail_stop = peak_price * (1 - trail_pct)

    # Profit floor — once armed, stop can never drop below entry * 1.001
    if ever_floor or gain_pct >= PROFIT_FLOOR_TRIGGER:
        floor = entry_price * (1 + PROFIT_FLOOR_LEVEL)
        if trail_stop < floor:
            return floor, trail_pct, f'TRAIL_STOP({trail_pct*100:.1f}%)+FLOOR', True
        return trail_stop, trail_pct, f'TRAIL_STOP({trail_pct*100:.1f}%)', True

    return trail_stop, trail_pct, f'TRAIL_STOP({trail_pct*100:.1f}%)', ever_floor

# ─────────────────────────────────────────────────────────────
# EXECUTION
# ─────────────────────────────────────────────────────────────
def execute_buy(s, sig_price, signal_name):
    exec_price    = sig_price * (1 + SLIPPAGE)
    s.virtual_btc = s.virtual_usd / exec_price
    s.virtual_usd = 0.0
    s.peak_price  = exec_price
    s.entry_price = exec_price
    s.ever_floor  = False
    s.trade_count += 1
    log_event(f"!!! BUY  @ ${exec_price:,.2f} | "
              f"BTC: {s.virtual_btc:.6f} | Signal: {signal_name} | "
              f"Regime: {s.current_regime}")
    return exec_price

def execute_sell(s, sig_price, reason):
    exec_price    = sig_price * (1 - SLIPPAGE)
    prev_entry    = s.entry_price
    pnl_usd       = s.virtual_btc * exec_price - (s.virtual_btc * prev_entry)
    pnl_pct       = (exec_price - prev_entry) / prev_entry * 100
    s.virtual_usd = s.virtual_btc * exec_price
    s.virtual_btc = 0.0
    s.peak_price  = 0.0
    s.entry_price = 0.0
    s.ever_floor  = False
    log_event(f"!!! SELL @ ${exec_price:,.2f} | Reason: {reason} | "
              f"P&L: ${pnl_usd:+,.2f} ({pnl_pct:+.2f}%) | "
              f"Cash: ${s.virtual_usd:,.2f}")
    return exec_price

# ─────────────────────────────────────────────────────────────
# AUDIT TRAIL
# ─────────────────────────────────────────────────────────────
_audit_rows = []

def log_audit(s, price, action, reason, exec_price, signal=''):
    global _audit_rows
    eq  = s.equity(price)
    s.max_equity = max(s.max_equity, eq)
    dd  = (s.max_equity - eq) / s.max_equity if s.max_equity > 0 else 0.0
    gross, paused, net = s.uptime()

    _audit_rows.append({
        'timestamp':    datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
        'action':       action,
        'reason':       reason,
        'signal':       signal,
        'regime':       s.current_regime,
        'price':        round(price, 2),
        'exec_price':   round(exec_price, 2),
        'entry_price':  round(s.entry_price, 2),
        'peak_price':   round(s.peak_price, 2),
        'equity':       round(eq, 2),
        'drawdown':     round(dd, 4),
        'trade_count':  s.trade_count,
        'gross_runtime': gross,
        'total_paused':  paused,
        'net_runtime':   net,
    })
    pd.DataFrame(_audit_rows).to_csv(AUDIT_FILE, index=False)
    return eq, dd

# ─────────────────────────────────────────────────────────────
# HEALTH CHECK PRINT
# ─────────────────────────────────────────────────────────────
def print_health(s, price, stop_price=0.0, trail_label='', trail_pct=0.0):
    eq     = s.equity(price)
    profit = eq - STARTING_CAPITAL
    pp     = profit / STARTING_CAPITAL * 100
    dd     = (s.max_equity - eq) / s.max_equity if s.max_equity > 0 else 0.0
    gross, paused, net = s.uptime()

    print("\n" + "=" * 62)
    print(f" GOAT FUNDED HEALTH CHECK | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f" REGIME: {s.current_regime}")
    print("─" * 62)
    print(f" Trades:       {s.trade_count}")
    print(f" Equity:       ${eq:,.2f}")
    print(f" P/L:          ${profit:+,.2f} ({pp:+.2f}%)")
    print(f" Max DD:       {dd*100:.2f}%  (limit: {MAX_DD_PCT*100:.0f}%)")
    print(f" Position:     {'LONG' if s.virtual_btc > 0 else 'FLAT'}")
    if s.virtual_btc > 0 and s.entry_price > 0:
        gain_pct  = (s.peak_price - s.entry_price) / s.entry_price * 100
        curr_pnl  = (price - s.entry_price) / s.entry_price * 100
        hard_stop = s.peak_price * (1 - STOP_PCT)
        print(f" Entry:        ${s.entry_price:,.2f}")
        print(f" Current:      ${price:,.2f}  ({curr_pnl:+.2f}%)")
        print(f" Peak:         ${s.peak_price:,.2f}  (+{gain_pct:.3f}%)")
        if stop_price > 0:
            print(f" Trail Stop:   ${stop_price:,.2f}  ({trail_pct*100:.2f}% | {trail_label})")
        print(f" Hard Stop:    ${hard_stop:,.2f}  (3.5% from peak)")
        print(f" Floor Armed:  {'YES' if s.ever_floor else 'NO (need +0.3% peak)'}")
    print("─" * 62)
    print(f" Gross:        {gross}")
    print(f" Paused:       {paused}")
    print(f" Net:          {net}")
    print("=" * 62 + "\n")

# ─────────────────────────────────────────────────────────────
# CANDLE FETCH
# ─────────────────────────────────────────────────────────────
def fetch_candles(exchange, rl, symbol, tf='1h', limit=100):
    raw = rl.call(exchange.fetch_ohlcv, symbol, tf, None, limit)
    if raw is None or len(raw) < 60:
        return None
    df = pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
    return df

# ─────────────────────────────────────────────────────────────
# MAIN BOT
# ─────────────────────────────────────────────────────────────
class GoatFunded:
    def __init__(self):
        self.exchange = ccxt.kraken({'enableRateLimit': False})
        self.rl       = RateLimiter()
        self.state    = StateManager()

        # Timing gates
        self._last_hourly    = 0.0
        self._last_subloop   = 0.0
        self._last_sell_ts   = 0.0   # for idleness guard

        # Trail state (updated in sub-loop)
        self._stop_price    = 0.0
        self._trail_pct     = 0.013
        self._trail_label   = 'tiered'

        self._audit_rows = []

    # ── Boot ──────────────────────────────────────────────────
    def boot(self):
        log_event("=" * 62)
        log_event("GOAT FUNDED — BTC/USD $100k | V4.1 Trail | BULL+NEUTRAL entries")
        log_event("=" * 62)

        log_event("[Boot] Waiting 15s for NTP clock sync...")
        time.sleep(15)

        log_event("[Boot] Verifying Kraken API connectivity...")
        deadline = time.time() + 300
        while time.time() < deadline:
            try:
                self.rl.call(self.exchange.fetch_time)
                log_event("[Boot] Kraken API reachable.")
                break
            except Exception as e:
                log_event(f"[Boot] Not reachable ({e}) — retry in 15s")
                time.sleep(15)
        else:
            log_event("[Boot] CRITICAL: API unreachable after 5 minutes.")
            raise SystemExit(1)

        resumed = self.state.load()
        if not resumed:
            log_event(f">>> FIRST START — fresh ${STARTING_CAPITAL:,.0f} virtual account.")
        self.state.save()

        # Restore audit trail
        global _audit_rows
        if os.path.exists(AUDIT_FILE):
            try:
                _audit_rows = pd.read_csv(AUDIT_FILE).to_dict('records')
                log_event(f"[Boot] Loaded {len(_audit_rows)} existing audit rows.")
            except Exception:
                _audit_rows = []

        log_event("[Boot] Boot complete — entering main loop.")

    # ── Hourly signal block ───────────────────────────────────
    def hourly_tick(self):
        """
        Full hourly pipeline:
        1. Fetch BTC + ETH 1h candles
        2. Compute regime + all signals
        3. Entry or exit decision
        4. Order book omitted (live version uses real price for exits)
        """
        s = self.state

        df_btc = fetch_candles(self.exchange, self.rl, 'BTC/USD', '1h', 100)
        df_eth = fetch_candles(self.exchange, self.rl, 'ETH/USD', '1h', 100)
        if df_btc is None or df_eth is None:
            log_event("[Hourly] OHLCV fetch failed — skipping cycle.")
            return

        # Use BB multiplier appropriate to current regime pre-check
        # (compute with BULL mult first to get regime, then recompute if NEUTRAL)
        ind = compute_signals(df_btc, df_eth, BB_MULT_BULL)
        if ind['regime'] != 'BULL':
            ind = compute_signals(df_btc, df_eth, BB_MULT_NEUTRAL)
            # Overwrite regime — compute_signals always calculates regime
            # from raw EMA/ADX so it's identical either way

        s.current_regime = ind['regime']

        # Fetch live price
        ticker = self.rl.call(self.exchange.fetch_ticker, 'BTC/USD')
        price  = sf(ticker['last']) if ticker else ind['live_close']

        action    = 'HOLD'
        sig_name  = ''

        # ── EXIT ──────────────────────────────────────────────
        if s.virtual_btc > 0:
            s.peak_price = max(s.peak_price, price)

            # Regime flip to BEAR
            if ind['regime'] == 'BEAR':
                action = 'SELL_REGIME_FLIP'

            # Hard stop (3.5% from peak)
            elif price <= s.peak_price * (1 - STOP_PCT):
                action = 'SELL_STOP'

            # BB upper target
            elif ind['bb_upper_exit']:
                # In BULL — hold if price still riding above EMA21
                if ind['regime'] == 'BULL' and price < ind['live_ema21'] * 1.01:
                    action = 'HOLD'
                else:
                    action = 'SELL_TARGET'

        # ── ENTRY ─────────────────────────────────────────────
        elif s.virtual_btc == 0:
            if ind['regime'] == 'BEAR':
                action = 'HOLD'

            elif ind['regime'] == 'BULL':
                flat_secs = time.time() - self._last_sell_ts if self._last_sell_ts else IDLE_HOURS * 3600 + 1
                idle_8h   = flat_secs > IDLE_HOURS * 3600

                if ind['sig_bb_lower']:
                    action = 'BUY'; sig_name = 'BB_LOWER'
                elif ind['sig_crossover']:
                    action = 'BUY'; sig_name = 'EMA21_CROSS'
                elif ind['sig_ema_pull']:
                    action = 'BUY'; sig_name = 'EMA21_PULL'
                elif ind['sig_sma_touch']:
                    action = 'BUY'; sig_name = 'SMA20_TOUCH'
                elif ind['sig_rsi_os']:
                    action = 'BUY'; sig_name = 'RSI_OVERSOLD'
                elif idle_8h and (ind['sig_ema_pull'] or ind['sig_sma_touch'] or ind['sig_rsi_os']):
                    action = 'BUY'; sig_name = 'IDLE_GUARD'

            else:  # NEUTRAL
                if ind['sig_bb_lower']:
                    action = 'BUY'; sig_name = 'BB_LOWER_NEUTRAL'

        # ── EXECUTE ───────────────────────────────────────────
        exec_price = 0.0
        if action == 'BUY' and s.virtual_btc == 0:
            exec_price = execute_buy(s, price, sig_name)
            self._stop_price  = exec_price * (1 - TRAIL_TIERS[-1][1])
            self._trail_pct   = TRAIL_TIERS[-1][1]
            self._trail_label = 'tiered'
            log_audit(s, price, 'BUY', sig_name, exec_price, signal=sig_name)

        elif action.startswith('SELL') and s.virtual_btc > 0:
            exec_price        = execute_sell(s, price, action)
            self._last_sell_ts = time.time()
            log_audit(s, price, 'SELL', action, exec_price)

        else:
            # Log hourly HOLD for audit trail visibility
            log_event(f"[Hourly] HOLD | regime={ind['regime']} | "
                      f"price=${price:,.0f} | "
                      f"RSI={ind['rsi']:.1f} | ADX={ind['adx']:.1f} | "
                      f"BB_lower=${ind['lower']:,.0f} | BB_upper=${ind['upper']:,.0f}")

        s.save()
        eq, dd = log_audit(s, price, 'HEARTBEAT', 'hourly', exec_price)
        print_health(s, price, self._stop_price, self._trail_label, self._trail_pct)
        return dd

    # ── 5-min sub-loop ────────────────────────────────────────
    def subloop_tick(self):
        """
        Between hourly signals:
        - Update trail stop using tiered logic + profit floor
        - Check if trail or hard stop is breached
        - Exits on breach
        Returns dd
        """
        s = self.state
        if s.virtual_btc == 0:
            return 0.0

        ticker = self.rl.call(self.exchange.fetch_ticker, 'BTC/USD')
        if ticker is None:
            return 0.0
        price = sf(ticker['last'])
        if price <= 0:
            return 0.0

        s.peak_price = max(s.peak_price, price)

        # Compute trail stop
        stop_price, trail_pct, trail_label, s.ever_floor = calc_trail_stop(
            s.entry_price, s.peak_price, s.ever_floor
        )
        self._stop_price  = stop_price
        self._trail_pct   = trail_pct
        self._trail_label = trail_label

        hard_stop = s.peak_price * (1 - STOP_PCT)
        action    = 'HOLD'

        if price <= hard_stop:
            action = 'SELL_STOP'
        elif price <= stop_price:
            action = trail_label   # e.g. 'SELL_TRAIL(0.5%)'
            action = f'SELL_{trail_label}'

        if action != 'HOLD':
            exec_price = execute_sell(s, price, action)
            self._last_sell_ts = time.time()
            log_audit(s, price, 'SELL', action, exec_price)
            s.save()

        eq = s.equity(price)
        s.max_equity = max(s.max_equity, eq)
        dd = (s.max_equity - eq) / s.max_equity if s.max_equity > 0 else 0.0

        # Print health on sub-loop if in position
        if s.virtual_btc > 0:
            print_health(s, price, stop_price, trail_label, trail_pct)

        return dd

    # ── Main loop ─────────────────────────────────────────────
    def run(self):
        self.boot()

        while True:
            try:
                now = time.time()

                # ── Hourly block ──────────────────────────────
                # Sync to next hour boundary
                next_hour = (now // HOURLY_INTERVAL + 1) * HOURLY_INTERVAL
                if now >= self._last_hourly + HOURLY_INTERVAL:
                    self._last_hourly = now
                    dd = self.hourly_tick()
                    if dd is not None and dd >= MAX_DD_PCT:
                        log_event(f"CRITICAL: {MAX_DD_PCT*100:.0f}% KILL SWITCH TRIGGERED.")
                        self.state.save()
                        raise SystemExit(0)

                # ── 5-min sub-loop ────────────────────────────
                if now >= self._last_subloop + SUBLOOP_INTERVAL:
                    self._last_subloop = now
                    if self.state.virtual_btc > 0:
                        dd = self.subloop_tick()
                        if dd >= MAX_DD_PCT:
                            log_event(f"CRITICAL: {MAX_DD_PCT*100:.0f}% KILL SWITCH (sub-loop).")
                            self.state.save()
                            raise SystemExit(0)

                # ── Sleep until next sub-loop tick ────────────
                sleep_secs = max(10, self._last_subloop + SUBLOOP_INTERVAL - time.time())
                time.sleep(sleep_secs)

            except SystemExit:
                raise
            except KeyboardInterrupt:
                log_event("[Main] KeyboardInterrupt — saving state and exiting.")
                self.state.save()
                log_event("[Main] Done.")
                break
            except Exception as e:
                log_event(f"[Main] Unhandled error: {e}")
                self.state.save()
                time.sleep(60)


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
if __name__ == '__main__':
    GoatFunded().run()
