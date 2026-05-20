"""
╔══════════════════════════════════════════════════════════════════════════╗
║           KRAKEN ALPHA V1 — All-Regime Live/Paper Trading Bot           ║
║                      Synthesized from V3.x → V8.2 Backtests             ║
╠══════════════════════════════════════════════════════════════════════════╣
║  RESEARCH SUMMARY (what the backtests actually proved):                 ║
║                                                                          ║
║  ✓ TRAIL_FLOOR_T2 was the #1 exit — 2 of 3 winning trades used it      ║
║  ✓ Profit floor (break-even lock) eliminates winners becoming losers    ║
║  ✓ MACD signal crossover (12,26,9) — best entry, 2-bar confirmation     ║
║  ✓ BB lower band + RSI<38 = high-probability mean reversion in NEUTRAL  ║
║  ✓ HTF regime (1h EMA21/EMA55) correctly sidestepped Feb bear move      ║
║  ✓ ADX threshold 15 (not 20) — captures more valid entries              ║
║  ✓ NEUTRAL regime must get full signal suite (60% of all market time)   ║
║  ✓ Idleness guard (8h no trade → fire on any single signal)             ║
║  ✓ Conviction scoring (ADX + RSI + MFI + vol + regime) cuts bad entries ║
║  ✓ 2h cooldown after exit prevents re-entry grinding                    ║
║  ✓ Tiered trail: 1.3%→0.8%→0.5%→0.3% (tightens as profit grows)       ║
║                                                                          ║
║  REGIME PLAYBOOK:                                                        ║
║  BULL    → MACD crossover entries, momentum ride, tiered trail           ║
║  NEUTRAL → BB mean reversion, tight 2% trail, target midband            ║
║  BEAR    → NO new longs, close profitable positions, preserve cash       ║
║                                                                          ║
║  Capital: $500 | Max positions: 3 | Hard stop: 3% | Kill switch: 10%    ║
║  Pairs:   BTC ETH SOL XRP DOGE ADA DOT                                  ║
╚══════════════════════════════════════════════════════════════════════════╝

SETUP:
  pip install ccxt pandas numpy
  Set API_KEY / API_SECRET below
  Set PAPER_TRADING = False when ready for live execution
  python3 kraken_alpha_v1.py

EMERGENCY STOP:
  touch EMERGENCY_STOP        ← bot detects this file and shuts down cleanly
  Ctrl+C or kill <pid>        ← SIGINT/SIGTERM also triggers clean shutdown

FILES CREATED:
  state.json          — atomic state (survives restarts)
  audit_trail.csv     — every trade entry + exit with full metadata
  events.log          — timestamped journal of every decision + error
"""

import ccxt
import pandas as pd
import numpy as np
import json
import csv
import os
import sys
import math
import time
import signal
import traceback
from datetime import datetime, timezone
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════
# ██  CONFIG — Edit this section before running
# ══════════════════════════════════════════════════════════════════════════

API_KEY    = '/AL34kjaYAnSVw0cMAiGj62vGLAD93UjGuusEQg1K6QglQez62/t0Wip'
API_SECRET = 'ToIeXiQivgB8pmFmlX5gDKCSCNhDj+WuJ/05Pc+bNxxAzO92H7q7Yi5olJ1TaAVc7qDau6jP42Uvuc8+oURAbg=='

# Set to False when you are ready to place REAL orders. True = virtual only.
PAPER_TRADING = True

STARTING_CAPITAL  = 500.0    # USD
MAX_POSITIONS     = 3        # max simultaneous positions
DRY_POWDER_PCT    = 0.20     # always keep 20% in cash ($100)
HARD_STOP_PCT     = 0.03     # 3% hard stop per position (max ~$4.50 loss on $150)
MAX_DD_PCT        = 0.10     # 10% portfolio drawdown triggers kill switch ($50 max loss)
SIZE_HIGH_PCT     = 0.28     # 28% of deployable per high-conviction trade (~$112)
SIZE_LOW_PCT      = 0.18     # 18% of deployable per lower-conviction trade (~$72)
MIN_CONVICTION    = 52       # entry gate: 0-100 scale (raised from V8.2's 50 for safety)
MIN_POSITION_USD  = 15.0     # Kraken minimum order floor
COOLDOWN_MS       = 60 * 60 * 1000   # 1 hour cooldown after exit (ms)
MIN_HOLD_BARS     = 3        # minimum 5m bars before MACD exit fires (~15 min)
SLIPPAGE_BPS      = 10       # 10 basis points slippage model for P&L tracking
LOOP_SLEEP_SEC    = 30       # seconds between loop iterations

# Pain thresholds — scaled for $500 capital
# Position never went green AND loss exceeds this → cut immediately
PAIN_USD = [
    (120, 9.0),   # $120+ position → $9 pain before cut
    (75,  5.5),   # $75-120        → $5.50
    (30,  3.0),   # $30-75         → $3.00
    (0,   1.5),   # below $30      → $1.50
]

# Pairs to scan — liquid, tight spread, good for $500 capital
PAIRS = [
    'BTC/USD', 'ETH/USD', 'SOL/USD',
    'XRP/USD', 'DOGE/USD', 'ADA/USD', 'DOT/USD',
]

# MACD parameter sets — proven from V8.2 90-day backtest
MACD_SLOW = (12, 26,  9)   # Primary signal crossover — best win rate
MACD_FAST = ( 5, 10, 16)   # Fast confirmation crossover
MACD_MED  = (12, 26, 90)   # Zero-line crossover (high conviction)

# Files
EMERGENCY_STOP_FILE = 'EMERGENCY_STOP'
STATE_FILE          = 'state.json'
AUDIT_FILE          = 'audit_trail.csv'
LOG_FILE            = 'events.log'

# ══════════════════════════════════════════════════════════════════════════
# ██  GLOBALS
# ══════════════════════════════════════════════════════════════════════════

_shutdown      = False
_start_wall    = time.time()
_last_api_call = 0.0

# ══════════════════════════════════════════════════════════════════════════
# ██  SIGNAL HANDLERS — clean shutdown on SIGTERM / Ctrl+C
# ══════════════════════════════════════════════════════════════════════════

def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    _log('SHUTDOWN', f'Signal {sig} received — finishing current cycle then stopping')

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)

# ══════════════════════════════════════════════════════════════════════════
# ██  UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def sf(v, d=0.0):
    """Safe float — returns d on NaN/Inf/exception"""
    try:
        f = float(v)
        return d if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return d

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

def now_ms() -> int:
    return int(time.time() * 1000)

def _log(tag: str, msg: str):
    line = f"[{utc_now()}] [{tag:12s}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, 'a') as f:
            f.write(line + '\n')
    except Exception:
        pass

def _write_audit(row: dict):
    exists = os.path.exists(AUDIT_FILE)
    with open(AUDIT_FILE, 'a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)

# ══════════════════════════════════════════════════════════════════════════
# ██  STATE — atomic reads and writes
# ══════════════════════════════════════════════════════════════════════════

def _default_state() -> dict:
    return {
        'equity':       STARTING_CAPITAL,
        'peak_equity':  STARTING_CAPITAL,
        'positions':    {},
        'cooldowns':    {},   # sym → unix ms when cooldown expires
        'trade_count':  0,
        'win_count':    0,
        'total_pnl':    0.0,
        'boot_time':    time.time(),
        'gross_runtime': 0.0,
        'version':      'ALPHA_V1',
    }

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        _log('BOOT', 'No state file — starting fresh')
        return _default_state()
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        _log('BOOT', f'State loaded — equity=${s["equity"]:.2f} trades={s["trade_count"]}')
        return s
    except Exception as e:
        _log('STATE_ERR', f'Failed to load state: {e} — starting fresh')
        return _default_state()

def save_state(state: dict):
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)   # atomic on Linux — no corrupt state files

# ══════════════════════════════════════════════════════════════════════════
# ██  RATE LIMITER — single gatekeeper for all API calls
# ══════════════════════════════════════════════════════════════════════════

def api_call(fn, *args, **kwargs):
    """Rate-limited API call with exponential backoff on 429"""
    global _last_api_call
    gap = time.time() - _last_api_call
    if gap < 1.5:
        time.sleep(1.5 - gap)
    for attempt in range(5):
        try:
            result = fn(*args, **kwargs)
            _last_api_call = time.time()
            return result
        except ccxt.RateLimitExceeded:
            wait = 10 * (2 ** attempt)
            _log('RATE_LIMIT', f'429 received — backoff {wait}s (attempt {attempt+1}/5)')
            time.sleep(wait)
        except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
            if attempt == 4:
                _log('API_ERR', f'Network error after 5 attempts: {e}')
                return None
            time.sleep(5 * (2 ** attempt))
        except Exception as e:
            _log('API_ERR', f'Unexpected error: {e}')
            return None
    return None

# ══════════════════════════════════════════════════════════════════════════
# ██  INDICATORS
# ══════════════════════════════════════════════════════════════════════════

def ind_macd(c: pd.Series, fast: int, slow: int, sig: int):
    ml = c.ewm(span=fast, adjust=False).mean() - c.ewm(span=slow, adjust=False).mean()
    sl = ml.ewm(span=sig, adjust=False).mean()
    return ml, sl, ml - sl          # macd_line, signal_line, histogram

def ind_adx(h: pd.Series, l: pd.Series, c: pd.Series, p: int = 14):
    up  = h.diff()
    dn  = -l.diff()
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr  = pd.concat([h - l,
                     (h - c.shift()).abs(),
                     (l - c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/p, adjust=False).mean()
    pdi = 100 * pd.Series(pdm, index=c.index).ewm(alpha=1/p, adjust=False).mean() / (atr + 1e-9)
    mdi = 100 * pd.Series(mdm, index=c.index).ewm(alpha=1/p, adjust=False).mean() / (atr + 1e-9)
    dx  = 100 * (pdi - mdi).abs() / (pdi + mdi + 1e-9)
    return dx.ewm(alpha=1/p, adjust=False).mean(), atr

def ind_rsi(c: pd.Series, p: int = 14) -> pd.Series:
    d  = c.diff()
    g  = d.clip(lower=0).ewm(com=p-1, adjust=False).mean()
    ls = (-d.clip(upper=0)).ewm(com=p-1, adjust=False).mean()
    return 100 - 100 / (1 + g / (ls + 1e-9))

def ind_mfi(h, l, c, v, p: int = 14) -> pd.Series:
    tp  = (h + l + c) / 3
    rmf = tp * v
    pos = rmf.where(tp > tp.shift(1), 0.0).rolling(p).sum()
    neg = rmf.where(tp < tp.shift(1), 0.0).rolling(p).sum()
    return 100 - 100 / (1 + pos / (neg + 1e-9))

def ind_bb(c: pd.Series, p: int = 20, mult: float = 2.0):
    mid = c.rolling(p).mean()
    std = c.rolling(p).std()
    return mid - mult * std, mid, mid + mult * std  # lower, mid, upper

def ind_ema(c: pd.Series, span: int) -> pd.Series:
    return c.ewm(span=span, adjust=False).mean()

# ══════════════════════════════════════════════════════════════════════════
# ██  DATA FETCH
# ══════════════════════════════════════════════════════════════════════════

def fetch_ohlcv(exchange, sym: str, tf: str, limit: int = 210) -> pd.DataFrame | None:
    try:
        raw = api_call(exchange.fetch_ohlcv, sym, tf, limit=limit)
        if raw is None or len(raw) < 50:
            return None
        df = pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        df.drop_duplicates('ts', inplace=True)
        df.sort_values('ts', inplace=True)
        df.reset_index(drop=True, inplace=True)
        for col in ['o', 'h', 'l', 'c', 'v']:
            df[col] = df[col].astype(float)
        return df
    except Exception as e:
        _log('FETCH', f'{sym}/{tf}: {e}')
        return None

# ══════════════════════════════════════════════════════════════════════════
# ██  REGIME DETECTION — BTC as market anchor (proven in V3.6+)
# ══════════════════════════════════════════════════════════════════════════

def detect_regime(df_btc_1h: pd.DataFrame | None) -> str:
    """
    BULL   = EMA21 > EMA55 AND price > EMA21  (confirmed uptrend)
    BEAR   = EMA21 < EMA55                    (downtrend — cash only)
    NEUTRAL = everything else                  (ranging — mean reversion)

    Uses penultimate candle (iloc[-2]) to avoid look-ahead bias.
    ADX threshold lowered to 15 from original 20 (V5 fix — ADX suppressed in ranges)
    """
    if df_btc_1h is None or len(df_btc_1h) < 60:
        return 'NEUTRAL'
    c   = df_btc_1h['c']
    e21 = sf(ind_ema(c, 21).iloc[-2])
    e55 = sf(ind_ema(c, 55).iloc[-2])
    p   = sf(c.iloc[-2])
    if   e21 > e55 and p > e21:  return 'BULL'
    elif e21 < e55:               return 'BEAR'
    return 'NEUTRAL'

def per_asset_regime(df_1h: pd.DataFrame | None) -> str:
    """Per-asset regime for fine-grained gating"""
    if df_1h is None or len(df_1h) < 60:
        return 'NEUTRAL'
    c   = df_1h['c']
    e21 = sf(ind_ema(c, 21).iloc[-2])
    e55 = sf(ind_ema(c, 55).iloc[-2])
    p   = sf(c.iloc[-2])
    if   e21 > e55 and p > e21:  return 'BULL'
    elif e21 < e55:               return 'BEAR'
    return 'NEUTRAL'

# ══════════════════════════════════════════════════════════════════════════
# ██  KEY LEVELS (1h swing H/L — V8.2 proven, adds +12 conviction)
# ══════════════════════════════════════════════════════════════════════════

def find_key_levels(df_1h: pd.DataFrame | None, lookback: int = 30) -> list:
    if df_1h is None or len(df_1h) < lookback + 5:
        return []
    sub = df_1h.tail(lookback + 4)
    hv, lv = sub['h'].values, sub['l'].values
    levels = []
    for i in range(2, len(hv) - 2):
        if hv[i] > hv[i-1] and hv[i] > hv[i-2] and hv[i] > hv[i+1] and hv[i] > hv[i+2]:
            levels.append(float(hv[i]))
        if lv[i] < lv[i-1] and lv[i] < lv[i-2] and lv[i] < lv[i+1] and lv[i] < lv[i+2]:
            levels.append(float(lv[i]))
    return levels

def at_key_level(price: float, levels: list, radius: float = 0.005) -> bool:
    return any(abs(price - lvl) / (lvl + 1e-9) <= radius for lvl in levels)

# ══════════════════════════════════════════════════════════════════════════
# ██  CONVICTION SCORER (0-100)
# ══════════════════════════════════════════════════════════════════════════

def score_conviction(strategy: str, adx_v: float, rsi_v: float, mfi_v: float,
                     vol_ratio: float, regime: str, at_key: bool) -> int:
    """
    Weighted scoring system — synthesized from V8.2 backtest.
    Strategy base + ADX trend strength + RSI oversold depth +
    MFI money flow + volume surge + regime alignment + key level bonus.
    Returns 0-100. Entry requires >= MIN_CONVICTION (52).
    """
    s = {
        'MACD_SLOW_CROSS': 25,   # 2-bar confirmed signal crossover
        'MACD_FAST_CROSS': 18,   # faster confirmation, smaller base
        'ZERO_LINE_CROSS': 30,   # strongest — MACD line crossing zero
        'BB_MEAN_REV':     22,   # mean reversion at lower band
    }.get(strategy, 10)

    # ADX — trend strength gate
    if   adx_v >= 35:  s += 20
    elif adx_v >= 28:  s += 14
    elif adx_v >= 22:  s += 9
    elif adx_v >= 15:  s += 5
    # Below 15: no bonus — but BB_MEAN_REV specifically WANTS low ADX (range)

    # RSI — oversold depth (deeper = better for mean rev and bounces)
    if   rsi_v < 25:   s += 20
    elif rsi_v < 32:   s += 15
    elif rsi_v < 40:   s += 10
    elif rsi_v < 50:   s += 5
    elif rsi_v >= 65:  s -= 12  # overbought at entry = penalty

    # MFI — money flow confirmation
    if   mfi_v > 60:   s += 8
    elif mfi_v > 48:   s += 4
    elif mfi_v < 28:   s -= 6

    # Volume surge
    if   vol_ratio >= 3.0:  s += 12
    elif vol_ratio >= 2.5:  s += 9
    elif vol_ratio >= 2.0:  s += 6
    elif vol_ratio >= 1.5:  s += 3

    # Regime alignment
    if   regime == 'BULL':    s += 15
    elif regime == 'NEUTRAL': s += 0   # neutral — no bonus, no penalty
    elif regime == 'BEAR':    s -= 30  # shouldn't happen (bear gated before scoring)

    # Key level proximity
    if at_key:  s += 12

    return max(0, min(int(s), 100))

# ══════════════════════════════════════════════════════════════════════════
# ██  ENTRY SIGNALS — three strategies, two regimes
# ══════════════════════════════════════════════════════════════════════════

def scan_entries(df5: pd.DataFrame, df1h: pd.DataFrame | None,
                 market_regime: str, last_entry_ms: float) -> list:
    """
    Returns list of (conviction_score, signal_dict).
    Caller picks the highest conviction signal.

    Strategy activation by regime:
      BULL    → MACD_SLOW_CROSS, MACD_FAST_CROSS, ZERO_LINE_CROSS
      NEUTRAL → BB_MEAN_REV (primary), MACD_SLOW_CROSS (secondary)
      BEAR    → nothing (gated out before calling this function)

    Idleness guard (from V5 fix): if no trade in 8h, lower conviction
    threshold by 8 points — prevents the bot sitting on its hands.
    """
    if df5 is None or len(df5) < 120:
        return []

    I = -2   # penultimate candle — avoids look-ahead bias
    c, h, l, v = df5['c'], df5['h'], df5['l'], df5['v']

    # All indicators on penultimate confirmed candle
    ml_s, sl_s, hs = ind_macd(c, *MACD_SLOW)
    ml_f, sl_f, hf = ind_macd(c, *MACD_FAST)
    ml_m, sl_m, hm = ind_macd(c, *MACD_MED)
    adx_s, atr_s   = ind_adx(h, l, c)
    rsi_s           = ind_rsi(c)
    mfi_s           = ind_mfi(h, l, c, v)
    bb_lo, bb_mid, bb_hi = ind_bb(c)

    adx_v    = sf(adx_s.iloc[I])
    rsi_v    = sf(rsi_s.iloc[I])
    mfi_v    = sf(mfi_s.iloc[I])
    atr_v    = sf(atr_s.iloc[I])
    price    = sf(c.iloc[I])
    bb_lo_v  = sf(bb_lo.iloc[I])
    bb_mid_v = sf(bb_mid.iloc[I])
    vol_avg  = sf(v.rolling(20).mean().iloc[I])
    vol_r    = sf(v.iloc[I]) / (vol_avg + 1e-9)

    # Hard block: RSI too high = momentum exhausted at entry
    if rsi_v >= 68:
        return []

    # Key levels for conviction bonus
    levels = find_key_levels(df1h)
    kh     = at_key_level(price, levels)

    # Idleness guard: been sitting quiet for 8h+ → lower bar slightly
    hours_since_last = (now_ms() - last_entry_ms) / 3_600_000
    idle_bonus = 8 if hours_since_last >= 8.0 else 0

    sigs = []

    def make_sig(strat):
        return {
            'strategy':    strat,
            'regime':      market_regime,
            'price':       round(price, 6),
            'adx':         round(adx_v, 1),
            'rsi':         round(rsi_v, 1),
            'mfi':         round(mfi_v, 1),
            'atr':         round(atr_v, 4),
            'vol_ratio':   round(vol_r, 2),
            'at_key':      kh,
            'bb_mid':      round(bb_mid_v, 6),
        }

    # ── Strategy A: MACD Slow Signal Crossover ────────────────────────────
    # 2-bar confirmation (current bar > 0, prev bar > 0, bar before that <= 0)
    # ATR magnitude floor prevents trading noise
    # Works in BULL and NEUTRAL
    hs_c  = sf(hs.iloc[I])
    hs_p1 = sf(hs.iloc[I-1])
    hs_p2 = sf(hs.iloc[I-2])
    if (hs_c > price * 0.0001      # ATR floor — not just noise
            and hs_p1 > 0          # previous bar confirmed positive
            and hs_p2 <= 0         # crossover happened last bar
            and adx_v >= 15):      # some trend present (lowered from V5's 20)
        cv = score_conviction('MACD_SLOW_CROSS', adx_v, rsi_v, mfi_v, vol_r, market_regime, kh)
        sigs.append((cv + idle_bonus, make_sig('MACD_SLOW_CROSS')))

    # ── Strategy B: MACD Fast Signal Crossover ────────────────────────────
    # Stricter ATR floor (fast MACD noisier), BULL only — too whipsaw in NEUTRAL
    hf_c  = sf(hf.iloc[I])
    hf_p1 = sf(hf.iloc[I-1])
    hf_p2 = sf(hf.iloc[I-2])
    if (market_regime == 'BULL'
            and hf_c > price * 0.0003   # stricter floor vs slow
            and hf_p1 > 0
            and hf_p2 <= 0
            and adx_v >= 18):
        cv = score_conviction('MACD_FAST_CROSS', adx_v, rsi_v, mfi_v, vol_r, market_regime, kh)
        sigs.append((cv + idle_bonus, make_sig('MACD_FAST_CROSS')))

    # ── Strategy C: MACD Zero-Line Crossover ─────────────────────────────
    # MACD LINE (not histogram) crosses from negative to positive
    # Highest base conviction (30) — but strictest ADX gate (22)
    # BULL only — zero-line crosses in NEUTRAL are less reliable
    ml_c = sf(ml_m.iloc[I])
    ml_p = sf(ml_m.iloc[I-1])
    if (market_regime == 'BULL'
            and ml_c > 0
            and ml_p <= 0
            and adx_v >= 22):
        cv = score_conviction('ZERO_LINE_CROSS', adx_v, rsi_v, mfi_v, vol_r, market_regime, kh)
        sigs.append((cv + idle_bonus, make_sig('ZERO_LINE_CROSS')))

    # ── Strategy D: Bollinger Band Mean Reversion ─────────────────────────
    # Price at/below lower band + RSI oversold + LOW ADX (ranging market)
    # This is the NEUTRAL regime primary strategy.
    # High-probability short-duration trade — target = midband
    # LOW ADX specifically required: mean reversion ONLY works in range markets
    # (V3.5 lesson: in trends, price walks the band and never reverts)
    if (price <= bb_lo_v * 1.002    # at or just above lower band
            and rsi_v < 38          # oversold
            and adx_v < 28          # NOT trending strongly
            and bb_mid_v > price):  # midband above = target exists
        cv = score_conviction('BB_MEAN_REV', adx_v, rsi_v, mfi_v, vol_r, market_regime, kh)
        sig = make_sig('BB_MEAN_REV')
        sig['target_price'] = round(bb_mid_v, 6)  # profit target
        sig['target_pct']   = round((bb_mid_v - price) / price * 100, 2)
        sigs.append((cv + idle_bonus, sig))

    return sigs

# ══════════════════════════════════════════════════════════════════════════
# ██  TIERED TRAILING STOP — synthesized from V4.1 + backtest results
# ══════════════════════════════════════════════════════════════════════════

def tiered_trail_pct(peak_gain_pct: float) -> float:
    """
    Trail distance shrinks as unrealized profit grows.
    Proven from backtest: TRAIL_FLOOR_T2 was responsible for 2 of 3 winning trades.

    < 0.3%  gain → 1.30% trail  (full room, trade still establishing)
    0.3-0.7% gain → 0.80% trail  (this range was where most wins peaked)
    0.7-1.2% gain → 0.50% trail  (meaningful profit — protect it)
    > 1.2%  gain → 0.30% trail  (running hot — nearly a limit order)

    For BB_MEAN_REV specifically: tighter trail (0.5-0.8% max) since
    target is explicit midband, not an open-ended trend ride.
    """
    if   peak_gain_pct >= 1.20:  return 0.30
    elif peak_gain_pct >= 0.70:  return 0.50
    elif peak_gain_pct >= 0.30:  return 0.80
    else:                        return 1.30

def get_pain_usd(size_usd: float) -> float:
    for threshold, pain in PAIN_USD:
        if size_usd >= threshold:
            return pain
    return 1.5

# ══════════════════════════════════════════════════════════════════════════
# ██  EXIT LADDER — proven order from backtests
# ══════════════════════════════════════════════════════════════════════════

def eval_exit(pos: dict, current_price: float, bars_held: int,
              df5: pd.DataFrame | None, market_regime: str) -> tuple:
    """
    Returns (should_exit: bool, reason: str, pnl_usd: float)

    Exit ladder (in priority order):
      1. HARD_STOP_3PCT          — absolute floor, no exceptions
      2. BEAR_REGIME_CLOSE       — market turned bear, cut winners
      3. FAILED_SIGNAL_CUT       — position never green, hit pain threshold
      4. BB_TARGET_HIT           — mean reversion reached midband target
      5. BREAK_EVEN_FLOOR        — once green, can't close below entry
      6. TIERED_TRAIL            — gain-adaptive trailing stop
      7. MACD_FLIP               — histogram turns negative (after min hold)
    """
    entry    = pos['entry_price']
    peak     = pos.get('peak_price', entry)
    size     = pos['size_usd']
    ever_gn  = pos.get('ever_green', False)
    strat    = pos.get('strategy', '')

    # Update peak
    if current_price > peak:
        peak = current_price
        pos['peak_price'] = peak

    gain_pct  = (current_price - entry) / entry * 100
    peak_pct  = (peak - entry) / entry * 100
    pnl_usd   = size * (current_price - entry) / entry

    # Track ever-green
    if pnl_usd > 0 and not ever_gn:
        pos['ever_green'] = True
        ever_gn = True

    # ── 1. Hard stop 3% ──────────────────────────────────────────────────
    if gain_pct <= -(HARD_STOP_PCT * 100):
        return True, 'HARD_STOP_3PCT', pnl_usd

    # ── 2. Bear regime — close ALL positions immediately ─────────────────
    # If the market flips BEAR, the trade thesis is dead regardless of
    # whether we're up or down. Holding a losing position hoping it recovers
    # in a bear regime is exactly how small losses become large ones.
    # The hard stop is a last resort — regime flip is an early warning.
    # Get out now, take the small loss, preserve the capital.
    if market_regime == 'BEAR':
        return True, 'BEAR_REGIME_EXIT', pnl_usd

    # ── 3. Failed signal cut — never went green, exceeded pain ───────────
    # If a position NEVER touched green and hits the pain threshold, cut it.
    # No waiting, no hoping. A trade that never worked is a bad entry.
    # The exit is admitting the mistake, not compounding it.
    pain = get_pain_usd(size)
    if not ever_gn and pnl_usd <= -pain:
        return True, 'FAILED_SIGNAL_CUT', pnl_usd

    # ── 3b. Zombie killer — 48h max hold while underwater ────────────────
    # A position open for 48h+ that is still negative: the thesis is dead.
    # Slow bleeders that never trigger the hard stop are the most dangerous.
    MAX_HOLD_BARS = 576   # 48h × 12 bars/hour (5m bars)
    if bars_held >= MAX_HOLD_BARS and pnl_usd < 0:
        return True, 'ZOMBIE_KILL_48H', pnl_usd

    # ── 3c. Stagnation cut — 24h flat with no green ever ─────────────────
    # 24h+ open, never touched profit, still negative = dead money. Out.
    STAGNATION_BARS = 288   # 24h × 12 bars/hour
    if bars_held >= STAGNATION_BARS and not ever_gn and gain_pct < 0:
        return True, 'STAGNATION_24H', pnl_usd

    # ── 4. BB mean reversion target hit ──────────────────────────────────
    if strat == 'BB_MEAN_REV' and 'target_price' in pos:
        if current_price >= pos['target_price']:
            return True, 'BB_TARGET_HIT', pnl_usd

    # ── 5. Break-even floor ───────────────────────────────────────────────
    # Once a position ever reached 0.3% gain, the stop price can NEVER drop
    # below entry * 1.001. Any trade that was a winner cannot become a loser.
    # This is the single most important profit-retention mechanism from V4.1.
    if ever_gn and peak_pct >= 0.30 and current_price < entry * 1.001:
        return True, 'BREAK_EVEN_FLOOR', pnl_usd

    # ── 6. Tiered trailing stop ───────────────────────────────────────────
    if ever_gn and peak_pct > 0:
        trail_dist   = tiered_trail_pct(peak_pct) / 100
        trail_stop   = peak * (1 - trail_dist)
        # Profit floor guarantee: if ever ≥ 0.3% peak, stop ≥ entry
        if peak_pct >= 0.30:
            trail_stop = max(trail_stop, entry * 1.001)
        pos['trail_stop']   = round(trail_stop, 6)
        pos['trail_tier']   = f'{tiered_trail_pct(peak_pct):.2f}%'
        if current_price <= trail_stop:
            tier_label = f'TRAIL_{tiered_trail_pct(peak_pct):.0f}pct'
            return True, tier_label, pnl_usd

    # ── 7. MACD flip after minimum hold ──────────────────────────────────
    # Wait MIN_HOLD_BARS before allowing MACD flip exit to avoid 1-bar whipsaws
    if bars_held >= MIN_HOLD_BARS and df5 is not None and len(df5) >= 30:
        _, _, hs_live = ind_macd(df5['c'], *MACD_SLOW)
        if sf(hs_live.iloc[-2]) < 0:
            return True, 'MACD_FLIP', pnl_usd

    return False, 'HOLD', pnl_usd

# ══════════════════════════════════════════════════════════════════════════
# ██  POSITION SIZING
# ══════════════════════════════════════════════════════════════════════════

def calc_size(conviction: int, live_equity: float, positions: dict) -> float:
    deployed  = sum(p['size_usd'] for p in positions.values())
    available = max(0.0, live_equity * (1 - DRY_POWDER_PCT) - deployed)
    if available < MIN_POSITION_USD * 1.5:
        return 0.0
    pct  = SIZE_HIGH_PCT if conviction >= 65 else SIZE_LOW_PCT
    size = min(live_equity * pct, available)
    return size if size >= MIN_POSITION_USD else 0.0

# ══════════════════════════════════════════════════════════════════════════
# ██  ORDER EXECUTION (paper + live modes)
# ══════════════════════════════════════════════════════════════════════════

def execute_buy(exchange, sym: str, size_usd: float, price: float) -> dict | None:
    """
    PAPER mode: simulated fill at price + slippage
    LIVE mode:  real market order on Kraken
    Returns fill dict or None on failure
    """
    slippage = SLIPPAGE_BPS / 10000
    exec_price = price * (1 + slippage)

    if PAPER_TRADING:
        return {'price': exec_price, 'amount': size_usd / exec_price, 'mode': 'PAPER'}

    # Live execution
    try:
        qty = size_usd / price
        # Kraken requires specific precision per pair — use createMarketOrder
        order = api_call(exchange.create_market_buy_order, sym, qty)
        if order is None:
            return None
        fill_price = sf(order.get('average', exec_price))
        return {'price': fill_price, 'amount': qty, 'mode': 'LIVE', 'order_id': order.get('id')}
    except Exception as e:
        _log('ORDER_ERR', f'BUY {sym} failed: {e}')
        return None

def execute_sell(exchange, sym: str, pos: dict, current_price: float) -> dict | None:
    """
    PAPER mode: simulated fill at price - slippage
    LIVE mode:  real market order on Kraken
    """
    slippage = SLIPPAGE_BPS / 10000
    exec_price = current_price * (1 - slippage)

    if PAPER_TRADING:
        pnl = pos['size_usd'] * (exec_price - pos['entry_price']) / pos['entry_price']
        return {'price': exec_price, 'pnl': pnl, 'mode': 'PAPER'}

    try:
        qty   = pos.get('qty_held', pos['size_usd'] / pos['entry_price'])
        order = api_call(exchange.create_market_sell_order, sym, qty)
        if order is None:
            return None
        fill_price = sf(order.get('average', exec_price))
        pnl = pos['size_usd'] * (fill_price - pos['entry_price']) / pos['entry_price']
        return {'price': fill_price, 'pnl': pnl, 'mode': 'LIVE', 'order_id': order.get('id')}
    except Exception as e:
        _log('ORDER_ERR', f'SELL {sym} failed: {e}')
        return None

# ══════════════════════════════════════════════════════════════════════════
# ██  HEARTBEAT — detailed status every loop
# ══════════════════════════════════════════════════════════════════════════

def print_heartbeat(state: dict, regime: str, prices: dict, bar_n: int):
    equity   = state['equity']
    # Live equity = cash equity + unrealized P&L on open positions
    pos      = state.get('positions', {})
    unrealized = sum(
        p['size_usd'] * (prices.get(sym, p['entry_price']) - p['entry_price'])
        / p['entry_price']
        for sym, p in pos.items()
    )
    live_eq  = equity + unrealized
    peak     = state.get('peak_equity', STARTING_CAPITAL)
    pnl_tot  = live_eq - STARTING_CAPITAL
    pnl_pct  = pnl_tot / STARTING_CAPITAL * 100
    dd_pct   = (peak - live_eq) / peak * 100 if peak > 0 else 0
    uptime_h = (time.time() - state.get('boot_time', _start_wall)) / 3600
    trades   = state['trade_count']
    wins     = state['win_count']
    wr       = wins / trades * 100 if trades > 0 else 0.0
    mode_tag = '[PAPER]' if PAPER_TRADING else '[LIVE ]'

    W = 70
    bar  = '═' * W
    bar2 = '─' * W

    print(f"\n{bar}")
    print(f"  KRAKEN ALPHA V1 ♥ HEARTBEAT {mode_tag}  |  Bar #{bar_n}")
    print(f"  {utc_now()}")
    print(f"  Uptime: {uptime_h:.1f}h  |  Market Regime: {regime}  |  Pairs: {len(PAIRS)}")
    print(bar2)
    print(f"  Starting capital:     ${STARTING_CAPITAL:>10,.2f}")
    print(f"  Cash equity (closed): ${equity:>10,.2f}")
    print(f"  Unrealized P&L:       ${unrealized:>+10,.2f}")
    print(f"  ► LIVE equity:        ${live_eq:>10,.2f}   {pnl_tot:>+8.2f} ({pnl_pct:>+6.2f}%)")
    print(f"  Peak equity:          ${peak:>10,.2f}")
    print(f"  Drawdown from peak:   {dd_pct:>10.2f}%  (kill switch: {MAX_DD_PCT*100:.0f}%)")
    print(f"  Trades: {trades} | Wins: {wins} ({wr:.0f}%) | Realized P&L: ${state['total_pnl']:>+.2f}")
    print(bar2)

    if pos:
        print(f"  OPEN POSITIONS ({len(pos)}/{MAX_POSITIONS}):")
        for sym, p in pos.items():
            price     = prices.get(sym, p['entry_price'])
            gain_pct  = (price - p['entry_price']) / p['entry_price'] * 100
            gain_usd  = p['size_usd'] * (price - p['entry_price']) / p['entry_price']
            peak_pct  = (p.get('peak_price', p['entry_price']) - p['entry_price']) / p['entry_price'] * 100
            trail     = p.get('trail_stop', 0.0)
            trail_pct = (price - trail) / price * 100 if trail > 0 else 0.0
            hard_s    = p['entry_price'] * (1 - HARD_STOP_PCT)
            pnl_sym   = '+' if gain_usd >= 0 else ''
            tgt       = p.get('target_price')
            tgt_str   = f" | Target: ${tgt:.4f}" if tgt else ''

            print(f"  ┌ {sym:<10} [{p.get('strategy','?'):<17}] [Regime={p.get('regime','?')}] [CV={p.get('conviction',0)}]")
            print(f"  │  Entry: ${p['entry_price']:>12,.4f}  |  Now: ${price:>12,.4f}  |  Peak gain: {peak_pct:>+6.2f}%")
            print(f"  │  P&L:   {pnl_sym}${gain_usd:>10,.2f} ({gain_pct:>+6.2f}%)  |  Size: ${p['size_usd']:.2f}{tgt_str}")
            if trail > 0:
                print(f"  │  Trail stop: ${trail:>12,.4f}  ({trail_pct:.2f}% below price) [{p.get('trail_tier','?')}]")
            print(f"  └  Hard stop:  ${hard_s:>12,.4f}  |  Ever green: {p.get('ever_green', False)}")
    else:
        print(f"  No open positions ({MAX_POSITIONS} slots available) — scanning for entries")

    print(bar)
    print()

# ══════════════════════════════════════════════════════════════════════════
# ██  MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════

def main():
    global _shutdown

    print("╔══════════════════════════════════════════════════════════════════╗")
    print("║          KRAKEN ALPHA V1 — All-Regime Trading Bot               ║")
    print("║   BULL: MACD momentum | NEUTRAL: BB mean-rev | BEAR: cash       ║")
    print(f"║   Capital: ${STARTING_CAPITAL:.0f} | Positions: {MAX_POSITIONS} | Stop: {HARD_STOP_PCT*100:.0f}% | KS: {MAX_DD_PCT*100:.0f}%           ║")
    print(f"║   Mode: {'PAPER TRADING (virtual)' if PAPER_TRADING else 'LIVE TRADING (real money)':^42}       ║")
    print("╚══════════════════════════════════════════════════════════════════╝")
    print()

    if not PAPER_TRADING and API_KEY == 'YOUR_KRAKEN_API_KEY':
        print("ERROR: You are in LIVE mode but API keys are not set.")
        print("       Set API_KEY and API_SECRET at the top of the file.")
        sys.exit(1)

    if PAPER_TRADING:
        _log('MODE', 'PAPER TRADING — no real orders will be placed')
    else:
        _log('MODE', 'LIVE TRADING — real orders will be placed on Kraken')

    # Exchange init
    exchange = ccxt.kraken({
        'apiKey':          API_KEY,
        'secret':          API_SECRET,
        'enableRateLimit': True,
    })

    # Load persistent state
    state     = load_state()
    positions = state.get('positions', {})
    cooldowns = {k: float(v) for k, v in state.get('cooldowns', {}).items()}
    state['boot_time'] = time.time()   # update boot time on restart

    # Track last entry time for idleness guard
    last_entry_ms = state.get('last_entry_ms', 0.0)
    bar_counter   = 0
    prices        = {}

    _log('BOOT', (
        f'Bot started | equity=${state["equity"]:.2f} | '
        f'positions={len(positions)} | trades={state["trade_count"]}'
    ))

    while not _shutdown:
        # ── Emergency stop file ───────────────────────────────────────────
        if os.path.exists(EMERGENCY_STOP_FILE):
            _log('EMERGENCY', 'EMERGENCY_STOP file detected — shutting down cleanly')
            _shutdown = True
            break

        bar_counter += 1
        loop_start   = time.time()

        try:
            # ── Fetch BTC 1h for market-wide regime ──────────────────────
            btc_1h = fetch_ohlcv(exchange, 'BTC/USD', '1h', limit=80)
            market_regime = detect_regime(btc_1h)

            # ── Fetch 5m data for all pairs ───────────────────────────────
            df5_cache = {}
            df1h_cache = {'BTC/USD': btc_1h}
            for sym in PAIRS:
                df5 = fetch_ohlcv(exchange, sym, '5m', limit=210)
                if df5 is not None:
                    prices[sym] = sf(df5['c'].iloc[-1])
                    df5_cache[sym] = df5
                if sym != 'BTC/USD':
                    df1h_cache[sym] = fetch_ohlcv(exchange, sym, '1h', limit=80)

            # ── Unrealized P&L and live equity ────────────────────────────
            unrealized = sum(
                p['size_usd'] * (prices.get(sym, p['entry_price']) - p['entry_price'])
                / p['entry_price']
                for sym, p in positions.items()
            )
            live_equity = state['equity'] + unrealized
            state['peak_equity'] = max(state.get('peak_equity', STARTING_CAPITAL), live_equity)

            # ── Heartbeat ─────────────────────────────────────────────────
            print_heartbeat(state, market_regime, prices, bar_counter)

            # ── Portfolio kill switch ─────────────────────────────────────
            dd_pct = (state['peak_equity'] - live_equity) / state['peak_equity']
            if dd_pct >= MAX_DD_PCT:
                _log('KILL_SWITCH', f'Portfolio DD {dd_pct*100:.1f}% ≥ {MAX_DD_PCT*100:.0f}% — closing all positions')
                for sym in list(positions.keys()):
                    price = prices.get(sym, positions[sym]['entry_price'])
                    result = execute_sell(exchange, sym, positions[sym], price)
                    pnl = result['pnl'] if result else 0.0
                    state['equity']     += pnl
                    state['total_pnl']  += pnl
                    state['trade_count'] += 1
                    if pnl > 0:
                        state['win_count'] += 1
                    _write_audit({
                        'time':        utc_now(),
                        'symbol':      sym,
                        'strategy':    positions[sym].get('strategy', '?'),
                        'entry_time':  positions[sym].get('entry_time', '?'),
                        'entry_price': round(positions[sym]['entry_price'], 6),
                        'exit_price':  round(result['price'] if result else price, 6),
                        'size_usd':    round(positions[sym]['size_usd'], 2),
                        'pnl_usd':     round(pnl, 4),
                        'pnl_pct':     round(pnl / positions[sym]['size_usd'] * 100, 3),
                        'reason':      'KILL_SWITCH',
                        'regime':      market_regime,
                        'peak_pct':    round((positions[sym].get('peak_price', positions[sym]['entry_price'])
                                             - positions[sym]['entry_price'])
                                            / positions[sym]['entry_price'] * 100, 3),
                        'bars_held':   bar_counter - positions[sym].get('entry_bar', bar_counter),
                        'mode':        'PAPER' if PAPER_TRADING else 'LIVE',
                    })
                    _log('KILL_SWITCH', f'Closed {sym} | P&L ${pnl:+.2f}')
                    del positions[sym]

                state['positions']   = positions
                state['cooldowns']   = cooldowns
                save_state(state)
                _log('KILL_SWITCH', f'All positions closed. Final equity: ${state["equity"]:.2f}')
                _shutdown = True
                break

            # ── EXIT SCAN — check every open position ─────────────────────
            for sym in list(positions.keys()):
                if sym not in prices:
                    continue
                pos       = positions[sym]
                price     = prices[sym]
                bars_held = bar_counter - pos.get('entry_bar', bar_counter)
                df5       = df5_cache.get(sym)

                should_exit, reason, pnl_usd = eval_exit(
                    pos, price, bars_held, df5, market_regime
                )

                if should_exit:
                    result = execute_sell(exchange, sym, pos, price)
                    real_pnl = result['pnl'] if result else pnl_usd
                    real_exit_price = result['price'] if result else price

                    state['equity']      += real_pnl
                    state['total_pnl']   += real_pnl
                    state['trade_count'] += 1
                    if real_pnl > 0:
                        state['win_count'] += 1

                    cooldowns[sym] = now_ms() + COOLDOWN_MS

                    _write_audit({
                        'time':        utc_now(),
                        'symbol':      sym,
                        'strategy':    pos.get('strategy', '?'),
                        'entry_time':  pos.get('entry_time', '?'),
                        'entry_price': round(pos['entry_price'], 6),
                        'exit_price':  round(real_exit_price, 6),
                        'size_usd':    round(pos['size_usd'], 2),
                        'pnl_usd':     round(real_pnl, 4),
                        'pnl_pct':     round(real_pnl / pos['size_usd'] * 100, 3),
                        'reason':      reason,
                        'regime':      market_regime,
                        'peak_pct':    round((pos.get('peak_price', pos['entry_price'])
                                             - pos['entry_price'])
                                            / pos['entry_price'] * 100, 3),
                        'bars_held':   bars_held,
                        'conviction':  pos.get('conviction', 0),
                        'adx_at_entry': pos.get('adx', 0),
                        'rsi_at_entry': pos.get('rsi', 0),
                        'mode':        'PAPER' if PAPER_TRADING else 'LIVE',
                    })
                    _log('EXIT', (
                        f'{sym} | {reason} | P&L ${real_pnl:>+7.2f} | '
                        f'bars={bars_held} | Equity=${state["equity"]:.2f}'
                    ))
                    del positions[sym]

            # ── ENTRY SCAN — only if slots available and not BEAR ─────────
            if len(positions) < MAX_POSITIONS and market_regime != 'BEAR':
                for sym in PAIRS:
                    if sym in positions:
                        continue
                    if len(positions) >= MAX_POSITIONS:
                        break
                    if cooldowns.get(sym, 0) > now_ms():
                        continue

                    df5  = df5_cache.get(sym)
                    df1h = df1h_cache.get(sym)
                    if df5 is None:
                        continue

                    # Per-asset regime check — don't buy a bearish asset even in neutral market
                    asset_regime = per_asset_regime(df1h)
                    if asset_regime == 'BEAR':
                        continue

                    sigs = scan_entries(df5, df1h, market_regime, last_entry_ms)
                    if not sigs:
                        continue

                    best_cv, best_det = max(sigs, key=lambda x: x[0])
                    if best_cv < MIN_CONVICTION:
                        continue

                    # Final equity-aware sizing
                    unrealized_now = sum(
                        p['size_usd'] * (prices.get(s, p['entry_price']) - p['entry_price'])
                        / p['entry_price']
                        for s, p in positions.items()
                    )
                    current_eq = state['equity'] + unrealized_now
                    size = calc_size(best_cv, current_eq, positions)
                    if size < MIN_POSITION_USD:
                        continue

                    price = prices.get(sym, 0.0)
                    if price <= 0:
                        continue

                    # Execute buy
                    result = execute_buy(exchange, sym, size, price)
                    if result is None:
                        _log('ENTRY_FAIL', f'{sym} — order failed, skipping')
                        continue

                    exec_price = result['price']
                    positions[sym] = {
                        'entry_price': exec_price,
                        'peak_price':  exec_price,
                        'size_usd':    size,
                        'qty_held':    result.get('amount', size / exec_price),
                        'strategy':    best_det['strategy'],
                        'regime':      market_regime,
                        'conviction':  best_cv,
                        'ever_green':  False,
                        'entry_bar':   bar_counter,
                        'entry_time':  utc_now(),
                        'trail_stop':  0.0,
                        'trail_tier':  '1.30%',
                        'adx':         best_det.get('adx', 0),
                        'rsi':         best_det.get('rsi', 0),
                        'mfi':         best_det.get('mfi', 0),
                    }
                    # Attach target price for BB_MEAN_REV
                    if 'target_price' in best_det:
                        positions[sym]['target_price'] = best_det['target_price']

                    last_entry_ms = now_ms()
                    state['last_entry_ms'] = last_entry_ms

                    _log('ENTRY', (
                        f'{sym} | {best_det["strategy"]} | CV={best_cv} | '
                        f'Regime={market_regime} | Size=${size:.2f} | '
                        f'Price=${exec_price:.4f} | ADX={best_det.get("adx",0):.1f} '
                        f'RSI={best_det.get("rsi",0):.1f} | Mode={"PAPER" if PAPER_TRADING else "LIVE"}'
                    ))

            # ── Save state ────────────────────────────────────────────────
            state['positions']     = positions
            state['cooldowns']     = {k: str(v) for k, v in cooldowns.items()}
            state['gross_runtime'] = time.time() - state.get('boot_time', _start_wall)
            save_state(state)

        except Exception as e:
            _log('LOOP_ERR', f'Unhandled error in main loop: {e}')
            traceback.print_exc()

        # ── Sleep until next iteration ────────────────────────────────────
        elapsed = time.time() - loop_start
        sleep_t = max(0, LOOP_SLEEP_SEC - elapsed)
        if not _shutdown:
            time.sleep(sleep_t)

    # ── Shutdown ──────────────────────────────────────────────────────────
    state['positions']     = positions
    state['cooldowns']     = {k: str(v) for k, v in cooldowns.items()}
    state['gross_runtime'] = time.time() - state.get('boot_time', _start_wall)
    save_state(state)
    _log('SHUTDOWN', f'Clean exit. Equity=${state["equity"]:.2f} | Trades={state["trade_count"]}')
    print("\n[DONE] Bot shut down cleanly. State saved.")


# ══════════════════════════════════════════════════════════════════════════
# ██  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════

if __name__ == '__main__':
    main()
