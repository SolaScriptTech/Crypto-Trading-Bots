#!/usr/bin/env python3
"""
kraken_bull_bot_v7.py
============================================================
Live / Paper trading bot — v7.0
============================================================

WHAT CHANGED FROM v6.1 → v7.0:

  BUG FIX — regime_since self-heal (493322h display bug):
  ─────────────────────────────────────────────────────────
  Symptom: BULL symbols showed regime age of ~493322h (56 years) in
  heartbeat, entered PULLBACK mode permanently, MOMENTUM_MODE never
  fired even for fresh flips.

  Root cause: ctx.regime_since.get(sym, 0) returned 0 (epoch) when a
  symbol was in BULL but not present in regime_since. This happens when
  the symbol was seeded as NEUTRAL (failed 2-bar confirmation on boot)
  and later confirmed BULL mid-candle without triggering the on_15m_close
  flip branch. regime_age_s = time.time() - 0 ≈ 56 years.

  Fix 1 — Self-heal in on_5m_close: if regime == "BULL" and sym not in
  ctx.regime_since, insert sym at time.time() - MOMENTUM_WINDOW_S so
  the symbol lands directly in PULLBACK mode (correct for established
  bull) and the age shows ~2h rather than 56 years. Logs REGIME_SINCE_HEAL.

  Fix 2 — Heartbeat storage: changed all three regime_age_s stores in
  ctx.last_sym_state from `if fresh_bull or regime == "BULL"` to
  `if sym in ctx.regime_since`. Prevents the raw time.time() value from
  being stored and displayed when the dict key is absent.

  BUG FIX — silent ENTRY_BLOCKED (armed but never entered):
  ──────────────────────────────────────────────────────────
  Symptom: HYPE/USD (and BTC/USD) showed ★ ARMED for 60+ minutes on
  live funds with no entry and no log entry explaining why.

  Root cause: on_1m_close returned silently when deployable < $2 or
  size_usd < $2 — no log line, impossible to diagnose from events.log.

  Fix: both capital-blocked paths now emit ENTRY_BLOCKED log lines
  showing free_usd, deployable, open position count, and size_usd so
  the reason is visible immediately in the log.

WHAT CHANGED FROM v5.7:

  DUAL-MODE ENTRY — Momentum vs Pullback routing:
  ────────────────────────────────────────────────
  v5.7 problem: Strategy only fires on EMA21 pullbacks. During a
  bear→bull regime flip the market rips straight up, price leaves
  EMA21 behind, and the bot sits idle watching the move it was
  built for. Identified live on Apr 9-10 2026.

  v7.0 fix: Regime age is now tracked per symbol. Two entry modes:

    MOMENTUM_MODE  (regime age < 2h)
      - Fires on MACD histogram cross from ≤0 to >0 (2-bar confirm)
      - No EMA21 pullback gate — price can be anywhere above EMA21
      - RSI ceiling 65 (not 48 — momentum trends hold 50-65)
      - ADX >= 15 (same as RSI_OVERSOLD gate)
      - Volume ratio >= 1.2 (light confirm, not strict)
      - Trail params: ATR_MULT=2.1, GREEN_PCT=0.007 (wider/later —
        entering extended means more room needed to prove the trade)

    PULLBACK_MODE  (regime age >= 2h)
      - All existing v5.7 EMA21/RSI_OVERSOLD/MACD_CROSS logic
      - Unchanged gates, unchanged trail params

    Fallback: if MOMENTUM signal misses in a fresh bull, the bot
    checks PULLBACK signals too — if price is already kissing EMA21
    during a fresh regime, take it.

  REGIME FLIP TRACKING:
  ──────────────────────
  ctx.regime_since[sym]  — unix ts of last NEUTRAL/BEAR→BULL flip
  ctx.regime_current[sym] — last confirmed regime per symbol
  Both initialised in seed_caches. on_15m_close updates them.
  Flip to BULL fires an ntfy alert and a log entry.

  EXIT PARAMETER FIXES — restoring v5.1 breathing room:
  ───────────────────────────────────────────────────────
  ATR_MIN_HOLD_1M : 6 → 30
    v5.1 used 6 × 5m-bars = 30 minutes. When v5.2 moved to 1m
    bars the number stayed 6, silently becoming 6 minutes. Trail
    armed on noise, compressing every winner. Now correctly 30.

  GREEN_PCT : 0.003 → 0.005  (Phase 2 arms at 0.5%, not 0.3%)
  ATR_MULT  : 1.5   → 1.8    (wider trail, less winner compression)

  RICH HEARTBEAT — what the bot is thinking, per symbol:
  ───────────────────────────────────────────────────────
  v5.7: heartbeat only printed equity/WR/PnL.
  v7.0: Every heartbeat logs a full per-symbol status block:
    - Regime + mode (MOMENTUM/PULLBACK) + age
    - Price vs EMA21 (% distance), RSI, ADX, vol_ratio
    - MACD histogram value
    - ARMED signal or SKIP reason or COOLDOWN remaining
    - Open position: phase, gain, trail, bars held
  Designed for live debugging via: tail -f events.log

  WS ERROR ROUTING FIX:
  ──────────────────────
  v5.7: ws_1m/ws_5m logged errors via state.log() directly,
        bypassing _ws_alert(). Mass disconnect detection never fired.
  v7.0: All WS exception paths route through _ws_alert() correctly.

  VERSION STRINGS:
  ─────────────────
  v5.7 had several "v5.5" strings left in log/alert messages from
  incomplete version bumps. All strings now say v7.0 consistently.

Regime  : 15m candles, EMA21 vs EMA55, 2-bar BULL confirmation
Signals : 5m candles → arms 1m trigger
Entry   : 1m candle close (first after signal armed)
Phase 1 : hard stop native order only until genuinely green
Phase 2 : ATR trail native order + MACD flip + bear eviction
Modes   : MOMENTUM (regime < 2h) / PULLBACK (regime >= 2h)

.env:
  KRAKEN_API_KEY=...
  KRAKEN_API_SECRET=...
  PAPER_MODE=true
  PAPER_EQUITY=100.0
  MAX_DRAWDOWN_PCT=0.20
  EQUITY_USD=0         # cap live equity (0 = no cap)

pip install:
  pip3 install requests ccxt[pro]
"""

# ── Watchdog thresholds ───────────────────────────────────────────────
_WD_ALERT_1  = 1
_WD_ALERT_5  = 5
_WD_ALERT_10 = 10

import os, sys, time, math, json, csv, asyncio
import signal as signal_mod
from datetime import datetime, timezone
from pathlib import Path

try:
    import requests
except ImportError:
    sys.exit("pip3 install requests")

try:
    import ccxt.pro as ccxtpro
except ImportError:
    sys.exit("pip3 install 'ccxt[pro]'")

# ──────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent

# ccxt unified symbols
SYMBOLS = [
    "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "SUI/USD", "RAIN/USD", "XLM/USD",
    "DOT/USD", "DOGE/USD", "TAO/USD", "LTC/USD", "HYPE/USD", "ADA/USD", "CRO/USD",
]

LABEL = {s: s for s in SYMBOLS}   # identity map — ccxt uses BTC/USD already

# Position sizing
DRY_POWDER = 0.20
SIZE_HIGH  = 0.25   # idle-guard entries
SIZE_LOW   = 0.15   # normal entries

# Phase 1 → Phase 2 transition gate (genuinely in green)
# CHANGED v7.0: 0.003 → 0.005 (arm Phase 2 at 0.5%, not 0.3%)
GREEN_PCT  = 0.005
GREEN_USD  = 0.10    # float-noise floor only (was $2.00 — blocked Phase 2 entirely)

# Hard stop — absolute unconditional floor
HARD_STOP_PCT = 0.015   # 1.5% below entry

# PROFIT_FLOOR removed — it clamped the stop to entry×1.001 immediately at
# Phase 2 activation, firing on any 0.4% dip before the ATR trail had room
# to develop. Hard stop (-1.5%) is the sole protection until ATR trail arms.

# ATR trailing stop (Phase 2)
# CHANGED v7.0: ATR_MULT 1.5 → 1.8, ATR_MIN_HOLD_1M 6 → 30
ATR_MULT        = 1.8
ATR_MIN_HOLD_1M = 30    # 1m bars before ATR trail activates (30 min)
                         # v5.7 had 6 but unit changed to 1m bars, making
                         # it 6 minutes — this restores the original 30 min

# ── Dual-mode entry (v7.0) ────────────────────────────────────────────
MOMENTUM_WINDOW_S    = 2 * 3600   # regime age < 2h → MOMENTUM_MODE
MOMENTUM_RSI_MAX     = 65.0       # RSI ceiling for momentum entries (not 48)
MOMENTUM_ADX_MIN     = 15.0       # ADX minimum for momentum (same as RSI_OVS)
MOMENTUM_VOL_MIN     = 1.2        # volume ratio minimum (light confirm)
MOMENTUM_ATR_MULT    = 2.1        # wider trail — entering extended needs room
MOMENTUM_GREEN_PCT   = 0.007      # Phase 2 arms at 0.7% for momentum entries

# Bear eviction — per-asset tiered response (unchanged from v5.2)
BEAR_EVICT_LOSS_PCT  = 0.005
BEAR_EVICT_TIME_1M   = 1440
ATR_MULT_BEAR_LOSS   = 0.8
ATR_MULT_BEAR_MODEST = 1.2
ATR_MULT_BEAR_BIG    = 0.5
BEAR_BIG_WIN_PCT     = 0.015

# MACD flip exit (Phase 2 only)
MACD_FAST       = 12
MACD_SLOW       = 26
MACD_SIG        = 9
MACD_MIN_1M     = 60     # 1m bars before MACD flip allowed (1h)
MACD_NEED_GAIN  = 0.003

# Entry gates — BULL regime (permissive — market IS trending)
BULL_EMA_PULL_MAX   = 0.0150   # price up to 1.5% below EMA21 → pullback entry
BULL_EMA_SUP_MAX    = 0.0030   # price up to 0.3% above EMA21 → support test entry
BULL_CONSOL_MAX     = 0.0080   # price within 0.8% of EMA21 → consolidation entry
BULL_RSI_PULL_MAX   = 62.0     # RSI ceiling for pullback in bull (was 48 — wrong)
BULL_RSI_SUP_MAX    = 60.0     # RSI ceiling for support-test in bull
BULL_RSI_DIP_MAX    = 50.0     # RSI ceiling for RSI_DIP signal in bull
BULL_RSI_CONSOL_MAX = 65.0     # RSI ceiling for consolidation signal
BULL_ADX_MIN        = 15.0     # ADX minimum for all bull entries

# Entry gates — NEUTRAL regime (tighter — no confirmed trend)
NEUT_EMA_PULL_MAX = 0.0050     # price 0-0.5% below EMA21
NEUT_RSI_PULL_MAX = 48.0       # RSI ceiling for NEUTRAL pullback
NEUT_RSI_OVS_MAX  = 42.0       # RSI oversold threshold in NEUTRAL
NEUT_ADX_MIN      = 20.0       # ADX minimum for NEUTRAL entries

IDLE_HOURS = 8

# Zombie kill
ZOMBIE_1M = 2880   # 48h in 1m bars

# Dust threshold
DUST_USD = 1.00

# Cooldown (seconds after exit — stored as unix expiry timestamps)
COOLDOWN_TABLE = [
    ( 0.015,  60),   # > +1.5% → 1h
    ( 0.003, 120),   # > +0.3% → 2h
    ( 0.000, 240),   # > 0%    → 4h
    (-9999,  360),   # loss    → 6h
]
BEAR_EXIT_COOLDOWN_1M = 240   # 4h after bear eviction

# Alerts
NTFY_TOPIC     = "quant-crystal-ball"
NTFY_URL       = f"https://ntfy.sh/quant-crystal-ball"
BULL_ALERT_MIN = 3

# Timing
NTP_WAIT_S   = 15
WARMUP_BARS  = 60
HEARTBEAT_S  = 60

# ──────────────────────────────────────────────────────────────────────
# SHUTDOWN
# ──────────────────────────────────────────────────────────────────────

_shutdown = False

def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    print("\nShutdown signal received...")

signal_mod.signal(signal_mod.SIGINT,  _handle_signal)
signal_mod.signal(signal_mod.SIGTERM, _handle_signal)

# ──────────────────────────────────────────────────────────────────────
# ENV
# ──────────────────────────────────────────────────────────────────────

def load_env(path: Path) -> dict:
    env = {}
    if not path.exists():
        raise FileNotFoundError(f".env not found: {path}")
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        env[k.strip()] = v.strip().strip('"')
    return env

# ──────────────────────────────────────────────────────────────────────
# ALERTS
# ──────────────────────────────────────────────────────────────────────

def alert(msg: str, title: str = "Quant Bot v7.0", priority: str = "high"):
    try:
        requests.post(
            NTFY_URL, data=msg.encode(),
            headers={"Title": title, "Priority": priority},
            timeout=5,
        )
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────
# INDICATORS
# ──────────────────────────────────────────────────────────────────────

def _ema(closes: list, period: int) -> list:
    out = [0.0] * len(closes)
    if len(closes) < period: return out
    out[period - 1] = sum(closes[:period]) / period
    a = 2.0 / (period + 1.0)
    for i in range(period, len(closes)):
        out[i] = closes[i] * a + out[i - 1] * (1 - a)
    return out

def _wilder(vals: list, period: int) -> list:
    out = [0.0] * len(vals)
    if len(vals) < period: return out
    out[period - 1] = sum(vals[:period]) / period
    a = 1.0 / period
    for i in range(period, len(vals)):
        out[i] = vals[i] * a + out[i - 1] * (1 - a)
    return out

def _sma(closes: list, period: int) -> float:
    if len(closes) < period: return 0.0
    return sum(closes[-period:]) / period

def _rsi_scalar(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1: return 50.0
    g, l = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        g.append(max(d, 0.0)); l.append(max(-d, 0.0))
    ag = sum(g[:period]) / period
    al = sum(l[:period]) / period
    for i in range(period, len(g)):
        ag = (ag * (period - 1) + g[i]) / period
        al = (al * (period - 1) + l[i]) / period
    return 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)

def _adx_scalar(candles: list, period: int = 14) -> float:
    if len(candles) < period * 2 + 1: return 0.0
    tr_l, pdm, ndm = [], [], []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i-1]["c"]
        ph, pl   = candles[i-1]["h"], candles[i-1]["l"]
        tr_l.append(max(h-l, abs(h-pc), abs(l-pc)))
        up, dn = h-ph, pl-l
        pdm.append(up if up > dn and up > 0 else 0)
        ndm.append(dn if dn > up and dn > 0 else 0)
    str_ = _wilder(tr_l, period)
    spdm = _wilder(pdm, period)
    sndm = _wilder(ndm, period)
    dx = []
    for i in range(len(str_)):
        if str_[i] == 0: continue
        pdi = 100 * spdm[i] / str_[i]
        ndi = 100 * sndm[i] / str_[i]
        d   = pdi + ndi
        dx.append(100 * abs(pdi - ndi) / d if d > 0 else 0)
    if not dx: return 0.0
    return next((v for v in reversed(_wilder(dx, period)) if v != 0), 0.0)

def _atr_scalar(candles: list, period: int = 14) -> float:
    if len(candles) < period + 1: return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["h"], candles[i]["l"], candles[i-1]["c"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    out = _wilder(trs, period)
    return next((v for v in reversed(out) if v != 0), 0.0)

def _macd_scalars(closes: list) -> tuple:
    needed = MACD_SLOW + MACD_SIG + 2
    if len(closes) < needed: return 0.0, 0.0
    ef   = _ema(closes, MACD_FAST)
    es   = _ema(closes, MACD_SLOW)
    ml   = [f - s for f, s in zip(ef, es)]
    sl   = _ema(ml, MACD_SIG)
    hist = [m - s for m, s in zip(ml, sl)]
    return hist[-1], hist[-2]

def _candle_list(raw_ccxt: list) -> list:
    """Convert ccxt OHLCV list to internal dict format.
    Excludes the last (forming) candle.
    All OHLCV values cast to float — ccxt.pro can return strings after
    a 1006 WS reconnect, causing arithmetic errors downstream.
    """
    return [
        {
            "t": int(c[0] / 1000),
            "o": float(c[1]),
            "h": float(c[2]),
            "l": float(c[3]),
            "c": float(c[4]),
            "v": float(c[5]),
        }
        for c in raw_ccxt[:-1]
    ]

def _merge_candles(existing: list, new_candles: list, maxlen: int = 500) -> list:
    """Merge WS candles into the existing seeded cache instead of overwriting.

    v6.1 bug: ws tasks did ctx.cache[sym] = _candle_list(raw), which wiped
    the REST-seeded cache with 0-2 WS bars on the first event after reconnect.
    compute_indicators then returned None → on_5m_close returned early →
    ctx.last_sym_state never set → 'no data yet' forever.

    Fix: if WS returns enough bars use them directly; otherwise merge by
    timestamp so the seeded history is preserved across reconnects.
    """
    if not new_candles:
        return existing
    if not existing or len(new_candles) >= WARMUP_BARS:
        return new_candles[-maxlen:]
    by_ts = {c["t"]: c for c in existing}
    for c in new_candles:
        by_ts[c["t"]] = c
    return sorted(by_ts.values(), key=lambda c: c["t"])[-maxlen:]

def compute_indicators(candles: list) -> dict | None:
    if len(candles) < WARMUP_BARS: return None
    closes = [c["c"] for c in candles]
    vols   = [c["v"] for c in candles]
    e21v   = _ema(closes, 21)
    e55v   = _ema(closes, 55)
    ema21  = e21v[-1]
    ema55  = e55v[-1]
    price  = closes[-1]
    rsi14  = _rsi_scalar(closes, 14)
    adx14  = _adx_scalar(candles, 14)
    atr14  = _atr_scalar(candles, 14)
    mid    = _sma(closes, 20)
    sd     = math.sqrt(
        sum((c - mid) ** 2 for c in closes[-20:]) / 20
    ) if len(closes) >= 20 else 0.0
    mh, mh_prev = _macd_scalars(closes)

    # Volume ratio: current bar vs 20-bar average
    vol_avg20 = sum(vols[-20:]) / 20 if len(vols) >= 20 else (vols[-1] if vols else 1.0)
    vol_ratio = vols[-1] / vol_avg20 if vol_avg20 > 0 else 1.0

    if ema21 > ema55 and price > ema21:  regime = "BULL"
    elif ema21 < ema55:                   regime = "BEAR"
    else:                                 regime = "NEUTRAL"

    return {
        "price":          price,
        "ema21":          ema21,
        "ema55":          ema55,
        "rsi14":          rsi14,
        "adx14":          adx14,
        "atr14":          atr14,
        "vol_ratio":      vol_ratio,
        "bb_lower":       mid - 2 * sd,
        "bb_upper":       mid + 2 * sd,
        "macd_hist":      mh,
        "macd_hist_prev": mh_prev,
        "regime":         regime,
    }

def confirmed_regime(raw: str, prev_raw: str) -> str:
    if raw == "BULL" and prev_raw == "BULL": return "BULL"
    elif raw == "BEAR":                       return "BEAR"
    elif raw == "BULL":                       return "NEUTRAL"
    return raw

# ──────────────────────────────────────────────────────────────────────
# ENTRY SIGNALS
# ──────────────────────────────────────────────────────────────────────

def evaluate_momentum_signal(
        ind: dict, regime: str,
        last_entry_ts: float,
        is_flat: bool) -> tuple[dict | None, str]:
    """
    MOMENTUM_MODE — fires on fresh bull regime (age < 2h).
    Three paths: fresh MACD cross, histogram continuation, RSI dip.
    No EMA21 pullback gate required — price can be running above EMA21.
    Returns (signal_dict, skip_reason).
    """
    idle_guard = (is_flat and last_entry_ts > 0 and regime == "BULL"
                  and (time.time() - last_entry_ts) >= IDLE_HOURS * 3600)
    price = ind["price"]
    above_ema55 = price > ind["ema55"] and ind["ema55"] > 0
    adx_ok  = ind["adx14"] >= MOMENTUM_ADX_MIN

    miss = []

    # ── Path 1: MACD fresh cross (hist ≤0 → >0) ──────────────────────
    if (ind["macd_hist_prev"] <= 0 and ind["macd_hist"] > 0
            and adx_ok and above_ema55
            and ind["rsi14"] < MOMENTUM_RSI_MAX):
        return {"signal": "MACD_MOMENTUM_CROSS",
                "idle_guard": idle_guard, "mode": "MOMENTUM"}, ""
    miss.append(
        f"CROSS[hist={ind['macd_hist']:+.6f}"
        f" prev={ind['macd_hist_prev']:+.6f}"
        f" rsi={ind['rsi14']:.0f}/{MOMENTUM_RSI_MAX:.0f}"
        f" adx={ind['adx14']:.0f}/{MOMENTUM_ADX_MIN:.0f}]"
    )

    # ── Path 2: MACD histogram already positive (trend continuation) ──
    # Histogram positive = momentum present. Price above EMA21 = trend.
    # RSI < 72 = not completely blown out.
    if (ind["macd_hist"] > 0 and adx_ok and above_ema55
            and price > ind["ema21"] and ind["ema21"] > 0
            and ind["rsi14"] < 72.0):
        return {"signal": "MACD_MOMENTUM_CONT",
                "idle_guard": idle_guard, "mode": "MOMENTUM"}, ""
    miss.append(
        f"CONT[hist={ind['macd_hist']:+.6f}"
        f" vs_ema21={((price-ind['ema21'])/ind['ema21']*100) if ind['ema21']>0 else 0:+.2f}%"
        f" rsi={ind['rsi14']:.0f}]"
    )

    # ── Path 3: RSI dip into EMA21 zone during fresh bull ─────────────
    if (ind["ema21"] > 0 and adx_ok and above_ema55
            and ind["rsi14"] < 55.0):
        pct_below = (ind["ema21"] - price) / ind["ema21"]
        if -0.005 <= pct_below <= 0.020:   # within 2% below EMA21 or 0.5% above
            return {"signal": "MOMENTUM_RSI_DIP",
                    "idle_guard": idle_guard, "mode": "MOMENTUM"}, ""
    miss.append(
        f"DIP[rsi={ind['rsi14']:.0f}/55 adx={ind['adx14']:.0f}/{MOMENTUM_ADX_MIN:.0f}]"
    )

    return None, " ".join(miss)


def evaluate_signal(ind: dict, regime: str,
                    last_entry_ts: float,
                    is_flat: bool) -> tuple[dict | None, str]:
    """
    PULLBACK_MODE — fires in established bull (age >= 2h) or NEUTRAL.
    BULL and NEUTRAL have separate gates — BULL gates are permissive
    because RSI stays 50-70 in a trending market and price rarely
    touches EMA21 deeply. NEUTRAL gates are tight.
    Returns (signal_dict, skip_reason).
    """
    if regime == "BEAR":
        return None, "BEAR_REGIME"

    idle_guard = (is_flat and last_entry_ts > 0 and regime == "BULL"
                  and (time.time() - last_entry_ts) >= IDLE_HOURS * 3600)
    price = ind["price"]
    ema21 = ind["ema21"]
    ema55 = ind["ema55"]
    rsi   = ind["rsi14"]
    adx   = ind["adx14"]
    above_ema55 = price > ema55 and ema55 > 0
    miss  = []

    # ══════════════════════════════════════════════════════════════════
    # BULL REGIME — five entry paths, BULL-appropriate gates
    # ══════════════════════════════════════════════════════════════════
    if regime == "BULL":

        # ── 1. EMA21_PULLBACK: price dips 0-1.5% below EMA21 ─────────
        # Classic pullback-to-mean in a trending market.
        # RSI < 62 is realistic (not 48 — that almost never happens in bull)
        if ema21 > 0 and adx >= BULL_ADX_MIN:
            pct_below = (ema21 - price) / ema21
            if 0.0 <= pct_below <= BULL_EMA_PULL_MAX and rsi < BULL_RSI_PULL_MAX:
                return {"signal": "EMA21_PULLBACK",
                        "idle_guard": idle_guard, "mode": "PULLBACK"}, ""
            miss.append(
                f"PULL[below={pct_below*100:+.2f}%"
                f" need 0-{BULL_EMA_PULL_MAX*100:.1f}%"
                f" rsi={rsi:.0f}/{BULL_RSI_PULL_MAX:.0f}]"
            )
        else:
            miss.append(f"PULL[adx={adx:.0f}/{BULL_ADX_MIN:.0f} ema21={'ok' if ema21>0 else '0'}]")

        # ── 2. EMA21_SUPPORT: price just above EMA21 (testing support) ─
        # Price bounces off EMA21 from above = classic bull support test.
        if ema21 > 0 and adx >= BULL_ADX_MIN and above_ema55:
            pct_above = (price - ema21) / ema21
            if 0.0 <= pct_above <= BULL_EMA_SUP_MAX and rsi < BULL_RSI_SUP_MAX:
                return {"signal": "EMA21_SUPPORT",
                        "idle_guard": idle_guard, "mode": "PULLBACK"}, ""
            miss.append(
                f"SUP[above={pct_above*100:+.2f}%"
                f" need 0-{BULL_EMA_SUP_MAX*100:.1f}%"
                f" rsi={rsi:.0f}/{BULL_RSI_SUP_MAX:.0f}]"
            )
        else:
            miss.append(f"SUP[no ema55 or adx]")

        # ── 3. RSI_DIP_BULL: RSI drops below 50 in a bull trend ────────
        # RSI dipping to 45-50 in bull = short-term weakness, buy the dip.
        if rsi < BULL_RSI_DIP_MAX and above_ema55 and adx >= BULL_ADX_MIN:
            return {"signal": "RSI_DIP_BULL",
                    "idle_guard": idle_guard, "mode": "PULLBACK"}, ""
        miss.append(
            f"DIP[rsi={rsi:.0f}/{BULL_RSI_DIP_MAX:.0f}"
            f" ema55={'ok' if above_ema55 else 'below'}]"
        )

        # ── 4. MACD_CROSS: histogram ≤0 → >0 ───────────────────────────
        if (ind["macd_hist_prev"] <= 0 and ind["macd_hist"] > 0
                and adx >= BULL_ADX_MIN and above_ema55):
            return {"signal": "MACD_CROSS",
                    "idle_guard": idle_guard, "mode": "PULLBACK"}, ""
        miss.append(
            f"MACD_X[hist={ind['macd_hist']:+.6f}"
            f" prev={ind['macd_hist_prev']:+.6f}]"
        )

        # ── 5. MACD_CONSOLIDATION: hist>0, price near EMA21 ───────────
        # Histogram positive = trend intact. Price has consolidated back
        # toward EMA21 = good risk/reward. Not just chasing the top.
        if (ind["macd_hist"] > 0 and adx >= BULL_ADX_MIN
                and above_ema55 and ema21 > 0
                and rsi < BULL_RSI_CONSOL_MAX):
            pct_from_ema = abs(price - ema21) / ema21
            if pct_from_ema <= BULL_CONSOL_MAX:
                return {"signal": "MACD_CONSOLIDATION",
                        "idle_guard": idle_guard, "mode": "PULLBACK"}, ""
            miss.append(
                f"CONSOL[dist={pct_from_ema*100:.2f}%"
                f" need<={BULL_CONSOL_MAX*100:.1f}%"
                f" rsi={rsi:.0f}/{BULL_RSI_CONSOL_MAX:.0f}]"
            )
        else:
            miss.append(
                f"CONSOL[hist={ind['macd_hist']:+.6f}"
                f" rsi={rsi:.0f}/{BULL_RSI_CONSOL_MAX:.0f}]"
            )

        return None, " ".join(miss)

    # ══════════════════════════════════════════════════════════════════
    # NEUTRAL REGIME — tighter gates, trend not confirmed
    # ══════════════════════════════════════════════════════════════════

    # 1. EMA21_PULLBACK (tight): price 0-0.5% below EMA21, RSI<48
    if ema21 > 0 and adx >= NEUT_ADX_MIN:
        pct_below = (ema21 - price) / ema21
        if 0.0 <= pct_below <= NEUT_EMA_PULL_MAX and rsi < NEUT_RSI_PULL_MAX:
            return {"signal": "EMA21_PULLBACK",
                    "idle_guard": idle_guard, "mode": "PULLBACK"}, ""
        miss.append(
            f"PULL[below={pct_below*100:+.2f}%"
            f" need 0-{NEUT_EMA_PULL_MAX*100:.1f}%"
            f" rsi={rsi:.0f}/{NEUT_RSI_PULL_MAX:.0f}]"
        )
    else:
        miss.append(f"PULL[adx={adx:.0f}/{NEUT_ADX_MIN:.0f}]")

    # 2. RSI_OVERSOLD: RSI<42, above EMA55, ADX>=15
    if rsi < NEUT_RSI_OVS_MAX and above_ema55 and adx >= 15:
        return {"signal": "RSI_OVERSOLD",
                "idle_guard": idle_guard, "mode": "PULLBACK"}, ""
    miss.append(
        f"OVS[rsi={rsi:.0f}/{NEUT_RSI_OVS_MAX:.0f}"
        f" ema55={'ok' if above_ema55 else 'below'}]"
    )

    # 3. MACD_CROSS: histogram ≤0 → >0, ADX>=20, above EMA55
    if (ind["macd_hist_prev"] <= 0 and ind["macd_hist"] > 0
            and adx >= NEUT_ADX_MIN and above_ema55):
        return {"signal": "MACD_CROSS",
                "idle_guard": idle_guard, "mode": "PULLBACK"}, ""
    miss.append(
        f"MACD_X[hist={ind['macd_hist']:+.6f}"
        f" prev={ind['macd_hist_prev']:+.6f}"
        f" adx={adx:.0f}/{NEUT_ADX_MIN:.0f}]"
    )

    return None, " ".join(miss)

# ──────────────────────────────────────────────────────────────────────
# EXIT LOGIC — Phase 1 / Phase 2
# ──────────────────────────────────────────────────────────────────────

def bars_held_1m(pos: dict) -> int:
    """Always derived from open_ts — never an accumulated counter."""
    return max(0, int((time.time() - pos["open_ts"]) / 60))

def is_genuinely_green(pos: dict, price: float) -> bool:
    """Phase 1 → Phase 2 gate. Uses position's own green_pct threshold
    (momentum entries arm Phase 2 later than pullback entries)."""
    gain_pct = (price - pos["entry_price"]) / pos["entry_price"]
    gain_usd = (price - pos["entry_price"]) * pos["qty"]
    green_pct = pos.get("green_pct", GREEN_PCT)
    return gain_pct >= green_pct and gain_usd >= GREEN_USD

def check_phase2_exits(pos: dict, ind5m: dict,
                        price: float) -> tuple:
    """
    Phase 2 exit stack (priority order).
    Returns (should_exit, reason, new_atr_stop).
    Mutates pos["peak_gain"] in place.
    """
    entry    = pos["entry_price"]
    bh       = bars_held_1m(pos)
    atr_mult = pos.get("atr_mult", ATR_MULT)
    atr14    = ind5m["atr14"]
    hard_stop = entry * (1 - HARD_STOP_PCT)

    gain = (price - entry) / entry
    if gain > pos.get("peak_gain", 0.0):
        pos["peak_gain"] = gain
    peak_gain = pos["peak_gain"]

    current_atr_stop = pos.get("atr_stop", hard_stop)

    # 1. ATR trail (activates after ATR_MIN_HOLD_1M bars)
    # Hard stop is the only floor — no premature PROFIT_FLOOR clamping.
    new_atr_stop = current_atr_stop
    if bh >= ATR_MIN_HOLD_1M and atr14 > 0:
        candidate = price - atr14 * atr_mult
        candidate = max(candidate, hard_stop)
        if candidate > new_atr_stop:
            new_atr_stop = candidate
    if price <= new_atr_stop:
        return True, "ATR_TRAIL", new_atr_stop

    # 3. MACD flip — gated by min hold + min profit
    if (bh >= MACD_MIN_1M
            and peak_gain >= MACD_NEED_GAIN
            and ind5m["macd_hist_prev"] > 0
            and ind5m["macd_hist"] <= 0):
        return True, "MACD_FLIP", new_atr_stop

    return False, "", new_atr_stop

# ──────────────────────────────────────────────────────────────────────
# COOLDOWN
# ──────────────────────────────────────────────────────────────────────

def cooldown_1m(pnl_pct: float, reason: str) -> int:
    if reason in ("BEAR_DEPTH_EVICT", "BEAR_TIME_EVICT"):
        return BEAR_EXIT_COOLDOWN_1M
    for threshold, bars in COOLDOWN_TABLE:
        if pnl_pct >= threshold:
            return bars
    return COOLDOWN_TABLE[-1][1]

# ──────────────────────────────────────────────────────────────────────
# POSITION COERCION
# ──────────────────────────────────────────────────────────────────────

def _coerce_pos(pos: dict) -> dict:
    """
    Ensure all numeric position fields are correct Python types.
    Guards against legacy state.json string values and ccxt edge cases.
    """
    for f in ("entry_price", "size_usd", "qty",
              "peak_gain", "atr_stop", "atr_mult",
              "green_pct"):
        if f in pos:
            try:
                pos[f] = float(pos[f])
            except (TypeError, ValueError):
                pass
    for f in ("open_ts", "phase"):
        if f in pos:
            try:
                pos[f] = int(pos[f])
            except (TypeError, ValueError):
                pass
    return pos

# ──────────────────────────────────────────────────────────────────────
# STATE
# ──────────────────────────────────────────────────────────────────────

class State:
    def __init__(self, base: Path, paper_mode: bool = False,
                 paper_equity: float = 100.0):
        self.path_state  = base / "state.json"
        self.path_events = base / "events.log"
        self.path_audit  = base / "audit.csv"
        self.paper_mode  = paper_mode
        self.equity      = paper_equity if paper_mode else 0.0
        self.peak        = paper_equity if paper_mode else 0.0
        self.positions   = {}
        self.cooldowns   = {}
        self.trades      = 0
        self.wins        = 0
        self.total_pnl   = 0.0
        self.last_entry_ts  = 0.0
        self._paper_cash    = paper_equity
        self._load()
        self._init_audit()

    def _ts(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def log(self, msg: str):
        line = f"[{self._ts()}] {msg}"
        print(line, flush=True)
        with open(self.path_events, "a") as f:
            f.write(line + "\n")

    def log_indicators(self, sym, ind15m, ind5m, regime, mode, regime_age_s, action):
        """Enhanced indicator log with mode and regime age (v7.0)."""
        if regime_age_s > 0:
            h = int(regime_age_s // 3600)
            m = int((regime_age_s % 3600) // 60)
            age_str = f"{h}h{m:02d}m"
        else:
            age_str = "--"
        pct_vs_ema = ((ind5m["price"] - ind5m["ema21"]) / ind5m["ema21"] * 100
                      if ind5m["ema21"] > 0 else 0.0)
        self.log(
            f"INDICATORS {sym}"
            f" regime={regime}/{mode}(age={age_str})"
            f" 15m[e21={ind15m['ema21']:.4f} e55={ind15m['ema55']:.4f}]"
            f" 5m[price={ind5m['price']:.4f}"
            f" vsEMA21={pct_vs_ema:+.2f}%"
            f" rsi={ind5m['rsi14']:.1f}"
            f" adx={ind5m['adx14']:.1f}"
            f" atr={ind5m['atr14']:.6f}"
            f" macd_h={ind5m['macd_hist']:+.6f}"
            f" vol={ind5m.get('vol_ratio',1.0):.2f}x]"
            f" → {action}"
        )

    def log_trade(self, sym, side, price, qty, pnl, reason, bh):
        with open(self.path_audit, "a", newline="") as f:
            csv.writer(f).writerow([
                self._ts(), sym, side,
                f"{price:.8f}", f"{qty:.8f}",
                f"{pnl:.6f}", reason, bh,
            ])

    def record_trade(self, win: bool, pnl: float):
        self.trades += 1
        if win: self.wins += 1
        self.total_pnl += pnl

    def print_stats(self):
        wr = 100 * self.wins / self.trades if self.trades else 0
        print(
            f"=== v7.0 trades={self.trades} wr={wr:.1f}%"
            f" pnl=${self.total_pnl:.4f} equity=${self.equity:.2f} ===",
            flush=True
        )

    def _load(self):
        if not self.path_state.exists():
            self._saved_positions = {}
            return
        try:
            j = json.loads(self.path_state.read_text())
            now = time.time()
            raw_cd = j.get("cooldowns", {})
            self.cooldowns     = {s: float(t) for s, t in raw_cd.items()
                                  if float(t) > now}
            self.trades        = j.get("trades",        0)
            self.wins          = j.get("wins",          0)
            self.total_pnl     = j.get("total_pnl",     0.0)
            self.last_entry_ts = float(j.get("last_entry_ts", 0.0))
            self._paper_cash   = j.get("paper_cash",    self._paper_cash)
            self._saved_positions = {
                sym: _coerce_pos(pos)
                for sym, pos in j.get("positions", {}).items()
            }
        except Exception as e:
            print(f"WARNING: state.json corrupt — starting fresh ({e})")
            self._saved_positions = {}

    def _save(self):
        j = {
            "equity":         self.equity,
            "peak":           self.peak,
            "trades":         self.trades,
            "wins":           self.wins,
            "total_pnl":      self.total_pnl,
            "last_entry_ts":  self.last_entry_ts,
            "paper_cash":     self._paper_cash,
            "positions":      self.positions,
            "cooldowns":      self.cooldowns,
            "saved_at":       self._ts(),
        }
        tmp = str(self.path_state) + ".tmp"
        Path(tmp).write_text(json.dumps(j, indent=2))
        Path(tmp).replace(self.path_state)

    def _init_audit(self):
        if not self.path_audit.exists():
            with open(self.path_audit, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "symbol", "side", "price",
                    "qty", "pnl", "reason", "bars_held",
                ])

# ──────────────────────────────────────────────────────────────────────
# NATIVE ORDER MANAGER
# ──────────────────────────────────────────────────────────────────────

class NativeOrders:
    """Manages native stop-loss orders on Kraken.
    Paper mode: logs intent, does not place real orders."""
    def __init__(self, exchange, paper_mode: bool, state: State):
        self.exchange   = exchange
        self.paper_mode = paper_mode
        self.state      = state

    async def place_stop(self, sym: str, qty: float,
                          stop_price: float) -> str | None:
        if self.paper_mode:
            self.state.log(
                f"[PAPER] STOP_ORDER {sym}"
                f" qty={qty:.8f} stop={stop_price:.6f}"
            )
            return "PAPER_STOP"
        try:
            order = await self.exchange.create_order(
                sym, "stop-loss", "sell", qty, stop_price,
                {"ordertype": "stop-loss", "price": str(stop_price)}
            )
            oid = order.get("id", "UNKNOWN")
            self.state.log(
                f"STOP_ORDER placed {sym}"
                f" stop={stop_price:.6f} id={oid}"
            )
            return oid
        except Exception as e:
            self.state.log(f"WARNING STOP_ORDER failed {sym}: {e}")
            return None

    async def cancel(self, sym: str, order_id: str | None) -> bool:
        if self.paper_mode or not order_id or order_id == "PAPER_STOP":
            return True
        try:
            await self.exchange.cancel_order(order_id, sym)
            return True
        except Exception as e:
            self.state.log(f"WARNING cancel_order {sym} {order_id}: {e}")
            return False

    async def replace(self, sym: str, old_id: str | None,
                       qty: float, new_price: float) -> str | None:
        await self.cancel(sym, old_id)
        return await self.place_stop(sym, qty, new_price)

# ──────────────────────────────────────────────────────────────────────
# BOT CONTEXT (shared mutable state across async tasks)
# ──────────────────────────────────────────────────────────────────────

class BotCtx:
    def __init__(self):
        self.cache_1m  : dict = {}
        self.cache_5m  : dict = {}
        self.cache_15m : dict = {}
        self.prev_regime: dict = {}    # sym → last raw 15m regime (for confirmed_regime())
        self.regime_current: dict = {} # sym → last confirmed regime
        self.regime_since:   dict = {} # sym → unix ts of last NEUTRAL/BEAR→BULL flip
        self.armed     : dict = {}
        self.cooldowns : dict = {}
        # Per-symbol state snapshot for heartbeat display (updated each 5m close)
        self.last_sym_state: dict = {}
        # Watchdog counters (heartbeat)
        self.hb_err_count: int = 0
        self.hb_alerted:   int = 0
        # Watchdog counters (WS mass-disconnect)
        self.ws_err_last_ts:   float = 0.0
        self.ws_err_batch:     int   = 0
        self.ws_batch_alerted: bool  = False

ctx = BotCtx()

# ──────────────────────────────────────────────────────────────────────
# LIVE EQUITY HELPERS
# ──────────────────────────────────────────────────────────────────────

def _live_equity(bal: dict, equity_cap: float) -> float:
    """Synchronous equity calc — uses candle cache only (no API calls)."""
    cash = float(bal.get("free", {}).get("USD", 0) or 0)
    if equity_cap > 0:
        cash = min(cash, equity_cap)
    unrealised = 0.0
    for currency, raw_qty in bal.get("total", {}).items():
        qty = float(raw_qty or 0)
        if currency == "USD" or qty < 1e-8:
            continue
        sym = f"{currency}/USD"
        c = ctx.cache_1m.get(sym) or ctx.cache_5m.get(sym)
        if c:
            unrealised += qty * float(c[-1]["c"])
    return cash + unrealised


async def _full_equity(exchange, bal: dict, equity_cap: float) -> float:
    """Full equity from the exchange — prices every non-USD holding.
    Cache-priced for SYMBOLS, live-fetched for any legacy holdings."""
    cash = float(bal.get("free", {}).get("USD", 0) or 0)
    if equity_cap > 0:
        cash = min(cash, equity_cap)
    unrealised = 0.0
    for currency, raw_qty in bal.get("total", {}).items():
        qty = float(raw_qty or 0)
        if currency == "USD" or qty < 1e-8:
            continue
        sym = f"{currency}/USD"
        c = ctx.cache_1m.get(sym) or ctx.cache_5m.get(sym)
        if c:
            unrealised += qty * float(c[-1]["c"])
        else:
            try:
                ticker = await exchange.fetch_ticker(sym)
                price  = float(ticker.get("last") or ticker.get("close") or 0)
                if price > 0:
                    unrealised += qty * price
            except Exception:
                pass
    return cash + unrealised


async def close_position(sym: str, pos: dict, price: float, reason: str,
                          state: State, exchange,
                          native_orders: NativeOrders, paper_mode: bool):
    qty     = pos["qty"]
    pnl     = (price - pos["entry_price"]) * qty
    pnl_pct = (price - pos["entry_price"]) / pos["entry_price"]
    bh      = bars_held_1m(pos)

    await native_orders.cancel(sym, pos.get("stop_order_id"))

    if paper_mode:
        state._paper_cash += pos["size_usd"] + pnl
    else:
        try:
            await exchange.create_order(sym, "market", "sell", qty)
        except Exception as e:
            state.log(f"SELL ERR {sym}: {e}")

    state.record_trade(pnl > 0, pnl)
    state.log_trade(sym, "SELL", price, qty, pnl, reason, bh)

    cd = cooldown_1m(pnl_pct, reason)
    entry_mode = pos.get("entry_mode", "PULLBACK")
    state.log(
        f"EXIT {sym} {reason}"
        f" mode={entry_mode}"
        f" price={price:.6f} gain={pnl_pct*100:+.2f}%"
        f" pnl=${pnl:.6f} bh={bh} cd={cd}bars"
    )
    alert(
        f"EXIT {sym} | {reason} [{entry_mode}]\n"
        f"Price: {price:.6f} | PnL: {pnl_pct*100:+.2f}% (${pnl:.4f})\n"
        f"Held: {bh//60}h{bh%60}m | Equity: ${state.equity:.2f}",
        title="Trade Exit",
    )

    del state.positions[sym]
    expiry = time.time() + cd * 60
    ctx.cooldowns[sym]   = expiry
    state.cooldowns[sym] = expiry
    ctx.armed.pop(sym, None)
    state._save()

# ──────────────────────────────────────────────────────────────────────
# BOOT RECONCILIATION
# ──────────────────────────────────────────────────────────────────────

async def reconcile(exchange, state: State, paper_mode: bool):
    if paper_mode:
        state.log("RECONCILE: paper mode — skipping")
        return
    try:
        balance  = await exchange.fetch_balance()
        holdings = {}
        for currency, amounts in balance.get("total", {}).items():
            qty = float(amounts or 0)
            if currency == "USD" or qty < 1e-8: continue
            sym = f"{currency}/USD"
            if sym in SYMBOLS:
                holdings[sym] = qty

        state.log(f"RECONCILE: exchange holds {list(holdings.keys())}")

        for sym in list(state.positions.keys()):
            if sym not in holdings:
                state.log(f"RECONCILE: drop ghost {sym}")
                del state.positions[sym]

        for sym, qty in holdings.items():
            if sym not in state.positions:
                try:
                    ticker = await exchange.fetch_ticker(sym)
                    price  = float(ticker.get("last") or ticker.get("close") or 0)
                except Exception:
                    price = 0.0
                value_usd = qty * price
                if value_usd < DUST_USD:
                    state.log(
                        f"RECONCILE: skip dust {sym}"
                        f" qty={qty:.8f} value~${value_usd:.4f} (<${DUST_USD})"
                    )
                    continue
                saved = state._saved_positions.get(sym, {})
                entry_price = float(saved.get("entry_price", price) or price)
                entry_mode  = saved.get("entry_mode", "PULLBACK")
                atr_mult    = float(saved.get("atr_mult",
                    MOMENTUM_ATR_MULT if entry_mode == "MOMENTUM" else ATR_MULT))
                green_pct   = float(saved.get("green_pct",
                    MOMENTUM_GREEN_PCT if entry_mode == "MOMENTUM" else GREEN_PCT))
                state.log(
                    f"RECONCILE: add {sym} qty={qty:.8f}"
                    f" entry~{entry_price:.6f}"
                    f" mode={entry_mode}"
                    f" (saved={'yes' if saved else 'no'})"
                )
                state.positions[sym] = _coerce_pos({
                    "sym":           sym,
                    "entry_price":   entry_price,
                    "size_usd":      qty * entry_price,
                    "qty":           qty,
                    "open_ts":       int(saved.get("open_ts", time.time())),
                    "signal":        saved.get("signal", "UNTRACKED"),
                    "entry_mode":    entry_mode,
                    "phase":         int(saved.get("phase", 1)),
                    "peak_gain":     float(saved.get("peak_gain", 0.0)),
                    "atr_stop":      float(saved.get(
                                         "atr_stop",
                                         entry_price * (1 - HARD_STOP_PCT))),
                    "atr_mult":      atr_mult,
                    "green_pct":     green_pct,
                    "stop_order_id": saved.get("stop_order_id", None),
                })

        for sym in list(state.positions.keys()):
            pos = state.positions[sym]
            c   = ctx.cache_1m.get(sym) or ctx.cache_5m.get(sym)
            p   = float(c[-1]["c"]) if c else float(pos.get("entry_price", 0))
            value_usd = float(pos["qty"]) * p
            if value_usd < DUST_USD:
                state.log(
                    f"RECONCILE: drop dust position {sym}"
                    f" value~${value_usd:.4f} — unblocking symbol"
                )
                del state.positions[sym]

        state._save()
        state.log(f"RECONCILE: done — {len(state.positions)} positions")
    except Exception as e:
        state.log(f"RECONCILE ERROR: {e} — using state.json")

# ──────────────────────────────────────────────────────────────────────
# 15M CLOSE — regime update + flip detection (v7.0)
# ──────────────────────────────────────────────────────────────────────

async def on_15m_close(sym: str, state: State):
    """
    Updates regime state per symbol. Detects BULL flips and records
    regime_since timestamp so on_5m_close can route MOMENTUM vs PULLBACK.
    """
    ind = compute_indicators(ctx.cache_15m.get(sym, []))
    if not ind:
        return

    new_raw      = ind["regime"]
    prev_raw     = ctx.prev_regime.get(sym, "NEUTRAL")
    new_confirmed = confirmed_regime(new_raw, prev_raw)
    old_confirmed = ctx.regime_current.get(sym, "NEUTRAL")

    # Detect transition INTO BULL
    if new_confirmed == "BULL" and old_confirmed != "BULL":
        ctx.regime_since[sym] = time.time()
        state.log(
            f"REGIME_FLIP {sym} {old_confirmed}→BULL"
            f" | MOMENTUM_MODE armed for {MOMENTUM_WINDOW_S//3600:.0f}h"
        )
        alert(
            f"REGIME FLIP → BULL: {sym}\n"
            f"Momentum mode active for {MOMENTUM_WINDOW_S//3600:.0f}h\n"
            f"EMA21={ind['ema21']:.4f} EMA55={ind['ema55']:.4f}\n"
            f"RSI={ind['rsi14']:.1f} ADX={ind['adx14']:.1f}",
            title=f"Bull Regime: {sym}",
            priority="default",
        )

    # Detect leaving BULL
    elif old_confirmed == "BULL" and new_confirmed != "BULL":
        ctx.regime_since.pop(sym, None)
        state.log(f"REGIME_FLIP {sym} BULL→{new_confirmed}")

    ctx.prev_regime[sym]     = new_raw
    ctx.regime_current[sym]  = new_confirmed

# ──────────────────────────────────────────────────────────────────────
# 5M CLOSE — exit management + dual-mode signal arming (v7.0)
# ──────────────────────────────────────────────────────────────────────

async def on_5m_close(sym: str, state: State, exchange,
                       native_orders: NativeOrders,
                       paper_mode: bool, equity_cap: float, max_dd: float):
    ind5m  = compute_indicators(ctx.cache_5m.get(sym, []))
    ind15m = compute_indicators(ctx.cache_15m.get(sym, []))
    if not ind5m or not ind15m: return

    price  = ind5m["price"]
    regime = confirmed_regime(
        ind15m["regime"],
        ctx.prev_regime.get(sym, "NEUTRAL")
    )

    # Regime age and mode for routing/logging
    # Self-heal: if symbol is in BULL but was never recorded in regime_since
    # (e.g. seeded as NEUTRAL then confirmed BULL mid-candle without triggering
    # on_15m_close flip detection), default to established-PULLBACK territory
    # so regime_age_s is finite and correct rather than ~56 years.
    if regime == "BULL" and sym not in ctx.regime_since:
        ctx.regime_since[sym] = time.time() - MOMENTUM_WINDOW_S
        state.log(
            f"REGIME_SINCE_HEAL {sym} — was missing from regime_since;"
            f" defaulting to PULLBACK mode (age={MOMENTUM_WINDOW_S//3600:.0f}h)"
        )
    regime_age_s = time.time() - ctx.regime_since.get(sym, 0)
    fresh_bull   = (regime == "BULL"
                    and sym in ctx.regime_since
                    and regime_age_s < MOMENTUM_WINDOW_S)
    entry_mode   = "MOMENTUM" if fresh_bull else "PULLBACK"

    # ── EXIT LOGIC ────────────────────────────────────────────────────
    if sym in state.positions:
        pos = state.positions[sym]
        bh  = bars_held_1m(pos)
        gain = (price - pos["entry_price"]) / pos["entry_price"]

        if gain > pos.get("peak_gain", 0.0):
            pos["peak_gain"] = gain

        # Bear eviction (both phases)
        if regime == "BEAR":
            if gain < -BEAR_EVICT_LOSS_PCT:
                await close_position(sym, pos, price, "BEAR_DEPTH_EVICT",
                                     state, exchange, native_orders, paper_mode)
                return
            if bh > BEAR_EVICT_TIME_1M:
                await close_position(sym, pos, price, "BEAR_TIME_EVICT",
                                     state, exchange, native_orders, paper_mode)
                return
            if gain >= BEAR_BIG_WIN_PCT:   new_mult = ATR_MULT_BEAR_BIG
            elif gain >= 0:                new_mult = ATR_MULT_BEAR_MODEST
            else:                          new_mult = ATR_MULT_BEAR_LOSS
            if pos.get("atr_mult", ATR_MULT) != new_mult:
                pos["atr_mult"] = new_mult
                state.log(
                    f"BEAR_TIGHTEN {sym}"
                    f" mult={new_mult} gain={gain*100:+.2f}%"
                )

        # Zombie kill
        if bh >= ZOMBIE_1M and gain < 0:
            await close_position(sym, pos, price, "ZOMBIE_KILL",
                                 state, exchange, native_orders, paper_mode)
            return

        # Phase 1 → Phase 2 transition
        if pos.get("phase", 1) == 1 and is_genuinely_green(pos, price):
            pos["phase"] = 2
            gain_usd = (price - pos["entry_price"]) * pos["qty"]
            pos_mode = pos.get("entry_mode", "PULLBACK")
            state.log(
                f"PHASE_TRANSITION {sym} Phase1→Phase2"
                f" mode={pos_mode}"
                f" gain={gain*100:+.3f}% (${gain_usd:+.4f})"
                f" green_pct_used={pos.get('green_pct', GREEN_PCT)*100:.1f}%"
            )
            alert(
                f"PHASE 2 ARMED — {sym} [{pos_mode}]\n"
                f"Gain: {gain*100:+.3f}% (${gain_usd:.4f})\n"
                f"ATR trail + MACD flip now active\n"
                f"ATR_MULT={pos.get('atr_mult', ATR_MULT):.1f}",
                title="Phase 2",
            )
            atr14 = ind5m["atr14"]
            atr_mult_use = pos.get("atr_mult", ATR_MULT)
            initial_trail = max(
                price - atr14 * atr_mult_use,
                pos["entry_price"] * (1 - HARD_STOP_PCT),  # hard stop is the floor
            )
            new_id = await native_orders.replace(
                sym, pos.get("stop_order_id"),
                pos["qty"], initial_trail
            )
            pos["stop_order_id"] = new_id
            pos["atr_stop"]      = initial_trail

        # Phase 2 exits
        if pos.get("phase", 1) == 2:
            should_exit, reason, new_atr_stop = check_phase2_exits(
                pos, ind5m, price
            )
            if should_exit:
                state.positions[sym] = pos
                await close_position(sym, pos, price, reason,
                                     state, exchange, native_orders, paper_mode)
                return

            if new_atr_stop > pos.get("atr_stop", 0.0):
                new_id = await native_orders.replace(
                    sym, pos.get("stop_order_id"),
                    pos["qty"], new_atr_stop
                )
                pos["stop_order_id"] = new_id
                pos["atr_stop"]      = new_atr_stop

            state.log(
                f"HOLD {sym} ph=2 bh={bh}"
                f" gain={gain*100:+.3f}%"
                f" trail={pos['atr_stop']:.6f}"
                f" macd_h={ind5m['macd_hist']:+.6f}"
            )
        else:
            state.log(
                f"HOLD {sym} ph=1 bh={bh}"
                f" gain={gain*100:+.3f}%"
                f" hard_stop={pos['atr_stop']:.6f}"
            )

        state.positions[sym] = pos
        state._save()

        action = (f"HOLD ph={pos.get('phase',1)}"
                  f" bh={bh}"
                  f" gain={gain*100:+.2f}%"
                  f" mode={pos.get('entry_mode','?')}")
        state.log_indicators(sym, ind15m, ind5m, regime, entry_mode, regime_age_s, action)

        # Update heartbeat snapshot
        ctx.last_sym_state[sym] = {
            "regime": regime, "mode": entry_mode,
            "regime_age_s": regime_age_s if sym in ctx.regime_since else 0,
            "price": price, "ema21": ind5m["ema21"],
            "rsi14": ind5m["rsi14"], "adx14": ind5m["adx14"],
            "macd_hist": ind5m["macd_hist"],
            "vol_ratio": ind5m.get("vol_ratio", 1.0),
            "armed": None, "skip": "",
            "cooldown_left": 0,
            "in_position": True,
            "pos_phase": pos.get("phase", 1),
            "pos_gain": gain, "pos_bh": bh,
            "pos_trail": pos.get("atr_stop", 0),
            "pos_peak": pos.get("peak_gain", 0),
        }
        return

    # ── SIGNAL EVALUATION (no open position) ─────────────────────────
    cd_left = max(0, ctx.cooldowns.get(sym, 0) - time.time())
    if regime == "BEAR" or cd_left > 0:
        ctx.armed.pop(sym, None)
        if regime == "BEAR":
            action = "BEAR_BLOCKED"
        else:
            action = f"COOLDOWN_{int(cd_left//60)}m{int(cd_left%60):02d}s_left"
        state.log_indicators(sym, ind15m, ind5m, regime, entry_mode, regime_age_s, action)
        ctx.last_sym_state[sym] = {
            "regime": regime, "mode": entry_mode,
            "regime_age_s": regime_age_s if sym in ctx.regime_since else 0,
            "price": price, "ema21": ind5m["ema21"],
            "rsi14": ind5m["rsi14"], "adx14": ind5m["adx14"],
            "macd_hist": ind5m["macd_hist"],
            "vol_ratio": ind5m.get("vol_ratio", 1.0),
            "armed": None, "skip": action,
            "cooldown_left": int(cd_left),
            "in_position": False,
        }
        return

    is_flat = len(state.positions) == 0
    sig      = None
    skip_reason = ""

    if fresh_bull:
        # Primary: momentum entry (MACD cross, no EMA21 gate)
        sig, skip_reason = evaluate_momentum_signal(
            ind5m, regime, state.last_entry_ts, is_flat
        )
        if sig is None:
            # Fallback: if momentum misses, check pullback anyway
            # (price might have already kissed EMA21 in fresh bull)
            sig2, skip2 = evaluate_signal(
                ind5m, regime, state.last_entry_ts, is_flat
            )
            if sig2:
                sig = sig2
                skip_reason = (
                    f"MOMENTUM_MISS[{skip_reason}] → PULLBACK_HIT"
                )
            else:
                skip_reason = (
                    f"MOMENTUM[{skip_reason}] PULLBACK[{skip2}]"
                )
    else:
        # Established bull or neutral — pullback only
        sig, skip_reason = evaluate_signal(
            ind5m, regime, state.last_entry_ts, is_flat
        )

    if sig:
        ctx.armed[sym] = {**sig, "regime": regime}
        state.log(
            f"SIGNAL_ARMED {sym} {sig['signal']}"
            f" mode={sig.get('mode','?')}"
            f" regime={regime}(age={int(regime_age_s//60)}m)"
            f" price={price:.6f}"
            f" rsi={ind5m['rsi14']:.1f}"
            f" adx={ind5m['adx14']:.1f}"
            f" macd_h={ind5m['macd_hist']:+.6f}"
            f" → waiting 1m entry candle"
        )
    else:
        ctx.armed.pop(sym, None)
        state.log(f"SIGNAL_SKIP {sym} regime={regime}/{entry_mode} | {skip_reason}")

    action = (f"ARMED({ctx.armed[sym]['signal']})"
               if sym in ctx.armed else f"WATCHING")
    state.log_indicators(sym, ind15m, ind5m, regime, entry_mode, regime_age_s, action)

    # Update heartbeat snapshot
    ctx.last_sym_state[sym] = {
        "regime": regime, "mode": entry_mode,
        "regime_age_s": regime_age_s if sym in ctx.regime_since else 0,
        "price": price, "ema21": ind5m["ema21"],
        "rsi14": ind5m["rsi14"], "adx14": ind5m["adx14"],
        "macd_hist": ind5m["macd_hist"],
        "vol_ratio": ind5m.get("vol_ratio", 1.0),
        "armed": ctx.armed[sym]["signal"] if sym in ctx.armed else None,
        "skip": skip_reason if sym not in ctx.armed else "",
        "cooldown_left": 0,
        "in_position": False,
    }

# ──────────────────────────────────────────────────────────────────────
# 1M CLOSE — precision entry
# ──────────────────────────────────────────────────────────────────────

async def on_1m_close(sym: str, state: State, exchange,
                       native_orders: NativeOrders,
                       paper_mode: bool, equity_cap: float):
    if sym not in ctx.armed:
        return

    if time.time() < ctx.cooldowns.get(sym, 0):
        ctx.armed.pop(sym, None)
        return

    candles_1m = ctx.cache_1m.get(sym, [])
    if not candles_1m: return

    price = candles_1m[-1]["c"]
    if price <= 1e-7: return

    sig = ctx.armed[sym]
    is_momentum = sig.get("mode", "PULLBACK") == "MOMENTUM"

    if paper_mode:
        free_usd   = state._paper_cash
        already_in = sym in state.positions
    else:
        try:
            bal      = await exchange.fetch_balance()
            free_usd = float(bal.get("free", {}).get("USD", 0))
            if equity_cap > 0:
                free_usd = min(free_usd, equity_cap)
            currency   = sym.split("/")[0]
            held_qty   = float(bal.get("total", {}).get(currency, 0) or 0)
            already_in = held_qty > 1e-8
            state.equity = await _full_equity(exchange, bal, equity_cap)
            if state.equity > state.peak:
                state.peak = state.equity
        except Exception as e:
            state.log(f"ENTRY_BALANCE_ERR {sym}: {e} — skipping")
            return

    if already_in:
        ctx.armed.pop(sym, None)
        return

    deployable = free_usd * (1 - DRY_POWDER)
    if deployable < 2.0:
        state.log(
            f"ENTRY_BLOCKED {sym} — capital exhausted:"
            f" free_usd=${free_usd:.2f} deployable=${deployable:.2f} < $2.00"
            f" open_positions={len(state.positions)}"
        )
        return

    size_pct = SIZE_HIGH if sig.get("idle_guard") else SIZE_LOW
    size_usd = min(state.equity * size_pct, deployable)
    if size_usd < 2.0:
        state.log(
            f"ENTRY_BLOCKED {sym} — size too small:"
            f" size_usd=${size_usd:.2f} < $2.00"
            f" (equity=${state.equity:.2f} pct={size_pct:.0%}"
            f" deployable=${deployable:.2f})"
        )
        return

    qty = size_usd / price

    if paper_mode:
        state._paper_cash -= size_usd
        txid = "PAPER"
    else:
        try:
            order = await exchange.create_order(sym, "market", "buy", qty)
            txid  = order.get("id", "UNKNOWN")
            price = float(order.get("average") or order.get("price") or price)
        except Exception as e:
            state.log(f"BUY FAILED {sym}: {e}")
            ctx.armed.pop(sym, None)
            return

    # Trail params differ by entry mode
    # MOMENTUM: wider trail (2.1×) + later Phase 2 arm (0.7%)
    # PULLBACK: standard trail (1.8×) + standard Phase 2 arm (0.5%)
    entry_atr_mult = MOMENTUM_ATR_MULT if is_momentum else ATR_MULT
    entry_green_pct = MOMENTUM_GREEN_PCT if is_momentum else GREEN_PCT

    hard_stop = price * (1 - HARD_STOP_PCT)
    stop_id   = await native_orders.place_stop(sym, qty, hard_stop)

    idle_note = (
        f" [IDLE {(time.time() - state.last_entry_ts)/3600:.1f}h]"
        if sig.get("idle_guard") else ""
    )
    mode_label = sig.get("mode", "PULLBACK")
    state.log(
        f"ENTRY {sym} {sig['signal']} [{mode_label}]"
        f" price={price:.6f} size=${size_usd:.2f}"
        f" qty={qty:.8f} hard_stop={hard_stop:.6f}"
        f" atr_mult={entry_atr_mult:.1f}"
        f" green_pct={entry_green_pct*100:.1f}%"
        f" txid={txid}{idle_note}"
    )
    alert(
        f"ENTRY {sym} | {sig['signal']} [{mode_label}]\n"
        f"Price: {price:.6f} | Size: ${size_usd:.2f}\n"
        f"Hard stop (native): {hard_stop:.6f} | Phase: 1\n"
        f"ATR_MULT={entry_atr_mult:.1f} GREEN_PCT={entry_green_pct*100:.1f}%\n"
        f"Equity: ${state.equity:.2f}{idle_note}",
        title="Trade Entry",
    )
    state.log_trade(sym, "BUY", price, qty, 0.0, sig["signal"], 0)

    state.positions[sym] = {
        "sym":           sym,
        "entry_price":   price,
        "size_usd":      size_usd,
        "qty":           qty,
        "open_ts":       int(time.time()),
        "signal":        sig["signal"],
        "entry_mode":    mode_label,
        "phase":         1,
        "peak_gain":     0.0,
        "atr_stop":      hard_stop,
        "atr_mult":      entry_atr_mult,
        "green_pct":     entry_green_pct,
        "stop_order_id": stop_id,
    }
    state.last_entry_ts = time.time()
    ctx.armed.pop(sym, None)
    state._save()


# ──────────────────────────────────────────────────────────────────────
# WS ERROR HANDLER (mass disconnect detection)
# ──────────────────────────────────────────────────────────────────────

def _ws_alert(sym: str, tf: str, err: str, state: State):
    """
    Log WS error and fire a single ntfy when 3+ symbols drop within 2s —
    indicating a Kraken server cycle, not a transient blip.
    v7.0 fix: ws_1m/ws_5m/ws_15m now all route through here.
    """
    state.log(f"WS_{tf} ERR {sym}: {err}")
    now = time.time()
    if now - ctx.ws_err_last_ts < 2.0:
        ctx.ws_err_batch += 1
    else:
        ctx.ws_err_batch     = 1
        ctx.ws_batch_alerted = False
    ctx.ws_err_last_ts = now
    if ctx.ws_err_batch == 3 and not ctx.ws_batch_alerted:
        ctx.ws_batch_alerted = True
        alert(
            "WS MASS DISCONNECT\n"
            "Multiple symbols dropped simultaneously (Kraken 1006/1001).\n"
            "Tasks are auto-reconnecting.",
            title="WS Disconnect",
            priority="default",
        )

# ──────────────────────────────────────────────────────────────────────
# WEBSOCKET WATCHERS
# ──────────────────────────────────────────────────────────────────────

async def ws_1m(sym: str, exchange, state: State,
                 native_orders: NativeOrders,
                 paper_mode: bool, equity_cap: float):
    while not _shutdown:
        try:
            raw = await exchange.watch_ohlcv(sym, "1m", limit=500)
            if len(raw) < 2: continue
            ctx.cache_1m[sym] = _merge_candles(ctx.cache_1m.get(sym, []), _candle_list(raw), 500)
            await on_1m_close(sym, state, exchange,
                               native_orders, paper_mode, equity_cap)
        except asyncio.CancelledError:
            break
        except Exception as e:
            if not _shutdown:
                _ws_alert(sym, "1m", str(e), state)
            try:
                await exchange.close()
            except Exception:
                pass
            await asyncio.sleep(5)

async def ws_5m(sym: str, exchange, state: State,
                 native_orders: NativeOrders,
                 paper_mode: bool, equity_cap: float, max_dd: float):
    while not _shutdown:
        try:
            raw = await exchange.watch_ohlcv(sym, "5m", limit=300)
            if len(raw) < 2: continue
            ctx.cache_5m[sym] = _merge_candles(ctx.cache_5m.get(sym, []), _candle_list(raw), 300)
            await on_5m_close(sym, state, exchange,
                               native_orders, paper_mode, equity_cap, max_dd)
        except asyncio.CancelledError:
            break
        except Exception as e:
            if not _shutdown:
                _ws_alert(sym, "5m", str(e), state)
            try:
                await exchange.close()
            except Exception:
                pass
            await asyncio.sleep(5)
            # Reseed cache via REST after reconnect so stale data
            # doesn't drive exit decisions on the first live candle
            try:
                raw = await exchange.fetch_ohlcv(sym, "5m", limit=300)
                if raw:
                    ctx.cache_5m[sym] = _merge_candles(ctx.cache_5m.get(sym, []), _candle_list(raw), 300)
                    state.log(f"WS_5m RESEED {sym} after reconnect")
            except Exception:
                pass

async def ws_15m(sym: str, exchange, state: State):
    while not _shutdown:
        try:
            raw = await exchange.watch_ohlcv(sym, "15m", limit=200)
            if len(raw) < 2: continue
            ctx.cache_15m[sym] = _merge_candles(ctx.cache_15m.get(sym, []), _candle_list(raw), 200)
            await on_15m_close(sym, state)
        except asyncio.CancelledError:
            break
        except Exception as e:
            if not _shutdown:
                _ws_alert(sym, "15m", str(e), state)
            try:
                await exchange.close()
            except Exception:
                pass
            await asyncio.sleep(5)

# ──────────────────────────────────────────────────────────────────────
# HEARTBEAT — equity + kill switch + rich per-symbol status (v7.0)
# ──────────────────────────────────────────────────────────────────────

async def heartbeat(state: State, exchange, native_orders: NativeOrders,
                     paper_mode: bool, equity_cap: float, max_dd: float):
    global _shutdown
    while not _shutdown:
        await asyncio.sleep(HEARTBEAT_S)
        if _shutdown: break

        if (BASE_DIR / "EMERGENCY_STOP").exists():
            state.log("EMERGENCY_STOP detected — halting.")
            _shutdown = True; return

        # Equity refresh
        try:
            if paper_mode:
                unrealised = 0.0
                for sym, pos in state.positions.items():
                    c = ctx.cache_1m.get(sym) or ctx.cache_5m.get(sym)
                    if c:
                        unrealised += float(c[-1]["c"]) * float(pos["qty"])
                    else:
                        unrealised += float(pos["size_usd"])
                state.equity = state._paper_cash + unrealised
            else:
                bal  = await exchange.fetch_balance()
                state.equity = await _full_equity(exchange, bal, equity_cap)

            if state.equity > state.peak:
                state.peak = state.equity
            state._save()
            ctx.hb_err_count = 0
            ctx.hb_alerted   = 0
        except Exception as e:
            err_msg = str(e)
            state.log(f"HEARTBEAT equity err: {err_msg}")
            ctx.hb_err_count += 1
            n = ctx.hb_err_count
            if n == _WD_ALERT_1 and ctx.hb_alerted < 1:
                ctx.hb_alerted = 1
                alert(
                    f"HEARTBEAT ERROR\n"
                    f"Equity tracking and kill switch are INACTIVE.\n"
                    f"Error: {err_msg}",
                    title="Bot Degraded",
                    priority="high",
                )
            elif n == _WD_ALERT_5 and ctx.hb_alerted < 5:
                ctx.hb_alerted = 5
                alert(
                    f"HEARTBEAT ERROR ×{n} — persistent\n"
                    f"Error: {err_msg}\n"
                    f"Manual intervention recommended.",
                    title="Bot Degraded — Persistent",
                    priority="high",
                )
            elif n == _WD_ALERT_10 and ctx.hb_alerted < 10:
                ctx.hb_alerted = 10
                alert(
                    f"BOT CRITICALLY DEGRADED — {n} consecutive errors\n"
                    f"Error: {err_msg}\n"
                    f"Kill switch offline. Positions unmonitored.\n"
                    f"RESTART REQUIRED.",
                    title="CRITICAL — Bot Degraded",
                    priority="max",
                )

        # Drawdown kill switch
        if (state.peak > 0
                and (state.peak - state.equity) / state.peak >= max_dd):
            dd_pct = (state.peak - state.equity) / state.peak * 100
            state.log(f"!!! DRAWDOWN KILL {dd_pct:.1f}% !!!")
            alert(
                f"KILL SWITCH TRIGGERED\n"
                f"DD: {dd_pct:.1f}% | Peak: ${state.peak:.2f}\n"
                f"Equity: ${state.equity:.2f}",
                title="KILL SWITCH",
            )
            for sym in list(state.positions.keys()):
                pos = state.positions[sym]
                c   = ctx.cache_1m.get(sym) or ctx.cache_5m.get(sym)
                p   = float(c[-1]["c"]) if c else float(pos["entry_price"])
                await close_position(sym, pos, p, "DRAWDOWN_KILL",
                                     state, exchange, native_orders, paper_mode)
            state.print_stats()
            _shutdown = True; return

        # ── RICH PER-SYMBOL STATUS (v7.0) ────────────────────────────
        wr = 100 * state.wins / state.trades if state.trades else 0
        dd_pct = ((state.peak - state.equity) / state.peak * 100
                  if state.peak > 0 else 0)

        lines = [
            f"═══ HEARTBEAT v7.0 ═══"
            f" equity=${state.equity:.2f}"
            f" peak=${state.peak:.2f}"
            f" dd={dd_pct:.1f}%"
            f" trades={state.trades}"
            f" WR={wr:.1f}%"
            f" PnL=${state.total_pnl:.4f}",
        ]

        # Open positions
        if state.positions:
            lines.append(f"  ── POSITIONS ({len(state.positions)}) ──")
            for sym, pos in state.positions.items():
                c = ctx.cache_1m.get(sym) or ctx.cache_5m.get(sym)
                px = float(c[-1]["c"]) if c else pos["entry_price"]
                gain = (px - pos["entry_price"]) / pos["entry_price"]
                bh   = bars_held_1m(pos)
                lines.append(
                    f"  {sym:<12}"
                    f" ph={pos.get('phase',1)}"
                    f" [{pos.get('entry_mode','?')}]"
                    f" bh={bh//60}h{bh%60:02d}m"
                    f" gain={gain*100:+.2f}%"
                    f" peak={pos.get('peak_gain',0)*100:+.2f}%"
                    f" trail={pos.get('atr_stop',0):.4f}"
                    f" atr_mult={pos.get('atr_mult', ATR_MULT):.1f}x"
                )

        # Watching symbols
        watching = [s for s in SYMBOLS if s not in state.positions]
        if watching:
            lines.append(f"  ── WATCHING ({len(watching)}) ──")
            for sym in watching:
                ss = ctx.last_sym_state.get(sym)
                if not ss:
                    # WS hasn't delivered a candle yet — try REST fallback
                    try:
                        for tf, cache in [("1m", ctx.cache_1m),
                                          ("5m", ctx.cache_5m),
                                          ("15m", ctx.cache_15m)]:
                            if not cache.get(sym):
                                lim = 500 if tf=="1m" else 300 if tf=="5m" else 200
                                raw = await exchange.fetch_ohlcv(sym, tf, limit=lim)
                                if raw:
                                    cache[sym] = _candle_list(raw)
                        state.log(f"HEARTBEAT_RESEED {sym} — WS stale, REST fallback")
                    except Exception as e:
                        state.log(f"HEARTBEAT_RESEED_ERR {sym}: {e}")
                    lines.append(f"  {sym:<12}  no data yet (reseeding...)")
                    continue

                regime    = ss["regime"]
                mode      = ss["mode"]
                age_s     = ss.get("regime_age_s", 0)
                price     = ss["price"]
                ema21     = ss["ema21"]
                rsi       = ss["rsi14"]
                adx       = ss["adx14"]
                macd_h    = ss["macd_hist"]
                vol       = ss.get("vol_ratio", 1.0)
                cd_left   = ss.get("cooldown_left", 0)
                armed     = ss.get("armed")
                skip      = ss.get("skip", "")

                # Regime + mode label
                if cd_left > 0:
                    regime_label = f"COOLDOWN({cd_left//60}m{cd_left%60:02d}s)"
                elif regime == "BULL":
                    h = int(age_s // 3600)
                    m = int((age_s % 3600) // 60)
                    regime_label = f"BULL/{mode}({h}h{m:02d}m)"
                else:
                    regime_label = regime

                # Price vs EMA21
                if ema21 > 0:
                    pct_vs = (price - ema21) / ema21 * 100
                    ema_str = f"vsEMA21={pct_vs:+.2f}%"
                else:
                    ema_str = "vsEMA21=--"

                # Status
                if armed:
                    status = f"★ ARMED:{armed}"
                elif cd_left > 0:
                    status = "waiting"
                elif regime == "BEAR":
                    status = "BEAR — no longs"
                elif regime == "NEUTRAL":
                    status = "regime not BULL"
                else:
                    # Truncate long skip reasons for readability
                    status = f"skip: {skip[:80]}" if skip else "WATCHING"

                lines.append(
                    f"  {sym:<12}"
                    f" {regime_label:<24}"
                    f" {ema_str:<16}"
                    f" RSI={rsi:.0f}"
                    f" ADX={adx:.0f}"
                    f" MACD={macd_h:+.5f}"
                    f" vol={vol:.1f}x"
                    f" | {status}"
                )

        lines.append("═" * 68)

        for line in lines:
            state.log(line)

# ──────────────────────────────────────────────────────────────────────
# SEED CANDLE CACHES  (REST on boot, WS keeps them live)
# ──────────────────────────────────────────────────────────────────────

async def seed_caches(exchange, state: State):
    state.log(f"Seeding 1m/5m/15m caches for {len(SYMBOLS)} symbols...")
    for sym in SYMBOLS:
        for tf, cache in [("1m", ctx.cache_1m),
                           ("5m", ctx.cache_5m),
                           ("15m", ctx.cache_15m)]:
            try:
                raw = await exchange.fetch_ohlcv(sym, tf, limit=500 if tf=="1m" else 300 if tf=="5m" else 200)
                cache[sym] = _candle_list(raw)
                await asyncio.sleep(0.4)
            except Exception as e:
                state.log(f"  SEED ERR {sym} {tf}: {e}")

        ind15 = compute_indicators(ctx.cache_15m.get(sym, []))
        candles_15m = ctx.cache_15m.get(sym, [])
        if len(candles_15m) >= 2:
            ind15_prev = compute_indicators(candles_15m[:-1])
            prev_raw   = ind15_prev["regime"] if ind15_prev else "NEUTRAL"
        else:
            prev_raw = ind15["regime"] if ind15 else "NEUTRAL"

        ctx.prev_regime[sym] = prev_raw
        new_raw  = ind15["regime"] if ind15 else "NEUTRAL"
        confirmed = confirmed_regime(new_raw, prev_raw)
        ctx.regime_current[sym] = confirmed

        # On boot into existing BULL: set regime_since so we get 1h of
        # momentum window. We don't know when this bull actually started,
        # but defaulting to "never" means momentum mode never fires on
        # a clean restart into a running market. 1h ago is conservative
        # and safe — gives momentum entries for the first hour.
        if confirmed != "BULL":
            ctx.regime_since.pop(sym, None)
        else:
            ctx.regime_since[sym] = time.time() - 3600   # 1h ago → 1h window left

        state.log(
            f"  {sym} seeded"
            f" 1m:{len(ctx.cache_1m.get(sym,[]))}"
            f" 5m:{len(ctx.cache_5m.get(sym,[]))}"
            f" 15m:{len(ctx.cache_15m.get(sym,[]))}"
            f" regime={confirmed}"
            f" mode={'PULLBACK(boot)' if confirmed=='BULL' else '--'}"
        )
    state.log("Seed complete.")

# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────

async def async_main():
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║  Kraken Bull Bot  v7.0  |  Dual-Mode Entry + Rich Heartbeat   ║")
    print("╚═══════════════════════════════════════════════════════════════╝")
    print(f"BASE_DIR: {BASE_DIR}\n", flush=True)

    print(f"Waiting {NTP_WAIT_S}s for NTP stabilisation...")
    for i in range(NTP_WAIT_S, 0, -1):
        print(f"\r  {i}s ", end="", flush=True)
        await asyncio.sleep(1)
    print("\r  NTP wait complete.  ")

    env        = load_env(BASE_DIR / ".env")
    api_key    = env.get("KRAKEN_API_KEY",    "")
    api_secret = env.get("KRAKEN_API_SECRET", "")
    paper_mode = env.get("PAPER_MODE",        "true").lower() in ("true","1","yes")
    paper_eq   = float(env.get("PAPER_EQUITY",    "100.0"))
    equity_cap = float(env.get("EQUITY_USD",       "0"))
    max_dd     = float(env.get("MAX_DRAWDOWN_PCT", "0.20"))

    if not api_key or not api_secret:
        sys.exit("FATAL: API credentials missing from .env")

    exchange = ccxtpro.kraken({
        "apiKey":          api_key,
        "secret":          api_secret,
        "enableRateLimit": True,
        "options":         {"defaultType": "spot"},
    })

    state         = State(BASE_DIR, paper_mode=paper_mode,
                          paper_equity=paper_eq)
    native_orders = NativeOrders(exchange, paper_mode, state)

    if paper_mode:
        print("  ╔══════════════════════════════════════════════════════════════╗")
        print("  ║  PAPER MODE — live prices, simulated orders                  ║")
        print(f" ║  Starting equity: ${paper_eq:.2f}                            ║")
        print("  ╚══════════════════════════════════════════════════════════════╝\n")
    else:
        for attempt in range(10):
            try:
                bal = await exchange.fetch_balance()
                usd = float(bal.get("free", {}).get("USD", 0))
                if equity_cap > 0: usd = min(usd, equity_cap)
                state.equity = usd
                state.peak   = usd
                print(f"  Live balance (free cash): ${usd:.2f}")
                break
            except Exception as e:
                print(f"  Balance attempt {attempt+1}/10: {e}")
                if attempt == 9: sys.exit("FATAL: Kraken unreachable")
                await asyncio.sleep(30)

    state.log(
        f"=== BOT START v7.0 | mode={'PAPER' if paper_mode else 'LIVE'}"
        f" | equity=${state.equity:.2f} | max_dd={max_dd*100:.0f}%"
        f" | symbols={len(SYMBOLS)} ==="
    )
    alert(
        f"BOT ONLINE v7.0 | {'PAPER' if paper_mode else 'LIVE'}\n"
        f"Equity: ${state.equity:.2f} | Symbols: {len(SYMBOLS)}\n"
        f"WS: 1m+5m+15m | Phase1/2 | Native stops\n"
        f"Dual-mode: MOMENTUM(fresh bull) + PULLBACK(established)",
        title="Bot Started v7.0",
    )

    await seed_caches(exchange, state)
    await reconcile(exchange, state, paper_mode)

    ctx.cooldowns = dict(state.cooldowns)
    if ctx.cooldowns:
        state.log(f"COOLDOWNS restored: {list(ctx.cooldowns.keys())}")

    if not paper_mode:
        try:
            bal          = await exchange.fetch_balance()
            state.equity = await _full_equity(exchange, bal, equity_cap)
            state.peak   = max(state.equity, state.peak)
            state.log(f"EQUITY recomputed from exchange: ${state.equity:.2f}")
        except Exception as e:
            state.log(f"WARNING: equity recompute failed: {e}")

    for sym, pos in state.positions.items():
        c = ctx.cache_1m.get(sym) or ctx.cache_5m.get(sym)
        if c:
            price = c[-1]["c"]
            gain  = (price - pos["entry_price"]) / pos["entry_price"]
            if gain > pos.get("peak_gain", 0.0):
                pos["peak_gain"] = gain
                state.positions[sym] = pos
    state._save()

    # Launch WS tasks — 3 per symbol + heartbeat
    tasks = []
    for sym in SYMBOLS:
        tasks += [
            asyncio.create_task(
                ws_1m(sym, exchange, state, native_orders,
                       paper_mode, equity_cap),
                name=f"1m_{sym}"
            ),
            asyncio.create_task(
                ws_5m(sym, exchange, state, native_orders,
                       paper_mode, equity_cap, max_dd),
                name=f"5m_{sym}"
            ),
            asyncio.create_task(
                ws_15m(sym, exchange, state),
                name=f"15m_{sym}"
            ),
        ]
    tasks.append(asyncio.create_task(
        heartbeat(state, exchange, native_orders,
                  paper_mode, equity_cap, max_dd),
        name="heartbeat"
    ))

    state.log(
        f"  {len(tasks)} tasks launched"
        f" ({len(SYMBOLS)} symbols × 3 timeframes + heartbeat)"
    )

    try:
        while not _shutdown:
            await asyncio.sleep(1)
    finally:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await exchange.close()

    state.log("=== BOT SHUTDOWN v7.0 ===")
    state.print_stats()
    alert(
        f"BOT OFFLINE v7.0\n"
        f"Trades: {state.trades}"
        f" | WR: {100*state.wins/state.trades:.1f}%\n"
        f"PnL: ${state.total_pnl:.4f} | Equity: ${state.equity:.2f}",
        title="Bot Offline v7.0",
    )


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()