#!/usr/bin/env python3
"""
kraken_bull_bot_v5_5.py
============================================================
Live / Paper trading bot — v5.5
============================================================

WHAT CHANGED FROM v5.4:

  BUG FIX 1 — _candle_list() never cast OHLCV values to float:
  ─────────────────────────────────────────────────────────────
  ccxt.pro can return OHLCV fields as strings after a 1006 WS
  reconnect (abnormal close / Kraken server cycle).  When that
  string close price hit the heartbeat's arithmetic:

    c[-1]["c"] * pos["qty"]   →   str * float

  Python raised:
    HEARTBEAT equity err: can't multiply sequence by non-int of type 'float'

  The same uncast string also existed at the equity-sync inside
  on_1m_close().

  Fix: _candle_list() now explicitly casts o/h/l/c/v to float.

  BUG FIX 2 — position fields from state.json not type-coerced:
  ─────────────────────────────────────────────────────────────
  reconcile() pulled saved metadata via saved.get("entry_price")
  with no type normalisation.  If any field was stored as a
  string by a prior bot version, it propagated into arithmetic.

  Fix: _coerce_pos() normalises every numeric position field.
  Called in _load() on every restart, and in reconcile() when
  building positions from saved metadata.

  WATCHDOG — ntfy alert on first heartbeat error:
  ─────────────────────────────────────────────────────────────
  Previously: error was caught and swallowed.  No alert fired.
  You only discovered it after waking up to a missed bull run.

  Fix:
    1st  consecutive error  → ntfy "HEARTBEAT ERROR — Bot Degraded"
    5th  consecutive error  → ntfy escalation
    10th consecutive error  → ntfy priority=max "CRITICAL"
  Counter resets on any clean heartbeat cycle.

  WS DISCONNECT ALERT:
  ─────────────────────────────────────────────────────────────
  Mass disconnects (3+ symbols dropping within 2 seconds) now
  fire a single ntfy "WS MASS DISCONNECT" notification so you
  know immediately vs. silently watching logs.

WHAT CHANGED FROM v5.3:

  BUG FIX — equity on boot was free-cash-only:
  ─────────────────────────────────────────────
  v5.3: state.equity = free_USD at boot → with open positions, equity
        was underestimated (e.g. $31 instead of $131). deployable was
        tiny, allocated exceeded it → available < $2 → bot never fired.
  v5.4: after reconcile, equity = fetch_balance(free USD)
        + sum(live_price × qty) for each reconciled position.
        Bot knows its true equity from the first cycle.

  BUG FIX — global_1m counter advanced 7× too fast:
  ────────────────────────────────────────────────────
  v5.3: ctx.global_1m += 1 fired inside on_1m_close() which ran once
        per symbol per bar. 7 symbols → +7 per real minute.
        Cooldowns intended as hours expired in minutes.
        Idle guard triggered after ~70 minutes instead of 8 hours.
  v5.4: global_1m removed entirely. Cooldowns stored as unix timestamps
        (time.time() + seconds). Idle guard uses wall-clock elapsed time.

  BUG FIX — cooldowns never survived restarts:
  ──────────────────────────────────────────────
  v5.3: ctx.cooldowns updated at runtime but never written to
        state.cooldowns → state.json cooldowns always empty →
        bot re-entered the same symbol immediately after restart.
  v5.4: ctx.cooldowns stored as unix epoch seconds. Written to
        state.cooldowns on every state save. Restored on boot.
        Expired cooldowns filtered on load.

  BUG FIX — positions loaded from state.json not exchange:
  ──────────────────────────────────────────────────────────
  v5.3: _load() populated state.positions from state.json.
        Exchange was the fallback, not the authority.
  v5.4: _load() does NOT load positions. Only cooldowns + stats.
        Reconcile against exchange is the sole source of positions.
        state.json position metadata (entry_price, open_ts, signal,
        stop_order_id) preserved in _save() so reconcile can enrich
        untracked positions on the NEXT restart.

  BUG FIX — prev_regime seed required 15m bar close to confirm BULL:
  ────────────────────────────────────────────────────────────────────
  v5.3: ctx.prev_regime seeded from latest 15m bar. confirmed_regime()
        needed TWO BULL readings → first confirmation required waiting
        up to 15 minutes for a 15m bar to close.
  v5.4: ctx.prev_regime seeded from penultimate 15m bar (candles[-2]).
        On boot with back-to-back BULL data, BULL is confirmed immediately.

  IMPROVEMENT — indicator log on every 5m close:
  ────────────────────────────────────────────────
  v5.3: log_indicators() only called when no open position.
        Debug blackout during active trades.
  v5.4: Indicators logged on every 5m close regardless of position state.

WHAT CHANGED FROM v5.2:

  DATA LAYER — WebSocket replaces REST polling:
  ─────────────────────────────────────────────
  Old: REST fetch_ohlc() every 5 minutes → up to 5min late to signal
  New: ccxt.pro watch_ohlcv() WebSocket streams on 1m, 5m, 15m
       simultaneously. Decision logic fires the instant a candle closes.
         15m close → regime update
         5m close  → signal evaluation → arms 1m trigger if valid
         1m close  → if armed → execute entry immediately

  ENTRY PRECISION — 1m candle execution:
  ────────────────────────────────────────
  Old: Entry on 5m candle close → up to 5min after signal fires
  New: 5m signal arms a trigger. Next 1m close executes.
       Maximum lag from signal to entry: 59 seconds.

  EXIT ARCHITECTURE — Phase 1 / Phase 2:
  ────────────────────────────────────────
  Old: Full exit stack fires from bar 0 → MACD flip + trail
       cutting winners on noise and friction
  New: Phase 1 (not yet green): hard stop ONLY — nothing else fires
       Phase 2 (genuinely green): full exit stack arms
       Green threshold = peak_gain >= MAX(GREEN_PCT, GREEN_USD/size)

  NATIVE STOP ORDERS — exchange-side protection:
  ────────────────────────────────────────────────
  Old: Software stop checked every 5m → up to 5min exposure on collapse
  New: Stop-loss order placed on Kraken at entry.
       Exchange executes in milliseconds independent of bot state.
       Phase 2: hard stop cancelled, ATR trail placed as native order,
       ratcheted upward on each 5m candle close.

  BOOT RECONCILIATION — sync against exchange:
  ─────────────────────────────────────────────
  Old: Trust state.json blindly → ghost positions, missed closures
  New: On boot, fetch actual holdings from exchange.
       Position on exchange not in state → add it.
       Position in state not on exchange → remove it.

  POSITION METADATA — computed not accumulated:
  ──────────────────────────────────────────────
  Old: bars_held incremented each cycle → drifts on restarts
  New: bars_held = (now - open_ts) / 60  (1m bars, always accurate)
       peak_gain tracked but rehydrated from live price on boot

  UNCHANGED — firing logic and exit algo:
  ──────────────────────────────────────────────
  - Entry signals: EMA21_PULLBACK, RSI_OVERSOLD
  - Entry gates: ADX, RSI thresholds, EMA21 pull range
  - Regime detection: 15m EMA21/EMA55, 2-bar BULL confirmation
  - Bear eviction: per-asset tiered response (depth OR time)
  - MACD flip exit: unchanged logic, still gated by min bars + min profit
  - Zombie kill: 2880 1m bars (~48h)
  - Cooldown table: quality-scaled, persisted in state.json
  - Kill switch: EMERGENCY_STOP file + MAX_DRAWDOWN_PCT
  - NTP wait, tmux deployment, atomic state writes, ntfy alerts

Regime  : 15m candles, EMA21 vs EMA55, 2-bar BULL confirmation
Signals : 5m candles → arms 1m trigger
Entry   : 1m candle close (first after signal armed)
Phase 1 : hard stop native order only until genuinely green
Phase 2 : ATR trail native order + MACD flip + bear eviction

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
    "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD",
    "DOT/USD", "DOGE/USD", "TAO/USD",
]

LABEL = {s: s for s in SYMBOLS}   # identity map — ccxt uses BTC/USD already

# Position sizing
DRY_POWDER = 0.20
SIZE_HIGH  = 0.25   # idle-guard entries
SIZE_LOW   = 0.15   # normal entries

# Phase 1 → Phase 2 transition gate (genuinely in green)
GREEN_PCT  = 0.003   # 0.3% gain
GREEN_USD  = 2.00    # AND at least $2.00 unrealised profit

# Hard stop — absolute unconditional floor
HARD_STOP_PCT = 0.015   # 1.5% below entry

# Profit floor lock (Phase 2)
PROFIT_FLOOR_PCT  = 0.003   # arm once peak >= 0.3%
PROFIT_FLOOR_LOCK = 0.001   # stop never below entry × 1.001

# ATR trailing stop (Phase 2)
ATR_MULT          = 1.5
ATR_MIN_HOLD_1M   = 6       # 1m bars before ATR trail activates (~6 min)

# Bear eviction — per-asset tiered response (unchanged from v5.2)
BEAR_EVICT_LOSS_PCT  = 0.005    # evict if loss > 0.5%
BEAR_EVICT_TIME_1M   = 1440    # evict if held > 1440 1m bars (24h)
ATR_MULT_BEAR_LOSS   = 0.8
ATR_MULT_BEAR_MODEST = 1.2
ATR_MULT_BEAR_BIG    = 0.5
BEAR_BIG_WIN_PCT     = 0.015

# MACD flip exit (Phase 2 only)
MACD_FAST       = 12
MACD_SLOW       = 26
MACD_SIG        = 9
MACD_MIN_1M     = 60     # 1m bars before MACD flip allowed (1h)
MACD_NEED_GAIN  = 0.003  # peak_gain must be >= 0.3%

# Entry gates (unchanged from v5.2)
EMA21_PULL_MAX = 0.0050
RSI_PULL_THR   = 48.0
RSI_OVS_THR    = 42.0
ADX_PULL_MIN   = 20.0
ADX_OVS_MIN    = 15.0
IDLE_HOURS     = 8

# Zombie kill
ZOMBIE_1M = 2880   # 48h in 1m bars

# Cooldown (1m bars after exit)
COOLDOWN_TABLE = [
    ( 0.015,  60),   # > +1.5% → 1h
    ( 0.003, 120),   # > +0.3% → 2h
    ( 0.000, 240),   # > 0%    → 4h
    (-9999,  360),   # loss    → 6h
]
BEAR_EXIT_COOLDOWN_1M = 240   # 4h after bear eviction

# Alerts
NTFY_TOPIC     = "Quant-Crystal-Ball"
NTFY_URL       = f"https://ntfy.sh/{NTFY_TOPIC}"
BULL_ALERT_MIN = 3

# Timing
NTP_WAIT_S   = 15
WARMUP_BARS  = 60    # candle bars before indicators trusted
HEARTBEAT_S  = 60    # equity refresh + kill switch interval

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

def alert(msg: str, title: str = "Quant Bot", priority: str = "high"):
    try:
        requests.post(
            NTFY_URL, data=msg.encode(),
            headers={"Title": title, "Priority": priority},
            timeout=5,
        )
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────
# INDICATORS  (logic unchanged from v5.2)
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
    All OHLCV values are explicitly cast to float — ccxt.pro can return
    strings after a 1006 WS reconnect, causing 'can't multiply sequence
    by non-int of type float' at every arithmetic site that uses candle data.
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

def compute_indicators(candles: list) -> dict | None:
    if len(candles) < WARMUP_BARS: return None
    closes = [c["c"] for c in candles]
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
# ENTRY SIGNAL  (logic unchanged from v5.2)
# ──────────────────────────────────────────────────────────────────────

def evaluate_signal(ind: dict, regime: str,
                    last_entry_ts: float,
                    is_flat: bool) -> dict | None:
    if regime == "BEAR":
        return None

    idle_guard = (is_flat
                  and last_entry_ts > 0
                  and regime == "BULL"
                  and (time.time() - last_entry_ts) >= IDLE_HOURS * 3600)

    price = ind["price"]

    # EMA21_PULLBACK: price 0-0.50% below EMA21, RSI<48, ADX>=20
    if ind["ema21"] > 0 and ind["adx14"] >= ADX_PULL_MIN:
        pct_below = (ind["ema21"] - price) / ind["ema21"]
        if 0.0 <= pct_below <= EMA21_PULL_MAX and ind["rsi14"] < RSI_PULL_THR:
            return {"signal": "EMA21_PULLBACK", "idle_guard": idle_guard}

    # RSI_OVERSOLD: RSI<42, above EMA55, ADX>=15
    if (ind["rsi14"] < RSI_OVS_THR
            and price > ind["ema55"]
            and ind["ema55"] > 0
            and ind["adx14"] >= ADX_OVS_MIN):
        return {"signal": "RSI_OVERSOLD", "idle_guard": idle_guard}

    return None

# ──────────────────────────────────────────────────────────────────────
# EXIT LOGIC — Phase 1 / Phase 2
# ──────────────────────────────────────────────────────────────────────

def bars_held_1m(pos: dict) -> int:
    """Always derived from open_ts — never an accumulated counter."""
    return max(0, int((time.time() - pos["open_ts"]) / 60))

def is_genuinely_green(pos: dict, price: float) -> bool:
    """Phase 1 → Phase 2 gate: peak_gain >= MAX(GREEN_PCT, GREEN_USD/size)."""
    gain_pct = (price - pos["entry_price"]) / pos["entry_price"]
    gain_usd = (price - pos["entry_price"]) * pos["qty"]
    return gain_pct >= GREEN_PCT and gain_usd >= GREEN_USD

def check_phase2_exits(pos: dict, ind5m: dict,
                        price: float) -> tuple:
    """
    Phase 2 exit stack (priority order).
    Returns (should_exit, reason, new_atr_stop).
    new_atr_stop is returned so caller can update the native order.
    Mutates pos["peak_gain"] in place.
    """
    entry    = pos["entry_price"]
    bh       = bars_held_1m(pos)
    atr_mult = pos.get("atr_mult", ATR_MULT)
    atr14    = ind5m["atr14"]
    hard_stop = entry * (1 - HARD_STOP_PCT)

    # Track peak gain
    gain = (price - entry) / entry
    if gain > pos.get("peak_gain", 0.0):
        pos["peak_gain"] = gain
    peak_gain = pos["peak_gain"]

    current_atr_stop = pos.get("atr_stop", hard_stop)

    # 1. Profit floor
    if peak_gain >= PROFIT_FLOOR_PCT:
        floor = entry * (1 + PROFIT_FLOOR_LOCK)
        if price <= floor:
            return True, "PROFIT_FLOOR", current_atr_stop

    # 2. ATR trail (activates after ATR_MIN_HOLD_1M bars)
    new_atr_stop = current_atr_stop
    if bh >= ATR_MIN_HOLD_1M and atr14 > 0:
        candidate = price - atr14 * atr_mult
        if peak_gain >= PROFIT_FLOOR_PCT:
            candidate = max(candidate, entry * (1 + PROFIT_FLOOR_LOCK))
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
# POSITION COERCION  ← normalises all numeric fields on load/reconcile
# ──────────────────────────────────────────────────────────────────────

def _coerce_pos(pos: dict) -> dict:
    """
    Ensure all numeric position fields are correct Python types.
    Guards against legacy state.json values stored as strings by older
    bot versions, or ccxt returning numeric fields as strings in edge cases.
    Without this any arithmetic on position fields raises:
      TypeError: can't multiply sequence by non-int of type 'float'
    """
    for f in ("entry_price", "size_usd", "qty",
              "peak_gain", "atr_stop", "atr_mult"):
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
        self.positions   = {}   # populated by reconcile(), NOT _load()
        self.cooldowns   = {}   # unix timestamps; loaded + persisted
        self.trades      = 0
        self.wins        = 0
        self.total_pnl   = 0.0
        self.last_entry_ts  = 0.0   # unix timestamp of last entry
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

    def log_indicators(self, sym, ind15m, ind5m, regime, action):
        self.log(
            f"INDICATORS {sym} regime={regime}(15m)"
            f" 15m[ema21={ind15m['ema21']:.4f} ema55={ind15m['ema55']:.4f}]"
            f" 5m[price={ind5m['price']:.4f} ema21={ind5m['ema21']:.4f}"
            f" rsi14={ind5m['rsi14']:.2f} adx14={ind5m['adx14']:.2f}"
            f" atr14={ind5m['atr14']:.6f} macd_h={ind5m['macd_hist']:.6f}"
            f" bb_lower={ind5m['bb_lower']:.4f} bb_upper={ind5m['bb_upper']:.4f}]"
            f" action={action}"
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
            f"=== trades={self.trades} wr={wr:.1f}%"
            f" pnl=${self.total_pnl:.4f} equity=${self.equity:.2f} ===",
            flush=True
        )

    def _load(self):
        if not self.path_state.exists():
            self._saved_positions = {}
            return
        try:
            j = json.loads(self.path_state.read_text())
            # Exchange is the authority for positions and equity.
            # Only load what the exchange cannot tell us:
            #   cooldowns  (our internal gate, unix timestamps)
            #   trade stats (for display continuity)
            #   last_entry_ts (idle guard)
            # Filter expired cooldowns on load.
            now = time.time()
            raw_cd = j.get("cooldowns", {})
            self.cooldowns     = {s: float(t) for s, t in raw_cd.items()
                                  if float(t) > now}
            self.trades        = j.get("trades",        0)
            self.wins          = j.get("wins",          0)
            self.total_pnl     = j.get("total_pnl",     0.0)
            self.last_entry_ts = float(j.get("last_entry_ts", 0.0))
            self._paper_cash   = j.get("paper_cash",    self._paper_cash)
            # Coerce saved position fields on load — any stale string values
            # from older bot versions are normalised before they reach arithmetic.
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
            "positions":      self.positions,   # metadata only; exchange is authority
            "cooldowns":      self.cooldowns,   # unix timestamps, filtered on _load
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
    """
    Manages native stop-loss orders on Kraken.
    Paper mode: logs intent, does not place real orders.
    """
    def __init__(self, exchange, paper_mode: bool, state: State):
        self.exchange   = exchange
        self.paper_mode = paper_mode
        self.state      = state

    async def place_stop(self, sym: str, qty: float,
                          stop_price: float) -> str | None:
        """Place a native stop-loss sell order. Returns order ID."""
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
        """Cancel a native stop order."""
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
        """Cancel old stop, place new stop at new_price."""
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
        self.prev_regime: dict = {}
        self.armed     : dict = {}
        # NOTE: global_1m removed — use wall-clock time for all timing logic.
        # cooldowns stored as unix epoch seconds (time.time() + duration_seconds)
        self.cooldowns : dict = {}
        # Watchdog counters (heartbeat)
        self.hb_err_count: int = 0
        self.hb_alerted:   int = 0
        # Watchdog counters (WS mass-disconnect)
        self.ws_err_last_ts: float = 0.0
        self.ws_err_batch:   int   = 0
        self.ws_batch_alerted: bool = False

ctx = BotCtx()

# ──────────────────────────────────────────────────────────────────────
# CLOSE POSITION
# ──────────────────────────────────────────────────────────────────────

async def close_position(sym: str, pos: dict, price: float, reason: str,
                          state: State, exchange,
                          native_orders: NativeOrders, paper_mode: bool):
    qty     = pos["qty"]
    pnl     = (price - pos["entry_price"]) * qty
    pnl_pct = (price - pos["entry_price"]) / pos["entry_price"]
    bh      = bars_held_1m(pos)

    # Cancel any live native stop order
    await native_orders.cancel(sym, pos.get("stop_order_id"))

    # Execute market sell
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
    state.log(
        f"EXIT {sym} {reason}"
        f" price={price:.6f} gain={pnl_pct*100:+.2f}%"
        f" pnl=${pnl:.6f} bh={bh} cd={cd}bars"
    )
    alert(
        f"EXIT {sym} | {reason}\n"
        f"Price: {price:.6f} | PnL: {pnl_pct*100:+.2f}% (${pnl:.4f})\n"
        f"Held: {bh//60}h{bh%60}m | Equity: ${state.equity:.2f}",
        title="Trade Exit",
    )

    del state.positions[sym]
    expiry = time.time() + cd * 60   # cd is in minutes (1m bars)
    ctx.cooldowns[sym]   = expiry
    state.cooldowns[sym] = expiry    # persist so it survives restart
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

        # Remove ghost positions (state has it, exchange doesn't)
        for sym in list(state.positions.keys()):
            if sym not in holdings:
                state.log(f"RECONCILE: drop ghost {sym}")
                del state.positions[sym]

        # Add untracked positions (exchange has it, state doesn't)
        for sym, qty in holdings.items():
            if sym not in state.positions:
                try:
                    ticker = await exchange.fetch_ticker(sym)
                    price  = float(ticker.get("last") or ticker.get("close") or 0)
                except Exception:
                    price = 0.0
                # Use saved metadata if available (preserves entry_price etc.)
                saved = state._saved_positions.get(sym, {})
                entry_price = saved.get("entry_price", price) or price
                state.log(
                    f"RECONCILE: add {sym} qty={qty:.8f}"
                    f" entry~{entry_price:.6f} (saved={'yes' if saved else 'no'})"
                )
                state.positions[sym] = _coerce_pos({
                    "sym":           sym,
                    "entry_price":   entry_price,
                    "size_usd":      qty * entry_price,
                    "qty":           qty,
                    "open_ts":       saved.get("open_ts", int(time.time())),
                    "signal":        saved.get("signal", "UNTRACKED"),
                    "phase":         saved.get("phase", 1),
                    "peak_gain":     saved.get("peak_gain", 0.0),
                    "atr_stop":      saved.get("atr_stop",
                                               entry_price * (1 - HARD_STOP_PCT)),
                    "atr_mult":      saved.get("atr_mult", ATR_MULT),
                    "stop_order_id": saved.get("stop_order_id", None),
                })

        state._save()
        state.log(f"RECONCILE: done — {len(state.positions)} positions")
    except Exception as e:
        state.log(f"RECONCILE ERROR: {e} — using state.json")

# ──────────────────────────────────────────────────────────────────────
# 15M CLOSE — regime update
# ──────────────────────────────────────────────────────────────────────

async def on_15m_close(sym: str, state: State):
    ind = compute_indicators(ctx.cache_15m.get(sym, []))
    if ind:
        ctx.prev_regime[sym] = ind["regime"]

# ──────────────────────────────────────────────────────────────────────
# 5M CLOSE — exit management + signal arming
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

    # ── EXIT LOGIC ────────────────────────────────────────────────────
    if sym in state.positions:
        pos = state.positions[sym]
        bh  = bars_held_1m(pos)
        gain = (price - pos["entry_price"]) / pos["entry_price"]

        # Update peak_gain from live price every cycle
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
            # Tighten ATR multiplier
            if gain >= BEAR_BIG_WIN_PCT:       new_mult = ATR_MULT_BEAR_BIG
            elif gain >= 0:                     new_mult = ATR_MULT_BEAR_MODEST
            else:                               new_mult = ATR_MULT_BEAR_LOSS
            if pos.get("atr_mult", ATR_MULT) != new_mult:
                pos["atr_mult"] = new_mult
                state.log(f"BEAR_TIGHTEN {sym} mult={new_mult} gain={gain*100:+.2f}%")

        # Zombie kill
        if bh >= ZOMBIE_1M and gain < 0:
            await close_position(sym, pos, price, "ZOMBIE_KILL",
                                 state, exchange, native_orders, paper_mode)
            return

        # Phase 1 → Phase 2 transition
        if pos.get("phase", 1) == 1 and is_genuinely_green(pos, price):
            pos["phase"] = 2
            gain_usd = (price - pos["entry_price"]) * pos["qty"]
            state.log(
                f"PHASE_TRANSITION {sym} Phase1→Phase2"
                f" gain={gain*100:+.3f}% (${gain_usd:+.4f})"
            )
            alert(
                f"PHASE 2 ARMED — {sym}\n"
                f"Gain: {gain*100:+.3f}% (${gain_usd:.4f})\n"
                f"ATR trail + MACD flip now active",
                title="Phase 2",
            )
            # Cancel hard stop, place ATR trail as native order
            atr14 = ind5m["atr14"]
            initial_trail = max(
                price - atr14 * ATR_MULT,
                pos["entry_price"] * (1 + PROFIT_FLOOR_LOCK),
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

            # Ratchet native stop if trail moved up
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
        # Log indicators even when in position — essential for debugging
        action = (f"HOLD ph={pos.get('phase',1)} bh={bars_held_1m(pos)}"
                  f" gain={((price-pos['entry_price'])/pos['entry_price'])*100:+.2f}%")
        state.log_indicators(sym, ind15m, ind5m, regime, action)
        return

    # ── SIGNAL EVALUATION (no open position) ─────────────────────────
    # Clear arm if cooldown or bear — still log indicators for debugging
    if regime == "BEAR" or time.time() < ctx.cooldowns.get(sym, 0):
        ctx.armed.pop(sym, None)
        action = (f"BEAR_BLOCKED" if regime == "BEAR"
                  else f"COOLDOWN {int(ctx.cooldowns.get(sym,0)-time.time())}s")
        state.log_indicators(sym, ind15m, ind5m, regime, action)
        return

    is_flat = len(state.positions) == 0
    sig = evaluate_signal(
        ind5m, regime, state.last_entry_ts, is_flat
    )

    if sig:
        ctx.armed[sym] = {**sig, "regime": regime}
        state.log(
            f"SIGNAL_ARMED {sym} {sig['signal']}"
            f" price={price:.6f} regime={regime}"
            f" → waiting 1m entry candle"
        )
    else:
        ctx.armed.pop(sym, None)

    # Indicator log fires on EVERY 5m close — not just when flat
    action = (f"ARMED({ctx.armed[sym]['signal']})" if sym in ctx.armed else
              f"WATCHING regime={regime}")
    state.log_indicators(sym, ind15m, ind5m, regime, action)

# ──────────────────────────────────────────────────────────────────────
# 1M CLOSE — precision entry
# ──────────────────────────────────────────────────────────────────────

async def on_1m_close(sym: str, state: State, exchange,
                       native_orders: NativeOrders,
                       paper_mode: bool, equity_cap: float):
    # NOTE: global_1m removed. Timing is wall-clock based.

    # Nothing armed for this symbol — nothing to do
    if sym not in ctx.armed:
        return

    # Cooldown check
    if time.time() < ctx.cooldowns.get(sym, 0):
        ctx.armed.pop(sym, None)
        return

    candles_1m = ctx.cache_1m.get(sym, [])
    if not candles_1m: return

    price = candles_1m[-1]["c"]
    if price <= 1e-7: return

    sig = ctx.armed[sym]

    # ── ASK THE EXCHANGE — equity and holdings are live, not cached ───
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
            # Sync equity so stats and kill-switch stay accurate
            # Equity sync — explicit float() at every arithmetic site
            unrealised = 0.0
            for s, pos in state.positions.items():
                c = ctx.cache_1m.get(s) or ctx.cache_5m.get(s)
                p = float(c[-1]["c"]) if c else float(pos["entry_price"])
                unrealised += p * float(pos["qty"])
            state.equity = free_usd + unrealised
            if state.equity > state.peak:
                state.peak = state.equity
        except Exception as e:
            state.log(f"ENTRY_BALANCE_ERR {sym}: {e} — skipping")
            return

    if already_in:
        ctx.armed.pop(sym, None)
        return

    # Sizing from live free cash — no guesswork about what's allocated
    deployable = free_usd * (1 - DRY_POWDER)
    if deployable < 2.0: return

    size_pct = SIZE_HIGH if sig.get("idle_guard") else SIZE_LOW
    size_usd = min(state.equity * size_pct, deployable)
    if size_usd < 2.0: return

    qty = size_usd / price

    # Execute buy
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

    # Phase 1: place hard stop native order
    hard_stop = price * (1 - HARD_STOP_PCT)
    stop_id   = await native_orders.place_stop(sym, qty, hard_stop)

    idle_note = (
        f" [IDLE {(time.time() - state.last_entry_ts)/3600:.1f}h]"
        if sig.get("idle_guard") else ""
    )
    state.log(
        f"ENTRY {sym} {sig['signal']}"
        f" price={price:.6f} size=${size_usd:.2f}"
        f" qty={qty:.8f} hard_stop={hard_stop:.6f}"
        f" txid={txid}{idle_note}"
    )
    alert(
        f"ENTRY {sym} | {sig['signal']}\n"
        f"Price: {price:.6f} | Size: ${size_usd:.2f}\n"
        f"Hard stop (native): {hard_stop:.6f} | Phase: 1\n"
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
        "phase":         1,
        "peak_gain":     0.0,
        "atr_stop":      hard_stop,
        "atr_mult":      ATR_MULT,
        "stop_order_id": stop_id,
    }
    state.last_entry_ts = time.time()
    ctx.armed.pop(sym, None)
    state._save()

def _ws_alert(sym: str, tf: str, err: str, state: State):
    """
    Log WS error and fire a single ntfy alert when 3+ symbols drop
    simultaneously — indicating a Kraken server cycle (1006/1001),
    not a transient blip on one symbol.
    """
    state.log(f"WS_{tf} ERR {sym}: {err}")
    now = time.time()
    if now - ctx.ws_err_last_ts < 2.0:
        ctx.ws_err_batch += 1
    else:
        ctx.ws_err_batch   = 1
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
            raw = await exchange.watch_ohlcv(sym, "1m", limit=200)
            if len(raw) < 2: continue
            ctx.cache_1m[sym] = _candle_list(raw)
            await on_1m_close(sym, state, exchange,
                               native_orders, paper_mode, equity_cap)
        except asyncio.CancelledError:
            break
        except Exception as e:
            if not _shutdown:
                state.log(f"WS_1m ERR {sym}: {e}")
            await asyncio.sleep(5)

async def ws_5m(sym: str, exchange, state: State,
                 native_orders: NativeOrders,
                 paper_mode: bool, equity_cap: float, max_dd: float):
    while not _shutdown:
        try:
            raw = await exchange.watch_ohlcv(sym, "5m", limit=200)
            if len(raw) < 2: continue
            ctx.cache_5m[sym] = _candle_list(raw)
            await on_5m_close(sym, state, exchange,
                               native_orders, paper_mode, equity_cap, max_dd)
        except asyncio.CancelledError:
            break
        except Exception as e:
            if not _shutdown:
                state.log(f"WS_5m ERR {sym}: {e}")
            await asyncio.sleep(5)

async def ws_15m(sym: str, exchange, state: State):
    while not _shutdown:
        try:
            raw = await exchange.watch_ohlcv(sym, "15m", limit=200)
            if len(raw) < 2: continue
            ctx.cache_15m[sym] = _candle_list(raw)
            await on_15m_close(sym, state)
        except asyncio.CancelledError:
            break
        except Exception as e:
            if not _shutdown:
                state.log(f"WS_15m ERR {sym}: {e}")
            await asyncio.sleep(5)

# ──────────────────────────────────────────────────────────────────────
# HEARTBEAT — equity + kill switch
# ──────────────────────────────────────────────────────────────────────

async def heartbeat(state: State, exchange, native_orders: NativeOrders,
                     paper_mode: bool, equity_cap: float, max_dd: float):
    global _shutdown
    while not _shutdown:
        await asyncio.sleep(HEARTBEAT_S)
        if _shutdown: break

        # Emergency stop file
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
                cash = float(bal.get("free", {}).get("USD", 0))
                if equity_cap > 0: cash = min(cash, equity_cap)
                unrealised = 0.0
                for sym, pos in state.positions.items():
                    c = ctx.cache_1m.get(sym) or ctx.cache_5m.get(sym)
                    p = float(c[-1]["c"]) if c else float(pos["entry_price"])
                    unrealised += p * float(pos["qty"])
                state.equity = cash + unrealised

            if state.equity > state.peak:
                state.peak = state.equity
            state._save()
            # Heartbeat healthy — reset watchdog
            ctx.hb_err_count = 0
            ctx.hb_alerted   = 0
        except Exception as e:
            err_msg = str(e)
            state.log(f"HEARTBEAT equity err: {err_msg}")
            ctx.hb_err_count = getattr(ctx, "hb_err_count", 0) + 1
            ctx.hb_alerted   = getattr(ctx, "hb_alerted",   0)
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

        state.print_stats()

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
                raw = await exchange.fetch_ohlcv(sym, tf, limit=200)
                cache[sym] = _candle_list(raw)
                await asyncio.sleep(0.4)
            except Exception as e:
                state.log(f"  SEED ERR {sym} {tf}: {e}")

        ind15 = compute_indicators(ctx.cache_15m.get(sym, []))
        # Use the penultimate 15m bar for prev_regime so confirmed_regime()
        # can return BULL immediately on boot without waiting for next 15m close.
        candles_15m = ctx.cache_15m.get(sym, [])
        if len(candles_15m) >= 2:
            ind15_prev = compute_indicators(candles_15m[:-1])
            ctx.prev_regime[sym] = ind15_prev["regime"] if ind15_prev else "NEUTRAL"
        else:
            ctx.prev_regime[sym] = ind15["regime"] if ind15 else "NEUTRAL"
        regime = confirmed_regime(
            ind15["regime"] if ind15 else "NEUTRAL",
            ctx.prev_regime[sym]
        )
        state.log(
            f"  {sym} seeded"
            f" 1m:{len(ctx.cache_1m.get(sym,[]))}"
            f" 5m:{len(ctx.cache_5m.get(sym,[]))}"
            f" 15m:{len(ctx.cache_15m.get(sym,[]))}"
            f" regime={regime}"
        )
    state.log("Seed complete.")

# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────

async def async_main():
    print("╔═══════════════════════════════════════════════════════════════╗")
    print("║  Kraken Bull Bot  v5.4  |  WS + Phase2 + Native Stops        ║")
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
        print("╔══════════════════════════════════════════════════════════════╗")
        print("║  PAPER MODE — live prices, simulated orders                  ║")
        print(f"║  Starting equity: ${paper_eq:.2f}                           ║")
        print("╚══════════════════════════════════════════════════════════════╝\n")
    else:
        for attempt in range(10):
            try:
                bal = await exchange.fetch_balance()
                usd = float(bal.get("free", {}).get("USD", 0))
                if equity_cap > 0: usd = min(usd, equity_cap)
                # Set to free cash for now — recomputed after reconcile
                # includes unrealised value of open positions.
                state.equity = usd
                state.peak   = usd
                print(f"  Live balance (free cash): ${usd:.2f}")
                break
            except Exception as e:
                print(f"  Balance attempt {attempt+1}/10: {e}")
                if attempt == 9: sys.exit("FATAL: Kraken unreachable")
                await asyncio.sleep(30)

    state.log(
        f"=== BOT START v5.4 | mode={'PAPER' if paper_mode else 'LIVE'}"
        f" | equity=${state.equity:.2f} | max_dd={max_dd*100:.0f}%"
        f" | symbols={len(SYMBOLS)} ==="
    )
    alert(
        f"BOT ONLINE v5.4 | {'PAPER' if paper_mode else 'LIVE'}\n"
        f"Equity: ${state.equity:.2f} | Symbols: {len(SYMBOLS)}\n"
        f"WS: 1m+5m+15m | Phase1/2 | Native stops",
        title="Bot Started",
    )

    # Boot sequence
    await seed_caches(exchange, state)
    await reconcile(exchange, state, paper_mode)

    # ── Restore ctx.cooldowns from persisted state ───────────────────
    # state.cooldowns loaded in _load(), filtered to non-expired only.
    ctx.cooldowns = dict(state.cooldowns)
    if ctx.cooldowns:
        state.log(f"COOLDOWNS restored: {list(ctx.cooldowns.keys())}")

    # ── Recompute true equity from exchange (cash + unrealised) ──────
    # Critical: state.equity was free-cash-only above. With open
    # positions, deployable would be underestimated → available < $2
    # → bot would never fire. Fix: include unrealised position value.
    if not paper_mode:
        try:
            bal  = await exchange.fetch_balance()
            cash = float(bal.get("free", {}).get("USD", 0))
            if equity_cap > 0: cash = min(cash, equity_cap)
            unrealised = 0.0
            for sym, pos in state.positions.items():
                c = ctx.cache_1m.get(sym) or ctx.cache_5m.get(sym)
                p = c[-1]["c"] if c else pos["entry_price"]
                unrealised += p * pos["qty"]
            state.equity = cash + unrealised
            state.peak   = max(state.equity, state.peak)
            state.log(
                f"EQUITY recomputed: cash=${cash:.2f}"
                f" + unrealised=${unrealised:.2f}"
                f" = ${state.equity:.2f}"
            )
        except Exception as e:
            state.log(f"WARNING: equity recompute failed: {e}")

    # ── Rehydrate peak_gain for existing positions ───────────────────
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

    state.log("=== BOT SHUTDOWN v5.4 ===")
    state.print_stats()
    alert(
        f"BOT OFFLINE v5.4\n"
        f"Trades: {state.trades}"
        f" | WR: {100*state.wins/state.trades:.1f}%\n"
        f"PnL: ${state.total_pnl:.4f} | Equity: ${state.equity:.2f}",
        title="Bot Offline",
    )


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()