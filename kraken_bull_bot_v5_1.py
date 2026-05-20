#!/usr/bin/env python3
"""
kraken_bull_bot_v5_1.py
============================================================
Live / Paper trading bot — v5.1
============================================================

WHAT CHANGED FROM v4.0 (optimizer findings 2026-04-06):

  EXIT SYSTEM — completely rebuilt:
  ─────────────────────────────────
  Old: Fixed tiered trail (1.0-1.3% widths regardless of asset volatility)
       → Fired on normal candle noise; exited XRP at +0.08% when move had 5%
  
  New: Three-layer exit stack (in priority order):
    1. HARD_STOP      — 1.5% below entry, always. Never removed.
    2. PROFIT_FLOOR   — once peak gain >= 0.3%, stop locks at entry × 1.001
    3. ATR_TRAIL      — trailing stop = price − (ATR14 × 1.5)
                        Auto-adapts: BTC gets ~$500 room, XRP gets ~$0.006
                        Only ratchets upward. Starts after MIN_HOLD_BARS.
    4. MACD_FLIP      — when MACD(12,26,9) histogram crosses from + to −
                        AND bars_held >= MACD_MIN_HOLD (12 bars = 1h)
                        AND peak_gain >= 0.003 (must have been in profit)
                        This catches the EXACT moment momentum dies.

  MACD exit fires independently of ATR trail — whichever triggers first
  wins. MACD catches sharp reversals; ATR catches slow bleed-outs.

  COOLDOWN — now variable based on exit quality:
  ───────────────────────────────────────────────
  Old: flat 3 bars (15 min) regardless of outcome → immediate re-entry
       into same downtrend that just stopped us out (FET × 4, XRP × 4)
  
  New: quality-scaled cooldown
    exit PnL > +1.5%  → 12 bars (1h)   trend has legs, re-enter soon
    exit PnL +0.3-1.5% → 24 bars (2h)  normal win, give it space
    exit PnL 0-0.3%   → 48 bars (4h)   marginal, market is choppy
    exit at loss      → 72 bars (6h)   signal failed, stay away

  BEAR_REGIME_EXIT cooldown is always 48 bars (4h) — regime break is
  structural, not a noise event.

  INDICATOR ADDITION — MACD(12,26,9) histogram computed per candle window:
  ─────────────────────────────────────────────────────────────────────────
  Stored in ind5m as "macd_hist" and "macd_hist_prev" for flip detection.
  Per-position: stores "macd_hist_at_entry" for contextual baseline.

  UNCHANGED from v4.0:
  ─────────────────────
  - Symbol list (8 symbols, no FET, no AVAX)
  - Entry signals: EMA21_PULLBACK + RSI_OVERSOLD
  - Entry gates: ADX >= 20, RSI < 48, EMA21_PULL_MAX = 0.50%
  - Regime detection: 15m EMA21/EMA55, 2-bar BULL confirmation
  - Position sizing: 15%/25% of equity, 20% dry powder
  - State management: atomic JSON writes, cooldowns persisted
  - Kill switch: EMERGENCY_STOP file + MAX_DRAWDOWN_PCT
  - NTP wait on boot, tmux deployment

Regime  : 15-minute candles, EMA21 vs EMA55, 2-bar BULL confirmation
Signals : 5-minute candles
  EMA21_PULLBACK  — price 0–0.50% below EMA21, RSI < 48, ADX >= 20
  RSI_OVERSOLD    — RSI < 42, price > EMA55, ADX >= 15

Exit stack (priority):
  1. HARD_STOP   1.5%
  2. PROFIT_FLOOR entry×1.001 once peak>=0.3%
  3. ATR_TRAIL   price − ATR14×1.5, ratchets up, starts bar 6
  4. MACD_FLIP   histogram + → −, bar >= 12, peak_gain >= 0.3%
  5. BEAR_EXIT   unconditional on regime flip
  6. ZOMBIE_KILL 48h open and negative

.env:
  KRAKEN_API_KEY=...
  KRAKEN_API_SECRET=...
  PAPER_MODE=true
  PAPER_EQUITY=100.0
  MAX_DRAWDOWN_PCT=0.20
"""

import os, sys, time, math, json, csv, hmac, hashlib, base64
import urllib.parse, signal
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

try:
    import requests
except ImportError:
    sys.exit("Missing: pip3 install requests")

# ──────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ──────────────────────────────────────────────────────────────────────

BASE_DIR = Path(__file__).resolve().parent

SYMBOLS = [
    "XXBTZUSD", "XETHZUSD", "SOLUSD",  "XXRPZUSD",
    "DOTUSD",   "XDGUSD",   "TAOUSD",
]

LABEL = {
    "XXBTZUSD": "BTC/USD",  "XETHZUSD": "ETH/USD",  "SOLUSD":   "SOL/USD",
    "XXRPZUSD": "XRP/USD",  "DOTUSD":   "DOT/USD",  "XDGUSD":   "DOGE/USD",
    "TAOUSD":   "TAO/USD",
}

# Position sizing
DRY_POWDER  = 0.20
SIZE_HIGH   = 0.25   # idle-guard entries (flat > 8h in BULL)
SIZE_LOW    = 0.15   # normal entries

# Hard stop — absolute unconditional floor
HARD_STOP_PCT     = 0.015   # 1.5%

# Profit floor — once peak gain reaches this, stop never below entry
PROFIT_FLOOR_PCT  = 0.003   # 0.3%
PROFIT_FLOOR_LOCK = 0.001   # lock at entry × 1.001

# ATR trailing stop
ATR_MULT          = 1.5     # stop = price − ATR14 × 1.5
ATR_MIN_HOLD_BARS = 6       # bars before ATR trail activates (30 min)

# MACD flip exit
MACD_FAST         = 12
MACD_SLOW         = 26
MACD_SIGNAL       = 9
MACD_MIN_HOLD     = 12      # bars before MACD flip exit allowed (1h)
MACD_NEED_PROFIT  = 0.003   # peak_gain must be >= 0.3% for MACD exit to fire

# Entry gates
EMA21_PULL_MAX    = 0.0050  # price 0-0.50% below EMA21
RSI_PULL_THR      = 48.0
RSI_OVS_THR       = 42.0
ADX_PULL_MIN      = 20.0   # ADX >= 20 for EMA21_PULLBACK
ADX_OVS_MIN       = 15.0   # ADX >= 15 for RSI_OVERSOLD
IDLE_HOURS        = 8

# Variable cooldown (bars after exit, based on exit PnL %)
# Key = minimum pnl_pct threshold → value = cooldown bars
COOLDOWN_TABLE = [
    ( 0.015,  12),   # > +1.5%   → 1h  (strong winner, trend may still run)
    ( 0.003,  24),   # > +0.3%   → 2h  (normal win)
    ( 0.000,  48),   # > 0%      → 4h  (tiny win, market choppy)
    (-9999,   72),   # loss      → 6h  (signal failed, stay away)
]
BEAR_EXIT_COOLDOWN = 48    # always 4h after bear regime exit

# Other timing
ZOMBIE_BARS        = 576   # 48h open and still negative → force close
WARMUP             = 60    # candle bars before indicators trusted
RATE_LIMIT_S       = 1.5
NTP_WAIT_S         = 15
SIGNAL_INTERVAL_S  = 300
REGIME_INTERVAL_S  = 900
HOURLY_INTERVAL_S  = 3600

# ──────────────────────────────────────────────────────────────────────
# SHUTDOWN
# ──────────────────────────────────────────────────────────────────────

_shutdown = False

def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    print("\nShutdown signal received...")

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

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
# KRAKEN API
# ──────────────────────────────────────────────────────────────────────

class KrakenAPI:
    BASE = "https://api.kraken.com"

    def __init__(self, key: str, secret: str, dry_run: bool = False):
        self.key        = key
        self.secret     = secret
        self.dry_run    = dry_run
        self._last_call = 0.0

    def _rate_limit(self):
        wait = RATE_LIMIT_S - (time.time() - self._last_call)
        if wait > 0:
            time.sleep(wait)
        self._last_call = time.time()

    def _sign(self, urlpath: str, data: dict) -> str:
        postdata = urllib.parse.urlencode(data)
        encoded  = (str(data["nonce"]) + postdata).encode()
        message  = urlpath.encode() + hashlib.sha256(encoded).digest()
        mac      = hmac.new(base64.b64decode(self.secret), message, hashlib.sha512)
        return base64.b64encode(mac.digest()).decode()

    def _private(self, endpoint: str, params: dict = None) -> dict:
        self._rate_limit()
        params = params or {}
        params["nonce"] = str(int(time.time() * 1000))
        path = f"/0/private/{endpoint}"
        sign = self._sign(path, params)
        r = requests.post(self.BASE + path, data=params,
                          headers={"API-Key": self.key, "API-Sign": sign}, timeout=10)
        data = r.json()
        if data["error"]:
            raise RuntimeError(f"{endpoint}: {data['error']}")
        return data["result"]

    def fetch_ohlc(self, pair: str, interval: int, max_bars: int = 720) -> list:
        self._rate_limit()
        r = requests.get(self.BASE + "/0/public/OHLC",
                         params={"pair": pair, "interval": interval}, timeout=15)
        data = r.json()
        if data["error"]:
            raise RuntimeError(f"OHLC {pair}: {data['error']}")
        result = data["result"]
        pkey   = next(k for k in result if k != "last")
        candles = sorted([
            {"time": int(c[0]), "open": float(c[1]), "high": float(c[2]),
             "low":  float(c[3]), "close": float(c[4]), "volume": float(c[6])}
            for c in result[pkey]
        ], key=lambda c: c["time"])
        if candles: candles.pop()           # drop forming candle
        return candles[-max_bars:]

    def fetch_balance(self) -> float:
        result = self._private("Balance")
        for key in ("ZUSD", "USD"):
            if key in result:
                return float(result[key])
        for k, v in result.items():
            if float(v) > 0:
                print(f"  balance key: {k} = {v}")
        return 0.0

    def fetch_price(self, pair: str) -> float:
        self._rate_limit()
        r = requests.get(self.BASE + "/0/public/Ticker",
                         params={"pair": pair}, timeout=10)
        for v in r.json()["result"].values():
            return float(v["c"][0])
        return 0.0

    def market_buy(self, pair: str, volume: float) -> str:
        if self.dry_run: return "DRY_RUN"
        r = self._private("AddOrder", {"pair": pair, "type": "buy",
                                        "ordertype": "market",
                                        "volume": f"{volume:.8f}"})
        return r["txid"][0]

    def market_sell(self, pair: str, volume: float) -> str:
        if self.dry_run: return "DRY_RUN"
        r = self._private("AddOrder", {"pair": pair, "type": "sell",
                                        "ordertype": "market",
                                        "volume": f"{volume:.8f}"})
        return r["txid"][0]

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
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        ph, pl   = candles[i-1]["high"], candles[i-1]["low"]
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
        pdi, ndi = 100*spdm[i]/str_[i], 100*sndm[i]/str_[i]
        d = pdi + ndi
        dx.append(100 * abs(pdi-ndi) / d if d > 0 else 0)
    if not dx: return 0.0
    return next((v for v in reversed(_wilder(dx, period)) if v != 0), 0.0)

def _atr_scalar(candles: list, period: int = 14) -> float:
    """Wilder ATR from the last N candles, returned as a single float."""
    if len(candles) < period + 1: return 0.0
    trs = []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    out = _wilder(trs, period)
    return next((v for v in reversed(out) if v != 0), 0.0)

def _macd_scalars(closes: list) -> tuple[float, float]:
    """
    Returns (macd_hist_current, macd_hist_prev) for the last two bars.
    Uses MACD(12,26,9).
    """
    needed = MACD_SLOW + MACD_SIGNAL + 2
    if len(closes) < needed:
        return 0.0, 0.0
    ema_fast = _ema(closes, MACD_FAST)
    ema_slow = _ema(closes, MACD_SLOW)
    macd_line = [f - s for f, s in zip(ema_fast, ema_slow)]
    # Signal line = EMA(MACD_SIGNAL) of macd_line
    signal_line = _ema(macd_line, MACD_SIGNAL)
    hist = [m - s for m, s in zip(macd_line, signal_line)]
    return hist[-1], hist[-2]

def compute_indicators(candles: list) -> dict | None:
    if len(candles) < WARMUP: return None
    closes = [c["close"] for c in candles]
    e21v   = _ema(closes, 21)
    e55v   = _ema(closes, 55)
    ema21  = e21v[-1]
    ema55  = e55v[-1]
    price  = closes[-1]
    rsi14  = _rsi_scalar(closes, 14)
    adx14  = _adx_scalar(candles, 14)
    atr14  = _atr_scalar(candles, 14)
    mid    = _sma(closes, 20)
    sd     = math.sqrt(sum((c - mid) ** 2 for c in closes[-20:]) / 20)
    mh_cur, mh_prev = _macd_scalars(closes)

    if ema21 > ema55 and price > ema21:  regime = "BULL"
    elif ema21 < ema55:                   regime = "BEAR"
    else:                                 regime = "NEUTRAL"

    return {
        "price":        price,
        "ema21":        ema21,
        "ema55":        ema55,
        "rsi14":        rsi14,
        "adx14":        adx14,
        "atr14":        atr14,
        "bb_lower":     mid - 2 * sd,
        "bb_mid":       mid,
        "bb_upper":     mid + 2 * sd,
        "macd_hist":    mh_cur,
        "macd_hist_prev": mh_prev,
        "regime":       regime,
    }

# ──────────────────────────────────────────────────────────────────────
# REGIME — 2-BAR BULL CONFIRMATION
# ──────────────────────────────────────────────────────────────────────

def confirmed_regime(raw: str, prev_raw: str) -> str:
    if raw == "BULL" and prev_raw == "BULL":  return "BULL"
    elif raw == "BEAR":                        return "BEAR"
    elif raw == "BULL":                        return "NEUTRAL"
    return raw

# ──────────────────────────────────────────────────────────────────────
# ENTRY SIGNAL
# ──────────────────────────────────────────────────────────────────────

def evaluate_signal(ind: dict, regime: str,
                    bar_idx: int, last_entry_bar: int,
                    is_flat: bool) -> dict | None:
    if regime == "BEAR":
        return None

    idle_guard = (is_flat and last_entry_bar >= 0 and regime == "BULL"
                  and (bar_idx - last_entry_bar) >= IDLE_HOURS * 12)

    price = ind["price"]

    # EMA21_PULLBACK: price 0-0.50% below EMA21, RSI<48, ADX>=20
    if ind["ema21"] > 0 and ind["adx14"] >= ADX_PULL_MIN:
        pct_below = (ind["ema21"] - price) / ind["ema21"]
        if 0.0 <= pct_below <= EMA21_PULL_MAX and ind["rsi14"] < RSI_PULL_THR:
            return {"signal": "EMA21_PULLBACK", "idle_guard": idle_guard}

    # RSI_OVERSOLD: RSI<42, price above EMA55, ADX>=15 (trend exists)
    if (ind["rsi14"] < RSI_OVS_THR and price > ind["ema55"]
            and ind["ema55"] > 0 and ind["adx14"] >= ADX_OVS_MIN):
        return {"signal": "RSI_OVERSOLD", "idle_guard": idle_guard}

    return None

# ──────────────────────────────────────────────────────────────────────
# EXIT SYSTEM — v5.1 three-layer stack
# ──────────────────────────────────────────────────────────────────────

def check_exits(pos: dict, ind: dict) -> tuple[bool, str]:
    """
    Evaluates all exit conditions in priority order.
    Mutates pos["peak_gain"], pos["atr_stop"], pos["macd_hist_prev"].
    Returns (should_exit, reason).
    """
    price      = ind["price"]
    bars_held  = pos["bars_held"]
    entry      = pos["entry_price"]

    # ── 1. Hard stop — unconditional ──────────────────────────────────
    hard_stop = entry * (1 - HARD_STOP_PCT)
    if price <= hard_stop:
        return True, "HARD_STOP"

    # ── Track peak gain ───────────────────────────────────────────────
    gain = (price - entry) / entry
    if gain > pos["peak_gain"]:
        pos["peak_gain"] = gain

    # ── 2. Profit floor — once in profit, stop never below entry ──────
    if pos["peak_gain"] >= PROFIT_FLOOR_PCT:
        floor = entry * (1 + PROFIT_FLOOR_LOCK)
        if price <= floor:
            return True, "PROFIT_FLOOR"

    # ── 3. ATR trailing stop — activates after ATR_MIN_HOLD_BARS ──────
    if bars_held >= ATR_MIN_HOLD_BARS and ind["atr14"] > 0:
        new_atr_stop = price - ind["atr14"] * ATR_MULT
        # Enforce profit floor in ATR stop level
        if pos["peak_gain"] >= PROFIT_FLOOR_PCT:
            new_atr_stop = max(new_atr_stop, entry * (1 + PROFIT_FLOOR_LOCK))
        # Hard stop floor
        new_atr_stop = max(new_atr_stop, hard_stop)
        # Ratchet: only moves up
        if new_atr_stop > pos["atr_stop"]:
            pos["atr_stop"] = new_atr_stop
        if price <= pos["atr_stop"]:
            return True, "ATR_TRAIL"

    # ── 4. MACD flip exit — momentum reversal signal ───────────────────
    # Fires only when:
    #   a) held long enough for trend to develop (MACD_MIN_HOLD bars)
    #   b) position was in meaningful profit (MACD_NEED_PROFIT)
    #   c) MACD histogram just crossed from positive to negative
    if (bars_held >= MACD_MIN_HOLD
            and pos["peak_gain"] >= MACD_NEED_PROFIT
            and ind["macd_hist_prev"] > 0
            and ind["macd_hist"] <= 0):
        return True, "MACD_FLIP"

    return False, ""

def cooldown_bars_for_exit(pnl_pct: float, reason: str) -> int:
    """Return cooldown bar count based on exit quality."""
    if reason == "BEAR_REGIME_EXIT":
        return BEAR_EXIT_COOLDOWN
    for threshold, bars in COOLDOWN_TABLE:
        if pnl_pct >= threshold:
            return bars
    return COOLDOWN_TABLE[-1][1]

# ──────────────────────────────────────────────────────────────────────
# STATE
# ──────────────────────────────────────────────────────────────────────

class State:
    def __init__(self, base: Path, paper_mode: bool = False, paper_equity: float = 100.0):
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
        self.last_entry_bar     = -1
        self._paper_cash        = paper_equity
        self._live_peak_seeded  = False
        self._load()
        self._init_audit()

    def _ts(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def log(self, msg: str):
        line = f"[{self._ts()}] {msg}"
        print(line, flush=True)
        with open(self.path_events, "a") as f:
            f.write(line + "\n")

    def log_indicators(self, label, ind15m, ind5m, regime, action):
        self.log(
            f"INDICATORS {label} regime={regime}(15m)"
            f" 15m[ema21={ind15m['ema21']:.4f} ema55={ind15m['ema55']:.4f}]"
            f" 5m[price={ind5m['price']:.4f} ema21={ind5m['ema21']:.4f}"
            f" rsi14={ind5m['rsi14']:.2f} adx14={ind5m['adx14']:.2f}"
            f" atr14={ind5m['atr14']:.6f} macd_h={ind5m['macd_hist']:.6f}"
            f" bb_lower={ind5m['bb_lower']:.4f} bb_upper={ind5m['bb_upper']:.4f}]"
            f" action={action}"
        )

    def log_trade(self, sym, side, price, qty, pnl, reason, bars):
        with open(self.path_audit, "a", newline="") as f:
            csv.writer(f).writerow([
                self._ts(), LABEL.get(sym, sym), side,
                f"{price:.8f}", f"{qty:.8f}", f"{pnl:.6f}", reason, bars,
            ])

    def record_trade(self, win: bool, pnl: float):
        self.trades += 1
        if win: self.wins += 1
        self.total_pnl += pnl
        self._save()

    def print_stats(self):
        wr = 100 * self.wins / self.trades if self.trades else 0
        print(f"=== trades={self.trades} wr={wr:.1f}%"
              f" pnl=${self.total_pnl:.4f} equity=${self.equity:.2f} ===",
              flush=True)

    def _load(self):
        if not self.path_state.exists(): return
        try:
            j = json.loads(self.path_state.read_text())
            self.equity            = j.get("equity",          self.equity)
            self.peak              = j.get("peak",             self.peak)
            self.trades            = j.get("trades",           0)
            self.wins              = j.get("wins",             0)
            self.total_pnl         = j.get("total_pnl",        0.0)
            self.last_entry_bar    = j.get("last_entry_bar",   -1)
            self._paper_cash       = j.get("paper_cash",       self._paper_cash)
            self._live_peak_seeded = j.get("live_peak_seeded", False)
            self.positions         = j.get("positions",        {})
            self.cooldowns         = j.get("cooldowns",        {})
        except Exception as e:
            print(f"WARNING: state.json corrupt — starting fresh ({e})")

    def _save(self):
        j = {
            "equity": self.equity, "peak": self.peak,
            "trades": self.trades, "wins": self.wins,
            "total_pnl": self.total_pnl,
            "last_entry_bar": self.last_entry_bar,
            "paper_cash": self._paper_cash,
            "live_peak_seeded": self._live_peak_seeded,
            "positions": self.positions,
            "cooldowns": self.cooldowns,
            "saved_at": self._ts(),
        }
        tmp = str(self.path_state) + ".tmp"
        Path(tmp).write_text(json.dumps(j, indent=2))
        Path(tmp).replace(self.path_state)

    def _init_audit(self):
        if not self.path_audit.exists():
            with open(self.path_audit, "w", newline="") as f:
                csv.writer(f).writerow([
                    "timestamp", "symbol", "side", "price", "qty",
                    "pnl", "reason", "bars_held",
                ])

# ──────────────────────────────────────────────────────────────────────
# CLOSE POSITION — shared DRY helper
# ──────────────────────────────────────────────────────────────────────

def close_position(sym, pos, price, reason, state, api,
                   paper_mode, cooldown_map, global_bar):
    label   = LABEL.get(sym, sym)
    qty     = pos["size_usd"] / pos["entry_price"]
    pnl     = (price - pos["entry_price"]) * qty
    pnl_pct = (price - pos["entry_price"]) / pos["entry_price"]

    try:
        api.market_sell(sym, qty)
    except Exception as e:
        state.log(f"SELL ERR {label}: {e}")

    if paper_mode:
        state._paper_cash += pos["size_usd"] + pnl

    state.record_trade(pnl > 0, pnl)
    state.log_trade(sym, "SELL", price, qty, pnl, reason, pos["bars_held"])

    cd_bars = cooldown_bars_for_exit(pnl_pct, reason)
    state.log(
        f"EXIT {label} {reason}"
        f" price={price:.6f} gain={pnl_pct*100:+.2f}%"
        f" pnl=${pnl:.4f} bars={pos['bars_held']}"
        f" cooldown={cd_bars}bars"
    )

    del state.positions[sym]
    cooldown_map[sym] = global_bar + cd_bars
    state._save()

# ──────────────────────────────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────────────────────────────

def main():
    print("╔═══════════════════════════════════════════════════════════╗")
    print("║  Kraken Bull Bot  v5.1  |  MACD-exit + ATR + VarCooldown ║")
    print("╚═══════════════════════════════════════════════════════════╝")
    print(f"BASE_DIR: {BASE_DIR}\n", flush=True)

    print(f"Waiting {NTP_WAIT_S}s for NTP clock to stabilise...")
    for i in range(NTP_WAIT_S, 0, -1):
        print(f"\r  {i}s remaining  ", end="", flush=True)
        time.sleep(1)
    print("\r  NTP wait complete.        ")

    env        = load_env(BASE_DIR / ".env")
    api_key    = env.get("KRAKEN_API_KEY",    "")
    api_secret = env.get("KRAKEN_API_SECRET", "")
    paper_mode = env.get("PAPER_MODE",        "true").lower()  in ("true","1","yes")
    paper_eq   = float(env.get("PAPER_EQUITY",    "100.0"))
    dry_run    = env.get("DRY_RUN",           "false").lower() in ("true","1","yes")
    equity_cap = float(env.get("EQUITY_USD",      "0"))
    max_dd     = float(env.get("MAX_DRAWDOWN_PCT","0.20"))

    if paper_mode: dry_run = True
    if not api_key or not api_secret:
        sys.exit("FATAL: API credentials missing from .env")

    if paper_mode:
        print("╔══════════════════════════════════════════════════════════╗")
        print("║  PAPER MODE — live prices, simulated orders              ║")
        print(f"║  Starting equity: ${paper_eq:.2f}                            ║")
        print("╚══════════════════════════════════════════════════════════╝\n")

    api   = KrakenAPI(api_key, api_secret, dry_run)
    state = State(BASE_DIR, paper_mode=paper_mode, paper_equity=paper_eq)

    print("Connecting to Kraken API...")
    if paper_mode:
        try:    api.fetch_balance()
        except Exception as e: print(f"  WARNING connectivity: {e}")
        balance = state.equity
        print(f"  Paper equity = ${balance:.2f}")
    else:
        balance = 0.0
        for attempt in range(10):
            try:
                balance = api.fetch_balance(); break
            except Exception as e:
                print(f"  Balance attempt {attempt+1}/10: {e}")
                if attempt == 9: sys.exit("FATAL: Kraken unreachable")
                time.sleep(30)
        if equity_cap > 0: balance = min(balance, equity_cap)
        state.equity = balance
        state.peak   = balance
        state._live_peak_seeded = True
        state._save()

    state.log(f"=== BOT START v5.1 | mode={'PAPER' if paper_mode else 'LIVE'}"
              f" | equity=${state.equity:.2f} | max_dd={max_dd*100:.0f}%"
              f" | symbols={len(SYMBOLS)} ===")

    cache_5m        = {}
    cache_15m       = {}
    prev_raw_regime = {}

    state.log(f"Seeding candle caches for {len(SYMBOLS)} symbols...")
    for sym in SYMBOLS:
        label = LABEL[sym]
        try:
            c5  = api.fetch_ohlc(sym, 5,  200)
            c15 = api.fetch_ohlc(sym, 15, 200)
            if len(c5) >= WARMUP and len(c15) >= WARMUP:
                cache_5m[sym]  = c5
                cache_15m[sym] = c15
                ind15 = compute_indicators(c15)
                prev_raw_regime[sym] = ind15["regime"] if ind15 else "NEUTRAL"
                state.log(f"  {label} seeded | 5m:{len(c5)} 15m:{len(c15)}"
                          f" | regime={confirmed_regime(prev_raw_regime[sym], 'NEUTRAL')}")
            else:
                state.log(f"  WARNING: {label} insufficient bars — skipped")
        except Exception as e:
            state.log(f"  ERROR seeding {label}: {e}")

    last_signal_t = 0.0
    last_regime_t = 0.0
    last_hourly_t = 0.0
    cooldown_map  = {}   # sym → bar index when cooldown expires
    global_bar    = 0

    state.log("Seed complete. Entering main loop.")

    while not _shutdown:

        if (BASE_DIR / "EMERGENCY_STOP").exists():
            state.log("EMERGENCY_STOP detected — halting.")
            break

        now = time.time()

        # ── Regime refresh every 15 min ───────────────────────────────
        if now - last_regime_t >= REGIME_INTERVAL_S:
            last_regime_t = now
            for sym in SYMBOLS:
                if sym not in cache_15m: continue
                try:
                    c15 = api.fetch_ohlc(sym, 15, 200)
                    cache_15m[sym] = c15
                    ind15 = compute_indicators(c15)
                    if ind15:
                        prev_raw_regime[sym] = ind15["regime"]
                except Exception as e:
                    state.log(f"WARNING 15m {LABEL[sym]}: {e}")

        # ── Signal + exit cycle every 5 min ──────────────────────────
        if now - last_signal_t >= SIGNAL_INTERVAL_S:
            last_signal_t  = now
            global_bar    += 1

            # Refresh equity
            try:
                cash = state._paper_cash if paper_mode else api.fetch_balance()
                if not paper_mode and equity_cap > 0:
                    cash = min(cash, equity_cap)
                unrealised = sum(
                    (api.fetch_price(s) or p["size_usd"] / p["entry_price"]) * p["qty"]
                    if not paper_mode else p["size_usd"]
                    for s, p in state.positions.items()
                )
                # Paper: track from paper_cash + open position value
                if paper_mode:
                    unrealised = 0.0
                    for s, p in state.positions.items():
                        try:
                            cp = api.fetch_price(s)
                            unrealised += cp * p["qty"] if cp > 0 else p["size_usd"]
                        except Exception:
                            unrealised += p["size_usd"]
                state.equity = cash + unrealised
                if state.equity > state.peak: state.peak = state.equity
                state._save()
            except Exception as e:
                state.log(f"WARNING equity refresh: {e}")

            # Drawdown kill switch
            if state.peak > 0 and (state.peak - state.equity) / state.peak >= max_dd:
                state.log(f"!!! DRAWDOWN KILL {(state.peak-state.equity)/state.peak*100:.1f}%"
                           " — closing all positions !!!")
                for sym in list(state.positions.keys()):
                    pos = state.positions[sym]
                    try:
                        price = api.fetch_price(sym)
                        close_position(sym, pos, price, "DRAWDOWN_KILL",
                                       state, api, paper_mode, cooldown_map, global_bar)
                    except Exception as e:
                        state.log(f"ERROR closing {LABEL.get(sym,sym)}: {e}")
                state.print_stats()
                return

            # ── Per-symbol loop ──────────────────────────────────────
            for sym in SYMBOLS:
                if sym not in cache_5m: continue
                label = LABEL[sym]
                try:
                    c5 = api.fetch_ohlc(sym, 5, 200)
                    cache_5m[sym] = c5
                    ind5m = compute_indicators(c5)
                    if not ind5m: continue

                    price = ind5m["price"]
                    if price <= 1e-7: continue

                    ind15m = compute_indicators(cache_15m.get(sym, []))
                    if not ind15m: continue
                    regime = confirmed_regime(ind15m["regime"],
                                              prev_raw_regime.get(sym, "NEUTRAL"))

                    # ── EXIT ────────────────────────────────────────
                    if sym in state.positions:
                        pos = state.positions[sym]
                        pos["bars_held"] += 1

                        # 1. Bear regime → unconditional close
                        if regime == "BEAR":
                            close_position(sym, pos, price, "BEAR_REGIME_EXIT",
                                           state, api, paper_mode, cooldown_map, global_bar)
                            continue

                        # 2. Zombie kill
                        if pos["bars_held"] >= ZOMBIE_BARS and price < pos["entry_price"]:
                            close_position(sym, pos, price, "ZOMBIE_KILL",
                                           state, api, paper_mode, cooldown_map, global_bar)
                            continue

                        # 3. Main exit stack (hard stop / floor / ATR trail / MACD flip)
                        should_exit, reason = check_exits(pos, ind5m)
                        if should_exit:
                            close_position(sym, pos, price, reason,
                                           state, api, paper_mode, cooldown_map, global_bar)
                        else:
                            gain = (price - pos["entry_price"]) / pos["entry_price"] * 100
                            state.log(
                                f"HOLD {label} bars={pos['bars_held']}"
                                f" gain={gain:+.2f}%"
                                f" atr_stop={pos['atr_stop']:.6f}"
                                f" macd_h={ind5m['macd_hist']:+.6f}"
                            )
                            state.positions[sym] = pos
                            state._save()
                        continue

                    # ── ENTRY ───────────────────────────────────────
                    if cooldown_map.get(sym, 0) > global_bar:
                        continue
                    if regime == "BEAR":
                        continue

                    is_flat = len(state.positions) == 0
                    sig = evaluate_signal(ind5m, regime, global_bar,
                                          state.last_entry_bar, is_flat)
                    if not sig: continue

                    deployable = state.equity * (1 - DRY_POWDER)
                    allocated  = sum(p["size_usd"] for p in state.positions.values())
                    available  = deployable - allocated
                    if available < 2.0: continue

                    size_pct = SIZE_HIGH if sig["idle_guard"] else SIZE_LOW
                    size_usd = min(state.equity * size_pct, available)
                    if size_usd < 2.0: continue

                    qty = size_usd / price
                    try:
                        txid = api.market_buy(sym, qty)
                    except Exception as e:
                        state.log(f"BUY FAILED {label}: {e}")
                        continue

                    idle_note = (f" [IDLE {(global_bar-state.last_entry_bar)//12:.0f}h]"
                                 if sig["idle_guard"] else "")
                    state.log(
                        f"ENTRY {label} {sig['signal']}"
                        f" price={price:.6f} size=${size_usd:.2f}"
                        f" qty={qty:.6f} txid={txid}{idle_note}"
                    )
                    state.log_trade(sym, "BUY", price, qty, 0.0, sig["signal"], 0)

                    if paper_mode:
                        state._paper_cash -= size_usd

                    # Initialise ATR stop at hard stop level
                    initial_atr_stop = max(
                        price - ind5m["atr14"] * ATR_MULT,
                        price * (1 - HARD_STOP_PCT),
                    )

                    state.positions[sym] = {
                        "sym":         sym,
                        "entry_price": price,
                        "size_usd":    size_usd,
                        "qty":         qty,
                        "peak_gain":   0.0,
                        "atr_stop":    initial_atr_stop,
                        "bars_held":   0,
                        "signal":      sig["signal"],
                        "open_ts":     int(time.time()),
                        "macd_at_entry": ind5m["macd_hist"],
                    }
                    state.last_entry_bar = global_bar
                    state._save()

                except Exception as e:
                    state.log(f"ERROR {label}: {e}")

            state.print_stats()

        # ── Hourly snapshot ──────────────────────────────────────────
        if now - last_hourly_t >= HOURLY_INTERVAL_S:
            last_hourly_t = now
            state.log("--- Hourly indicator snapshot ---")
            for sym in SYMBOLS:
                if sym not in cache_5m or sym not in cache_15m: continue
                try:
                    ind5m  = compute_indicators(cache_5m[sym])
                    ind15m = compute_indicators(cache_15m[sym])
                    if not ind5m or not ind15m: continue
                    regime = confirmed_regime(ind15m["regime"],
                                              prev_raw_regime.get(sym, "NEUTRAL"))
                    action = "IN_POSITION" if sym in state.positions else f"WATCHING regime={regime}"
                    state.log_indicators(LABEL[sym], ind15m, ind5m, regime, action)
                except Exception as e:
                    state.log(f"Hourly error {sym}: {e}")

        time.sleep(10)

    state.log("=== BOT SHUTDOWN v5.1 ===")
    state.print_stats()


if __name__ == "__main__":
    main()