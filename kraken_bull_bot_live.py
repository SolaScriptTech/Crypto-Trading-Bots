#!/usr/bin/env python3
"""
kraken_bull_bot_live.py  v3.0
============================================================
Live trading bot — exact logic from backtest v2.1
============================================================

Regime  : 15-minute candles, EMA21 vs EMA55, 2-bar BULL confirmation
Signals : 5-minute candles
  EMA21_PULLBACK  — price 0–0.75% below EMA21, RSI < 52
  RSI_OVERSOLD    — RSI < 42 AND price > EMA55

Hard stop  : 1.5%  (tightened for 5m volatility)
Trail stop : tiered, 3-bar minimum (15 minutes)
Cooldown   : 3 bars = 15 minutes (zero fee — minimal cooldown)
Dry powder : 20% cash reserve
Size       : 15% normal, 25% idle guard entries

LOOP TIMING:
  Every  5 min → signal check + trail stop
  Every 15 min → regime refresh
  Every  1 hr  → full indicator snapshot to events.log

RUN:
  pip3 install requests
  python3 kraken_bull_bot_live.py

.env (same directory as script):
  KRAKEN_API_KEY=your_key
  KRAKEN_API_SECRET=your_secret
  PAPER_MODE=true       # default true — live prices, simulated orders
  PAPER_EQUITY=100.0    # simulated starting equity in paper mode
  DRY_RUN=false         # ignored when PAPER_MODE=true
  EQUITY_USD=0          # 0 = use live balance (ignored in paper mode)
  MAX_DRAWDOWN_PCT=0.20
"""

import os
import sys
import time
import math
import json
import csv
import hmac
import hashlib
import base64
import urllib.parse
import signal
from datetime import datetime, timezone
from pathlib import Path
from collections import defaultdict

try:
    import requests
except ImportError:
    sys.exit("Missing: pip3 install requests")

# ============================================================
# CONFIGURATION — identical to backtest v2.1
# ============================================================

BASE_DIR        = Path(__file__).resolve().parent

SYMBOLS = [
    "XXBTZUSD", "XETHZUSD", "SOLUSD",  "XXRPZUSD",
    "AVAXUSD",  "DOTUSD",   "BONKUSD", "ARBUSD",
    "PEPEUSD",  "XDGUSD",
]

LABEL = {
    "XXBTZUSD":"BTC/USD",  "XETHZUSD":"ETH/USD",  "SOLUSD":"SOL/USD",
    "XXRPZUSD":"XRP/USD",  "AVAXUSD":"AVAX/USD",  "DOTUSD":"DOT/USD",
    "BONKUSD":"BONK/USD",  "ARBUSD":"ARB/USD",    "PEPEUSD":"PEPE/USD",
    "XDGUSD":"DOGE/USD",
}

START_EQUITY    = 100.0
DRY_POWDER      = 0.20
SIZE_HIGH       = 0.25      # idle guard entries
SIZE_LOW        = 0.15      # normal entries
HARD_STOP_PCT   = 0.015     # 1.5% — same as backtest
MIN_HOLD_BARS   = 3         # 3 × 5min = 15min before trail fires
IDLE_HOURS      = 8
COOLDOWN_BARS   = 3         # 15 minutes — zero fee, minimal cooldown

EMA21_PULL_MAX  = 0.0075
RSI_PULL_THR    = 52.0
RSI_OVS_THR     = 42.0

WARMUP          = 60        # bars needed before indicators are trusted
RATE_LIMIT_S    = 1.5       # seconds between API calls
NTP_WAIT_S      = 15

SIGNAL_INTERVAL_S  = 300    # 5 minutes
REGIME_INTERVAL_S  = 900    # 15 minutes
HOURLY_INTERVAL_S  = 3600   # 1 hour

# ============================================================
# SHUTDOWN FLAG
# ============================================================

_shutdown = False
def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    print("\nShutdown signal received...")

signal.signal(signal.SIGINT,  _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

# ============================================================
# ENV READER
# ============================================================

def load_env(path: Path) -> dict:
    env = {}
    if not path.exists():
        raise FileNotFoundError(f".env not found: {path}")
    for line in path.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"): continue
        if "=" not in line: continue
        k, v = line.split("=", 1)
        v = v.strip().strip('"')
        env[k.strip()] = v
    return env

# ============================================================
# KRAKEN API
# ============================================================

class KrakenAPI:
    BASE = "https://api.kraken.com"

    def __init__(self, key: str, secret: str, dry_run: bool = False):
        self.key     = key
        self.secret  = secret
        self.dry_run = dry_run
        self._last_call = 0.0

    def _rate_limit(self):
        elapsed = time.time() - self._last_call
        if elapsed < RATE_LIMIT_S:
            time.sleep(RATE_LIMIT_S - elapsed)
        self._last_call = time.time()

    def _sign(self, urlpath: str, data: dict) -> str:
        postdata  = urllib.parse.urlencode(data)
        encoded   = (str(data["nonce"]) + postdata).encode()
        message   = urlpath.encode() + hashlib.sha256(encoded).digest()
        mac       = hmac.new(base64.b64decode(self.secret), message, hashlib.sha512)
        return base64.b64encode(mac.digest()).decode()

    def _private(self, endpoint: str, params: dict = None) -> dict:
        self._rate_limit()
        params = params or {}
        params["nonce"] = str(int(time.time() * 1000))
        path = f"/0/private/{endpoint}"
        sign = self._sign(path, params)
        r = requests.post(
            self.BASE + path,
            data=params,
            headers={"API-Key": self.key, "API-Sign": sign},
            timeout=10,
        )
        data = r.json()
        if data["error"]:
            raise RuntimeError(f"{endpoint} error: {data['error']}")
        return data["result"]

    def fetch_ohlc(self, pair: str, interval: int, max_bars: int = 720) -> list:
        """Fetch OHLC candles. interval = 5 or 15 (minutes)."""
        self._rate_limit()
        r = requests.get(
            self.BASE + "/0/public/OHLC",
            params={"pair": pair, "interval": interval},
            timeout=15,
        )
        data = r.json()
        if data["error"]:
            raise RuntimeError(f"OHLC error: {data['error']}")
        result  = data["result"]
        pkey    = next(k for k in result if k != "last")
        candles = [
            {
                "time":   int(c[0]),
                "open":   float(c[1]),
                "high":   float(c[2]),
                "low":    float(c[3]),
                "close":  float(c[4]),
                "volume": float(c[6]),
            }
            for c in result[pkey]
        ]
        candles.sort(key=lambda c: c["time"])
        if candles: candles.pop()   # drop incomplete forming candle
        if len(candles) > max_bars: candles = candles[-max_bars:]
        return candles

    def fetch_balance(self) -> float:
        """Returns USD free balance."""
        result = self._private("Balance")
        if "ZUSD" in result: return float(result["ZUSD"])
        if "USD"  in result: return float(result["USD"])
        # Fallback: log all non-zero keys so user can identify correct key
        for k, v in result.items():
            if float(v) > 0:
                print(f"  balance key: {k} = {v}")
        return 0.0

    def market_buy(self, pair: str, volume: float) -> str:
        if self.dry_run: return "DRY_RUN"
        result = self._private("AddOrder", {
            "pair": pair, "type": "buy",
            "ordertype": "market", "volume": f"{volume:.8f}"
        })
        return result["txid"][0]

    def market_sell(self, pair: str, volume: float) -> str:
        if self.dry_run: return "DRY_RUN"
        result = self._private("AddOrder", {
            "pair": pair, "type": "sell",
            "ordertype": "market", "volume": f"{volume:.8f}"
        })
        return result["txid"][0]

    def fetch_price(self, pair: str) -> float:
        self._rate_limit()
        r = requests.get(
            self.BASE + "/0/public/Ticker",
            params={"pair": pair}, timeout=10
        )
        data = r.json()
        for v in data["result"].values():
            return float(v["c"][0])
        return 0.0

# ============================================================
# INDICATOR MATH — identical to backtest v2.1
# ============================================================

def _ema(closes: list, period: int) -> list:
    out = [0.0] * len(closes)
    if len(closes) < period: return out
    out[period-1] = sum(closes[:period]) / period
    alpha = 2.0 / (period + 1.0)
    for i in range(period, len(closes)):
        out[i] = closes[i] * alpha + out[i-1] * (1 - alpha)
    return out

def _wilder(values: list, period: int) -> list:
    out = [0.0] * len(values)
    if len(values) < period: return out
    out[period-1] = sum(values[:period]) / period
    alpha = 1.0 / period
    for i in range(period, len(values)):
        out[i] = values[i] * alpha + out[i-1] * (1 - alpha)
    return out

def _sma(closes: list, period: int) -> float:
    if len(closes) < period: return 0.0
    return sum(closes[-period:]) / period

def _rsi(closes: list, period: int = 14) -> float:
    if len(closes) < period + 1: return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i-1]
        gains.append(max(d, 0)); losses.append(max(-d, 0))
    ag = sum(gains[:period]) / period
    al = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        ag = (ag * (period-1) + gains[i]) / period
        al = (al * (period-1) + losses[i]) / period
    if al == 0: return 100.0
    return 100.0 - 100.0 / (1.0 + ag / al)

def _adx(candles: list, period: int = 14) -> float:
    if len(candles) < period * 2 + 1: return 0.0
    tr_l, pdm_l, ndm_l = [], [], []
    for i in range(1, len(candles)):
        h, l, pc = candles[i]["high"], candles[i]["low"], candles[i-1]["close"]
        ph, pl   = candles[i-1]["high"], candles[i-1]["low"]
        tr_l.append(max(h-l, abs(h-pc), abs(l-pc)))
        up, dn = h-ph, pl-l
        pdm_l.append(up if up > dn and up > 0 else 0)
        ndm_l.append(dn if dn > up and dn > 0 else 0)
    str_ = _wilder(tr_l, period)
    spdm = _wilder(pdm_l, period)
    sndm = _wilder(ndm_l, period)
    dx = []
    for i in range(len(str_)):
        if str_[i] == 0: continue
        pdi, ndi = 100*spdm[i]/str_[i], 100*sndm[i]/str_[i]
        denom = pdi + ndi
        dx.append(100 * abs(pdi-ndi) / denom if denom > 0 else 0)
    if not dx: return 0.0
    smoothed = _wilder(dx, period)
    return next((v for v in reversed(smoothed) if v != 0), 0.0)

def compute_indicators(candles: list) -> dict:
    """Compute all indicators from a candle list. Returns None if not enough data."""
    if len(candles) < WARMUP: return None
    closes    = [c["close"] for c in candles]
    ema21_vec = _ema(closes, 21)
    ema55_vec = _ema(closes, 55)
    ema21     = ema21_vec[-1]
    ema55     = ema55_vec[-1]
    price     = closes[-1]
    sma20     = _sma(closes, 20)
    rsi14     = _rsi(closes, 14)
    adx14     = _adx(candles, 14)
    mid       = _sma(closes, 20)
    var       = sum((c-mid)**2 for c in closes[-20:]) / 20
    sd        = math.sqrt(var)

    if ema21 > ema55 and price > ema21:   regime = "BULL"
    elif ema21 < ema55:                    regime = "BEAR"
    else:                                  regime = "NEUTRAL"

    return {
        "price": price, "ema21": ema21, "ema55": ema55, "sma20": sma20,
        "rsi14": rsi14, "adx14": adx14,
        "bb_lower": mid - 2*sd, "bb_mid": mid, "bb_upper": mid + 2*sd,
        "regime": regime,
    }

# ============================================================
# REGIME WITH 2-BAR BULL CONFIRMATION
# ============================================================

def confirmed_regime(raw_regime: str, prev_raw_regime: str) -> str:
    """
    BULL requires 2 consecutive raw BULL readings.
    Single-candle BULL flip → NEUTRAL.
    BEAR flips immediately.
    """
    if raw_regime == "BULL" and prev_raw_regime == "BULL":
        return "BULL"
    elif raw_regime == "BEAR":
        return "BEAR"
    elif raw_regime == "BULL":
        return "NEUTRAL"  # only one bar — not confirmed yet
    return raw_regime

# ============================================================
# SIGNAL EVALUATION — identical to backtest v2.1
# ============================================================

def evaluate_signal(ind5m: dict, regime: str,
                    bar_idx: int, last_entry_bar: int,
                    is_flat: bool) -> dict:
    if regime == "BEAR": return None

    # Idleness guard: flat > 8h (96 five-min bars) in BULL
    idle_guard = False
    idle_bars  = 0
    if is_flat and last_entry_bar >= 0 and regime == "BULL":
        idle_bars  = bar_idx - last_entry_bar
        idle_guard = idle_bars >= (IDLE_HOURS * 12)

    price = ind5m["price"]

    # Signal 1: EMA21_PULLBACK
    if ind5m["ema21"] > 0:
        pct_below = (ind5m["ema21"] - price) / ind5m["ema21"]
        if 0.0 <= pct_below <= EMA21_PULL_MAX and ind5m["rsi14"] < RSI_PULL_THR:
            return {"signal": "EMA21_PULLBACK", "idle_guard": idle_guard}

    # Signal 2: RSI_OVERSOLD + Trend Intact
    if ind5m["rsi14"] < RSI_OVS_THR and price > ind5m["ema55"] and ind5m["ema55"] > 0:
        return {"signal": "RSI_OVERSOLD", "idle_guard": idle_guard}

    return None

# ============================================================
# TRAIL / HARD STOP — identical to backtest v2.1
# ============================================================

def check_trail(pos: dict, current_price: float) -> tuple:
    """Returns (should_exit, reason)."""
    hard_stop = pos["entry_price"] * (1 - HARD_STOP_PCT)
    if current_price <= hard_stop:
        return True, "HARD_STOP"

    if pos["bars_held"] >= MIN_HOLD_BARS:
        gain = (current_price - pos["entry_price"]) / pos["entry_price"]
        if gain > pos["peak_gain"]: pos["peak_gain"] = gain

        if   pos["peak_gain"] < 0.003: trail_pct = 0.0130
        elif pos["peak_gain"] < 0.007: trail_pct = 0.0080
        elif pos["peak_gain"] < 0.012: trail_pct = 0.0050
        else:                          trail_pct = 0.0030

        new_stop = current_price * (1 - trail_pct)
        if pos["peak_gain"] >= 0.003:
            new_stop = max(new_stop, pos["entry_price"] * 1.001)
        if new_stop > pos["stop_price"]:
            pos["stop_price"] = new_stop

        if current_price <= pos["stop_price"]:
            return True, "TRAIL_STOP"

    return False, ""

# ============================================================
# STATE MANAGER
# ============================================================

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
        self.last_entry_bar = -1
        self._paper_cash    = paper_equity   # tracks cash in paper mode only
        self._live_peak_seeded = False       # True once real balance seeds the peak
        self._load()
        self._init_audit()

    def _ts(self):
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def log(self, msg: str):
        line = f"[{self._ts()}] {msg}"
        print(line, flush=True)
        with open(self.path_events, "a") as f:
            f.write(line + "\n")

    def log_indicators(self, label: str, ind15m: dict, ind5m: dict,
                       regime: str, action: str):
        msg = (
            f"INDICATORS {label}"
            f" regime={regime}(15m)"
            f" 15m[ema21={ind15m['ema21']:.4f} ema55={ind15m['ema55']:.4f}]"
            f" 5m[price={ind5m['price']:.4f}"
            f" ema21={ind5m['ema21']:.4f}"
            f" ema55={ind5m['ema55']:.4f}"
            f" rsi14={ind5m['rsi14']:.2f}"
            f" bb_lower={ind5m['bb_lower']:.4f}"
            f" bb_upper={ind5m['bb_upper']:.4f}"
            f" adx14={ind5m['adx14']:.2f}]"
            f" action={action}"
        )
        self.log(msg)

    def log_trade(self, sym, side, price, qty, pnl, reason, bars):
        with open(self.path_audit, "a", newline="") as f:
            csv.writer(f).writerow([
                self._ts(), LABEL.get(sym,sym), side,
                f"{price:.6f}", f"{qty:.8f}", f"{pnl:.4f}",
                reason, bars
            ])

    def record_trade(self, win: bool, pnl: float):
        self.trades += 1
        if win: self.wins += 1
        self.total_pnl += pnl
        self._save()

    def print_stats(self):
        wr = 100 * self.wins / self.trades if self.trades else 0
        print(f"=== Stats: trades={self.trades} wr={wr:.1f}%"
              f" pnl=${self.total_pnl:.2f} equity=${self.equity:.2f} ===",
              flush=True)

    def _load(self):
        if not self.path_state.exists(): return
        try:
            j = json.loads(self.path_state.read_text())
            self.equity          = j.get("equity",         0.0)
            self.peak            = j.get("peak",           0.0)
            self.trades          = j.get("trades",         0)
            self.wins            = j.get("wins",           0)
            self.total_pnl       = j.get("total_pnl",      0.0)
            self.last_entry_bar  = j.get("last_entry_bar", -1)
            self._paper_cash     = j.get("paper_cash",     self._paper_cash)
            self._live_peak_seeded = j.get("live_peak_seeded", False)
            self.positions       = j.get("positions",      {})
            self.cooldowns       = j.get("cooldowns",      {})
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
                    "timestamp","symbol","side","price","qty",
                    "pnl","reason","bars_held"
                ])

# ============================================================
# MAIN BOT
# ============================================================

def main():
    print("╔══════════════════════════════════════════════════════╗")
    print("║   Kraken Bull Bot  v3.0  |  15m Regime / 5m Signal  ║")
    print("╚══════════════════════════════════════════════════════╝")
    print(f"BASE_DIR: {BASE_DIR}\n", flush=True)

    # ── NTP wait ──────────────────────────────────────────
    print(f"Waiting {NTP_WAIT_S}s for NTP clock to stabilise...")
    for i in range(NTP_WAIT_S, 0, -1):
        print(f"\r  {i}s remaining  ", end="", flush=True)
        time.sleep(1)
    print("\r  NTP wait complete.        ")

    # ── Load config ───────────────────────────────────────
    env = load_env(BASE_DIR / ".env")
    api_key    = env.get("KRAKEN_API_KEY", "")
    api_secret = env.get("KRAKEN_API_SECRET", "")
    paper_mode = env.get("PAPER_MODE",     "true").lower()  in ("true","1","yes")
    paper_eq   = float(env.get("PAPER_EQUITY",    "100.0"))
    dry_run    = env.get("DRY_RUN",        "false").lower() in ("true","1","yes")
    equity_cap = float(env.get("EQUITY_USD",       "0"))
    max_dd     = float(env.get("MAX_DRAWDOWN_PCT", "0.20"))

    # PAPER_MODE forces dry_run — no real orders under any circumstance
    if paper_mode: dry_run = True

    if not api_key or not api_secret:
        sys.exit("FATAL: KRAKEN_API_KEY or KRAKEN_API_SECRET missing from .env")

    if paper_mode:
        print("╔══════════════════════════════════════════════════════╗")
        print("║   PAPER MODE — live prices, simulated orders         ║")
        print(f"║   Starting equity: ${paper_eq:.2f}                        ║")
        print("║   Set PAPER_MODE=false in .env to trade for real     ║")
        print("╚══════════════════════════════════════════════════════╝\n")
    elif dry_run:
        print("*** DRY RUN — no real orders ***\n")

    api   = KrakenAPI(api_key, api_secret, dry_run)
    state = State(BASE_DIR, paper_mode=paper_mode, paper_equity=paper_eq)

    # ── Fetch live balance (skipped in paper mode) ───────
    print("Connecting to Kraken API...")
    if paper_mode:
        # Paper mode: use simulated equity, still verify API connectivity
        try:
            api.fetch_balance()   # connectivity check only
        except Exception as e:
            print(f"  WARNING: API connectivity check failed: {e}")
        balance = state.equity   # use paper equity
        print(f"  Paper mode: simulated equity = ${balance:.2f}")
    else:
        for attempt in range(10):
            try:
                balance = api.fetch_balance()
                break
            except Exception as e:
                print(f"  Balance fetch attempt {attempt+1}/10: {e}")
                if attempt == 9: sys.exit("FATAL: Cannot reach Kraken API")
                time.sleep(30)
        if equity_cap > 0: balance = min(balance, equity_cap)
        state.equity = balance
        # In live mode, always seed peak from real balance on boot.
        # This ensures drawdown % is relative to actual account value,
        # not a hardcoded constant or stale state.json value.
        state.peak   = balance
        state._live_peak_seeded = True
        state._save()

    mode_str = "PAPER" if paper_mode else ("DRY_RUN" if dry_run else "LIVE")
    state.log(f"=== BOT START | mode={mode_str}"
              f" | equity=${state.equity:.2f}"
              f" | max_dd={max_dd*100:.0f}% ===")

    # ── Candle caches ──────────────────────────────────────
    cache_5m  = {}   # sym -> list of candles
    cache_15m = {}   # sym -> list of candles
    prev_raw_regime = {}  # sym -> last raw regime string (for 2-bar confirmation)

    # ── Seed caches ───────────────────────────────────────
    state.log(f"Seeding candle cache for {len(SYMBOLS)} symbols...")
    for sym in SYMBOLS:
        label = LABEL[sym]
        try:
            c5  = api.fetch_ohlc(sym, 5,  200)
            c15 = api.fetch_ohlc(sym, 15, 200)
            if len(c5) >= WARMUP and len(c15) >= WARMUP:
                cache_5m[sym]  = c5
                cache_15m[sym] = c15
                ind15m = compute_indicators(c15)
                prev_raw_regime[sym] = ind15m["regime"] if ind15m else "NEUTRAL"
                conf = confirmed_regime(prev_raw_regime[sym], "NEUTRAL")
                state.log(f"  Seeded {label} | 5m:{len(c5)} 15m:{len(c15)} bars"
                          f" | regime={conf}")
            else:
                state.log(f"  WARNING: {label} insufficient bars — skipped")
        except Exception as e:
            state.log(f"  ERROR seeding {label}: {e}")

    # ── Timing gates ──────────────────────────────────────
    last_signal_t  = 0.0
    last_regime_t  = 0.0
    last_hourly_t  = 0.0
    bar_counter    = {}   # sym -> bar index counter for cooldowns

    # Force immediate first evaluation
    last_signal_t = 0.0
    last_regime_t = 0.0

    state.log("Seed complete. Entering main loop.")

    # ── Main loop ──────────────────────────────────────────
    global_bar = 0   # increments every 5-min cycle

    while not _shutdown:

        # Emergency stop file
        if (BASE_DIR / "EMERGENCY_STOP").exists():
            state.log("EMERGENCY_STOP file detected — shutting down.")
            break

        now = time.time()

        # ── Regime refresh (every 15 minutes) ─────────────
        if now - last_regime_t >= REGIME_INTERVAL_S:
            last_regime_t = now
            for sym in SYMBOLS:
                if sym not in cache_15m: continue
                try:
                    c15 = api.fetch_ohlc(sym, 15, 200)
                    cache_15m[sym] = c15
                    ind15m = compute_indicators(c15)
                    if ind15m:
                        new_raw = ind15m["regime"]
                        conf    = confirmed_regime(new_raw, prev_raw_regime.get(sym, "NEUTRAL"))
                        prev_raw_regime[sym] = new_raw
                except Exception as e:
                    state.log(f"WARNING: 15m fetch failed for {LABEL[sym]}: {e}")

        # ── Signal + trail check (every 5 minutes) ────────
        if now - last_signal_t >= SIGNAL_INTERVAL_S:
            last_signal_t = now
            global_bar   += 1

            # Refresh equity = cash + unrealised position value
            # CRITICAL: never compare cash-only to peak — positions are deployed capital
            try:
                cash = api.fetch_balance()
                if equity_cap > 0: cash = min(cash, equity_cap)
                if paper_mode:
                    cash = state._paper_cash

                # Add unrealised value of all open positions
                unrealised = 0.0
                for sym, pos in list(state.positions.items()):
                    try:
                        cur_price = api.fetch_price(sym)
                        if cur_price > 0:
                            unrealised += cur_price * pos["qty"]
                        else:
                            unrealised += pos["size_usd"]  # fallback: use cost basis
                    except Exception:
                        unrealised += pos["size_usd"]  # fallback: use cost basis

                total_equity = cash + unrealised
                state.equity = total_equity
                if total_equity > state.peak: state.peak = total_equity
                state._save()
            except Exception as e:
                state.log(f"WARNING: Equity refresh failed: {e}")

            # Drawdown kill switch
            if state.peak > 0:
                dd = (state.peak - state.equity) / state.peak
                if dd >= max_dd:
                    state.log(f"!!! DRAWDOWN KILL SWITCH: {dd*100:.1f}% drawdown"
                               f" from peak ${state.peak:.2f} — closing all positions !!!")
                    for sym in list(state.positions.keys()):
                        pos = state.positions[sym]
                        try:
                            price = api.fetch_price(sym)
                            qty   = pos["size_usd"] / pos["entry_price"]
                            pnl   = (price - pos["entry_price"]) * qty
                            api.market_sell(sym, qty)
                            state.record_trade(pnl > 0, pnl)
                            state.log_trade(sym, "SELL", price, qty, pnl,
                                            "DRAWDOWN_KILL", pos["bars_held"])
                            del state.positions[sym]
                        except Exception as e:
                            state.log(f"ERROR closing {sym}: {e}")
                    state._save()
                    state.print_stats()
                    return

            # ── Per-symbol evaluation ──────────────────────
            for sym in SYMBOLS:
                if sym not in cache_5m: continue
                label = LABEL[sym]

                try:
                    # Fetch fresh 5m candles
                    c5 = api.fetch_ohlc(sym, 5, 200)
                    cache_5m[sym] = c5
                    ind5m = compute_indicators(c5)
                    if not ind5m: continue
                    if ind5m["price"] <= 0.0000001:
                        # Pair returning zero prices — likely unavailable on Kraken
                        # Skip silently to avoid bad signals and division by zero
                        continue

                    # Get confirmed regime from 15m
                    ind15m = compute_indicators(cache_15m.get(sym, []))
                    if not ind15m: continue
                    regime = confirmed_regime(
                        ind15m["regime"],
                        prev_raw_regime.get(sym, "NEUTRAL")
                    )

                    # ── Exit logic ─────────────────────────
                    if sym in state.positions:
                        pos = state.positions[sym]
                        pos["bars_held"] += 1

                        # Bear regime exit
                        if regime == "BEAR":
                            price = ind5m["price"]
                            qty   = pos["size_usd"] / pos["entry_price"]
                            pnl   = (price - pos["entry_price"]) * qty
                            try: api.market_sell(sym, qty)
                            except Exception as e: state.log(f"SELL ERR {label}: {e}")
                            if paper_mode: state._paper_cash += pos["size_usd"] + pnl
                            state.record_trade(pnl > 0, pnl)
                            state.log_trade(sym, "SELL", price, qty, pnl,
                                            "BEAR_REGIME_EXIT", pos["bars_held"])
                            state.log(f"EXIT {label} BEAR_REGIME_EXIT"
                                      f" pnl=${pnl:.2f}")
                            del state.positions[sym]
                            bar_counter[sym] = global_bar + COOLDOWN_BARS
                            state._save()
                            continue

                        # Zombie kill: open > 48h (576 bars) and negative
                        if pos["bars_held"] >= 576 and ind5m["price"] < pos["entry_price"]:
                            price = ind5m["price"]
                            qty   = pos["size_usd"] / pos["entry_price"]
                            pnl   = (price - pos["entry_price"]) * qty
                            try: api.market_sell(sym, qty)
                            except Exception as e: state.log(f"SELL ERR {label}: {e}")
                            if paper_mode: state._paper_cash += pos["size_usd"] + pnl
                            state.record_trade(pnl > 0, pnl)
                            state.log_trade(sym, "SELL", price, qty, pnl,
                                            "ZOMBIE_KILL_48H", pos["bars_held"])
                            state.log(f"EXIT {label} ZOMBIE_KILL_48H pnl=${pnl:.2f}")
                            del state.positions[sym]
                            bar_counter[sym] = global_bar + COOLDOWN_BARS
                            state._save()
                            continue

                        # Trail / hard stop
                        price          = ind5m["price"]
                        should_exit, reason = check_trail(pos, price)
                        if should_exit:
                            qty = pos["size_usd"] / pos["entry_price"]
                            pnl = (price - pos["entry_price"]) * qty
                            try: api.market_sell(sym, qty)
                            except Exception as e: state.log(f"SELL ERR {label}: {e}")
                            if paper_mode: state._paper_cash += pos["size_usd"] + pnl
                            state.record_trade(pnl > 0, pnl)
                            state.log_trade(sym, "SELL", price, qty, pnl,
                                            reason, pos["bars_held"])
                            state.log(f"EXIT {label} {reason}"
                                      f" price={price:.4f} pnl=${pnl:.2f}"
                                      f" bars={pos['bars_held']}")
                            del state.positions[sym]
                            bar_counter[sym] = global_bar + COOLDOWN_BARS
                            state._save()
                        else:
                            gain = (price - pos["entry_price"]) / pos["entry_price"] * 100
                            state.log(f"HOLD {label} bars={pos['bars_held']}"
                                      f" gain={gain:+.2f}%"
                                      f" stop={pos['stop_price']:.4f}")
                            state.positions[sym] = pos
                            state._save()
                        continue

                    # ── Entry logic ────────────────────────
                    if bar_counter.get(sym, 0) > global_bar:
                        continue
                    if regime == "BEAR":
                        continue

                    is_flat = len(state.positions) == 0
                    sig = evaluate_signal(
                        ind5m, regime,
                        global_bar, state.last_entry_bar, is_flat
                    )
                    if not sig: continue

                    # Size position
                    deployable = state.equity * (1 - DRY_POWDER)
                    allocated  = sum(p["size_usd"] for p in state.positions.values())
                    available  = deployable - allocated
                    if available < 2.0: continue

                    size_pct = SIZE_HIGH if sig["idle_guard"] else SIZE_LOW
                    size_usd = min(state.equity * size_pct, available)
                    if size_usd < 2.0: continue

                    price = ind5m["price"]

                    # Guard: skip if price is zero or suspiciously small
                    # (indicates Kraken returned no data for this pair)
                    if price <= 0.0000001:
                        state.log(f"SKIP {label}: price={price} — pair may be unavailable on Kraken")
                        continue

                    qty   = size_usd / price

                    # Place order
                    try:
                        txid = api.market_buy(sym, qty)
                    except Exception as e:
                        state.log(f"BUY FAILED {label}: {e}")
                        continue

                    idle_note = (f" [IDLE_GUARD {(global_bar - state.last_entry_bar)//12:.0f}h flat]"
                                 if sig["idle_guard"] else "")
                    # Dynamic decimal places: micro-priced tokens need more precision
                    price_fmt = f"{price:.8f}".rstrip('0').rstrip('.')
                    state.log(f"ENTRY {label} {sig['signal']}"
                               f" price={price_fmt} size=${size_usd:.2f}"
                               f" qty={qty:.6f} txid={txid}{idle_note}")
                    state.log_trade(sym, "BUY", price, qty, 0.0, sig["signal"], 0)

                    if paper_mode: state._paper_cash -= size_usd
                    state.positions[sym] = {
                        "sym":         sym,
                        "entry_price": price,
                        "size_usd":    size_usd,
                        "qty":         qty,
                        "peak_gain":   0.0,
                        "stop_price":  price * (1 - HARD_STOP_PCT),
                        "bars_held":   0,
                        "signal":      sig["signal"],
                        "open_ts":     int(time.time()),
                    }
                    state.last_entry_bar = global_bar
                    state._save()

                except Exception as e:
                    state.log(f"ERROR processing {label}: {e}")

            state.print_stats()

        # ── Hourly indicator snapshot ──────────────────────
        if now - last_hourly_t >= HOURLY_INTERVAL_S:
            last_hourly_t = now
            state.log("--- Hourly indicator snapshot ---")
            for sym in SYMBOLS:
                if sym not in cache_5m or sym not in cache_15m: continue
                try:
                    ind5m  = compute_indicators(cache_5m[sym])
                    ind15m = compute_indicators(cache_15m[sym])
                    if not ind5m or not ind15m: continue
                    regime = confirmed_regime(
                        ind15m["regime"],
                        prev_raw_regime.get(sym, "NEUTRAL")
                    )
                    action = "IN_POSITION" if sym in state.positions else f"WATCHING regime={regime}"
                    state.log_indicators(LABEL[sym], ind15m, ind5m, regime, action)
                except Exception as e:
                    state.log(f"Hourly snapshot error {sym}: {e}")

        time.sleep(10)  # poll every 10 seconds, gates handle actual intervals

    # ── Shutdown ───────────────────────────────────────────
    state.log("=== BOT SHUTDOWN ===")
    state.print_stats()


if __name__ == "__main__":
    main()
