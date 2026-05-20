"""
signal_engine.py — HyroTrader / Cleo Real-Time Signal Engine
=============================================================
Watches crypto markets via WebSocket and fires actionable signals
for manual execution in the Cleo trading terminal.

No API keys needed for market data — uses Bybit public WebSocket feeds
(EU IP on EC2 avoids US geo-block).

Signals delivered via:
  1. Terminal  — colored, timestamped, always-on
  2. ntfy.sh   — push notification to your phone (free)

Virtual position tracker mirrors what you've entered in Cleo so the
engine knows when to signal EXIT (stop hit, trail moved, MACD flip).

HyroTrader rules tracked:
  Daily loss limit : -4.0% halt (HyroTrader limit -5%)
  Total loss limit : -8.0% halt (HyroTrader limit -10%)
  Profit target    : stop signalling new entries at +4.8%
  Min trade size   : 5% of $10,000 = $500
  Max risk/trade   : 3% of $10,000 = $300

Usage:
  pip install ccxt pandas pandas_ta requests
  python3 signal_engine.py

Config:
  Edit NTFY_TOPIC below, or set env var NTFY_TOPIC=your_topic
  Edit ACCOUNT_SIZE if different from $10,000
  Edit SYMBOLS if Cleo offers different assets

Phone notifications:
  Install the free "ntfy" app (iOS / Android)
  Subscribe to your NTFY_TOPIC
"""

import asyncio
import json
import logging
import math
import os
import sys
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import ccxt.pro as ccxtpro
import pandas as pd
import pandas_ta as ta
import requests

# ═══════════════════════════════════════════════════════════════
# CONFIGURATION  — edit these
# ═══════════════════════════════════════════════════════════════

ACCOUNT_SIZE  = float(os.getenv("ACCOUNT_SIZE", "10000"))   # HyroTrader account
NTFY_TOPIC    = os.getenv("NTFY_TOPIC", "hyro_signals_change_me")  # ntfy.sh topic

# Assets to watch — edit to match what Cleo offers
SYMBOLS = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "BNB/USDT:USDT",
    "DOGE/USDT:USDT",
    "AVAX/USDT:USDT",
]
REGIME_ANCHOR = "BTC/USDT:USDT"

# ── HyroTrader limits ─────────────────────────────────────────
DAILY_HALT_PCT  = 0.040   # stop signalling at -4% daily
TOTAL_HALT_PCT  = 0.080   # stop signalling at -8% total
ENTRY_CUTOFF    = 0.048   # stop new entries at +4.8%
PROFIT_LOCK_PCT = 0.040   # halve size at +4%

# ── Position sizing ───────────────────────────────────────────
MIN_TRADE_PCT   = 0.05    # 5% of account = $500 minimum
MAX_RISK_PCT    = 0.03    # 3% of account = $300 max loss
SIZE_HIGH_PCT   = 0.20    # 20% of account for high-conviction
SIZE_LOW_PCT    = 0.12    # 12% of account for standard

# ── Phase management ──────────────────────────────────────────
HARD_STOP_PCT      = 0.015   # 1.5% initial stop (Phase 1)
GREEN_TRIGGER_PCT  = 0.005   # +0.5% → switch to ATR trail (Phase 2)
BREAKEVEN_BUFFER   = 0.001   # floor = entry × 1.001

ATR_TRAIL_TIERS = [
    (0.000, 1.8),
    (0.005, 1.4),
    (0.010, 1.0),
    (0.020, 0.6),
    (0.030, 0.3),
]
LIGHT_GREEN_MULT = 0.3

# ── Indicators ────────────────────────────────────────────────
EMA_FAST, EMA_SLOW    = 21, 55
MACD_F, MACD_S, MACD_SIG = 12, 26, 9
RSI_LEN               = 14
BB_LEN, BB_STD        = 20, 2.0
VOL_RATIO_MIN         = 1.4
MIN_CONVICTION        = 62
MOMENTUM_WINDOW_H     = 2.0
MAX_HOLD_HOURS        = 12
FAIL_BARS_5M          = 6
FAIL_PAIN_PCT         = -0.008
COOLDOWN_HOURS        = 2

# ── Infrastructure ────────────────────────────────────────────
HEARTBEAT_SEC = 30
CANDLE_LIMIT  = 200
LOG_FILE      = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signal_engine.log")


# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
    datefmt="%H:%M:%S",
)
log = logging.getLogger("signals")

# ANSI colours for terminal
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"


# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

@dataclass
class SymbolState:
    candles_1m:     deque = field(default_factory=lambda: deque(maxlen=60))
    candles_5m:     deque = field(default_factory=lambda: deque(maxlen=200))
    candles_15m:    deque = field(default_factory=lambda: deque(maxlen=200))
    regime:         str   = "NEUTRAL"
    regime_flip_ts: float = 0.0
    macd_state_5m:  str   = "NONE"
    atr_5m:         float = 0.0
    atr_15m:        float = 0.0
    cooldown_until: float = 0.0
    last_price:     float = 0.0


@dataclass
class VirtualPosition:
    """Mirrors what you've entered in Cleo so we can track exits."""
    symbol:       str
    entry_price:  float
    size_usd:     float
    qty:          float
    entry_ts:     float
    entry_mode:   str
    hard_stop:    float
    trail_stop:   float
    phase:        int   = 1
    peak_price:   float = 0.0
    ever_green:   bool  = False
    bars_5m_held: int   = 0
    light_green:  bool  = False


# ═══════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════

sym_state:  Dict[str, SymbolState]    = {s: SymbolState() for s in SYMBOLS}
virt_pos:   Dict[str, VirtualPosition] = {}

# P&L tracking
_starting_eq  = ACCOUNT_SIZE
_virtual_eq   = ACCOUNT_SIZE     # updated as virtual trades close
_peak_eq      = ACCOUNT_SIZE
_day_start_eq = ACCOUNT_SIZE
_current_day  = ""
_sizing_mod   = 1.0
_halted       = False
_halt_reason  = ""
_trading_days: set = set()
_total_pnl    = 0.0

_exchange: Optional[ccxtpro.bybit] = None
_lock = asyncio.Lock()


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def sf(v, d: float = 0.0) -> float:
    try:
        f = float(v)
        return d if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return d

def now_ts() -> float:   return time.time()
def utc_day() -> str:    return datetime.now(timezone.utc).strftime("%Y-%m-%d")
def ts_str() -> str:     return datetime.now(timezone.utc).strftime("%H:%M:%S")

def short_sym(sym: str) -> str:
    return sym.split("/")[0]   # BTC/USDT:USDT → BTC

def atr_mult(gain_pct: float) -> float:
    for thr, mult in reversed(ATR_TRAIL_TIERS):
        if gain_pct >= thr:
            return mult
    return ATR_TRAIL_TIERS[0][1]

def candles_to_df(buf: deque) -> Optional[pd.DataFrame]:
    if len(buf) < 35:
        return None
    df = pd.DataFrame(list(buf), columns=["ts","open","high","low","close","volume"])
    for c in ("open","high","low","close","volume"):
        df[c] = df[c].astype(float)
    return df.reset_index(drop=True)

def total_pnl_pct() -> float:
    return (_virtual_eq - _starting_eq) / _starting_eq

def daily_pnl_pct() -> float:
    return (_virtual_eq - _day_start_eq) / _day_start_eq

def unrealised_pnl() -> float:
    total = 0.0
    for sym, pos in virt_pos.items():
        price = sym_state[sym].last_price
        if price > 0:
            total += (price - pos.entry_price) * pos.qty
    return total


# ═══════════════════════════════════════════════════════════════
# PUSH NOTIFICATIONS
# ═══════════════════════════════════════════════════════════════

def push(title: str, message: str, priority: str = "default") -> None:
    """Send push notification via ntfy.sh (free, no account needed)."""
    if NTFY_TOPIC == "hyro_signals_change_me":
        return   # not configured yet
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=message.encode("utf-8"),
            headers={
                "Title":    title,
                "Priority": priority,
                "Tags":     "chart_with_upwards_trend",
            },
            timeout=5,
        )
    except Exception:
        pass   # notifications are best-effort


# ═══════════════════════════════════════════════════════════════
# RISK CHECKS
# ═══════════════════════════════════════════════════════════════

def update_risk(eq: float) -> None:
    global _virtual_eq, _peak_eq, _day_start_eq, _current_day
    global _halted, _halt_reason, _sizing_mod

    today = utc_day()
    if today != _current_day:
        _current_day  = today
        _day_start_eq = _virtual_eq
        if _halted and "DAILY" in _halt_reason:
            _halted, _halt_reason = False, ""
            log.info("Daily halt cleared — new trading day")

    _virtual_eq = eq
    if eq > _peak_eq:
        _peak_eq = eq

    if daily_pnl_pct() <= -DAILY_HALT_PCT and not _halted:
        _halted      = True
        _halt_reason = f"DAILY_HALT {daily_pnl_pct():.2%}"
        msg = f"TRADING HALTED — daily loss {daily_pnl_pct():.2%}"
        log.critical(f"{RED}{BOLD}{msg}{RESET}")
        push("⛔ HALT", msg, "urgent")

    if total_pnl_pct() <= -TOTAL_HALT_PCT and not _halted:
        _halted      = True
        _halt_reason = f"TOTAL_HALT {total_pnl_pct():.2%}"
        msg = f"TRADING HALTED — total loss {total_pnl_pct():.2%}"
        log.critical(f"{RED}{BOLD}{msg}{RESET}")
        push("⛔ HALT", msg, "urgent")

    pnl = total_pnl_pct()
    new_mod = 0.5 if pnl >= PROFIT_LOCK_PCT else (0.65 if pnl >= PROFIT_LOCK_PCT * 0.75 else 1.0)
    if new_mod != _sizing_mod:
        log.info(f"{YELLOW}Sizing reduced to {new_mod:.0%} — protecting profit{RESET}")
        _sizing_mod = new_mod


def can_signal() -> Tuple[bool, str]:
    if _halted:
        return False, _halt_reason
    if daily_pnl_pct() <= -DAILY_HALT_PCT:
        return False, f"daily loss {daily_pnl_pct():.2%}"
    if total_pnl_pct() <= -TOTAL_HALT_PCT:
        return False, f"total loss {total_pnl_pct():.2%}"
    if total_pnl_pct() >= ENTRY_CUTOFF:
        return False, f"near profit target ({total_pnl_pct():.2%}) — protect gains"
    return True, "ok"


# ═══════════════════════════════════════════════════════════════
# INDICATOR HELPERS
# ═══════════════════════════════════════════════════════════════

def compute_regime(df15: pd.DataFrame) -> Tuple[str, float]:
    df = df15.copy()
    df["ef"] = ta.ema(df["close"], length=EMA_FAST)
    df["es"] = ta.ema(df["close"], length=EMA_SLOW)
    atr_s    = ta.atr(df["high"], df["low"], df["close"], length=14)
    r        = df.iloc[-2]
    ef, es, cl = sf(r.get("ef",0)), sf(r.get("es",0)), sf(r["close"])
    atr      = sf(atr_s.iloc[-2]) if atr_s is not None and len(atr_s) >= 2 else 0.0
    if ef > es and cl > ef:   return "BULL",    atr
    elif ef < es:              return "BEAR",    atr
    return "NEUTRAL", atr


def compute_5m_signals(df5: pd.DataFrame) -> Tuple[str, dict, float]:
    df = df5.copy()
    macd_df  = ta.macd(df["close"], fast=MACD_F, slow=MACD_S, signal=MACD_SIG)
    hist_key = f"MACDh_{MACD_F}_{MACD_S}_{MACD_SIG}"
    if macd_df is None or hist_key not in macd_df.columns:
        return "NONE", {}, 0.0

    df["hist"] = macd_df[hist_key]
    df["ef"]   = ta.ema(df["close"], length=EMA_FAST)
    df["es"]   = ta.ema(df["close"], length=EMA_SLOW)
    df["rsi"]  = ta.rsi(df["close"], length=RSI_LEN)
    bb = ta.bbands(df["close"], length=BB_LEN, std=BB_STD)
    if bb is not None:
        df["bb_lower"] = bb[f"BBL_{BB_LEN}_{float(BB_STD)}"]
        df["bb_mid"]   = bb[f"BBM_{BB_LEN}_{float(BB_STD)}"]
    atr_s = ta.atr(df["high"], df["low"], df["close"], length=14)
    atr5  = sf(atr_s.iloc[-2]) if atr_s is not None and len(atr_s) >= 2 else 0.0

    r, r1, r2 = df.iloc[-2], df.iloc[-3], df.iloc[-4]
    h0 = sf(r.get("hist",0)); h1 = sf(r1.get("hist",0)); h2 = sf(r2.get("hist",0))

    if h0 < 0 and h1 < 0 and h2 < 0 and h0 > h1 and h1 > h2:
        macd_state = "PINK"
    elif h0 < 0 and h1 < 0 and h0 > h1:
        macd_state = "PINK_1"
    elif h0 > 0 and h1 <= 0:
        macd_state = "GREEN_CROSS"
    elif h0 > 0 and h1 > 0 and h0 < h1:
        macd_state = "LIGHT_GREEN"
    else:
        macd_state = "NONE"

    vol_avg = df["volume"].rolling(20).mean().iloc[-2]
    vol_r   = sf(r["volume"]) / max(sf(vol_avg), 1e-10)

    ind = {
        "close":    sf(r["close"]),
        "ef":       sf(r.get("ef",0)),
        "es":       sf(r.get("es",0)),
        "rsi":      sf(r.get("rsi",50)),
        "bb_lower": sf(r.get("bb_lower",0)),
        "bb_mid":   sf(r.get("bb_mid",0)),
        "vol_ratio": vol_r,
        "h0": h0, "h1": h1,
    }
    return macd_state, ind, atr5


# ═══════════════════════════════════════════════════════════════
# SIGNAL PRINTING
# ═══════════════════════════════════════════════════════════════

def print_entry_signal(sym: str, mode: str, price: float, stop: float,
                       size_usd: float, conviction: int, reasons: list) -> None:
    name     = short_sym(sym)
    stop_pct = abs(price - stop) / price * 100
    qty      = size_usd / price

    line = (
        f"\n{GREEN}{BOLD}{'═'*55}{RESET}\n"
        f"{GREEN}{BOLD}  ▶ BUY SIGNAL  {name}  [{mode}]{RESET}\n"
        f"{GREEN}{'─'*55}{RESET}\n"
        f"  Time      : {ts_str()} UTC\n"
        f"  Entry at  : {price:.4f} USDT\n"
        f"  Stop loss : {stop:.4f}  ({stop_pct:.1f}% below)\n"
        f"  Size      : ${size_usd:,.0f}  ({size_usd/ACCOUNT_SIZE*100:.0f}% of account)\n"
        f"  Qty       : {qty:.4f} {name}\n"
        f"  Conviction: {conviction}/100\n"
        f"  Signals   : {', '.join(reasons)}\n"
        f"{GREEN}{'═'*55}{RESET}\n"
    )
    print(line)
    log.info(f"BUY {name} | {mode} | entry={price:.4f} stop={stop:.4f} size=${size_usd:.0f}")

    push(
        f"▶ BUY {name}",
        f"Entry: {price:.4f}\nStop: {stop:.4f} ({stop_pct:.1f}%)\n"
        f"Size: ${size_usd:.0f} | {mode}\n"
        f"Signals: {', '.join(reasons)}",
        "high",
    )


def print_exit_signal(sym: str, reason: str, entry: float, exit_price: float,
                      size_usd: float) -> None:
    name    = short_sym(sym)
    pnl_pct = (exit_price - entry) / entry * 100
    pnl_usd = (exit_price - entry) * (size_usd / entry)
    colour  = GREEN if pnl_pct >= 0 else RED

    line = (
        f"\n{colour}{BOLD}{'═'*55}{RESET}\n"
        f"{colour}{BOLD}  ✖ EXIT SIGNAL  {name}  [{reason}]{RESET}\n"
        f"{colour}{'─'*55}{RESET}\n"
        f"  Time      : {ts_str()} UTC\n"
        f"  Exit at   : {exit_price:.4f} USDT\n"
        f"  Entry was : {entry:.4f} USDT\n"
        f"  P&L       : {pnl_pct:+.2f}%  (${pnl_usd:+.2f})\n"
        f"{colour}{'═'*55}{RESET}\n"
    )
    print(line)
    log.info(f"EXIT {name} | {reason} | pnl={pnl_pct:+.2f}% ${pnl_usd:+.2f}")

    emoji = "✅" if pnl_pct >= 0 else "❌"
    push(
        f"{emoji} CLOSE {name}",
        f"Reason: {reason}\nExit: {exit_price:.4f}\n"
        f"P&L: {pnl_pct:+.2f}% (${pnl_usd:+.2f})",
        "high" if abs(pnl_pct) > 1 else "default",
    )


def print_sl_update(sym: str, old_sl: float, new_sl: float) -> None:
    name = short_sym(sym)
    line = (
        f"{YELLOW}  ↑ MOVE STOP  {name}  "
        f"{old_sl:.4f} → {new_sl:.4f}{RESET}"
    )
    print(line)
    log.info(f"MOVE SL {name} | {old_sl:.4f} → {new_sl:.4f}")
    push(f"↑ Move stop {name}", f"New stop: {new_sl:.4f} (was {old_sl:.4f})")


# ═══════════════════════════════════════════════════════════════
# ENTRY EVALUATION
# ═══════════════════════════════════════════════════════════════

def evaluate_entry(sym: str) -> Optional[dict]:
    ss = sym_state[sym]

    ok, _ = can_signal()
    if not ok:
        return None
    if now_ts() < ss.cooldown_until:
        return None
    if sym in virt_pos or len(virt_pos) >= 3:
        return None

    df5 = candles_to_df(ss.candles_5m)
    if df5 is None:
        return None

    macd_state, ind, atr5 = compute_5m_signals(df5)
    ss.macd_state_5m = macd_state
    ss.atr_5m        = atr5

    if ind["vol_ratio"] < VOL_RATIO_MIN:
        return None
    if sym != REGIME_ANCHOR and sym_state[REGIME_ANCHOR].regime == "BEAR":
        return None
    if ss.regime == "BEAR":
        return None

    regime_age_h = (now_ts() - ss.regime_flip_ts) / 3600 if ss.regime_flip_ts > 0 else 99.0

    close    = ind["close"]
    reasons  = []

    # MOMENTUM_MODE
    if ss.regime == "BULL" and regime_age_h < MOMENTUM_WINDOW_H:
        if macd_state != "GREEN_CROSS":
            return None
        if ind["rsi"] >= 65:
            return None
        mode, size_pct, conviction = "MOMENTUM", SIZE_HIGH_PCT, 80
        reasons = ["MACD_CROSS_ZERO", f"RSI={ind['rsi']:.0f}",
                   f"REGIME_AGE={regime_age_h:.1f}h"]

    # PULLBACK_MODE
    elif ss.regime == "BULL" and regime_age_h >= MOMENTUM_WINDOW_H:
        if macd_state != "PINK":
            return None
        if ind["rsi"] >= 55:
            return None

        ef, es, bb_lo, rsi = ind["ef"], ind["es"], ind["bb_lower"], ind["rsi"]
        at_bb  = bb_lo > 0 and close <= bb_lo * 1.005
        at_ema = ef > 0 and close >= ef and (close - ef) / ef < 0.0075
        rsi_dip = rsi < 45 and es > 0 and close > es

        if not (at_bb or at_ema or rsi_dip):
            return None

        conviction = 40
        if at_bb:               conviction += 12; reasons.append("BB_LOWER")
        if at_ema:              conviction += 8;  reasons.append("EMA21_PULLBACK")
        if rsi_dip:             conviction += 7;  reasons.append(f"RSI_DIP={rsi:.0f}")
        if rsi < 35:            conviction += 5;  reasons.append("RSI_EXTREME")
        if ind["vol_ratio"] >= 2.0: conviction += 5; reasons.append(f"VOL={ind['vol_ratio']:.1f}x")
        reasons.append("MACD_PINK")

        if conviction < MIN_CONVICTION:
            return None
        mode     = "PULLBACK"
        size_pct = SIZE_HIGH_PCT if conviction >= 75 else SIZE_LOW_PCT

    # NEUTRAL mean-reversion
    elif ss.regime == "NEUTRAL":
        if macd_state != "PINK":
            return None
        bb_lo = ind["bb_lower"]
        if not (bb_lo > 0 and close <= bb_lo * 1.003):
            return None
        if ind["rsi"] >= 48:
            return None
        mode, size_pct, conviction = "PULLBACK", SIZE_LOW_PCT, 62
        reasons = ["MACD_PINK", "BB_LOWER", "NEUTRAL_REGIME"]

    else:
        return None

    # Size
    raw_size = ACCOUNT_SIZE * size_pct * _sizing_mod
    min_size = ACCOUNT_SIZE * MIN_TRADE_PCT
    max_size = (ACCOUNT_SIZE * MAX_RISK_PCT) / HARD_STOP_PCT
    size_usd = max(min(raw_size, max_size), min_size)

    if close <= 0:
        return None

    qty   = size_usd / close
    stop  = close * (1 - HARD_STOP_PCT)

    return {
        "entry_price": close,
        "size_usd":    size_usd,
        "qty":         qty,
        "stop_price":  stop,
        "mode":        mode,
        "conviction":  conviction,
        "reasons":     reasons,
    }


# ═══════════════════════════════════════════════════════════════
# VIRTUAL POSITION MANAGEMENT
# ═══════════════════════════════════════════════════════════════

def open_virtual(sym: str, entry: dict) -> None:
    pos = VirtualPosition(
        symbol      = sym,
        entry_price = entry["entry_price"],
        size_usd    = entry["size_usd"],
        qty         = entry["qty"],
        entry_ts    = now_ts(),
        entry_mode  = entry["mode"],
        hard_stop   = entry["stop_price"],
        trail_stop  = entry["stop_price"],
        peak_price  = entry["entry_price"],
    )
    virt_pos[sym] = pos
    print_entry_signal(
        sym, entry["mode"], entry["entry_price"],
        entry["stop_price"], entry["size_usd"],
        entry["conviction"], entry["reasons"],
    )


def close_virtual(sym: str, reason: str, price: float) -> None:
    global _virtual_eq, _total_pnl
    if sym not in virt_pos:
        return
    pos = virt_pos.pop(sym)

    pnl_usd = (price - pos.entry_price) * pos.qty
    _virtual_eq  += pnl_usd
    _total_pnl   += pnl_usd
    update_risk(_virtual_eq)

    sym_state[sym].cooldown_until = now_ts() + COOLDOWN_HOURS * 3600
    if abs((price - pos.entry_price) / pos.entry_price) >= 0.01:
        _trading_days.add(utc_day())

    print_exit_signal(sym, reason, pos.entry_price, price, pos.size_usd)


# ═══════════════════════════════════════════════════════════════
# WEBSOCKET TASKS
# ═══════════════════════════════════════════════════════════════

async def get_exchange() -> ccxtpro.bybit:
    global _exchange
    if _exchange is None:
        _exchange = ccxtpro.bybit({
            "options":         {"defaultType": "linear"},
            "enableRateLimit": True,
        })
        await _exchange.load_markets()
    return _exchange


async def task_15m(sym: str) -> None:
    exch = await get_exchange()
    while True:
        try:
            candles = await exch.watch_ohlcv(sym, "15m")
            ss = sym_state[sym]
            for c in candles:
                ss.candles_15m.append(c)
            df15 = candles_to_df(ss.candles_15m)
            if df15 is None:
                continue
            new_regime, atr15 = compute_regime(df15)
            ss.atr_15m = atr15
            if new_regime != ss.regime:
                log.info(f"{CYAN}{short_sym(sym)} regime: {ss.regime}→{new_regime}{RESET}")
                if new_regime == "BEAR" and sym in virt_pos:
                    log.info(f"{YELLOW}  → BEAR flip while holding {short_sym(sym)} — signalling exit{RESET}")
                    price = sym_state[sym].last_price
                    if price > 0:
                        close_virtual(sym, "BEAR_REGIME_EXIT", price)
                ss.prev_regime    = ss.regime
                ss.regime         = new_regime
                ss.regime_flip_ts = now_ts()
        except Exception as e:
            log.warning(f"{sym} 15m error: {e}")
            await asyncio.sleep(10)


async def task_5m(sym: str) -> None:
    exch = await get_exchange()
    while True:
        try:
            candles = await exch.watch_ohlcv(sym, "5m")
            ss = sym_state[sym]
            for c in candles:
                ss.candles_5m.append(c)
            if sym in virt_pos:
                virt_pos[sym].bars_5m_held += 1

            df5 = candles_to_df(ss.candles_5m)
            if df5 is None:
                continue
            macd_state, ind, atr5 = compute_5m_signals(df5)
            ss.macd_state_5m = macd_state
            ss.atr_5m        = atr5

            # LIGHT_GREEN → tighten trail
            if sym in virt_pos and macd_state == "LIGHT_GREEN":
                pos = virt_pos[sym]
                if pos.phase == 2 and not pos.light_green:
                    pos.light_green = True
                    atr   = ss.atr_5m or ss.atr_15m or (pos.entry_price * 0.005)
                    trail = pos.peak_price - LIGHT_GREEN_MULT * atr
                    floor = pos.entry_price * (1 + BREAKEVEN_BUFFER)
                    trail = max(trail, floor)
                    if trail > pos.trail_stop:
                        old = pos.trail_stop
                        pos.trail_stop = trail
                        print_sl_update(sym, old, trail)

            # Failed signal cut
            if sym in virt_pos:
                pos  = virt_pos[sym]
                last = ss.last_price
                if (pos.phase == 1 and not pos.ever_green
                        and pos.bars_5m_held >= FAIL_BARS_5M and last > 0):
                    gain = (last - pos.entry_price) / pos.entry_price
                    if gain <= FAIL_PAIN_PCT:
                        close_virtual(sym, "FAILED_SIGNAL", last)
                        continue

            # Entry evaluation
            if sym not in virt_pos:
                entry = evaluate_entry(sym)
                if entry is not None:
                    open_virtual(sym, entry)

        except Exception as e:
            log.warning(f"{sym} 5m error: {e}")
            await asyncio.sleep(10)


async def task_1m(sym: str) -> None:
    exch = await get_exchange()
    while True:
        try:
            candles = await exch.watch_ohlcv(sym, "1m")
            ss = sym_state[sym]
            for c in candles:
                ss.candles_1m.append(c)

            if len(ss.candles_1m) < 2:
                continue
            prev  = list(ss.candles_1m)[-2]
            low   = sf(prev[3])
            high  = sf(prev[2])
            close = sf(prev[4])
            ss.last_price = close

            if sym not in virt_pos:
                continue
            pos = virt_pos[sym]

            if high > pos.peak_price:
                pos.peak_price = high
            if close > pos.entry_price:
                pos.ever_green = True

            gain_pct = (close - pos.entry_price) / pos.entry_price
            hold_h   = (now_ts() - pos.entry_ts) / 3600

            # Phase 1
            if pos.phase == 1:
                if low <= pos.hard_stop:
                    close_virtual(sym, "HARD_STOP", pos.hard_stop)
                    continue
                if hold_h >= MAX_HOLD_HOURS:
                    close_virtual(sym, "TIME_STOP", close)
                    continue
                if gain_pct >= GREEN_TRIGGER_PCT:
                    atr   = ss.atr_5m or ss.atr_15m or (pos.entry_price * 0.008)
                    trail = pos.peak_price - ATR_TRAIL_TIERS[0][1] * atr
                    floor = pos.entry_price * (1 + BREAKEVEN_BUFFER)
                    trail = max(trail, floor)
                    pos.phase      = 2
                    pos.trail_stop = trail
                    print_sl_update(sym, pos.hard_stop, trail)

            # Phase 2
            else:
                atr   = ss.atr_5m or ss.atr_15m or (pos.entry_price * 0.008)
                mult  = atr_mult(gain_pct)
                trail = pos.peak_price - mult * atr
                floor = pos.entry_price * (1 + BREAKEVEN_BUFFER)
                trail = max(trail, floor)
                if trail > pos.trail_stop:
                    old = pos.trail_stop
                    pos.trail_stop = trail
                    # Only print SL update if moved by >0.1% (avoid spam)
                    if abs(trail - old) / max(old, 1e-10) > 0.001:
                        print_sl_update(sym, old, trail)

                if low <= pos.trail_stop:
                    close_virtual(sym, "TRAIL_STOP", pos.trail_stop)
                    continue
                if hold_h >= MAX_HOLD_HOURS:
                    close_virtual(sym, "TIME_STOP", close)
                    continue

        except Exception as e:
            log.warning(f"{sym} 1m error: {e}")
            await asyncio.sleep(5)


# ═══════════════════════════════════════════════════════════════
# HEARTBEAT
# ═══════════════════════════════════════════════════════════════

async def task_heartbeat() -> None:
    while True:
        await asyncio.sleep(HEARTBEAT_SEC)
        try:
            # Update risk with unrealised P&L included
            eq_with_unreal = _virtual_eq + unrealised_pnl()
            update_risk(eq_with_unreal)

            ok, block_reason = can_signal()
            status = f"{GREEN}ACTIVE{RESET}" if ok else f"{RED}BLOCKED: {block_reason}{RESET}"

            # Position table
            if virt_pos:
                pos_lines = []
                for sym, pos in virt_pos.items():
                    price = sym_state[sym].last_price
                    gain  = (price - pos.entry_price) / pos.entry_price * 100 if price > 0 else 0
                    col   = GREEN if gain >= 0 else RED
                    stop  = pos.trail_stop if pos.phase == 2 else pos.hard_stop
                    pos_lines.append(
                        f"  {short_sym(sym):6s}  entry={pos.entry_price:.4f}  "
                        f"now={price:.4f}  {col}{gain:+.2f}%{RESET}  "
                        f"stop={stop:.4f}  ph={pos.phase}"
                    )
                positions_str = "\n".join(pos_lines)
            else:
                positions_str = "  (no open positions)"

            total_col = GREEN if total_pnl_pct() >= 0 else RED
            daily_col = GREEN if daily_pnl_pct() >= 0 else RED

            print(
                f"\n{CYAN}{'─'*55}{RESET}\n"
                f"  {ts_str()} UTC  |  Status: {status}\n"
                f"  Account  : ${_virtual_eq:,.2f}  "
                f"(start ${_starting_eq:,.2f})\n"
                f"  Total P&L: {total_col}{total_pnl_pct():+.2f}%{RESET}  "
                f"Daily: {daily_col}{daily_pnl_pct():+.2f}%{RESET}  "
                f"Days: {len(_trading_days)}/5\n"
                f"  Positions: {len(virt_pos)}/3\n"
                f"{positions_str}\n"
                f"{CYAN}{'─'*55}{RESET}"
            )
        except Exception as e:
            log.warning(f"heartbeat error: {e}")


# ═══════════════════════════════════════════════════════════════
# BOOT + MAIN
# ═══════════════════════════════════════════════════════════════

async def boot() -> None:
    global _current_day, _day_start_eq

    print(f"\n{BOLD}{'═'*55}")
    print(f"  HyroTrader Signal Engine")
    print(f"  Account: ${ACCOUNT_SIZE:,.0f}  |  Symbols: {len(SYMBOLS)}")
    print(f"  ntfy topic: {NTFY_TOPIC}")
    print(f"{'═'*55}{RESET}\n")

    if NTFY_TOPIC == "hyro_signals_change_me":
        print(f"{YELLOW}  ⚠  Set NTFY_TOPIC env var for phone alerts{RESET}\n")

    await asyncio.sleep(2)
    exch = await get_exchange()

    log.info("Seeding candle history...")
    for sym in SYMBOLS:
        for tf in ("1m", "5m", "15m"):
            try:
                ohlcvs = await exch.fetch_ohlcv(sym, tf, limit=CANDLE_LIMIT)
                buf    = getattr(sym_state[sym], f"candles_{tf}")
                for c in ohlcvs:
                    buf.append(c)
                await asyncio.sleep(0.2)
            except Exception as e:
                log.warning(f"  {sym} {tf} seed failed: {e}")

        df15 = candles_to_df(sym_state[sym].candles_15m)
        if df15 is not None:
            r, atr = compute_regime(df15)
            sym_state[sym].regime   = r
            sym_state[sym].atr_15m  = atr
            col = GREEN if r == "BULL" else (RED if r == "BEAR" else YELLOW)
            log.info(f"  {short_sym(sym):6s} regime: {col}{r}{RESET}")

    _current_day  = utc_day()
    _day_start_eq = _virtual_eq

    print(f"\n{GREEN}{BOLD}Signal engine running. Watching {len(SYMBOLS)} symbols.{RESET}")
    print(f"Signals will appear here and on ntfy/{NTFY_TOPIC}\n")


async def main() -> None:
    await boot()

    tasks = []
    for sym in SYMBOLS:
        tasks.append(asyncio.create_task(task_15m(sym)))
        tasks.append(asyncio.create_task(task_5m(sym)))
        tasks.append(asyncio.create_task(task_1m(sym)))
    tasks.append(asyncio.create_task(task_heartbeat()))

    log.info(f"Running {len(tasks)} tasks")
    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        pass
    finally:
        if _exchange:
            await _exchange.close()
        print("\nShutdown.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped.")
