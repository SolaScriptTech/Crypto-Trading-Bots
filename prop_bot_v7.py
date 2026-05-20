"""
prop_bot_v7.py — HyroTrader Bybit Prop Evaluation Bot (V7 Architecture)
==============================================================================
Ported from kraken_bull_bot_v7.py (live: 54.8% WR, $100 → $145 in 7 days).

HyroTrader rules enforced
  Profit target   : 5%  closed P&L    (bot stops new entries at +4.8%)
  Daily drawdown  : 5%  max           (bot halts at -4.0%)
  Max total loss  : 10% max           (bot halts at -8.0%)
  Stop loss       : Required within 5 min — Bybit native position SL only
  Max risk/trade  : 3%  of starting equity
  Min trade size  : 5%  of starting equity
  Min trading days: 5 (tracked in audit)
  Qualifying trade: |PnL| >= 1% of trade size (auto-satisfied by our stops)

Architecture  — 3 async WebSocket tasks per symbol + heartbeat
  task_15m   regime detection   EMA21/55 crossover, regime-age, 15m ATR
  task_5m    signal evaluation  MACD state (PINK/GREEN_CROSS/LIGHT_GREEN), entry
  task_1m    position monitor   Phase 1 hard stop → Phase 2 ATR trail, Bybit SL sync
  heartbeat  equity fetch, state persistence, kill-switch, P&L report

Dual-mode entry
  MOMENTUM_MODE  regime just flipped BULL (<2 h ago)  MACD GREEN_CROSS, RSI<65
  PULLBACK_MODE  established BULL (>=2 h)              MACD PINK(2+) + price cond

Native Bybit SL
  POST /v5/position/trading-stop  (NOT a conditional order)
  Placed immediately after every entry; updated as trail moves.

Usage
  export BYBIT_API_KEY=xxx
  export BYBIT_API_SECRET=xxx
  python3 bybit_prop_bot_v7.py           # paper mode (default)
  python3 bybit_prop_bot_v7.py --live    # live trading

Files
  bybit_v7_state.json   atomic state snapshot
  bybit_v7_audit.csv    one row per closed trade
  bybit_v7_events.log   all log lines
  EMERGENCY_STOP        touch this file to halt + close all positions
"""

import asyncio
import csv
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


# ═══════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════

PAPER_MODE  = "--live" not in sys.argv
API_KEY     = os.getenv("BYBIT_API_KEY",    "")
API_SECRET  = os.getenv("BYBIT_API_SECRET", "")
TESTNET     = os.getenv("BYBIT_TESTNET", "false").lower() == "true"

SYMBOLS = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "BNB/USDT:USDT",
    "DOGE/USDT:USDT",
    "AVAX/USDT:USDT",
]
REGIME_ANCHOR = "BTC/USDT:USDT"   # global market bias driven by BTC 15m regime

# ── Virtual paper equity (overridden from live account on --live boot) ─
PAPER_EQUITY = 10_000.0

# ── HyroTrader prop limits ─────────────────────────────────────
PROP_DAILY_HALT    = 0.040   # halt at -4.0% daily   (HyroTrader limit: -5.0%)
PROP_TOTAL_HALT    = 0.080   # halt at -8.0% total   (HyroTrader limit: -10.0%)
PROP_ENTRY_CUTOFF  = 0.048   # block new entries once +4.8% (protect 5% target)
PROP_PROFIT_LOCK   = 0.040   # reduce sizing 50% at +4.0% profit
MAX_RISK_PCT       = 0.030   # 3% of starting equity max loss per trade
MIN_SIZE_PCT       = 0.050   # HyroTrader: min 5% of starting equity per trade

# ── Capital allocation ─────────────────────────────────────────
DRY_POWDER    = 0.15    # keep 15% cash reserve at all times
MAX_POSITIONS = 3       # max concurrent open positions
SIZE_HIGH     = 0.25    # 25% of deployable for high-conviction
SIZE_LOW      = 0.15    # 15% of deployable for standard

# ── Regime detection (15m timeframe) ──────────────────────────
EMA_FAST          = 21
EMA_SLOW          = 55
MOMENTUM_WINDOW_H = 2.0   # regime younger than this → MOMENTUM_MODE

# ── Indicators ────────────────────────────────────────────────
MACD_F, MACD_S, MACD_SIG = 12, 26, 9
RSI_LEN     = 14
BB_LEN      = 20
BB_STD      = 2.0
ADX_LEN     = 14
VOL_RATIO_MIN = 1.4    # current volume must be >= 1.4× 20-bar average

# ── Entry gate ────────────────────────────────────────────────
MIN_CONVICTION = 62

# ── Phase 1: hard stop (active immediately after entry) ───────
HARD_STOP_PCT = 0.015   # 1.5% below entry price; this IS the Bybit SL

# ── Phase 2: ATR trailing stop (arms at GREEN_TRIGGER_PCT gain) ─
GREEN_TRIGGER_PCT     = 0.005   # +0.5% gain → switch to ATR trail
BREAKEVEN_BUFFER      = 0.001   # floor = entry × (1 + 0.1%)

# Trailing ATR multiplier tiers  (gain_pct_threshold, atr_mult)
ATR_TRAIL_TIERS = [
    (0.000, 1.8),   # 0.0–0.5%  → 1.8× ATR
    (0.005, 1.4),   # 0.5–1.0%  → 1.4× ATR
    (0.010, 1.0),   # 1.0–2.0%  → 1.0× ATR
    (0.020, 0.6),   # 2.0–3.0%  → 0.6× ATR
    (0.030, 0.3),   # 3.0%+     → 0.3× ATR  (lock profits)
]
ATR_MOMENTUM_BASE   = 2.1   # MOMENTUM_MODE entries get slightly wider base
LIGHT_GREEN_MULT    = 0.3   # collapse trail to 0.3×ATR on LIGHT_GREEN signal

# ── Exit rules ────────────────────────────────────────────────
MAX_HOLD_HOURS   = 12     # force-close after 12 h
FAIL_BARS_5M     = 6      # never-green after 6×5m bars → early cut
FAIL_PAIN_PCT    = -0.008 # only cut if loss also >= 0.8%
COOLDOWN_HOURS   = 2      # per-symbol cooldown after any exit

# ── Infrastructure ────────────────────────────────────────────
HEARTBEAT_SEC    = 30
CANDLE_LIMIT     = 200

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATE_FILE = os.path.join(BASE_DIR, "bybit_v7_state.json")
AUDIT_FILE = os.path.join(BASE_DIR, "bybit_v7_audit.csv")
LOG_FILE   = os.path.join(BASE_DIR, "bybit_v7_events.log")
EMERGENCY  = os.path.join(BASE_DIR, "EMERGENCY_STOP")


# ═══════════════════════════════════════════════════════════════
# LOGGING
# ═══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("BybitV7")


# ═══════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════

@dataclass
class SymbolState:
    candles_1m:      deque = field(default_factory=lambda: deque(maxlen=60))
    candles_5m:      deque = field(default_factory=lambda: deque(maxlen=200))
    candles_15m:     deque = field(default_factory=lambda: deque(maxlen=200))
    regime:          str   = "NEUTRAL"
    regime_flip_ts:  float = 0.0
    prev_regime:     str   = "NEUTRAL"
    macd_state_5m:   str   = "NONE"    # NONE / PINK / PINK_1 / GREEN_CROSS / LIGHT_GREEN
    atr_5m:          float = 0.0
    atr_15m:         float = 0.0
    cooldown_until:  float = 0.0
    last_price:      float = 0.0


@dataclass
class Position:
    symbol:       str
    side:         str      # "long"
    entry_price:  float
    size_usd:     float
    qty:          float
    entry_ts:     float    # unix timestamp
    entry_mode:   str      # "MOMENTUM" / "PULLBACK"
    hard_stop:    float = 0.0
    trail_stop:   float = 0.0
    bybit_sl:     float = 0.0   # last SL price confirmed to Bybit
    phase:        int   = 1     # 1 = hard stop, 2 = ATR trail
    peak_price:   float = 0.0
    ever_green:   bool  = False
    bars_5m_held: int   = 0
    light_green:  bool  = False  # True once LIGHT_GREEN collapse triggered


# ═══════════════════════════════════════════════════════════════
# GLOBAL STATE
# ═══════════════════════════════════════════════════════════════

sym_state: Dict[str, SymbolState] = {s: SymbolState() for s in SYMBOLS}
positions: Dict[str, Position]    = {}
_lock = asyncio.Lock()

_starting_eq:       float = PAPER_EQUITY
_equity:            float = PAPER_EQUITY
_peak_equity:       float = PAPER_EQUITY
_day_start_eq:      float = PAPER_EQUITY
_current_day:       str   = ""
_halted:            bool  = False
_halt_reason:       str   = ""
_sizing_mod:        float = 1.0
_total_closed_pnl:  float = 0.0
_trading_days:      set   = set()
_exchange: Optional[ccxtpro.bybit] = None


# ═══════════════════════════════════════════════════════════════
# PURE HELPERS
# ═══════════════════════════════════════════════════════════════

def bybit_sym(ccxt_sym: str) -> str:
    """BTC/USDT:USDT  →  BTCUSDT"""
    base  = ccxt_sym.split("/")[0]
    quote = ccxt_sym.split("/")[1].split(":")[0]
    return base + quote


def sf(v, d: float = 0.0) -> float:
    try:
        f = float(v)
        return d if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return d


def now_ts() -> float:
    return time.time()


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def atr_mult_for_gain(gain_pct: float) -> float:
    for thr, mult in reversed(ATR_TRAIL_TIERS):
        if gain_pct >= thr:
            return mult
    return ATR_TRAIL_TIERS[0][1]


def candles_to_df(buf: deque) -> Optional[pd.DataFrame]:
    if len(buf) < 35:
        return None
    df = pd.DataFrame(list(buf), columns=["ts", "open", "high", "low", "close", "volume"])
    for col in ("open", "high", "low", "close", "volume"):
        df[col] = df[col].astype(float)
    return df.reset_index(drop=True)


# ═══════════════════════════════════════════════════════════════
# RISK MANAGEMENT
# ═══════════════════════════════════════════════════════════════

def _total_pnl_pct() -> float:
    return (_equity - _starting_eq) / _starting_eq


def _daily_pnl_pct() -> float:
    return (_equity - _day_start_eq) / _day_start_eq


def can_trade() -> Tuple[bool, str]:
    if os.path.exists(EMERGENCY):
        return False, "EMERGENCY_STOP file"
    if _halted:
        return False, _halt_reason
    if _daily_pnl_pct() <= -PROP_DAILY_HALT:
        return False, f"daily loss {_daily_pnl_pct():.2%} hit limit"
    if _total_pnl_pct() <= -PROP_TOTAL_HALT:
        return False, f"total loss {_total_pnl_pct():.2%} hit limit"
    if _total_pnl_pct() >= PROP_ENTRY_CUTOFF:
        return False, f"at {_total_pnl_pct():.2%} — protecting profit target"
    return True, "ok"


def update_risk(eq: float) -> None:
    global _equity, _peak_equity, _day_start_eq, _current_day
    global _halted, _halt_reason, _sizing_mod

    today = utc_day()
    if today != _current_day:
        log.info(f"Day roll {_current_day}→{today} | daily PnL was {_daily_pnl_pct():.2%}")
        _current_day  = today
        _day_start_eq = _equity
        if _halted and "DAILY" in _halt_reason:
            _halted, _halt_reason = False, ""
            log.info("Daily halt CLEARED — new trading day")

    _equity = eq
    if eq > _peak_equity:
        _peak_equity = eq

    if _daily_pnl_pct() <= -PROP_DAILY_HALT and not _halted:
        _halted, _halt_reason = True, f"DAILY_HALT {_daily_pnl_pct():.2%}"
        log.critical(f"TRADING HALTED — {_halt_reason}")

    if _total_pnl_pct() <= -PROP_TOTAL_HALT and not _halted:
        _halted, _halt_reason = True, f"TOTAL_HALT {_total_pnl_pct():.2%}"
        log.critical(f"TRADING HALTED — {_halt_reason}")

    pnl = _total_pnl_pct()
    new_mod = 0.5 if pnl >= PROP_PROFIT_LOCK else (0.65 if pnl >= PROP_PROFIT_LOCK * 0.75 else 1.0)
    if new_mod != _sizing_mod:
        log.info(f"Sizing modifier {_sizing_mod:.2f}→{new_mod:.2f} (PnL={pnl:.2%})")
        _sizing_mod = new_mod


# ═══════════════════════════════════════════════════════════════
# INDICATOR HELPERS
# ═══════════════════════════════════════════════════════════════

def compute_regime(df15: pd.DataFrame) -> Tuple[str, float]:
    """Returns (regime, atr_15m) from the penultimate confirmed 15m candle."""
    df = df15.copy()
    df["ef"] = ta.ema(df["close"], length=EMA_FAST)
    df["es"] = ta.ema(df["close"], length=EMA_SLOW)
    atr_s    = ta.atr(df["high"], df["low"], df["close"], length=14)

    r   = df.iloc[-2]
    ef  = sf(r.get("ef", 0))
    es  = sf(r.get("es", 0))
    cl  = sf(r["close"])
    atr = sf(atr_s.iloc[-2]) if atr_s is not None and len(atr_s) >= 2 else 0.0

    if ef > es and cl > ef:
        regime = "BULL"
    elif ef < es:
        regime = "BEAR"
    else:
        regime = "NEUTRAL"
    return regime, atr


def compute_5m_signals(df5: pd.DataFrame) -> Tuple[str, dict, float]:
    """
    Returns (macd_state, indicators_dict, atr_5m).

    macd_state values
      PINK        2+ consecutive negative bars shrinking toward 0 (bearish exhaust → long signal)
      PINK_1      only 1 shrinking negative bar (blocked — not strong enough)
      GREEN_CROSS histogram just crossed from negative to positive (momentum flip)
      LIGHT_GREEN 2+ consecutive positive bars shrinking (bullish exhaust → tighten trail)
      NONE        no pattern
    """
    df = df5.copy()

    macd_df = ta.macd(df["close"], fast=MACD_F, slow=MACD_S, signal=MACD_SIG)
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
        df["bb_upper"] = bb[f"BBU_{BB_LEN}_{float(BB_STD)}"]

    atr_s = ta.atr(df["high"], df["low"], df["close"], length=14)
    atr5  = sf(atr_s.iloc[-2]) if atr_s is not None and len(atr_s) >= 2 else 0.0

    # Penultimate candle (avoid look-ahead)
    r  = df.iloc[-2]
    r1 = df.iloc[-3]
    r2 = df.iloc[-4]

    h0 = sf(r.get("hist",  0))
    h1 = sf(r1.get("hist", 0))
    h2 = sf(r2.get("hist", 0))

    # MACD histogram state detection
    if h0 < 0 and h1 < 0 and h2 < 0 and h0 > h1 and h1 > h2:
        # 2 consecutive negative bars shrinking → confirmed PINK
        macd_state = "PINK"
    elif h0 < 0 and h1 < 0 and h0 > h1:
        # Only 1 shrinking bar — PINK_1, not used for entry
        macd_state = "PINK_1"
    elif h0 > 0 and h1 <= 0:
        # Fresh cross from negative to positive
        macd_state = "GREEN_CROSS"
    elif h0 > 0 and h1 > 0 and h0 < h1:
        # Positive bars shrinking — momentum fading
        macd_state = "LIGHT_GREEN"
    else:
        macd_state = "NONE"

    vol_avg = df["volume"].rolling(20).mean().iloc[-2]
    vol_r   = sf(r["volume"]) / max(sf(vol_avg), 1e-10)

    ind = {
        "close":     sf(r["close"]),
        "ef":        sf(r.get("ef", 0)),
        "es":        sf(r.get("es", 0)),
        "rsi":       sf(r.get("rsi", 50)),
        "bb_lower":  sf(r.get("bb_lower", 0)),
        "bb_mid":    sf(r.get("bb_mid",   0)),
        "bb_upper":  sf(r.get("bb_upper", 0)),
        "vol_ratio": vol_r,
        "h0": h0, "h1": h1, "h2": h2,
    }
    return macd_state, ind, atr5


# ═══════════════════════════════════════════════════════════════
# ENTRY EVALUATION
# ═══════════════════════════════════════════════════════════════

def evaluate_entry(sym: str) -> Optional[dict]:
    """
    Returns entry dict or None.
    Called from task_5m on each confirmed 5m candle close.
    """
    ss = sym_state[sym]

    # Risk gates
    ok, reason = can_trade()
    if not ok:
        return None

    # Cooldown
    if now_ts() < ss.cooldown_until:
        return None

    # Position limits
    if sym in positions or len(positions) >= MAX_POSITIONS:
        return None

    # Need 5m indicators
    df5 = candles_to_df(ss.candles_5m)
    if df5 is None:
        return None

    macd_state, ind, atr5 = compute_5m_signals(df5)
    ss.macd_state_5m = macd_state
    ss.atr_5m        = atr5

    # Volume filter
    if ind["vol_ratio"] < VOL_RATIO_MIN:
        return None

    # Global bias: BTC anchor must not be in BEAR
    if sym != REGIME_ANCHOR and sym_state[REGIME_ANCHOR].regime == "BEAR":
        return None

    sym_regime = ss.regime
    if sym_regime == "BEAR":
        return None

    regime_age_h = (
        (now_ts() - ss.regime_flip_ts) / 3600.0
        if ss.regime_flip_ts > 0 else 99.0
    )

    close = ind["close"]
    if close <= 0:
        return None

    # ── MOMENTUM_MODE: fresh BULL flip, MACD GREEN_CROSS ─────
    if sym_regime == "BULL" and regime_age_h < MOMENTUM_WINDOW_H:
        if macd_state != "GREEN_CROSS":
            return None
        if ind["rsi"] >= 65:
            return None
        mode       = "MOMENTUM"
        size_pct   = SIZE_HIGH
        conviction = 80

    # ── PULLBACK_MODE: established BULL, PINK(2+) + price cond ─
    elif sym_regime == "BULL" and regime_age_h >= MOMENTUM_WINDOW_H:
        if macd_state != "PINK":
            return None
        if ind["rsi"] >= 55:
            return None

        ef     = ind["ef"]
        es     = ind["es"]
        bb_lo  = ind["bb_lower"]
        rsi    = ind["rsi"]

        at_bb_lower    = bb_lo > 0 and close <= bb_lo * 1.005
        ema21_pullback = ef > 0 and es > 0 and close >= ef and (close - ef) / ef < 0.0075
        rsi_dip        = rsi < 45 and es > 0 and close > es

        if not (at_bb_lower or ema21_pullback or rsi_dip):
            return None

        conviction = 40
        if at_bb_lower:              conviction += 12
        if ema21_pullback:           conviction += 8
        if rsi_dip:                  conviction += 7
        if rsi < 35:                 conviction += 5
        if ind["vol_ratio"] >= 2.0:  conviction += 5

        if conviction < MIN_CONVICTION:
            return None

        mode     = "PULLBACK"
        size_pct = SIZE_HIGH if conviction >= 75 else SIZE_LOW

    # ── NEUTRAL regime: PINK + BB lower only ─────────────────
    elif sym_regime == "NEUTRAL":
        if macd_state != "PINK":
            return None
        bb_lo = ind["bb_lower"]
        if not (bb_lo > 0 and close <= bb_lo * 1.003):
            return None
        if ind["rsi"] >= 48:
            return None
        mode       = "PULLBACK"
        size_pct   = SIZE_LOW
        conviction = 62

    else:
        return None

    # ── Size calculation ──────────────────────────────────────
    deployable = _equity * (1.0 - DRY_POWDER)
    raw_size   = deployable * size_pct * _sizing_mod
    min_size   = _starting_eq * MIN_SIZE_PCT          # HyroTrader minimum
    max_size   = (_starting_eq * MAX_RISK_PCT) / HARD_STOP_PCT  # risk cap

    size_usd = max(raw_size, min_size)
    size_usd = min(size_usd, max_size, deployable * 0.85)

    if size_usd < min_size:
        return None

    qty        = size_usd / close
    stop_price = close * (1.0 - HARD_STOP_PCT)

    return {
        "entry_price": close,
        "size_usd":    size_usd,
        "qty":         qty,
        "stop_price":  stop_price,
        "mode":        mode,
        "conviction":  conviction,
    }


# ═══════════════════════════════════════════════════════════════
# EXCHANGE OPERATIONS
# ═══════════════════════════════════════════════════════════════

async def get_exchange() -> ccxtpro.bybit:
    global _exchange
    if _exchange is None:
        _exchange = ccxtpro.bybit({
            "apiKey":          API_KEY,
            "secret":          API_SECRET,
            "options":         {"defaultType": "linear"},
            "sandbox":         TESTNET,
            "enableRateLimit": True,
        })
        await _exchange.load_markets()
    return _exchange


async def fetch_equity() -> float:
    exch = await get_exchange()
    try:
        resp  = await exch.private_get_v5_account_wallet_balance({"accountType": "UNIFIED"})
        items = resp.get("result", {}).get("list", [])
        if items:
            return sf(items[0].get("totalEquity", _equity))
    except Exception as e:
        log.warning(f"fetch_equity error: {e}")
    return _equity


async def set_bybit_sl(sym: str, sl_price: float) -> bool:
    """
    Place / update native Bybit position stop loss.
    POST /v5/position/trading-stop  — NOT a conditional order.
    HyroTrader requires this within 5 min of entry.
    """
    if PAPER_MODE:
        return True

    exch   = await get_exchange()
    rounded = exch.price_to_precision(sym, sl_price)
    params  = {
        "category":    "linear",
        "symbol":      bybit_sym(sym),
        "stopLoss":    str(rounded),
        "slTriggerBy": "LastPrice",
        "tpslMode":    "Full",
    }
    try:
        await exch.private_post_v5_position_trading_stop(params)
        log.info(f"{sym} | Bybit SL → {rounded}")
        return True
    except Exception as e:
        log.error(f"{sym} | set_bybit_sl failed: {e}")
        return False


async def set_leverage_1x(sym: str) -> None:
    if PAPER_MODE:
        return
    exch = await get_exchange()
    try:
        await exch.set_leverage(
            1, sym,
            params={"category": "linear", "buyLeverage": "1", "sellLeverage": "1"},
        )
        log.info(f"{sym} | leverage set 1×")
    except Exception as e:
        log.debug(f"{sym} | set_leverage (may already be 1×): {e}")


async def place_market_entry(sym: str, qty: float) -> Optional[float]:
    """Place market buy. Returns fill price or None on failure."""
    if PAPER_MODE:
        return sym_state[sym].last_price
    exch = await get_exchange()
    try:
        order = await exch.create_order(
            sym, "market", "buy", qty, params={"category": "linear"},
        )
        return sf(order.get("average") or order.get("price"), 0.0) or None
    except Exception as e:
        log.error(f"{sym} | place_market_entry failed: {e}")
        return None


async def place_market_exit(sym: str, qty: float) -> bool:
    if PAPER_MODE:
        return True
    exch = await get_exchange()
    try:
        await exch.create_order(
            sym, "market", "sell", qty,
            params={"category": "linear", "reduceOnly": True},
        )
        return True
    except Exception as e:
        log.error(f"{sym} | place_market_exit failed: {e}")
        return False


# ═══════════════════════════════════════════════════════════════
# POSITION LIFECYCLE
# ═══════════════════════════════════════════════════════════════

async def enter_position(sym: str, entry: dict) -> None:
    global _equity

    async with _lock:
        if sym in positions or len(positions) >= MAX_POSITIONS:
            return

        fill = await place_market_entry(sym, entry["qty"])
        if fill is None:
            log.error(f"{sym} | entry order failed")
            return

        stop = fill * (1.0 - HARD_STOP_PCT)
        pos  = Position(
            symbol      = sym,
            side        = "long",
            entry_price = fill,
            size_usd    = entry["size_usd"],
            qty         = entry["qty"],
            entry_ts    = now_ts(),
            entry_mode  = entry["mode"],
            hard_stop   = stop,
            trail_stop  = stop,
            bybit_sl    = stop,
            phase       = 1,
            peak_price  = fill,
        )
        positions[sym] = pos

        log.info(
            f"ENTER {sym} | {entry['mode']} | fill={fill:.4f} | "
            f"size=${entry['size_usd']:.0f} | stop={stop:.4f} | "
            f"conviction={entry['conviction']}"
        )

    # Native Bybit SL — must be set within 5 min (HyroTrader compliance)
    await set_bybit_sl(sym, stop)
    positions[sym].bybit_sl = stop


async def exit_position(sym: str, reason: str, price: float) -> None:
    global _equity, _total_closed_pnl

    async with _lock:
        if sym not in positions:
            return
        pos = positions.pop(sym)

    await place_market_exit(sym, pos.qty)

    pnl_usd = (price - pos.entry_price) * pos.qty
    pnl_pct = (price - pos.entry_price) / pos.entry_price

    _equity           += pnl_usd
    _total_closed_pnl += pnl_usd
    update_risk(_equity)

    sym_state[sym].cooldown_until = now_ts() + COOLDOWN_HOURS * 3600

    # Track qualifying trading days  (|PnL| >= 1% of trade size is auto-satisfied
    # since our hard stop at 1.5% > 1%)
    if abs(pnl_pct) >= 0.01:
        _trading_days.add(utc_day())

    hold_m = int((now_ts() - pos.entry_ts) / 60)
    log.info(
        f"EXIT  {sym} | {reason} | entry={pos.entry_price:.4f} "
        f"exit={price:.4f} | pnl={pnl_pct:+.2%} (${pnl_usd:+.2f}) | "
        f"mode={pos.entry_mode} | held={hold_m}m | "
        f"equity=${_equity:,.2f} | total={_total_pnl_pct():+.2%}"
    )

    _write_audit({
        "timestamp":      datetime.now(timezone.utc).isoformat(),
        "symbol":         sym,
        "side":           pos.side,
        "entry_mode":     pos.entry_mode,
        "exit_reason":    reason,
        "entry_price":    round(pos.entry_price, 6),
        "exit_price":     round(price, 6),
        "qty":            round(pos.qty, 6),
        "size_usd":       round(pos.size_usd, 2),
        "pnl_usd":        round(pnl_usd, 4),
        "pnl_pct":        round(pnl_pct * 100, 3),
        "hold_minutes":   hold_m,
        "phase":          pos.phase,
        "peak_price":     round(pos.peak_price, 6),
        "equity_after":   round(_equity, 2),
        "total_pnl_pct":  round(_total_pnl_pct() * 100, 3),
        "trading_days":   len(_trading_days),
    })


# ═══════════════════════════════════════════════════════════════
# PERSISTENCE
# ═══════════════════════════════════════════════════════════════

def _save_state() -> None:
    data = {
        "equity":          _equity,
        "peak_equity":     _peak_equity,
        "starting_equity": _starting_eq,
        "total_pnl_pct":   round(_total_pnl_pct() * 100, 3),
        "daily_pnl_pct":   round(_daily_pnl_pct() * 100, 3),
        "halted":          _halted,
        "halt_reason":     _halt_reason,
        "sizing_mod":      _sizing_mod,
        "trading_days":    sorted(_trading_days),
        "paper_mode":      PAPER_MODE,
        "positions": {
            sym: {
                "entry_price": pos.entry_price,
                "size_usd":    pos.size_usd,
                "qty":         pos.qty,
                "entry_ts":    pos.entry_ts,
                "entry_mode":  pos.entry_mode,
                "phase":       pos.phase,
                "hard_stop":   pos.hard_stop,
                "trail_stop":  pos.trail_stop,
                "peak_price":  pos.peak_price,
                "bybit_sl":    pos.bybit_sl,
            }
            for sym, pos in positions.items()
        },
        "regimes": {sym: ss.regime for sym, ss in sym_state.items()},
        "regime_ages_h": {
            sym: round((now_ts() - ss.regime_flip_ts) / 3600, 2)
            for sym, ss in sym_state.items()
            if ss.regime_flip_ts > 0
        },
        "saved_at": datetime.now(timezone.utc).isoformat(),
    }
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)


def _write_audit(row: dict) -> None:
    exists = os.path.exists(AUDIT_FILE)
    with open(AUDIT_FILE, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)


# ═══════════════════════════════════════════════════════════════
# WEBSOCKET TASKS
# ═══════════════════════════════════════════════════════════════

async def task_15m(sym: str) -> None:
    """Regime detection and 15m ATR.  Updates sym_state[sym].regime."""
    exch = await get_exchange()
    log.info(f"{sym} | 15m task ready")

    while True:
        try:
            candles = await exch.watch_ohlcv(sym, "15m")
            ss = sym_state[sym]

            for c in candles:
                ss.candles_15m.append(c)
                if len(c) > 4:
                    ss.last_price = sf(c[4])

            df15 = candles_to_df(ss.candles_15m)
            if df15 is None:
                continue

            new_regime, atr15 = compute_regime(df15)
            ss.atr_15m = atr15

            if new_regime != ss.regime:
                log.info(f"{sym} | regime {ss.regime}→{new_regime}")
                ss.prev_regime    = ss.regime
                ss.regime         = new_regime
                ss.regime_flip_ts = now_ts()

                # Regime flipped to BEAR while holding → force exit on next 1m tick
                if new_regime == "BEAR" and sym in positions:
                    pos = positions[sym]
                    # Collapse hard stop to current price so 1m task exits immediately
                    pos.hard_stop  = ss.last_price * 0.9999
                    pos.trail_stop = ss.last_price * 0.9999
                    log.info(f"{sym} | BEAR flip — queued BEAR_REGIME_EXIT")

        except ccxtpro.NetworkError as e:
            log.warning(f"{sym} | 15m net error: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            log.error(f"{sym} | 15m error: {e}")
            await asyncio.sleep(10)


async def task_5m(sym: str) -> None:
    """
    Signal arming + entry evaluation.
    Also handles LIGHT_GREEN trail tighten and failed-signal cut.
    """
    exch = await get_exchange()
    log.info(f"{sym} | 5m task ready")

    while True:
        try:
            candles = await exch.watch_ohlcv(sym, "5m")
            ss = sym_state[sym]

            for c in candles:
                ss.candles_5m.append(c)

            # Increment held-bar counter on open position
            if sym in positions:
                positions[sym].bars_5m_held += 1

            df5 = candles_to_df(ss.candles_5m)
            if df5 is None:
                continue

            macd_state, ind, atr5 = compute_5m_signals(df5)
            ss.macd_state_5m = macd_state
            ss.atr_5m        = atr5

            # ── LIGHT_GREEN: tighten trail on open position ──────
            if sym in positions and macd_state == "LIGHT_GREEN":
                pos = positions[sym]
                if pos.phase == 2 and not pos.light_green:
                    pos.light_green = True
                    atr = ss.atr_5m or ss.atr_15m or (pos.entry_price * 0.005)
                    new_trail = pos.peak_price - LIGHT_GREEN_MULT * atr
                    floor     = pos.entry_price * (1.0 + BREAKEVEN_BUFFER)
                    new_trail = max(new_trail, floor)
                    if new_trail > pos.trail_stop:
                        pos.trail_stop = new_trail
                        log.info(f"{sym} | LIGHT_GREEN tighten → trail={new_trail:.4f}")
                        await set_bybit_sl(sym, new_trail)
                        pos.bybit_sl = new_trail

            # ── Failed-signal cut (Phase 1 only) ─────────────────
            if sym in positions:
                pos  = positions[sym]
                last = ss.last_price
                if (pos.phase == 1 and not pos.ever_green
                        and pos.bars_5m_held >= FAIL_BARS_5M
                        and last > 0):
                    gain = (last - pos.entry_price) / pos.entry_price
                    if gain <= FAIL_PAIN_PCT:
                        await exit_position(sym, "FAILED_SIGNAL", last)
                        continue

            # ── Entry evaluation ─────────────────────────────────
            if sym not in positions:
                entry = evaluate_entry(sym)
                if entry is not None:
                    asyncio.ensure_future(enter_position(sym, entry))

        except ccxtpro.NetworkError as e:
            log.warning(f"{sym} | 5m net error: {e}")
            await asyncio.sleep(5)
        except Exception as e:
            log.error(f"{sym} | 5m error: {e}")
            await asyncio.sleep(10)


async def task_1m(sym: str) -> None:
    """
    Real-time position monitor.
    Phase 1: checks hard stop, triggers Phase 2 transition at +0.5%.
    Phase 2: updates ATR trailing stop, syncs Bybit SL when trail moves.
    Also enforces time stop and bear-regime exit.
    """
    exch = await get_exchange()
    log.info(f"{sym} | 1m task ready")

    while True:
        try:
            candles = await exch.watch_ohlcv(sym, "1m")
            ss = sym_state[sym]

            for c in candles:
                ss.candles_1m.append(c)

            if len(ss.candles_1m) < 2:
                continue

            # Use penultimate confirmed candle
            prev = list(ss.candles_1m)[-2]
            low   = sf(prev[3])
            high  = sf(prev[2])
            close = sf(prev[4])
            ss.last_price = close

            if sym not in positions:
                continue

            pos = positions[sym]

            # Update peak
            if high > pos.peak_price:
                pos.peak_price = high
            if close > pos.entry_price:
                pos.ever_green = True

            gain_pct  = (close - pos.entry_price) / pos.entry_price
            hold_h    = (now_ts() - pos.entry_ts) / 3600.0

            # ── PHASE 1 ───────────────────────────────────────────
            if pos.phase == 1:

                # Hard stop hit
                if low <= pos.hard_stop:
                    await exit_position(sym, "HARD_STOP", pos.hard_stop)
                    continue

                # Time stop
                if hold_h >= MAX_HOLD_HOURS:
                    await exit_position(sym, "TIME_STOP", close)
                    continue

                # Transition to Phase 2
                if gain_pct >= GREEN_TRIGGER_PCT:
                    atr  = ss.atr_5m or ss.atr_15m or (pos.entry_price * 0.008)
                    mult = (ATR_MOMENTUM_BASE
                            if pos.entry_mode == "MOMENTUM"
                            else ATR_TRAIL_TIERS[0][1])
                    trail = pos.peak_price - mult * atr
                    floor = pos.entry_price * (1.0 + BREAKEVEN_BUFFER)
                    trail = max(trail, floor)

                    pos.phase      = 2
                    pos.trail_stop = trail
                    log.info(
                        f"{sym} | Phase 1→2 | gain={gain_pct:.2%} | "
                        f"trail={trail:.4f} | atr={atr:.4f}"
                    )
                    await set_bybit_sl(sym, trail)
                    pos.bybit_sl = trail

            # ── PHASE 2 ───────────────────────────────────────────
            else:
                atr   = ss.atr_5m or ss.atr_15m or (pos.entry_price * 0.008)
                mult  = atr_mult_for_gain(gain_pct)
                trail = pos.peak_price - mult * atr
                floor = pos.entry_price * (1.0 + BREAKEVEN_BUFFER)
                trail = max(trail, floor)

                # Only ever raise the trail
                if trail > pos.trail_stop:
                    pos.trail_stop = trail

                    # Update Bybit SL when trail moves >0.05% (avoid API spam)
                    if abs(trail - pos.bybit_sl) / max(pos.bybit_sl, 1e-10) > 0.0005:
                        ok = await set_bybit_sl(sym, trail)
                        if ok:
                            pos.bybit_sl = trail

                # Trail breached
                if low <= pos.trail_stop:
                    await exit_position(sym, "TRAIL_STOP", pos.trail_stop)
                    continue

                # Time stop
                if hold_h >= MAX_HOLD_HOURS:
                    await exit_position(sym, "TIME_STOP", close)
                    continue

        except ccxtpro.NetworkError as e:
            log.warning(f"{sym} | 1m net error: {e}")
            await asyncio.sleep(3)
        except Exception as e:
            log.error(f"{sym} | 1m error: {e}")
            await asyncio.sleep(5)


# ═══════════════════════════════════════════════════════════════
# HEARTBEAT TASK
# ═══════════════════════════════════════════════════════════════

async def task_heartbeat() -> None:
    global _equity

    while True:
        await asyncio.sleep(HEARTBEAT_SEC)
        try:
            if not PAPER_MODE:
                eq = await fetch_equity()
                update_risk(eq)

            _save_state()

            n_pos = len(positions)
            pos_str = " | ".join(
                f"{sym}:{((sym_state[sym].last_price - pos.entry_price)/pos.entry_price):+.2%}"
                for sym, pos in positions.items()
            ) or "—"

            log.info(
                f"♥  eq=${_equity:,.2f} | daily={_daily_pnl_pct():+.2%} "
                f"| total={_total_pnl_pct():+.2%} "
                f"| pos={n_pos}/{MAX_POSITIONS} "
                f"| days={len(_trading_days)} "
                f"| {pos_str}"
            )

            if _halted:
                log.warning(f"HALTED: {_halt_reason}")

            # Emergency stop
            if os.path.exists(EMERGENCY):
                log.critical("EMERGENCY_STOP — closing all positions")
                for sym in list(positions.keys()):
                    ss = sym_state[sym]
                    await exit_position(sym, "EMERGENCY_STOP", ss.last_price or 0.0)
                _save_state()
                return

        except Exception as e:
            log.error(f"heartbeat error: {e}")


# ═══════════════════════════════════════════════════════════════
# BOOT SEQUENCE
# ═══════════════════════════════════════════════════════════════

async def boot() -> None:
    global _equity, _starting_eq, _peak_equity, _day_start_eq, _current_day

    log.info("=" * 60)
    log.info(
        f"bybit_prop_bot_v7 | PAPER={PAPER_MODE} | TESTNET={TESTNET} | "
        f"symbols={len(SYMBOLS)}"
    )
    log.info(f"HyroTrader limits: daily halt {PROP_DAILY_HALT:.0%} | "
             f"total halt {PROP_TOTAL_HALT:.0%} | target {PROP_ENTRY_CUTOFF:.0%}")
    log.info("=" * 60)

    await asyncio.sleep(3)   # NTP stabilise

    exch = await get_exchange()

    # Set 1× leverage on every symbol
    for sym in SYMBOLS:
        await set_leverage_1x(sym)
        await asyncio.sleep(0.4)

    # Seed candle buffers via REST
    log.info("Seeding candle history...")
    for sym in SYMBOLS:
        for tf in ("1m", "5m", "15m"):
            try:
                ohlcvs = await exch.fetch_ohlcv(sym, tf, limit=CANDLE_LIMIT)
                attr   = f"candles_{tf}"
                buf    = getattr(sym_state[sym], attr)
                for c in ohlcvs:
                    buf.append(c)
                log.info(f"  {sym} {tf}: {len(ohlcvs)} candles seeded")
                await asyncio.sleep(0.25)
            except Exception as e:
                log.warning(f"  {sym} {tf} seed failed: {e}")

        # Bootstrap regime from seeded 15m data
        df15 = candles_to_df(sym_state[sym].candles_15m)
        if df15 is not None:
            r, atr = compute_regime(df15)
            sym_state[sym].regime    = r
            sym_state[sym].atr_15m  = atr
            log.info(f"  {sym} boot regime: {r}")

    # Live: fetch real equity
    if not PAPER_MODE:
        live_eq = await fetch_equity()
        if live_eq <= 0:
            raise RuntimeError("Could not fetch live equity — aborting")
        _equity = _starting_eq = _peak_equity = _day_start_eq = live_eq
        log.info(f"Live equity: ${_equity:,.2f}")
    else:
        log.info(f"Paper equity: ${_equity:,.2f}")

    _current_day  = utc_day()
    _day_start_eq = _equity

    log.info("Boot complete")


# ═══════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════

async def main() -> None:
    await boot()

    tasks = []
    for sym in SYMBOLS:
        tasks.append(asyncio.create_task(task_15m(sym), name=f"15m|{sym}"))
        tasks.append(asyncio.create_task(task_5m(sym),  name=f"5m|{sym}"))
        tasks.append(asyncio.create_task(task_1m(sym),  name=f"1m|{sym}"))

    tasks.append(asyncio.create_task(task_heartbeat(), name="heartbeat"))

    log.info(f"Running {len(tasks)} tasks ({len(SYMBOLS)} symbols × 3 TF + heartbeat)")

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        log.info("KeyboardInterrupt — shutting down")
    finally:
        if _exchange is not None:
            await _exchange.close()
        log.info("Done")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutdown")
