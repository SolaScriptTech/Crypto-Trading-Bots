"""
prop_bot_system.py — Prop Firm Evaluation Multi-Strategy Bot
============================================================

Three coordinated strategies sharing a single risk manager and regime detector:

  Bot 1 — TREND_FOLLOW   MACD slow/fast crossovers (BULL + NEUTRAL)
  Bot 2 — MEAN_REVERSION Bollinger Band mean reversion (NEUTRAL only, low ADX)
  Bot 3 — MOMENTUM       Zero-line cross breakouts (BULL confirmed only)
  Bot 4 — BEAR_SHORT     MACD bear crossovers + BB upper rejection (BEAR only)

Shared infrastructure:
  • Regime detector (BTC/USD anchor, EMA21/55, ADX)
  • Conviction scorer (0–100, MIN_CONVICTION = 62)
  • Tiered trailing stops (gain-adaptive, ratchet-only)
  • Prop firm risk manager (DD halt, daily loss halt, profit lock)
  • Atomic state persistence (prop_state.json)
  • Full trade audit (prop_audit.csv)

Deployment:
  tmux new -s prop_bot
  python3 prop_bot_system.py

Emergency stop:
  touch EMERGENCY_STOP
"""

import asyncio
import csv
import json
import logging
import math
import os
import time
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from typing import Optional

import ccxt.pro as ccxtpro
import pandas as pd
import pandas_ta as ta

import config as C
from risk_manager import PropRiskManager

# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(C.LOG_FILE),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("PropBot")


# ─────────────────────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Position:
    symbol:        str
    strategy:      str
    side:          str          # "long" | "short"
    entry_price:   float
    size_usd:      float
    qty:           float
    entry_time_ms: int
    bars_held:     int          = 0
    peak_price:    float        = 0.0
    stop_price:    float        = 0.0
    hard_stop:     float        = 0.0
    ever_green:    bool         = False
    peak_gain_pct: float        = 0.0
    conviction:    int          = 0


@dataclass
class Regime:
    label:    str    # "BULL" | "BEAR" | "NEUTRAL"
    adx:      float
    ema_fast: float
    ema_slow: float
    price:    float


# ─────────────────────────────────────────────────────────────────────────────
# INDICATOR HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """Compute all indicators on a candle DataFrame (OHLCV)."""
    df = df.copy()

    # EMAs for regime detection
    df["ema_fast"] = ta.ema(df["close"], length=C.EMA_FAST)
    df["ema_slow"] = ta.ema(df["close"], length=C.EMA_SLOW)

    # ADX
    adx_df = ta.adx(df["high"], df["low"], df["close"], length=C.ADX_PERIOD)
    if adx_df is not None:
        df["adx"] = adx_df[f"ADX_{C.ADX_PERIOD}"]

    # RSI
    df["rsi"] = ta.rsi(df["close"], length=C.RSI_PERIOD)

    # Bollinger Bands
    bb_df = ta.bbands(df["close"], length=C.BB_PERIOD, std=C.BB_STDDEV)
    if bb_df is not None:
        df["bb_upper"]  = bb_df[f"BBU_{C.BB_PERIOD}_{C.BB_STDDEV}"]
        df["bb_mid"]    = bb_df[f"BBM_{C.BB_PERIOD}_{C.BB_STDDEV}"]
        df["bb_lower"]  = bb_df[f"BBL_{C.BB_PERIOD}_{C.BB_STDDEV}"]
        df["bb_width"]  = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]

    # MACD Slow (12,26,9)
    macd_slow = ta.macd(df["close"], fast=C.MACD_SLOW_FAST, slow=C.MACD_SLOW_SLOW,
                        signal=C.MACD_SLOW_SIGNAL)
    if macd_slow is not None:
        df["macd_slow_hist"] = macd_slow[f"MACDh_{C.MACD_SLOW_FAST}_{C.MACD_SLOW_SLOW}_{C.MACD_SLOW_SIGNAL}"]

    # MACD Fast (5,10,16)
    macd_fast = ta.macd(df["close"], fast=C.MACD_FAST_FAST, slow=C.MACD_FAST_SLOW,
                        signal=C.MACD_FAST_SIGNAL)
    if macd_fast is not None:
        df["macd_fast_hist"] = macd_fast[f"MACDh_{C.MACD_FAST_FAST}_{C.MACD_FAST_SLOW}_{C.MACD_FAST_SIGNAL}"]

    # MACD Zero-line (12,26,90)
    macd_zero = ta.macd(df["close"], fast=C.MACD_ZERO_FAST, slow=C.MACD_ZERO_SLOW,
                        signal=C.MACD_ZERO_SIGNAL)
    if macd_zero is not None:
        df["macd_zero_line"] = macd_zero[f"MACD_{C.MACD_ZERO_FAST}_{C.MACD_ZERO_SLOW}_{C.MACD_ZERO_SIGNAL}"]

    # MFI (Money Flow Index)
    df["mfi"] = ta.mfi(df["high"], df["low"], df["close"], df["volume"], length=14)

    # Volume ratio
    df["vol_avg"]   = df["volume"].rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["vol_avg"]

    return df


def detect_regime(df: pd.DataFrame) -> Regime:
    """
    Regime from the penultimate confirmed candle (iloc[-2]).
    Returns BULL / BEAR / NEUTRAL.
    """
    r = df.iloc[-2]
    price    = r["close"]
    ema_fast = r.get("ema_fast", 0)
    ema_slow = r.get("ema_slow", 0)
    adx      = r.get("adx", 0)

    if ema_fast > ema_slow and price > ema_fast:
        label = "BULL"
    elif ema_fast < ema_slow:
        label = "BEAR"
    else:
        label = "NEUTRAL"

    return Regime(label=label, adx=adx, ema_fast=ema_fast, ema_slow=ema_slow, price=price)


# ─────────────────────────────────────────────────────────────────────────────
# CONVICTION SCORER
# ─────────────────────────────────────────────────────────────────────────────

def score_long(
    base: int,
    row: pd.Series,
    regime: Regime,
    swing_proximity: bool = False,
) -> int:
    score = base
    if regime.label == "BULL":
        score += C.SCORE_REGIME_BULL_BONUS
    adx = row.get("adx", 0)
    if adx > 35:
        score += C.SCORE_ADX_STRONG_BONUS
    rsi = row.get("rsi", 50)
    if rsi < 35:
        score += C.SCORE_RSI_OVERSOLD_BONUS
    mfi = row.get("mfi", 50)
    if mfi < 30:
        score += C.SCORE_MFI_BONUS
    vol_ratio = row.get("vol_ratio", 1.0)
    if vol_ratio > 2.5:
        score += C.SCORE_VOLUME_BONUS
    if swing_proximity:
        score += C.SCORE_KEY_LEVEL_BONUS
    return min(score, 100)


def score_short(
    base: int,
    row: pd.Series,
    regime: Regime,
    swing_proximity: bool = False,
) -> int:
    """Short scorer: high RSI and low MFI are positive signals."""
    score = base
    rsi = row.get("rsi", 50)
    if rsi > 70:
        score += C.SCORE_RSI_OVERSOLD_BONUS
    mfi = row.get("mfi", 50)
    if mfi > 70:
        score += C.SCORE_MFI_BONUS
    adx = row.get("adx", 0)
    if adx > 35:
        score += C.SCORE_ADX_STRONG_BONUS
    vol_ratio = row.get("vol_ratio", 1.0)
    if vol_ratio > 2.5:
        score += C.SCORE_VOLUME_BONUS
    if swing_proximity:
        score += C.SCORE_KEY_LEVEL_BONUS
    return min(score, 100)


def near_swing_level(df: pd.DataFrame, price: float, lookback: int = 20) -> bool:
    """True if price is within 0.5% of a 20-bar swing high or low."""
    recent = df.iloc[-lookback - 2:-2]
    swing_high = recent["high"].max()
    swing_low  = recent["low"].min()
    return (
        abs(price - swing_high) / swing_high < 0.005 or
        abs(price - swing_low)  / swing_low  < 0.005
    )


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL DETECTORS (one function per strategy)
# ─────────────────────────────────────────────────────────────────────────────

def signal_trend_follow(df: pd.DataFrame, regime: Regime) -> Optional[dict]:
    """
    TREND_FOLLOW — MACD slow crossover (2-bar confirmation).
    Valid in BULL and NEUTRAL. Requires ADX >= 25 and vol_ratio >= 1.5.
    """
    if not C.ENABLE_TREND_FOLLOW:
        return None
    if regime.label == "BEAR":
        return None

    r0 = df.iloc[-2]  # confirmed penultimate bar
    r1 = df.iloc[-3]
    r2 = df.iloc[-4]

    hist0 = r0.get("macd_slow_hist", 0)
    hist1 = r1.get("macd_slow_hist", 0)
    hist2 = r2.get("macd_slow_hist", 0)

    # 2-bar confirmation: current > 0, prev > 0, bar-before <= 0
    if not (hist0 > 0 and hist1 > 0 and hist2 <= 0):
        return None

    adx        = r0.get("adx", 0)
    vol_ratio  = r0.get("vol_ratio", 0)
    rsi        = r0.get("rsi", 50)

    if adx < C.ADX_MIN_TREND:
        return None
    if vol_ratio < C.VOL_RATIO_MIN:
        return None
    if rsi > 70:
        return None  # Don't chase overbought

    swing = near_swing_level(df, r0["close"])
    conviction = score_long(C.SCORE_MACD_SLOW, r0, regime, swing)

    if conviction < C.MIN_CONVICTION:
        return None

    return {"signal": "MACD_SLOW_CROSS", "strategy": "TREND_FOLLOW",
            "side": "long", "conviction": conviction}


def signal_macd_fast(df: pd.DataFrame, regime: Regime) -> Optional[dict]:
    """
    TREND_FOLLOW variant — MACD fast crossover (5,10,16).
    Same rules but slightly lower base score.
    """
    if not C.ENABLE_TREND_FOLLOW:
        return None
    if regime.label == "BEAR":
        return None

    r0 = df.iloc[-2]
    r1 = df.iloc[-3]
    r2 = df.iloc[-4]

    hist0 = r0.get("macd_fast_hist", 0)
    hist1 = r1.get("macd_fast_hist", 0)
    hist2 = r2.get("macd_fast_hist", 0)

    if not (hist0 > 0 and hist1 > 0 and hist2 <= 0):
        return None
    if abs(hist0) < C.MACD_FAST_THRESHOLD:
        return None

    adx       = r0.get("adx", 0)
    vol_ratio = r0.get("vol_ratio", 0)
    rsi       = r0.get("rsi", 50)

    if adx < C.ADX_MIN_TREND:
        return None
    if vol_ratio < C.VOL_RATIO_MIN:
        return None
    if rsi > 72:
        return None

    swing = near_swing_level(df, r0["close"])
    conviction = score_long(C.SCORE_MACD_FAST, r0, regime, swing)

    if conviction < C.MIN_CONVICTION:
        return None

    return {"signal": "MACD_FAST_CROSS", "strategy": "TREND_FOLLOW",
            "side": "long", "conviction": conviction}


def signal_momentum(df: pd.DataFrame, regime: Regime) -> Optional[dict]:
    """
    MOMENTUM — Zero-line cross (12,26,90 MACD line crosses zero from below).
    BULL only. Strong trend confirmation required (ADX >= 22).
    """
    if not C.ENABLE_MOMENTUM:
        return None
    if regime.label != "BULL":
        return None

    r0 = df.iloc[-2]
    r1 = df.iloc[-3]

    zl0 = r0.get("macd_zero_line", 0)
    zl1 = r1.get("macd_zero_line", 0)

    # Zero-line cross: from below to above
    if not (zl0 > 0 and zl1 <= 0):
        return None

    adx       = r0.get("adx", 0)
    vol_ratio = r0.get("vol_ratio", 0)
    rsi       = r0.get("rsi", 50)

    if adx < 22:
        return None
    if vol_ratio < C.VOL_RATIO_MIN:
        return None
    if rsi > 68:
        return None

    swing = near_swing_level(df, r0["close"])
    conviction = score_long(C.SCORE_ZERO_LINE, r0, regime, swing)

    if conviction < C.MIN_CONVICTION:
        return None

    return {"signal": "ZERO_LINE_CROSS", "strategy": "MOMENTUM",
            "side": "long", "conviction": conviction}


def signal_mean_reversion(df: pd.DataFrame, regime: Regime) -> Optional[dict]:
    """
    MEAN_REVERSION — BB lower band touch in ranging market.
    NEUTRAL only, ADX < 28, RSI < 40, vol_ratio >= 1.5.
    """
    if not C.ENABLE_MEAN_REVERSION:
        return None
    if regime.label != "NEUTRAL":
        return None
    if regime.adx >= C.ADX_MAX_RANGING:
        return None

    r0 = df.iloc[-2]

    bb_lower   = r0.get("bb_lower", 0)
    bb_width   = r0.get("bb_width", 0)
    close      = r0["close"]
    rsi        = r0.get("rsi", 50)
    vol_ratio  = r0.get("vol_ratio", 0)

    if close > bb_lower:
        return None
    if bb_width < C.BB_WIDTH_MIN:
        return None
    if rsi > C.RSI_OVERSOLD:
        return None
    if vol_ratio < C.VOL_RATIO_MIN:
        return None

    swing = near_swing_level(df, close)
    conviction = score_long(C.SCORE_BB_MEAN_REV, r0, regime, swing)

    if conviction < C.MIN_CONVICTION:
        return None

    return {"signal": "BB_LOWER_TOUCH", "strategy": "MEAN_REVERSION",
            "side": "long", "conviction": conviction}


def signal_bear_short(df: pd.DataFrame, regime: Regime) -> Optional[dict]:
    """
    BEAR_SHORT — MACD slow crosses negative (2-bar confirm) + RSI >= 40.
    BEAR regime only.
    """
    if not C.ENABLE_BEAR_SHORTS:
        return None
    if regime.label != "BEAR":
        return None

    r0 = df.iloc[-2]
    r1 = df.iloc[-3]
    r2 = df.iloc[-4]

    hist0 = r0.get("macd_slow_hist", 0)
    hist1 = r1.get("macd_slow_hist", 0)
    hist2 = r2.get("macd_slow_hist", 0)

    if not (hist0 < 0 and hist1 < 0 and hist2 >= 0):
        return None

    rsi       = r0.get("rsi", 50)
    vol_ratio = r0.get("vol_ratio", 0)

    if rsi < C.RSI_BEAR_SHORT_MIN:
        return None
    if vol_ratio < C.VOL_RATIO_MIN:
        return None

    swing = near_swing_level(df, r0["close"])
    conviction = score_short(C.SCORE_MACD_SLOW, r0, regime, swing)

    if conviction < C.MIN_CONVICTION:
        return None

    return {"signal": "MACD_BEAR_CROSS", "strategy": "BEAR_SHORT",
            "side": "short", "conviction": conviction}


def signal_bb_upper_reject(df: pd.DataFrame, regime: Regime) -> Optional[dict]:
    """
    BEAR_SHORT variant — price rejected at BB upper band.
    BEAR regime only. RSI >= 60.
    """
    if not C.ENABLE_BEAR_SHORTS:
        return None
    if regime.label != "BEAR":
        return None

    r0 = df.iloc[-2]

    bb_upper  = r0.get("bb_upper", float("inf"))
    close     = r0["close"]
    rsi       = r0.get("rsi", 50)
    vol_ratio = r0.get("vol_ratio", 0)

    if close < bb_upper:
        return None
    if rsi < 60:
        return None
    if vol_ratio < C.VOL_RATIO_MIN:
        return None

    swing = near_swing_level(df, close)
    conviction = score_short(C.SCORE_BB_MEAN_REV, r0, regime, swing)

    if conviction < C.MIN_CONVICTION:
        return None

    return {"signal": "BB_UPPER_REJECT", "strategy": "BEAR_SHORT",
            "side": "short", "conviction": conviction}


ALL_SIGNAL_FUNCTIONS = [
    signal_trend_follow,
    signal_macd_fast,
    signal_momentum,
    signal_mean_reversion,
    signal_bear_short,
    signal_bb_upper_reject,
]


# ─────────────────────────────────────────────────────────────────────────────
# TRAILING STOP ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def tiered_trail_pct(gain_pct: float) -> float:
    """Return the trailing distance for a given gain level."""
    for threshold, trail in reversed(C.TRAIL_TIERS):
        if gain_pct >= threshold:
            return trail
    return C.TRAIL_TIERS[0][1]


def update_trailing_stop(pos: Position, current_price: float) -> Position:
    """
    Ratchet the trailing stop upward (longs) or downward (shorts).
    Never moves against the position.
    Applies profit floor once peak gain >= 0.3%.
    """
    if pos.side == "long":
        gain_pct = (current_price - pos.entry_price) / pos.entry_price
        if current_price > pos.peak_price:
            pos.peak_price    = current_price
            pos.peak_gain_pct = max(pos.peak_gain_pct, gain_pct)

        trail_pct  = tiered_trail_pct(pos.peak_gain_pct)
        new_stop   = pos.peak_price * (1.0 - trail_pct)

        # Profit floor: once peak >= 0.3%, stop never below entry × 1.001
        if pos.peak_gain_pct >= C.PROFIT_FLOOR_TRIGGER:
            floor = pos.entry_price * (1.0 + C.PROFIT_FLOOR_BUFFER)
            new_stop = max(new_stop, floor)

        pos.stop_price = max(pos.stop_price, new_stop)

    else:  # short
        gain_pct = (pos.entry_price - current_price) / pos.entry_price
        if current_price < pos.peak_price or pos.peak_price == 0:
            pos.peak_price    = current_price
            pos.peak_gain_pct = max(pos.peak_gain_pct, gain_pct)

        trail_pct = tiered_trail_pct(pos.peak_gain_pct)
        new_stop  = pos.peak_price * (1.0 + trail_pct)

        if pos.peak_gain_pct >= C.PROFIT_FLOOR_TRIGGER:
            floor = pos.entry_price * (1.0 - C.PROFIT_FLOOR_BUFFER)
            new_stop = min(new_stop, floor)

        pos.stop_price = min(pos.stop_price, new_stop) if pos.stop_price else new_stop

    if gain_pct > 0:
        pos.ever_green = True

    return pos


# ─────────────────────────────────────────────────────────────────────────────
# EXIT LOGIC
# ─────────────────────────────────────────────────────────────────────────────

def check_exit(
    pos: Position,
    current_price: float,
    market_regime: Regime,
    asset_regime: Regime,
    df: pd.DataFrame,
    now_ms: int,
) -> Optional[str]:
    """
    Returns an exit reason string if the position should be closed, else None.
    Priority order matches the exit ladder in the skill doc.
    """
    gain_pct   = (current_price - pos.entry_price) / pos.entry_price
    loss_pct   = -gain_pct  # positive means losing
    age_hours  = (now_ms - pos.entry_time_ms) / 3_600_000

    if pos.side == "short":
        gain_pct  = -gain_pct
        loss_pct  = -gain_pct

    # ── 1. HARD STOP ──────────────────────────────────────────────────────────
    hard_stop_pct = C.HARD_STOP_PCT if pos.side == "long" else C.SHORT_HARD_STOP_PCT
    if pos.side == "long" and current_price <= pos.entry_price * (1 - hard_stop_pct):
        return "HARD_STOP_3PCT"
    if pos.side == "short" and current_price >= pos.entry_price * (1 + hard_stop_pct):
        return "HARD_STOP_SHORT"

    # ── 2. BEAR REGIME EXIT (close all longs immediately) ─────────────────────
    if pos.side == "long" and (market_regime.label == "BEAR" or asset_regime.label == "BEAR"):
        return "BEAR_REGIME_EXIT"

    # ── 3. BULL REGIME EXIT (close all shorts) ────────────────────────────────
    if pos.side == "short" and market_regime.label == "BULL":
        return "BULL_REGIME_EXIT"

    # ── 4. FAILED SIGNAL CUT ─────────────────────────────────────────────────
    if (not pos.ever_green
            and pos.bars_held >= C.FAILED_SIGNAL_BARS
            and gain_pct <= C.FAILED_SIGNAL_PAIN):
        return "FAILED_SIGNAL_CUT"

    # ── 5. ZOMBIE KILL (48h negative) ────────────────────────────────────────
    if age_hours >= C.ZOMBIE_KILL_HOURS and gain_pct < 0:
        return "ZOMBIE_KILL_48H"

    # ── 6. STAGNATION (24h, never green) ──────────────────────────────────────
    if age_hours >= C.STAGNATION_HOURS and not pos.ever_green and gain_pct < 0:
        return "STAGNATION_24H"

    # ── 7. BB TARGET HIT (mean reversion only) ────────────────────────────────
    if pos.strategy == "MEAN_REVERSION":
        bb_mid = df.iloc[-2].get("bb_mid", 0)
        if pos.side == "long" and current_price >= bb_mid:
            return "BB_TARGET_HIT"

    # ── 8. TRAILING STOP HIT ──────────────────────────────────────────────────
    if pos.stop_price and pos.side == "long" and current_price <= pos.stop_price:
        return "TRAIL_STOP_HIT"
    if pos.stop_price and pos.side == "short" and current_price >= pos.stop_price:
        return "TRAIL_STOP_HIT"

    # ── 9. MACD FLIP (after MIN_HOLD_BARS — no 1-bar whipsaws) ───────────────
    if pos.bars_held >= C.MIN_HOLD_BARS:
        r0 = df.iloc[-2]
        hist = r0.get("macd_slow_hist", 0)
        if pos.side == "long" and hist < 0:
            return "MACD_FLIP_EXIT"
        if pos.side == "short" and hist > 0:
            return "MACD_FLIP_EXIT"

    return None


# ─────────────────────────────────────────────────────────────────────────────
# STATE PERSISTENCE
# ─────────────────────────────────────────────────────────────────────────────

def save_state(state: dict) -> None:
    """Atomic write: tmp file → os.replace. Safe through AWS reboots."""
    tmp = C.STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, C.STATE_FILE)


def load_state() -> dict:
    if not os.path.exists(C.STATE_FILE):
        return {}
    with open(C.STATE_FILE) as f:
        return json.load(f)


def append_audit(row: dict) -> None:
    exists = os.path.exists(C.AUDIT_FILE)
    with open(C.AUDIT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


# ─────────────────────────────────────────────────────────────────────────────
# MAIN BOT CLASS
# ─────────────────────────────────────────────────────────────────────────────

class PropBotSystem:
    """
    Orchestrates all four strategy bots with a shared risk manager.
    Single asyncio event loop — no threading.
    """

    def __init__(self):
        self.exchange      = None
        self.positions: dict[str, Position] = {}   # symbol → Position
        self.candle_cache: dict[tuple, pd.DataFrame] = {}  # (symbol, tf) → df
        self.cooldowns: dict[str, int] = {}        # symbol → expiry ms
        self.risk: Optional[PropRiskManager] = None
        self.shorts_enabled = C.ENABLE_BEAR_SHORTS
        self._running       = True
        self._last_heartbeat_ms = 0

    # ── BOOT ──────────────────────────────────────────────────────────────────

    async def boot(self) -> None:
        logger.info("=" * 60)
        logger.info("PROP BOT SYSTEM — BOOT")
        logger.info("=" * 60)

        # NTP stabilization (mandatory — Kraken rejects if clock > 30s off)
        logger.info(f"Waiting {C.NTP_WAIT_SEC}s for NTP stabilization…")
        await asyncio.sleep(C.NTP_WAIT_SEC)

        # Exchange
        self.exchange = ccxtpro.kraken({"enableRateLimit": True})
        await self.exchange.load_markets()
        logger.info("Markets loaded")

        # Check shorts permission (non-ECP accounts can't short)
        await self._check_shorts_permission()

        # Initial balance → initialize risk manager
        equity = await self._fetch_equity()
        state  = load_state()
        starting_equity = state.get("starting_equity", equity)
        self.risk = PropRiskManager(starting_equity)
        self.risk.update_equity(equity)

        # Restore positions from state
        for sym, p in state.get("positions", {}).items():
            self.positions[sym] = Position(**p)
        self.cooldowns = state.get("cooldowns", {})
        logger.info(f"Restored {len(self.positions)} open positions")

        # Seed candle cache via REST (WS blocked until next candle close)
        await self._seed_candle_cache()

        logger.info(f"Starting equity: ${starting_equity:,.2f} | Current: ${equity:,.2f}")
        logger.info(f"Risk status: {self.risk.status_report()}")
        logger.info("Boot complete — entering main loop")

    # ── MAIN LOOP ─────────────────────────────────────────────────────────────

    async def run(self) -> None:
        await self.boot()

        ws_tasks = [
            asyncio.create_task(self._watch_symbol(sym, C.TIMEFRAME))
            for sym in C.TRADE_PAIRS + [C.REGIME_ANCHOR]
            if sym not in [t for t in C.TRADE_PAIRS] or sym == C.REGIME_ANCHOR
        ]
        # Unique pairs for WS
        all_pairs = list(set(C.TRADE_PAIRS + [C.REGIME_ANCHOR]))
        ws_tasks  = [
            asyncio.create_task(self._watch_symbol(sym, C.TIMEFRAME))
            for sym in all_pairs
        ]

        decision_task = asyncio.create_task(self._decision_loop())
        await asyncio.gather(decision_task, *ws_tasks, return_exceptions=True)

    async def _decision_loop(self) -> None:
        while self._running:
            try:
                if os.path.exists(C.EMERGENCY_FILE):
                    logger.critical("EMERGENCY_STOP detected — closing all positions and exiting")
                    await self._close_all("EMERGENCY_STOP")
                    self._running = False
                    break

                equity = await self._fetch_equity()
                self.risk.update_equity(equity)

                can_trade, reason = self.risk.can_trade()
                if not can_trade:
                    logger.warning(f"Trading halted: {reason}")
                    self._save_full_state(equity)
                    await asyncio.sleep(C.LOOP_INTERVAL_SEC)
                    continue

                # Fetch market regime (BTC anchor)
                market_regime = self._get_regime(C.REGIME_ANCHOR)

                # Exit check (all open positions)
                for sym in list(self.positions.keys()):
                    await self._check_and_exit(sym, market_regime)

                # Entry check (all eligible pairs)
                for sym in C.TRADE_PAIRS:
                    if sym not in self.positions:
                        await self._check_and_enter(sym, market_regime)

                # Heartbeat (every 5 minutes)
                now_ms = int(time.time() * 1000)
                if now_ms - self._last_heartbeat_ms >= 300_000:
                    self._heartbeat(equity, market_regime)
                    self._last_heartbeat_ms = now_ms

                self._save_full_state(equity)
                await asyncio.sleep(C.LOOP_INTERVAL_SEC)

            except Exception as e:
                logger.error(f"Decision loop error: {e}", exc_info=True)
                await asyncio.sleep(10)

    # ── ENTRY ─────────────────────────────────────────────────────────────────

    async def _check_and_enter(self, symbol: str, market_regime: Regime) -> None:
        # Cooldown check
        now_ms = int(time.time() * 1000)
        if self.cooldowns.get(symbol, 0) > now_ms:
            return

        # Max positions check
        if len(self.positions) >= C.MAX_POSITIONS:
            return

        df = self.candle_cache.get((symbol, C.TIMEFRAME))
        if df is None or len(df) < 60:
            return

        asset_regime = detect_regime(df)

        # Per-asset bear check: don't buy a bearish asset
        if asset_regime.label == "BEAR" and market_regime.label != "BEAR":
            return

        # Run all signal functions
        signal = None
        for fn in ALL_SIGNAL_FUNCTIONS:
            result = fn(df, market_regime if symbol == C.REGIME_ANCHOR else asset_regime)
            if result:
                signal = result
                break  # take highest-priority signal that fires

        if not signal:
            return

        # Short check
        if signal["side"] == "short" and not self.shorts_enabled:
            return

        # Size the trade
        equity    = self.risk.current_equity
        deployable = equity * (1 - C.DRY_POWDER_PCT)
        used       = sum(p.size_usd for p in self.positions.values())
        available  = deployable - used

        size_pct  = C.SIZE_HIGH_PCT if signal["conviction"] >= 65 else C.SIZE_LOW_PCT
        raw_size  = min(equity * size_pct, available)
        size_usd  = self.risk.size_trade(raw_size)

        if size_usd < 10:
            return  # too small to trade

        current_price = df.iloc[-1]["close"]
        qty           = size_usd / current_price

        # Hard stop from risk-based sizing
        hard_stop = self.risk.stop_price_from_risk(current_price, signal["side"], size_usd)
        # Also enforce strategy hard stop
        if signal["side"] == "long":
            strategy_stop = current_price * (1 - C.HARD_STOP_PCT)
            hard_stop     = max(hard_stop, strategy_stop)
        else:
            strategy_stop = current_price * (1 + C.SHORT_HARD_STOP_PCT)
            hard_stop     = min(hard_stop, strategy_stop)

        try:
            order_side = "buy" if signal["side"] == "long" else "sell"
            order = await self.exchange.create_market_order(symbol, order_side, qty)
            fill_price = order.get("average") or current_price

            pos = Position(
                symbol        = symbol,
                strategy      = signal["strategy"],
                side          = signal["side"],
                entry_price   = fill_price,
                size_usd      = size_usd,
                qty           = qty,
                entry_time_ms = now_ms,
                peak_price    = fill_price,
                stop_price    = hard_stop,
                hard_stop     = hard_stop,
                conviction    = signal["conviction"],
            )
            self.positions[symbol] = pos

            logger.info(
                f"ENTER [{signal['strategy']}] {symbol} {signal['side'].upper()} | "
                f"price=${fill_price:,.4f} | size=${size_usd:.0f} | "
                f"conviction={signal['conviction']} | signal={signal['signal']}"
            )

        except Exception as e:
            logger.error(f"Order failed for {symbol}: {e}")

    # ── EXIT ──────────────────────────────────────────────────────────────────

    async def _check_and_exit(self, symbol: str, market_regime: Regime) -> None:
        pos = self.positions.get(symbol)
        if not pos:
            return

        df = self.candle_cache.get((symbol, C.TIMEFRAME))
        if df is None:
            return

        current_price = df.iloc[-1]["close"]
        asset_regime  = detect_regime(df)
        now_ms        = int(time.time() * 1000)

        # Update trailing stop
        pos = update_trailing_stop(pos, current_price)
        pos.bars_held += 1

        exit_reason = check_exit(pos, current_price, market_regime, asset_regime, df, now_ms)

        if exit_reason:
            await self._close_position(symbol, exit_reason, current_price)

    async def _close_position(self, symbol: str, reason: str, price: float) -> None:
        pos = self.positions.pop(symbol, None)
        if not pos:
            return

        try:
            close_side = "sell" if pos.side == "long" else "buy"
            order      = await self.exchange.create_market_order(symbol, close_side, pos.qty)
            fill_price = order.get("average") or price
        except Exception as e:
            logger.error(f"Close order failed for {symbol}: {e}")
            fill_price = price

        if pos.side == "long":
            pnl = (fill_price - pos.entry_price) * pos.qty
        else:
            pnl = (pos.entry_price - fill_price) * pos.qty

        pnl_pct = pnl / pos.size_usd
        is_win  = pnl > 0

        self.risk.record_trade(pnl, is_win)

        # Cooldown
        now_ms = int(time.time() * 1000)
        self.cooldowns[symbol] = now_ms + C.COOLDOWN_MS

        logger.info(
            f"EXIT [{reason}] {symbol} | pnl=${pnl:+.2f} ({pnl_pct:+.2%}) | "
            f"bars={pos.bars_held} | fill=${fill_price:,.4f}"
        )

        append_audit({
            "timestamp":   datetime.utcnow().isoformat(),
            "symbol":      symbol,
            "strategy":    pos.strategy,
            "side":        pos.side,
            "entry_price": round(pos.entry_price, 4),
            "exit_price":  round(fill_price, 4),
            "size_usd":    round(pos.size_usd, 2),
            "pnl_usd":     round(pnl, 2),
            "pnl_pct":     round(pnl_pct * 100, 3),
            "bars_held":   pos.bars_held,
            "exit_reason": reason,
            "conviction":  pos.conviction,
            "ever_green":  pos.ever_green,
            "peak_gain":   round(pos.peak_gain_pct * 100, 3),
        })

    async def _close_all(self, reason: str) -> None:
        for symbol in list(self.positions.keys()):
            df = self.candle_cache.get((symbol, C.TIMEFRAME))
            price = df.iloc[-1]["close"] if df is not None else 0
            await self._close_position(symbol, reason, price)

    # ── WS CANDLE WATCHER ─────────────────────────────────────────────────────

    async def _watch_symbol(self, symbol: str, timeframe: str) -> None:
        while self._running:
            try:
                candles = await self.exchange.watch_ohlcv(symbol, timeframe)
                df = pd.DataFrame(
                    candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                df = compute_indicators(df)
                self.candle_cache[(symbol, timeframe)] = df
            except Exception as e:
                logger.warning(f"WS error {symbol}: {e}")
                await asyncio.sleep(5)

    async def _seed_candle_cache(self) -> None:
        """REST seed on boot — WS blocks until next candle close."""
        all_pairs = list(set(C.TRADE_PAIRS + [C.REGIME_ANCHOR]))
        logger.info(f"Seeding candle cache for {len(all_pairs)} pairs…")
        for i, symbol in enumerate(all_pairs):
            try:
                candles = await self.exchange.fetch_ohlcv(
                    symbol, C.TIMEFRAME, limit=C.CANDLE_LIMIT
                )
                df = pd.DataFrame(
                    candles, columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                df = compute_indicators(df)
                self.candle_cache[(symbol, C.TIMEFRAME)] = df
                logger.info(f"  [{i+1}/{len(all_pairs)}] {symbol} — {len(df)} candles seeded")
                await asyncio.sleep(C.REST_SEED_SPACING_SEC)
            except Exception as e:
                logger.warning(f"Seed failed for {symbol}: {e}")
        logger.info("Candle cache seeded")

    # ── HELPERS ───────────────────────────────────────────────────────────────

    def _get_regime(self, symbol: str) -> Regime:
        df = self.candle_cache.get((symbol, C.TIMEFRAME))
        if df is None or len(df) < 60:
            return Regime("NEUTRAL", 0, 0, 0, 0)
        return detect_regime(df)

    async def _fetch_equity(self) -> float:
        try:
            bal = await self.exchange.fetch_balance()
            usd = bal["free"].get("USD", 0)
            # Add USD value of open positions
            for sym, pos in self.positions.items():
                df = self.candle_cache.get((sym, C.TIMEFRAME))
                if df is not None:
                    price = df.iloc[-1]["close"]
                    usd += pos.qty * price
            return usd
        except Exception as e:
            logger.error(f"Balance fetch failed: {e}")
            return self.risk.current_equity if self.risk else 10_000.0

    async def _check_shorts_permission(self) -> None:
        try:
            await self.exchange.create_order(
                "BTC/USD", "market", "sell", 0.0001, params={"validate": True}
            )
        except Exception as e:
            if "Non-ECP" in str(e) or "Reduce only" in str(e):
                logger.warning("Non-ECP account detected — SHORTS DISABLED")
                self.shorts_enabled = False
            # Other errors during dry-run validate are fine

    def _save_full_state(self, equity: float) -> None:
        state = {
            "starting_equity": self.risk.starting_equity,
            "current_equity":  equity,
            "peak_equity":     self.risk.peak_equity,
            "positions":       {k: asdict(v) for k, v in self.positions.items()},
            "cooldowns":       self.cooldowns,
            "risk_status":     self.risk.status_report(),
            "saved_at":        datetime.utcnow().isoformat(),
        }
        save_state(state)

    def _heartbeat(self, equity: float, regime: Regime) -> None:
        rs = self.risk.status_report()
        logger.info(
            f"\n{'─'*55}\n"
            f"  HEARTBEAT | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"  Equity: ${equity:,.2f} | Total PnL: {rs['total_pnl_pct']:+.3f}%\n"
            f"  Drawdown: {rs['drawdown_pct']:.3f}% | Daily: {rs['daily_pnl_pct']:+.3f}%\n"
            f"  Market Regime: {regime.label} (ADX {regime.adx:.1f})\n"
            f"  Open positions: {len(self.positions)} | "
            f"Win rate: {rs['win_rate']:.1f}% ({rs['total_trades']} trades)\n"
            f"  Sizing modifier: {rs['sizing_modifier']:.2f} | "
            f"Target met: {rs['prop_target_met']}\n"
            f"{'─'*55}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    bot = PropBotSystem()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())
