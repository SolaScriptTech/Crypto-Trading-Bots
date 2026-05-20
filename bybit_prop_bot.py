"""
prop_bot.py — HyroTrader / Bybit Prop Evaluation Bot
==========================================================
Calibrated for the HyroTrader free-trial challenge (Bybit USDT perp futures).

HyroTrader rules enforced:
  Profit target:    5%  closed P&L    (bot stops pushing at 4.8%)
  Daily drawdown:   5%  max           (bot halts at 4.0%)
  Max total loss:   10% max           (bot halts at 8.0%)
  Stop loss:        Required within 5 min — Bybit native SL on position
  Max risk/trade:   3%  of initial balance
  Min trade size:   5%  of initial balance
  Min trading days: 5
  Qualifying trade: |PnL| >= 1% of trade size (met naturally by our exits)

Strategy:
  TREND_FOLLOW  — MACD(12,26,9) 2-bar histogram crossover, BULL/NEUTRAL
  MEAN_REVERSION — BB lower-band touch, NEUTRAL + low ADX
  BEAR_SHORT    — MACD 2-bar bear cross, BEAR regime

Exchange: Bybit Linear USDT Perpetual (1x leverage, one-way mode)
Timeframe: 1h candles
Pairs: BTC, ETH, SOL, XRP, BNB USDT perps

Deployment:
  export BYBIT_API_KEY=xxx
  export BYBIT_API_SECRET=xxx
  pip install ccxt pandas pandas_ta
  python3 bybit_prop_bot.py

Emergency stop:  touch EMERGENCY_STOP
State:           bybit_prop_state.json
Audit:           bybit_prop_audit.csv
Log:             bybit_prop_events.log
"""

import asyncio
import csv
import json
import logging
import math
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Optional

import ccxt.pro as ccxtpro
import pandas as pd
import pandas_ta as ta

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

API_KEY    = os.getenv("BYBIT_API_KEY",    "")
API_SECRET = os.getenv("BYBIT_API_SECRET", "")
TESTNET    = os.getenv("BYBIT_TESTNET", "false").lower() == "true"

# Bybit linear USDT perpetual symbols (CCXT format)
TRADE_PAIRS = [
    "BTC/USDT:USDT",
    "ETH/USDT:USDT",
    "SOL/USDT:USDT",
    "XRP/USDT:USDT",
    "BNB/USDT:USDT",
]
REGIME_ANCHOR = "BTC/USDT:USDT"
TIMEFRAME     = "1h"
CANDLE_LIMIT  = 120

# ── HyroTrader prop limits ────────────────────────────────────
PROP_DAILY_HALT       = 0.040   # halt at -4% daily   (limit -5%)
PROP_TOTAL_HALT       = 0.080   # halt at -8% total   (limit -10%)
PROP_PROFIT_TARGET    = 0.050   # 5% target — stop new entries at 4.8%
PROP_ENTRY_CUTOFF     = 0.048   # no new entries above this gain
PROP_PROFIT_LOCK      = 0.040   # reduce sizing 50% at +4% profit
PROP_MAX_RISK_PER_TRADE = 0.030 # 3% of initial balance per trade

# ── Position sizing ───────────────────────────────────────────
DRY_POWDER  = 0.15   # keep 15% cash reserve
MAX_POS     = 3      # max concurrent positions
SIZE_HIGH   = 0.25   # 25% per high-conviction trade (conviction >= 65)
SIZE_LOW    = 0.15   # 15% per standard trade
MIN_SIZE    = 0.05   # 5% minimum (HyroTrader minimum trade size rule)

# ── Indicators ────────────────────────────────────────────────
EMA_FAST = 21;  EMA_SLOW = 55
ADX_LEN  = 14;  ADX_TREND_MIN = 20;  ADX_RANGE_MAX = 28
RSI_LEN  = 14;  RSI_OVERSOLD  = 40
BB_LEN   = 20;  BB_STD  = 2.0;  BB_WIDTH_MIN = 0.003
MACD_F   = 12;  MACD_S  = 26;   MACD_SIG = 9
VOL_MIN  = 1.4   # current vol must be 1.4× 20-bar avg

# ── Exit parameters ───────────────────────────────────────────
HARD_STOP_PCT    = 0.020   # 2.0% hard stop from fill — Bybit SL placed here
SHORT_STOP_PCT   = 0.015   # 1.5% hard stop for shorts

# Tiered trailing stop (min_gain_to_activate, trail_distance_from_peak)
TRAIL_TIERS = [
    (0.000, 0.015),   # 0 – 0.5% gain → 1.5% trail
    (0.005, 0.010),   # 0.5–1.0% gain → 1.0% trail
    (0.010, 0.007),   # 1.0–2.0% gain → 0.7% trail
    (0.020, 0.004),   # 2.0–3.0% gain → 0.4% trail
    (0.030, 0.002),   # 3.0%+ gain    → 0.2% trail (lock profits)
]
BREAKEVEN_TRIGGER = 0.005   # once +0.5%, move Bybit SL to break-even
BREAKEVEN_BUFFER  = 0.001   # floor = entry × 1.001 (tiny profit guarantee)

MIN_HOLD_BARS    = 6        # minimum bars before MACD-flip exit allowed
FAIL_BARS        = 5        # cut if never green after N bars AND pain >= threshold
FAIL_PAIN        = -0.008   # -0.8% loss threshold for failed-signal cut
ZOMBIE_HOURS     = 24       # close after 24h if still in loss (no zombie trades)

MIN_CONVICTION   = 60       # entry blocked if conviction < 60
COOLDOWN_HOURS   = 2        # 2h cooldown per symbol after any exit

# ── Timing ────────────────────────────────────────────────────
DECISION_LOOP_SEC  = 30     # decision loop cadence
RECONCILE_INTERVAL = 300    # verify positions vs Bybit every 5 min
NTP_WAIT_SEC       = 10     # boot delay for NTP

# ── File paths ────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
STATE_FILE  = os.path.join(BASE_DIR, "bybit_prop_state.json")
AUDIT_FILE  = os.path.join(BASE_DIR, "bybit_prop_audit.csv")
LOG_FILE    = os.path.join(BASE_DIR, "bybit_prop_events.log")
EMERGENCY   = os.path.join(BASE_DIR, "EMERGENCY_STOP")

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("BybitProp")


# ─────────────────────────────────────────────────────────────
# DATA STRUCTURES
# ─────────────────────────────────────────────────────────────

@dataclass
class Position:
    symbol:          str
    strategy:        str
    side:            str      # "long" | "short"
    entry_price:     float
    size_usd:        float
    qty:             float
    entry_time_ms:   int
    bars_held:       int   = 0
    peak_price:      float = 0.0
    stop_price:      float = 0.0   # code-managed trailing stop
    hard_stop:       float = 0.0   # absolute hard stop (synced to Bybit SL)
    bybit_sl_price:  float = 0.0   # currently set Bybit exchange SL price
    breakeven_set:   bool  = False  # True once Bybit SL moved to break-even
    ever_green:      bool  = False
    peak_gain_pct:   float = 0.0
    conviction:      int   = 0


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def bybit_sym(ccxt_symbol: str) -> str:
    """Convert CCXT format to Bybit format: BTC/USDT:USDT → BTCUSDT"""
    base  = ccxt_symbol.split("/")[0]
    quote = ccxt_symbol.split("/")[1].split(":")[0]
    return base + quote


def sf(v, d: float = 0.0) -> float:
    """Safe float conversion."""
    try:
        f = float(v)
        return d if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return d


def utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def tiered_trail(gain_pct: float) -> float:
    """Return trail distance for a given gain level."""
    for threshold, trail in reversed(TRAIL_TIERS):
        if gain_pct >= threshold:
            return trail
    return TRAIL_TIERS[0][1]


def save_atomic(data: dict) -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)


def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        return {}
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def write_audit(row: dict) -> None:
    exists = os.path.exists(AUDIT_FILE)
    with open(AUDIT_FILE, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            writer.writeheader()
        writer.writerow(row)


# ─────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["ema_fast"] = ta.ema(df["close"], length=EMA_FAST)
    df["ema_slow"] = ta.ema(df["close"], length=EMA_SLOW)

    adx_df = ta.adx(df["high"], df["low"], df["close"], length=ADX_LEN)
    if adx_df is not None:
        df["adx"] = adx_df[f"ADX_{ADX_LEN}"]

    df["rsi"] = ta.rsi(df["close"], length=RSI_LEN)

    bb_df = ta.bbands(df["close"], length=BB_LEN, std=BB_STD)
    if bb_df is not None:
        df["bb_upper"] = bb_df[f"BBU_{BB_LEN}_{float(BB_STD)}"]
        df["bb_mid"]   = bb_df[f"BBM_{BB_LEN}_{float(BB_STD)}"]
        df["bb_lower"] = bb_df[f"BBL_{BB_LEN}_{float(BB_STD)}"]
        df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"]

    macd = ta.macd(df["close"], fast=MACD_F, slow=MACD_S, signal=MACD_SIG)
    if macd is not None:
        key = f"MACDh_{MACD_F}_{MACD_S}_{MACD_SIG}"
        df["macd_hist"] = macd[key] if key in macd.columns else 0.0

    df["vol_avg"]   = df["volume"].rolling(20).mean()
    df["vol_ratio"] = df["volume"] / df["vol_avg"].replace(0, 1)

    return df


def get_regime(df: pd.DataFrame) -> str:
    """BULL / BEAR / NEUTRAL from penultimate confirmed candle."""
    r = df.iloc[-2]
    ema_f = sf(r.get("ema_fast", 0))
    ema_s = sf(r.get("ema_slow", 0))
    price = sf(r["close"])
    if ema_f > ema_s and price > ema_f:
        return "BULL"
    elif ema_f < ema_s:
        return "BEAR"
    return "NEUTRAL"


def near_swing(df: pd.DataFrame, price: float, lookback: int = 20) -> bool:
    """True if price is within 0.5% of a recent swing high or low."""
    recent = df.iloc[-lookback - 2: -2]
    hi = recent["high"].max()
    lo = recent["low"].min()
    return (abs(price - hi) / max(hi, 1e-10) < 0.005 or
            abs(price - lo) / max(lo, 1e-10) < 0.005)


# ─────────────────────────────────────────────────────────────
# SIGNAL DETECTORS
# ─────────────────────────────────────────────────────────────

def signal_macd_cross(df: pd.DataFrame, regime: str) -> Optional[dict]:
    """
    TREND_FOLLOW — MACD(12,26,9) 2-bar histogram crossover from negative.
    Valid in BULL and NEUTRAL. Requires ADX >= 20, vol >= 1.4×, RSI < 70.
    """
    if regime == "BEAR" or len(df) < 6:
        return None

    r0 = df.iloc[-2]
    r1 = df.iloc[-3]
    r2 = df.iloc[-4]

    h0 = sf(r0.get("macd_hist", 0))
    h1 = sf(r1.get("macd_hist", 0))
    h2 = sf(r2.get("macd_hist", 0))

    # 2-bar confirm: current > 0, prev > 0, bar-before <= 0
    if not (h0 > 0 and h1 > 0 and h2 <= 0):
        return None

    adx = sf(r0.get("adx", 0))
    vol = sf(r0.get("vol_ratio", 0))
    rsi = sf(r0.get("rsi", 50))

    if adx < ADX_TREND_MIN or vol < VOL_MIN or rsi > 70:
        return None

    score = 30
    if regime == "BULL":       score += 12
    if adx > 30:               score += 8
    if rsi < 45:               score += 7
    if vol > 2.0:              score += 5
    if near_swing(df, sf(r0["close"])): score += 10

    if score < MIN_CONVICTION:
        return None

    return {"signal": "MACD_CROSS", "strategy": "TREND_FOLLOW",
            "side": "long", "conviction": min(score, 100)}


def signal_bb_lower(df: pd.DataFrame, regime: str) -> Optional[dict]:
    """
    MEAN_REVERSION — BB lower band touch in ranging market.
    NEUTRAL only, ADX < 28, RSI < 40, vol >= 1.4×.
    """
    if regime != "NEUTRAL" or len(df) < 30:
        return None

    r0    = df.iloc[-2]
    adx   = sf(r0.get("adx", 0))
    vol   = sf(r0.get("vol_ratio", 0))
    rsi   = sf(r0.get("rsi", 50))
    close = sf(r0["close"])
    bb_lo = sf(r0.get("bb_lower", 0))
    bb_w  = sf(r0.get("bb_width", 0))

    if adx >= ADX_RANGE_MAX or close > bb_lo:
        return None
    if bb_w < BB_WIDTH_MIN or rsi > RSI_OVERSOLD or vol < VOL_MIN:
        return None

    score = 25
    if rsi < 30:  score += 10
    if vol > 2.0: score += 5
    if near_swing(df, close): score += 10

    if score < MIN_CONVICTION:
        return None

    return {"signal": "BB_LOWER", "strategy": "MEAN_REVERSION",
            "side": "long", "conviction": min(score, 100)}


def signal_bear_short(df: pd.DataFrame, regime: str) -> Optional[dict]:
    """
    BEAR_SHORT — MACD 2-bar bear crossover, BEAR regime only.
    Requires RSI >= 40 (not already oversold), vol >= 1.4×.
    """
    if regime != "BEAR" or len(df) < 6:
        return None

    r0 = df.iloc[-2]
    r1 = df.iloc[-3]
    r2 = df.iloc[-4]

    h0 = sf(r0.get("macd_hist", 0))
    h1 = sf(r1.get("macd_hist", 0))
    h2 = sf(r2.get("macd_hist", 0))

    if not (h0 < 0 and h1 < 0 and h2 >= 0):
        return None

    rsi = sf(r0.get("rsi", 50))
    vol = sf(r0.get("vol_ratio", 0))

    if rsi < 40 or vol < VOL_MIN:   # don't short already-oversold
        return None

    score = 25
    if rsi > 55: score += 7
    if vol > 2.0: score += 5

    if score < MIN_CONVICTION:
        return None

    return {"signal": "MACD_BEAR", "strategy": "BEAR_SHORT",
            "side": "short", "conviction": min(score, 100)}


ALL_SIGNALS = [signal_macd_cross, signal_bb_lower, signal_bear_short]


# ─────────────────────────────────────────────────────────────
# TRAILING STOP ENGINE
# ─────────────────────────────────────────────────────────────

def update_trail(pos: Position, current_price: float) -> Position:
    """Ratchet trailing stop upward (longs) or downward (shorts). Never moves against position."""
    if pos.side == "long":
        gain_pct = (current_price - pos.entry_price) / pos.entry_price
        if current_price > pos.peak_price:
            pos.peak_price    = current_price
            pos.peak_gain_pct = max(pos.peak_gain_pct, gain_pct)
        trail    = tiered_trail(pos.peak_gain_pct)
        new_stop = pos.peak_price * (1.0 - trail)
        if pos.breakeven_set:
            floor    = pos.entry_price * (1.0 + BREAKEVEN_BUFFER)
            new_stop = max(new_stop, floor)
        pos.stop_price = max(pos.stop_price, new_stop)
    else:  # short
        gain_pct = (pos.entry_price - current_price) / pos.entry_price
        if current_price < pos.peak_price or pos.peak_price == 0.0:
            pos.peak_price    = current_price
            pos.peak_gain_pct = max(pos.peak_gain_pct, gain_pct)
        trail    = tiered_trail(pos.peak_gain_pct)
        new_stop = pos.peak_price * (1.0 + trail)
        pos.stop_price = min(pos.stop_price, new_stop) if pos.stop_price > 0 else new_stop

    if gain_pct > 0:
        pos.ever_green = True
    return pos


# ─────────────────────────────────────────────────────────────
# EXIT LOGIC
# ─────────────────────────────────────────────────────────────

def check_exit(
    pos:          Position,
    price:        float,
    mkt_regime:   str,
    asset_regime: str,
    df:           pd.DataFrame,
    now_ms:       int,
) -> Optional[str]:
    """Returns exit reason string or None. Priority order is explicit."""

    gain_pct = (
        (price - pos.entry_price) / pos.entry_price if pos.side == "long"
        else (pos.entry_price - price) / pos.entry_price
    )
    age_h = (now_ms - pos.entry_time_ms) / 3_600_000

    # 1. Hard stop (Bybit SL is also set here as safety net)
    if pos.side == "long"  and price <= pos.entry_price * (1 - HARD_STOP_PCT):
        return "HARD_STOP"
    if pos.side == "short" and price >= pos.entry_price * (1 + SHORT_STOP_PCT):
        return "HARD_STOP_SHORT"

    # 2. Regime flip
    if pos.side == "long"  and (mkt_regime == "BEAR" or asset_regime == "BEAR"):
        return "REGIME_FLIP_BEAR"
    if pos.side == "short" and mkt_regime == "BULL":
        return "REGIME_FLIP_BULL"

    # 3. Failed signal — never went green after N bars with >0.8% loss
    if not pos.ever_green and pos.bars_held >= FAIL_BARS and gain_pct <= FAIL_PAIN:
        return "FAILED_SIGNAL"

    # 4. Zombie kill — 24h and still losing
    if age_h >= ZOMBIE_HOURS and gain_pct < 0:
        return "ZOMBIE_KILL_24H"

    # 5. Mean-reversion target — price reaches BB mid
    if pos.strategy == "MEAN_REVERSION":
        bb_mid = sf(df.iloc[-2].get("bb_mid", 0))
        if pos.side == "long" and bb_mid > 0 and price >= bb_mid:
            return "BB_TARGET_HIT"

    # 6. Trailing stop
    if pos.stop_price > 0:
        if pos.side == "long"  and price <= pos.stop_price:
            return "TRAIL_STOP"
        if pos.side == "short" and price >= pos.stop_price:
            return "TRAIL_STOP"

    # 7. MACD flip (not before MIN_HOLD_BARS — prevents 1-bar whipsaws)
    if pos.bars_held >= MIN_HOLD_BARS:
        hist = sf(df.iloc[-2].get("macd_hist", 0))
        if pos.side == "long"  and hist < 0:
            return "MACD_FLIP_EXIT"
        if pos.side == "short" and hist > 0:
            return "MACD_FLIP_EXIT"

    return None


# ─────────────────────────────────────────────────────────────
# RISK MANAGER
# ─────────────────────────────────────────────────────────────

class RiskManager:
    def __init__(self, starting_equity: float):
        self.starting_equity = starting_equity
        self.peak_equity     = starting_equity
        self.current_equity  = starting_equity
        self.day_start_eq    = starting_equity
        self.current_day     = utc_day()
        self.daily_pnl_pct   = 0.0
        self.halted          = False
        self.halt_reason     = ""
        self.sizing_mod      = 1.0
        self.trades          = 0
        self.wins            = 0
        self.realized_pnl    = 0.0

    def update(self, equity: float) -> None:
        day = utc_day()
        if day != self.current_day:
            log.info(f"Day roll {self.current_day}→{day} | daily_pnl={self.daily_pnl_pct:+.2%}")
            self.current_day   = day
            self.day_start_eq  = equity
            self.daily_pnl_pct = 0.0
            # Clear daily-loss halt on new UTC day
            if self.halted and "DAILY" in self.halt_reason:
                self.halted      = False
                self.halt_reason = ""
                log.info("Daily loss halt CLEARED — new trading day")

        self.current_equity  = equity
        self.peak_equity     = max(self.peak_equity, equity)
        self.daily_pnl_pct   = (equity - self.day_start_eq) / self.day_start_eq

        # Update sizing modifier
        gain = (equity - self.starting_equity) / self.starting_equity
        new_mod = 0.5 if gain >= PROP_PROFIT_LOCK else (0.65 if gain >= PROP_PROFIT_LOCK * 0.75 else 1.0)
        if new_mod != self.sizing_mod:
            log.info(f"Sizing modifier {self.sizing_mod:.2f}→{new_mod:.2f} (gain={gain:+.2%})")
            self.sizing_mod = new_mod

        # Halt checks
        dd    = (equity - self.peak_equity) / self.peak_equity
        daily = self.daily_pnl_pct
        if dd <= -PROP_TOTAL_HALT and not self.halted:
            self.halted      = True
            self.halt_reason = f"TOTAL_DD_HALT: {dd:.2%} (limit -{PROP_TOTAL_HALT:.0%})"
            log.critical(f"TRADING HALTED — {self.halt_reason}")
        elif daily <= -PROP_DAILY_HALT and not self.halted:
            self.halted      = True
            self.halt_reason = f"DAILY_LOSS_HALT: {daily:.2%} (limit -{PROP_DAILY_HALT:.0%})"
            log.warning(f"TRADING HALTED — {self.halt_reason}")

    def can_trade(self) -> tuple[bool, str]:
        if os.path.exists(EMERGENCY):
            return False, "EMERGENCY_STOP file detected"
        if self.halted:
            return False, self.halt_reason
        gain = (self.current_equity - self.starting_equity) / self.starting_equity
        if gain >= PROP_ENTRY_CUTOFF:
            return False, f"Near profit target ({gain:.2%}) — protecting gains"
        return True, "ok"

    def trade_done(self, pnl: float) -> None:
        self.trades       += 1
        self.realized_pnl += pnl
        if pnl > 0:
            self.wins += 1

    def apply_mod(self, raw_size: float) -> float:
        return raw_size * self.sizing_mod

    def max_risk_usd(self) -> float:
        """3% of starting equity — HyroTrader hard rule."""
        return self.starting_equity * PROP_MAX_RISK_PER_TRADE

    def status(self) -> dict:
        gain  = (self.current_equity - self.starting_equity) / self.starting_equity
        dd    = (self.current_equity - self.peak_equity) / self.peak_equity
        return {
            "equity":        round(self.current_equity, 2),
            "total_gain":    f"{gain:+.3%}",
            "drawdown":      f"{dd:.3%}",
            "daily_pnl":     f"{self.daily_pnl_pct:+.3%}",
            "sizing_mod":    self.sizing_mod,
            "halted":        self.halted,
            "halt_reason":   self.halt_reason,
            "trades":        self.trades,
            "wins":          self.wins,
            "win_rate":      f"{self.wins / max(self.trades, 1):.1%}",
            "realized_pnl":  round(self.realized_pnl, 2),
            "target_met":    gain >= PROP_PROFIT_TARGET,
        }


# ─────────────────────────────────────────────────────────────
# MAIN BOT
# ─────────────────────────────────────────────────────────────

class BybitPropBot:
    def __init__(self):
        self.exchange:        Optional[ccxtpro.bybit] = None
        self.positions:       dict[str, Position]     = {}
        self.candles:         dict[str, pd.DataFrame] = {}
        self.cooldowns:       dict[str, int]          = {}   # symbol → expiry ms
        self.risk:            Optional[RiskManager]   = None
        self._running         = True
        self._last_hb_ms      = 0
        self._last_reconcile  = 0

    # ── BOOT ──────────────────────────────────────────────────

    async def boot(self) -> None:
        log.info("=" * 62)
        log.info("  BYBIT PROP BOT — BOOT")
        log.info("=" * 62)
        await asyncio.sleep(NTP_WAIT_SEC)

        exchange_cfg: dict = {
            "apiKey":          API_KEY,
            "secret":          API_SECRET,
            "enableRateLimit": True,
            "options":         {"defaultType": "linear"},
        }
        if TESTNET:
            exchange_cfg["options"]["testnet"] = True

        self.exchange = ccxtpro.bybit(exchange_cfg)
        await self.exchange.load_markets()
        log.info(f"Markets loaded")

        # Set 1× leverage on all pairs (safety — prop firm may require this)
        for sym in TRADE_PAIRS:
            try:
                await self.exchange.set_leverage(1, sym, {"category": "linear"})
                log.info(f"  Leverage 1× set: {sym}")
            except Exception as e:
                log.debug(f"  Leverage set skipped {sym}: {e}")

        # Fetch current equity
        equity = await self._fetch_equity()

        # Restore or initialise state
        state = load_state()
        starting = state.get("starting_equity", equity)
        self.risk = RiskManager(starting)
        self.risk.update(equity)
        for sym, pd_ in state.get("positions", {}).items():
            self.positions[sym] = Position(**pd_)
        self.cooldowns = state.get("cooldowns", {})

        log.info(f"Starting equity: ${starting:,.2f} | Current: ${equity:,.2f}")
        log.info(f"Restored {len(self.positions)} open positions")

        # Seed candle cache via REST
        await self._seed_candles()
        log.info(f"Risk: {self.risk.status()}")
        log.info("Boot complete — entering main loop")

    # ── MAIN LOOP ─────────────────────────────────────────────

    async def run(self) -> None:
        await self.boot()

        all_pairs = list(set(TRADE_PAIRS + [REGIME_ANCHOR]))
        ws_tasks  = [asyncio.create_task(self._watch(sym)) for sym in all_pairs]
        decision  = asyncio.create_task(self._decision_loop())
        await asyncio.gather(decision, *ws_tasks, return_exceptions=True)

    async def _decision_loop(self) -> None:
        while self._running:
            try:
                if os.path.exists(EMERGENCY):
                    log.critical("EMERGENCY_STOP — closing all positions")
                    await self._close_all("EMERGENCY_STOP")
                    self._running = False
                    break

                equity = await self._fetch_equity()
                self.risk.update(equity)

                can, reason = self.risk.can_trade()
                if not can:
                    log.warning(f"No new entries: {reason}")
                    self._persist(equity)
                    await asyncio.sleep(DECISION_LOOP_SEC)
                    # Still run exits even when new entries blocked
                    mkt_regime = self._regime(REGIME_ANCHOR)
                    for sym in list(self.positions.keys()):
                        await self._exit_check(sym, mkt_regime)
                    self._persist(equity)
                    continue

                mkt_regime = self._regime(REGIME_ANCHOR)

                # Exit checks (all open positions)
                for sym in list(self.positions.keys()):
                    await self._exit_check(sym, mkt_regime)

                # Entry checks
                for sym in TRADE_PAIRS:
                    if sym not in self.positions and len(self.positions) < MAX_POS:
                        await self._entry_check(sym, mkt_regime)

                # Heartbeat every 5 min
                now_ms = int(time.time() * 1000)
                if now_ms - self._last_hb_ms >= 300_000:
                    self._heartbeat(equity, mkt_regime)
                    self._last_hb_ms = now_ms

                # Reconcile positions vs Bybit every 5 min
                if now_ms - self._last_reconcile >= RECONCILE_INTERVAL * 1000:
                    await self._reconcile()
                    self._last_reconcile = now_ms

                self._persist(equity)
                await asyncio.sleep(DECISION_LOOP_SEC)

            except Exception as e:
                log.error(f"Decision loop error: {e}", exc_info=True)
                await asyncio.sleep(10)

    # ── ENTRY ─────────────────────────────────────────────────

    async def _entry_check(self, symbol: str, mkt_regime: str) -> None:
        now_ms = int(time.time() * 1000)
        if self.cooldowns.get(symbol, 0) > now_ms:
            return

        df = self.candles.get(symbol)
        if df is None or len(df) < 60:
            return

        asset_regime = self._regime(symbol)

        # Find best qualifying signal
        signal = None
        for fn in ALL_SIGNALS:
            regime_for_signal = mkt_regime if symbol == REGIME_ANCHOR else asset_regime
            result = fn(df, regime_for_signal)
            if result:
                signal = result
                break

        if not signal:
            return

        # Shorts only allowed when market regime is BEAR
        if signal["side"] == "short" and mkt_regime != "BEAR":
            return

        # ── Size the trade ────────────────────────────────────
        equity     = self.risk.current_equity
        deployable = equity * (1.0 - DRY_POWDER)
        used_usd   = sum(p.size_usd for p in self.positions.values())
        available  = max(0.0, deployable - used_usd)

        raw_size = equity * (SIZE_HIGH if signal["conviction"] >= 65 else SIZE_LOW)
        raw_size = min(raw_size, available)
        size_usd = self.risk.apply_mod(raw_size)

        # Enforce 5% minimum (HyroTrader rule)
        min_size_usd = self.risk.starting_equity * MIN_SIZE
        if size_usd < min_size_usd:
            return

        current_price = sf(df.iloc[-1]["close"])
        if current_price <= 0:
            return
        qty = size_usd / current_price

        # Calculate stop price
        sl_price = (
            current_price * (1.0 - HARD_STOP_PCT) if signal["side"] == "long"
            else current_price * (1.0 + SHORT_STOP_PCT)
        )

        # Verify max-risk rule: potential loss <= 3% of starting equity
        potential_loss = abs(current_price - sl_price) * qty
        max_loss_usd   = self.risk.max_risk_usd()
        if potential_loss > max_loss_usd:
            # Scale down quantity to respect the 3% risk limit
            qty      = max_loss_usd / abs(current_price - sl_price)
            size_usd = qty * current_price
            if size_usd < min_size_usd:
                log.debug(f"Risk-adjusted size below minimum for {symbol} — skip")
                return

        # ── Execute market entry ──────────────────────────────
        order_side = "buy" if signal["side"] == "long" else "sell"
        try:
            order = await self.exchange.create_market_order(
                symbol, order_side, qty,
                params={"category": "linear", "positionIdx": 0}
            )
            fill_price = sf(order.get("average") or order.get("price") or current_price)
            if fill_price <= 0:
                fill_price = current_price
        except Exception as e:
            log.error(f"Entry order failed {symbol}: {e}")
            return

        # Recalculate SL from actual fill (slippage may differ from signal price)
        sl_price = (
            fill_price * (1.0 - HARD_STOP_PCT) if signal["side"] == "long"
            else fill_price * (1.0 + SHORT_STOP_PCT)
        )

        # ── Place Bybit stop loss (COMPLIANCE REQUIREMENT) ────
        sl_ok = await self._set_bybit_sl(symbol, sl_price, signal["side"])
        sl_note = "SL_SET" if sl_ok else "SL_PENDING"
        log.info(
            f"ENTER [{signal['strategy']}] {symbol} {signal['side'].upper()} | "
            f"fill=${fill_price:,.4f} | size=${size_usd:.0f} | qty={qty:.6f} | "
            f"SL=${sl_price:,.4f} [{sl_note}] | signal={signal['signal']} | "
            f"conviction={signal['conviction']}"
        )

        self.positions[symbol] = Position(
            symbol        = symbol,
            strategy      = signal["strategy"],
            side          = signal["side"],
            entry_price   = fill_price,
            size_usd      = size_usd,
            qty           = qty,
            entry_time_ms = now_ms,
            peak_price    = fill_price,
            stop_price    = sl_price,
            hard_stop     = sl_price,
            bybit_sl_price = sl_price if sl_ok else 0.0,
            conviction    = signal["conviction"],
        )

    # ── EXIT ──────────────────────────────────────────────────

    async def _exit_check(self, symbol: str, mkt_regime: str) -> None:
        pos = self.positions.get(symbol)
        if not pos:
            return

        df = self.candles.get(symbol)
        if df is None:
            return

        current_price = sf(df.iloc[-1]["close"])
        asset_regime  = self._regime(symbol)
        now_ms        = int(time.time() * 1000)

        # Update code-managed trailing stop
        pos = update_trail(pos, current_price)
        pos.bars_held += 1

        # ── Move Bybit SL to break-even when profit threshold hit ──
        if (not pos.breakeven_set
                and pos.side == "long"
                and current_price >= pos.entry_price * (1.0 + BREAKEVEN_TRIGGER)):
            be_price = pos.entry_price * (1.0 + BREAKEVEN_BUFFER)
            if await self._set_bybit_sl(symbol, be_price, pos.side):
                pos.breakeven_set  = True
                pos.bybit_sl_price = be_price
                log.info(f"  [{symbol}] Break-even SL set @ ${be_price:,.4f}")

        # ── Retry SL placement if it failed at entry (5-min compliance window) ──
        if pos.bybit_sl_price == 0.0:
            age_min = (now_ms - pos.entry_time_ms) / 60_000
            if age_min < 4.5:
                if await self._set_bybit_sl(symbol, pos.hard_stop, pos.side):
                    pos.bybit_sl_price = pos.hard_stop
                    log.info(f"  [{symbol}] Delayed SL placed @ ${pos.hard_stop:,.4f}")
            elif age_min < 5.0:
                log.warning(f"  [{symbol}] SL still not placed after {age_min:.1f} min!")

        # Check all exit conditions
        exit_reason = check_exit(
            pos, current_price, mkt_regime, asset_regime, df, now_ms
        )

        if exit_reason:
            await self._close_position(symbol, exit_reason, current_price)
        else:
            self.positions[symbol] = pos

    async def _close_position(self, symbol: str, reason: str, approx_price: float) -> None:
        pos = self.positions.pop(symbol, None)
        if not pos:
            return

        try:
            close_side = "sell" if pos.side == "long" else "buy"
            order = await self.exchange.create_market_order(
                symbol, close_side, pos.qty,
                params={"category": "linear", "positionIdx": 0, "reduceOnly": True}
            )
            fill_price = sf(order.get("average") or order.get("price") or approx_price)
            if fill_price <= 0:
                fill_price = approx_price
        except Exception as e:
            log.error(f"Close order failed {symbol}: {e}")
            fill_price = approx_price

        # Remove Bybit SL (auto-cancelled when position closes, but tidy up)
        await self._remove_bybit_sl(symbol)

        # P&L calculation
        if pos.side == "long":
            pnl     = (fill_price - pos.entry_price) * pos.qty
            pnl_pct = (fill_price - pos.entry_price) / pos.entry_price
        else:
            pnl     = (pos.entry_price - fill_price) * pos.qty
            pnl_pct = (pos.entry_price - fill_price) / pos.entry_price

        self.risk.trade_done(pnl)

        # Cooldown — don't re-enter same symbol for 2h after exit
        now_ms = int(time.time() * 1000)
        self.cooldowns[symbol] = now_ms + COOLDOWN_HOURS * 3_600_000

        log.info(
            f"EXIT [{reason}] {symbol} {pos.side.upper()} | "
            f"pnl=${pnl:+.2f} ({pnl_pct:+.2%}) | "
            f"bars={pos.bars_held} | fill=${fill_price:,.4f} | "
            f"total_gain={self.risk.status()['total_gain']}"
        )

        # Audit row — every closed trade recorded
        write_audit({
            "timestamp":     datetime.utcnow().isoformat(),
            "symbol":        symbol,
            "strategy":      pos.strategy,
            "side":          pos.side,
            "entry_price":   round(pos.entry_price, 6),
            "exit_price":    round(fill_price, 6),
            "size_usd":      round(pos.size_usd, 2),
            "pnl_usd":       round(pnl, 2),
            "pnl_pct":       round(pnl_pct * 100, 3),
            "bars_held":     pos.bars_held,
            "exit_reason":   reason,
            "signal":        pos.strategy,
            "conviction":    pos.conviction,
            "ever_green":    pos.ever_green,
            "peak_gain_pct": round(pos.peak_gain_pct * 100, 3),
            "bybit_sl_set":  pos.bybit_sl_price > 0,
            "total_gain_pct": round(
                (self.risk.current_equity - self.risk.starting_equity)
                / self.risk.starting_equity * 100, 3
            ),
        })

    async def _close_all(self, reason: str) -> None:
        for symbol in list(self.positions.keys()):
            df    = self.candles.get(symbol)
            price = sf(df.iloc[-1]["close"]) if df is not None else 0.0
            await self._close_position(symbol, reason, price)

    # ── BYBIT STOP LOSS MANAGEMENT ────────────────────────────

    async def _set_bybit_sl(self, symbol: str, sl_price: float, side: str) -> bool:
        """
        Set/update native Bybit position stop loss via V5 trading-stop endpoint.
        This creates a Bybit SL on the position (NOT a conditional order),
        which satisfies the HyroTrader stop-loss compliance rule.

        Returns True on success, False on failure.
        """
        bsym = bybit_sym(symbol)
        # Price precision: round to reasonable number of decimals
        sl_rounded = round(sl_price, 4)

        try:
            resp = await self.exchange.private_post_v5_position_trading_stop({
                "category":    "linear",
                "symbol":      bsym,
                "stopLoss":    str(sl_rounded),
                "slTriggerBy": "LastPrice",
                "slOrderType": "Market",
                "positionIdx": 0,       # 0 = one-way mode
            })
            ret_code = resp.get("retCode", -1) if isinstance(resp, dict) else -1
            if ret_code == 0:
                log.debug(f"Bybit SL confirmed: {symbol} @ {sl_rounded}")
                return True
            else:
                log.warning(f"Bybit SL rejected for {symbol}: retCode={ret_code} | {resp.get('retMsg', '')}")
                return False
        except Exception as e:
            log.warning(f"Bybit SL exception {symbol}: {e}")
            return False

    async def _remove_bybit_sl(self, symbol: str) -> None:
        """Remove SL from position (position already closed — tidy-up only)."""
        bsym = bybit_sym(symbol)
        try:
            await self.exchange.private_post_v5_position_trading_stop({
                "category":    "linear",
                "symbol":      bsym,
                "stopLoss":    "0",     # 0 = remove stop loss
                "positionIdx": 0,
            })
        except Exception:
            pass    # position is already closed; auto-cancelled by Bybit

    # ── POSITION RECONCILIATION ───────────────────────────────

    async def _reconcile(self) -> None:
        """
        Compare code-tracked positions vs actual Bybit positions.
        If Bybit closed a position externally (e.g. SL triggered), record the exit.
        """
        try:
            bybit_positions = await self.exchange.fetch_positions(
                None, {"category": "linear"}
            )
            # Build set of symbols with non-zero size on Bybit
            bybit_open = set()
            for p in bybit_positions:
                contracts = abs(sf(p.get("contracts", 0) or p.get("contractSize", 0)))
                if contracts > 0:
                    bybit_open.add(p.get("symbol", ""))

            for symbol in list(self.positions.keys()):
                bsym = bybit_sym(symbol)
                if bsym not in bybit_open:
                    # Bybit has no position — likely SL was hit
                    df    = self.candles.get(symbol)
                    price = sf(df.iloc[-1]["close"]) if df is not None else self.positions[symbol].entry_price
                    log.info(f"[RECONCILE] Bybit closed {symbol} externally — recording BYBIT_SL_HIT")
                    await self._close_position(symbol, "BYBIT_SL_HIT", price)
        except Exception as e:
            log.debug(f"Reconcile error: {e}")

    # ── WEBSOCKET CANDLE FEED ─────────────────────────────────

    async def _watch(self, symbol: str) -> None:
        """Maintain live 1h candle cache via Bybit WebSocket."""
        while self._running:
            try:
                candles = await self.exchange.watch_ohlcv(
                    symbol, TIMEFRAME,
                    params={"category": "linear"}
                )
                df = pd.DataFrame(
                    candles,
                    columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                df = compute_indicators(df)
                self.candles[symbol] = df
            except Exception as e:
                log.warning(f"WS error {symbol}: {e}")
                await asyncio.sleep(5)

    async def _seed_candles(self) -> None:
        """Seed candle cache via REST on boot (WS only updates on new close)."""
        all_pairs = list(set(TRADE_PAIRS + [REGIME_ANCHOR]))
        log.info(f"Seeding candles for {len(all_pairs)} pairs…")
        for i, sym in enumerate(all_pairs):
            try:
                raw = await self.exchange.fetch_ohlcv(
                    sym, TIMEFRAME, limit=CANDLE_LIMIT,
                    params={"category": "linear"}
                )
                df = pd.DataFrame(
                    raw, columns=["timestamp", "open", "high", "low", "close", "volume"]
                )
                df = compute_indicators(df)
                self.candles[sym] = df
                log.info(f"  [{i+1}/{len(all_pairs)}] {sym} — {len(df)} candles")
                await asyncio.sleep(0.4)
            except Exception as e:
                log.warning(f"Seed failed {sym}: {e}")
        log.info("Candle cache ready")

    # ── HELPERS ───────────────────────────────────────────────

    def _regime(self, symbol: str) -> str:
        df = self.candles.get(symbol)
        if df is None or len(df) < 60:
            return "NEUTRAL"
        return get_regime(df)

    async def _fetch_equity(self) -> float:
        """
        Fetch Bybit unified account total equity (includes unrealized P&L).
        HyroTrader drawdown rules use equity mark-to-market, not just cash.
        """
        try:
            resp = await self.exchange.private_get_v5_account_wallet_balance(
                {"accountType": "UNIFIED"}
            )
            eq = sf(resp["result"]["list"][0]["totalEquity"])
            if eq > 0:
                return eq
        except Exception as e:
            log.debug(f"Equity fetch error: {e}")

        # Fallback: estimate from risk manager
        if self.risk:
            return self.risk.current_equity

        return 10_000.0     # boot fallback

    def _persist(self, equity: float) -> None:
        save_atomic({
            "starting_equity": self.risk.starting_equity,
            "current_equity":  equity,
            "peak_equity":     self.risk.peak_equity,
            "positions":       {k: asdict(v) for k, v in self.positions.items()},
            "cooldowns":       self.cooldowns,
            "risk":            self.risk.status(),
            "saved_at":        datetime.utcnow().isoformat(),
        })

    def _heartbeat(self, equity: float, mkt_regime: str) -> None:
        rs = self.risk.status()
        log.info(
            f"\n{'─' * 62}\n"
            f"  HEARTBEAT | {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}\n"
            f"  Equity:     ${equity:,.2f}  |  Total P&L: {rs['total_gain']}\n"
            f"  Drawdown:   {rs['drawdown']}  |  Daily:    {rs['daily_pnl']}\n"
            f"  Market:     {mkt_regime}  |  Sizing mod: {rs['sizing_mod']:.2f}\n"
            f"  Positions:  {len(self.positions)}/{MAX_POS}  |  Trades: {rs['trades']}  |  WR: {rs['win_rate']}\n"
            f"  Halted:     {rs['halted']}  |  Target met: {rs['target_met']}\n"
            f"{'─' * 62}"
        )


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

async def main():
    bot = BybitPropBot()
    try:
        await bot.run()
    finally:
        if bot.exchange:
            await bot.exchange.close()


if __name__ == "__main__":
    asyncio.run(main())
