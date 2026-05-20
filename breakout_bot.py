#!/usr/bin/env python3
"""
breakout_bot.py — Breakout/POL Prop Challenge Bot
1-Step Classic: $5,000 account | Target: 10% ($500) | Daily limit: 3% | Total DD: 6% | Funded: $100k

Strategy
--------
  Entry : MACD PINK(2+ consecutive shrinking negative bars)
          + RSI(14) < 52
          + Price at BB-lower OR EMA21 pullback (0–0.75%) OR SMA20 touch OR RSI<42 above EMA55
          + Regime ≠ BEAR
          + Symbol not in 2h cooldown
  Exit  : K_PHASE2 (proven 75% WR on live trades)
            Phase 1 — hard stop 1.5% (always active)
            Phase 2 — ATR trail arms only when gain ≥0.3% AND unrealised ≥$2
          + MACD LIGHT_GREEN → collapse trail to 0.3×ATR
          + BEAR regime flip
          + 12h time stop

Architecture: 3 daemon threads
  EXIT_LOOP    every 20 s   — evaluate all open positions
  ENTRY_SCAN   every 5 min  — score universe, enter if conditions met
  HEARTBEAT    every 5 min  — print summary, save state, daily-reset check

Usage
-----
  python breakout_bot.py           # paper mode (default)
  python breakout_bot.py --live    # live orders via Kraken API
"""

import os, sys, time, json, csv, logging, threading, argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import ccxt
import pandas as pd
import pandas_ta as ta
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Challenge constants (Classic tier)
# ─────────────────────────────────────────────────────────────────────────────
STARTING_BALANCE  = 5_000.0
DAILY_HALT_PCT    = 0.020      # bot halts at 2% daily loss — buffer before 3% limit
TOTAL_HALT_PCT    = 0.045      # bot halts at 4.5% total DD — buffer before 6% limit
SLIPPAGE_BPS      = 10         # 10 basis points each side

# ─────────────────────────────────────────────────────────────────────────────
# Risk / position constants
# ─────────────────────────────────────────────────────────────────────────────
MAX_POSITIONS      = 5
POSITION_SIZE_PCT  = 0.15      # 15% of cash per position
MIN_POSITION_USD   = 50.0
HARD_STOP_PCT      = 0.015     # K_PHASE2 Phase 1 — 1.5% hard stop
ATR_ARM_PCT        = 0.003     # K_PHASE2 Phase 2 — arm trail at 0.3% gain
ATR_ARM_ABS        = 2.0       # K_PHASE2 Phase 2 — arm trail at $2 unrealised
ATR_TRAIL_MULT     = 1.5       # base ATR trail multiplier
ATR_TIGHT_MULT     = 0.7       # tighter multiplier once gain > 3%
ATR_COLLAPSE_MULT  = 0.3       # LIGHT_GREEN collapse multiplier
ATR_GAIN_TIGHT_PCT = 0.030     # tighten trail above this gain
MAX_HOLD_HOURS     = 12
COOLDOWN_MINUTES   = 120

# ─────────────────────────────────────────────────────────────────────────────
# Universe
# ─────────────────────────────────────────────────────────────────────────────
MIN_VOLUME_USD     = 5_000_000
MAX_UNIVERSE       = 20
UNIVERSE_REFRESH_H = 1

# ─────────────────────────────────────────────────────────────────────────────
# Files
# ─────────────────────────────────────────────────────────────────────────────
STATE_FILE = "breakout_state.json"
AUDIT_FILE = "breakout_audit.csv"
EVENTS_LOG = "breakout_events.log"

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    handlers=[
        logging.FileHandler(EVENTS_LOG, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Shared mutable state + lock
# ─────────────────────────────────────────────────────────────────────────────
_lock = threading.Lock()

state: dict = {}

DEFAULT_STATE = {
    "positions": {},
    "cash": STARTING_BALANCE,
    "equity": STARTING_BALANCE,
    "peak_equity": STARTING_BALANCE,
    "day_start_equity": STARTING_BALANCE,
    "day_start_date": "",
    "cooldowns": {},
    "universe": [],
    "universe_refreshed": "",
    "halted": False,
    "halt_reason": "",
    "total_trades": 0,
    "wins": 0,
    "losses": 0,
    "gross_profit": 0.0,
    "gross_loss": 0.0,
}


# ─────────────────────────────────────────────────────────────────────────────
# State persistence
# ─────────────────────────────────────────────────────────────────────────────
def load_state() -> None:
    global state
    if Path(STATE_FILE).exists():
        with open(STATE_FILE, encoding="utf-8") as f:
            state = json.load(f)
        log.info(f"State loaded — {len(state['positions'])} positions, "
                 f"cash=${state['cash']:.2f}, equity=${state['equity']:.2f}")
    else:
        state = {k: v for k, v in DEFAULT_STATE.items()}
        save_state()
        log.info("Fresh state initialised")


def save_state() -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)


def write_audit(row: dict) -> None:
    exists = Path(AUDIT_FILE).exists()
    with open(AUDIT_FILE, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)


# ─────────────────────────────────────────────────────────────────────────────
# Exchange + rate limiter
# ─────────────────────────────────────────────────────────────────────────────
def make_exchange() -> ccxt.kraken:
    return ccxt.kraken({
        "apiKey":          os.getenv("KRAKEN_API_KEY", ""),
        "secret":          os.getenv("KRAKEN_API_SECRET", ""),
        "enableRateLimit": True,
    })


_last_api_call = 0.0
_api_lock = threading.Lock()


def api(fn, *args, retries: int = 4, **kwargs):
    """Rate-limited API wrapper with exponential backoff."""
    global _last_api_call
    with _api_lock:
        gap = time.time() - _last_api_call
        if gap < 1.5:
            time.sleep(1.5 - gap)
        _last_api_call = time.time()

    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except (ccxt.RateLimitExceeded, ccxt.NetworkError) as e:
            wait = 2 ** attempt * 2
            log.warning(f"API retry {attempt+1}/{retries} after {wait}s — {e}")
            time.sleep(wait)
        except ccxt.ExchangeError as e:
            log.error(f"Exchange error: {e}")
            raise
    raise RuntimeError("Max API retries exceeded")


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV + indicators
# ─────────────────────────────────────────────────────────────────────────────
def fetch_ohlcv(ex, symbol: str, timeframe: str = "1h", limit: int = 150) -> pd.DataFrame:
    raw = api(ex.fetch_ohlcv, symbol, timeframe, limit=limit)
    df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    macd_df = ta.macd(df["close"], fast=12, slow=26, signal=9)
    df["macd_hist"] = macd_df["MACDh_12_26_9"]
    df["rsi"]       = ta.rsi(df["close"], length=14)
    bb              = ta.bbands(df["close"], length=20, std=2.0)
    df["bb_lower"]  = bb["BBL_20_2.0"]
    df["bb_upper"]  = bb["BBU_20_2.0"]
    df["bb_mid"]    = bb["BBM_20_2.0"]
    df["ema21"]     = ta.ema(df["close"], length=21)
    df["ema55"]     = ta.ema(df["close"], length=55)
    df["ema20"]     = ta.ema(df["close"], length=20)
    df["ema50"]     = ta.ema(df["close"], length=50)
    df["sma20"]     = ta.sma(df["close"], length=20)
    df["atr"]       = ta.atr(df["high"], df["low"], df["close"], length=14)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Signal helpers
# ─────────────────────────────────────────────────────────────────────────────
def _hist(df: pd.DataFrame, offset: int) -> float:
    return float(df["macd_hist"].iloc[offset])


def macd_state(df: pd.DataFrame) -> str:
    """
    Evaluate penultimate candle (iloc[-2]) — never the last incomplete bar.

    PINK        ≥2 consecutive shrinking negative bars → entry signal
    PINK_1      exactly 1 shrinking negative bar      → blocked
    LIGHT_GREEN ≥2 consecutive shrinking positive bars → exit/collapse signal
    NEUTRAL     everything else
    """
    h2 = _hist(df, -2)   # signal candle
    h3 = _hist(df, -3)
    if len(df) < 5:
        return "NEUTRAL"
    h4 = _hist(df, -4)

    if h2 < 0 and h3 < 0 and abs(h2) < abs(h3):
        if h4 < 0 and abs(h3) < abs(h4):
            return "PINK"
        return "PINK_1"

    if h2 > 0 and h3 > 0 and abs(h2) < abs(h3):
        if h4 > 0 and abs(h3) < abs(h4):
            return "LIGHT_GREEN"

    return "NEUTRAL"


def market_regime(df: pd.DataFrame) -> str:
    row = df.iloc[-2]
    ema20 = float(row["ema20"])
    ema50 = float(row["ema50"])
    if ema20 > ema50:
        return "BULL"
    if ema20 < ema50 * 0.98:
        return "BEAR"
    return "NEUTRAL"


def entry_signal(df: pd.DataFrame) -> tuple[bool, str]:
    """Returns (qualify, reason_label). Evaluates penultimate candle."""
    row   = df.iloc[-2]
    close = float(row["close"])

    # 1. MACD PINK (2+ bars required — PINK_1 blocked)
    mstate = macd_state(df)
    if mstate != "PINK":
        return False, f"MACD={mstate}"

    # 2. RSI < 52
    rsi = float(row["rsi"])
    if rsi >= 52:
        return False, f"RSI={rsi:.1f}>=52"

    # 3. Price condition — any one qualifies
    bb_low = float(row["bb_lower"])
    ema21  = float(row["ema21"])
    sma20  = float(row["sma20"])
    ema55  = float(row["ema55"])

    at_bb      = close <= bb_low * 1.002
    ema21_pull = ema21 <= close <= ema21 * 1.0075
    sma20_tch  = sma20 * 0.999 <= close <= sma20 * 1.002
    rsi_dip    = rsi < 42 and close > ema55

    if not any([at_bb, ema21_pull, sma20_tch, rsi_dip]):
        return False, "no_price_condition"

    # 4. Regime gate
    reg = market_regime(df)
    if reg == "BEAR":
        return False, "BEAR_regime"

    tag = ("BB_LOWER"      if at_bb      else
           "EMA21_PULLBACK" if ema21_pull else
           "SMA20_TOUCH"    if sma20_tch  else "RSI_DIP")
    return True, f"PINK+RSI{rsi:.0f}+{tag}+{reg}"


# ─────────────────────────────────────────────────────────────────────────────
# Universe management
# ─────────────────────────────────────────────────────────────────────────────
def refresh_universe(ex) -> list[str]:
    try:
        tickers = api(ex.fetch_tickers)
        pairs = [
            (sym, t.get("quoteVolume") or 0)
            for sym, t in tickers.items()
            if sym.endswith("/USD") and (t.get("quoteVolume") or 0) >= MIN_VOLUME_USD
        ]
        pairs.sort(key=lambda x: -x[1])
        universe = [p[0] for p in pairs[:MAX_UNIVERSE]]
        log.info(f"Universe: {len(universe)} pairs — top: {universe[:5]}")
        return universe
    except Exception as e:
        log.error(f"Universe refresh failed: {e}")
        return state.get("universe", [])


# ─────────────────────────────────────────────────────────────────────────────
# Cooldown registry
# ─────────────────────────────────────────────────────────────────────────────
def in_cooldown(symbol: str) -> bool:
    until = state["cooldowns"].get(symbol)
    if not until:
        return False
    return datetime.now(timezone.utc) < datetime.fromisoformat(until)


def set_cooldown(symbol: str) -> None:
    until = datetime.now(timezone.utc) + timedelta(minutes=COOLDOWN_MINUTES)
    state["cooldowns"][symbol] = until.isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Risk / limit checks
# ─────────────────────────────────────────────────────────────────────────────
def check_daily_reset() -> None:
    today = datetime.now(timezone.utc).date().isoformat()
    if state["day_start_date"] != today:
        state["day_start_date"]   = today
        state["day_start_equity"] = state["equity"]
        log.info(f"Day reset — day_start_equity=${state['equity']:.2f}")


def daily_loss_ok() -> bool:
    loss = (state["day_start_equity"] - state["equity"]) / state["day_start_equity"]
    return loss < DAILY_HALT_PCT


def total_dd_ok() -> bool:
    dd = (STARTING_BALANCE - state["equity"]) / STARTING_BALANCE
    return dd < TOTAL_HALT_PCT


def halt(reason: str) -> None:
    state["halted"]      = True
    state["halt_reason"] = reason
    log.warning(f"⛔ BOT HALTED: {reason}")
    save_state()


def can_enter() -> bool:
    if state["halted"]:
        return False
    if not daily_loss_ok():
        halt(f"Daily loss ≥{DAILY_HALT_PCT*100:.0f}%")
        return False
    if not total_dd_ok():
        halt(f"Total DD ≥{TOTAL_HALT_PCT*100:.0f}%")
        return False
    if len(state["positions"]) >= MAX_POSITIONS:
        return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# Position sizing
# ─────────────────────────────────────────────────────────────────────────────
def position_size_usd() -> float:
    size = state["cash"] * POSITION_SIZE_PCT
    return max(size, MIN_POSITION_USD)


# ─────────────────────────────────────────────────────────────────────────────
# Virtual execution (paper fills)
# ─────────────────────────────────────────────────────────────────────────────
def _slippage_buy(price: float) -> float:
    return price * (1 + SLIPPAGE_BPS / 10_000)


def _slippage_sell(price: float) -> float:
    return price * (1 - SLIPPAGE_BPS / 10_000)


def execute_buy(ex, symbol: str, usd: float, live: bool) -> dict | None:
    try:
        ticker       = api(ex.fetch_ticker, symbol)
        signal_price = float(ticker["last"])
        exec_price   = _slippage_buy(signal_price)
        qty          = usd / exec_price

        if live:
            order      = api(ex.create_market_buy_order, symbol, qty)
            exec_price = float(order.get("average") or exec_price)
            qty        = float(order.get("amount") or qty)

        return {
            "symbol":       symbol,
            "qty":          qty,
            "entry_price":  exec_price,
            "signal_price": signal_price,
            "usd_cost":     qty * exec_price,
            "peak_price":   exec_price,
            "stop_price":   exec_price * (1 - HARD_STOP_PCT),
            "trail_armed":  False,
            "trail_stop":   None,
            "entry_time":   datetime.now(timezone.utc).isoformat(),
            "entry_ts":     time.time(),
        }
    except Exception as e:
        log.error(f"BUY failed {symbol}: {e}")
        return None


def execute_sell(ex, symbol: str, pos: dict, reason: str, live: bool,
                 price: float | None = None) -> float:
    try:
        if price is None:
            ticker = api(ex.fetch_ticker, symbol)
            price  = float(ticker["last"])

        signal_price = price
        exec_price   = _slippage_sell(signal_price)
        qty          = pos["qty"]

        if live:
            order      = api(ex.create_market_sell_order, symbol, qty)
            exec_price = float(order.get("average") or exec_price)

        pnl      = (exec_price - pos["entry_price"]) * qty
        pnl_pct  = (exec_price / pos["entry_price"] - 1) * 100
        hold_h   = (time.time() - pos["entry_ts"]) / 3600
        delay_ms = int(hold_h * 3_600_000)

        # Stats
        state["total_trades"] += 1
        if pnl > 0:
            state["wins"]         += 1
            state["gross_profit"] += pnl
        else:
            state["losses"]       += 1
            state["gross_loss"]   += abs(pnl)

        state["cash"]        += pos["usd_cost"] + pnl
        state["equity"]       = state["cash"]   # heartbeat marks-to-market open positions
        state["peak_equity"]  = max(state["peak_equity"], state["equity"])

        write_audit({
            "timestamp":    datetime.now(timezone.utc).isoformat(),
            "symbol":       symbol,
            "action":       "SELL",
            "reason":       reason,
            "signal_price": round(signal_price, 8),
            "exec_price":   round(exec_price, 8),
            "entry_price":  round(pos["entry_price"], 8),
            "qty":          round(qty, 8),
            "pnl":          round(pnl, 4),
            "pnl_pct":      round(pnl_pct, 4),
            "hold_hours":   round(hold_h, 2),
            "equity":       round(state["equity"], 2),
            "drawdown":     round((STARTING_BALANCE - state["equity"]) / STARTING_BALANCE, 6),
            "delay_ms":     delay_ms,
        })

        set_cooldown(symbol)
        log.info(f"SELL {symbol:<14} {reason:<20} pnl={pnl:+.2f}  "
                 f"({pnl_pct:+.2f}%)  equity=${state['equity']:.2f}")
        return pnl

    except Exception as e:
        log.error(f"SELL failed {symbol}: {e}")
        return 0.0


# ─────────────────────────────────────────────────────────────────────────────
# K_PHASE2 exit evaluation
# ─────────────────────────────────────────────────────────────────────────────
def evaluate_exit(ex, symbol: str, pos: dict,
                  df: pd.DataFrame) -> tuple[bool, str, float]:
    """
    Returns (should_exit, reason, current_price).
    Mutates pos in-place (peak_price, trail_armed, trail_stop).
    """
    try:
        ticker = api(ex.fetch_ticker, symbol)
        price  = float(ticker["last"])
    except Exception as e:
        log.warning(f"Ticker fetch failed {symbol}: {e}")
        return False, "", 0.0

    # Track peak
    if price > pos["peak_price"]:
        pos["peak_price"] = price

    gain_pct = (price / pos["entry_price"]) - 1
    gain_abs = (price - pos["entry_price"]) * pos["qty"]

    # ── Phase 1: hard stop (always active) ───────────────────────────────────
    if price <= pos["stop_price"]:
        return True, "HARD_STOP", price

    # ── Phase 2: arm ATR trail ────────────────────────────────────────────────
    atr = float(df["atr"].iloc[-2])

    if not pos["trail_armed"] and gain_pct >= ATR_ARM_PCT and gain_abs >= ATR_ARM_ABS:
        mult               = ATR_TIGHT_MULT if gain_pct >= ATR_GAIN_TIGHT_PCT else ATR_TRAIL_MULT
        pos["trail_armed"] = True
        pos["trail_stop"]  = pos["peak_price"] - mult * atr
        log.info(f"  Trail ARMED {symbol}  trail=${pos['trail_stop']:.6f}  "
                 f"gain={gain_pct*100:.2f}%")

    if pos["trail_armed"]:
        # LIGHT_GREEN → collapse trail aggressively
        mstate = macd_state(df)
        if mstate == "LIGHT_GREEN":
            mult = ATR_COLLAPSE_MULT
            log.info(f"  LIGHT_GREEN collapse {symbol}")
        elif gain_pct >= ATR_GAIN_TIGHT_PCT:
            mult = ATR_TIGHT_MULT
        else:
            mult = ATR_TRAIL_MULT

        new_trail         = pos["peak_price"] - mult * atr
        pos["trail_stop"] = max(pos["trail_stop"], new_trail)  # only ratchet up

        if price <= pos["trail_stop"]:
            return True, "TRAIL_STOP", price

    # ── BEAR regime flip ──────────────────────────────────────────────────────
    if market_regime(df) == "BEAR":
        return True, "REGIME_BEAR", price

    # ── Time stop ─────────────────────────────────────────────────────────────
    hold_h = (time.time() - pos["entry_ts"]) / 3600
    if hold_h >= MAX_HOLD_HOURS:
        return True, "TIME_STOP_12H", price

    return False, "", price


# ─────────────────────────────────────────────────────────────────────────────
# Thread 1 — EXIT LOOP  (every 20 s)
# ─────────────────────────────────────────────────────────────────────────────
def exit_loop(ex, live: bool) -> None:
    log.info("EXIT LOOP started")
    while True:
        try:
            with _lock:
                symbols = list(state["positions"].keys())

            for symbol in symbols:
                try:
                    df = add_indicators(fetch_ohlcv(ex, symbol))
                    with _lock:
                        if symbol not in state["positions"]:
                            continue
                        pos = state["positions"][symbol]
                        should_exit, reason, price = evaluate_exit(ex, symbol, pos, df)
                        if should_exit:
                            execute_sell(ex, symbol, pos, reason, live, price)
                            del state["positions"][symbol]
                            save_state()
                except Exception as e:
                    log.error(f"Exit eval {symbol}: {e}")

            time.sleep(20)

        except Exception as e:
            log.error(f"Exit loop: {e}")
            time.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# Thread 2 — ENTRY SCAN  (every 5 min)
# ─────────────────────────────────────────────────────────────────────────────
def entry_scan(ex, live: bool) -> None:
    log.info("ENTRY SCAN started")
    last_universe_ts = 0.0

    while True:
        try:
            # Refresh universe hourly
            if time.time() - last_universe_ts > UNIVERSE_REFRESH_H * 3600:
                with _lock:
                    state["universe"]           = refresh_universe(ex)
                    state["universe_refreshed"] = datetime.now(timezone.utc).isoformat()
                last_universe_ts = time.time()

            with _lock:
                check_daily_reset()
                if not can_enter():
                    time.sleep(60)
                    continue
                universe  = list(state["universe"])
                open_syms = set(state["positions"].keys())

            for symbol in universe:
                if symbol in open_syms:
                    continue
                if in_cooldown(symbol):
                    continue

                try:
                    df       = add_indicators(fetch_ohlcv(ex, symbol))
                    qualify, reason = entry_signal(df)

                    if not qualify:
                        continue

                    with _lock:
                        if not can_enter():
                            break
                        if symbol in state["positions"]:
                            continue

                        usd = position_size_usd()
                        if usd > state["cash"] * 0.95:
                            log.info(f"Skipping {symbol} — insufficient cash")
                            continue

                        pos = execute_buy(ex, symbol, usd, live)
                        if pos is None:
                            continue

                        state["positions"][symbol] = pos
                        state["cash"] -= pos["usd_cost"]
                        open_syms.add(symbol)

                        write_audit({
                            "timestamp":    datetime.now(timezone.utc).isoformat(),
                            "symbol":       symbol,
                            "action":       "BUY",
                            "reason":       reason,
                            "signal_price": round(pos["signal_price"], 8),
                            "exec_price":   round(pos["entry_price"], 8),
                            "entry_price":  round(pos["entry_price"], 8),
                            "qty":          round(pos["qty"], 8),
                            "pnl":          0.0,
                            "pnl_pct":      0.0,
                            "hold_hours":   0.0,
                            "equity":       round(state["equity"], 2),
                            "drawdown":     round(
                                (STARTING_BALANCE - state["equity"]) / STARTING_BALANCE, 6),
                            "delay_ms":     0,
                        })
                        save_state()
                        log.info(f"BUY  {symbol:<14} ${usd:.0f}  {reason}  "
                                 f"@{pos['entry_price']:.6f}")

                except Exception as e:
                    log.error(f"Entry scan {symbol}: {e}")

            time.sleep(300)

        except Exception as e:
            log.error(f"Entry scan loop: {e}")
            time.sleep(30)


# ─────────────────────────────────────────────────────────────────────────────
# Thread 3 — HEARTBEAT  (every 5 min, offset 30 s)
# ─────────────────────────────────────────────────────────────────────────────
def heartbeat(ex, live: bool) -> None:
    log.info("HEARTBEAT started")
    time.sleep(30)

    while True:
        try:
            with _lock:
                # Mark-to-market all open positions
                unrealised = 0.0
                for sym, pos in state["positions"].items():
                    try:
                        t     = api(ex.fetch_ticker, sym)
                        price = float(t["last"])
                        unrealised += (price - pos["entry_price"]) * pos["qty"]
                    except Exception:
                        pass

                state["equity"]      = state["cash"] + unrealised
                state["peak_equity"] = max(state["peak_equity"], state["equity"])

                profit_pct = (state["equity"] - STARTING_BALANCE) / STARTING_BALANCE * 100
                daily_dd   = max(0.0, (state["day_start_equity"] - state["equity"])
                                      / state["day_start_equity"] * 100)
                total_dd   = max(0.0, (STARTING_BALANCE - state["equity"])
                                      / STARTING_BALANCE * 100)
                wr         = state["wins"] / max(state["total_trades"], 1) * 100
                pf_denom   = state["gross_loss"] or 1
                pf         = state["gross_profit"] / pf_denom

                print(
                    f"\n{'─'*62}\n"
                    f"  Equity     ${state['equity']:>10.2f}   ({profit_pct:+.2f}%)\n"
                    f"  Cash       ${state['cash']:>10.2f}\n"
                    f"  Unrealised ${unrealised:>+10.2f}\n"
                    f"  Daily DD    {daily_dd:>7.2f}%   (bot halt ≥{DAILY_HALT_PCT*100:.0f}% | limit 3%)\n"
                    f"  Total DD    {total_dd:>7.2f}%   (bot halt ≥{TOTAL_HALT_PCT*100:.0f}% | limit 6%)\n"
                    f"  Positions   {len(state['positions'])}/{MAX_POSITIONS}\n"
                    f"  Trades      {state['total_trades']}   WR={wr:.1f}%   PF={pf:.2f}\n"
                    f"  Halted      {state['halted']}  {state.get('halt_reason','')}\n"
                    f"  Mode        {'LIVE' if live else 'PAPER'}\n"
                    f"{'─'*62}\n"
                )

                save_state()

        except Exception as e:
            log.error(f"Heartbeat: {e}")

        time.sleep(300)


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Breakout Prop Challenge Bot")
    parser.add_argument("--live", action="store_true",
                        help="Place real orders via Kraken API (default: paper)")
    args = parser.parse_args()

    mode = "LIVE" if args.live else "PAPER"

    log.info("=" * 62)
    log.info(f"  Breakout Bot — {mode} MODE")
    log.info(f"  Account:      ${STARTING_BALANCE:,.0f}")
    log.info(f"  Goal:         +10% minimum — bot only stops for loss limits")
    log.info(f"  Daily halt:   {DAILY_HALT_PCT*100:.0f}%  (challenge limit 3%)")
    log.info(f"  Total halt:   {TOTAL_HALT_PCT*100:.0f}%  (challenge limit 6%)")
    log.info(f"  Strategy:     MACD-PINK + K_PHASE2 exits")
    log.info("=" * 62)

    ex = make_exchange()
    load_state()

    threads = [
        threading.Thread(target=exit_loop,   args=(ex, args.live),
                         daemon=True, name="EXIT_LOOP"),
        threading.Thread(target=entry_scan,  args=(ex, args.live),
                         daemon=True, name="ENTRY_SCAN"),
        threading.Thread(target=heartbeat,   args=(ex, args.live),
                         daemon=True, name="HEARTBEAT"),
    ]
    for t in threads:
        t.start()
        log.info(f"Thread started: {t.name}")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        log.info("Shutdown — saving state")
        with _lock:
            save_state()


if __name__ == "__main__":
    main()
