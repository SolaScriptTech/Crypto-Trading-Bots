#!/usr/bin/env python
"""
breakout_signal_bot.py — Breakout Prop Challenge Signal Copilot

Watches the market and tells you exactly what to do.
You execute manually on DX Trade. Bot tracks everything.

Modes
-----
  python breakout_signal_bot.py --live              real signals, confirm each trade
  python breakout_signal_bot.py --replay            60-day dress rehearsal (ask Y/N)
  python breakout_signal_bot.py --replay --auto     replay with auto-confirm (fastest)

What it tells you
-----------------
  BUY  : symbol | how much USD | entry price | stop loss price to set on DX Trade
  SELL : symbol | exit price   | reason      | P&L
  STOP : move your stop to this price (trail update)
  WARN : daily loss or drawdown approaching limit
"""

import os, sys, time, json, csv, logging, argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

import ccxt
import pandas as pd
import pandas_ta as ta
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# Challenge rules
# ─────────────────────────────────────────────────────────────────────────────
STARTING_BALANCE = 5_000.0
DAILY_LOSS_LIMIT = 0.030        # challenge: 3% = $150
TOTAL_DD_LIMIT   = 0.060        # challenge: 6% = $300
DAILY_HALT_PCT   = 0.020        # bot warns/halts at 2% daily loss
TOTAL_HALT_PCT   = 0.045        # bot warns/halts at 4.5% total DD

# ─────────────────────────────────────────────────────────────────────────────
# Strategy constants
# ─────────────────────────────────────────────────────────────────────────────
MAX_POSITIONS     = 5
POSITION_SIZE_PCT = 0.15        # 15% of remaining cash per trade
MIN_POSITION_USD  = 50.0
HARD_STOP_PCT     = 0.015       # K_PHASE2 Phase 1 — 1.5% hard stop
ATR_ARM_PCT       = 0.003       # Phase 2 — trail arms at 0.3% gain
ATR_ARM_ABS       = 2.0         # Phase 2 — trail arms at $2 unrealised
ATR_TRAIL_MULT    = 1.5         # base ATR trail multiplier
ATR_TIGHT_MULT    = 0.7         # tighter once gain > 3%
ATR_COLLAPSE_MULT = 0.3         # LIGHT_GREEN collapse
ATR_TIGHT_GAIN    = 0.030
MAX_HOLD_HOURS    = 12
COOLDOWN_MINUTES  = 120
SLIPPAGE_BPS      = 4          # Breakout fee: 0.04% = 4 bps per side
SWAP_FEE_DAILY    = 0.00033    # 0.033%/day per open position, charged at 00:00 UTC

# ─────────────────────────────────────────────────────────────────────────────
# Universe
# ─────────────────────────────────────────────────────────────────────────────
MIN_VOLUME_USD    = 5_000_000
MAX_UNIVERSE      = 20

# fixed replay universe (liquid Kraken USD pairs)
REPLAY_UNIVERSE = [
    "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD", "DOGE/USD",
    "ADA/USD", "DOT/USD", "AVAX/USD", "LINK/USD", "LTC/USD",
    "MATIC/USD", "UNI/USD", "ATOM/USD", "FIL/USD", "NEAR/USD",
]

# ─────────────────────────────────────────────────────────────────────────────
# Files
# ─────────────────────────────────────────────────────────────────────────────
STATE_FILE = "breakout_signal_state.json"
AUDIT_FILE = "breakout_signal_audit.csv"
LOG_FILE   = "breakout_signal_events.log"

# ─────────────────────────────────────────────────────────────────────────────
# Logging (file only — terminal output is formatted separately)
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8")],
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# State
# ─────────────────────────────────────────────────────────────────────────────
state = {
    "positions":        {},
    "cash":             STARTING_BALANCE,
    "peak_equity":      STARTING_BALANCE,
    "day_start_cash":   STARTING_BALANCE,   # cash-only at 00:30 UTC (per Breakout rules)
    "day_start_date":   "",
    "swap_checked_date": "",                # date swap fee was last deducted
    "cooldowns":        {},
    "universe":         [],
    "total_trades":     0,
    "wins":             0,
    "losses":           0,
    "gross_profit":     0.0,
    "gross_loss":       0.0,
    "total_swap_paid":  0.0,
    "halted":           False,
    "halt_reason":      "",
}


def save_state() -> None:
    tmp = STATE_FILE + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)


def load_state() -> None:
    if Path(STATE_FILE).exists():
        with open(STATE_FILE, encoding="utf-8") as f:
            saved = json.load(f)
        state.update(saved)
        print(f"  Resumed: {len(state['positions'])} open positions, "
              f"equity=${equity():.2f}")


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
_last_call = 0.0


def api(fn, *args, retries: int = 4, **kwargs):
    global _last_call
    gap = time.time() - _last_call
    if gap < 1.5:
        time.sleep(1.5 - gap)
    _last_call = time.time()
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except (ccxt.RateLimitExceeded, ccxt.NetworkError) as e:
            wait = 2 ** attempt * 2
            time.sleep(wait)
        except Exception as e:
            raise
    raise RuntimeError("Max API retries exceeded")


def make_exchange() -> ccxt.kraken:
    return ccxt.kraken({
        "apiKey":          os.getenv("KRAKEN_API_KEY", ""),
        "secret":          os.getenv("KRAKEN_API_SECRET", ""),
        "enableRateLimit": True,
    })


# ─────────────────────────────────────────────────────────────────────────────
# OHLCV cache
# ─────────────────────────────────────────────────────────────────────────────
_cache: dict = {}   # symbol -> (fetched_at, df)
CACHE_TTL = 290     # seconds — re-fetch after ~5 min


def get_ohlcv(ex, symbol: str, timeframe: str = "1h",
              limit: int = 150, force: bool = False) -> pd.DataFrame:
    key = f"{symbol}:{timeframe}"
    now = time.time()
    if not force and key in _cache and now - _cache[key][0] < CACHE_TTL:
        return _cache[key][1]
    raw = api(ex.fetch_ohlcv, symbol, timeframe, limit=limit)
    df  = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    _cache[key] = (now, df)
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Indicators
# ─────────────────────────────────────────────────────────────────────────────
def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    m = ta.macd(df["close"], fast=12, slow=26, signal=9)
    df["macd_hist"] = m["MACDh_12_26_9"]
    df["rsi"]       = ta.rsi(df["close"], length=14)
    bb              = ta.bbands(df["close"], length=20, std=2.0)
    df["bb_lower"]  = bb["BBL_20_2.0"]
    df["ema21"]     = ta.ema(df["close"], length=21)
    df["ema55"]     = ta.ema(df["close"], length=55)
    df["ema20"]     = ta.ema(df["close"], length=20)
    df["ema50"]     = ta.ema(df["close"], length=50)
    df["sma20"]     = ta.sma(df["close"], length=20)
    df["atr"]       = ta.atr(df["high"], df["low"], df["close"], length=14)
    return df


def macd_state(df: pd.DataFrame) -> str:
    if len(df) < 5:
        return "NEUTRAL"
    h2 = float(df["macd_hist"].iloc[-2])
    h3 = float(df["macd_hist"].iloc[-3])
    h4 = float(df["macd_hist"].iloc[-4])
    if h2 < 0 and h3 < 0 and abs(h2) < abs(h3):
        return "PINK" if (h4 < 0 and abs(h3) < abs(h4)) else "PINK_1"
    if h2 > 0 and h3 > 0 and abs(h2) < abs(h3):
        if h4 > 0 and abs(h3) < abs(h4):
            return "LIGHT_GREEN"
    return "NEUTRAL"


def regime(df: pd.DataFrame) -> str:
    r = df.iloc[-2]
    if float(r["ema20"]) > float(r["ema50"]):
        return "BULL"
    if float(r["ema20"]) < float(r["ema50"]) * 0.98:
        return "BEAR"
    return "NEUTRAL"


def entry_signal(df: pd.DataFrame) -> tuple[bool, str, float, float]:
    """Returns (qualify, reason, signal_price, atr)"""
    row   = df.iloc[-2]
    close = float(row["close"])
    atr   = float(row["atr"])

    if macd_state(df) != "PINK":
        return False, f"MACD={macd_state(df)}", close, atr

    rsi = float(row["rsi"])
    if rsi >= 52:
        return False, f"RSI={rsi:.1f}", close, atr

    bb_low = float(row["bb_lower"])
    ema21  = float(row["ema21"])
    sma20  = float(row["sma20"])
    ema55  = float(row["ema55"])

    at_bb      = close <= bb_low * 1.002
    ema21_pull = ema21 <= close <= ema21 * 1.0075
    sma20_tch  = sma20 * 0.999 <= close <= sma20 * 1.002
    rsi_dip    = rsi < 42 and close > ema55

    if not any([at_bb, ema21_pull, sma20_tch, rsi_dip]):
        return False, "no_price_condition", close, atr

    if regime(df) == "BEAR":
        return False, "BEAR_regime", close, atr

    tag = ("BB_LOWER"       if at_bb      else
           "EMA21_PULLBACK" if ema21_pull else
           "SMA20_TOUCH"    if sma20_tch  else "RSI_DIP")
    return True, f"PINK+RSI{rsi:.0f}+{tag}+{regime(df)}", close, atr


# ─────────────────────────────────────────────────────────────────────────────
# Account helpers
# ─────────────────────────────────────────────────────────────────────────────
def equity(prices: dict = None) -> float:
    unrealised = 0.0
    for sym, pos in state["positions"].items():
        price = (prices or {}).get(sym, pos["entry_price"])
        unrealised += (price - pos["entry_price"]) * pos["qty"]
    return state["cash"] + unrealised


def daily_loss_pct(current_equity: float = None) -> float:
    """
    Breakout rule: daily limit = 3% below cash balance at 00:30 UTC.
    Cash excludes open positions — only closed/settled balance.
    Compare that floor against current equity (including open positions).
    """
    dsc = state["day_start_cash"]
    eq  = current_equity if current_equity is not None else equity()
    return max(0.0, (dsc - eq) / dsc)


def total_dd_pct(current_equity: float = None) -> float:
    eq = current_equity if current_equity is not None else equity()
    return max(0.0, (STARTING_BALANCE - eq) / STARTING_BALANCE)


def check_daily_reset() -> None:
    """
    Reset at 00:30 UTC using CASH ONLY (no open positions).
    This matches Breakout's exact rule.
    """
    now   = datetime.now(timezone.utc)
    today = now.date().isoformat()

    # Reset window: between 00:30 and 00:35 UTC each day
    in_reset_window = (now.hour == 0 and 30 <= now.minute < 35)

    if state["day_start_date"] != today and in_reset_window:
        state["day_start_date"] = today
        state["day_start_cash"] = state["cash"]   # cash only, no open positions
        print(f"\n  📅 Daily reset at 00:30 UTC — "
              f"reference cash = ${state['cash']:,.2f}  "
              f"daily floor = ${state['cash'] * (1 - DAILY_LOSS_LIMIT):,.2f}\n")


def apply_swap_fees() -> None:
    """
    Deduct 0.033%/day swap fee at 00:00 UTC on all open positions.
    Based on notional value of each position.
    """
    now   = datetime.now(timezone.utc)
    today = now.date().isoformat()

    in_swap_window = (now.hour == 0 and now.minute < 5)

    if state["swap_checked_date"] != today and in_swap_window and state["positions"]:
        total_swap = 0.0
        for sym, pos in state["positions"].items():
            notional   = pos["entry_price"] * pos["qty"]
            swap_cost  = notional * SWAP_FEE_DAILY
            total_swap += swap_cost

        state["cash"]             -= total_swap
        state["total_swap_paid"]  += total_swap
        state["swap_checked_date"] = today
        print(f"\n  💸 Swap fee deducted: ${total_swap:.4f}  "
              f"(total paid: ${state['total_swap_paid']:.4f})\n")


def in_cooldown(symbol: str) -> bool:
    until = state["cooldowns"].get(symbol)
    return bool(until and datetime.now(timezone.utc) < datetime.fromisoformat(until))


def set_cooldown(symbol: str) -> None:
    state["cooldowns"][symbol] = (
        datetime.now(timezone.utc) + timedelta(minutes=COOLDOWN_MINUTES)
    ).isoformat()


def can_enter() -> bool:
    if state["halted"]:
        return False
    if daily_loss_pct() >= DAILY_HALT_PCT:
        return False
    if total_dd_pct() >= TOTAL_HALT_PCT:
        return False
    return len(state["positions"]) < MAX_POSITIONS


def position_size() -> float:
    return max(state["cash"] * POSITION_SIZE_PCT, MIN_POSITION_USD)


# ─────────────────────────────────────────────────────────────────────────────
# Alert formatting
# ─────────────────────────────────────────────────────────────────────────────
W = 54   # box width

def _box(lines: list[str], border: str = "─") -> str:
    top    = "┌" + border * (W - 2) + "┐"
    bottom = "└" + border * (W - 2) + "┘"
    body   = "\n".join(f"│  {l:<{W-4}}│" for l in lines)
    return f"\n{top}\n{body}\n{bottom}\n"


def alert_buy(symbol: str, usd: float, entry: float,
              stop: float, reason: str) -> str:
    lines = [
        "🟢  BUY SIGNAL",
        "",
        f"  Symbol :  {symbol}",
        f"  Allocate:  ${usd:,.2f}",
        f"  Entry  :  ${entry:,.6f}",
        f"  Stop   :  ${stop:,.6f}  ← SET THIS ON DX TRADE",
        f"  Reason :  {reason}",
    ]
    return _box(lines, "═")


def alert_sell(symbol: str, price: float, reason: str,
               pnl: float, pnl_pct: float) -> str:
    sign  = "+" if pnl >= 0 else ""
    emoji = "✅" if pnl >= 0 else "🔴"
    lines = [
        f"{emoji}  SELL — {symbol}",
        "",
        f"  Exit price:  ${price:,.6f}",
        f"  Reason    :  {reason}",
        f"  P&L       :  {sign}${pnl:,.2f}  ({sign}{pnl_pct:.2f}%)",
    ]
    return _box(lines, "═")


def alert_stop_move(symbol: str, old_stop: float, new_stop: float) -> str:
    return (f"\n  ⚠️  MOVE STOP — {symbol}\n"
            f"     New stop : ${new_stop:,.6f}  (was ${old_stop:,.6f})\n")


def alert_warning(msg: str) -> str:
    return f"\n  ⛔  WARNING: {msg}\n"


# ─────────────────────────────────────────────────────────────────────────────
# Dashboard
# ─────────────────────────────────────────────────────────────────────────────
def bar(used: float, limit: float, width: int = 20) -> str:
    pct   = min(used / limit, 1.0) if limit else 0
    filled = int(pct * width)
    color  = "!!" if pct >= 0.85 else ("! " if pct >= 0.70 else "  ")
    return f"[{'█' * filled}{'░' * (width - filled)}] {color} {pct*100:.0f}%"


def print_dashboard(prices: dict = None) -> None:
    eq         = equity(prices)
    profit     = eq - STARTING_BALANCE
    profit_pct = profit / STARTING_BALANCE * 100
    dsc        = state["day_start_cash"]          # cash-only at 00:30 UTC
    daily_floor = dsc * (1 - DAILY_LOSS_LIMIT)    # breach level
    daily_loss  = max(0.0, dsc - eq)
    daily_lim   = dsc * DAILY_LOSS_LIMIT
    dd_floor    = STARTING_BALANCE * (1 - TOTAL_DD_LIMIT)
    dd_loss     = max(0.0, STARTING_BALANCE - eq)
    dd_lim      = STARTING_BALANCE * TOTAL_DD_LIMIT
    wr         = state["wins"] / max(state["total_trades"], 1) * 100
    now        = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"\n{'─'*W}")
    print(f"  BREAKOUT SIGNAL BOT   {now}")
    print(f"{'─'*W}")

    if state["positions"]:
        print("  OPEN POSITIONS")
        for sym, pos in state["positions"].items():
            price   = (prices or {}).get(sym, pos["entry_price"])
            unreal  = (price - pos["entry_price"]) * pos["qty"]
            pct     = (price / pos["entry_price"] - 1) * 100
            stop    = pos.get("trail_stop") or pos["stop_price"]
            sign    = "+" if unreal >= 0 else ""
            hold_h  = (time.time() - pos["entry_ts"]) / 3600
            print(f"  {sym:<12}  entry ${pos['entry_price']:>10,.4f}  "
                  f"now ${price:>10,.4f}  "
                  f"PnL {sign}${unreal:>7,.2f} ({sign}{pct:.2f}%)  "
                  f"stop ${stop:,.4f}  "
                  f"hold {hold_h:.1f}h")
    else:
        print("  No open positions")

    print(f"{'─'*W}")
    print(f"  Equity  : ${eq:>10,.2f}  ({'+' if profit>=0 else ''}{profit_pct:.2f}%)")
    print(f"  Cash    : ${state['cash']:>10,.2f}")
    print(f"{'─'*W}")
    print(f"  Daily loss  ${daily_loss:>7,.2f} / ${daily_lim:,.2f}  "
          f"{bar(daily_loss, daily_lim)}  floor ${daily_floor:,.2f}")
    print(f"  Max DD      ${dd_loss:>7,.2f} / ${dd_lim:,.2f}  "
          f"{bar(dd_loss, dd_lim)}  floor ${dd_floor:,.2f}")
    print(f"  Swap paid   ${state.get('total_swap_paid', 0):,.4f}")
    print(f"{'─'*W}")
    print(f"  Trades: {state['total_trades']}   "
          f"W: {state['wins']}  L: {state['losses']}  WR: {wr:.1f}%")
    if state["halted"]:
        print(f"  ⛔ HALTED: {state['halt_reason']}")
    print(f"{'─'*W}\n")


# ─────────────────────────────────────────────────────────────────────────────
# K_PHASE2 exit evaluation
# ─────────────────────────────────────────────────────────────────────────────
def evaluate_exit_live(pos: dict, price: float,
                       atr: float, mstate: str) -> tuple[bool, str]:
    """
    Returns (should_exit, reason). Mutates pos (peak, trail).
    """
    if price > pos["peak_price"]:
        pos["peak_price"] = price

    gain_pct = (price / pos["entry_price"]) - 1
    gain_abs = (price - pos["entry_price"]) * pos["qty"]

    # Phase 1 — hard stop
    if price <= pos["stop_price"]:
        return True, "HARD_STOP"

    # Phase 2 — arm trail
    if not pos["trail_armed"] and gain_pct >= ATR_ARM_PCT and gain_abs >= ATR_ARM_ABS:
        mult               = ATR_TIGHT_MULT if gain_pct >= ATR_TIGHT_GAIN else ATR_TRAIL_MULT
        pos["trail_armed"] = True
        pos["trail_stop"]  = pos["peak_price"] - mult * atr

    if pos["trail_armed"]:
        if mstate == "LIGHT_GREEN":
            mult = ATR_COLLAPSE_MULT
        elif gain_pct >= ATR_TIGHT_GAIN:
            mult = ATR_TIGHT_MULT
        else:
            mult = ATR_TRAIL_MULT

        new_trail         = pos["peak_price"] - mult * atr
        pos["trail_stop"] = max(pos["trail_stop"], new_trail)

        if price <= pos["trail_stop"]:
            return True, "TRAIL_STOP"

    # Regime flip
    # (regime checked by caller passing df)
    hold_h = (time.time() - pos["entry_ts"]) / 3600
    if hold_h >= MAX_HOLD_HOURS:
        return True, "TIME_STOP_12H"

    return False, ""


def record_sell(symbol: str, pos: dict, price: float, reason: str) -> float:
    exec_price = price * (1 - SLIPPAGE_BPS / 10_000)
    pnl        = (exec_price - pos["entry_price"]) * pos["qty"]
    pnl_pct    = (exec_price / pos["entry_price"] - 1) * 100
    hold_h     = (time.time() - pos["entry_ts"]) / 3600

    state["total_trades"] += 1
    if pnl >= 0:
        state["wins"]         += 1
        state["gross_profit"] += pnl
    else:
        state["losses"]       += 1
        state["gross_loss"]   += abs(pnl)

    state["cash"]       += pos["usd_cost"] + pnl
    state["peak_equity"] = max(state["peak_equity"], equity())

    write_audit({
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "symbol":      symbol,
        "action":      "SELL",
        "reason":      reason,
        "entry_price": round(pos["entry_price"], 8),
        "exec_price":  round(exec_price, 8),
        "qty":         round(pos["qty"], 8),
        "pnl":         round(pnl, 4),
        "pnl_pct":     round(pnl_pct, 4),
        "hold_hours":  round(hold_h, 2),
        "equity":      round(equity(), 2),
        "daily_dd":    round(daily_loss_pct() * 100, 3),
        "total_dd":    round(total_dd_pct() * 100, 3),
    })
    set_cooldown(symbol)
    log.info(f"SELL {symbol} {reason} pnl={pnl:+.2f} equity={equity():.2f}")
    return pnl


def record_buy(symbol: str, entry_price: float, usd: float,
               stop_price: float, reason: str) -> dict:
    qty = usd / entry_price
    pos = {
        "symbol":      symbol,
        "qty":         qty,
        "entry_price": entry_price,
        "usd_cost":    usd,
        "peak_price":  entry_price,
        "stop_price":  stop_price,
        "trail_armed": False,
        "trail_stop":  None,
        "entry_time":  datetime.now(timezone.utc).isoformat(),
        "entry_ts":    time.time(),
        "last_stop_alerted": stop_price,
    }
    state["cash"] -= usd
    write_audit({
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "symbol":      symbol,
        "action":      "BUY",
        "reason":      reason,
        "entry_price": round(entry_price, 8),
        "exec_price":  round(entry_price, 8),
        "qty":         round(qty, 8),
        "pnl":         0.0,
        "pnl_pct":     0.0,
        "hold_hours":  0.0,
        "equity":      round(equity(), 2),
        "daily_dd":    round(daily_loss_pct() * 100, 3),
        "total_dd":    round(total_dd_pct() * 100, 3),
    })
    log.info(f"BUY {symbol} ${usd:.0f} @{entry_price:.6f} {reason}")
    return pos


# ─────────────────────────────────────────────────────────────────────────────
# Risk warnings
# ─────────────────────────────────────────────────────────────────────────────
def check_limits() -> bool:
    """Print warnings. Returns False if bot should halt."""
    dl = daily_loss_pct()
    dd = total_dd_pct()

    if dl >= DAILY_HALT_PCT:
        msg = (f"Daily loss {dl*100:.1f}% — bot halting. "
               f"CLOSE ALL POSITIONS. Challenge limit is 3%.")
        print(alert_warning(msg))
        state["halted"]      = True
        state["halt_reason"] = f"Daily loss {dl*100:.1f}%"
        return False

    if dd >= TOTAL_HALT_PCT:
        msg = (f"Total drawdown {dd*100:.1f}% — bot halting. "
               f"CLOSE ALL POSITIONS. Challenge limit is 6%.")
        print(alert_warning(msg))
        state["halted"]      = True
        state["halt_reason"] = f"Total DD {dd*100:.1f}%"
        return False

    if dl >= 0.017:
        print(alert_warning(
            f"Daily loss at {dl*100:.1f}% — approaching 2% internal halt"))

    if dd >= 0.038:
        print(alert_warning(
            f"Total DD at {dd*100:.1f}% — approaching 4.5% internal halt"))

    return True


# ─────────────────────────────────────────────────────────────────────────────
# LIVE MODE
# ─────────────────────────────────────────────────────────────────────────────
def startup_timing_check() -> None:
    """
    Warn if starting at a bad time.
    Optimal: 00:30–23:30 UTC (avoid the midnight swap window and pre-reset window).
    In PST/PDT terms: best start is after 4:30 PM PST / 5:30 PM PDT.
    """
    now     = datetime.now(timezone.utc)
    h, m    = now.hour, now.minute
    utc_str = now.strftime("%H:%M UTC")

    # Detect local offset (rough — April = PDT = UTC-7)
    import time as _t
    local_offset_h = -(_t.timezone if not _t.daylight else _t.altzone) // 3600
    local_now      = datetime.now()
    local_str      = local_now.strftime("%I:%M %p")
    tz_label       = "PDT" if _t.daylight and _t.localtime().tm_isdst else "PST"

    print(f"\n  Current time : {utc_str}  ({local_str} {tz_label})")

    # Bad window: 23:30–00:30 UTC (swap fires at 00:00, reset at 00:30)
    in_bad_window = (h == 23 and m >= 30) or (h == 0 and m < 30)

    if in_bad_window:
        bad_end_utc   = "00:30 UTC"
        bad_end_local = "4:30 PM PDT / 3:30 PM PST"
        print(f"\n  ⚠️  BAD START TIME")
        print(f"     You are in the midnight window ({bad_end_utc} reset).")
        print(f"     Swap fee fires at 00:00 UTC — any open position pays immediately.")
        print(f"     Wait until after 00:30 UTC ({bad_end_local}) to start trading.")
    else:
        # Calculate time until next bad window
        mins_until_reset = ((23 * 60 + 30) - (h * 60 + m)) % (24 * 60)
        hrs  = mins_until_reset // 60
        mins = mins_until_reset % 60
        print(f"  Good start time — {hrs}h {mins}m until next midnight window")
        print(f"  Optimal trading window: 00:30–23:30 UTC  "
              f"(5:30 PM–4:30 PM PDT the next day)")
    print()


def run_live(ex) -> None:
    print("\n" + "=" * W)
    print("  LIVE MODE — signal copilot")
    print(f"  Account: ${STARTING_BALANCE:,.0f}")
    print(f"  Daily halt: {DAILY_HALT_PCT*100:.0f}%  |  DD halt: {TOTAL_HALT_PCT*100:.0f}%")
    print("  Scanning every 30s — execute signals on DX Trade")
    print("=" * W)

    startup_timing_check()
    load_state()
    last_universe_refresh = 0.0
    last_entry_scan       = {}   # symbol -> last signal ts (to avoid duplicates)

    while True:
        try:
            # ── Universe refresh (hourly) ─────────────────────────────────
            if time.time() - last_universe_refresh > 3600:
                try:
                    tickers = api(ex.fetch_tickers)
                    pairs = sorted(
                        [(s, t.get("quoteVolume") or 0)
                         for s, t in tickers.items()
                         if s.endswith("/USD") and (t.get("quoteVolume") or 0) >= MIN_VOLUME_USD],
                        key=lambda x: -x[1]
                    )
                    state["universe"] = [p[0] for p in pairs[:MAX_UNIVERSE]]
                    last_universe_refresh = time.time()
                    print(f"  Universe updated: {len(state['universe'])} pairs")
                except Exception as e:
                    print(f"  Universe refresh failed: {e}")

            apply_swap_fees()
            check_daily_reset()

            # ── Exit checks for open positions ────────────────────────────
            prices = {}
            for symbol in list(state["positions"].keys()):
                try:
                    df    = add_indicators(get_ohlcv(ex, symbol))
                    t     = api(ex.fetch_ticker, symbol)
                    price = float(t["last"])
                    atr   = float(df["atr"].iloc[-2])
                    mst   = macd_state(df)
                    reg   = regime(df)
                    prices[symbol] = price

                    pos = state["positions"][symbol]
                    old_stop = pos.get("trail_stop") or pos["stop_price"]

                    should_exit, reason = evaluate_exit_live(pos, price, atr, mst)

                    # Regime flip
                    if not should_exit and reg == "BEAR":
                        should_exit, reason = True, "REGIME_BEAR"

                    # Trail move alert
                    new_stop = pos.get("trail_stop") or pos["stop_price"]
                    if (pos.get("trail_armed") and
                            abs(new_stop - old_stop) / old_stop > 0.001):
                        print(alert_stop_move(symbol, old_stop, new_stop))
                        pos["last_stop_alerted"] = new_stop

                    if should_exit:
                        pnl_est = (price - pos["entry_price"]) * pos["qty"]
                        pnl_pct = (price / pos["entry_price"] - 1) * 100
                        print(alert_sell(symbol, price, reason, pnl_est, pnl_pct))
                        input("  Press ENTER once you have closed this position on DX Trade... ")
                        record_sell(symbol, pos, price, reason)
                        del state["positions"][symbol]
                        save_state()

                except Exception as e:
                    print(f"  Exit check failed {symbol}: {e}")

            if not check_limits():
                print("  Bot halted. Close all open positions on DX Trade.")
                save_state()
                return

            # ── Entry scan ────────────────────────────────────────────────
            if can_enter():
                for symbol in state["universe"]:
                    if symbol in state["positions"] or in_cooldown(symbol):
                        continue
                    try:
                        df = add_indicators(get_ohlcv(ex, symbol))
                        candle_ts = str(df.iloc[-2]["ts"])

                        # skip if we already signalled on this candle
                        if last_entry_scan.get(symbol) == candle_ts:
                            continue

                        qualify, reason, sig_price, atr = entry_signal(df)
                        if not qualify or not can_enter():
                            continue

                        last_entry_scan[symbol] = candle_ts
                        usd        = position_size()
                        entry      = sig_price * (1 + SLIPPAGE_BPS / 10_000)
                        stop       = entry * (1 - HARD_STOP_PCT)

                        print(alert_buy(symbol, usd, entry, stop, reason))
                        ans = input("  Execute this trade? [Y/N]: ").strip().upper()

                        if ans == "Y":
                            # Ask for actual DX Trade fill price (may differ slightly from signal)
                            fill_str = input(
                                f"  Actual fill price on DX Trade "
                                f"(press Enter to use signal price ${entry:,.6f}): "
                            ).strip()
                            actual_fill = float(fill_str) if fill_str else entry
                            actual_stop = actual_fill * (1 - HARD_STOP_PCT)

                            if fill_str:
                                print(f"  Using your fill: ${actual_fill:,.6f}  "
                                      f"→ stop adjusted to ${actual_stop:,.6f}")

                            pos = record_buy(symbol, actual_fill, usd, actual_stop, reason)
                            state["positions"][symbol] = pos
                            save_state()
                            print(f"  ✅ Logged. Track this position in the dashboard.\n")
                        else:
                            print(f"  Skipped.\n")

                    except Exception as e:
                        print(f"  Entry scan {symbol}: {e}")

            # ── Dashboard ─────────────────────────────────────────────────
            print_dashboard(prices)
            time.sleep(30)

        except KeyboardInterrupt:
            print("\n  Shutting down — saving state")
            save_state()
            return
        except Exception as e:
            print(f"  Loop error: {e}")
            time.sleep(10)


# ─────────────────────────────────────────────────────────────────────────────
# REPLAY MODE
# ─────────────────────────────────────────────────────────────────────────────
def run_replay(ex, auto: bool = False) -> None:
    print("\n" + "=" * W)
    print("  REPLAY MODE — 60-day dress rehearsal")
    print(f"  Auto-confirm: {'YES' if auto else 'NO — you will be asked Y/N'}")
    print("  Fetching historical data from Kraken...")
    print("=" * W + "\n")

    # ── Fetch data ────────────────────────────────────────────────────────
    data: dict = {}   # symbol -> df with indicators
    for symbol in REPLAY_UNIVERSE:
        try:
            # Kraken returns max 720 per call for 1h — fetch twice for 60 days
            raw1 = api(ex.fetch_ohlcv, symbol, "1h", limit=720)
            raw2 = api(ex.fetch_ohlcv, symbol, "1h", limit=720,
                       params={"since": raw1[0][0]})
            raw  = sorted(set(map(tuple, raw1 + raw2)), key=lambda x: x[0])
            df   = pd.DataFrame(raw, columns=["ts","open","high","low","close","volume"])
            df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
            df   = add_indicators(df).dropna().reset_index(drop=True)
            data[symbol] = df
            print(f"  ✓ {symbol}  ({len(df)} candles)")
        except Exception as e:
            print(f"  ✗ {symbol}: {e}")

    if not data:
        print("  No data fetched. Check API keys.")
        return

    # ── Build unified timeline ────────────────────────────────────────────
    all_ts = sorted(set(
        ts for df in data.values() for ts in df["ts"].tolist()
    ))

    # reset state for clean replay
    state.update({
        "positions": {}, "cash": STARTING_BALANCE,
        "peak_equity": STARTING_BALANCE,
        "day_start_equity": STARTING_BALANCE,
        "day_start_date": "",
        "cooldowns": {}, "total_trades": 0,
        "wins": 0, "losses": 0,
        "gross_profit": 0.0, "gross_loss": 0.0,
        "halted": False, "halt_reason": "",
    })
    last_entry_scan = {}

    print(f"\n  Starting replay over {len(all_ts)} hourly candles "
          f"across {len(data)} symbols...\n")

    for i, ts in enumerate(all_ts):
        if state["halted"]:
            break

        # ── Daily reset ───────────────────────────────────────────────
        date_str = ts.date().isoformat() if hasattr(ts, 'date') else str(ts)[:10]
        if state["day_start_date"] != date_str:
            state["day_start_date"]   = date_str
            state["day_start_equity"] = equity()

        # ── Exit checks ───────────────────────────────────────────────
        for symbol in list(state["positions"].keys()):
            if symbol not in data:
                continue
            df  = data[symbol]
            row = df[df["ts"] == ts]
            if row.empty:
                continue
            row = row.iloc[0]

            candle_high  = float(row["high"])
            candle_low   = float(row["low"])
            candle_close = float(row["close"])
            atr          = float(row["atr"]) if not pd.isna(row["atr"]) else 0

            pos = state["positions"][symbol]
            old_stop = pos.get("trail_stop") or pos["stop_price"]

            # Simulate intra-candle: price first goes to high, then to low
            # (conservative assumption)
            for test_price in [candle_high, candle_low, candle_close]:
                # update peak on the way up
                if test_price == candle_high and test_price > pos["peak_price"]:
                    pos["peak_price"] = test_price

                mst = "NEUTRAL"   # simplified for replay
                should_exit, reason = evaluate_exit_live(pos, test_price, atr, mst)

                if should_exit:
                    exit_price = test_price
                    pnl_est    = (exit_price - pos["entry_price"]) * pos["qty"]
                    pnl_pct    = (exit_price / pos["entry_price"] - 1) * 100
                    print(f"  [{ts}]")
                    print(alert_sell(symbol, exit_price, reason, pnl_est, pnl_pct))
                    record_sell(symbol, pos, exit_price, reason)
                    del state["positions"][symbol]
                    break

            # Trail move alert
            new_stop = pos.get("trail_stop") or pos.get("stop_price", 0) if symbol in state["positions"] else None
            if (new_stop and symbol in state["positions"] and
                    state["positions"][symbol].get("trail_armed") and
                    abs(new_stop - old_stop) / old_stop > 0.002):
                print(alert_stop_move(symbol, old_stop, new_stop))

        # ── Entry scan ────────────────────────────────────────────────
        if can_enter():
            for symbol, df in data.items():
                if symbol in state["positions"] or in_cooldown(symbol):
                    continue

                # get rows up to current ts
                hist = df[df["ts"] <= ts]
                if len(hist) < 5:
                    continue

                candle_ts_str = str(hist.iloc[-1]["ts"])
                if last_entry_scan.get(symbol) == candle_ts_str:
                    continue

                qualify, reason, sig_price, atr = entry_signal(hist)
                if not qualify or not can_enter():
                    continue

                last_entry_scan[symbol] = candle_ts_str
                usd   = position_size()
                entry = sig_price * (1 + SLIPPAGE_BPS / 10_000)
                stop  = entry * (1 - HARD_STOP_PCT)

                print(f"\n  [{ts}]")
                print(alert_buy(symbol, usd, entry, stop, reason))

                if auto:
                    ans = "Y"
                    print("  [AUTO] Confirmed.\n")
                else:
                    ans = input("  Execute? [Y/N]: ").strip().upper()

                if ans == "Y":
                    pos = record_buy(symbol, entry, usd, stop, reason)
                    pos["entry_ts"] = ts.timestamp() if hasattr(ts, 'timestamp') else time.time()
                    state["positions"][symbol] = pos

        # Check limits
        dl = daily_loss_pct()
        dd = total_dd_pct()
        if dl >= DAILY_HALT_PCT:
            print(alert_warning(
                f"[{ts}] Daily loss {dl*100:.1f}% — would halt here"))
            state["halted"]      = True
            state["halt_reason"] = f"Daily loss {dl*100:.1f}%"
        if dd >= TOTAL_HALT_PCT:
            print(alert_warning(
                f"[{ts}] Total DD {dd*100:.1f}% — would halt here"))
            state["halted"]      = True
            state["halt_reason"] = f"Total DD {dd*100:.1f}%"

        # Progress every 24 candles (once per day)
        if i % 24 == 0:
            print_dashboard()

        time.sleep(0.05)   # fast replay — 60 days in ~minutes

    # ── Final summary ─────────────────────────────────────────────────
    eq       = equity()
    ret_pct  = (eq - STARTING_BALANCE) / STARTING_BALANCE * 100
    wr       = state["wins"] / max(state["total_trades"], 1) * 100
    pf       = state["gross_profit"] / max(state["gross_loss"], 0.01)

    print("\n" + "=" * W)
    print("  REPLAY COMPLETE — 60-day summary")
    print("=" * W)
    print(f"  Starting equity : ${STARTING_BALANCE:,.2f}")
    print(f"  Final equity    : ${eq:,.2f}  ({'+' if ret_pct>=0 else ''}{ret_pct:.2f}%)")
    print(f"  Total trades    : {state['total_trades']}")
    print(f"  Win rate        : {wr:.1f}%  ({state['wins']}W / {state['losses']}L)")
    print(f"  Profit factor   : {pf:.2f}")
    print(f"  Gross profit    : ${state['gross_profit']:,.2f}")
    print(f"  Gross loss      : ${state['gross_loss']:,.2f}")
    print(f"  Halted          : {state['halted']}  {state.get('halt_reason','')}")
    print("=" * W + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    parser = argparse.ArgumentParser(description="Breakout Signal Copilot")
    parser.add_argument("--live",   action="store_true", help="Live signal mode")
    parser.add_argument("--replay", action="store_true", help="60-day replay mode")
    parser.add_argument("--auto",   action="store_true",
                        help="Auto-confirm all signals in replay (fastest)")
    args = parser.parse_args()

    if not args.live and not args.replay:
        parser.print_help()
        print("\n  Run with --live or --replay\n")
        sys.exit(1)

    ex = make_exchange()

    if args.replay:
        run_replay(ex, auto=args.auto)
    else:
        run_live(ex)


if __name__ == "__main__":
    main()
