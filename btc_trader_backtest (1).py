"""
btc_trader_backtest.py  v2
==========================
Backtester for the BTC_trader dual-regime strategy.

Fetches real historical 1h OHLCV data from Kraken (BTC/USD),
runs the exact same signal logic as the live bot, and produces
a full trade-by-trade audit trail + summary statistics.

Strategy:
  BULL regime    → trend-following  (EMA pullback, BB lower, SMA touch, RSI oversold)
  NEUTRAL regime → range trading    (same signals — bot no longer sits on hands)
  BEAR regime    → no entry
  Idleness guard → after 8h flat in BULL/NEUTRAL, any one soft signal fires

Regime detection:
  BULL    = price > EMA21 AND EMA21 > EMA55 AND ADX > 15
  BEAR    = EMA21 < EMA55 confirmed for 2 consecutive bars  ← v2 fix
  NEUTRAL = everything else

v2 fixes applied after first backtest analysis:
  1. NEUTRAL trail stop widened 1.8% → 2.8%
     (1.8% was catching normal noise before moves developed;
      winners needed 20-25h — trail was too impatient)
  2. Regime flip confirmation — BEAR requires 2 consecutive bars before
     forcing an exit. Single-candle flips were causing whipsaws on
     trades 4, 6, 7 (all exited within 3h of entry).
  3. ATR contraction filter — skip entries when ATR(14) is below its
     10-bar average. Low/falling volatility = no follow-through energy.
     Trades 3, 5, 9, 11, 12 all entered in contracting ATR environments.

Usage:
    pip install ccxt pandas numpy tabulate colorama
    python btc_trader_backtest.py

    Optional flags:
    --days 90          how many days of history to test (default 90)
    --capital 2000     starting capital in USD (default 2000)
    --slippage 0.0010  slippage per side in decimal (default 10bps)
    --no-fetch         skip API fetch, use cached data/btc_1h.csv if present

Output:
    - Console summary: total return, win rate, avg trade, max drawdown, Sharpe
    - Equity curve printed as ASCII sparkline
    - Full trade log written to backtest_trades.csv
    - Equity curve written to backtest_equity_curve.csv
    - Summary JSON written to backtest_summary.json
    - All output appended to kraken_btc_trader_events.log (unified log)
"""

import argparse
import csv
import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

# ── optional pretty output ───────────────────────────────────────────────────
try:
    from tabulate import tabulate
    HAS_TABULATE = True
except ImportError:
    HAS_TABULATE = False

try:
    from colorama import Fore, Style, init as colorama_init
    colorama_init(autoreset=True)
    HAS_COLOR = True
except ImportError:
    HAS_COLOR = False

import pandas as pd
import numpy as np

try:
    import ccxt
except ImportError:
    print("ERROR: ccxt not installed.  Run: pip install ccxt pandas numpy tabulate colorama")
    sys.exit(1)


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG — mirrors live bot constants
# ─────────────────────────────────────────────────────────────────────────────
BASE_DIR       = Path(__file__).parent
LOG_FILE       = BASE_DIR / "kraken_btc_trader_events.log"   # unified log
CACHE_FILE     = BASE_DIR / "data" / "btc_1h.csv"
TRADE_CSV      = BASE_DIR / "backtest_trades.csv"

SLIPPAGE       = 0.0010    # 10bps per side
STOP_PCT       = 0.035     # 3.5% hard stop
BULL_TRAIL     = 0.013     # 1.3% trailing stop in bull
NEUTRAL_TRAIL  = 0.028     # 2.8% trailing stop in neutral — v2: widened from 1.8%
BEAR_CONFIRM   = 2         # v2: bars BEAR must persist before forcing exit
ATR_LOOKBACK   = 10        # v2: ATR contraction filter lookback
BB_MULT_BULL   = 1.5
BB_MULT_OTHER  = 2.0
ADX_PERIOD     = 14
ADX_THRESHOLD  = 15        # lowered from 20
EMA_FAST       = 21
EMA_SLOW       = 55
MAX_DD         = 0.15      # 15% kill switch
IDLE_HOURS     = 8         # idleness guard threshold


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING — writes to unified log file AND stdout
# ─────────────────────────────────────────────────────────────────────────────
def log(msg: str, level: str = "INFO"):
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] [BACKTEST] [{level}] {msg}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ─────────────────────────────────────────────────────────────────────────────
# DATA FETCH
# ─────────────────────────────────────────────────────────────────────────────
def fetch_ohlcv(days: int = 90, use_cache: bool = False) -> pd.DataFrame:
    """Fetch 1h BTC/USD candles from Kraken. Caches to CSV."""
    if use_cache and CACHE_FILE.exists():
        log(f"Loading cached OHLCV from {CACHE_FILE}")
        df = pd.read_csv(CACHE_FILE, parse_dates=["datetime"])
        return df

    log(f"Fetching {days} days of 1h BTC/USD OHLCV from Kraken...")
    exchange = ccxt.kraken({"enableRateLimit": True})

    limit        = days * 24 + 100   # extra buffer for indicator warmup
    since_ms     = int((time.time() - limit * 3600) * 1000)
    all_candles  = []
    batch        = 720   # Kraken max per call

    since = since_ms
    while True:
        try:
            candles = exchange.fetch_ohlcv("BTC/USD", "1h", since=since, limit=batch)
        except Exception as e:
            log(f"API error: {e}", "WARN")
            time.sleep(5)
            continue
        if not candles:
            break
        all_candles.extend(candles)
        last_ts = candles[-1][0]
        if last_ts >= int(time.time() * 1000) - 3600_000:
            break
        since = last_ts + 1
        time.sleep(1.5)

    df = pd.DataFrame(all_candles, columns=["ts", "o", "h", "l", "c", "v"])
    df["datetime"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df = df.drop_duplicates("ts").sort_values("ts").reset_index(drop=True)

    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHE_FILE, index=False)
    log(f"Fetched {len(df)} candles. Cached to {CACHE_FILE}")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────────────────────
def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Bollinger Bands (SMA20 base)
    df["sma20"] = df["c"].rolling(20).mean()
    df["std20"] = df["c"].rolling(20).std()

    # EMAs
    df["ema21"] = df["c"].ewm(span=EMA_FAST, adjust=False).mean()
    df["ema55"] = df["c"].ewm(span=EMA_SLOW, adjust=False).mean()

    # RSI(14)
    delta       = df["c"].diff()
    gain        = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    loss        = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
    df["rsi"]   = 100 - 100 / (1 + gain / (loss + 1e-9))

    # ATR(14) + contraction filter
    h, l, c     = df["h"], df["l"], df["c"]
    tr          = pd.concat([(h-l), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    df["atr"]   = tr.ewm(span=ADX_PERIOD, adjust=False).mean()
    df["atr_avg"] = df["atr"].rolling(ATR_LOOKBACK).mean()  # v2: contraction baseline

    # ADX(14)
    pdm         = h.diff().clip(lower=0)
    ndm         = (-l.diff()).clip(lower=0)
    atr_adx     = tr.ewm(span=ADX_PERIOD, adjust=False).mean()
    pdi         = 100 * pdm.ewm(span=ADX_PERIOD, adjust=False).mean() / (atr_adx + 1e-9)
    ndi         = 100 * ndm.ewm(span=ADX_PERIOD, adjust=False).mean() / (atr_adx + 1e-9)
    dx          = (abs(pdi - ndi) / (pdi + ndi + 1e-9)) * 100
    df["adx"]   = dx.ewm(span=ADX_PERIOD, adjust=False).mean()

    return df


def bb_bands(df: pd.DataFrame, idx: int, regime: str):
    mult   = BB_MULT_BULL if regime == "BULL" else BB_MULT_OTHER
    sma    = df["sma20"].iloc[idx]
    std    = df["std20"].iloc[idx]
    return sma - mult * std, sma + mult * std


# ─────────────────────────────────────────────────────────────────────────────
# REGIME DETECTION  (fixed: ETH removed, ADX threshold 15)
# ─────────────────────────────────────────────────────────────────────────────
def detect_regime(df: pd.DataFrame, idx: int) -> str:
    ema21 = df["ema21"].iloc[idx]
    ema55 = df["ema55"].iloc[idx]
    price = df["c"].iloc[idx]
    adx   = df["adx"].iloc[idx]

    btc_bull = (price > ema21) and (ema21 > ema55)
    btc_bear = ema21 < ema55

    if btc_bull and adx > ADX_THRESHOLD:
        return "BULL"
    if btc_bear:
        return "BEAR"
    return "NEUTRAL"


# ─────────────────────────────────────────────────────────────────────────────
# SIGNAL ENGINE  (exact mirror of proposed BTC_trader fixes)
# ─────────────────────────────────────────────────────────────────────────────
def entry_signal(df: pd.DataFrame, idx: int, regime: str,
                 last_sell_idx: int) -> tuple[bool, str]:
    """
    Returns (should_buy, reason).
    Signals A-E same as BTC_trader.  Idleness guard applies in BULL/NEUTRAL.
    NEUTRAL now gets full signal suite (not just bare BB).
    v2: ATR contraction filter blocks entries in low-energy environments.
    """
    if regime == "BEAR":
        return False, ""

    row    = df.iloc[idx]
    price  = row["c"]
    ema21  = row["ema21"]
    ema55  = row["ema55"]
    sma20  = row["sma20"]
    rsi    = row["rsi"]
    atr    = row["atr"]
    atr_avg = row["atr_avg"]

    lower, _ = bb_bands(df, idx, regime)

    if any(pd.isna([ema21, ema55, sma20, rsi, lower, atr, atr_avg])):
        return False, ""

    # v2 FIX 3: ATR contraction filter — skip if volatility is contracting
    # No energy = no follow-through = trail stop will catch us before the move
    if atr < atr_avg * 0.85:
        return False, ""

    # A. BB lower band touch
    sig_bb = price < lower

    # B. EMA21 crossover (prev bar below, current above)
    if idx > 0:
        sig_cross = (df["c"].iloc[idx-1] < df["ema21"].iloc[idx-1]) and (price > ema21)
    else:
        sig_cross = False

    # C. EMA21 pullback: 0-0.75% below EMA21, RSI < 52
    sig_ema_pb = (price < ema21) and (price >= ema21 * 0.9925) and (rsi < 52)

    # D. SMA20 touch: price < SMA20, RSI < 50
    sig_sma = (price < sma20) and (rsi < 50)

    # E. RSI oversold + uptrend: RSI < 42, price > EMA55
    sig_rsi = (rsi < 42) and (price > ema55)

    # Idleness guard: flat > 8h in BULL or NEUTRAL → any soft signal fires
    flat_bars = idx - last_sell_idx
    idle      = flat_bars > IDLE_HOURS

    if sig_bb:
        return True, "BB_LOWER"
    if sig_cross:
        return True, "EMA21_CROSS"
    if sig_ema_pb:
        return True, f"EMA21_PULLBACK{'_IDLE' if idle else ''}"
    if sig_sma:
        return True, f"SMA20_TOUCH{'_IDLE' if idle else ''}"
    if sig_rsi:
        return True, f"RSI_OVERSOLD{'_IDLE' if idle else ''}"

    return False, ""


def exit_signal(df: pd.DataFrame, idx: int, regime: str,
                entry_price: float, peak_price: float,
                trail_pct: float, bear_confirmed: bool = True) -> tuple[bool, str]:
    """Returns (should_sell, reason).
    v2: SELL_REGIME_FLIP only triggers when bear_confirmed (2+ consecutive BEAR bars).
    """
    price    = df["c"].iloc[idx]
    _, upper = bb_bands(df, idx, regime)
    ema21    = df["ema21"].iloc[idx]

    # Hard stop
    if price <= entry_price * (1 - STOP_PCT):
        return True, "SELL_STOP"

    # Trailing stop
    trail_stop = peak_price * (1 - trail_pct)
    if price <= trail_stop:
        return True, f"SELL_TRAIL"

    # BB upper target (but not in BULL if price < EMA21*1.01 — let it run)
    if not pd.isna(upper) and price > upper:
        if not (regime == "BULL" and price < ema21 * 1.01):
            return True, "SELL_TARGET"

    # v2 FIX 2: regime flip to BEAR requires confirmation
    if regime == "BEAR" and bear_confirmed:
        return True, "SELL_REGIME_FLIP"

    return False, ""


# ─────────────────────────────────────────────────────────────────────────────
# BACKTESTER
# ─────────────────────────────────────────────────────────────────────────────
class Backtester:
    def __init__(self, starting_capital: float = 2000.0, slippage: float = SLIPPAGE):
        self.capital   = starting_capital
        self.slippage  = slippage
        self.start_cap = starting_capital

        # position state
        self.btc         = 0.0
        self.entry_price = 0.0
        self.peak_price  = 0.0
        self.entry_idx   = 0
        self.last_sell_idx = 0
        self.bear_bars   = 0   # v2: consecutive BEAR bars counter

        # results
        self.trades     = []
        self.equity_curve = []
        self.max_equity = starting_capital

    def run(self, df: pd.DataFrame) -> dict:
        log(f"Starting backtest on {len(df)} candles "
            f"({df['datetime'].iloc[0]} → {df['datetime'].iloc[-1]})")

        # Warmup: skip first 60 bars so indicators are stable
        WARMUP = 60

        for i in range(WARMUP, len(df)):
            row    = df.iloc[i]
            price  = row["c"]
            regime = detect_regime(df, i)

            # v2 FIX 2: track consecutive BEAR bars for flip confirmation
            if regime == "BEAR":
                self.bear_bars += 1
            else:
                self.bear_bars = 0

            # confirmed BEAR = BEAR_CONFIRM consecutive bars
            bear_confirmed = self.bear_bars >= BEAR_CONFIRM

            # Update peak if in position
            if self.btc > 0:
                self.peak_price = max(self.peak_price, price)

            # Current equity
            equity = self.capital + self.btc * price
            self.max_equity = max(self.max_equity, equity)
            dd = (self.max_equity - equity) / self.max_equity

            self.equity_curve.append({
                "datetime": row["datetime"],
                "equity":   round(equity, 2),
                "price":    price,
                "regime":   regime,
                "dd":       round(dd, 4),
            })

            # Kill switch
            if dd >= MAX_DD:
                log(f"KILL SWITCH at bar {i}: drawdown {dd*100:.1f}%", "WARN")
                if self.btc > 0:
                    self._sell(df, i, regime, "KILL_SWITCH")
                break

            # ── EXIT ──────────────────────────────────────────────────────
            if self.btc > 0:
                trail = BULL_TRAIL if regime == "BULL" else NEUTRAL_TRAIL
                do_sell, reason = exit_signal(
                    df, i, regime,
                    self.entry_price, self.peak_price, trail,
                    bear_confirmed
                )
                if do_sell:
                    self._sell(df, i, regime, reason)

            # ── ENTRY ─────────────────────────────────────────────────────
            elif self.btc == 0:
                # v2: don't enter if BEAR is confirmed
                if not bear_confirmed:
                    do_buy, reason = entry_signal(df, i, regime, self.last_sell_idx)
                    if do_buy:
                        self._buy(df, i, regime, reason)

        # Close any open position at end
        if self.btc > 0:
            self._sell(df, len(df)-1, detect_regime(df, len(df)-1), "END_OF_TEST")

        return self._summary(df)

    def _buy(self, df, idx, regime, reason):
        price      = df["c"].iloc[idx]
        exec_price = price * (1 + self.slippage)
        self.btc   = self.capital / exec_price
        self.capital = 0.0
        self.entry_price = price
        self.peak_price  = price
        self.entry_idx   = idx

        dt = df["datetime"].iloc[idx]
        log(f"BUY  @ ${exec_price:,.2f} | regime={regime} | signal={reason} | "
            f"btc={self.btc:.6f} | bar={dt}")

    def _sell(self, df, idx, regime, reason):
        price        = df["c"].iloc[idx]
        exec_price   = price * (1 - self.slippage)
        proceeds     = self.btc * exec_price
        pnl          = proceeds - (self.btc * self.entry_price * (1 + self.slippage))
        pnl_pct      = pnl / (self.btc * self.entry_price * (1 + self.slippage)) * 100
        hold_bars    = idx - self.entry_idx
        entry_dt     = df["datetime"].iloc[self.entry_idx]
        exit_dt      = df["datetime"].iloc[idx]

        self.trades.append({
            "trade_n":    len(self.trades) + 1,
            "entry_dt":   str(entry_dt),
            "exit_dt":    str(exit_dt),
            "entry_price": round(self.entry_price, 2),
            "exit_price":  round(exec_price, 2),
            "peak_price":  round(self.peak_price, 2),
            "pnl_usd":     round(pnl, 2),
            "pnl_pct":     round(pnl_pct, 3),
            "hold_hours":  hold_bars,
            "regime":      regime,
            "exit_reason": reason,
            "equity_after": round(proceeds, 2),
        })

        self.capital     = proceeds
        self.btc         = 0.0
        self.entry_price = 0.0
        self.peak_price  = 0.0
        self.last_sell_idx = idx

        win = "WIN" if pnl > 0 else "LOSS"
        log(f"SELL @ ${exec_price:,.2f} | {win} ${pnl:+.2f} ({pnl_pct:+.2f}%) | "
            f"held {hold_bars}h | reason={reason}")

    def _summary(self, df) -> dict:
        trades = self.trades
        if not trades:
            log("NO TRADES EXECUTED — strategy produced zero signals", "WARN")
            return {"total_trades": 0}

        pnls       = [t["pnl_usd"] for t in trades]
        wins       = [p for p in pnls if p > 0]
        losses     = [p for p in pnls if p <= 0]
        win_rate   = len(wins) / len(trades) * 100

        equity_vals = [e["equity"] for e in self.equity_curve]
        final_eq    = equity_vals[-1]
        total_ret   = (final_eq - self.start_cap) / self.start_cap * 100

        # Max drawdown from equity curve
        peak = self.start_cap
        max_dd = 0.0
        for e in equity_vals:
            peak   = max(peak, e)
            dd     = (peak - e) / peak
            max_dd = max(max_dd, dd)

        # Sharpe (annualised, assuming 1h bars → 8760 bars/year)
        eq_series  = pd.Series(equity_vals)
        returns    = eq_series.pct_change().dropna()
        sharpe     = 0.0
        if returns.std() > 0:
            sharpe = (returns.mean() / returns.std()) * math.sqrt(8760)

        # Profit factor
        gross_win  = sum(wins)   if wins   else 0
        gross_loss = abs(sum(losses)) if losses else 1e-9
        pf         = gross_win / gross_loss

        # Avg trade duration
        avg_hold   = sum(t["hold_hours"] for t in trades) / len(trades)

        # Regime breakdown
        regime_counts = {}
        for t in trades:
            r = t["regime"]
            regime_counts[r] = regime_counts.get(r, 0) + 1

        summary = {
            "total_trades":   len(trades),
            "win_rate_pct":   round(win_rate, 1),
            "total_return_pct": round(total_ret, 2),
            "final_equity":   round(final_eq, 2),
            "start_equity":   self.start_cap,
            "max_drawdown_pct": round(max_dd * 100, 2),
            "sharpe_ratio":   round(sharpe, 3),
            "profit_factor":  round(pf, 3),
            "avg_hold_hours": round(avg_hold, 1),
            "gross_profit":   round(gross_win, 2),
            "gross_loss":     round(sum(losses), 2),
            "avg_win_usd":    round(sum(wins)/len(wins), 2) if wins else 0,
            "avg_loss_usd":   round(sum(losses)/len(losses), 2) if losses else 0,
            "largest_win":    round(max(pnls), 2),
            "largest_loss":   round(min(pnls), 2),
            "regime_breakdown": regime_counts,
            "trades":         trades,
            "equity_curve":   self.equity_curve,
        }
        return summary


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT / REPORTING
# ─────────────────────────────────────────────────────────────────────────────
def sparkline(values: list, width: int = 60) -> str:
    """ASCII equity curve sparkline."""
    if len(values) < 2:
        return ""
    blocks = " ▁▂▃▄▅▆▇█"
    mn, mx = min(values), max(values)
    rng    = mx - mn or 1
    step   = max(1, len(values) // width)
    sampled = values[::step]
    return "".join(blocks[int((v - mn) / rng * 8)] for v in sampled)

def color(text, c):
    if not HAS_COLOR:
        return text
    return f"{c}{text}{Style.RESET_ALL}"

def print_summary(s: dict):
    if s.get("total_trades", 0) == 0:
        log("─" * 60)
        log("RESULT: ZERO TRADES — strategy needs further adjustment", "WARN")
        log("─" * 60)
        return

    ret_color  = Fore.GREEN if HAS_COLOR and s["total_return_pct"] > 0 else (Fore.RED if HAS_COLOR else "")
    wrate_color= Fore.GREEN if HAS_COLOR and s["win_rate_pct"] >= 50 else (Fore.RED if HAS_COLOR else "")

    print("\n" + "═" * 62)
    print("  BTC_TRADER BACKTEST RESULTS")
    print("═" * 62)

    rows = [
        ["Total trades",       s["total_trades"]],
        ["Win rate",           color(f"{s['win_rate_pct']}%", wrate_color)],
        ["Total return",       color(f"{s['total_return_pct']:+.2f}%", ret_color)],
        ["Start equity",       f"${s['start_equity']:,.2f}"],
        ["Final equity",       f"${s['final_equity']:,.2f}"],
        ["Max drawdown",       f"{s['max_drawdown_pct']:.2f}%"],
        ["Sharpe ratio",       f"{s['sharpe_ratio']:.3f}"],
        ["Profit factor",      f"{s['profit_factor']:.3f}"],
        ["Avg hold (hours)",   f"{s['avg_hold_hours']:.1f}h"],
        ["Gross profit",       f"${s['gross_profit']:,.2f}"],
        ["Gross loss",         f"${s['gross_loss']:,.2f}"],
        ["Avg win",            f"${s['avg_win_usd']:,.2f}"],
        ["Avg loss",           f"${s['avg_loss_usd']:,.2f}"],
        ["Largest win",        f"${s['largest_win']:,.2f}"],
        ["Largest loss",       f"${s['largest_loss']:,.2f}"],
        ["Regime breakdown",   str(s["regime_breakdown"])],
    ]

    if HAS_TABULATE:
        print(tabulate(rows, headers=["Metric", "Value"], tablefmt="simple"))
    else:
        for k, v in rows:
            print(f"  {k:<22} {v}")

    # Equity curve sparkline
    eq_vals = [e["equity"] for e in s["equity_curve"]]
    print(f"\n  Equity curve:")
    print(f"  ${min(eq_vals):,.0f} {sparkline(eq_vals)} ${max(eq_vals):,.0f}")

    # Trade log (last 20)
    print(f"\n  Last trades (showing up to 20 of {s['total_trades']}):")
    trade_rows = []
    for t in s["trades"][-20:]:
        win_str = color(f"+${t['pnl_usd']:.2f}", Fore.GREEN if HAS_COLOR else "") \
                  if t["pnl_usd"] > 0 \
                  else color(f"${t['pnl_usd']:.2f}", Fore.RED if HAS_COLOR else "")
        trade_rows.append([
            t["trade_n"],
            t["entry_dt"][:16],
            t["exit_dt"][:16],
            f"${t['entry_price']:,.0f}",
            f"${t['exit_price']:,.0f}",
            win_str,
            f"{t['pnl_pct']:+.2f}%",
            f"{t['hold_hours']}h",
            t["regime"],
            t["exit_reason"],
        ])

    hdrs = ["#", "Entry", "Exit", "Buy@", "Sell@", "P&L", "%", "Hold", "Regime", "Reason"]
    if HAS_TABULATE:
        print(tabulate(trade_rows, headers=hdrs, tablefmt="simple"))
    else:
        for row in trade_rows:
            print("  " + " | ".join(str(x) for x in row))

    print("═" * 62)

    # Log summary to unified log
    log(f"BACKTEST COMPLETE | trades={s['total_trades']} | "
        f"return={s['total_return_pct']:+.2f}% | "
        f"win_rate={s['win_rate_pct']}% | "
        f"max_dd={s['max_drawdown_pct']}% | "
        f"sharpe={s['sharpe_ratio']} | "
        f"pf={s['profit_factor']}")


def write_trade_csv(s: dict):
    if not s.get("trades"):
        return
    fieldnames = list(s["trades"][0].keys())
    with open(TRADE_CSV, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(s["trades"])
    log(f"Trade log written to {TRADE_CSV}")


def write_equity_csv(s: dict):
    eq_path = BASE_DIR / "backtest_equity_curve.csv"
    if not s.get("equity_curve"):
        return
    fieldnames = list(s["equity_curve"][0].keys())
    with open(eq_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(s["equity_curve"])
    log(f"Equity curve written to {eq_path}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="BTC_trader backtester")
    parser.add_argument("--days",      type=int,   default=90,     help="Days of history (default 90)")
    parser.add_argument("--capital",   type=float, default=2000.0, help="Starting capital USD (default 2000)")
    parser.add_argument("--slippage",  type=float, default=SLIPPAGE, help="Slippage per side (default 0.001)")
    parser.add_argument("--no-fetch",  action="store_true",        help="Use cached data only")
    parser.add_argument("--csv",       action="store_true",        help="Write trade CSV output")
    args = parser.parse_args()

    log("=" * 62)
    log(f"BTC_TRADER BACKTESTER v2 | days={args.days} | "
        f"capital=${args.capital:,.2f} | slippage={args.slippage*100:.1f}bps | "
        f"neutral_trail=2.8% | bear_confirm={BEAR_CONFIRM}bars | atr_filter=ON")
    log("=" * 62)

    # Fetch data
    df = fetch_ohlcv(days=args.days, use_cache=args.no_fetch)

    # Trim to requested days + warmup
    cutoff = df["datetime"].max() - pd.Timedelta(days=args.days + 3)
    df     = df[df["datetime"] >= cutoff].reset_index(drop=True)
    log(f"Testing on {len(df)} candles after trim + warmup")

    # Compute indicators
    df = compute_indicators(df)

    # Run backtest
    bt = Backtester(starting_capital=args.capital, slippage=args.slippage)
    s  = bt.run(df)

    # Print results
    print_summary(s)

    # Write CSVs
    if args.csv or True:   # always write, flag kept for compat
        write_trade_csv(s)
        write_equity_csv(s)

    # Write full summary JSON for machine consumption
    summary_path = BASE_DIR / "backtest_summary.json"
    clean = {k: v for k, v in s.items() if k not in ("trades", "equity_curve")}
    with open(summary_path, "w") as f:
        json.dump(clean, f, indent=2)
    log(f"Summary JSON written to {summary_path}")

    return s


if __name__ == "__main__":
    main()
