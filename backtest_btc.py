#!/usr/bin/env python3
"""
backtest_btc.py — btc_trader pitch-deck strategy backtester
Strategy: BB_LOWER / EMA21_PULLBACK / SMA20_TOUCH mean-reversion on BTC/USD 1h
Matches btc_trader.py running in tmux:pitch_deck on EC2 eu-west-1.
Outputs mandatory audit trail (timestamp, equity, drawdown, signal_price, exec_price, delay_ms).
"""

import os
import csv
import json
import math
import time
from collections import Counter
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import ccxt
from dotenv import load_dotenv

load_dotenv()

# ─── CONFIG ───────────────────────────────────────────────────────────────────
SYMBOL          = "BTC/USD"
TIMEFRAME       = "15m"
STARTING_EQUITY = 5_000.0   # set to match your observed starting balance
LOOKBACK_DAYS   = 90        # 90-day window to match pitch-deck track record
POSITION_SIZE   = 0.25      # 25% of equity per trade
SLIPPAGE_BPS    = 10        # 10 bps = 0.10% per side

HARD_STOP_PCT   = 0.035     # 3.5% hard floor
TARGET_PCT      = 0.025     # 2.5% take-profit
NEUTRAL_TRAIL   = 0.028     # 2.8% trail in NEUTRAL (matches btc_trader)
BULL_TRAIL      = 0.020     # 2.0% trail in BULL
BEAR_CONFIRM    = 2         # consecutive bars required to confirm BEAR

# Tiered trailing stops (tighten as profit grows)
TRAIL_TIERS = [
    (0.012, 0.003),   # gain > 1.2% → trail 0.30% (lock in profit)
    (0.007, 0.005),   # gain > 0.7% → trail 0.50%
    (0.003, 0.008),   # gain > 0.3% → trail 0.80%
    (0.000, 0.013),   # gain > 0.0% → trail 1.30%
]
PROFIT_FLOOR_GATE = 0.003   # once gain >= 0.3%, stop never below entry × 1.001

OUTPUT_AUDIT    = "backtest_audit_trail.csv"
OUTPUT_EQUITY   = "backtest_equity_curve.csv"
OUTPUT_SUMMARY  = "backtest_summary.json"


# ─── EXCHANGE ─────────────────────────────────────────────────────────────────
def make_exchange():
    return ccxt.kraken({
        "apiKey": os.getenv("KRAKEN_API_KEY", ""),
        "secret": os.getenv("KRAKEN_API_SECRET", ""),
        "enableRateLimit": True,
    })


# ─── FETCH OHLCV ──────────────────────────────────────────────────────────────
def fetch_ohlcv(exchange, symbol, timeframe, days):
    since_ms = exchange.parse8601(
        (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT00:00:00Z")
    )
    all_candles = []
    print(f"  Fetching candles", end="", flush=True)
    while True:
        batch = exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=720)
        if not batch:
            break
        all_candles.extend(batch)
        print(".", end="", flush=True)
        if len(batch) < 720:
            break
        since_ms = batch[-1][0] + 1
        time.sleep(1.5)  # respect rate limit: 1.5s spacing
    print()

    df = pd.DataFrame(all_candles, columns=["ts_ms", "open", "high", "low", "close", "volume"])
    df = df.drop_duplicates("ts_ms").sort_values("ts_ms").reset_index(drop=True)
    df["timestamp"] = pd.to_datetime(df["ts_ms"], unit="ms", utc=True)
    return df


# ─── INDICATORS ───────────────────────────────────────────────────────────────
def compute_indicators(df):
    c = df["close"]

    # Bollinger Bands (20, 2)
    sma20          = c.rolling(20).mean()
    std20          = c.rolling(20).std()
    df["bb_upper"] = sma20 + 2 * std20
    df["bb_lower"] = sma20 - 2 * std20
    df["bb_mid"]   = sma20
    df["sma20"]    = sma20

    # EMAs for regime and entry
    df["ema21"] = c.ewm(span=21, adjust=False).mean()
    df["ema55"] = c.ewm(span=55, adjust=False).mean()

    # ATR(14) for reference in summary
    h  = df["high"]
    lo = df["low"]
    tr = pd.concat(
        [h - lo, (h - c.shift()).abs(), (lo - c.shift()).abs()], axis=1
    ).max(axis=1)
    df["atr"] = tr.rolling(14).mean()

    return df


# ─── REGIME DETECTION ─────────────────────────────────────────────────────────
def compute_regimes(df):
    regimes    = []
    bear_count = 0
    for _, r in df.iterrows():
        if r["ema21"] > r["ema55"] and r["close"] > r["ema21"]:
            bear_count = 0
            regimes.append("BULL")
        elif r["ema21"] < r["ema55"]:
            bear_count += 1
            # BEAR requires BEAR_CONFIRM consecutive bars
            regimes.append("BEAR" if bear_count >= BEAR_CONFIRM else "NEUTRAL")
        else:
            bear_count = 0
            regimes.append("NEUTRAL")
    return regimes


# ─── ENTRY SIGNAL ─────────────────────────────────────────────────────────────
def entry_signal(r):
    if r["regime"] == "BEAR":
        return None

    c   = r["close"]
    e21 = r["ema21"]
    s20 = r["sma20"]
    bbl = r["bb_lower"]

    pullback = (c - e21) / e21

    if c <= bbl:
        return "BB_LOWER"
    if 0.0 <= pullback <= 0.0075:
        return "EMA21_PULLBACK"
    if abs(c - s20) / s20 < 0.002:
        return "SMA20_TOUCH"
    return None


# ─── TRAIL PERCENTAGE (tiered, gain-adaptive) ─────────────────────────────────
def get_trail_pct(entry_px, peak_px, regime):
    gain = (peak_px - entry_px) / entry_px
    # Base tier by regime
    base = BULL_TRAIL if regime == "BULL" else NEUTRAL_TRAIL
    # Tighten based on unrealised gain
    for threshold, pct in TRAIL_TIERS:
        if gain >= threshold:
            return min(base, pct)
    return base


# ─── BACKTEST ENGINE ──────────────────────────────────────────────────────────
def run_backtest(df):
    warmup = 55  # bars needed for EMA55 to stabilise
    df = df.iloc[warmup:].reset_index(drop=True)
    df["regime"] = compute_regimes(df)

    equity       = STARTING_EQUITY
    peak_equity  = equity
    position     = None
    trades       = []
    equity_curve = [{"timestamp": df.iloc[0]["timestamp"].isoformat(), "equity": equity}]

    for i in range(1, len(df) - 1):
        # Always evaluate penultimate confirmed candle — no look-ahead
        r  = df.iloc[i]
        ts = r["timestamp"].isoformat()

        if position is None:
            sig = entry_signal(r)
            if sig:
                buy_px = r["close"] * (1 + SLIPPAGE_BPS / 10_000)
                size   = equity * POSITION_SIZE
                position = {
                    "entry_px"   : buy_px,
                    "peak_px"    : buy_px,
                    "entry_ts"   : ts,
                    "entry_ms"   : int(r["ts_ms"]),
                    "signal"     : sig,
                    "size_usd"   : size,
                    "regime"     : r["regime"],
                    "hard_stop"  : buy_px * (1 - HARD_STOP_PCT),
                    "target_px"  : buy_px * (1 + TARGET_PCT),
                    "trail_stop" : buy_px * (1 - (BULL_TRAIL if r["regime"] == "BULL" else NEUTRAL_TRAIL)),
                    "floor_px"   : None,  # set once profit_floor_gate is crossed
                }
        else:
            pos       = position
            cur_price = r["close"]

            # Ratchet peak and trail stop upward only
            if cur_price > pos["peak_px"]:
                pos["peak_px"] = cur_price
                gain    = (cur_price - pos["entry_px"]) / pos["entry_px"]
                trail_p = get_trail_pct(pos["entry_px"], cur_price, r["regime"])
                new_stop = cur_price * (1 - trail_p)
                pos["trail_stop"] = max(pos["trail_stop"], new_stop)

                # Set profit floor once gain >= PROFIT_FLOOR_GATE
                if gain >= PROFIT_FLOOR_GATE and pos["floor_px"] is None:
                    pos["floor_px"] = pos["entry_px"] * 1.001

            # Apply profit floor
            if pos["floor_px"] is not None:
                pos["trail_stop"] = max(pos["trail_stop"], pos["floor_px"])

            exit_reason = None
            if cur_price <= pos["hard_stop"]:
                exit_reason = "SELL_STOP"
            elif r["regime"] == "BEAR" and pos["regime"] != "BEAR":
                exit_reason = "SELL_REGIME_FLIP"
            elif cur_price <= pos["trail_stop"]:
                exit_reason = "SELL_TRAIL"
            elif cur_price >= pos["target_px"]:
                exit_reason = "SELL_TARGET"

            if exit_reason:
                sell_px    = cur_price * (1 - SLIPPAGE_BPS / 10_000)
                pnl_pct    = (sell_px - pos["entry_px"]) / pos["entry_px"]
                pnl_usd    = pos["size_usd"] * pnl_pct
                equity    += pnl_usd
                peak_equity = max(peak_equity, equity)
                drawdown   = (peak_equity - equity) / peak_equity
                delay_ms   = int(r["ts_ms"]) - pos["entry_ms"]

                row = {
                    # Mandatory audit trail columns
                    "timestamp"   : ts,
                    "equity"      : round(equity, 4),
                    "drawdown"    : round(drawdown, 6),
                    "signal_price": round(r["close"], 2),
                    "exec_price"  : round(sell_px, 2),
                    "delay_ms"    : delay_ms,
                    # Extended columns for analysis
                    "entry_price" : round(pos["entry_px"], 2),
                    "entry_signal": pos["signal"],
                    "exit_reason" : exit_reason,
                    "pnl_usd"     : round(pnl_usd, 4),
                    "pnl_pct"     : round(pnl_pct * 100, 4),
                    "entry_time"  : pos["entry_ts"],
                    "exit_time"   : ts,
                    "regime"      : r["regime"],
                    "atr"         : round(r["atr"], 2),
                }
                trades.append(row)
                equity_curve.append({"timestamp": ts, "equity": round(equity, 4)})
                position = None

    return trades, equity_curve


# ─── METRICS ──────────────────────────────────────────────────────────────────
def compute_metrics(trades):
    if not trades:
        return {"error": "no trades executed"}

    pnls    = [t["pnl_usd"] for t in trades]
    winners = [p for p in pnls if p > 0]
    losers  = [p for p in pnls if p < 0]

    win_rate      = len(winners) / len(pnls) * 100
    profit_factor = sum(winners) / abs(sum(losers)) if losers else 9999.0
    final_equity  = trades[-1]["equity"]
    total_return  = (final_equity - STARTING_EQUITY) / STARTING_EQUITY * 100
    max_drawdown  = max(t["drawdown"] for t in trades) * 100
    avg_win       = float(np.mean(winners)) if winners else 0.0
    avg_loss      = float(np.mean(losers)) if losers else 0.0
    expectancy    = (win_rate / 100 * avg_win) + ((1 - win_rate / 100) * avg_loss)

    # Sharpe: annualise daily grouped P&L
    df_t = pd.DataFrame(trades)
    df_t["date"] = pd.to_datetime(df_t["timestamp"]).dt.date
    daily = df_t.groupby("date")["pnl_usd"].sum()
    sharpe = float(daily.mean() / daily.std() * math.sqrt(365)) if daily.std() > 0 else 0.0

    exit_breakdown = dict(Counter(t["exit_reason"] for t in trades))
    signal_breakdown = dict(Counter(t["entry_signal"] for t in trades))

    return {
        "total_trades"      : len(trades),
        "win_count"         : len(winners),
        "loss_count"        : len(losers),
        "win_rate_pct"      : round(win_rate, 2),
        "profit_factor"     : round(profit_factor, 3),
        "sharpe_ratio"      : round(sharpe, 3),
        "expectancy_usd"    : round(expectancy, 4),
        "total_return_pct"  : round(total_return, 2),
        "total_pnl_usd"     : round(sum(pnls), 2),
        "avg_win_usd"       : round(avg_win, 2),
        "avg_loss_usd"      : round(avg_loss, 2),
        "max_drawdown_pct"  : round(max_drawdown, 2),
        "starting_equity"   : STARTING_EQUITY,
        "final_equity"      : round(final_equity, 2),
        "exit_breakdown"    : exit_breakdown,
        "signal_breakdown"  : signal_breakdown,
        "kill_switch_breach": bool(max_drawdown >= 15.0),
    }


# ─── FILE WRITERS ─────────────────────────────────────────────────────────────
def write_csv(rows, path):
    if not rows:
        print(f"  No rows to write → {path}")
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"  Written: {path} ({len(rows)} rows)")


def write_summary(metrics, path):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(metrics, f, indent=2)
    os.replace(tmp, path)  # atomic write — safe across power cuts / reboots
    print(f"  Written: {path}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────
def main():
    print("=" * 58)
    print(f"  backtest_btc.py — BTC/USD 15m mean-reversion")
    print(f"  Symbol:   {SYMBOL}   Timeframe: {TIMEFRAME}")
    print(f"  Capital:  ${STARTING_EQUITY:,.2f}   Lookback: {LOOKBACK_DAYS}d")
    print("=" * 58)

    exchange = make_exchange()
    exchange.load_markets()

    df = fetch_ohlcv(exchange, SYMBOL, TIMEFRAME, LOOKBACK_DAYS)
    df = compute_indicators(df)
    print(f"  Candles:  {len(df)} "
          f"({df.iloc[0]['timestamp'].date()} → {df.iloc[-1]['timestamp'].date()})")

    print("\nRunning backtest...")
    trades, equity_curve = run_backtest(df)
    metrics = compute_metrics(trades)

    print("\n" + "─" * 58)
    print(f"  Trades:              {metrics.get('total_trades', 0)}")
    print(f"  Win Rate:            {metrics.get('win_rate_pct', 0):.1f}%")
    print(f"  Profit Factor:       {metrics.get('profit_factor', 0):.3f}")
    print(f"  Sharpe (ann.):       {metrics.get('sharpe_ratio', 0):.3f}")
    print(f"  Expectancy:          ${metrics.get('expectancy_usd', 0):.2f}/trade")
    print(f"  Total Return:        {metrics.get('total_return_pct', 0):.2f}%")
    print(f"  Total P&L:           ${metrics.get('total_pnl_usd', 0):,.2f}")
    print(f"  Max Drawdown:        {metrics.get('max_drawdown_pct', 0):.2f}%")
    print(f"  Final Equity:        ${metrics.get('final_equity', 0):,.2f}")
    print(f"  Kill-switch breach:  {metrics.get('kill_switch_breach', False)}")
    print("─" * 58)
    print("  Exit breakdown:  ", metrics.get("exit_breakdown", {}))
    print("  Signal breakdown:", metrics.get("signal_breakdown", {}))
    print("─" * 58)

    print("\nWriting outputs...")
    write_csv(trades, OUTPUT_AUDIT)
    write_csv(equity_curve, OUTPUT_EQUITY)
    write_summary(metrics, OUTPUT_SUMMARY)
    print("Done.")


if __name__ == "__main__":
    main()
