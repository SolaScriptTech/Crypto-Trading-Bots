"""
backtest_macd_tiered.py — MACD Histogram Flip + Volume Spike Backtest
═══════════════════════════════════════════════════════════════════════
Assets  : BTC/USD, FARTCOIN/USD
Candles : 1h | Lookback: 90 days
Entry   : MACD(12,26,9) histogram crosses ≤0 → >0 on confirmed close
          + volume spike: bar volume > 1.5× 20-bar rolling avg (prior bars only)
Slippage: 10bps on every entry and exit
Capital : $2,000 per trade

EXIT TIERS
──────────
Tier 1  peak_profit never reached $100
          → hard stop: exit if price ≤ entry × (1 − 5%)
Tier 2  peak_profit has hit $100 or more at any point
          → trailing floor: exit if current_profit < 75% of peak_profit_usd
          → hard stop still active as absolute floor beneath entry

Example: hit $200 peak → floor locks at $150 → you walk away with at least $150
"""

import ccxt
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime, timezone, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
ASSETS        = ["BTC/USD", "FARTCOIN/USD"]
DAYS          = 90
TF            = "1h"
CAPITAL       = 2_000.0    # USD per trade
SLIPPAGE      = 0.0010     # 10bps
HARD_STOP_PCT = 0.05       # 5% from entry (Tier 1 and absolute floor)
TIER2_THRESH  = 100.0      # profit ($) that arms Tier 2
TIER2_FLOOR   = 0.75       # lock in 75% of peak profit once Tier 2 armed
VOL_MULT      = 1.5        # volume spike multiplier vs 20-bar avg
MACD_FAST     = 12
MACD_SLOW     = 26
MACD_SIG      = 9


# ─────────────────────────────────────────────────────────────
# DATA
# ─────────────────────────────────────────────────────────────
def fetch_ohlcv(exchange, symbol, days=90):
    since_ms = int(
        (datetime.now(timezone.utc) - timedelta(days=days + 5)).timestamp() * 1000
    )
    all_bars = []
    while True:
        bars = exchange.fetch_ohlcv(symbol, timeframe=TF, since=since_ms, limit=500)
        if not bars:
            break
        all_bars.extend(bars)
        if len(bars) < 500:
            break
        since_ms = bars[-1][0] + 1

    df = pd.DataFrame(all_bars, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    df.set_index("ts", inplace=True)
    df = df[~df.index.duplicated(keep="last")].sort_index()

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    return df[df.index >= cutoff].copy()


# ─────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────
def add_indicators(df):
    ema_fast    = df["close"].ewm(span=MACD_FAST, adjust=False).mean()
    ema_slow    = df["close"].ewm(span=MACD_SLOW, adjust=False).mean()
    macd_line   = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=MACD_SIG, adjust=False).mean()
    df["hist"]  = macd_line - signal_line

    # Volume spike: compare this bar's volume to average of PREVIOUS 20 bars
    df["vol_avg20"] = df["volume"].shift(1).rolling(20).mean()
    df["vol_spike"] = df["volume"] > (VOL_MULT * df["vol_avg20"])
    return df


# ─────────────────────────────────────────────────────────────
# BACKTEST CORE
# ─────────────────────────────────────────────────────────────
def run_backtest(df, symbol):
    trades      = []
    equity_rows = []

    in_trade         = False
    entry_price      = 0.0
    entry_time       = None
    entry_bar_idx    = 0
    peak_profit_usd  = 0.0
    tier2_armed      = False

    equity      = CAPITAL
    peak_equity = CAPITAL
    max_dd_pct  = 0.0

    warmup = MACD_SLOW + MACD_SIG + 20   # need enough bars for indicators

    for i in range(warmup, len(df)):
        bar      = df.iloc[i]
        prev_bar = df.iloc[i - 1]
        price    = bar["close"]

        # ── ENTRY ────────────────────────────────────────────
        if not in_trade:
            hist_flip = (bar["hist"] > 0) and (prev_bar["hist"] <= 0)
            vol_ok    = bool(bar["vol_spike"])

            if hist_flip and vol_ok:
                entry_price     = price * (1 + SLIPPAGE)
                entry_time      = bar.name
                entry_bar_idx   = i
                peak_profit_usd = 0.0
                tier2_armed     = False
                in_trade        = True

        # ── EXIT ─────────────────────────────────────────────
        else:
            unrealized_usd = (price - entry_price) / entry_price * CAPITAL

            # Track peak profit and arm Tier 2
            if unrealized_usd > peak_profit_usd:
                peak_profit_usd = unrealized_usd
            if peak_profit_usd >= TIER2_THRESH:
                tier2_armed = True

            exit_reason = None
            exit_price  = None

            # Hard stop — always active (5% below entry)
            hard_stop_price = entry_price * (1 - HARD_STOP_PCT)
            if price <= hard_stop_price:
                exit_reason = "HARD_STOP_5PCT"
                exit_price  = price * (1 - SLIPPAGE)

            # Tier 2 trailing floor (only when armed, and not already stopped)
            elif tier2_armed:
                floor_usd   = TIER2_FLOOR * peak_profit_usd      # 75% of peak profit
                floor_price = entry_price * (1 + floor_usd / CAPITAL)
                if price <= floor_price:
                    exit_reason = "TRAIL_FLOOR_T2"
                    exit_price  = price * (1 - SLIPPAGE)

            # Force-close on last bar
            if exit_reason is None and i == len(df) - 1:
                exit_reason = "END_OF_DATA"
                exit_price  = price * (1 - SLIPPAGE)

            if exit_reason:
                pnl_usd = (exit_price - entry_price) / entry_price * CAPITAL
                equity += pnl_usd
                peak_equity = max(peak_equity, equity)
                dd = (peak_equity - equity) / peak_equity * 100
                max_dd_pct = max(max_dd_pct, dd)

                trades.append({
                    "symbol"         : symbol,
                    "entry_time"     : entry_time.isoformat(),
                    "exit_time"      : bar.name.isoformat(),
                    "entry_price"    : round(entry_price, 6),
                    "exit_price"     : round(exit_price, 6),
                    "pnl_usd"        : round(pnl_usd, 2),
                    "pnl_pct"        : round(pnl_usd / CAPITAL * 100, 3),
                    "peak_profit_usd": round(peak_profit_usd, 2),
                    "tier"           : 2 if tier2_armed else 1,
                    "reason"         : exit_reason,
                    "bars_held"      : i - entry_bar_idx,
                })

                in_trade        = False
                peak_profit_usd = 0.0
                tier2_armed     = False

        equity_rows.append({"ts": bar.name.isoformat(), "equity": round(equity, 2)})

    return trades, equity_rows, max_dd_pct


# ─────────────────────────────────────────────────────────────
# SUMMARY STATS
# ─────────────────────────────────────────────────────────────
def summarize(trades, max_dd_pct):
    if not trades:
        return {"trades": 0, "note": "no signals fired"}

    pnls   = [t["pnl_usd"] for t in trades]
    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    gross_profit  = sum(wins)   if wins   else 0.0
    gross_loss    = abs(sum(losses)) if losses else 0.0
    profit_factor = round(gross_profit / gross_loss, 3) if gross_loss > 0 else float("inf")

    # Annualised Sharpe from per-trade returns (scaled by sqrt of trade frequency)
    if len(pnls) > 1:
        r     = np.array(pnls) / CAPITAL
        avg_r = r.mean()
        std_r = r.std(ddof=1)
        # assume ~8760 hours/year; avg bars_held gives approximate trades/year
        avg_bars = np.mean([t["bars_held"] for t in trades])
        trades_per_year = 8760 / avg_bars if avg_bars > 0 else 1
        sharpe = round((avg_r / std_r) * np.sqrt(trades_per_year), 3) if std_r > 0 else 0.0
    else:
        sharpe = 0.0

    total_pnl = sum(pnls)
    return {
        "trades"          : len(trades),
        "wins"            : len(wins),
        "losses"          : len(losses),
        "win_rate_pct"    : round(len(wins) / len(trades) * 100, 1),
        "total_pnl_usd"   : round(total_pnl, 2),
        "total_return_pct": round(total_pnl / CAPITAL * 100, 2),
        "avg_win_usd"     : round(np.mean(wins),   2) if wins   else 0.0,
        "avg_loss_usd"    : round(np.mean(losses),  2) if losses else 0.0,
        "largest_win_usd" : round(max(wins),         2) if wins   else 0.0,
        "largest_loss_usd": round(min(losses),        2) if losses else 0.0,
        "profit_factor"   : profit_factor,
        "sharpe_ratio"    : sharpe,
        "max_drawdown_pct": round(max_dd_pct, 2),
        "tier1_exits"     : sum(1 for t in trades if t["tier"] == 1),
        "tier2_exits"     : sum(1 for t in trades if t["tier"] == 2),
    }


# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    exchange = ccxt.kraken({"enableRateLimit": True})
    all_results = {}

    for symbol in ASSETS:
        sep = "═" * 64
        print(f"\n{sep}")
        print(f"  {symbol}  |  1h  |  90d  |  MACD({MACD_FAST},{MACD_SLOW},{MACD_SIG}) Flip + {VOL_MULT}× Vol Spike")
        print(sep)

        try:
            print("  Fetching OHLCV from Kraken...", flush=True)
            df = fetch_ohlcv(exchange, symbol, days=DAYS)
            print(f"  {len(df)} bars  ({df.index[0].strftime('%Y-%m-%d')} → {df.index[-1].strftime('%Y-%m-%d')})")

            df = add_indicators(df)
            trades, equity_rows, max_dd = run_backtest(df, symbol)
            summary = summarize(trades, max_dd)

            all_results[symbol] = {"summary": summary, "trades": trades}

            # ── Print summary ────────────────────────────────
            print(f"\n  {'SUMMARY':─<56}")
            for k, v in summary.items():
                print(f"  {k:<25}  {v}")

            # ── Print trade log ──────────────────────────────
            if trades:
                hdr = f"  {'#':<4} {'Entry Time':<18} {'Exit Time':<18} {'Bars':>5} {'PnL ($)':>10} {'Peak ($)':>9} {'Tier':>5}  Reason"
                print(f"\n  {'TRADE LOG':─<56}")
                print(hdr)
                print(f"  {'─'*90}")
                for n, t in enumerate(trades, 1):
                    en = t["entry_time"][:16].replace("T", " ")
                    ex = t["exit_time"][:16].replace("T", " ")
                    print(
                        f"  {n:<4} {en:<18} {ex:<18} {t['bars_held']:>5}"
                        f"  {t['pnl_usd']:>9.2f}  {t['peak_profit_usd']:>8.2f}"
                        f"  {'T'+str(t['tier']):>4}  {t['reason']}"
                    )

            # ── Save CSVs ────────────────────────────────────
            safe = symbol.replace("/", "_")
            trades_path = os.path.join(BASE_DIR, f"backtest_{safe}_trades.csv")
            equity_path = os.path.join(BASE_DIR, f"backtest_{safe}_equity.csv")
            pd.DataFrame(trades).to_csv(trades_path, index=False)
            pd.DataFrame(equity_rows).to_csv(equity_path, index=False)
            print(f"\n  Trades → {trades_path}")
            print(f"  Equity → {equity_path}")

        except Exception as e:
            print(f"  ERROR: {e}")
            all_results[symbol] = {"error": str(e)}

    # ── Save combined JSON ───────────────────────────────────
    json_path = os.path.join(BASE_DIR, "backtest_macd_tiered_summary.json")
    with open(json_path, "w") as f:
        json.dump(all_results, f, indent=2, default=str)
    print(f"\n  Summary JSON → {json_path}")
    print(f"\n{'═'*64}")
    print("  BACKTEST COMPLETE")
    print(f"{'═'*64}\n")


if __name__ == "__main__":
    main()
