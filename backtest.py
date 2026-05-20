"""
backtest.py — Prop Bot System Offline Backtester
=================================================

Runs the full signal + exit stack against historical OHLCV data
to validate that parameters meet prop firm requirements before going live.

Usage:
    python3 backtest.py --days 90 --equity 10000

Output:
    - backtest_results.json   — full trade log
    - backtest_summary.txt    — human-readable report
"""

import argparse
import json
import time
from datetime import datetime, timezone, timedelta
from typing import Optional
import pandas as pd
import pandas_ta as ta

import config as C
from prop_bot_system import (
    compute_indicators,
    detect_regime,
    Position,
    update_trailing_stop,
    check_exit,
    score_long,
    score_short,
    near_swing_level,
    ALL_SIGNAL_FUNCTIONS,
    tiered_trail_pct,
)
from risk_manager import PropRiskManager


# ─────────────────────────────────────────────────────────────────────────────
# BACKTEST ENGINE
# ─────────────────────────────────────────────────────────────────────────────

class Backtester:
    def __init__(self, starting_equity: float):
        self.starting_equity  = starting_equity
        self.equity           = starting_equity
        self.risk             = PropRiskManager(starting_equity)
        self.positions: dict  = {}
        self.cooldowns: dict  = {}
        self.trades: list     = []
        self.equity_curve: list = []

    def run(self, data: dict[str, pd.DataFrame]) -> dict:
        """
        data: symbol → full OHLCV DataFrame with indicators computed.
        Simulates bar-by-bar from bar 60 onward.
        Uses fee of 0.26% per side (Kraken maker/taker blended).
        """
        FEE_PCT = 0.0026  # 0.26% per side

        # Align all series to the same timestamps
        btc_df = data.get(C.REGIME_ANCHOR)
        if btc_df is None:
            raise ValueError(f"Regime anchor {C.REGIME_ANCHOR} not in data")

        timestamps = btc_df["timestamp"].tolist()
        warmup     = 60  # bars needed for indicators

        print(f"\nBacktest: {len(timestamps) - warmup} bars | "
              f"starting_equity=${starting_equity:,.0f}")
        print("─" * 55)

        for i in range(warmup, len(timestamps)):
            ts_ms = timestamps[i]

            # Slice each symbol's data to [0:i+1] (simulate real-time)
            slices = {sym: df.iloc[:i+1].copy() for sym, df in data.items()}

            market_regime = detect_regime(slices[C.REGIME_ANCHOR]) \
                if len(slices[C.REGIME_ANCHOR]) >= 60 else None
            if not market_regime:
                continue

            # Update equity estimate (mark-to-market open positions)
            mtm = self._mark_to_market(slices)
            self.risk.update_equity(mtm)
            self.equity_curve.append({
                "ts": ts_ms,
                "equity": round(mtm, 2),
                "drawdown": round(self.risk._total_drawdown_pct() * 100, 3),
            })

            can_trade, halt_reason = self.risk.can_trade()

            # ── EXIT CHECK ───────────────────────────────────────────────────
            for sym in list(self.positions.keys()):
                if sym not in slices or len(slices[sym]) < 60:
                    continue
                pos = self.positions[sym]
                df  = slices[sym]
                current_price = df.iloc[-1]["close"]
                asset_regime  = detect_regime(df)

                pos = update_trailing_stop(pos, current_price)
                pos.bars_held += 1

                exit_reason = check_exit(pos, current_price, market_regime,
                                         asset_regime, df, ts_ms)
                if exit_reason:
                    self._close(sym, pos, current_price, exit_reason, ts_ms, FEE_PCT)

            # ── ENTRY CHECK ──────────────────────────────────────────────────
            if can_trade and len(self.positions) < C.MAX_POSITIONS:
                for sym in C.TRADE_PAIRS:
                    if sym in self.positions:
                        continue
                    if self.cooldowns.get(sym, 0) > ts_ms:
                        continue
                    if sym not in slices or len(slices[sym]) < 60:
                        continue

                    df           = slices[sym]
                    asset_regime = detect_regime(df)

                    if asset_regime.label == "BEAR" and market_regime.label != "BEAR":
                        continue

                    signal = None
                    for fn in ALL_SIGNAL_FUNCTIONS:
                        r = fn(df, market_regime if sym == C.REGIME_ANCHOR else asset_regime)
                        if r:
                            signal = r
                            break

                    if not signal:
                        continue
                    if signal["side"] == "short" and not C.ENABLE_BEAR_SHORTS:
                        continue

                    # Sizing
                    deployable = mtm * (1 - C.DRY_POWDER_PCT)
                    used       = sum(p.size_usd for p in self.positions.values())
                    available  = deployable - used
                    size_pct   = C.SIZE_HIGH_PCT if signal["conviction"] >= 65 else C.SIZE_LOW_PCT
                    raw_size   = min(mtm * size_pct, available)
                    size_usd   = self.risk.size_trade(raw_size)

                    if size_usd < 10:
                        continue

                    entry_price = df.iloc[-1]["close"] * (1 + FEE_PCT)  # slippage model
                    qty         = size_usd / entry_price

                    hard_stop = self.risk.stop_price_from_risk(entry_price, signal["side"], size_usd)
                    if signal["side"] == "long":
                        hard_stop = max(hard_stop, entry_price * (1 - C.HARD_STOP_PCT))
                    else:
                        hard_stop = min(hard_stop, entry_price * (1 + C.SHORT_HARD_STOP_PCT))

                    pos = Position(
                        symbol        = sym,
                        strategy      = signal["strategy"],
                        side          = signal["side"],
                        entry_price   = entry_price,
                        size_usd      = size_usd,
                        qty           = qty,
                        entry_time_ms = ts_ms,
                        peak_price    = entry_price,
                        stop_price    = hard_stop,
                        hard_stop     = hard_stop,
                        conviction    = signal["conviction"],
                    )
                    self.positions[sym] = pos

        # Close remaining positions at last price
        for sym in list(self.positions.keys()):
            if sym in data:
                price = data[sym].iloc[-1]["close"]
                self._close(sym, self.positions[sym], price, "END_OF_BACKTEST",
                            timestamps[-1], FEE_PCT)

        return self._build_report()

    def _close(self, sym, pos, price, reason, ts_ms, fee_pct):
        exit_price = price * (1 - fee_pct) if pos.side == "long" else price * (1 + fee_pct)
        if pos.side == "long":
            pnl = (exit_price - pos.entry_price) * pos.qty
        else:
            pnl = (pos.entry_price - exit_price) * pos.qty

        pnl_pct = pnl / pos.size_usd
        self.equity += pnl
        is_win = pnl > 0
        self.risk.record_trade(pnl, is_win)

        self.cooldowns[sym] = ts_ms + C.COOLDOWN_MS
        self.positions.pop(sym, None)

        self.trades.append({
            "ts":        datetime.utcfromtimestamp(ts_ms / 1000).isoformat(),
            "symbol":    sym,
            "strategy":  pos.strategy,
            "side":      pos.side,
            "entry":     round(pos.entry_price, 4),
            "exit":      round(exit_price, 4),
            "size_usd":  round(pos.size_usd, 2),
            "pnl_usd":   round(pnl, 2),
            "pnl_pct":   round(pnl_pct * 100, 3),
            "bars":      pos.bars_held,
            "reason":    reason,
            "conviction": pos.conviction,
            "ever_green": pos.ever_green,
        })

    def _mark_to_market(self, slices) -> float:
        """Current equity = cash + open position value."""
        mtm = self.equity
        for sym, pos in self.positions.items():
            if sym in slices and len(slices[sym]) > 0:
                price = slices[sym].iloc[-1]["close"]
                if pos.side == "long":
                    mtm += (price - pos.entry_price) * pos.qty
                else:
                    mtm += (pos.entry_price - price) * pos.qty
        return mtm

    def _build_report(self) -> dict:
        total_trades = len(self.trades)
        wins         = sum(1 for t in self.trades if t["pnl_usd"] > 0)
        total_pnl    = sum(t["pnl_usd"] for t in self.trades)
        win_rate     = wins / total_trades * 100 if total_trades else 0

        pnl_pct      = total_pnl / self.starting_equity * 100
        max_dd       = min((e["drawdown"] for e in self.equity_curve), default=0)

        by_strategy  = {}
        for t in self.trades:
            s = t["strategy"]
            by_strategy.setdefault(s, {"trades": 0, "wins": 0, "pnl": 0})
            by_strategy[s]["trades"] += 1
            by_strategy[s]["wins"]   += int(t["pnl_usd"] > 0)
            by_strategy[s]["pnl"]    += t["pnl_usd"]

        exit_reasons = {}
        for t in self.trades:
            exit_reasons[t["reason"]] = exit_reasons.get(t["reason"], 0) + 1

        prop_pass = (
            3.0 <= pnl_pct <= 7.0 and abs(max_dd) <= 15.0
        )

        return {
            "summary": {
                "starting_equity":  self.starting_equity,
                "ending_equity":    round(self.equity, 2),
                "total_pnl_usd":    round(total_pnl, 2),
                "total_pnl_pct":    round(pnl_pct, 3),
                "max_drawdown_pct": round(max_dd, 3),
                "total_trades":     total_trades,
                "win_rate_pct":     round(win_rate, 1),
                "prop_challenge_pass": prop_pass,
                "prop_target_3_7":  f"{'✅ PASS' if 3 <= pnl_pct <= 7 else '❌ FAIL'} ({pnl_pct:.2f}%)",
                "prop_dd_15":       f"{'✅ PASS' if abs(max_dd) <= 15 else '❌ FAIL'} ({abs(max_dd):.2f}%)",
            },
            "by_strategy": by_strategy,
            "exit_reasons": exit_reasons,
            "trades":        self.trades,
            "equity_curve":  self.equity_curve[-500:],  # last 500 bars for charting
        }


# ─────────────────────────────────────────────────────────────────────────────
# DATA LOADER (uses CCXT REST — no live creds needed for public data)
# ─────────────────────────────────────────────────────────────────────────────

async def load_historical_data(days: int) -> dict[str, pd.DataFrame]:
    """Load OHLCV history for all pairs from Kraken (public endpoint)."""
    import ccxt.async_support as ccxt_async

    exchange = ccxt_async.kraken()
    data = {}
    limit  = min(days * 24, 720)  # 1h candles, max 720 per call on Kraken
    pairs  = list(set(C.TRADE_PAIRS + [C.REGIME_ANCHOR]))

    print(f"Loading {days} days of 1h data ({limit} candles per pair)…")
    for sym in pairs:
        try:
            candles = await exchange.fetch_ohlcv(sym, "1h", limit=limit)
            df = pd.DataFrame(candles, columns=["timestamp", "open", "high", "low", "close", "volume"])
            df = compute_indicators(df)
            data[sym] = df
            print(f"  {sym}: {len(df)} candles loaded")
            await asyncio.sleep(1.0)
        except Exception as e:
            print(f"  {sym}: FAILED — {e}")

    await exchange.close()
    return data


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

import asyncio

async def _run_backtest(days: int, equity: float):
    data    = await load_historical_data(days)
    tester  = Backtester(equity)
    results = tester.run(data)

    # Save JSON
    with open("backtest_results.json", "w") as f:
        json.dump(results, f, indent=2)

    # Print summary
    s = results["summary"]
    print("\n" + "═" * 55)
    print("  PROP FIRM BACKTEST RESULTS")
    print("═" * 55)
    print(f"  Starting equity:   ${s['starting_equity']:,.2f}")
    print(f"  Ending equity:     ${s['ending_equity']:,.2f}")
    print(f"  Total PnL:         ${s['total_pnl_usd']:+,.2f} ({s['total_pnl_pct']:+.2f}%)")
    print(f"  Max drawdown:      {abs(s['max_drawdown_pct']):.2f}%")
    print(f"  Total trades:      {s['total_trades']}")
    print(f"  Win rate:          {s['win_rate_pct']:.1f}%")
    print(f"\n  Prop target (3–7%): {s['prop_target_3_7']}")
    print(f"  Prop DD (<15%):     {s['prop_dd_15']}")
    print(f"\n  Overall: {'✅ PROP CHALLENGE PASS' if s['prop_challenge_pass'] else '❌ NEEDS CALIBRATION'}")
    print("═" * 55)

    print("\nBy strategy:")
    for name, st in results["by_strategy"].items():
        wr = st["wins"] / st["trades"] * 100 if st["trades"] else 0
        print(f"  {name:<20} {st['trades']:3d} trades | "
              f"WR {wr:.0f}% | PnL ${st['pnl']:+.2f}")

    print("\nExit reasons:")
    for reason, count in sorted(results["exit_reasons"].items(),
                                 key=lambda x: -x[1]):
        print(f"  {reason:<25} {count:3d}")

    print(f"\nFull results saved to backtest_results.json")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Prop Bot Backtester")
    parser.add_argument("--days",   type=int,   default=90,     help="Days of history")
    parser.add_argument("--equity", type=float, default=10000,  help="Starting equity")
    args = parser.parse_args()

    starting_equity = args.equity
    asyncio.run(_run_backtest(args.days, args.equity))
