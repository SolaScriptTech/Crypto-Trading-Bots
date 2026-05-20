"""market_open_scanner.py — Ranks coins by Bot 2 signal frequency at NY market open.

For each of the 43 mapped coins, simulates what Bot 2 would have seen at
9:00 AM ET on each of the past N days (default 3). Ranks coins by how often
all conditions are met during the 9am-11am ET window.

Logic:
  - 9am ET = 13:00 UTC (DST, Apr-Nov) / 14:00 UTC (EST, Nov-Mar)
  - Pulls 720 x 15m and 720 x 5m candles per coin (Kraken max)
  - For each target day, slices candles to snapshot at 9am ET
  - Runs Bot 2 analysis on that snapshot (same logic as bot2.py)
  - Records verdict, type, direction for each day
  - Ranks by signal count descending

Usage:
    python market_open_scanner.py
    python market_open_scanner.py --days 3 --strategy both --out open_scan.csv
"""

import argparse
import csv
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from candles import get_candles, SYMBOL_MAP
from bot2 import analyze, build_recommendation_combined

HERE = Path(__file__).parent

# 9am-1pm ET window in UTC. May = DST → ET is UTC-4 → 9am ET = 13:00 UTC
# Using 1pm ET (17:00 UTC) as the close to catch signals that develop mid-morning
OPEN_HOUR_UTC  = 13   # 9am ET during DST (Apr-Nov)
CLOSE_HOUR_UTC = 17   # 1pm ET during DST (captures full morning session)
MIN_CANDLES    = 50   # minimum candles needed to run analysis meaningfully


def open_window_utc(date: datetime) -> tuple[int, int]:
    """Return (open_ts, close_ts) Unix timestamps for the 9am-11am ET window on date."""
    open_dt  = date.replace(hour=OPEN_HOUR_UTC,  minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    close_dt = date.replace(hour=CLOSE_HOUR_UTC, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    return int(open_dt.timestamp()), int(close_dt.timestamp())


def slice_candles(candles: list[dict], up_to_ts: int) -> list[dict]:
    """Return all candles with time < up_to_ts (i.e. what was visible at that moment)."""
    return [c for c in candles if c["time"] < up_to_ts]


def window_candles(candles: list[dict], from_ts: int, to_ts: int) -> list[dict]:
    """Return candles that opened within [from_ts, to_ts)."""
    return [c for c in candles if from_ts <= c["time"] < to_ts]


def scan_coin(symbol: str, target_days: list[datetime], strategy: str,
              debug: bool = False) -> list[dict]:
    """Run Bot 2 on each target day's 9am ET snapshot. Returns list of day results."""
    try:
        c15 = get_candles(symbol, "15m", 720)
        time.sleep(0.3)  # polite Kraken rate limit
        c5  = get_candles(symbol, "5m",  720)
        time.sleep(0.3)
    except Exception as e:
        print(f"  {symbol}: fetch error — {e}")
        return []

    results = []
    for day in target_days:
        open_ts, close_ts = open_window_utc(day)

        snap15 = slice_candles(c15, open_ts)
        snap5  = slice_candles(c5,  open_ts)

        if len(snap15) < MIN_CANDLES or len(snap5) < MIN_CANDLES:
            results.append({
                "symbol": symbol, "date": day.strftime("%Y-%m-%d"),
                "verdict": "INSUFFICIENT DATA", "type": "", "direction": "",
                "strategy": "", "reasons_for": 0, "reasons_against": 0,
            })
            continue

        try:
            a15 = analyze(snap15, "15m")
            a5  = analyze(snap5,  "5m")
        except Exception as e:
            results.append({
                "symbol": symbol, "date": day.strftime("%Y-%m-%d"),
                "verdict": f"ANALYSIS ERROR: {e}", "type": "", "direction": "",
                "strategy": "", "reasons_for": 0, "reasons_against": 0,
            })
            continue

        recs15 = build_recommendation_combined(a15, strategy)
        recs5  = build_recommendation_combined(a5,  strategy)

        if debug:
            print(f"\n  --- {symbol} {day.strftime('%Y-%m-%d')} ---")
            print(f"  15m trend={a15['trend']} swings={a15['swing_count']} "
                  f"waves_before_choc={a15['wave_count_before_last_choc']['wave_count']} "
                  f"waves_since={a15['wave_count_since_last_choc']['wave_count']} "
                  f"tl_touches={a15['trendline']['touches']} "
                  f"fvg_unchall={a15['fvg_unchallenged']} "
                  f"post_choc_bos={'yes' if a15.get('post_choc_structure') and a15['post_choc_structure'].get('first_bos_after') else 'no'}")
            print(f"   5m trend={a5['trend']} swings={a5['swing_count']} "
                  f"waves_before_choc={a5['wave_count_before_last_choc']['wave_count']} "
                  f"waves_since={a5['wave_count_since_last_choc']['wave_count']} "
                  f"tl_touches={a5['trendline']['touches']} "
                  f"fvg_unchall={a5['fvg_unchallenged']} "
                  f"post_choc_bos={'yes' if a5.get('post_choc_structure') and a5['post_choc_structure'].get('first_bos_after') else 'no'}")
            for r in recs15:
                strat = r.get("strategy","?")
                if r["verdict"] == "TRADE":
                    print(f"  15m [{strat}] TRADE {r.get('side','')}  for={r.get('reasons_for',[])}  against={r.get('reasons_against',[])}")
                else:
                    print(f"  15m [{strat}] NO TRADE  against={r.get('reasons_against', [r.get('reason','?')])}")
            for r in recs5:
                strat = r.get("strategy","?")
                if r["verdict"] == "TRADE":
                    print(f"   5m [{strat}] TRADE {r.get('side','')}  for={r.get('reasons_for',[])}  against={r.get('reasons_against',[])}")
                else:
                    print(f"   5m [{strat}] NO TRADE  against={r.get('reasons_against', [r.get('reason','?')])}")

        # Check for any TAKE TRADE on both timeframes agreeing
        fired = False
        best_rec = None
        for r15, r5 in zip(recs15, recs5):
            if r15["verdict"] == "TRADE" and r5["verdict"] == "TRADE":
                if r15.get("side") == r5.get("side"):
                    fired    = True
                    best_rec = r15
                    break

        # Even if no dual-TF agree, record the strongest single-TF signal
        if not fired:
            for r15 in recs15:
                if r15["verdict"] == "TRADE":
                    best_rec = r15
                    break
            if not best_rec:
                best_rec = recs15[0] if recs15 else {}

        strat_name = best_rec.get("strategy", "")
        # Type by waves-since-CHOC: <=2 = Reversion (catching the flip),
        # >=3 = Momentum (riding the new trend)
        waves_since = best_rec.get("waves_since_choc", 0)
        strat_type = "Reversion" if waves_since <= 2 else "Momentum"
        if not strat_name:
            strat_type = ""

        results.append({
            "symbol":          symbol,
            "date":            day.strftime("%Y-%m-%d"),
            "verdict":         "TAKE TRADE" if fired else "NO TRADE",
            "single_tf_fire":  best_rec.get("verdict") == "TRADE" and not fired,
            "type":            strat_type,
            "direction":       best_rec.get("side", ""),
            "strategy":        strat_name,
            "reasons_for":     len(best_rec.get("reasons_for", [])),
            "reasons_against": len(best_rec.get("reasons_against", [])),
            "trend_15m":       a15.get("trend", ""),
            "trend_5m":        a5.get("trend", ""),
        })

    return results


def rank_results(all_results: list[dict], days: int) -> list[dict]:
    """Aggregate per-day rows into per-coin summary, sorted by signal count."""
    from collections import defaultdict
    coins = defaultdict(lambda: {
        "signals": 0, "single_tf": 0, "days_checked": 0,
        "directions": [], "types": [], "trends_15m": [],
    })

    for r in all_results:
        sym = r["symbol"]
        coins[sym]["days_checked"] += 1
        if r["verdict"] == "TAKE TRADE":
            coins[sym]["signals"] += 1
            coins[sym]["directions"].append(r["direction"])
            coins[sym]["types"].append(r["type"])
        elif r.get("single_tf_fire"):
            coins[sym]["single_tf"] += 1
        if r.get("trend_15m"):
            coins[sym]["trends_15m"].append(r["trend_15m"])

    summary = []
    for sym, d in coins.items():
        sig = d["signals"]
        chk = d["days_checked"]
        dirs  = d["directions"]
        types = d["types"]
        summary.append({
            "symbol":       sym,
            "signals":      sig,
            "days_checked": chk,
            "pct":          f"{sig/chk*100:.0f}%" if chk else "0%",
            "single_tf":    d["single_tf"],
            "direction":    _most_common(dirs) if dirs else "",
            "type":         _most_common(types) if types else "",
            "consistent_dir": len(set(dirs)) == 1 and len(dirs) > 0,
        })

    summary.sort(key=lambda x: (x["signals"], x["single_tf"]), reverse=True)
    return summary


def _most_common(lst: list) -> str:
    if not lst:
        return ""
    return max(set(lst), key=lst.count)


def print_summary(summary: list[dict], days: int):
    print()
    print("=" * 72)
    print(f"MARKET OPEN SCANNER — 9am-11am ET — past {days} days")
    print("=" * 72)
    print(f"{'Symbol':<14} {'Signals':>7} {'SingleTF':>8} {'Direction':<10} {'Type':<20} {'Consistent'}")
    print("-" * 72)
    for r in summary:
        if r["signals"] == 0 and r["single_tf"] == 0:
            continue  # skip coins with zero activity
        consistent = "✓ SAME DIR" if r["consistent_dir"] else ""
        print(
            f"{r['symbol']:<14} "
            f"{r['signals']}/{r['days_checked']:>1} days{' ':>2} "
            f"{r['single_tf']:>4} 1TF   "
            f"{r['direction']:<10} "
            f"{r['type']:<20} "
            f"{consistent}"
        )
    print()
    print("Signals   = both 15m+5m agreed (TAKE TRADE)")
    print("1TF       = one timeframe said TAKE TRADE but not both")
    print("Consistent= same direction every day it fired")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days",     type=int, default=3,    help="How many past days to check (default 3)")
    ap.add_argument("--strategy", default="both",         choices=["fvg","trendline","both"])
    ap.add_argument("--out",      default=None,           help="Optional CSV output path")
    ap.add_argument("--symbols",  nargs="*", default=None, help="Subset of symbols (default: all 43)")
    ap.add_argument("--debug",    action="store_true",     help="Print per-day failure reasons for each coin")
    args = ap.parse_args()

    symbols = args.symbols if args.symbols else list(SYMBOL_MAP.keys())

    # Build target days: include today if past 1pm ET (17:00 UTC), else skip today
    now_utc   = datetime.now(timezone.utc)
    today_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
    include_today = now_utc.hour >= CLOSE_HOUR_UTC
    if include_today:
        target_days = [today_utc - timedelta(days=i) for i in range(args.days)]
    else:
        target_days = [today_utc - timedelta(days=i+1) for i in range(args.days)]
    target_days.reverse()  # oldest first

    print(f"Scanning {len(symbols)} coins across {args.days} days "
          f"({target_days[0].strftime('%b %d')} → {target_days[-1].strftime('%b %d')})...")
    print(f"Strategy: {args.strategy} | Window: 9am-11am ET (13:00-15:00 UTC)")
    print()

    all_results = []
    for i, symbol in enumerate(symbols, 1):
        print(f"[{i:02d}/{len(symbols)}] {symbol}...", end=" ", flush=True)
        rows = scan_coin(symbol, target_days, args.strategy, debug=args.debug)
        fired = sum(1 for r in rows if r["verdict"] == "TAKE TRADE")
        print(f"{fired}/{len(rows)} signals")
        all_results.extend(rows)

    summary = rank_results(all_results, args.days)
    print_summary(summary, args.days)

    if args.out:
        path = Path(args.out)
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=[
                "symbol","signals","days_checked","pct","single_tf",
                "direction","type","consistent_dir"
            ])
            writer.writeheader()
            writer.writerows(summary)
        print(f"Results saved to {path}")


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except KeyboardInterrupt:
        print("\naborted.")
        sys.exit(130)