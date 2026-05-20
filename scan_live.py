"""Scan many symbols, record current TRADE signals to a JSON file.

Logic mirrors bot2.main: both 15m and 5m must say TRADE on the same side.
Each signal is anchored by the close time of the latest 15m candle, which is
what later grading needs — we can pull history-since-that-time and see whether
the limit filled and then hit TP or SL.

Usage:
    python scan_live.py
    python scan_live.py --target 10 --symbols BTC/USD ETH/USD SOL/USD
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from candles import get_candles, SYMBOL_MAP
from bot2 import analyze, build_recommendation_combined, score_signal, format_score

DEFAULT_SYMBOLS = list(SYMBOL_MAP.keys())  # all 14 mapped pairs


def scan_one(symbol, n15, n5, strategy):
    c15 = get_candles(symbol, "15m", n15)
    c5 = get_candles(symbol, "5m", n5)
    a15 = analyze(c15, "15m")
    a5 = analyze(c5, "5m")
    recs15 = build_recommendation_combined(a15, strategy)
    recs5 = build_recommendation_combined(a5, strategy)

    hits = []
    for r15, r5 in zip(recs15, recs5):
        if (r15["verdict"] == "TRADE" and r5["verdict"] == "TRADE"
                and r15["side"] == r5["side"]):
            # Only score signals that actually fire on both TFs
            score = score_signal(r15, r5, a15, a5)
            hits.append({"strategy": r15.get("strategy", "fvg"),
                         "rec_15m": r15, "rec_5m": r5,
                         "score": score})
    return {
        "symbol": symbol,
        "anchor_time_unix": c15[-1]["time"],
        "anchor_time_iso": datetime.fromtimestamp(c15[-1]["time"], tz=timezone.utc).isoformat(),
        "current_price": c15[-1]["close"],
        "hits": hits,
        "verdicts_15m": [(r.get("strategy", "fvg"), r["verdict"]) for r in recs15],
        "verdicts_5m": [(r.get("strategy", "fvg"), r["verdict"]) for r in recs5],
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbols", nargs="+", default=DEFAULT_SYMBOLS)
    ap.add_argument("--target", type=int, default=10, help="stop once this many agreeing signals are found")
    ap.add_argument("--candles-15m", type=int, default=288)
    ap.add_argument("--candles-5m", type=int, default=720)
    ap.add_argument("--out", default=None, help="output JSON path (default signals_<ts>.json)")
    ap.add_argument("--strategy", choices=["fvg", "trendline", "both"], default="both")
    args = ap.parse_args()

    signals = []
    skipped = []

    for sym in args.symbols:
        if len(signals) >= args.target:
            break
        print(f"scanning {sym}...", end=" ", flush=True)
        try:
            r = scan_one(sym, args.candles_15m, args.candles_5m, args.strategy)
        except Exception as e:
            print(f"ERROR ({e})")
            skipped.append({"symbol": sym, "error": str(e)})
            continue

        if r["hits"]:
            for h in r["hits"]:
                rec = h["rec_15m"]
                sc = h["score"]
                print(f"  [{h['strategy'].upper()}] TRADE {rec['side']}  "
                      f"entry={rec['entry']:.6f}  SL={rec['stop_loss']:.6f}  "
                      f"TP={rec['take_profit']:.6f}  (now {r['current_price']:.6f})  "
                      f"score={sc['weighted_score']}/{sc['max_score']} {sc['verdict']}")
                # Full breakdown block for explainability
                print()
                print(format_score(sc))
                print()
                signals.append({**r, "strategy": h["strategy"],
                                "rec_15m": h["rec_15m"], "rec_5m": h["rec_5m"],
                                "score": sc})
            print()
        else:
            print(f"no signal  15m={r['verdicts_15m']}  5m={r['verdicts_5m']}")

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_path = Path(args.out) if args.out else Path(f"signals_{ts}.json")
    payload = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "target": args.target,
        "found": len(signals),
        "signals": signals,
        "skipped": skipped,
    }
    out_path.write_text(json.dumps(payload, indent=2, default=str))

    print("\n" + "=" * 70)
    print(f"FOUND {len(signals)} live trade signals (target was {args.target})")
    print("=" * 70)
    for s in signals:
        rec = s["rec_15m"]
        strat = s.get("strategy", "fvg").upper()
        sc = s.get("score", {})
        score_str = f"{sc.get('weighted_score', 0)}/{sc.get('max_score', 100)} {sc.get('verdict', '?')}"
        print(f"  [{strat:9s}] {s['symbol']:12s} {rec['side']:4s}  "
              f"entry {rec['entry']:.6f}  SL {rec['stop_loss']:.6f}  "
              f"TP {rec['take_profit']:.6f}  score {score_str:<14}  @ anchor {s['anchor_time_iso']}")
    print(f"\nrecorded to: {out_path.resolve()}")
    print("Grade later with: python grade_signals.py " + str(out_path))


if __name__ == "__main__":
    sys.exit(main() or 0)