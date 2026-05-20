"""Rebuild signal entry/SL/TP using the edge-FVG rule.

Takes one or more signals_*.json files produced by scan_live.py (old mid-FVG
logic) and rewrites entry/SL/TP for each signal using the new edge-FVG rule:
  - BUY  entry = FVG high, SL = FVG low
  - SELL entry = FVG low,  SL = FVG high
  - risk = full FVG height, TP = entry +/- 4*risk

Writes a new file alongside each input: <name>.edge.json

Usage:
    python rebuild_signals_edge.py signals_20260512T152052Z.json [more.json ...]
    python grade_signals.py signals_20260512T152052Z.edge.json
"""
import json
import sys
from pathlib import Path


def rebuild_rec(rec):
    fvg = rec.get("fvg")
    if not fvg:
        return rec
    side = rec["side"]
    if side == "BUY":
        entry = fvg["high"]
        sl = fvg["low"]
        risk = entry - sl
        tp = entry + 4 * risk
    else:
        entry = fvg["low"]
        sl = fvg["high"]
        risk = sl - entry
        tp = entry - 4 * risk
    new = dict(rec)
    new["entry"] = entry
    new["stop_loss"] = sl
    new["take_profit"] = tp
    new["risk_per_unit"] = risk
    new["entry_rule"] = "edge_fvg"
    return new


def rebuild_file(path: Path):
    payload = json.loads(path.read_text())
    for s in payload["signals"]:
        if "rec_15m" in s and s["rec_15m"].get("verdict") == "TRADE":
            s["rec_15m"] = rebuild_rec(s["rec_15m"])
        if "rec_5m" in s and s["rec_5m"].get("verdict") == "TRADE":
            s["rec_5m"] = rebuild_rec(s["rec_5m"])
    out = path.with_suffix(".edge.json")
    out.write_text(json.dumps(payload, indent=2, default=str))
    print(f"  wrote {out.name}")
    for s in payload["signals"]:
        r = s.get("rec_15m", {})
        if r.get("verdict") == "TRADE":
            print(f"    {s['symbol']:12s} {r['side']:4s}  "
                  f"entry {r['entry']:.6f}  SL {r['stop_loss']:.6f}  "
                  f"TP {r['take_profit']:.6f}")
    return out


def main():
    if len(sys.argv) < 2:
        print("usage: python rebuild_signals_edge.py <signals.json> [more ...]")
        return 1
    for arg in sys.argv[1:]:
        path = Path(arg)
        print(f"rebuilding {path.name}...")
        rebuild_file(path)
    return 0


if __name__ == "__main__":
    sys.exit(main())
