"""Grade a signals_*.json produced by scan_live.py.

For each recorded signal, pull 15m candles from the anchor time forward and
apply the same fill/SL/TP simulation as backtest.py:
  - Limit fills if a bar's wick crosses entry within FILL_EXPIRY_BARS.
  - After fill, the first bar to touch SL or TP closes the trade
    (same-bar tie -> SL, conservative).

Usage:
    python grade_signals.py signals_20260512T120000Z.json
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import request as urlrequest
from urllib.parse import urlencode

from candles import _kraken_pair, INTERVAL_MIN, UA

FILL_EXPIRY_BARS = 20


def fetch_15m_since(symbol, since_unix):
    """Pull 15m candles whose start time is > since_unix."""
    pair = _kraken_pair(symbol)
    url = "https://api.kraken.com/0/public/OHLC?" + urlencode({
        "pair": pair, "interval": 15, "since": since_unix,
    })
    req = urlrequest.Request(url, headers={"User-Agent": UA})
    with urlrequest.urlopen(req, timeout=15) as resp:
        data = json.loads(resp.read())
    if data.get("error"):
        raise RuntimeError(str(data["error"]))
    result = data["result"]
    key = next(k for k in result if k != "last")
    rows = result[key]
    return [{"time": int(r[0]), "open": float(r[1]), "high": float(r[2]),
             "low": float(r[3]), "close": float(r[4])} for r in rows]


def simulate(side, entry, sl, tp, candles):
    n = len(candles)
    filled_at = None
    for j in range(min(n, FILL_EXPIRY_BARS)):
        c = candles[j]
        if c["low"] <= entry <= c["high"]:
            filled_at = j
            # check same-bar exit
            if side == "BUY":
                if c["low"] <= sl:
                    return _result("filled+sl", j, sl, side, entry, sl)
                if c["high"] >= tp:
                    return _result("filled+tp", j, tp, side, entry, sl)
            else:
                if c["high"] >= sl:
                    return _result("filled+sl", j, sl, side, entry, sl)
                if c["low"] <= tp:
                    return _result("filled+tp", j, tp, side, entry, sl)
            break

    if filled_at is None:
        return {"outcome": "expired", "exit_index": None, "exit_price": None,
                "r_multiple": None, "bars_to_exit": None}

    for j in range(filled_at + 1, n):
        c = candles[j]
        if side == "BUY":
            hit_sl = c["low"] <= sl
            hit_tp = c["high"] >= tp
        else:
            hit_sl = c["high"] >= sl
            hit_tp = c["low"] <= tp
        if hit_sl:
            return _result("filled+sl", j, sl, side, entry, sl, filled_at)
        if hit_tp:
            return _result("filled+tp", j, tp, side, entry, sl, filled_at)
    return {"outcome": "still_open", "exit_index": None, "exit_price": None,
            "r_multiple": None, "bars_to_exit": None, "filled_at": filled_at}


def _result(outcome, exit_idx, exit_price, side, entry, sl, filled_at=None):
    if side == "BUY":
        risk = entry - sl
        pnl = exit_price - entry
    else:
        risk = sl - entry
        pnl = entry - exit_price
    r = pnl / risk if risk else 0.0
    return {"outcome": outcome, "exit_index": exit_idx, "exit_price": exit_price,
            "r_multiple": r, "filled_at": filled_at if filled_at is not None else exit_idx}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("signals_file")
    args = ap.parse_args()

    payload = json.loads(Path(args.signals_file).read_text())
    signals = payload["signals"]
    graded = []
    total_r = 0.0
    wins = losses = expired = open_ = 0

    for s in signals:
        sym = s["symbol"]
        rec = s["rec_15m"]
        anchor = s["anchor_time_unix"]
        print(f"grading {sym} {rec['side']}  (anchor {s['anchor_time_iso']})...", end=" ", flush=True)
        try:
            candles = fetch_15m_since(sym, anchor)
        except Exception as e:
            print(f"FETCH ERROR ({e})")
            continue
        # drop the anchor candle itself if present
        candles = [c for c in candles if c["time"] > anchor]
        if not candles:
            print("no bars yet since anchor — try again later")
            continue

        res = simulate(rec["side"], rec["entry"], rec["stop_loss"], rec["take_profit"], candles)
        out = res["outcome"]
        r = res["r_multiple"]
        if out == "filled+tp":
            wins += 1; total_r += r
        elif out == "filled+sl":
            losses += 1; total_r += r
        elif out == "expired":
            expired += 1
        else:
            open_ += 1
        print(f"{out}  R={r:.2f}" if r is not None else f"{out}")
        graded.append({"symbol": sym, "side": rec["side"], "entry": rec["entry"],
                       "sl": rec["stop_loss"], "tp": rec["take_profit"],
                       "bars_available": len(candles), **res})
        time.sleep(0.5)  # be nice to Kraken

    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    filled = wins + losses
    print(f"  signals      : {len(signals)}")
    print(f"  filled       : {filled}")
    print(f"  wins / losses: {wins} / {losses}")
    print(f"  expired      : {expired}")
    print(f"  still open   : {open_}")
    print(f"  total R      : {total_r:.2f}")
    if filled:
        print(f"  win rate     : {wins/filled:.1%}")
        print(f"  avg R/trade  : {total_r/filled:.2f}")

    out_path = Path(args.signals_file).with_suffix(".graded.json")
    out_path.write_text(json.dumps({"generated_utc": datetime.now(timezone.utc).isoformat(),
                                    "results": graded}, indent=2, default=str))
    print(f"\nwrote grades to {out_path}")


if __name__ == "__main__":
    sys.exit(main() or 0)
