"""exit_scout.py — Exit target finder for open DXtrade positions.

Fetches your open positions, lets you pick one, then scans 15m and 5m
candles for unchallenged FVGs ahead of the trade (in the profit direction).
Those FVGs are ranked by proximity and shown as candidate exit levels.

Usage:
    python exit_scout.py
    python exit_scout.py --tf-high 15m --tf-low 5m
"""
import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

from dxtrade_client import DXtradeClient, load_env
from candles import get_candles
from bot2 import detect_fvgs

HERE = Path(__file__).parent


def fetch_positions(client) -> list[dict]:
    return client.positions()


def current_price_from_candles(candles: list[dict]) -> float:
    return candles[-1]["close"]


def fvgs_ahead(fvgs: list[dict], side: str, current_price: float) -> list[dict]:
    """
    Return unchallenged FVGs that price is heading toward.
    SELL (short): price moving DOWN → bullish FVGs below current price are the draw.
    BUY  (long):  price moving UP   → bearish FVGs above current price are the draw.
    """
    targets = []
    for fvg in fvgs:
        if fvg["challenged"]:
            continue
        mid = (fvg["low"] + fvg["high"]) / 2
        if side == "SELL" and fvg["type"] == "bullish" and mid < current_price:
            dist_pct = (current_price - mid) / current_price * 100
            targets.append({**fvg, "mid": mid, "dist_pct": dist_pct})
        elif side == "BUY" and fvg["type"] == "bearish" and mid > current_price:
            dist_pct = (mid - current_price) / current_price * 100
            targets.append({**fvg, "mid": mid, "dist_pct": dist_pct})
    targets.sort(key=lambda x: x["dist_pct"])
    return targets


def r_multiple(target_price: float, entry: float, sl: float) -> float:
    risk = abs(entry - sl)
    if risk == 0:
        return 0.0
    reward = abs(target_price - entry)
    return reward / risk


def print_position_list(positions: list[dict], metrics: dict):
    equity = float(metrics.get("equity", 0))
    print()
    print("=" * 60)
    print("OPEN POSITIONS")
    print("=" * 60)
    for i, p in enumerate(positions, 1):
        sym    = p["symbol"]
        side   = p["side"]
        entry  = float(p["openPrice"])
        qty    = float(p["quantity"])
        sl     = float(p.get("stopLossPrice") or 0)
        tp     = float(p.get("takeProfitPrice") or 0)
        opened = p["openTime"][:16].replace("T", " ")
        sl_str = f"SL={sl}" if sl else "NO SL"
        tp_str = f"TP={tp}" if tp else "no TP"
        print(f"  [{i}] {sym:12} {side:5}  entry={entry}  {sl_str}  {tp_str}  opened={opened}")
    print()


def analyze_position(pos: dict, tf_high: str, tf_low: str):
    sym    = pos["symbol"]
    side   = pos["side"]
    entry  = float(pos["openPrice"])
    sl     = float(pos.get("stopLossPrice") or 0)
    tp     = float(pos.get("takeProfitPrice") or 0)
    qty    = float(pos["quantity"])

    kraken_sym = sym  # already in X/USD format from DXtrade

    print(f"\nFetching candles for {sym}...")
    try:
        c_high = get_candles(kraken_sym, tf_high, 720)
        c_low  = get_candles(kraken_sym, tf_low,  720)
    except Exception as e:
        print(f"  ERROR fetching candles: {e}")
        return

    price_high = current_price_from_candles(c_high)
    price_low  = current_price_from_candles(c_low)
    current    = price_high

    fvgs_h = detect_fvgs(c_high)
    fvgs_l = detect_fvgs(c_low)

    targets_h = fvgs_ahead(fvgs_h, side, current)
    targets_l = fvgs_ahead(fvgs_l, side, current)

    # Current P&L estimate
    if side == "SELL":
        open_pl = (entry - current) * qty
        direction_word = "DOWN"
        looking_for = "bullish FVGs below price (draws in liquidity)"
    else:
        open_pl = (current - entry) * qty
        direction_word = "UP"
        looking_for = "bearish FVGs above price (draws in liquidity)"

    risk_per_unit = abs(entry - sl) if sl else None

    print()
    print("=" * 60)
    print(f"EXIT SCOUT — {sym}  ({side})")
    print("=" * 60)
    print(f"Entry:         {entry}")
    print(f"Current price: {current}  (direction: {direction_word})")
    if sl:
        print(f"Stop Loss:     {sl}  (risk/unit: {abs(entry - sl):.6f})")
    if tp:
        print(f"Current TP:    {tp}")
    print(f"Est. open P&L: ${open_pl:+,.2f}")
    print(f"Qty:           {qty:,.0f}")
    print(f"Scanning for:  {looking_for}")
    print()

    def print_targets(targets: list[dict], tf_label: str):
        print(f"--- {tf_label} unchallenged FVGs ahead ({len(targets)} found) ---")
        if not targets:
            print("  None found — path may be clear to TP, or no data in range.")
            print()
            return
        for i, t in enumerate(targets, 1):
            mid      = t["mid"]
            dist_pct = t["dist_pct"]
            fvg_type = t["type"]
            low      = t["low"]
            high     = t["high"]
            height   = high - low

            if risk_per_unit and risk_per_unit > 0:
                r = r_multiple(mid, entry, sl)
                r_str = f"  {r:.2f}R"
            else:
                r_str = "  (no SL set)"

            warn = ""
            if i == 1:
                warn = "  ← NEAREST — likely first rejection zone"

            print(f"  #{i}  {fvg_type:8} FVG  range={low:.6f}..{high:.6f}  mid={mid:.6f}"
                  f"  height={height:.6f}  dist={dist_pct:.2f}%{r_str}{warn}")
        print()

    print_targets(targets_h, tf_high)
    print_targets(targets_l, tf_low)

    # Summary recommendation
    all_targets = sorted(targets_h + targets_l, key=lambda x: x["dist_pct"])
    if all_targets:
        nearest = all_targets[0]
        nearest_mid = nearest["mid"]
        if risk_per_unit and risk_per_unit > 0:
            r_at_nearest = r_multiple(nearest_mid, entry, sl)
            print(f"NEAREST EXIT CANDIDATE: {nearest_mid:.6f}  ({nearest['dist_pct']:.2f}% away,  {r_at_nearest:.2f}R)")
        else:
            print(f"NEAREST EXIT CANDIDATE: {nearest_mid:.6f}  ({nearest['dist_pct']:.2f}% away)")

        if tp and risk_per_unit:
            r_at_tp = r_multiple(tp, entry, sl)
            blocked = [t for t in all_targets if (
                (side == "SELL" and t["mid"] > nearest_mid and t["mid"] < tp) or
                (side == "BUY"  and t["mid"] < nearest_mid and t["mid"] > tp)
            )]
            print(f"Current TP at {tp}: {r_at_tp:.2f}R")
            fvgs_between = [t for t in all_targets if (
                (side == "SELL" and nearest_mid >= t["mid"] >= tp) or
                (side == "BUY"  and nearest_mid <= t["mid"] <= tp)
            )]
            if len(fvgs_between) > 1:
                print(f"WARNING: {len(fvgs_between)} FVGs between price and TP — "
                      f"consider taking profit at {nearest_mid:.6f} instead.")
            else:
                print("Path to current TP looks relatively clear.")
    else:
        print("No unchallenged FVGs found ahead of position on either timeframe.")
        print("Path to TP may be clear — or candle history doesn't cover the range.")

    print()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tf-high", default="15m")
    ap.add_argument("--tf-low",  default="5m")
    args = ap.parse_args()

    env    = load_env(HERE / ".env")
    client = DXtradeClient(env)
    client.login()

    metrics   = client.metrics()
    positions = fetch_positions(client)

    if not positions:
        print("No open positions.")
        return 0

    print_position_list(positions, metrics)

    while True:
        raw = input("Enter position number to scout (or q to quit): ").strip()
        if raw.lower() in ("q", "quit", "exit"):
            break
        if not raw.isdigit():
            print("  Enter a number.")
            continue
        idx = int(raw) - 1
        if idx < 0 or idx >= len(positions):
            print(f"  Pick a number between 1 and {len(positions)}.")
            continue

        analyze_position(positions[idx], args.tf_high, args.tf_low)

        again = input("Check another position? (y/n): ").strip().lower()
        if again != "y":
            break

    return 0


if __name__ == "__main__":
    sys.exit(main())
