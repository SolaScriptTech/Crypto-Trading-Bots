"""replay.py — Simulate what the agentic loop would have done at a past moment,
then immediately grade it against what actually happened.

Two phases:
  Phase 1 — REPLAY   : slice candles to the target snapshot time, run full
                        three-routine analysis (research / signal / journal)
                        exactly as agent_loop.py would have, record every signal.
  Phase 2 — GRADE    : pull price history from anchor forward, simulate
                        fill / SL / TP, report outcome in R.

Default target: 9:00 AM ET today (13:00 UTC during DST).

Usage:
    python replay.py                          # 9am ET today, all 64 symbols
    python replay.py --date 2026-05-18        # specific date
    python replay.py --hour-utc 15            # different open hour (e.g. 11am ET = 15:00 UTC)
    python replay.py --symbols BTC/USD ETH/USD SOL/USD
    python replay.py --strategy fvg
    python replay.py --no-grade               # skip grading (phase 1 only)
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from candles import get_candles, SYMBOL_MAP, _kraken_pair, UA, INTERVAL_MIN
from bot2 import analyze, build_recommendation_combined
from grade_signals import simulate, fetch_15m_since
from urllib import request as urlrequest
from urllib.parse import urlencode

HERE = Path(__file__).parent

OPEN_HOUR_UTC  = 13   # 9am ET during DST (Apr–Nov)
MIN_CANDLES    = 50
FILL_EXPIRY_BARS = 20


# ── helpers ──────────────────────────────────────────────────────────────────

def _get_json(url: str) -> dict | None:
    from urllib import request as urlrequest
    req = urlrequest.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urlrequest.urlopen(req, timeout=10) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None


def fetch_fear_greed() -> dict:
    d = _get_json("https://api.alternative.me/fng/?limit=1")
    if d and d.get("data"):
        v = d["data"][0]
        return {"value": int(v["value"]), "classification": v["value_classification"]}
    return {"value": -1, "classification": "unavailable"}


def fetch_btc_dominance() -> float:
    d = _get_json("https://api.coingecko.com/api/v3/global")
    if d and "data" in d:
        return round(d["data"].get("market_cap_percentage", {}).get("btc", 0.0), 2)
    return 0.0


def slice_to(candles: list[dict], up_to_ts: int) -> list[dict]:
    """Return only candles whose open time is strictly before up_to_ts."""
    return [c for c in candles if c["time"] < up_to_ts]


def fetch_candles_full(symbol: str, tf: str, count: int = 720) -> list[dict]:
    return get_candles(symbol, tf, count)


# ── Phase 1: Replay ───────────────────────────────────────────────────────────

def replay_symbol(
    symbol:    str,
    snap_ts:   int,
    tf_high:   str,
    tf_low:    str,
    strategy:  str,
) -> dict | None:
    """
    Returns a signal dict if both TFs agree, else None.
    Also returns 'no_trade_reasons' for the hold log.
    """
    try:
        c_high_full = fetch_candles_full(symbol, tf_high, 720)
        time.sleep(0.25)
        c_low_full  = fetch_candles_full(symbol, tf_low,  720)
        time.sleep(0.25)
    except Exception as e:
        return {"symbol": symbol, "error": str(e)}

    c_high = slice_to(c_high_full, snap_ts)
    c_low  = slice_to(c_low_full,  snap_ts)

    if len(c_high) < MIN_CANDLES or len(c_low) < MIN_CANDLES:
        return {"symbol": symbol, "error": "insufficient candles at snapshot time"}

    try:
        a_high = analyze(c_high, tf_high)
        a_low  = analyze(c_low,  tf_low)
    except Exception as e:
        return {"symbol": symbol, "error": f"analysis failed: {e}"}

    recs_high = build_recommendation_combined(a_high, strategy)
    recs_low  = build_recommendation_combined(a_low,  strategy)

    all_reasons = []
    for r_h, r_l in zip(recs_high, recs_low):
        if (r_h.get("verdict") == "TRADE"
                and r_l.get("verdict") == "TRADE"
                and r_h.get("side") == r_l.get("side")):
            anchor_candle = c_high[-1]
            return {
                "symbol":       symbol,
                "side":         r_h["side"].upper(),
                "strategy":     r_h.get("strategy", "?"),
                "entry":        r_h.get("entry"),
                "stop_loss":    r_h.get("sl"),
                "take_profit":  r_h.get("tp"),
                "sl_distance":  r_h.get("sl_distance"),
                "reasons_for":  r_h.get("reasons_for", []),
                "anchor_time_unix": anchor_candle["time"],
                "anchor_time_iso":  datetime.fromtimestamp(
                    anchor_candle["time"], tz=timezone.utc).isoformat(),
                "snapshot_price": c_high[-1]["close"],
                "tf_high": tf_high,
                "tf_low":  tf_low,
                "verdict": "TRADE",
            }
        all_reasons.extend(r_h.get("reasons_against", []))
        all_reasons.extend(r_l.get("reasons_against", []))

    return {
        "symbol":  symbol,
        "verdict": "NO_TRADE",
        "reasons": list(dict.fromkeys(str(r) for r in all_reasons)),
    }


# ── Phase 2: Grade ────────────────────────────────────────────────────────────

def grade_signal(sig: dict) -> dict:
    sym    = sig["symbol"]
    anchor = sig["anchor_time_unix"]
    side   = sig["side"]
    entry  = sig["entry"]
    sl     = sig["stop_loss"]
    tp     = sig["take_profit"]

    try:
        candles = fetch_15m_since(sym, anchor)
        candles = [c for c in candles if c["time"] > anchor]
    except Exception as e:
        return {**sig, "outcome": "fetch_error", "error": str(e), "r_multiple": None}

    if not candles:
        return {**sig, "outcome": "still_open", "r_multiple": None,
                "note": "no bars yet since anchor"}

    result = simulate(side, entry, sl, tp, candles)
    return {**sig, **result, "bars_available": len(candles)}


# ── Writer / reporter ─────────────────────────────────────────────────────────

def write_journal(
    snap_dt:   datetime,
    signals:   list[dict],
    no_trades: list[dict],
    errors:    list[dict],
    graded:    list[dict] | None,
    fg:        dict,
    dom:       float,
    live:      bool,
) -> Path:
    from pathlib import Path
    JOURNAL = HERE / "journal"
    JOURNAL.mkdir(exist_ok=True)
    path = JOURNAL / f"replay_{snap_dt.strftime('%Y-%m-%d_%H%M')}UTC.md"

    lines = []
    lines.append(f"# Replay — {snap_dt.strftime('%Y-%m-%d %H:%M UTC')}\n\n")
    lines.append(f"*Simulates what the agentic loop would have done at this moment.*\n\n")

    lines.append(f"## Macro at snapshot time\n\n")
    fg_str = f"{fg['value']} ({fg['classification']})" if fg["value"] >= 0 else "unavailable"
    lines.append(f"| Indicator | Value |\n|---|---|\n")
    lines.append(f"| Fear & Greed | {fg_str} |\n")
    lines.append(f"| BTC Dominance | {dom:.1f}% |\n\n")

    lines.append(f"## Signal Summary\n\n")
    lines.append(f"- Symbols scanned: **{len(signals) + len(no_trades) + len(errors)}**\n")
    lines.append(f"- Signals fired: **{len(signals)}**\n")
    lines.append(f"- No trade: **{len(no_trades)}**\n")
    lines.append(f"- Errors: **{len(errors)}**\n\n")

    if signals:
        lines.append(f"## Signals That Would Have Fired\n\n")
        lines.append(f"| Symbol | Side | Strategy | Entry | SL | TP | Snapshot Price |\n")
        lines.append(f"|---|---|---|---|---|---|---|\n")
        for s in signals:
            lines.append(
                f"| {s['symbol']} | {s['side']} | {s['strategy']} "
                f"| {s['entry']} | {s['stop_loss']} | {s['take_profit']} "
                f"| {s['snapshot_price']} |\n"
            )
        lines.append("\n")

    if graded:
        lines.append(f"## Grading — What Actually Happened\n\n")
        lines.append(f"| Symbol | Side | Entry | Outcome | R | Bars to Exit |\n")
        lines.append(f"|---|---|---|---|---|---|\n")
        total_r = 0.0
        wins = losses = expired = open_ = 0
        for g in graded:
            outcome = g.get("outcome", "?")
            r       = g.get("r_multiple")
            bars    = g.get("bars_to_exit") or g.get("exit_index") or "—"
            r_str   = f"{r:.2f}" if r is not None else "—"
            lines.append(
                f"| {g['symbol']} | {g['side']} | {g['entry']} "
                f"| **{outcome}** | {r_str} | {bars} |\n"
            )
            if outcome == "filled+tp":
                wins += 1
                if r: total_r += r
            elif outcome == "filled+sl":
                losses += 1
                if r: total_r += r
            elif outcome == "expired":
                expired += 1
            else:
                open_ += 1
        lines.append("\n")
        filled = wins + losses
        lines.append(f"**Results:**\n\n")
        lines.append(f"- Filled: {filled} / {len(graded)}\n")
        lines.append(f"- Wins / Losses: {wins} / {losses}\n")
        lines.append(f"- Expired: {expired}  |  Still open: {open_}\n")
        lines.append(f"- Total R: **{total_r:.2f}**\n")
        if filled:
            lines.append(f"- Win rate: **{wins/filled:.1%}**\n")
            lines.append(f"- Avg R/filled: **{total_r/filled:.2f}**\n")
        lines.append("\n")

    lines.append(f"## Hold Log — No Signal Symbols\n\n")
    lines.append(f"| Symbol | Top rejection reasons |\n|---|---|\n")
    for d in no_trades:
        reasons = (d.get("reasons") or [])[:3]
        lines.append(f"| {d['symbol']} | {' / '.join(reasons) or '—'} |\n")
    lines.append("\n")

    path.write_text("".join(lines), encoding="utf-8")
    return path


def print_results(signals, graded, no_trades, errors):
    print()
    print("=" * 65)
    print("REPLAY RESULTS")
    print("=" * 65)
    print(f"  Signals fired : {len(signals)}")
    for s in signals:
        print(f"    {s['symbol']:14} {s['side']:5} [{s['strategy']}]  "
              f"entry={s['entry']}  SL={s['stop_loss']}  TP={s['take_profit']}")

    if graded:
        print()
        print("  Grading:")
        total_r = 0.0
        wins = losses = expired = open_ = 0
        for g in graded:
            outcome = g.get("outcome", "?")
            r       = g.get("r_multiple")
            r_str   = f"R={r:.2f}" if r is not None else ""
            print(f"    {g['symbol']:14} {g['side']:5}  {outcome:12}  {r_str}")
            if outcome == "filled+tp":   wins   += 1; total_r += r or 0
            elif outcome == "filled+sl": losses += 1; total_r += r or 0
            elif outcome == "expired":   expired += 1
            else:                        open_  += 1
        filled = wins + losses
        print()
        print(f"  Filled: {filled}/{len(graded)}  |  Wins: {wins}  Losses: {losses}  "
              f"Expired: {expired}  Open: {open_}")
        print(f"  Total R: {total_r:.2f}", end="")
        if filled:
            print(f"  |  Win rate: {wins/filled:.1%}  |  Avg R/filled: {total_r/filled:.2f}")
        else:
            print()

    if errors:
        print(f"\n  Errors ({len(errors)}): {', '.join(e['symbol'] for e in errors)}")

    print()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date",      default=None,   help="YYYY-MM-DD (default: today)")
    ap.add_argument("--hour-utc",  type=int, default=OPEN_HOUR_UTC,
                    help=f"Snapshot hour UTC (default {OPEN_HOUR_UTC} = 9am ET DST)")
    ap.add_argument("--symbols",   nargs="*", default=None)
    ap.add_argument("--strategy",  default="both", choices=["fvg","trendline","both"])
    ap.add_argument("--tf-high",   default="15m")
    ap.add_argument("--tf-low",    default="5m")
    ap.add_argument("--no-grade",  action="store_true", help="skip grading phase")
    args = ap.parse_args()

    # Build snapshot datetime
    if args.date:
        base = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        base = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    snap_dt = base.replace(hour=args.hour_utc, minute=0, second=0, microsecond=0)
    snap_ts = int(snap_dt.timestamp())

    symbols = args.symbols if args.symbols else list(SYMBOL_MAP.keys())

    print()
    print("=" * 65)
    print(f"  REPLAY — {snap_dt.strftime('%Y-%m-%d %H:%M UTC')}  "
          f"({'9am ET' if args.hour_utc == 13 else f'{args.hour_utc}:00 UTC'})")
    print("=" * 65)
    print(f"  Symbols  : {len(symbols)}")
    print(f"  Strategy : {args.strategy}  |  TFs: {args.tf_high}/{args.tf_low}")
    print(f"  Grading  : {'disabled' if args.no_grade else 'enabled'}")
    print()

    # Macro (current values — historical not available without paid API)
    fg  = fetch_fear_greed()
    dom = fetch_btc_dominance()
    fg_str = f"{fg['value']} ({fg['classification']})" if fg["value"] >= 0 else "unavailable"
    print(f"  Fear & Greed   : {fg_str}")
    print(f"  BTC Dominance  : {dom:.1f}%")
    print()

    # ── Phase 1: Replay ──────────────────────────────────────────────────────
    print(f"[1/2] REPLAY — slicing candles to {snap_dt.strftime('%H:%M UTC')} snapshot\n")

    signals   = []
    no_trades = []
    errors    = []

    for i, sym in enumerate(symbols, 1):
        print(f"  [{i:02d}/{len(symbols)}] {sym}...", end=" ", flush=True)
        result = replay_symbol(sym, snap_ts, args.tf_high, args.tf_low, args.strategy)
        if result is None:
            print("skip")
            continue
        if result.get("error"):
            print(f"ERROR: {result['error']}")
            errors.append(result)
        elif result.get("verdict") == "TRADE":
            print(f"SIGNAL {result['side']}  entry={result['entry']}  "
                  f"SL={result['stop_loss']}  TP={result['take_profit']}")
            signals.append(result)
        else:
            top = (result.get("reasons") or [])[:2]
            print(f"no trade  ({' / '.join(top) or 'insufficient structure'})")
            no_trades.append(result)

    print(f"\n  Found {len(signals)} signal(s) out of {len(symbols)} symbols.\n")

    # Save raw signals to JSON
    raw_path = HERE / f"replay_{snap_dt.strftime('%Y%m%dT%H%M')}Z.json"
    raw_path.write_text(json.dumps({
        "replay_time_utc": snap_dt.isoformat(),
        "generated_utc":   datetime.now(timezone.utc).isoformat(),
        "strategy":        args.strategy,
        "tf_high":         args.tf_high,
        "tf_low":          args.tf_low,
        "signals":         signals,
        "no_trade_count":  len(no_trades),
        "error_count":     len(errors),
    }, indent=2, default=str))
    print(f"  Signals saved to {raw_path}\n")

    # ── Phase 2: Grade ───────────────────────────────────────────────────────
    graded = []
    if not args.no_grade and signals:
        print(f"[2/2] GRADING — checking what actually happened since {snap_dt.strftime('%H:%M UTC')}\n")
        for s in signals:
            print(f"  grading {s['symbol']} {s['side']}...", end=" ", flush=True)
            g = grade_signal(s)
            outcome = g.get("outcome", "?")
            r       = g.get("r_multiple")
            r_str   = f"R={r:.2f}" if r is not None else ""
            print(f"{outcome}  {r_str}")
            graded.append(g)
            time.sleep(0.5)
    elif not signals:
        print("[2/2] GRADING — no signals to grade.\n")
    else:
        print("[2/2] GRADING — skipped (--no-grade).\n")

    print_results(signals, graded, no_trades, errors)

    # Write journal entry
    journal_path = write_journal(snap_dt, signals, no_trades, errors,
                                 graded if graded else None, fg, dom, live=False)
    print(f"  Journal written to {journal_path}")

    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\naborted.")
        sys.exit(130)
