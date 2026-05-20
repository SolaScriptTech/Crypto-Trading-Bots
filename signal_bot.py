#!/usr/bin/env python3
"""
signal_bot.py — manual signal to Kraken bracket order automation
                fixed-dollar risk, fixed reward:risk ratio

Usage:
    python signal_bot.py long  XBT/USD 100250 --sl 100200
    python signal_bot.py short ETH/USD 3050   --sl 3055
    python signal_bot.py long  XBT/USD 100250 --sl 100200 --shadow
    python signal_bot.py long  XBT/USD 100250 --sl 100200 --max-loss 50 --rr 4

Strategy (fixed-risk, fixed-RR):
    Max loss      = $150  (full position)
    Reward:Risk   = 4:1   (TP distance = rr * SL distance)
    Realized P&L  = -$150 on stop, +$600 on target

Sizing (works for any asset / price scale):
    sl_distance = |entry - sl_price|
    qty         = max_loss / sl_distance
    tp_distance = rr * sl_distance

Execution model on Kraken spot (live mode):
    1. Limit entry order at entry_price
    2. Stop-loss order at sl_price (triggered close)
    3. Take-profit limit order at tp_price
    NOTE: Kraken spot has no native OCO. SL and TP are independent — if
    price whipsaws, both could fill. Bot does not auto-cancel one when
    the other fills. For shadow/paper testing this is fine.

Configure via .env:
    KRAKEN_API_KEY
    KRAKEN_API_SECRET
"""

import os
import sys
import csv
import argparse
from datetime import datetime, timezone
from pathlib import Path

import ccxt
from dotenv import load_dotenv

load_dotenv()

KRAKEN_API_KEY    = os.getenv("KRAKEN_API_KEY", "")
KRAKEN_API_SECRET = os.getenv("KRAKEN_API_SECRET", "")

MAX_LOSS_DEFAULT = 150.0
RR_DEFAULT       = 4.0

MAX_ENTRY_DEVIATION_PCT = 2.0
MAX_LOSS_HARD_CAP       = 5000.0

AUDIT_LOG_PATH = Path("signal_bot_audit.csv")


# KRAKEN CLIENT
def make_client():
    if not KRAKEN_API_KEY or not KRAKEN_API_SECRET:
        raise RuntimeError("KRAKEN_API_KEY / KRAKEN_API_SECRET not set in .env")
    ex = ccxt.kraken({
        "apiKey": KRAKEN_API_KEY,
        "secret": KRAKEN_API_SECRET,
        "enableRateLimit": True,
    })
    ex.load_markets()
    return ex


def get_equity_usd(ex):
    bal = ex.fetch_balance()
    total = bal.get("total", {})
    usd = float(total.get("USD") or total.get("ZUSD") or 0)
    return usd


def get_quote(ex, symbol):
    t = ex.fetch_ticker(symbol)
    return float(t["bid"]), float(t["ask"])


def place_bracket(ex, symbol, side, qty, entry_price, sl_price, tp_price):
    """
    Three independent orders on Kraken spot:
      1. limit entry
      2. stop-loss (triggered close)
      3. take-profit limit (close)
    Returns dict with order ids. Not OCO — see module docstring.
    """
    close_side = "sell" if side == "buy" else "buy"

    entry = ex.create_order(symbol, "limit", side, qty, entry_price)

    sl = ex.create_order(
        symbol, "stop-loss", close_side, qty, None,
        params={"stopPrice": sl_price, "trigger": "last"},
    )

    tp = ex.create_order(symbol, "limit", close_side, qty, tp_price,
                         params={"reduceOnly": True} if False else {})

    return {
        "entry_id": entry.get("id"),
        "sl_id":    sl.get("id"),
        "tp_id":    tp.get("id"),
    }


# MATH
def compute_levels(direction, entry_price, sl_price, max_loss, rr):
    sl_distance = abs(entry_price - sl_price)
    if sl_distance <= 0:
        raise ValueError("SL price must differ from entry")
    if direction == "long" and sl_price >= entry_price:
        raise ValueError(f"long requires SL below entry (sl={sl_price}, entry={entry_price})")
    if direction == "short" and sl_price <= entry_price:
        raise ValueError(f"short requires SL above entry (sl={sl_price}, entry={entry_price})")

    qty         = max_loss / sl_distance
    tp_distance = rr * sl_distance
    tp_dollars  = max_loss * rr
    sign        = 1 if direction == "long" else -1
    tp_price    = entry_price + sign * tp_distance
    return qty, sl_distance, tp_price, tp_distance, tp_dollars


def round_qty_to_market(ex, symbol, qty):
    try:
        return float(ex.amount_to_precision(symbol, qty))
    except Exception:
        return float(f"{qty:.8f}")


def round_price_to_market(ex, symbol, price):
    try:
        return float(ex.price_to_precision(symbol, price))
    except Exception:
        return float(f"{price:.8f}")


# AUDIT LOG
AUDIT_FIELDS = [
    "timestamp_utc", "mode", "direction", "symbol",
    "entry_price", "sl_price", "tp_price",
    "max_loss", "tp_dollars", "rr", "sl_distance",
    "equity", "qty", "entry_id", "sl_id", "tp_id", "status", "notes",
]

def log_audit(row):
    new = not AUDIT_LOG_PATH.exists()
    with AUDIT_LOG_PATH.open("a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=AUDIT_FIELDS)
        if new:
            w.writeheader()
        w.writerow({k: row.get(k, "") for k in AUDIT_FIELDS})


# CLI
def parse_args():
    p = argparse.ArgumentParser(description="Manual signal to Kraken bracket (fixed-$ risk, fixed RR)")
    p.add_argument("direction", choices=["long", "short"])
    p.add_argument("symbol", help="ccxt symbol e.g. XBT/USD, ETH/USD, SOL/USD")
    p.add_argument("entry_price", type=float)

    sl_group = p.add_mutually_exclusive_group(required=True)
    sl_group.add_argument("--sl", type=float, help="Stop loss price")
    sl_group.add_argument("--stop-distance", type=float,
                          help="Stop distance in price (alternative to --sl)")

    p.add_argument("--max-loss", type=float, default=MAX_LOSS_DEFAULT,
                   help=f"Max dollar loss when SL hits (default ${MAX_LOSS_DEFAULT})")
    p.add_argument("--rr", type=float, default=RR_DEFAULT,
                   help=f"Reward:risk ratio — TP distance = rr * SL distance (default {RR_DEFAULT})")
    p.add_argument("--shadow", action="store_true",
                   help="Compute and log only — DO NOT submit orders to Kraken")
    p.add_argument("--equity-override", type=float, default=None,
                   help="Force equity value (skips API call). Useful for shadow without keys.")
    p.add_argument("--yes", action="store_true",
                   help="Skip interactive confirmation in live mode")
    return p.parse_args()


def print_summary(args, equity, sl_price, tp_price, tp_dollars, qty,
                  sl_distance, tp_distance, bid=None, ask=None):
    side = "BUY" if args.direction == "long" else "SELL"
    mode = "SHADOW" if args.shadow else "LIVE"
    print()
    print("=" * 64)
    print(f"  SIGNAL BOT [{mode}] — {args.direction.upper()} {args.symbol}")
    print("=" * 64)
    print(f"  Entry (limit):     {args.entry_price:>14,.6f}  ({side})")
    if bid is not None:
        print(f"  Current bid/ask:   {bid:>14,.6f} / {ask:,.6f}")
    print()
    print(f"  Stop Loss:         {sl_price:>14,.6f}   distance: {sl_distance:>12,.6f}  (-${args.max_loss:.2f})")
    print(f"  Take Profit:       {tp_price:>14,.6f}   distance: {tp_distance:>12,.6f}  (+${tp_dollars:.2f})")
    print()
    print(f"  Equity:           ${equity:>14,.2f}")
    print(f"  Quantity:          {qty:>14,.8f}")
    print(f"  Reward:Risk:       {args.rr:>14,.2f} : 1")
    print("=" * 64)


def main():
    args = parse_args()

    if args.max_loss <= 0 or args.max_loss > MAX_LOSS_HARD_CAP:
        print(f"ERROR: --max-loss must be in (0, {MAX_LOSS_HARD_CAP}]", file=sys.stderr)
        sys.exit(2)
    if args.rr <= 0:
        print("ERROR: --rr must be positive", file=sys.stderr)
        sys.exit(2)

    if args.sl is not None:
        sl_price = args.sl
    else:
        sign = -1 if args.direction == "long" else 1
        sl_price = args.entry_price + sign * args.stop_distance

    try:
        qty, sl_distance, tp_price, tp_distance, tp_dollars = compute_levels(
            args.direction, args.entry_price, sl_price, args.max_loss, args.rr)
    except ValueError as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(2)

    ex = None
    bid = ask = None
    need_client = not (args.shadow and args.equity_override is not None)
    if need_client:
        try:
            ex = make_client()
            if args.equity_override is None:
                equity = get_equity_usd(ex)
            else:
                equity = args.equity_override
            try:
                bid, ask = get_quote(ex, args.symbol)
                mid = (bid + ask) / 2
                dev_pct = abs(args.entry_price - mid) / mid * 100
                if dev_pct > MAX_ENTRY_DEVIATION_PCT:
                    print(f"ERROR: entry {args.entry_price} is {dev_pct:.2f}% from market mid {mid:.6f} "
                          f"(max {MAX_ENTRY_DEVIATION_PCT}%)", file=sys.stderr)
                    sys.exit(3)
            except SystemExit:
                raise
            except Exception as e:
                print(f"WARN: could not fetch quote for sanity check: {e}", file=sys.stderr)

            entry_price = round_price_to_market(ex, args.symbol, args.entry_price)
            sl_price    = round_price_to_market(ex, args.symbol, sl_price)
            tp_price    = round_price_to_market(ex, args.symbol, tp_price)
            qty         = round_qty_to_market(ex, args.symbol, qty)
            args.entry_price = entry_price
        except Exception as e:
            if not args.shadow:
                print(f"ERROR: Kraken client init failed: {e}", file=sys.stderr)
                sys.exit(1)
            print(f"WARN: Kraken client unavailable, continuing in shadow: {e}", file=sys.stderr)
            equity = args.equity_override or 0.0
    else:
        equity = args.equity_override

    if qty <= 0:
        print(f"ERROR: computed qty too small ({qty}). Tighter SL or larger max-loss?",
              file=sys.stderr)
        sys.exit(4)

    print_summary(args, equity, sl_price, tp_price, tp_dollars, qty,
                  sl_distance, tp_distance, bid, ask)

    audit = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "mode":          "SHADOW" if args.shadow else "LIVE",
        "direction":     args.direction,
        "symbol":        args.symbol,
        "entry_price":   args.entry_price,
        "sl_price":      sl_price,
        "tp_price":      tp_price,
        "max_loss":      args.max_loss,
        "tp_dollars":    round(tp_dollars, 2),
        "rr":            args.rr,
        "sl_distance":   sl_distance,
        "equity":        round(equity, 2),
        "qty":           qty,
    }

    if args.shadow:
        audit["status"] = "SHADOW"
        log_audit(audit)
        print("\n  SHADOW MODE — no orders sent. Logged to", AUDIT_LOG_PATH)
        return

    if not args.yes:
        confirm = input("\nSubmit LIVE orders to Kraken? type 'yes' to confirm: ").strip().lower()
        if confirm != "yes":
            print("Aborted.")
            audit["status"] = "ABORTED_BY_USER"
            log_audit(audit)
            return

    side = "buy" if args.direction == "long" else "sell"
    try:
        ids = place_bracket(ex, args.symbol, side, qty,
                            args.entry_price, sl_price, tp_price)
    except Exception as e:
        audit["status"] = f"FAIL: {e}"
        log_audit(audit)
        print(f"\nERROR placing orders: {e}", file=sys.stderr)
        sys.exit(5)

    audit["entry_id"] = ids["entry_id"]
    audit["sl_id"]    = ids["sl_id"]
    audit["tp_id"]    = ids["tp_id"]
    audit["status"]   = "SUBMITTED"
    audit["notes"]    = "SL and TP are independent — not OCO. Whipsaws may fill both."
    log_audit(audit)
    print(f"\n  Submitted: entry={ids['entry_id']}  sl={ids['sl_id']}  tp={ids['tp_id']}")
    print("  Audit logged to:", AUDIT_LOG_PATH)


if __name__ == "__main__":
    main()
