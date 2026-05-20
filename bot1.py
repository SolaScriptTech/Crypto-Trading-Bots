"""Bot 1 — Entry Positioning Bot (interactive).

You supply: symbol, side, entry price.
Bot fetches live equity, computes lot from leverage, auto-places SL and TP,
shows full plan with fee impact, and submits to DXtrade on confirmation.

Usage:
    python bot1.py            # dry-run (prints plan, no order sent)
    python bot1.py --live     # live submission to DXtrade
"""
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from dxtrade_client import DXtradeClient, load_env
from risk_monitor import evaluate as risk_evaluate, load_state as load_risk_state, save_state as save_risk_state

HERE = Path(__file__).parent

# --- Account / strategy constants (edit these to match your account) ---
RISK_USD        = 250.0    # dollar loss if SL hit
TP_MULT         = 4.0      # reward multiplier
FEE_PER_SIDE    = 0.0004   # Tradeify Crypto taker: 0.04% per side (round-trip 0.08%)
LEVERAGE_MAJORS = 5        # BTC/USD, ETH/USD, PAXG/USD on 1-Step/2-Step
LEVERAGE_ALTS   = 2        # all other pairs (and all pairs on Instant Funding)
MAJOR_SYMBOLS   = {"BTC/USD", "ETH/USD", "PAXG/USD"}
INSTANT_FUNDING = True     # True = 2:1 on everything; False = 5:1 on majors
MARGIN_FRACTION = 0.10     # fraction of available margin per trade (0.10 = up to 10 simultaneous trades, wider SL = less noise stop-outs)
# ----------------------------------------------------------------------

ROUND_TRIP_FEE = FEE_PER_SIDE * 2


def leverage_for(symbol: str) -> int:
    if INSTANT_FUNDING:
        return LEVERAGE_ALTS
    return LEVERAGE_MAJORS if symbol.upper() in MAJOR_SYMBOLS else LEVERAGE_ALTS


def ask(prompt: str, validator=None, transform=None):
    while True:
        raw = input(prompt).strip()
        if not raw:
            print("  (empty — try again, or Ctrl+C to abort)")
            continue
        try:
            val = transform(raw) if transform else raw
        except Exception as e:
            print(f"  parse error: {e}")
            continue
        if validator:
            err = validator(val)
            if err:
                print(f"  {err}")
                continue
        return val


def positive_float(x):
    f = float(x)
    if f <= 0:
        raise ValueError("must be > 0")
    return f


def side_check(s: str):
    if s.lower() not in ("long", "short", "buy", "sell"):
        return "enter 'long' or 'short' (or 'buy'/'sell')"
    return None


def compute_plan(symbol: str, side: str, entry: float, equity: float) -> dict:
    lev = leverage_for(symbol)
    max_notional = equity * lev * MARGIN_FRACTION
    lot = max_notional / entry          # float; DXtrade takes fractional lots
    lot = max(1.0, round(lot))          # floor to nearest whole unit, minimum 1

    sl_distance = RISK_USD / lot        # per-unit risk = $RISK / lot
    actual_risk = lot * sl_distance     # should be ≈ RISK_USD (slight rounding)

    if side in ("long", "buy"):
        sl_price  = entry - sl_distance
        tp_price  = entry + sl_distance * TP_MULT
        order_side = "BUY"
        side_name  = "long"
    else:
        sl_price  = entry + sl_distance
        tp_price  = entry - sl_distance * TP_MULT
        order_side = "SELL"
        side_name  = "short"

    notional     = lot * entry
    reward_usd   = actual_risk * TP_MULT

    entry_fee    = notional * FEE_PER_SIDE
    tp_fee       = lot * tp_price * FEE_PER_SIDE
    sl_fee       = lot * sl_price  * FEE_PER_SIDE
    fee_win      = entry_fee + tp_fee
    fee_loss     = entry_fee + sl_fee
    net_reward   = reward_usd - fee_win
    net_loss     = actual_risk + fee_loss
    net_rr       = net_reward / net_loss if net_loss > 0 else 0.0
    fees_pct     = fee_win / actual_risk if actual_risk > 0 else 0.0

    return {
        "symbol":        symbol,
        "side":          side_name,
        "order_side":    order_side,
        "entry":         entry,
        "sl_price":      sl_price,
        "tp_price":      tp_price,
        "sl_distance":   sl_distance,
        "leverage":      lev,
        "lot":           lot,
        "notional_usd":  notional,
        "actual_risk":   actual_risk,
        "reward_usd":    reward_usd,
        "fee_win":       fee_win,
        "fee_loss":      fee_loss,
        "net_reward":    net_reward,
        "net_loss":      net_loss,
        "net_rr":        net_rr,
        "fees_pct":      fees_pct,
    }


def validate_plan(plan: dict, metrics: dict, verdict: dict) -> list[str]:
    issues = []
    margin_req = plan["notional_usd"] / plan["leverage"]
    available  = metrics.get("availableFunds", 0)
    if margin_req > available * 1.01:   # 1% tolerance for rounding
        issues.append(
            f"required margin ~${margin_req:.2f} > available ${available:.2f}"
        )
    if verdict["verdict"] in ("BREACHED", "BLOCKED"):
        issues.append(f"risk monitor: {verdict['verdict']} — {verdict['reasons']}")
    if plan["net_rr"] < 1.0:
        issues.append(f"net R:R after fees is {plan['net_rr']:.2f} — below 1:1")
    return issues


def build_order_group(plan: dict, account: str) -> dict:
    base = str(int(time.time() * 1000))
    return {
        "account":       account,
        "orderType":     "LIMIT",
        "instrument":    plan["symbol"],
        "quantity":      plan["lot"],
        "side":          plan["order_side"],
        "limitPrice":    plan["entry"],
        "tif":           "GTC",
        "clientOrderId": f"b1e{base}",
        "stopLoss": {
            "type":          "STOP",
            "stopPrice":     round(plan["sl_price"], 6),
            "clientOrderId": f"b1s{base}",
        },
        "takeProfit": {
            "type":          "LIMIT",
            "limitPrice":    round(plan["tp_price"], 6),
            "clientOrderId": f"b1t{base}",
        },
    }


def print_plan(plan: dict, metrics: dict, verdict: dict, issues: list[str]) -> None:
    print()
    print("=" * 62)
    print(f"PLAN — {plan['symbol']}  {plan['side'].upper()}  ({plan['order_side']})")
    print("=" * 62)
    print(f"  Entry        : {plan['entry']:.6f}  (LIMIT, GTC)")
    print(f"  Stop loss    : {plan['sl_price']:.6f}  ({plan['sl_distance']:.6f} / unit)")
    print(f"  Take profit  : {plan['tp_price']:.6f}  ({TP_MULT}x R)")
    print(f"  Lot          : {plan['lot']:,.0f}  ({plan['leverage']}x leverage)")
    print(f"  Notional     : ${plan['notional_usd']:,.2f}")
    print(f"  Risk at SL   : ${plan['actual_risk']:.2f}  (target ${RISK_USD:.2f})")
    print(f"  Reward at TP : ${plan['reward_usd']:.2f}")
    print()
    print(f"--- Fee impact ({FEE_PER_SIDE*100:.3f}% per side / {ROUND_TRIP_FEE*100:.3f}% round-trip) ---")
    print(f"  Fees on win  : ${plan['fee_win']:.2f}")
    print(f"  Fees on loss : ${plan['fee_loss']:.2f}")
    print(f"  Net reward   : ${plan['net_reward']:.2f}")
    print(f"  Net loss     : -${plan['net_loss']:.2f}")
    print(f"  Net R:R      : 1:{plan['net_rr']:.2f}  (gross 1:{TP_MULT:.1f})")
    fee_warn = "  ⚠️  HIGH" if plan["fees_pct"] > 0.25 else ""
    print(f"  Fees / risk  : {plan['fees_pct']*100:.1f}%{fee_warn}")
    print()
    avail = metrics.get("availableFunds", 0)
    print(f"  Equity: ${metrics['equity']:.2f}  |  Available: ${avail:.2f}")
    print(f"  Risk monitor: {verdict['verdict']}  "
          f"(max-DD buffer ${verdict['max_dd_remaining']:.2f}  "
          f"daily buffer ${verdict['daily_dd_remaining']:.2f})")
    if issues:
        print("\n  Validation issues:")
        for i in issues:
            print(f"    ! {i}")
    else:
        print("\n  Validation: OK")
    print()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true",
                    help="submit order to DXtrade (default: dry-run)")
    args = ap.parse_args()

    print("=" * 62)
    print(f"BOT 1 — Entry Bot  {'[LIVE]' if args.live else '[DRY-RUN]'}"
          f"  risk=${RISK_USD:.0f}  TP={TP_MULT}x  fee={FEE_PER_SIDE*100:.3f}%/side")
    print("=" * 62)

    symbol = ask("Symbol (e.g. MANA/USD): ", transform=lambda s: s.upper())
    side   = ask("Side (long/short): ", validator=side_check, transform=lambda s: s.lower())
    if side == "buy":
        side = "long"
    if side == "sell":
        side = "short"
    entry  = ask("Entry price: ", transform=positive_float)

    env    = load_env(HERE / ".env")
    client = DXtradeClient(env)
    client.login()
    metrics = client.metrics()

    equity = float(metrics.get("availableFunds", metrics["equity"]))
    plan   = compute_plan(symbol, side, entry, equity)

    risk_state = load_risk_state()
    verdict    = risk_evaluate(risk_state, metrics, datetime.now(timezone.utc))
    save_risk_state(risk_state)

    issues = validate_plan(plan, metrics, verdict)
    print_plan(plan, metrics, verdict, issues)

    if not args.live:
        confirm = input("Submit? [DRY-RUN — prints body only]  (yes/no): ").strip().lower()
        if confirm != "yes":
            print("aborted.")
            return 0
        body = build_order_group(plan, env["DXTRADE_ACCOUNT"])
        print("\n[DRY-RUN] order body:")
        print(json.dumps(body, indent=2))
        return 0

    if verdict["verdict"] in ("BREACHED", "BLOCKED"):
        print("REFUSING — risk monitor blocking. Resolve before live submission.")
        return 1
    if issues:
        override = input(
            f"{len(issues)} validation issue(s). Override and continue? (yes/no): "
        ).strip().lower()
        if override != "yes":
            print("aborted.")
            return 0

    confirm = input("SUBMIT LIVE ORDER to DXtrade? (type 'yes' exactly): ").strip().lower()
    if confirm != "yes":
        print("aborted.")
        return 0

    body   = build_order_group(plan, env["DXTRADE_ACCOUNT"])
    path   = f"/dxsca-web/accounts/{quote(env['DXTRADE_ACCOUNT'], safe='')}/orders"
    print("\nsubmitting...")
    status, resp = client.post(path, body)
    print(f"HTTP {status}")
    print(json.dumps(resp, indent=2) if isinstance(resp, dict) else resp)
    return 0 if status in (200, 201) else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\naborted.")
        sys.exit(130)
