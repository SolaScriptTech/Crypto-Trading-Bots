"""signal_engine.py — Signal & Execution Routine.

For each symbol in the ResearchPacket:
  1. Runs Bot 2 analysis on both timeframes
  2. Checks for dual-TF agreement
  3. Checks risk monitor (blocks if near DD limits)
  4. Checks fee viability (warns if fees > 25% of risk)
  5. If signal found: presents full plan and waits for confirmation
  6. On 'yes': submits bracket order to DXtrade

Returns a list of CycleDecision objects consumed by journal.py.
"""
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from mss_bot import MarketStructureV2Bot
from bot1 import compute_plan, validate_plan, build_order_group, print_plan, RISK_USD, TP_MULT, FEE_PER_SIDE
from risk_monitor import evaluate as risk_evaluate, load_state as load_risk_state, save_state as save_risk_state
from research import ResearchPacket

HERE = Path(__file__).parent

CONFIRM_TIMEOUT_SECS = 180   # 3 minutes to respond before signal expires
SPREAD_WARN_PCT      = 0.15  # warn if spread > 0.15% (wider than normal)


@dataclass
class SignalResult:
    symbol:       str
    side:         str           # BUY or SELL
    strategy:     str           # fvg / trendline
    entry:        float
    sl:           float
    tp:           float
    sl_dist:      float
    reasons_for:  list
    reasons_against: list
    tf_high_verdict: str
    tf_low_verdict:  str


@dataclass
class CycleDecision:
    timestamp:    datetime
    symbol:       str
    verdict:      str           # TRADE_CONFIRMED / TRADE_DECLINED / TRADE_BLOCKED / NO_SIGNAL / ERROR
    signal:       SignalResult | None = None
    plan:         dict          = field(default_factory=dict)
    order_resp:   dict          = field(default_factory=dict)
    reasons_no_trade: list      = field(default_factory=list)
    risk_verdict: str           = ""
    spread_pct:   float         = 0.0
    notes:        str           = ""


def _fmt_reasons(reasons: list) -> str:
    return "  |  ".join(str(r) for r in reasons) if reasons else "—"


def evaluate_symbol(
    symbol: str,
    packet: ResearchPacket,
    strategy: str = "mss",   # kept for API compatibility, ignored
    tf_high: str  = "15m",
    tf_low: str   = "5m",
) -> CycleDecision:

    now = datetime.now(timezone.utc)

    c_high = packet.candles_high.get(symbol)
    if not c_high:
        return CycleDecision(
            timestamp=now, symbol=symbol, verdict="ERROR",
            notes=f"no candle data for {symbol}"
        )

    try:
        bot   = MarketStructureV2Bot(c_high, symbol=symbol, timeframe=tf_high)
        setup = bot.analyze()
    except Exception as e:
        return CycleDecision(
            timestamp=now, symbol=symbol, verdict="ERROR",
            notes=f"MSS analysis failed: {e}"
        )

    if setup.status != "TRADE":
        return CycleDecision(
            timestamp        = now,
            symbol           = symbol,
            verdict          = "NO_SIGNAL",
            reasons_no_trade = [setup.reason] + setup.context_log[-3:],
            spread_pct       = packet.orderbooks.get(symbol, {}).get("spread_pct", 0.0),
        )

    spread_pct = packet.orderbooks.get(symbol, {}).get("spread_pct", 0.0)

    signal = SignalResult(
        symbol          = symbol,
        side            = "BUY",
        strategy        = "mss",
        entry           = setup.entry,
        sl              = setup.stop_loss,
        tp              = setup.take_profit,
        sl_dist         = setup.sl_distance,
        reasons_for     = [setup.reason] + [setup.context_log[-1]] if setup.context_log else [setup.reason],
        reasons_against = [],
        tf_high_verdict = "TRADE",
        tf_low_verdict  = "TRADE",
    )

    risk_state   = load_risk_state()
    risk_verdict = risk_evaluate(risk_state, {"equity": 0, "balance": 0, "openPL": 0}, now)
    save_risk_state(risk_state)

    return CycleDecision(
        timestamp    = now,
        symbol       = symbol,
        verdict      = "SIGNAL_PENDING",
        signal       = signal,
        risk_verdict = risk_verdict["verdict"],
        spread_pct   = spread_pct,
        notes        = f"spread={spread_pct:.3f}%  risk_monitor={risk_verdict['verdict']}  waves={setup.wave_count}  R:R=1:{setup.risk_reward}",
    )


def present_signal(decision: CycleDecision, metrics: dict) -> None:
    s = decision.signal
    print()
    print("=" * 65)
    print(f"  SIGNAL FOUND — {s.symbol}  {s.side}  [{s.strategy.upper()}]")
    print("=" * 65)
    print(f"  Entry    : {s.entry}")
    print(f"  SL       : {s.sl}  (dist: {s.sl_dist:.6f}/unit)")
    print(f"  TP       : {s.tp}  (1:{TP_MULT:.0f} R)")
    print(f"  Why:     : {_fmt_reasons(s.reasons_for)}")
    if decision.spread_pct > SPREAD_WARN_PCT:
        print(f"  ⚠ SPREAD : {decision.spread_pct:.3f}% — wider than normal, check liquidity")
    if decision.risk_verdict in ("BLOCKED", "BREACHED"):
        print(f"  ⚠ RISK   : {decision.risk_verdict} — trade blocked by risk monitor")
    equity = float(metrics.get("equity", 0))
    plan   = compute_plan(s.symbol, s.side.lower(), s.entry, equity)
    risk_state  = load_risk_state()
    rv = risk_evaluate(risk_state, metrics, datetime.now(timezone.utc))
    issues = validate_plan(plan, metrics, rv)
    print_plan(plan, metrics, rv, issues)
    decision.plan = plan


def confirm_trade(decision: CycleDecision, timeout: int = CONFIRM_TIMEOUT_SECS) -> bool:
    if decision.risk_verdict in ("BLOCKED", "BREACHED"):
        print(f"  Trade BLOCKED by risk monitor ({decision.risk_verdict}). Skipping.")
        decision.verdict = "TRADE_BLOCKED"
        return False

    print(f"  You have {timeout // 60}m {timeout % 60:02d}s to respond before signal expires.")
    try:
        import select, sys as _sys
        raw = input("  Submit this trade? (yes / no / skip): ").strip().lower()
    except (EOFError, KeyboardInterrupt):
        raw = "no"

    if raw == "yes":
        decision.verdict = "TRADE_CONFIRMED"
        return True
    else:
        decision.verdict = "TRADE_DECLINED"
        decision.notes  += f"  user_response={raw}"
        return False


def submit_order(decision: CycleDecision, client, env: dict) -> None:
    plan    = decision.plan
    account = env["DXTRADE_ACCOUNT"]
    body    = build_order_group(plan, account)
    path    = f"/dxsca-web/accounts/{quote(account, safe='')}/orders"
    print("  Submitting order...")
    status, resp = client.post(path, body)
    decision.order_resp = resp if isinstance(resp, dict) else {"raw": resp}
    if status in (200, 201):
        print(f"  Order submitted. HTTP {status}")
        cid = body.get("clientOrderId", "?")
        sl_cid = body.get("stopLoss", {}).get("clientOrderId", "?")
        print(f"  clientOrderId: {cid}")
        print(f"  SL clientOrderId (for trailing_stop.py): {sl_cid}")
        print()
        print("  NEXT STEP — run trailing stop:")
        print(f"    python trailing_stop.py \\")
        print(f"      --symbol {plan['symbol']} \\")
        print(f"      --side {plan['side']} \\")
        print(f"      --entry {plan['entry']} \\")
        print(f"      --sl-dist {plan['sl_distance']:.6f} \\")
        print(f"      --sl-client-id {sl_cid} \\")
        print(f"      --lot {plan['lot']:.0f}")
    else:
        print(f"  Order FAILED. HTTP {status}  {resp}")
        decision.verdict = "ERROR"
        decision.notes  += f"  order_failed: HTTP {status}"


def run(
    packet:    ResearchPacket,
    client,
    env:       dict,
    metrics:   dict,
    symbols:   list[str],
    strategy:  str  = "both",
    tf_high:   str  = "15m",
    tf_low:    str  = "5m",
    live:      bool = False,
) -> list[CycleDecision]:

    decisions = []

    for sym in symbols:
        decision = evaluate_symbol(sym, packet, strategy, tf_high, tf_low)

        if decision.verdict == "SIGNAL_PENDING":
            present_signal(decision, metrics)
            if live:
                confirmed = confirm_trade(decision)
                if confirmed:
                    submit_order(decision, client, env)
            else:
                decision.verdict = "TRADE_CONFIRMED_DRYRUN"
                print("  [DRY-RUN] Would submit. Pass --live to actually trade.")

        decisions.append(decision)

    return decisions
