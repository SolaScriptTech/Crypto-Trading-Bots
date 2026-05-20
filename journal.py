"""journal.py — Journaling Routine.

Appends a structured Markdown entry to journal/YYYY-MM-DD.md after every
agent cycle. Logs:
  - Macro context (Fear & Greed, BTC dominance)
  - Every symbol evaluated with full verdict and reasoning
  - Full trade plan if a trade was confirmed
  - Detailed "hold" reasoning if no trade was taken
  - Order response and trailing-stop instructions if order was submitted
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from research import ResearchPacket
from signal_engine import CycleDecision

HERE    = Path(__file__).parent
JOURNAL = HERE / "journal"


def _day_file(ts: datetime) -> Path:
    JOURNAL.mkdir(exist_ok=True)
    return JOURNAL / f"{ts.strftime('%Y-%m-%d')}.md"


def _ts(ts: datetime) -> str:
    return ts.strftime("%H:%M:%S UTC")


def _append(path: Path, text: str) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(text)


def log_cycle(
    packet:    ResearchPacket,
    decisions: list[CycleDecision],
    cycle_num: int,
    live:      bool = False,
) -> Path:

    ts   = packet.timestamp
    path = _day_file(ts)

    lines = []
    lines.append(f"\n---\n")
    lines.append(f"## Cycle {cycle_num} — {_ts(ts)}  {'[LIVE]' if live else '[DRY-RUN]'}\n\n")

    # ── Macro context ────────────────────────────────────────────────────────
    fg  = packet.fear_greed
    dom = packet.btc_dominance
    fg_val = f"{fg['value']} — {fg['classification']}" if fg["value"] >= 0 else "unavailable"
    lines.append(f"### Macro\n\n")
    lines.append(f"| Indicator | Value |\n|---|---|\n")
    lines.append(f"| Fear & Greed | {fg_val} |\n")
    lines.append(f"| BTC Dominance | {dom:.1f}% |\n")
    if packet.fetch_errors:
        lines.append(f"| Fetch errors | {', '.join(s for s, _ in packet.fetch_errors)} |\n")
    lines.append("\n")

    # ── Signal summary ───────────────────────────────────────────────────────
    trades     = [d for d in decisions if d.verdict in ("TRADE_CONFIRMED", "TRADE_CONFIRMED_DRYRUN")]
    declined   = [d for d in decisions if d.verdict == "TRADE_DECLINED"]
    blocked    = [d for d in decisions if d.verdict == "TRADE_BLOCKED"]
    no_signals = [d for d in decisions if d.verdict == "NO_SIGNAL"]
    errors     = [d for d in decisions if d.verdict == "ERROR"]

    lines.append(f"### Summary\n\n")
    lines.append(f"- Symbols scanned: **{len(decisions)}**\n")
    lines.append(f"- Signals found: **{len(trades) + len(declined) + len(blocked)}**\n")
    lines.append(f"- Trades taken: **{len(trades)}**\n")
    lines.append(f"- Trades declined: **{len(declined)}**\n")
    lines.append(f"- Blocked by risk monitor: **{len(blocked)}**\n")
    lines.append(f"- No signal: **{len(no_signals)}**\n")
    if errors:
        lines.append(f"- Errors: **{len(errors)}** ({', '.join(d.symbol for d in errors)})\n")
    lines.append("\n")

    # ── Trades taken ─────────────────────────────────────────────────────────
    for d in trades:
        s = d.signal
        p = d.plan
        lines.append(f"### TRADE — {d.symbol} {s.side} [{s.strategy.upper()}]\n\n")
        lines.append(f"| Field | Value |\n|---|---|\n")
        lines.append(f"| Symbol | {d.symbol} |\n")
        lines.append(f"| Side | {s.side} |\n")
        lines.append(f"| Strategy | {s.strategy} |\n")
        lines.append(f"| Entry | {p.get('entry', s.entry)} |\n")
        lines.append(f"| Stop Loss | {p.get('sl_price', s.sl)} |\n")
        lines.append(f"| Take Profit | {p.get('tp_price', s.tp)} |\n")
        lines.append(f"| SL distance / unit | {p.get('sl_distance', s.sl_dist):.6f} |\n")
        lines.append(f"| Lot | {p.get('lot', '?'):,} |\n")
        lines.append(f"| Notional | ${p.get('notional_usd', 0):,.2f} |\n")
        lines.append(f"| Risk at SL | ${p.get('actual_risk', 0):.2f} |\n")
        lines.append(f"| Reward at TP | ${p.get('reward_usd', 0):.2f} |\n")
        lines.append(f"| Net R:R | 1:{p.get('net_rr', 0):.2f} |\n")
        lines.append(f"| Fees (round-trip) | ${p.get('fee_win', 0):.2f} on win / ${p.get('fee_loss', 0):.2f} on loss |\n")
        lines.append(f"| Spread at entry | {d.spread_pct:.3f}% |\n")
        lines.append(f"| Risk monitor | {d.risk_verdict} |\n")
        lines.append(f"| Dry-run | {'Yes' if d.verdict == 'TRADE_CONFIRMED_DRYRUN' else 'No'} |\n")
        lines.append("\n")
        lines.append(f"**Why entered:**\n\n")
        for r in s.reasons_for:
            lines.append(f"- {r}\n")
        lines.append("\n")
        if d.order_resp:
            lines.append(f"**Order response:**\n\n```json\n{json.dumps(d.order_resp, indent=2)}\n```\n\n")
        if d.notes:
            lines.append(f"**Notes:** {d.notes}\n\n")

    # ── Signals declined / blocked ────────────────────────────────────────────
    for d in (declined + blocked):
        s = d.signal
        lines.append(f"### SIGNAL DECLINED — {d.symbol} {s.side} [{s.strategy.upper()}]\n\n")
        lines.append(f"- Entry: {s.entry}  SL: {s.sl}  TP: {s.tp}\n")
        lines.append(f"- Verdict: **{d.verdict}**\n")
        lines.append(f"- Risk monitor: {d.risk_verdict}\n")
        lines.append(f"- Spread: {d.spread_pct:.3f}%\n")
        lines.append(f"- Notes: {d.notes or '—'}\n")
        lines.append(f"- Reasons signal fired: {', '.join(str(r) for r in s.reasons_for) or '—'}\n")
        lines.append("\n")

    # ── Hold log (NO_SIGNAL — the critical audit trail) ───────────────────────
    lines.append(f"### Hold Log — No Signal Symbols ({len(no_signals)})\n\n")
    lines.append(f"*Agent evaluated and held on all of the following. "
                 f"Every rule failure is recorded for audit.*\n\n")
    lines.append(f"| Symbol | Top rejection reasons |\n|---|---|\n")
    for d in no_signals:
        reasons = d.reasons_no_trade[:3]  # top 3 to keep table readable
        reason_str = " / ".join(str(r) for r in reasons) if reasons else "insufficient structure"
        lines.append(f"| {d.symbol} | {reason_str} |\n")
    lines.append("\n")

    # Full rejection detail for each no-signal (for deep audit)
    if no_signals:
        lines.append("<details>\n<summary>Full rejection detail (expand)</summary>\n\n")
        for d in no_signals:
            if d.reasons_no_trade:
                lines.append(f"**{d.symbol}**\n\n")
                for r in d.reasons_no_trade:
                    lines.append(f"- {r}\n")
                lines.append("\n")
        lines.append("</details>\n\n")

    text = "".join(lines)
    _append(path, text)
    return path


def log_startup(symbols: list[str], live: bool, tf_high: str, tf_low: str) -> None:
    ts   = datetime.now(timezone.utc)
    path = _day_file(ts)
    text = (
        f"\n# Agent Session — {ts.strftime('%Y-%m-%d')}  {'[LIVE]' if live else '[DRY-RUN]'}\n\n"
        f"Started: {_ts(ts)}\n\n"
        f"- Timeframes: {tf_high} / {tf_low}\n"
        f"- Symbols: {len(symbols)} ({', '.join(symbols[:10])}"
        f"{'...' if len(symbols) > 10 else ''})\n"
        f"- Mode: {'LIVE — orders will be submitted' if live else 'DRY-RUN — no orders submitted'}\n\n"
    )
    _append(path, text)
    print(f"  Journal: {path}")
