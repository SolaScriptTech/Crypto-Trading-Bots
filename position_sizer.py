"""
Position Sizer — Breakout Prop Edition
=======================================
Sizes positions against BOTH the daily loss limit AND the max drawdown,
returning the tighter of the two as the binding constraint.

Built for Breakout Prop evaluation rules:
- 1-step: 4% daily loss, 6% static max drawdown
- 2-step: 5% daily loss, 8% trailing max drawdown

The daily limit resets at 00:30 UTC. The max drawdown does not reset —
it's the running line you cannot cross or you fail the eval.

Static drawdown: floor is (starting_account_balance × (1 - max_dd_pct/100))
Trailing drawdown: floor is (peak_equity_ever × (1 - max_dd_pct/100)),
                   but only ratchets up, never down.

Breakout confirms real-time equity measurement (balance ± unrealized P&L).
This means intraday wicks count. Size accordingly.

Usage (CLI):
    # 1-step eval, fresh account
    python3 position_sizer.py \\
        --equity 5000 --daily-limit-pct 4.0 \\
        --max-drawdown-pct 6.0 --drawdown-type static \\
        --tolerated-drawdown-pct 5.0

    # 2-step eval, mid-journey with peak at 5400
    python3 position_sizer.py \\
        --equity 5280 --starting-equity 5320 --peak-equity 5400 \\
        --daily-limit-pct 5.0 --max-drawdown-pct 8.0 --drawdown-type trailing \\
        --tolerated-drawdown-pct 6.0 \\
        --open-position "BTC/USD:1500:-0.4"
"""

from dataclasses import dataclass, field
from typing import List, Optional
import argparse
import sys


# ---- Data structures ----

@dataclass
class OpenPosition:
    """An already-open position. unrealized_pnl_pct is signed."""
    symbol: str
    size_usd: float
    unrealized_pnl_pct: float

    @property
    def unrealized_pnl_usd(self) -> float:
        return self.size_usd * (self.unrealized_pnl_pct / 100.0)


@dataclass
class SizingInputs:
    equity: float                       # current real-time equity
    starting_equity_today: float        # equity at 00:30 UTC reset
    starting_account_balance: float     # initial balance at eval start (static DD anchor)
    peak_equity: float                  # highest equity ever (trailing DD anchor)
    daily_limit_pct: float
    max_drawdown_pct: float
    drawdown_type: str                  # "static" or "trailing"
    tolerated_drawdown_pct: float
    open_positions: List[OpenPosition] = field(default_factory=list)
    slippage_buffer_pct: float = 0.5
    max_account_pct: float = 60.0
    correlation_stress_pct: float = 3.0


@dataclass
class SizingResult:
    recommended_size_usd: float
    binding_constraint: str
    daily_budget_usd: float
    daily_budget_remaining_usd: float
    drawdown_floor_usd: float
    drawdown_budget_remaining_usd: float
    max_size_daily_usd: float
    max_size_drawdown_usd: float
    correlated_stress_loss_usd: float
    reasoning: List[str]
    warnings: List[str]
    can_trade: bool


# ---- Core logic ----

def size_position(inp: SizingInputs) -> SizingResult:
    reasoning = []
    warnings = []

    # Daily limit
    daily_budget = inp.starting_equity_today * (inp.daily_limit_pct / 100.0)
    equity_change_today = inp.equity - inp.starting_equity_today
    already_lost_today = max(0.0, -equity_change_today)
    daily_budget_remaining = daily_budget - already_lost_today

    reasoning.append(
        f"Daily budget: ${daily_budget:.2f} "
        f"({inp.daily_limit_pct}% of SOD ${inp.starting_equity_today:.2f})"
    )
    if already_lost_today > 0:
        reasoning.append(
            f"Down ${already_lost_today:.2f} today → ${daily_budget_remaining:.2f} left"
        )

    # Max drawdown
    if inp.drawdown_type == "static":
        dd_anchor = inp.starting_account_balance
        dd_label = f"static from starting balance ${inp.starting_account_balance:.2f}"
    elif inp.drawdown_type == "trailing":
        dd_anchor = inp.peak_equity
        dd_label = f"trailing from peak ${inp.peak_equity:.2f}"
    else:
        raise ValueError(f"drawdown_type must be 'static' or 'trailing'")

    drawdown_floor = dd_anchor * (1 - inp.max_drawdown_pct / 100.0)
    drawdown_budget_remaining = inp.equity - drawdown_floor

    reasoning.append(
        f"Max DD ({dd_label}, {inp.max_drawdown_pct}%) → floor ${drawdown_floor:.2f}"
    )
    reasoning.append(
        f"Current ${inp.equity:.2f} → ${drawdown_budget_remaining:.2f} room to floor"
    )

    # Early exits
    if daily_budget_remaining <= 0:
        warnings.append("DAILY LIMIT HIT. No trading today.")
        return _zero_result(daily_budget, daily_budget_remaining, drawdown_floor,
                            drawdown_budget_remaining, reasoning, warnings,
                            "daily_exhausted")

    if drawdown_budget_remaining <= 0:
        warnings.append("MAX DRAWDOWN BREACHED. Eval failed.")
        return _zero_result(daily_budget, daily_budget_remaining, drawdown_floor,
                            drawdown_budget_remaining, reasoning, warnings,
                            "drawdown_breached")

    # Correlated stress test
    total_open_exposure = sum(p.size_usd for p in inp.open_positions)
    correlated_stress_loss = total_open_exposure * (inp.correlation_stress_pct / 100.0)

    if inp.open_positions:
        reasoning.append(
            f"Open exposure ${total_open_exposure:.2f} → stress reserve "
            f"${correlated_stress_loss:.2f} at {inp.correlation_stress_pct}% adverse"
        )

    daily_budget_for_new = daily_budget_remaining - correlated_stress_loss
    drawdown_budget_for_new = drawdown_budget_remaining - correlated_stress_loss

    if daily_budget_for_new <= 0:
        warnings.append("Existing exposure consumes daily budget under stress.")
        return _zero_result(daily_budget, daily_budget_remaining, drawdown_floor,
                            drawdown_budget_remaining, reasoning, warnings,
                            "daily_stress_exhausted")

    if drawdown_budget_for_new <= 0:
        warnings.append("Existing exposure consumes DD budget under stress.")
        return _zero_result(daily_budget, daily_budget_remaining, drawdown_floor,
                            drawdown_budget_remaining, reasoning, warnings,
                            "drawdown_stress_exhausted")

    # Max position under each constraint
    effective_dd = (inp.tolerated_drawdown_pct + inp.slippage_buffer_pct) / 100.0
    max_size_daily = daily_budget_for_new / effective_dd
    max_size_drawdown = drawdown_budget_for_new / effective_dd
    account_cap = inp.equity * (inp.max_account_pct / 100.0)

    reasoning.append(f"At {inp.tolerated_drawdown_pct}%+{inp.slippage_buffer_pct}% buffer:")
    reasoning.append(f"  Daily-capped max:   ${max_size_daily:.2f}")
    reasoning.append(f"  DD-capped max:      ${max_size_drawdown:.2f}")
    reasoning.append(f"  Account cap ({inp.max_account_pct}%): ${account_cap:.2f}")

    # Binding constraint
    candidates = [
        (max_size_daily, "daily_limit"),
        (max_size_drawdown, "max_drawdown"),
        (account_cap, "account_cap"),
    ]
    recommended_size, binding = min(candidates, key=lambda x: x[0])
    reasoning.append(f"Binding: {binding} → ${recommended_size:.2f}")

    # Warnings
    if binding == "max_drawdown":
        warnings.append(
            "Max drawdown is binding — you're closer to eval failure than to a daily breach."
        )
    if already_lost_today > daily_budget * 0.5:
        warnings.append("More than half the daily budget spent. Revenge trading kills evals.")
    if drawdown_budget_remaining < dd_anchor * 0.02:
        warnings.append("Less than 2% room to DD floor. One bad candle ends the eval.")
    if inp.tolerated_drawdown_pct < 2.0:
        warnings.append(f"Position tolerance {inp.tolerated_drawdown_pct}% is very tight for crypto.")
    if recommended_size < 100:
        warnings.append(f"Size ${recommended_size:.2f} is tiny — fees eat the edge.")

    return SizingResult(
        recommended_size_usd=round(recommended_size, 2),
        binding_constraint=binding,
        daily_budget_usd=round(daily_budget, 2),
        daily_budget_remaining_usd=round(daily_budget_remaining, 2),
        drawdown_floor_usd=round(drawdown_floor, 2),
        drawdown_budget_remaining_usd=round(drawdown_budget_remaining, 2),
        max_size_daily_usd=round(max_size_daily, 2),
        max_size_drawdown_usd=round(max_size_drawdown, 2),
        correlated_stress_loss_usd=round(correlated_stress_loss, 2),
        reasoning=reasoning,
        warnings=warnings,
        can_trade=True,
    )


def _zero_result(daily_budget, daily_remaining, dd_floor, dd_remaining,
                 reasoning, warnings, binding):
    return SizingResult(
        recommended_size_usd=0.0, binding_constraint=binding,
        daily_budget_usd=round(daily_budget, 2),
        daily_budget_remaining_usd=round(daily_remaining, 2),
        drawdown_floor_usd=round(dd_floor, 2),
        drawdown_budget_remaining_usd=round(dd_remaining, 2),
        max_size_daily_usd=0.0, max_size_drawdown_usd=0.0,
        correlated_stress_loss_usd=0.0,
        reasoning=reasoning, warnings=warnings, can_trade=False,
    )


# ---- CLI ----

def parse_open_position(spec: str) -> OpenPosition:
    parts = spec.split(":")
    if len(parts) != 3:
        raise argparse.ArgumentTypeError(
            f"Open position must be 'SYMBOL:SIZE:PNL_PCT', got '{spec}'"
        )
    return OpenPosition(parts[0], float(parts[1]), float(parts[2]))


def print_result(result: SizingResult, inp: SizingInputs):
    print()
    print("=" * 68)
    print(f"BREAKOUT EVAL — POSITION SIZER ({inp.drawdown_type.upper()} DRAWDOWN)")
    print("=" * 68)
    print(f"Current equity:        ${inp.equity:,.2f}")
    print(f"Start-of-day equity:   ${inp.starting_equity_today:,.2f}")
    if inp.drawdown_type == "trailing":
        print(f"Peak equity ever:      ${inp.peak_equity:,.2f}")
    else:
        print(f"Starting balance:      ${inp.starting_account_balance:,.2f}")
    print()
    print(f"Daily limit:           {inp.daily_limit_pct}% = ${result.daily_budget_usd:,.2f}  "
          f"(remaining: ${result.daily_budget_remaining_usd:,.2f})")
    print(f"Max drawdown:          {inp.max_drawdown_pct}% "
          f"→ floor ${result.drawdown_floor_usd:,.2f}  "
          f"(room: ${result.drawdown_budget_remaining_usd:,.2f})")
    if inp.open_positions:
        print()
        print(f"Open positions ({len(inp.open_positions)}):")
        for p in inp.open_positions:
            print(f"  • {p.symbol}: ${p.size_usd:,.2f} @ {p.unrealized_pnl_pct:+.2f}%")
        print(f"Correlated-stress reserve: ${result.correlated_stress_loss_usd:,.2f}")
    print("-" * 68)
    print("REASONING:")
    for r in result.reasoning:
        print(f"  • {r}")
    print("-" * 68)
    if result.warnings:
        print("WARNINGS:")
        for w in result.warnings:
            print(f"  ⚠  {w}")
        print("-" * 68)
    print()
    if result.can_trade:
        print(f"  RECOMMENDED SIZE: ${result.recommended_size_usd:,.2f}")
        print(f"  Bound by:         {result.binding_constraint}")
        print(f"  % of equity:      {(result.recommended_size_usd / inp.equity * 100):.1f}%")
        print(f"  Daily-cap max:    ${result.max_size_daily_usd:,.2f}")
        print(f"  DD-cap max:       ${result.max_size_drawdown_usd:,.2f}")
    else:
        print(f"  DO NOT TRADE — {result.binding_constraint}")
    print("=" * 68)
    print()


def main():
    parser = argparse.ArgumentParser(
        description="Breakout Prop sizer (daily limit + max drawdown)",
    )
    parser.add_argument("--equity", type=float, required=True)
    parser.add_argument("--starting-equity", type=float, default=None,
                        help="Equity at 00:30 UTC today (defaults to --equity)")
    parser.add_argument("--starting-balance", type=float, default=None,
                        help="Initial eval balance for static DD (defaults to --equity)")
    parser.add_argument("--peak-equity", type=float, default=None,
                        help="Highest equity ever for trailing DD (defaults to --equity)")
    parser.add_argument("--daily-limit-pct", type=float, required=True,
                        help="4.0 (1-step) or 5.0 (2-step)")
    parser.add_argument("--max-drawdown-pct", type=float, required=True,
                        help="6.0 (1-step) or 8.0 (2-step)")
    parser.add_argument("--drawdown-type", choices=["static", "trailing"], required=True)
    parser.add_argument("--tolerated-drawdown-pct", type=float, required=True)
    parser.add_argument("--slippage-buffer-pct", type=float, default=0.5)
    parser.add_argument("--max-account-pct", type=float, default=60.0)
    parser.add_argument("--correlation-stress-pct", type=float, default=3.0)
    parser.add_argument("--open-position", type=parse_open_position, action="append", default=[])

    args = parser.parse_args()

    inp = SizingInputs(
        equity=args.equity,
        starting_equity_today=args.starting_equity or args.equity,
        starting_account_balance=args.starting_balance or args.equity,
        peak_equity=args.peak_equity or args.equity,
        daily_limit_pct=args.daily_limit_pct,
        max_drawdown_pct=args.max_drawdown_pct,
        drawdown_type=args.drawdown_type,
        tolerated_drawdown_pct=args.tolerated_drawdown_pct,
        slippage_buffer_pct=args.slippage_buffer_pct,
        max_account_pct=args.max_account_pct,
        correlation_stress_pct=args.correlation_stress_pct,
        open_positions=args.open_position,
    )

    result = size_position(inp)
    print_result(result, inp)
    sys.exit(0 if result.can_trade else 1)


if __name__ == "__main__":
    main()