"""agent_loop.py — Agentic Trading Loop.

Three-routine cycle that fires on every 15m candle close:

  Routine 1 — RESEARCH   : collect OHLCV, order book, Fear & Greed, BTC dominance
  Routine 2 — SIGNAL     : evaluate Bot 2 rules, check risk, prompt for confirmation
  Routine 3 — JOURNAL    : write structured Markdown audit log

Usage:
    python agent_loop.py                  # dry-run, all 64 symbols
    python agent_loop.py --live           # live order submission
    python agent_loop.py --symbols BTC/USD ETH/USD SOL/USD
    python agent_loop.py --strategy fvg
    python agent_loop.py --once           # run one cycle immediately and exit
    python agent_loop.py --tf-high 30m --tf-low 15m
"""
import argparse
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dxtrade_client import DXtradeClient, load_env
from candles import SYMBOL_MAP
import research as research_module
import signal_engine
import journal

HERE = Path(__file__).parent

DEFAULT_SYMBOLS = list(SYMBOL_MAP.keys())   # all 64 mapped pairs

CYCLE_MINUTES = 15   # fire on 15m candle close


def seconds_to_next_close(interval_min: int = CYCLE_MINUTES) -> float:
    """Seconds until the next interval-minute boundary (candle close)."""
    now = datetime.now(timezone.utc)
    total_secs = now.minute * 60 + now.second + now.microsecond / 1e6
    interval_secs = interval_min * 60
    elapsed = total_secs % interval_secs
    wait = interval_secs - elapsed
    # Add a 5-second buffer so Kraken has published the closed candle
    return wait + 5.0


def print_banner(live: bool, symbols: list[str], tf_high: str, tf_low: str) -> None:
    print()
    print("=" * 65)
    print(f"  AGENTIC TRADING LOOP  {'[LIVE]' if live else '[DRY-RUN]'}")
    print("=" * 65)
    print(f"  Timeframes  : {tf_high} (signal) / {tf_low} (confirmation)")
    print(f"  Symbols     : {len(symbols)}")
    print(f"  Cycle       : every {CYCLE_MINUTES}m (fires on candle close)")
    print(f"  Execution   : {'LIVE — orders submitted on confirmation' if live else 'DRY-RUN — no orders submitted'}")
    print(f"  Strategy    : both (FVG + trendline)")
    print(f"  Journal     : {HERE / 'journal'}/")
    print("=" * 65)
    print()


def run_one_cycle(
    cycle_num: int,
    client,
    env: dict,
    symbols: list[str],
    strategy: str,
    tf_high: str,
    tf_low: str,
    live: bool,
) -> None:

    now = datetime.now(timezone.utc)
    print(f"\n{'='*65}")
    print(f"  CYCLE {cycle_num}  —  {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*65}")

    # ── Routine 1: Research ───────────────────────────────────────────────────
    print("\n[1/3] RESEARCH")
    packet = research_module.collect(
        symbols    = symbols,
        tf_high    = tf_high,
        tf_low     = tf_low,
        verbose    = True,
    )

    # ── Routine 2: Signal & Execution ────────────────────────────────────────
    print("\n[2/3] SIGNAL & EXECUTION")
    try:
        metrics = client.metrics()
    except Exception as e:
        print(f"  WARNING: could not fetch account metrics: {e}")
        metrics = {}

    decisions = signal_engine.run(
        packet   = packet,
        client   = client,
        env      = env,
        metrics  = metrics,
        symbols  = symbols,
        strategy = strategy,
        tf_high  = tf_high,
        tf_low   = tf_low,
        live     = live,
    )

    # Quick console summary
    signals = [d for d in decisions if d.signal is not None]
    print(f"\n  Signals found: {len(signals)} / {len(decisions)} symbols scanned")
    for d in decisions:
        if d.verdict not in ("NO_SIGNAL", "ERROR"):
            print(f"    {d.symbol:14} {d.verdict}")

    # ── Routine 3: Journal ────────────────────────────────────────────────────
    print("\n[3/3] JOURNAL")
    journal_path = journal.log_cycle(
        packet    = packet,
        decisions = decisions,
        cycle_num = cycle_num,
        live      = live,
    )
    print(f"  Written to {journal_path}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live",     action="store_true", help="submit live orders")
    ap.add_argument("--once",     action="store_true", help="run one cycle immediately and exit")
    ap.add_argument("--symbols",  nargs="*", default=None, help="subset of symbols to scan")
    ap.add_argument("--strategy", default="both", choices=["fvg", "trendline", "both"])
    ap.add_argument("--tf-high",  default="15m")
    ap.add_argument("--tf-low",   default="5m")
    args = ap.parse_args()

    symbols = args.symbols if args.symbols else DEFAULT_SYMBOLS

    env    = load_env(HERE / ".env")
    client = DXtradeClient(env)
    client.login()
    print("  DXtrade: connected")

    print_banner(args.live, symbols, args.tf_high, args.tf_low)
    journal.log_startup(symbols, args.live, args.tf_high, args.tf_low)

    cycle_num = 1

    if args.once:
        run_one_cycle(cycle_num, client, env, symbols, args.strategy,
                      args.tf_high, args.tf_low, args.live)
        return 0

    print("  Waiting for next 15m candle close...\n")

    while True:
        wait = seconds_to_next_close(CYCLE_MINUTES)
        next_fire = datetime.now(timezone.utc) + timedelta(seconds=wait)
        print(f"  Next cycle at {next_fire.strftime('%H:%M:%S UTC')} "
              f"(in {int(wait // 60)}m {int(wait % 60):02d}s)")

        try:
            time.sleep(wait)
        except KeyboardInterrupt:
            print("\n  Interrupted. Exiting.")
            return 0

        try:
            # Re-login each cycle (session tokens can expire)
            client.login()
            run_one_cycle(cycle_num, client, env, symbols, args.strategy,
                          args.tf_high, args.tf_low, args.live)
        except KeyboardInterrupt:
            print("\n  Interrupted during cycle. Exiting.")
            return 0
        except Exception as e:
            print(f"\n  ERROR in cycle {cycle_num}: {e}")
            print("  Continuing to next cycle...")

        cycle_num += 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\naborted.")
        sys.exit(130)
