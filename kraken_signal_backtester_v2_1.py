#!/usr/bin/env python3
import argparse
import csv
import json
import math
import sys
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import ccxt
from dotenv import load_dotenv


load_dotenv()


# =========================
# Defaults
# =========================
DEFAULT_EVENTS_FILE = "kraken_signal_events_v2_1.jsonl"
DEFAULT_TIMEFRAME = "1m"
DEFAULT_EXTRA_BARS_BUFFER = 10
DEFAULT_FETCH_RETRIES = 3
DEFAULT_SLEEP_BETWEEN_REQUESTS_S = 1.2
DEFAULT_OUTPUT_TRADES_CSV = "kraken_backtest_results_v2_1.csv"
DEFAULT_OUTPUT_SUMMARY_JSON = "kraken_backtest_summary_v2_1.json"
DEFAULT_OUTPUT_SKIPPED_CSV = "kraken_backtest_skipped_v2_1.csv"


# =========================
# Helpers
# =========================
def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        if isinstance(value, bool):
            return float(value)
        f = float(value)
        if math.isnan(f) or math.isinf(f):
            return default
        return f
    except Exception:
        return default


def pct_change(entry_price: float, price: float) -> float:
    if entry_price <= 0:
        return 0.0
    return ((price / entry_price) - 1.0) * 100.0


def parse_iso_to_ms(ts: str) -> int:
    dt = datetime.fromisoformat(ts)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def floor_minute_ms(ts_ms: int) -> int:
    return (ts_ms // 60000) * 60000


def next_minute_ms(ts_ms: int) -> int:
    return floor_minute_ms(ts_ms) + 60000


def ensure_parent_dir(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def candle_to_dict(c: List[Any]) -> Dict[str, Any]:
    # ccxt ohlcv: [timestamp, open, high, low, close, volume]
    return {
        "ts_ms": int(c[0]),
        "open": safe_float(c[1]),
        "high": safe_float(c[2]),
        "low": safe_float(c[3]),
        "close": safe_float(c[4]),
        "volume": safe_float(c[5]),
    }


def minute_diff(entry_ms: int, ts_ms: int) -> int:
    return int((ts_ms - entry_ms) // 60000)


# =========================
# Data models
# =========================
@dataclass
class BacktestTradeResult:
    symbol: str
    event: str
    entry_ts_utc: str
    entry_ts_ms: int
    entry_price: float
    exit_ts_utc: str
    exit_ts_ms: int
    exit_price: float
    exit_reason: str
    hold_minutes: int
    pnl_pct_gross: float
    pnl_pct_net: float
    fee_bps_each_side: float
    slippage_bps_each_side: float
    mfe_pct: float
    mae_pct: float
    peak_price: float
    trough_price: float
    hard_stop_pct: float
    trailing_stop_pct: float
    move_to_be_after_gain_pct: float
    arm_trailing_after_gain_pct: float
    time_stop_minutes: int
    max_hold_minutes: int
    confidence: float
    v1_quality_score: float
    strength_score: float
    scanner_score: float
    scanner_change_pct: float
    scanner_spread_pct: float
    scanner_quote_vol: float
    break_even_promoted: bool
    trailing_armed: bool
    trailing_stop_last: float
    peak_gain_pct: float
    skipped: bool = False
    skipped_reason: str = ""


@dataclass
class SkippedEvent:
    symbol: str
    event: str
    ts_utc: str
    reason: str


# =========================
# Event loading
# =========================
def load_would_buy_events(events_file: Path) -> Tuple[List[Dict[str, Any]], List[SkippedEvent]]:
    events: List[Dict[str, Any]] = []
    skipped: List[SkippedEvent] = []

    with events_file.open("r", encoding="utf-8") as f:
        for line_no, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw:
                continue

            try:
                obj = json.loads(raw)
            except Exception as e:
                skipped.append(
                    SkippedEvent(symbol="", event="", ts_utc="", reason=f"json_parse_error_line_{line_no}:{e}")
                )
                continue

            ev = str(obj.get("event", ""))
            symbol = str(obj.get("symbol", ""))
            ts_utc = str(obj.get("ts_utc", ""))
            signal = obj.get("signal", {}) if isinstance(obj.get("signal"), dict) else {}
            exit_plan = signal.get("exit_plan", {}) if isinstance(signal.get("exit_plan"), dict) else {}

            if ev != "would_buy_v2_1":
                continue

            if not symbol:
                skipped.append(SkippedEvent(symbol="", event=ev, ts_utc=ts_utc, reason="missing_symbol"))
                continue

            if not ts_utc:
                skipped.append(SkippedEvent(symbol=symbol, event=ev, ts_utc="", reason="missing_ts_utc"))
                continue

            if not signal:
                skipped.append(SkippedEvent(symbol=symbol, event=ev, ts_utc=ts_utc, reason="missing_signal"))
                continue

            if not exit_plan:
                skipped.append(SkippedEvent(symbol=symbol, event=ev, ts_utc=ts_utc, reason="missing_exit_plan"))
                continue

            entry_price = safe_float(signal.get("last_close"), 0.0)
            if entry_price <= 0:
                skipped.append(
                    SkippedEvent(symbol=symbol, event=ev, ts_utc=ts_utc, reason="missing_or_invalid_signal.last_close")
                )
                continue

            events.append(obj)

    # deterministic order
    events.sort(key=lambda x: (str(x.get("ts_utc", "")), str(x.get("symbol", ""))))
    return events, skipped


# =========================
# Kraken OHLCV fetch
# =========================
class KrakenOhlcvFetcher:
    def __init__(self, sleep_between_requests_s: float = DEFAULT_SLEEP_BETWEEN_REQUESTS_S):
        self.exchange = ccxt.kraken({"enableRateLimit": True})
        self.sleep_between_requests_s = sleep_between_requests_s
        self._cache: Dict[Tuple[str, int, int], List[Dict[str, Any]]] = {}
        self._markets_loaded = False

    def close(self) -> None:
        try:
            self.exchange.close()
        except Exception:
            pass

    def ensure_markets(self) -> None:
        if not self._markets_loaded:
            self.exchange.load_markets()
            self._markets_loaded = True

    def fetch_ohlcv_1m(self, symbol: str, since_ms: int, limit: int, retries: int) -> List[Dict[str, Any]]:
        key = (symbol, since_ms, limit)
        if key in self._cache:
            return self._cache[key]

        self.ensure_markets()

        last_err: Optional[Exception] = None
        for attempt in range(1, retries + 1):
            try:
                rows = self.exchange.fetch_ohlcv(symbol, timeframe="1m", since=since_ms, limit=limit)
                out = [candle_to_dict(r) for r in rows]
                self._cache[key] = out
                if self.sleep_between_requests_s > 0:
                    time.sleep(self.sleep_between_requests_s)
                return out
            except Exception as e:
                last_err = e
                if attempt < retries:
                    # small backoff
                    time.sleep(min(1.0 * attempt, 3.0))
                continue

        raise RuntimeError(f"fetch_ohlcv failed for {symbol} after {retries} attempts: {last_err}")


# =========================
# Exit simulation engine
# =========================
def simulate_dynamic_exit(
    event_obj: Dict[str, Any],
    future_candles: List[Dict[str, Any]],
    fee_bps_each_side: float,
    slippage_bps_each_side: float,
) -> BacktestTradeResult:
    symbol = str(event_obj.get("symbol", ""))
    event = str(event_obj.get("event", ""))
    ts_utc = str(event_obj.get("ts_utc", ""))
    entry_ts_ms = parse_iso_to_ms(ts_utc)

    scanner = event_obj.get("scanner", {}) if isinstance(event_obj.get("scanner"), dict) else {}
    signal = event_obj.get("signal", {}) if isinstance(event_obj.get("signal"), dict) else {}
    plan = signal.get("exit_plan", {}) if isinstance(signal.get("exit_plan"), dict) else {}
    momentum_fail = plan.get("momentum_fail", {}) if isinstance(plan.get("momentum_fail"), dict) else {}

    entry_price = safe_float(signal.get("last_close"), 0.0)
    if entry_price <= 0:
        raise ValueError(f"Invalid entry price for {symbol}")

    hard_stop_pct = safe_float(plan.get("hard_stop_pct"), 0.0)
    trailing_stop_pct = safe_float(plan.get("trailing_stop_pct"), 0.0)
    move_to_be_after_gain_pct = safe_float(plan.get("move_to_break_even_after_gain_pct"), 0.0)
    arm_trailing_after_gain_pct = safe_float(plan.get("arm_trailing_after_gain_pct"), 0.0)
    time_stop_minutes = int(safe_float(plan.get("time_stop_minutes"), 0))
    max_hold_minutes = int(safe_float(plan.get("max_hold_minutes"), 0))

    be_trigger_price = entry_price * (1.0 + move_to_be_after_gain_pct / 100.0)
    trail_arm_trigger_price = entry_price * (1.0 + arm_trailing_after_gain_pct / 100.0)

    initial_stop_price = entry_price * (1.0 - hard_stop_pct / 100.0)
    active_stop_price = initial_stop_price

    break_even_promoted = False
    trailing_armed = False
    trailing_stop_last = 0.0

    peak_price = entry_price
    trough_price = entry_price
    peak_gain_pct = 0.0
    consec_red_closes = 0
    prev_close: Optional[float] = None

    mf_enabled = bool(momentum_fail.get("enabled", False))
    mf_check_after_min = int(safe_float(momentum_fail.get("check_after_minutes"), 0))
    mf_min_gain_pct = safe_float(momentum_fail.get("min_unrealized_gain_pct_to_avoid_fail"), 0.0)
    mf_max_dd_early_pct = safe_float(momentum_fail.get("max_drawdown_from_peak_pct_early"), 0.0)
    mf_two_red_after_min = int(safe_float(momentum_fail.get("two_red_closes_exit_after_minutes"), 0))

    if not future_candles:
        raise ValueError(f"No future candles for {symbol}")

    exit_reason = ""
    exit_price = 0.0
    exit_ts_ms = 0

    for c in future_candles:
        c_ts = int(c["ts_ms"])
        o = safe_float(c.get("open"), 0.0)
        h = safe_float(c.get("high"), 0.0)
        l = safe_float(c.get("low"), 0.0)
        cl = safe_float(c.get("close"), 0.0)

        if min(o, h, l, cl) <= 0:
            continue

        elapsed_min = max(1, minute_diff(entry_ts_ms, c_ts))

        trough_price = min(trough_price, l)
        peak_price = max(peak_price, h)
        peak_gain_pct = max(peak_gain_pct, pct_change(entry_price, peak_price))

        # Maintain consecutive red closes using close to close direction
        if prev_close is not None and cl < prev_close:
            consec_red_closes += 1
        else:
            consec_red_closes = 0
        prev_close = cl

        # 1. hard stop
        if l <= active_stop_price:
            exit_reason = "hard_stop" if not (break_even_promoted or trailing_armed) else "stop_hit"
            exit_price = active_stop_price
            exit_ts_ms = c_ts
            break

        # 2. move stop to break even when trigger is hit
        if (not break_even_promoted) and (h >= be_trigger_price):
            break_even_promoted = True
            active_stop_price = max(active_stop_price, entry_price)

        # 3. arm trailing when trigger is hit
        if (not trailing_armed) and (h >= trail_arm_trigger_price):
            trailing_armed = True

        # update trailing stop if armed
        if trailing_armed and trailing_stop_pct > 0:
            trail_candidate = peak_price * (1.0 - trailing_stop_pct / 100.0)
            trailing_stop_last = max(trailing_stop_last, trail_candidate)
            active_stop_price = max(active_stop_price, trailing_stop_last)

        # 4. trailing stop
        if trailing_armed and l <= active_stop_price:
            exit_reason = "trailing_stop" if active_stop_price > entry_price else "stop_hit"
            exit_price = active_stop_price
            exit_ts_ms = c_ts
            break

        # 5. momentum failure rule
        if mf_enabled:
            if elapsed_min >= mf_check_after_min:
                current_gain_pct = pct_change(entry_price, cl)
                drawdown_from_peak_pct = peak_gain_pct - current_gain_pct

                if peak_gain_pct < mf_min_gain_pct and current_gain_pct <= 0:
                    exit_reason = "momentum_fail_no_progress"
                    exit_price = cl
                    exit_ts_ms = c_ts
                    break

                if peak_gain_pct >= mf_min_gain_pct and drawdown_from_peak_pct >= mf_max_dd_early_pct:
                    exit_reason = "momentum_fail_drawdown"
                    exit_price = cl
                    exit_ts_ms = c_ts
                    break

            if elapsed_min >= mf_two_red_after_min and consec_red_closes >= 2:
                exit_reason = "momentum_fail_two_red_closes"
                exit_price = cl
                exit_ts_ms = c_ts
                break

        # 6. time stop fallback
        if time_stop_minutes > 0 and elapsed_min >= time_stop_minutes:
            exit_reason = "time_stop"
            exit_price = cl
            exit_ts_ms = c_ts
            break

        # safety cap if present
        if max_hold_minutes > 0 and elapsed_min >= max_hold_minutes:
            exit_reason = "max_hold"
            exit_price = cl
            exit_ts_ms = c_ts
            break

    if not exit_reason:
        last = future_candles[-1]
        exit_reason = "data_end"
        exit_price = safe_float(last.get("close"), entry_price)
        exit_ts_ms = int(last.get("ts_ms", entry_ts_ms))

    effective_entry = entry_price * (1.0 + (slippage_bps_each_side / 10000.0))
    effective_exit = exit_price * (1.0 - (slippage_bps_each_side / 10000.0))
    fee_pct_total = (fee_bps_each_side * 2.0) / 100.0

    pnl_pct_gross = pct_change(entry_price, exit_price)
    pnl_pct_net = pct_change(effective_entry, effective_exit) - fee_pct_total

    hold_minutes = max(0, minute_diff(entry_ts_ms, exit_ts_ms))

    result = BacktestTradeResult(
        symbol=symbol,
        event=event,
        entry_ts_utc=datetime.fromtimestamp(entry_ts_ms / 1000, tz=timezone.utc).isoformat(),
        entry_ts_ms=entry_ts_ms,
        entry_price=round(entry_price, 10),
        exit_ts_utc=datetime.fromtimestamp(exit_ts_ms / 1000, tz=timezone.utc).isoformat(),
        exit_ts_ms=exit_ts_ms,
        exit_price=round(exit_price, 10),
        exit_reason=exit_reason,
        hold_minutes=int(hold_minutes),
        pnl_pct_gross=round(pnl_pct_gross, 6),
        pnl_pct_net=round(pnl_pct_net, 6),
        fee_bps_each_side=round(fee_bps_each_side, 6),
        slippage_bps_each_side=round(slippage_bps_each_side, 6),
        mfe_pct=round(pct_change(entry_price, peak_price), 6),
        mae_pct=round(pct_change(entry_price, trough_price), 6),
        peak_price=round(peak_price, 10),
        trough_price=round(trough_price, 10),
        hard_stop_pct=round(hard_stop_pct, 6),
        trailing_stop_pct=round(trailing_stop_pct, 6),
        move_to_be_after_gain_pct=round(move_to_be_after_gain_pct, 6),
        arm_trailing_after_gain_pct=round(arm_trailing_after_gain_pct, 6),
        time_stop_minutes=int(time_stop_minutes),
        max_hold_minutes=int(max_hold_minutes),
        confidence=round(safe_float(signal.get("confidence"), 0.0), 6),
        v1_quality_score=round(safe_float(signal.get("v1_quality_score"), 0.0), 6),
        strength_score=round(safe_float(plan.get("strength_score"), 0.0), 6),
        scanner_score=round(safe_float(scanner.get("score"), 0.0), 6),
        scanner_change_pct=round(safe_float(scanner.get("change_pct"), 0.0), 6),
        scanner_spread_pct=round(safe_float(scanner.get("spread_pct"), 0.0), 6),
        scanner_quote_vol=round(safe_float(scanner.get("quote_vol"), 0.0), 6),
        break_even_promoted=bool(break_even_promoted),
        trailing_armed=bool(trailing_armed),
        trailing_stop_last=round(trailing_stop_last, 10),
        peak_gain_pct=round(peak_gain_pct, 6),
    )
    return result


# =========================
# Output
# =========================
def write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    ensure_parent_dir(path)
    if not rows:
        with path.open("w", newline="", encoding="utf-8") as f:
            f.write("")
        return

    headers = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def build_summary(results: List[BacktestTradeResult], skipped: List[SkippedEvent], meta: Dict[str, Any]) -> Dict[str, Any]:
    total = len(results)
    pnl_net = [r.pnl_pct_net for r in results]
    pnl_gross = [r.pnl_pct_gross for r in results]

    wins = [r for r in results if r.pnl_pct_net > 0]
    losses = [r for r in results if r.pnl_pct_net < 0]
    flats = [r for r in results if r.pnl_pct_net == 0]

    by_reason: Dict[str, Dict[str, Any]] = {}
    for r in results:
        d = by_reason.setdefault(
            r.exit_reason,
            {
                "count": 0,
                "avg_pnl_pct_net": 0.0,
                "avg_hold_minutes": 0.0,
                "wins": 0,
                "losses": 0,
            },
        )
        d["count"] += 1
        d["avg_pnl_pct_net"] += r.pnl_pct_net
        d["avg_hold_minutes"] += r.hold_minutes
        if r.pnl_pct_net > 0:
            d["wins"] += 1
        elif r.pnl_pct_net < 0:
            d["losses"] += 1

    for d in by_reason.values():
        c = max(d["count"], 1)
        d["avg_pnl_pct_net"] = round(d["avg_pnl_pct_net"] / c, 6)
        d["avg_hold_minutes"] = round(d["avg_hold_minutes"] / c, 3)

    avg_net = round(sum(pnl_net) / total, 6) if total else 0.0
    avg_gross = round(sum(pnl_gross) / total, 6) if total else 0.0
    median_net = round(sorted(pnl_net)[len(pnl_net) // 2], 6) if total else 0.0

    win_rate = round((len(wins) / total) * 100.0, 3) if total else 0.0
    avg_win = round(sum(r.pnl_pct_net for r in wins) / len(wins), 6) if wins else 0.0
    avg_loss = round(sum(r.pnl_pct_net for r in losses) / len(losses), 6) if losses else 0.0

    expectancy = round(
        ((len(wins) / total) * avg_win + (len(losses) / total) * avg_loss) if total else 0.0,
        6,
    )

    summary = {
        "generated_at_utc": now_utc_iso(),
        "meta": meta,
        "totals": {
            "events_backtested": total,
            "skipped_events": len(skipped),
            "wins": len(wins),
            "losses": len(losses),
            "flats": len(flats),
            "win_rate_pct": win_rate,
            "avg_pnl_pct_gross": avg_gross,
            "avg_pnl_pct_net": avg_net,
            "median_pnl_pct_net": median_net,
            "avg_win_pct_net": avg_win,
            "avg_loss_pct_net": avg_loss,
            "expectancy_pct_net": expectancy,
            "sum_pnl_pct_net": round(sum(pnl_net), 6) if total else 0.0,
        },
        "exit_reason_breakdown": by_reason,
        "notes": [
            "Bar based approximation using OHLCV only. Intrabar order is assumed using the requested rule order.",
            "Entry uses signal.last_close from the event file. Future evaluation begins on the next minute candle after event timestamp.",
            "Partials metadata is currently ignored for position sizing and only the final exit is simulated.",
        ],
    }
    return summary


# =========================
# Main
# =========================
def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backtest Kraken v2.1 WOULD BUY signals using dynamic exit plans logged in kraken_signal_events_v2_1.jsonl"
    )
    parser.add_argument("--events", default=DEFAULT_EVENTS_FILE, help="Path to kraken_signal_events_v2_1.jsonl")
    parser.add_argument("--out-trades", default=DEFAULT_OUTPUT_TRADES_CSV, help="Output CSV for trade results")
    parser.add_argument("--out-summary", default=DEFAULT_OUTPUT_SUMMARY_JSON, help="Output summary JSON")
    parser.add_argument("--out-skipped", default=DEFAULT_OUTPUT_SKIPPED_CSV, help="Output CSV for skipped events")
    parser.add_argument("--limit-events", type=int, default=0, help="Only backtest the first N would_buy events (0 = all)")
    parser.add_argument("--sleep", type=float, default=DEFAULT_SLEEP_BETWEEN_REQUESTS_S, help="Sleep seconds between Kraken requests")
    parser.add_argument("--retries", type=int, default=DEFAULT_FETCH_RETRIES, help="OHLCV fetch retries per request")
    parser.add_argument("--extra-bars", type=int, default=DEFAULT_EXTRA_BARS_BUFFER, help="Extra bars fetched past max_hold")
    parser.add_argument("--fee-bps", type=float, default=0.0, help="Fee bps per side")
    parser.add_argument("--slippage-bps", type=float, default=0.0, help="Slippage bps per side")
    parser.add_argument("--verbose", action="store_true", help="Print per trade status")
    args = parser.parse_args()

    events_path = Path(args.events).expanduser().resolve()
    if not events_path.exists():
        print(f"ERROR: events file not found: {events_path}", file=sys.stderr)
        return 1

    events, skipped_events = load_would_buy_events(events_path)
    if args.limit_events and args.limit_events > 0:
        events = events[: args.limit_events]

    if not events:
        print("No would_buy_v2_1 events found to backtest")
        return 1

    print(f"Loaded {len(events)} would_buy_v2_1 events from {events_path}")

    fetcher = KrakenOhlcvFetcher(sleep_between_requests_s=max(args.sleep, 0.0))
    results: List[BacktestTradeResult] = []

    try:
        for idx, ev in enumerate(events, start=1):
            symbol = str(ev.get("symbol", ""))
            ts_utc = str(ev.get("ts_utc", ""))
            signal = ev.get("signal", {}) if isinstance(ev.get("signal"), dict) else {}
            plan = signal.get("exit_plan", {}) if isinstance(signal.get("exit_plan"), dict) else {}

            entry_ts_ms = parse_iso_to_ms(ts_utc)
            since_ms = next_minute_ms(entry_ts_ms)
            max_hold_minutes = int(safe_float(plan.get("max_hold_minutes"), 0))
            time_stop_minutes = int(safe_float(plan.get("time_stop_minutes"), 0))
            bars_needed = max(max_hold_minutes, time_stop_minutes, 5) + max(args.extra_bars, 0)

            try:
                future_candles = fetcher.fetch_ohlcv_1m(
                    symbol=symbol,
                    since_ms=since_ms,
                    limit=bars_needed,
                    retries=max(args.retries, 1),
                )
                future_candles = [c for c in future_candles if int(c.get("ts_ms", 0)) >= since_ms]

                if not future_candles:
                    raise RuntimeError("no_future_candles_returned")

                trade = simulate_dynamic_exit(
                    ev,
                    future_candles=future_candles,
                    fee_bps_each_side=max(args.fee_bps, 0.0),
                    slippage_bps_each_side=max(args.slippage_bps, 0.0),
                )
                results.append(trade)

                if args.verbose:
                    print(
                        f"[{idx}/{len(events)}] {trade.symbol} | entry={trade.entry_price} | exit={trade.exit_price} | "
                        f"{trade.exit_reason} | pnl_net={trade.pnl_pct_net:.4f}% | hold={trade.hold_minutes}m"
                    )

            except Exception as e:
                skipped_events.append(
                    SkippedEvent(symbol=symbol, event=str(ev.get("event", "")), ts_utc=ts_utc, reason=f"backtest_error:{e}")
                )
                if args.verbose:
                    print(f"[{idx}/{len(events)}] SKIP {symbol} | {e}")
                continue

    finally:
        fetcher.close()

    out_trades = Path(args.out_trades).expanduser().resolve()
    out_summary = Path(args.out_summary).expanduser().resolve()
    out_skipped = Path(args.out_skipped).expanduser().resolve()

    trade_rows = [asdict(r) for r in results]
    skipped_rows = [asdict(s) for s in skipped_events]

    write_csv(out_trades, trade_rows)
    write_csv(out_skipped, skipped_rows if skipped_rows else [])

    summary = build_summary(
        results,
        skipped_events,
        meta={
            "events_file": str(events_path),
            "fee_bps_each_side": args.fee_bps,
            "slippage_bps_each_side": args.slippage_bps,
            "sleep_between_requests_s": args.sleep,
            "retries": args.retries,
            "extra_bars": args.extra_bars,
            "timeframe": DEFAULT_TIMEFRAME,
            "script": "kraken_signal_backtester_v2_1.py",
        },
    )

    ensure_parent_dir(out_summary)
    with out_summary.open("w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print("\nBacktest complete")
    print(f"Trades:   {len(results)}")
    print(f"Skipped:  {len(skipped_events)}")
    print(f"Trades CSV:   {out_trades}")
    print(f"Summary JSON: {out_summary}")
    print(f"Skipped CSV:  {out_skipped}")

    totals = summary.get("totals", {})
    print(
        "Summary | "
        f"win_rate={totals.get('win_rate_pct', 0)}% | "
        f"avg_net={totals.get('avg_pnl_pct_net', 0)}% | "
        f"sum_net={totals.get('sum_pnl_pct_net', 0)}% | "
        f"expectancy={totals.get('expectancy_pct_net', 0)}%"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
