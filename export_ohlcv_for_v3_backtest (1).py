import os
import json
import csv
from datetime import datetime, timezone, timedelta
from typing import Dict, Set, Tuple, List, Optional


# =========================
# CONFIG
# =========================
V3_1_JSONL = "kraken_signal_events_v3_1.jsonl"
V3_2_JSONL = "kraken_signal_events_v3_2.jsonl"

INPUT_OHLCV_CSV = "ohlcv_1m_for_v3_backtest.csv"
OUTPUT_OHLCV_CSV = "ohlcv_1m_for_v3_backtest_trimmed_needed_window.csv"

# Buffers around combined signal window
START_BUFFER_MINUTES = 30
END_BUFFER_MINUTES = 120


# =========================
# HELPERS
# =========================
def parse_iso_utc(ts: str) -> datetime:
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat()


def load_signal_window_and_symbols(path: str) -> Tuple[Set[str], List[datetime]]:
    symbols: Set[str] = set()
    times: List[datetime] = []

    if not os.path.exists(path):
        print(f"[WARN] Missing signal file: {path}")
        return symbols, times

    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                print(f"[WARN] JSON parse error in {path}:{line_num}: {e}")
                continue

            sym = obj.get("symbol")
            ts = obj.get("ts_utc")

            if isinstance(sym, str) and sym.strip():
                symbols.add(sym.strip())

            if isinstance(ts, str) and ts.strip():
                try:
                    times.append(parse_iso_utc(ts))
                except Exception as e:
                    print(f"[WARN] Bad ts_utc in {path}:{line_num}: {ts} ({e})")

    return symbols, times


# =========================
# MAIN
# =========================
def main() -> None:
    if not os.path.exists(INPUT_OHLCV_CSV):
        print(f"[ERROR] Missing input OHLCV CSV: {INPUT_OHLCV_CSV}")
        return

    s1, t1 = load_signal_window_and_symbols(V3_1_JSONL)
    s2, t2 = load_signal_window_and_symbols(V3_2_JSONL)

    all_symbols = sorted(s1 | s2)
    all_times = t1 + t2

    if not all_symbols:
        print("[ERROR] No symbols found in signal files")
        return
    if not all_times:
        print("[ERROR] No timestamps found in signal files")
        return

    first_signal = min(all_times)
    last_signal = max(all_times)

    need_start = first_signal - timedelta(minutes=START_BUFFER_MINUTES)
    need_end = last_signal + timedelta(minutes=END_BUFFER_MINUTES)

    print(f"[INFO] v3_1 symbols: {len(s1)}")
    print(f"[INFO] v3_2 symbols: {len(s2)}")
    print(f"[INFO] Combined symbols: {len(all_symbols)}")
    print(f"[INFO] Signal window: {iso(first_signal)} -> {iso(last_signal)}")
    print(f"[INFO] Needed OHLCV window: {iso(need_start)} -> {iso(need_end)}")

    rows_in = 0
    rows_out = 0

    # Coverage diagnostics
    seen_symbols_in_range: Set[str] = set()
    min_ohlcv_dt: Optional[datetime] = None
    max_ohlcv_dt: Optional[datetime] = None

    with open(INPUT_OHLCV_CSV, "r", encoding="utf-8", newline="") as fin, \
         open(OUTPUT_OHLCV_CSV, "w", encoding="utf-8", newline="") as fout:

        reader = csv.DictReader(fin)
        fieldnames = ["symbol", "timestamp_utc", "open", "high", "low", "close", "volume"]
        writer = csv.DictWriter(fout, fieldnames=fieldnames)
        writer.writeheader()

        for row in reader:
            rows_in += 1

            symbol = (row.get("symbol") or "").strip()
            ts_raw = (row.get("timestamp_utc") or "").strip()

            if not symbol or not ts_raw:
                continue

            try:
                ts_dt = parse_iso_utc(ts_raw)
            except Exception:
                continue

            # Track source OHLCV global min/max
            if min_ohlcv_dt is None or ts_dt < min_ohlcv_dt:
                min_ohlcv_dt = ts_dt
            if max_ohlcv_dt is None or ts_dt > max_ohlcv_dt:
                max_ohlcv_dt = ts_dt

            if symbol not in all_symbols:
                continue
            if ts_dt < need_start or ts_dt > need_end:
                continue

            writer.writerow(
                {
                    "symbol": symbol,
                    "timestamp_utc": iso(ts_dt),
                    "open": row.get("open", ""),
                    "high": row.get("high", ""),
                    "low": row.get("low", ""),
                    "close": row.get("close", ""),
                    "volume": row.get("volume", ""),
                }
            )
            rows_out += 1
            seen_symbols_in_range.add(symbol)

    print(f"[INFO] Input OHLCV rows scanned: {rows_in}")
    print(f"[INFO] Output OHLCV rows written: {rows_out}")
    if min_ohlcv_dt and max_ohlcv_dt:
        print(f"[INFO] Input OHLCV coverage: {iso(min_ohlcv_dt)} -> {iso(max_ohlcv_dt)}")

    missing_symbols = [s for s in all_symbols if s not in seen_symbols_in_range]
    print(f"[INFO] Symbols with OHLCV in needed range: {len(seen_symbols_in_range)} / {len(all_symbols)}")

    if missing_symbols:
        print("[WARN] Missing symbols in trimmed output (not present in input OHLCV during needed window):")
        for s in missing_symbols[:50]:
            print(f"  - {s}")
        if len(missing_symbols) > 50:
            print(f"  ... and {len(missing_symbols) - 50} more")

    # Hard coverage warning if source OHLCV does not span needed time window
    if min_ohlcv_dt and max_ohlcv_dt:
        if min_ohlcv_dt > need_start or max_ohlcv_dt < need_end:
            print("[WARN] SOURCE OHLCV FILE DOES NOT FULLY COVER THE NEEDED TIME WINDOW.")
            print("[WARN] You need to re-export OHLCV using the exact v3_1 + v3_2 JSONLs for a full backtest.")
        else:
            print("[INFO] Source OHLCV file covers the needed time window.")

    print(f"[DONE] Wrote: {OUTPUT_OHLCV_CSV}")


if __name__ == "__main__":
    main()
