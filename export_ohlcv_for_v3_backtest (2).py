import os
import json
import time
import math
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set, Tuple, Optional

import ccxt.async_support as ccxt
from dotenv import load_dotenv

load_dotenv()

V3_1_JSONL = "kraken_signal_events_v3_1.jsonl"
V3_2_JSONL = "kraken_signal_events_v3_2.jsonl"
OUT_CSV = "ohlcv_1m_for_v3_backtest.csv"

TIMEFRAME = "1m"
SINCE_BUFFER_MINUTES = 30
UNTIL_BUFFER_MINUTES = 120

# Kraken 1m candles usually paginate in chunks
FETCH_LIMIT = 720  # 12 hours per call if exchange honors it
SLEEP_BETWEEN_CALLS_SEC = 0.35

ENABLE_RATE_LIMIT = True
HTTP_TIMEOUT_MS = 30000


def parse_iso_utc(ts: str) -> datetime:
    # Handles "Z" and "+00:00"
    ts = ts.strip()
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts).astimezone(timezone.utc)


def dt_to_ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def ms_to_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


def load_signal_file(path: str) -> Tuple[Set[str], List[datetime]]:
    symbols: Set[str] = set()
    times: List[datetime] = []

    if not os.path.exists(path):
        print(f"[WARN] Missing file: {path}")
        return symbols, times

    with open(path, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except Exception as e:
                print(f"[WARN] JSON parse error {path}:{line_num}: {e}")
                continue

            symbol = obj.get("symbol")
            ts = obj.get("ts_utc")

            # Keep both signal and reject events because we still want full symbol coverage
            if symbol and isinstance(symbol, str):
                symbols.add(symbol)

            if ts and isinstance(ts, str):
                try:
                    times.append(parse_iso_utc(ts))
                except Exception as e:
                    print(f"[WARN] Bad ts_utc {path}:{line_num}: {ts} ({e})")

    return symbols, times


async def make_exchange() -> ccxt.kraken:
    return ccxt.kraken(
        {
            "apiKey": os.getenv("KRAKEN_API_KEY", ""),
            "secret": os.getenv("KRAKEN_API_SECRET", ""),
            "enableRateLimit": ENABLE_RATE_LIMIT,
            "timeout": HTTP_TIMEOUT_MS,
            "options": {
                "adjustForTimeDifference": True,
            },
        }
    )


async def fetch_symbol_ohlcv_range(
    exchange: ccxt.kraken,
    symbol: str,
    since_ms: int,
    until_ms: int,
    timeframe: str = TIMEFRAME,
) -> List[List[float]]:
    all_rows: List[List[float]] = []
    cursor = since_ms

    while cursor < until_ms:
        try:
            rows = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, since=cursor, limit=FETCH_LIMIT)
        except Exception as e:
            print(f"[WARN] fetch_ohlcv failed for {symbol} at {ms_to_iso(cursor)}: {e}")
            break

        if not rows:
            break

        # Dedup and keep only rows in requested range
        added = 0
        for r in rows:
            if not r or len(r) < 6:
                continue
            ts_ms = int(r[0])
            if ts_ms < since_ms:
                continue
            if ts_ms > until_ms:
                continue
            if all_rows and ts_ms <= int(all_rows[-1][0]):
                continue
            all_rows.append(r[:6])
            added += 1

        last_ts = int(rows[-1][0])

        # Advance cursor by one minute beyond last returned row
        next_cursor = last_ts + 60_000

        if next_cursor <= cursor:
            break

        cursor = next_cursor

        # If exchange returned less than limit, we may be near the end
        if len(rows) < FETCH_LIMIT and last_ts >= until_ms:
            break

        await asyncio.sleep(SLEEP_BETWEEN_CALLS_SEC)

    return all_rows


async def main() -> None:
    s1, t1 = load_signal_file(V3_1_JSONL)
    s2, t2 = load_signal_file(V3_2_JSONL)

    symbols = sorted(s1 | s2)
    all_times = t1 + t2

    if not symbols:
        print("[ERROR] No symbols found in signal files.")
        return

    if not all_times:
        print("[ERROR] No timestamps found in signal files.")
        return

    first_ts = min(all_times)
    last_ts = max(all_times)

    start_dt = first_ts - timedelta(minutes=SINCE_BUFFER_MINUTES)
    end_dt = last_ts + timedelta(minutes=UNTIL_BUFFER_MINUTES)

    since_ms = dt_to_ms(start_dt)
    until_ms = dt_to_ms(end_dt)

    print(f"[INFO] Symbols found: {len(symbols)}")
    print(f"[INFO] Signal window: {first_ts.isoformat()} -> {last_ts.isoformat()}")
    print(f"[INFO] Fetch window:  {start_dt.isoformat()} -> {end_dt.isoformat()}")
    print(f"[INFO] Output CSV: {OUT_CSV}")

    exchange = await make_exchange()

    try:
        # load markets first to validate symbols
        await exchange.load_markets()

        valid_symbols = []
        for s in symbols:
            if s in exchange.markets:
                valid_symbols.append(s)
            else:
                print(f"[WARN] Symbol not in Kraken markets now: {s}")

        print(f"[INFO] Valid symbols on Kraken now: {len(valid_symbols)} / {len(symbols)}")

        total_rows = 0

        with open(OUT_CSV, "w", encoding="utf-8") as out:
            out.write("symbol,timestamp_utc,open,high,low,close,volume\n")

            for i, symbol in enumerate(valid_symbols, 1):
                print(f"[INFO] ({i}/{len(valid_symbols)}) Fetching {symbol}")
                rows = await fetch_symbol_ohlcv_range(exchange, symbol, since_ms, until_ms, timeframe=TIMEFRAME)

                kept = 0
                for r in rows:
                    ts_ms, o, h, l, c, v = r[:6]
                    if ts_ms < since_ms or ts_ms > until_ms:
                        continue
                    out.write(
                        f"{symbol},{ms_to_iso(int(ts_ms))},{o},{h},{l},{c},{v}\n"
                    )
                    kept += 1

                total_rows += kept
                print(f"[INFO] {symbol}: wrote {kept} rows")
                await asyncio.sleep(SLEEP_BETWEEN_CALLS_SEC)

        print(f"[DONE] Export complete. Total rows written: {total_rows}")
        print(f"[DONE] File: {OUT_CSV}")

    finally:
        try:
            await exchange.close()
        except Exception:
            pass


if __name__ == "__main__":
    asyncio.run(main())
