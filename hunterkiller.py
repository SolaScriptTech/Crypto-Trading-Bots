import textwrap, os, pathlib, re, json, math, datetime
code = r
import asyncio
import sqlite3
import os
import sys
import time
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Dict, Any, List, Optional, Tuple

from dotenv import load_dotenv
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta

load_dotenv()

# =========================
# SETTINGS
# =========================
TZ_PST = ZoneInfo("America/Los_Angeles")

RUN_START_HOUR_PST = 3
RUN_END_HOUR_PST = 12

DB_FILE = "whale_hunter.db"
LOG_FILE = "whale_hunter.log"

CONCURRENCY = 8
LOOP_SLEEP_S = 1.0
UNIVERSE_REFRESH_S = 300
TICKERS_REFRESH_S = 8

QUOTE = "USD"

MAX_POSITIONS = 5
MAX_USD_PER_TRADE = 10.0

# Universe filters
MIN_QUOTE_VOL_24H = 250_000.0
MAX_SPREAD_PCT = 0.006  # 0.6%

# Entry filters
CANDLE_TF = "1m"
OHLCV_LIMIT = 80
RVOL_MULT = 2.0
CHASE_CAP_5M_PCT = 0.08  # do not chase > 8% in last 5m

# Audition rules (dump losers fast)
AUDIT_60S_MIN_GAIN = 0.006   # +0.6% within 60s
AUDIT_120S_MIN_GAIN = 0.012  # +1.2% within 120s
HARD_STOP_LOSS = 0.010       # -1.0% any time

# Whale management
BREAKEVEN_AT = 0.012
TRAIL_START = 0.025
TRAIL_PCT = 0.012            # 1.2% trail once running
STALL_S = 120                # no new high for 120s => dump if not really running
STALL_MIN_PROFIT = 0.02      # if stall and profit < 2% => dump

# Candidate pool sizing
CANDIDATE_POOL = 80  # number of pairs to fetch candles for each scan cycle


# =========================
# LOGGING
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("WhaleHunter")


# =========================
# DB
# =========================
class Database:
    def __init__(self, path: str):
        self.path = path
        self._init_db()

    def _conn(self):
        return sqlite3.connect(self.path, timeout=30)

    def _init_db(self):
        with self._conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS positions (
                    symbol TEXT PRIMARY KEY,
                    entry_price REAL,
                    size REAL,
                    highest_price REAL,
                    stop_price REAL,
                    entry_time INTEGER,
                    last_high_time INTEGER,
                    whale INTEGER
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS trades (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT,
                    side TEXT,
                    price REAL,
                    size REAL,
                    pnl_usd REAL,
                    reason TEXT,
                    timestamp INTEGER
                )
                """
            )
            conn.commit()

    def get_positions(self) -> Dict[str, Dict[str, Any]]:
        with self._conn() as conn:
            cur = conn.execute("SELECT * FROM positions")
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return {row[0]: dict(zip(cols, row)) for row in rows}

    def upsert_position(
        self,
        symbol: str,
        entry_price: float,
        size: float,
        highest_price: float,
        stop_price: float,
        entry_time: int,
        last_high_time: int,
        whale: int,
    ):
        with self._conn() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO positions VALUES (?,?,?,?,?,?,?,?)",
                (symbol, entry_price, size, highest_price, stop_price, entry_time, last_high_time, whale),
            )
            conn.commit()

    def update_position(self, symbol: str, highest_price: float, stop_price: float, last_high_time: int, whale: int):
        with self._conn() as conn:
            conn.execute(
                "UPDATE positions SET highest_price=?, stop_price=?, last_high_time=?, whale=? WHERE symbol=?",
                (highest_price, stop_price, last_high_time, whale, symbol),
            )
            conn.commit()

    def delete_position(self, symbol: str):
        with self._conn() as conn:
            conn.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
            conn.commit()

    def log_trade(self, symbol: str, side: str, price: float, size: float, pnl_usd: float, reason: str):
        with self._conn() as conn:
            conn.execute(
                "INSERT INTO trades (symbol, side, price, size, pnl_usd, reason, timestamp) VALUES (?,?,?,?,?,?,?)",
                (symbol, side, price, size, pnl_usd, reason, int(time.time())),
            )
            conn.commit()


# =========================
# SIGNALS
# =========================
def compute_macd_hist(df: pd.DataFrame) -> pd.Series:
    macd = df.ta.macd(fast=12, slow=26, signal=9)
    if macd is None or "MACDh_12_26_9" not in macd:
        return pd.Series([float("nan")] * len(df))
    return macd["MACDh_12_26_9"]


def safe_pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a - b) / b


def now_pst() -> datetime:
    return datetime.now(timezone.utc).astimezone(TZ_PST)


def in_run_window() -> bool:
    h = now_pst().hour
    return RUN_START_HOUR_PST <= h < RUN_END_HOUR_PST


# =========================
# BOT
# =========================
class WhaleHunterBot:
    def __init__(self):
        self.db = Database(DB_FILE)
        self.db_lock = asyncio.Lock()
        self.buy_lock = asyncio.Lock()

        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = os.getenv("KRAKEN_API_SECRET")
        if not api_key or not api_secret:
            raise ValueError("Missing KRAKEN_API_KEY or KRAKEN_API_SECRET in .env")

        self.exchange = ccxt.kraken(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "options": {"defaultType": "spot"},
            }
        )

        self.sem = asyncio.Semaphore(CONCURRENCY)
        self.last_universe_refresh = 0.0
        self.last_tickers_refresh = 0.0
        self.universe: List[str] = []
        self.cached_tickers: Dict[str, Any] = {}

    # ---- DB wrappers
    async def _get_positions(self) -> Dict[str, Dict[str, Any]]:
        async with self.db_lock:
            return self.db.get_positions()

    async def _upsert_position(self, pos: Dict[str, Any]):
        async with self.db_lock:
            self.db.upsert_position(
                pos["symbol"],
                pos["entry_price"],
                pos["size"],
                pos["highest_price"],
                pos["stop_price"],
                pos["entry_time"],
                pos["last_high_time"],
                pos["whale"],
            )

    async def _update_position(self, symbol: str, highest: float, stop: float, last_high_time: int, whale: int):
        async with self.db_lock:
            self.db.update_position(symbol, highest, stop, last_high_time, whale)

    async def _close_position(self, symbol: str, sell_price: float, size: float, pnl_usd: float, reason: str):
        async with self.db_lock:
            self.db.delete_position(symbol)
            self.db.log_trade(symbol, "SELL", sell_price, size, pnl_usd, reason)

    # ---- precision
    def _clean_precision(self, symbol: str, amount: float, price: Optional[float] = None) -> Tuple[float, Optional[float]]:
        a = amount
        p = price
        try:
            a = float(self.exchange.amount_to_precision(symbol, amount))
        except Exception:
            pass
        if price is not None:
            try:
                p = float(self.exchange.price_to_precision(symbol, price))
            except Exception:
                p = price
        return a, p

    # ---- universe
    async def refresh_universe(self):
        if not self.exchange.markets:
            await self.exchange.load_markets()

        tickers = await self.exchange.fetch_tickers()
        self.cached_tickers = tickers
        self.last_tickers_refresh = time.time()

        valid: List[str] = []
        for symbol, t in tickers.items():
            if f"/{QUOTE}" not in symbol:
                continue
            m = self.exchange.markets.get(symbol)
            if not m:
                continue
            if not m.get("active", True):
                continue
            if m.get("spot") is False:
                continue

            bid = float(t.get("bid") or 0.0)
            ask = float(t.get("ask") or 0.0)
            if bid <= 0 or ask <= 0:
                continue

            spread = (ask - bid) / ask
            if spread > MAX_SPREAD_PCT:
                continue

            qv = t.get("quoteVolume")
            if qv is None:
                continue
            try:
                if float(qv) < MIN_QUOTE_VOL_24H:
                    continue
            except Exception:
                continue

            valid.append(symbol)

        self.universe = valid
        self.last_universe_refresh = time.time()
        log.info(f"🌌 Universe: {len(self.universe)} {QUOTE} spot pairs")

    async def refresh_tickers(self):
        tickers = await self.exchange.fetch_tickers()
        self.cached_tickers = tickers
        self.last_tickers_refresh = time.time()

    def pick_candidates(self, positions: Dict[str, Dict[str, Any]]) -> List[str]:
        # Prefer liquid + moving pairs; ticker "percentage" is 24h, but it helps avoid dead coins.
        rows = []
        for s in self.universe:
            if s in positions:
                continue
            t = self.cached_tickers.get(s) or {}
            qv = float(t.get("quoteVolume") or 0.0)
            pct = float(t.get("percentage") or 0.0)  # 24h
            last = float(t.get("last") or 0.0)
            if last <= 0:
                continue
            rows.append((s, qv, pct))

        rows.sort(key=lambda x: (x[2], x[1]), reverse=True)
        return [r[0] for r in rows[:CANDIDATE_POOL]]

    # ---- trading
    async def fetch_balance_usd(self) -> float:
        bal = await self.exchange.fetch_balance()
        return float((bal.get(QUOTE) or {}).get("free") or 0.0)

    async def market_buy_usd(self, symbol: str, usd_amount: float) -> Optional[Tuple[float, float]]:
        # returns (filled_qty, avg_price)
        ticker = await self.exchange.fetch_ticker(symbol)
        ask = float(ticker.get("ask") or 0.0)
        if ask <= 0:
            return None
        qty = usd_amount / ask

        # minimums and precision
        qty, _ = self._clean_precision(symbol, qty, None)
        if qty <= 0:
            return None

        order = await self.exchange.create_order(symbol, "market", "buy", qty)
        filled = float(order.get("filled") or 0.0)
        if filled <= 0:
            return None
        avg = float(order.get("average") or ask)
        return filled, avg

    async def market_sell(self, symbol: str, qty: float) -> bool:
        qty, _ = self._clean_precision(symbol, qty, None)
        if qty <= 0:
            return False
        await self.exchange.create_order(symbol, "market", "sell", qty)
        return True

    # ---- signals
    def entry_signal(self, df: pd.DataFrame) -> Tuple[bool, float, str]:
        if len(df) < 40:
            return False, 0.0, ""

        hist = compute_macd_hist(df)
        if hist.isna().all():
            return False, 0.0, ""

        h0 = float(hist.iloc[-1])
        h1 = float(hist.iloc[-2])
        h2 = float(hist.iloc[-3])
        h3 = float(hist.iloc[-4])

        # "red -> pink -> white": negative but rising toward zero
        neg_rising = (h0 < 0) and (h0 > h1 > h2) and (h2 > h3)
        crossed = (h0 > 0) and (h1 <= 0)

        # volume spike
        vol_ma = float(df["volume"].rolling(20).mean().iloc[-1] or 0.0)
        vol_now = float(df["volume"].iloc[-1] or 0.0)
        rvol = vol_now / (vol_ma + 1e-9)

        # avoid chasing already vertical
        close = float(df["close"].iloc[-1])
        close_5 = float(df["close"].iloc[-6]) if len(df) >= 6 else float(df["close"].iloc[0])
        ret_5m = safe_pct(close, close_5)

        if ret_5m > CHASE_CAP_5M_PCT:
            return False, 0.0, ""

        if rvol < RVOL_MULT:
            return False, 0.0, ""

        # basic momentum confirm
        if close <= float(df["close"].iloc[-2]):
            return False, 0.0, ""

        if not (neg_rising or crossed):
            return False, 0.0, ""

        # score: favor rvol + rising histogram + recent movement
        hist_slope = (h0 - h3)
        score = (rvol * 2.0) + (max(0.0, hist_slope) * 50.0) + (max(0.0, ret_5m) * 10.0)
        reason = "MACD_RISE" if neg_rising else "MACD_CROSS"
        return True, float(score), reason

    async def evaluate_symbol(self, symbol: str) -> Optional[Tuple[str, float, str]]:
        async with self.sem:
            try:
                ohlcv = await self.exchange.fetch_ohlcv(symbol, CANDLE_TF, limit=OHLCV_LIMIT)
                if not ohlcv:
                    return None
                df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
                ok, score, reason = self.entry_signal(df)
                if not ok:
                    return None
                return symbol, score, reason
            except Exception:
                return None

    async def try_open_position(self, symbol: str, reason: str):
        async with self.buy_lock:
            if not in_run_window():
                return

            positions = await self._get_positions()
            if symbol in positions:
                return
            if len(positions) >= MAX_POSITIONS:
                return

            usd_free = await self.fetch_balance_usd()
            if usd_free < MAX_USD_PER_TRADE:
                return

            try:
                filled = await self.market_buy_usd(symbol, MAX_USD_PER_TRADE)
                if not filled:
                    return
                qty, entry = filled

                pos = {
                    "symbol": symbol,
                    "entry_price": float(entry),
                    "size": float(qty),
                    "highest_price": float(entry),
                    "stop_price": float(entry) * (1 - HARD_STOP_LOSS),
                    "entry_time": int(time.time()),
                    "last_high_time": int(time.time()),
                    "whale": 0,
                }
                await self._upsert_position(pos)
                async with self.db_lock:
                    self.db.log_trade(symbol, "BUY", float(entry), float(qty), 0.0, reason)

                log.info(f"🟢 BUY {symbol} | {reason} | qty {qty} @ {entry}")

            except Exception as e:
                log.error(f"Buy error {symbol}: {e}")

    async def manage_positions(self):
        positions = await self._get_positions()
        if not positions:
            return

        for sym, pos in positions.items():
            try:
                ticker = await self.exchange.fetch_ticker(sym)
                bid = float(ticker.get("bid") or 0.0)
                if bid <= 0:
                    continue

                entry = float(pos.get("entry_price") or bid)
                qty = float(pos.get("size") or 0.0)
                if qty <= 0:
                    await self._close_position(sym, bid, 0.0, 0.0, "BAD_SIZE_CLEANUP")
                    continue

                highest = float(pos.get("highest_price") or entry)
                stop = float(pos.get("stop_price") or (entry * (1 - HARD_STOP_LOSS)))
                entry_t = int(pos.get("entry_time") or int(time.time()))
                last_high_t = int(pos.get("last_high_time") or entry_t)
                whale = int(pos.get("whale") or 0)

                held = int(time.time()) - entry_t
                pnl_pct = safe_pct(bid, entry)
                drawdown_pct = safe_pct(bid, highest) if highest > 0 else 0.0

                # Update high water mark
                if bid > highest:
                    highest = bid
                    last_high_t = int(time.time())

                # Hard stop loss (dump instantly)
                if pnl_pct <= -HARD_STOP_LOSS:
                    await self.market_sell(sym, qty)
                    pnl_usd = (bid - entry) * qty
                    await self._close_position(sym, bid, qty, pnl_usd, "HARD_STOP")
                    log.info(f"🔴 SOLD {sym} | HARD_STOP | pnl ${pnl_usd:.2f}")
                    continue

                # Audition rules: dump losers fast
                if whale == 0:
                    if held >= 60 and pnl_pct < AUDIT_60S_MIN_GAIN:
                        await self.market_sell(sym, qty)
                        pnl_usd = (bid - entry) * qty
                        await self._close_position(sym, bid, qty, pnl_usd, "AUDIT_FAIL_60S")
                        log.info(f"🟠 SOLD {sym} | AUDIT_FAIL_60S | pnl ${pnl_usd:.2f}")
                        continue

                    if held >= 120 and pnl_pct < AUDIT_120S_MIN_GAIN:
                        await self.market_sell(sym, qty)
                        pnl_usd = (bid - entry) * qty
                        await self._close_position(sym, bid, qty, pnl_usd, "AUDIT_FAIL_120S")
                        log.info(f"🟠 SOLD {sym} | AUDIT_FAIL_120S | pnl ${pnl_usd:.2f}")
                        continue

                    # Promote to whale if it proves itself early
                    if held <= 120 and pnl_pct >= AUDIT_120S_MIN_GAIN:
                        whale = 1
                        log.info(f"🐋 PROMOTED {sym} | pnl {pnl_pct*100:.2f}%")

                # Whale management
                if whale == 1:
                    # Breakeven protection
                    if pnl_pct >= BREAKEVEN_AT:
                        stop = max(stop, entry)

                    # Trailing stop once it is running
                    if pnl_pct >= TRAIL_START:
                        stop = max(stop, highest * (1 - TRAIL_PCT))

                    # Stall exit
                    if int(time.time()) - last_high_t >= STALL_S and pnl_pct < STALL_MIN_PROFIT:
                        await self.market_sell(sym, qty)
                        pnl_usd = (bid - entry) * qty
                        await self._close_position(sym, bid, qty, pnl_usd, "STALL_EXIT")
                        log.info(f"🟣 SOLD {sym} | STALL_EXIT | pnl ${pnl_usd:.2f}")
                        continue

                # Stop hit
                if bid <= stop:
                    await self.market_sell(sym, qty)
                    pnl_usd = (bid - entry) * qty
                    await self._close_position(sym, bid, qty, pnl_usd, "STOP_HIT")
                    log.info(f"🔻 SOLD {sym} | STOP_HIT | pnl ${pnl_usd:.2f}")
                    continue

                await self._update_position(sym, highest, stop, last_high_t, whale)

            except Exception as e:
                log.error(f"Manage error {sym}: {e}")

    async def scan(self):
        if not in_run_window():
            return

        # refresh universe and tickers on cadence
        now = time.time()
        if (now - self.last_universe_refresh) > UNIVERSE_REFRESH_S or not self.universe:
            await self.refresh_universe()

        if (now - self.last_tickers_refresh) > TICKERS_REFRESH_S or not self.cached_tickers:
            await self.refresh_tickers()

        positions = await self._get_positions()
        if len(positions) >= MAX_POSITIONS:
            return

        candidates = self.pick_candidates(positions)
        if not candidates:
            return

        # Evaluate in parallel, pick best score
        results = await asyncio.gather(*(self.evaluate_symbol(s) for s in candidates), return_exceptions=True)

        best_sym = None
        best_score = -1.0
        best_reason = ""

        for r in results:
            if not r or isinstance(r, Exception):
                continue
            sym, score, reason = r
            if score > best_score:
                best_sym = sym
                best_score = score
                best_reason = reason

        if best_sym:
            await self.try_open_position(best_sym, f"{best_reason}_SCORE_{best_score:.1f}")

    async def run(self):
        log.info("🐋 WhaleHunter STARTED (Kraken spot, USD)")
        await self.exchange.load_markets()

        while True:
            try:
                if in_run_window():
                    await self.scan()
                    await self.manage_positions()
                else:
                    # outside window: manage existing positions only, then sleep longer
                    await self.manage_positions()
                    await asyncio.sleep(5.0)
                    continue

                await asyncio.sleep(LOOP_SLEEP_S)

            except KeyboardInterrupt:
                break
            except Exception as e:
                log.error(f"Loop error: {e}")
                await asyncio.sleep(3.0)

        await self.exchange.close()


if __name__ == "__main__":
    try:
        asyncio.run(WhaleHunterBot().run())
    except KeyboardInterrupt:
        pass

path = "/mnt/data/WhaleHunter_Kraken_USD.py"
with open(path, "w", encoding="utf-8") as f:
    f.write(code.strip() + "\n")
path

