import asyncio
import logging
import os
import sqlite3
import time
from typing import Dict, Optional

from dotenv import load_dotenv
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta

load_dotenv()

USD_PER_TRADE = 15.0
MAX_POSITIONS = 3
WHITELIST = ["BTC/USD", "ETH/USD", "SENT/USD", "XMN/USD", "ZEC/USD", "FUN/USD"]

TF_MACRO = "1d"
TF_SETUP = "4h"
TF_CONFIRM = "1h"

STRICT_TIME_STOP_MINUTES = 180
HEARTBEAT_INTERVAL_S = 60
SCAN_INTERVAL_S = 45

DB_FILE = "macro_hunter_v6_research.db"
SENTRY_DB = "v_sentry_intelligence.db"
TAPE_DB = "v6_tape_sniffer.db"

MIN_SCORE_TO_ENTER = 7.0
INITIAL_STOP_PCT = 0.028
TRAIL_STOP_PCT = 0.022
TAKE_PROFIT_PCT = 0.06


def setup_logger(mode_name: str) -> logging.Logger:
    log_file = f"research_{mode_name.lower()}.log"
    logger = logging.getLogger(f"MacroHunter_{mode_name}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS virtual_positions
        (
            symbol TEXT,
            mode TEXT,
            qty REAL,
            entry_price REAL,
            peak_price REAL,
            stop_loss REAL,
            entry_ts REAL,
            status TEXT,
            PRIMARY KEY (symbol, mode)
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS trade_history
        (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT,
            mode TEXT,
            entry_ts REAL,
            exit_ts REAL,
            entry_price REAL,
            exit_price REAL,
            pnl_pct REAL,
            exit_reason TEXT
        )
        """
    )
    conn.commit()
    return conn


class ResearchBot:
    def __init__(self, mode: str = "STRICT"):
        self.mode = mode.upper()
        self.log = setup_logger(self.mode)
        self.ex = ccxt.kraken({"enableRateLimit": True})
        self.db = init_db()
        self.positions: Dict[str, dict] = {}
        self._last_heartbeat = 0.0

    def load_db_positions(self) -> Dict[str, dict]:
        c = self.db.cursor()
        c.execute(
            """
            SELECT symbol, qty, entry_price, peak_price, stop_loss, entry_ts
            FROM virtual_positions
            WHERE status='OPEN' AND mode=?
            """,
            (self.mode,),
        )
        rows = c.fetchall()
        return {
            row[0]: {
                "qty": row[1],
                "entry_price": row[2],
                "peak_price": row[3],
                "stop_loss": row[4],
                "entry_ts": row[5],
            }
            for row in rows
        }

    def upsert_position(self, symbol: str, data: dict) -> None:
        c = self.db.cursor()
        c.execute(
            """
            INSERT INTO virtual_positions (symbol, mode, qty, entry_price, peak_price, stop_loss, entry_ts, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN')
            ON CONFLICT(symbol, mode) DO UPDATE SET
                qty=excluded.qty,
                entry_price=excluded.entry_price,
                peak_price=excluded.peak_price,
                stop_loss=excluded.stop_loss,
                entry_ts=excluded.entry_ts,
                status='OPEN'
            """,
            (
                symbol,
                self.mode,
                data["qty"],
                data["entry_price"],
                data["peak_price"],
                data["stop_loss"],
                data["entry_ts"],
            ),
        )
        self.db.commit()

    def close_position_record(self, symbol: str, exit_price: float, exit_reason: str) -> None:
        position = self.positions[symbol]
        pnl_pct = ((exit_price - position["entry_price"]) / position["entry_price"]) * 100.0
        now_ts = time.time()

        c = self.db.cursor()
        c.execute(
            "INSERT INTO trade_history (symbol, mode, entry_ts, exit_ts, entry_price, exit_price, pnl_pct, exit_reason) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                symbol,
                self.mode,
                position["entry_ts"],
                now_ts,
                position["entry_price"],
                exit_price,
                pnl_pct,
                exit_reason,
            ),
        )
        c.execute(
            "UPDATE virtual_positions SET status='CLOSED' WHERE symbol=? AND mode=?",
            (symbol, self.mode),
        )
        self.db.commit()

    async def fetch_ohlcv_df(self, symbol: str, timeframe: str, limit: int) -> pd.DataFrame:
        raw = await self.ex.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=["ts", "open", "high", "low", "close", "volume"])
        if df.empty:
            return df
        df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        return df

    def latest_sentry(self, symbol: str) -> Optional[dict]:
        if not os.path.exists(SENTRY_DB):
            return None
        conn = sqlite3.connect(SENTRY_DB)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT timestamp, spread_pct, buy_pressure_ratio, bband_width, rsi_1h
            FROM market_intelligence
            WHERE symbol=?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return {
            "timestamp": row[0],
            "spread_pct": row[1],
            "buy_pressure_ratio": row[2],
            "bband_width": row[3],
            "rsi_1h": row[4],
        }

    def latest_tape(self, symbol: str) -> Optional[dict]:
        if not os.path.exists(TAPE_DB):
            return None
        conn = sqlite3.connect(TAPE_DB)
        cur = conn.cursor()
        cur.execute(
            """
            SELECT timestamp, total_trades, buy_volume, sell_volume, volume_delta
            FROM trade_flow
            WHERE symbol=?
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            (symbol,),
        )
        row = cur.fetchone()
        conn.close()
        if not row:
            return None
        return {
            "timestamp": row[0],
            "total_trades": row[1],
            "buy_volume": row[2],
            "sell_volume": row[3],
            "volume_delta": row[4],
        }

    async def analyze_symbol(self, symbol: str) -> Optional[dict]:
        daily = await self.fetch_ohlcv_df(symbol, TF_MACRO, 120)
        setup = await self.fetch_ohlcv_df(symbol, TF_SETUP, 120)
        confirm = await self.fetch_ohlcv_df(symbol, TF_CONFIRM, 120)

        if len(daily) < 60 or len(setup) < 60 or len(confirm) < 60:
            return None

        daily["sma20"] = ta.sma(daily["close"], length=20)
        daily["sma50"] = ta.sma(daily["close"], length=50)
        daily["rsi14"] = ta.rsi(daily["close"], length=14)

        setup["ema20"] = ta.ema(setup["close"], length=20)
        setup["rsi14"] = ta.rsi(setup["close"], length=14)
        setup_macd = ta.macd(setup["close"], fast=12, slow=26, signal=9)
        if setup_macd is None or setup_macd.empty:
            return None
        setup = pd.concat([setup, setup_macd], axis=1)

        confirm["ema20"] = ta.ema(confirm["close"], length=20)
        confirm["rsi14"] = ta.rsi(confirm["close"], length=14)
        confirm["vol_ma20"] = confirm["volume"].rolling(20).mean()

        d = daily.iloc[-1]
        s = setup.iloc[-1]
        s_prev = setup.iloc[-2]
        c = confirm.iloc[-1]
        c_prev = confirm.iloc[-2]

        if pd.isna(d["sma20"]) or pd.isna(d["sma50"]) or pd.isna(s["ema20"]) or pd.isna(c["ema20"]):
            return None

        macd_hist_col = next((col for col in setup.columns if col.startswith("MACDh_")), None)
        macd_col = next((col for col in setup.columns if col.startswith("MACD_") and not col.startswith("MACDh_") and not col.startswith("MACDs_")), None)
        macd_signal_col = next((col for col in setup.columns if col.startswith("MACDs_")), None)
        if not macd_hist_col or not macd_col or not macd_signal_col:
            return None

        score = 0.0
        notes = []

        if d["close"] > d["sma50"]:
            score += 2.0
            notes.append("daily_above_sma50")
        if d["sma20"] > d["sma50"]:
            score += 1.0
            notes.append("daily_sma20_over_sma50")
        if d["rsi14"] >= 50:
            score += 1.0
            notes.append("daily_rsi_supportive")

        if s["close"] > s["ema20"]:
            score += 1.0
            notes.append("setup_above_ema20")
        if s[macd_col] > s[macd_signal_col]:
            score += 1.0
            notes.append("setup_macd_bullish")
        if s[macd_hist_col] > s_prev[macd_hist_col]:
            score += 1.0
            notes.append("setup_hist_rising")
        if 45 <= s["rsi14"] <= 72:
            score += 0.5
            notes.append("setup_rsi_ok")

        if c["close"] > c["ema20"]:
            score += 1.0
            notes.append("confirm_above_ema20")
        if c["close"] > c_prev["high"]:
            score += 0.75
            notes.append("confirm_break_prev_high")
        if c["volume"] > c["vol_ma20"]:
            score += 0.75
            notes.append("confirm_volume_expansion")
        if c["rsi14"] >= 52:
            score += 0.5
            notes.append("confirm_rsi_ok")

        sentry = self.latest_sentry(symbol)
        if sentry:
            if sentry["spread_pct"] <= 0.35:
                score += 1.0
                notes.append("tight_spread")
            elif sentry["spread_pct"] > 1.20:
                score -= 1.5
                notes.append("spread_too_wide")

            if sentry["buy_pressure_ratio"] >= 1.10:
                score += 1.0
                notes.append("order_book_bid_support")
            elif sentry["buy_pressure_ratio"] < 0.90:
                score -= 1.0
                notes.append("order_book_ask_heavy")

        tape = self.latest_tape(symbol)
        if tape:
            if tape["volume_delta"] > 0:
                score += 1.0
                notes.append("positive_tape_delta")
            else:
                score -= 0.5
                notes.append("negative_tape_delta")

        last_price = float(c["close"])
        return {
            "symbol": symbol,
            "score": round(score, 2),
            "price": last_price,
            "notes": ",".join(notes),
            "confirm_low": float(c["low"]),
        }

    async def enter_virtual_position(self, symbol: str, price: float) -> None:
        if symbol in self.positions:
            return
        qty = USD_PER_TRADE / price if price > 0 else 0.0
        stop_loss = price * (1.0 - INITIAL_STOP_PCT)
        data = {
            "qty": qty,
            "entry_price": price,
            "peak_price": price,
            "stop_loss": stop_loss,
            "entry_ts": time.time(),
        }
        self.positions[symbol] = data
        self.upsert_position(symbol, data)
        self.log.info(f"BUY  {symbol:<10} | Entry: ${price:,.4f} | Qty: {qty:.8f} | Stop: ${stop_loss:,.4f}")

    async def exit_virtual_position(self, symbol: str, price: float, reason: str) -> None:
        if symbol not in self.positions:
            return
        self.close_position_record(symbol, price, reason)
        entry = self.positions[symbol]["entry_price"]
        pnl_pct = ((price - entry) / entry) * 100.0
        self.log.info(f"SELL {symbol:<10} | Exit: ${price:,.4f} | PnL: {pnl_pct:+.2f}% | Reason: {reason}")
        del self.positions[symbol]

    async def manage_positions(self) -> None:
        if not self.positions:
            return
        tickers = await self.ex.fetch_tickers(list(self.positions.keys()))
        now_ts = time.time()
        exits = []

        for symbol, pos in self.positions.items():
            last_price = tickers.get(symbol, {}).get("last")
            if not last_price:
                continue
            last_price = float(last_price)

            if last_price > pos["peak_price"]:
                pos["peak_price"] = last_price
                trailing_stop = pos["peak_price"] * (1.0 - TRAIL_STOP_PCT)
                pos["stop_loss"] = max(pos["stop_loss"], trailing_stop)
                self.upsert_position(symbol, pos)

            held_minutes = (now_ts - pos["entry_ts"]) / 60.0
            pnl_pct = ((last_price - pos["entry_price"]) / pos["entry_price"]) * 100.0

            if last_price <= pos["stop_loss"]:
                exits.append((symbol, last_price, "stop_loss"))
                continue
            if pnl_pct >= TAKE_PROFIT_PCT * 100.0:
                exits.append((symbol, last_price, "take_profit"))
                continue
            if held_minutes >= STRICT_TIME_STOP_MINUTES and pnl_pct <= 0.5:
                exits.append((symbol, last_price, "time_stop"))
                continue

        for symbol, price, reason in exits:
            await self.exit_virtual_position(symbol, price, reason)

    async def scan_market(self) -> None:
        if len(self.positions) >= MAX_POSITIONS:
            return

        analyses = []
        for symbol in WHITELIST:
            if symbol in self.positions:
                continue
            try:
                result = await self.analyze_symbol(symbol)
                if result:
                    analyses.append(result)
                    self.log.info(f"SCAN {symbol:<10} | Score: {result['score']:.2f} | Price: ${result['price']:,.4f} | {result['notes']}")
            except Exception as e:
                self.log.error(f"Analyze error on {symbol}: {e}")
            await asyncio.sleep(1)

        analyses.sort(key=lambda x: x["score"], reverse=True)
        open_slots = MAX_POSITIONS - len(self.positions)
        candidates = [x for x in analyses if x["score"] >= MIN_SCORE_TO_ENTER][:open_slots]

        for candidate in candidates:
            await self.enter_virtual_position(candidate["symbol"], candidate["price"])

    async def print_heartbeat(self) -> None:
        now = time.time()
        if now - self._last_heartbeat < HEARTBEAT_INTERVAL_S:
            return
        self._last_heartbeat = now

        if not self.positions:
            self.log.info(f"HEARTBEAT {self.mode} | No active virtual positions | Watching {len(WHITELIST)} symbols")
            return

        tickers = await self.ex.fetch_tickers(list(self.positions.keys()))
        self.log.info(f"HEARTBEAT {self.mode} | Active positions: {len(self.positions)}")
        for symbol, pos in self.positions.items():
            current = tickers.get(symbol, {}).get("last")
            if not current:
                continue
            current = float(current)
            pnl_pct = ((current - pos["entry_price"]) / pos["entry_price"]) * 100.0
            self.log.info(
                f"  {symbol:<10} | PnL: {pnl_pct:+6.2f}% | Entry: ${pos['entry_price']:,.4f} | Stop: ${pos['stop_loss']:,.4f} | Peak: ${pos['peak_price']:,.4f}"
            )

    async def run(self) -> None:
        self.log.info(f"MacroHunter V6 Research [{self.mode}] starting")
        try:
            await self.ex.load_markets()
            self.positions = self.load_db_positions()
            self.log.info(f"Recovered {len(self.positions)} open virtual positions from DB")

            while True:
                await self.print_heartbeat()
                await self.manage_positions()
                await self.scan_market()
                await asyncio.sleep(SCAN_INTERVAL_S)
        except KeyboardInterrupt:
            self.log.info("Shutdown requested by user")
        except Exception as e:
            self.log.exception(f"Fatal error in run loop: {e}")
            raise
        finally:
            try:
                await self.ex.close()
            except Exception:
                pass
            try:
                self.db.close()
            except Exception:
                pass
            self.log.info("MacroHunter shutdown complete")


if __name__ == "__main__":
    bot = ResearchBot(mode="STRICT")
    asyncio.run(bot.run())
