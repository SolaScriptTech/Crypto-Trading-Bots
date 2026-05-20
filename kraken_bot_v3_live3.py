#!/usr/bin/env python3
import os
import json
import time
import asyncio
import logging
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import ccxt.async_support as ccxt

load_dotenv()

# =========================================================
# CONFIG
# =========================================================
VERSION = "green_buyer_v1"

LOG_FILE = "kraken_bot_green_buyer.log"
EVENT_LOG_JSONL = "kraken_bot_green_buyer_events.jsonl"
STATE_FILE = "kraken_bot_green_buyer_state.json"

QUOTE = "USD"
MAX_POSITIONS = 3
USD_PER_TRADE = 25.0
MIN_ORDER_USD_BUFFER = 0.25

SCAN_INTERVAL_SECONDS = 20
POSITION_MONITOR_INTERVAL_SECONDS = 3
HEARTBEAT_EVERY_SECONDS = 20

# Kraken safe pacing
REQUEST_SLEEP_SECONDS = 0.20
FETCH_TICKERS_CHUNK_SIZE = 20
TICKER_CACHE_TTL_SECONDS = 6
UNIVERSE_REFRESH_SECONDS = 1800

# "If it is green buy it"
# Green definition uses Kraken ticker percentage > 0
MIN_GREEN_PCT = 0.01

# Basic filters so it does not buy garbage with insane spread
MIN_QUOTE_VOLUME_24H = 300_000.0
MAX_SPREAD_PCT = 0.75
MAX_NEW_ENTRIES_PER_LOOP = 2
SYMBOL_COOLDOWN_SECONDS = 120

# Exits requested
STOP_LOSS_FROM_ENTRY_PCT = 1.0           # sell if down 1 percent from entry
PROFIT_ARM_PCT = 1.0                     # once up 1 percent
TRAIL_STOP_FROM_PEAK_PCT = 0.5           # then trail 0.5 percent from peak

EXCLUDED_BASES = {
    "USD", "USDT", "USDC", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY"
}

# =========================================================
# LOGGING
# =========================================================
logger = logging.getLogger("kraken_green_buyer")
logger.setLevel(logging.INFO)
logger.handlers.clear()

fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

sh = logging.StreamHandler()
sh.setFormatter(fmt)
logger.addHandler(sh)

fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)
logger.addHandler(fh)

# =========================================================
# HELPERS
# =========================================================
def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default

def pct_change(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return ((b - a) / a) * 100.0

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

async def pace_sleep() -> None:
    await asyncio.sleep(REQUEST_SLEEP_SECONDS)

def jsonl_append(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, separators=(",", ":"), ensure_ascii=False) + "\n")

def read_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def write_json(path: str, obj: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2)
    os.replace(tmp, path)

# =========================================================
# DATA MODELS
# =========================================================
@dataclass
class Position:
    symbol: str
    amount: float
    entry_price: float
    entry_cost_usd: float
    entry_time_ts: float
    peak_price: float
    trailing_armed: bool = False
    trailing_stop_price: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Position":
        return Position(**d)

class BotState:
    def __init__(self) -> None:
        self.positions: Dict[str, Position] = {}
        self.last_trade_ts_by_symbol: Dict[str, float] = {}
        self.realized_pnl_usd: float = 0.0
        self.total_buys: int = 0
        self.total_sells: int = 0
        self.total_wins: int = 0
        self.total_losses: int = 0
        self.started_ts: float = time.time()

    def load(self) -> None:
        raw = read_json(STATE_FILE, {})
        self.positions = {k: Position.from_dict(v) for k, v in raw.get("positions", {}).items()}
        self.last_trade_ts_by_symbol = {k: safe_float(v) for k, v in raw.get("last_trade_ts_by_symbol", {}).items()}
        self.realized_pnl_usd = safe_float(raw.get("realized_pnl_usd"))
        self.total_buys = int(raw.get("total_buys", 0))
        self.total_sells = int(raw.get("total_sells", 0))
        self.total_wins = int(raw.get("total_wins", 0))
        self.total_losses = int(raw.get("total_losses", 0))
        self.started_ts = safe_float(raw.get("started_ts"), time.time())

    def save(self) -> None:
        write_json(STATE_FILE, {
            "positions": {k: v.to_dict() for k, v in self.positions.items()},
            "last_trade_ts_by_symbol": self.last_trade_ts_by_symbol,
            "realized_pnl_usd": self.realized_pnl_usd,
            "total_buys": self.total_buys,
            "total_sells": self.total_sells,
            "total_wins": self.total_wins,
            "total_losses": self.total_losses,
            "started_ts": self.started_ts,
        })

class TickerCache:
    def __init__(self) -> None:
        self.ts: float = 0.0
        self.data: Dict[str, Dict[str, Any]] = {}

    def is_fresh(self) -> bool:
        return bool(self.data) and (time.time() - self.ts) <= TICKER_CACHE_TTL_SECONDS

    def set(self, data: Dict[str, Dict[str, Any]]) -> None:
        self.data = data
        self.ts = time.time()

# =========================================================
# EXCHANGE
# =========================================================
async def make_exchange() -> ccxt.kraken:
    key = os.getenv("KRAKEN_API_KEY")
    secret = os.getenv("KRAKEN_API_SECRET")
    if not key or not secret:
        raise RuntimeError("Missing KRAKEN_API_KEY or KRAKEN_API_SECRET")

    ex = ccxt.kraken({
        "apiKey": key,
        "secret": secret,
        "enableRateLimit": True,
        "timeout": 30000,
        "options": {
            "createMarketBuyOrderRequiresPrice": False,
        },
    })
    return ex

def is_spot_market(m: Dict[str, Any]) -> bool:
    if not isinstance(m, dict):
        return False
    if m.get("spot") is False:
        return False
    if m.get("swap") is True or m.get("future") is True or m.get("option") is True:
        return False
    return True

def market_symbol_ok(symbol: str) -> bool:
    if not isinstance(symbol, str) or "/" not in symbol:
        return False
    base, quote = symbol.split("/", 1)
    if quote != QUOTE:
        return False
    if base in EXCLUDED_BASES:
        return False
    return True

async def get_candidate_symbols(exchange: ccxt.kraken) -> List[str]:
    markets = await exchange.load_markets()
    await pace_sleep()

    out: List[str] = []
    for symbol, m in markets.items():
        if not is_spot_market(m):
            continue
        if not m.get("active", True):
            continue
        if not market_symbol_ok(symbol):
            continue
        out.append(symbol)

    out.sort()
    return out

async def fetch_tickers_chunked(exchange: ccxt.kraken, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    for i in range(0, len(symbols), FETCH_TICKERS_CHUNK_SIZE):
        chunk = symbols[i:i + FETCH_TICKERS_CHUNK_SIZE]
        try:
            data = await exchange.fetch_tickers(chunk)
            await pace_sleep()
            if isinstance(data, dict):
                results.update(data)
        except Exception as e:
            logger.warning(f"fetch_tickers chunk failed {i}:{i+len(chunk)} | {e}")
            await asyncio.sleep(0.6)
    return results

async def get_or_refresh_tickers(
    exchange: ccxt.kraken,
    symbols: List[str],
    cache: TickerCache,
    force: bool = False
) -> Dict[str, Dict[str, Any]]:
    if not force and cache.is_fresh():
        return cache.data
    tickers = await fetch_tickers_chunked(exchange, symbols)
    cache.set(tickers)
    return tickers

def parse_ticker_metrics(symbol: str, t: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(t, dict):
        return None

    bid = safe_float(t.get("bid"))
    ask = safe_float(t.get("ask"))
    last = safe_float(t.get("last"))
    pct = safe_float(t.get("percentage"))
    quote_volume = safe_float(t.get("quoteVolume"))
    base_volume = safe_float(t.get("baseVolume"))

    if last <= 0:
        return None

    if quote_volume <= 0 and base_volume > 0:
        quote_volume = base_volume * last

    spread_pct = 999.0
    if bid > 0 and ask > 0 and ask >= bid:
        spread_pct = ((ask - bid) / max(bid, 1e-12)) * 100.0

    return {
        "symbol": symbol,
        "bid": bid,
        "ask": ask,
        "last": last,
        "change_pct": pct,
        "quote_vol": quote_volume,
        "spread_pct": spread_pct,
    }

def get_best_prices_from_ticker(t: Optional[Dict[str, Any]]) -> Tuple[float, float, float]:
    if not isinstance(t, dict):
        return 0.0, 0.0, 0.0
    bid = safe_float(t.get("bid"))
    ask = safe_float(t.get("ask"))
    last = safe_float(t.get("last"))
    px = ask if ask > 0 else (last if last > 0 else bid)
    return bid, ask, px

# =========================================================
# ORDER HELPERS
# =========================================================
async def ensure_market_metadata(exchange: ccxt.kraken, symbol: str) -> Dict[str, Any]:
    m = exchange.markets.get(symbol)
    if m is None:
        await exchange.load_markets()
        await pace_sleep()
        m = exchange.markets.get(symbol)
    if m is None:
        raise RuntimeError(f"Missing market metadata for {symbol}")
    return m

def amount_from_usd(price: float, usd: float) -> float:
    if price <= 0:
        return 0.0
    return usd / price

def round_amount_for_market(exchange: ccxt.kraken, symbol: str, amount: float) -> float:
    try:
        return float(exchange.amount_to_precision(symbol, amount))
    except Exception:
        return amount

def extract_order_fill(order: Dict[str, Any], fallback_price: float) -> Tuple[float, float]:
    filled = safe_float(order.get("filled"))
    cost = safe_float(order.get("cost"))
    avg = safe_float(order.get("average"))
    if avg <= 0 and filled > 0 and cost > 0:
        avg = cost / filled
    if avg <= 0:
        avg = fallback_price
    return filled, avg

async def create_market_buy(exchange: ccxt.kraken, symbol: str, usd_budget: float, ticker: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    await ensure_market_metadata(exchange, symbol)

    bid, ask, px = get_best_prices_from_ticker(ticker)
    if px <= 0:
        return None

    spend = max(0.0, usd_budget - MIN_ORDER_USD_BUFFER)
    raw_amount = amount_from_usd(px, spend)
    amount = round_amount_for_market(exchange, symbol, raw_amount)
    if amount <= 0:
        return None

    try:
        order = await exchange.create_order(symbol, "market", "buy", amount)
        await pace_sleep()
        return order
    except Exception as e:
        logger.warning(f"BUY failed {symbol} | {e}")
        return None

async def create_market_sell(exchange: ccxt.kraken, symbol: str, amount: float) -> Optional[Dict[str, Any]]:
    if amount <= 0:
        return None

    await ensure_market_metadata(exchange, symbol)
    amount = round_amount_for_market(exchange, symbol, amount)
    if amount <= 0:
        return None

    try:
        order = await exchange.create_order(symbol, "market", "sell", amount)
        await pace_sleep()
        return order
    except Exception as e:
        logger.warning(f"SELL failed {symbol} | {e}")
        return None

# =========================================================
# STRATEGY
# =========================================================
def green_buy_candidates(tickers: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for symbol, t in tickers.items():
        row = parse_ticker_metrics(symbol, t)
        if row is None:
            continue
        if row["quote_vol"] < MIN_QUOTE_VOLUME_24H:
            continue
        if row["spread_pct"] > MAX_SPREAD_PCT:
            continue
        if row["change_pct"] <= MIN_GREEN_PCT:
            continue

        # rank simple and aggressive for bullish tape
        score = (
            min(row["change_pct"], 15.0) * 1.5
            + min(row["quote_vol"] / 1_000_000.0, 10.0) * 0.8
            - row["spread_pct"] * 2.0
        )
        row["score"] = round(score, 4)
        rows.append(row)

    rows.sort(key=lambda x: x["score"], reverse=True)
    return rows

# =========================================================
# PNL + HEARTBEAT
# =========================================================
def calc_unrealized(pos: Position, mark_price: float) -> Tuple[float, float]:
    if pos.amount <= 0 or pos.entry_price <= 0 or mark_price <= 0:
        return 0.0, 0.0
    pnl_usd = (mark_price - pos.entry_price) * pos.amount
    pnl_pct = pct_change(pos.entry_price, mark_price)
    return pnl_usd, pnl_pct

# =========================================================
# MAIN LOOP
# =========================================================
async def bot_loop() -> None:
    exchange = await make_exchange()
    state = BotState()
    state.load()

    ticker_cache = TickerCache()
    universe_symbols: List[str] = []
    universe_last_refresh = 0.0
    last_monitor_ts = 0.0
    last_heartbeat_ts = 0.0

    logger.info(f"Starting {VERSION}")

    try:
        while True:
            loop_start = time.time()

            # Refresh universe
            if not universe_symbols or (time.time() - universe_last_refresh) > UNIVERSE_REFRESH_SECONDS:
                try:
                    universe_symbols = await get_candidate_symbols(exchange)
                    universe_last_refresh = time.time()
                    logger.info(f"Universe loaded | {len(universe_symbols)} symbols")
                except Exception as e:
                    logger.exception(f"Universe refresh failed | {e}")
                    await asyncio.sleep(5)
                    continue

            # Monitor positions often
            if state.positions and (time.time() - last_monitor_ts) >= POSITION_MONITOR_INTERVAL_SECONDS:
                held_symbols = list(state.positions.keys())
                try:
                    held_tickers = await get_or_refresh_tickers(exchange, held_symbols, ticker_cache, force=True)

                    for symbol in list(state.positions.keys()):
                        pos = state.positions.get(symbol)
                        if pos is None:
                            continue

                        t = held_tickers.get(symbol, {})
                        bid, ask, px = get_best_prices_from_ticker(t)
                        mark = bid if bid > 0 else px
                        if mark <= 0:
                            continue

                        if mark > pos.peak_price:
                            pos.peak_price = mark

                        gain_pct = pct_change(pos.entry_price, mark)

                        if (not pos.trailing_armed) and gain_pct >= PROFIT_ARM_PCT:
                            pos.trailing_armed = True
                            trail = pos.peak_price * (1.0 - TRAIL_STOP_FROM_PEAK_PCT / 100.0)
                            # make sure it locks profit once armed
                            trail = max(trail, pos.entry_price * 1.0005)
                            pos.trailing_stop_price = trail
                            logger.info(
                                f"TRAIL ARMED | {symbol} | gain={gain_pct:.3f}% | peak={pos.peak_price:.8f} | stop={pos.trailing_stop_price:.8f}"
                            )

                        if pos.trailing_armed:
                            new_trail = pos.peak_price * (1.0 - TRAIL_STOP_FROM_PEAK_PCT / 100.0)
                            new_trail = max(new_trail, pos.entry_price * 1.0005)
                            if new_trail > pos.trailing_stop_price:
                                pos.trailing_stop_price = new_trail

                        exit_reason = None

                        # down 1 percent from purchase sell immediately
                        if gain_pct <= -STOP_LOSS_FROM_ENTRY_PCT:
                            exit_reason = "hard_stop_minus_1pct"

                        # after +1 percent arm, trail by 0.5 percent
                        if exit_reason is None and pos.trailing_armed and pos.trailing_stop_price > 0 and mark <= pos.trailing_stop_price:
                            exit_reason = "trail_stop_0_5pct"

                        if exit_reason:
                            sell_order = await create_market_sell(exchange, symbol, pos.amount)
                            if sell_order is None:
                                logger.warning(f"SELL retry later | {symbol} | reason={exit_reason}")
                                continue

                            filled_amt, avg_sell = extract_order_fill(sell_order, mark)
                            if filled_amt <= 0:
                                filled_amt = pos.amount

                            pnl_usd = (avg_sell - pos.entry_price) * filled_amt
                            pnl_pct = pct_change(pos.entry_price, avg_sell)

                            state.realized_pnl_usd += pnl_usd
                            state.total_sells += 1
                            if pnl_usd >= 0:
                                state.total_wins += 1
                            else:
                                state.total_losses += 1

                            logger.info(
                                f"SELL | {symbol} | reason={exit_reason} | entry={pos.entry_price:.8f} | exit={avg_sell:.8f} | pnl={pnl_usd:.4f} USD ({pnl_pct:.3f}%)"
                            )

                            jsonl_append(EVENT_LOG_JSONL, {
                                "ts_utc": utc_now_iso(),
                                "event": "sell",
                                "symbol": symbol,
                                "reason": exit_reason,
                                "entry_price": pos.entry_price,
                                "exit_price": avg_sell,
                                "amount": filled_amt,
                                "pnl_usd": pnl_usd,
                                "pnl_pct": pnl_pct,
                            })

                            state.last_trade_ts_by_symbol[symbol] = time.time()
                            state.positions.pop(symbol, None)
                            state.save()

                    last_monitor_ts = time.time()

                except Exception as e:
                    logger.exception(f"Position monitor error | {e}")

            # Heartbeat with PnL
            if (time.time() - last_heartbeat_ts) >= HEARTBEAT_EVERY_SECONDS:
                try:
                    unrealized_total = 0.0
                    held_symbols = list(state.positions.keys())

                    if held_symbols:
                        held_tickers = await get_or_refresh_tickers(exchange, held_symbols, ticker_cache, force=True)
                        for sym, pos in state.positions.items():
                            t = held_tickers.get(sym, {})
                            bid, ask, px = get_best_prices_from_ticker(t)
                            mark = bid if bid > 0 else px
                            pnl_usd, _ = calc_unrealized(pos, mark)
                            unrealized_total += pnl_usd

                    total = state.realized_pnl_usd + unrealized_total
                    logger.info(
                        "HEARTBEAT | pos=%d/%d | buys=%d sells=%d | wins=%d losses=%d | realized=%.4f | unrealized=%.4f | total=%.4f",
                        len(state.positions),
                        MAX_POSITIONS,
                        state.total_buys,
                        state.total_sells,
                        state.total_wins,
                        state.total_losses,
                        state.realized_pnl_usd,
                        unrealized_total,
                        total,
                    )
                    last_heartbeat_ts = time.time()
                except Exception as e:
                    logger.exception(f"Heartbeat error | {e}")

            # Entry logic
            open_slots = MAX_POSITIONS - len(state.positions)
            if open_slots > 0:
                try:
                    all_tickers = await get_or_refresh_tickers(exchange, universe_symbols, ticker_cache, force=True)
                    candidates = green_buy_candidates(all_tickers)

                    logger.info(
                        f"SCAN | green_candidates={len(candidates)} | positions={len(state.positions)}/{MAX_POSITIONS}"
                    )

                    entries = 0
                    for row in candidates:
                        if len(state.positions) >= MAX_POSITIONS:
                            break
                        if entries >= MAX_NEW_ENTRIES_PER_LOOP:
                            break

                        symbol = row["symbol"]
                        if symbol in state.positions:
                            continue

                        last_ts = safe_float(state.last_trade_ts_by_symbol.get(symbol))
                        if (time.time() - last_ts) < SYMBOL_COOLDOWN_SECONDS:
                            continue

                        t = all_tickers.get(symbol, {})
                        buy_order = await create_market_buy(exchange, symbol, USD_PER_TRADE, t)
                        state.last_trade_ts_by_symbol[symbol] = time.time()

                        if buy_order is None:
                            continue

                        bid, ask, px = get_best_prices_from_ticker(t)
                        fill_amount, fill_avg = extract_order_fill(buy_order, ask if ask > 0 else px)
                        if fill_amount <= 0 or fill_avg <= 0:
                            logger.warning(f"BUY invalid fill | {symbol}")
                            continue

                        entry_cost = fill_amount * fill_avg
                        pos = Position(
                            symbol=symbol,
                            amount=fill_amount,
                            entry_price=fill_avg,
                            entry_cost_usd=entry_cost,
                            entry_time_ts=time.time(),
                            peak_price=fill_avg,
                            trailing_armed=False,
                            trailing_stop_price=0.0,
                        )
                        state.positions[symbol] = pos
                        state.total_buys += 1
                        entries += 1
                        state.save()

                        logger.info(
                            f"BUY | {symbol} | change={row['change_pct']:.3f}% | spread={row['spread_pct']:.3f}% | filled={fill_amount:.10f} | avg={fill_avg:.8f} | cost={entry_cost:.4f}"
                        )

                        jsonl_append(EVENT_LOG_JSONL, {
                            "ts_utc": utc_now_iso(),
                            "event": "buy",
                            "symbol": symbol,
                            "change_pct": row["change_pct"],
                            "spread_pct": row["spread_pct"],
                            "quote_vol": row["quote_vol"],
                            "score": row["score"],
                            "filled": fill_amount,
                            "avg_buy": fill_avg,
                            "cost_usd": entry_cost,
                        })

                except Exception as e:
                    logger.exception(f"Entry loop error | {e}")

            state.save()

            elapsed = time.time() - loop_start
            sleep_for = max(1.0, SCAN_INTERVAL_SECONDS - elapsed)
            await asyncio.sleep(sleep_for)

    finally:
        state.save()
        try:
            await exchange.close()
        except Exception:
            pass

def main() -> None:
    try:
        asyncio.run(bot_loop())
    except KeyboardInterrupt:
        logger.info("Stopped by user")

if __name__ == "__main__":
    main()
