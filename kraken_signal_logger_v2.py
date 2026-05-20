import os
import sys
import time
import json
import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import ccxt.async_support as ccxt


load_dotenv()


# =========================
# CONFIG
# =========================
SCAN_INTERVAL_SECONDS = 30
TOP_N_SCANNER = 20
TOP_N_CANDLE_CHECK = 12

QUOTE_CURRENCIES = {"USD", "USDT"}
PREFER_QUOTE = "USD"

MIN_QUOTE_VOLUME_24H = 250000.0
MAX_SPREAD_PCT = 0.80

LOG_FILE = "kraken_signal_logger_v2.log"
SIGNAL_LOG_JSONL = "kraken_signal_events_v2.jsonl"

EXCLUDED_BASES = {
    "USD",
    "USDT",
    "USDC",
    "USDG",
    "EUR",
    "GBP",
    "AUD",
    "CAD",
    "CHF",
    "JPY",
}

EXCLUDED_SYMBOLS = {
    "USDT/USD",
    "USDC/USD",
    "USDG/USD",
    "EUR/USD",
    "GBP/USD",
    "AUD/USD",
    "CAD/USD",
    "CHF/USD",
    "JPY/USD",
    "USDC/USDT",
    "USDT/USDC",
}

EXCLUDED_BASE_SUBSTRINGS = {
    "USD",
    "EUR",
    "GBP",
    "AUD",
    "CAD",
    "CHF",
    "JPY",
}

BASE_SUBSTRING_EXCEPTIONS = set()

# Stage 1 momentum scanner tuning
MIN_POSITIVE_CHANGE_PCT = 0.05
RED_COIN_HARD_PENALTY = 30.0
LOW_POSITIVE_PENALTY = 4.0
VOLUME_CAP_M = 20.0
PREFER_USD_BONUS = 0.5

# Stage 2 candle signal checks (read only)
OHLCV_TIMEFRAME = "1m"
OHLCV_LIMIT = 30

MIN_CANDLES_REQUIRED = 20
MIN_LAST_CLOSE = 0.00000001

# v2 is intentionally looser than v1
REQUIRE_LAST_BAR_GREEN = False
REQUIRE_HIGHER_LOW = False
REQUIRE_BREAKOUT_ABOVE_PREV_HIGH = False

MIN_3BAR_RETURN_PCT = 0.05
MIN_5BAR_RETURN_PCT = 0.10

MAX_DISTANCE_FROM_5BAR_LOW_PCT = 5.5
MAX_LAST_CANDLE_BODY_TO_RANGE_RATIO = 1.0
MAX_PULLBACK_FROM_RECENT_HIGH_PCT = 4.0

# Confidence thresholds
WOULD_BUY_CONFIDENCE_MIN = 65.0
LOG_WATCHLIST_CONFIDENCE_MIN = 45.0

# Cooldown
SIGNAL_COOLDOWN_SECONDS = 180
WATCH_COOLDOWN_SECONDS = 120


# =========================
# LOGGING
# =========================
logger = logging.getLogger("kraken_signal_logger_v2")
logger.setLevel(logging.INFO)
logger.handlers.clear()

_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

_console = logging.StreamHandler(sys.stdout)
_console.setFormatter(_formatter)
logger.addHandler(_console)

_file = logging.FileHandler(LOG_FILE, encoding="utf-8")
_file.setFormatter(_formatter)
logger.addHandler(_file)


# =========================
# HELPERS
# =========================
def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def pct_change_from_ticker(ticker: Dict[str, Any]) -> float:
    percentage = ticker.get("percentage")
    if percentage is not None:
        return safe_float(percentage, 0.0)

    last_price = safe_float(ticker.get("last"))
    open_price = safe_float(ticker.get("open"))
    if open_price <= 0:
        return 0.0
    return ((last_price - open_price) / open_price) * 100.0


def spread_pct_from_ticker(ticker: Dict[str, Any]) -> float:
    bid = safe_float(ticker.get("bid"))
    ask = safe_float(ticker.get("ask"))
    if bid <= 0 or ask <= 0:
        return 999.0
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return 999.0
    return ((ask - bid) / mid) * 100.0


def quote_volume_from_ticker(ticker: Dict[str, Any]) -> float:
    qv = ticker.get("quoteVolume")
    if qv is not None:
        return safe_float(qv, 0.0)
    return 0.0


def extract_quote_from_symbol(symbol: str) -> str:
    if "/" not in symbol:
        return ""
    return symbol.split("/")[-1].strip().upper()


def extract_base_from_symbol(symbol: str) -> str:
    if "/" not in symbol:
        return symbol.upper()
    return symbol.split("/")[0].strip().upper()


def is_spot_market(market: Dict[str, Any]) -> bool:
    if market.get("spot") is True:
        return True
    if market.get("type") == "spot":
        return True
    return False


def is_market_active(market: Dict[str, Any]) -> bool:
    active = market.get("active")
    if active is None:
        return True
    return bool(active)


def base_looks_like_stable_or_fiat(base: str) -> bool:
    if base in BASE_SUBSTRING_EXCEPTIONS:
        return False

    if base in EXCLUDED_BASES:
        return True

    for frag in EXCLUDED_BASE_SUBSTRINGS:
        if frag in base:
            return True

    return False


def symbol_is_allowed(market: Dict[str, Any]) -> bool:
    symbol = market.get("symbol", "")
    if not symbol or "/" not in symbol:
        return False

    symbol = symbol.upper()

    if symbol in EXCLUDED_SYMBOLS:
        return False

    if not is_spot_market(market):
        return False

    if not is_market_active(market):
        return False

    quote = extract_quote_from_symbol(symbol)
    base = extract_base_from_symbol(symbol)

    if base_looks_like_stable_or_fiat(base):
        return False

    if quote not in QUOTE_CURRENCIES:
        return False

    return True


def quote_pref_bonus(symbol: str) -> float:
    if extract_quote_from_symbol(symbol) == "USD":
        return PREFER_USD_BONUS
    return 0.0


def score_candidate(symbol: str, change_pct: float, quote_vol: float, spread_pct: float) -> float:
    volume_m = min(quote_vol / 1_000_000.0, VOLUME_CAP_M)
    spread_penalty = spread_pct * 4.0
    momentum_score = change_pct * 8.0

    score = momentum_score + volume_m - spread_penalty + quote_pref_bonus(symbol)

    if change_pct <= 0:
        score -= RED_COIN_HARD_PENALTY
    elif change_pct < MIN_POSITIVE_CHANGE_PCT:
        score -= LOW_POSITIVE_PENALTY

    return score


def pct_move(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return ((b - a) / a) * 100.0


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def append_jsonl(filepath: str, row: Dict[str, Any]) -> None:
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


# =========================
# EXCHANGE
# =========================
def build_exchange() -> ccxt.kraken:
    api_key = os.getenv("KRAKEN_API_KEY", "").strip()
    secret = os.getenv("KRAKEN_API_SECRET", "").strip()

    if not api_key or not secret:
        raise RuntimeError("Missing KRAKEN_API_KEY or KRAKEN_API_SECRET in .env")

    exchange = ccxt.kraken(
        {
            "apiKey": api_key,
            "secret": secret,
            "enableRateLimit": True,
            "timeout": 30000,
        }
    )
    return exchange


# =========================
# STAGE 1 SCANNER
# =========================
async def load_tradeable_symbols(exchange: ccxt.kraken) -> List[str]:
    markets = await exchange.load_markets()
    symbols: List[str] = []

    for symbol, market in markets.items():
        if not isinstance(market, dict):
            continue
        if symbol_is_allowed(market):
            symbols.append(symbol.upper())

    symbols = sorted(set(symbols))
    symbols.sort(key=lambda s: (extract_quote_from_symbol(s) != PREFER_QUOTE, s))
    return symbols


async def fetch_tickers_safe(
    exchange: ccxt.kraken, symbols: List[str]
) -> Dict[str, Dict[str, Any]]:
    try:
        tickers = await exchange.fetch_tickers(symbols)
        if isinstance(tickers, dict) and tickers:
            return {k.upper(): v for k, v in tickers.items() if isinstance(v, dict)}
    except Exception as e:
        logger.warning(f"Batch fetch_tickers failed, falling back to per symbol: {e}")

    out: Dict[str, Dict[str, Any]] = {}
    for symbol in symbols:
        try:
            t = await exchange.fetch_ticker(symbol)
            if isinstance(t, dict):
                out[symbol.upper()] = t
        except Exception as e:
            logger.debug(f"fetch_ticker failed for {symbol}: {e}")
            await asyncio.sleep(0.03)
    return out


def build_ranked_list(tickers: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []

    for symbol, t in tickers.items():
        if not isinstance(t, dict):
            continue

        if symbol in EXCLUDED_SYMBOLS:
            continue

        base = extract_base_from_symbol(symbol)
        if base_looks_like_stable_or_fiat(base):
            continue

        quote = extract_quote_from_symbol(symbol)
        if quote not in QUOTE_CURRENCIES:
            continue

        last_price = safe_float(t.get("last"))
        if last_price <= 0:
            continue

        quote_vol = quote_volume_from_ticker(t)
        change_pct = pct_change_from_ticker(t)
        spread_pct = spread_pct_from_ticker(t)

        if quote_vol < MIN_QUOTE_VOLUME_24H:
            continue

        if spread_pct > MAX_SPREAD_PCT:
            continue

        if change_pct <= 0:
            continue

        score = score_candidate(symbol, change_pct, quote_vol, spread_pct)

        ranked.append(
            {
                "symbol": symbol,
                "last": last_price,
                "change_pct": change_pct,
                "quote_vol": quote_vol,
                "spread_pct": spread_pct,
                "score": score,
            }
        )

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked


def log_top_candidates(ranked: List[Dict[str, Any]], top_n: int) -> None:
    if not ranked:
        logger.info("Momentum scanner v2 found no candidates after filters")
        return

    top = ranked[:top_n]
    logger.info(f"Top {len(top)} momentum candidates v2")

    for i, row in enumerate(top, start=1):
        logger.info(
            f"{i:02d} | {row['symbol']:<12} | "
            f"score={row['score']:>7.2f} | "
            f"chg={row['change_pct']:>7.2f}% | "
            f"spread={row['spread_pct']:>5.3f}% | "
            f"qv={row['quote_vol']:>12,.0f} | "
            f"last={row['last']}"
        )


# =========================
# STAGE 2 CANDLE SIGNALS
# =========================
def parse_ohlcv_rows(rows: List[List[Any]]) -> List[Dict[str, float]]:
    candles: List[Dict[str, float]] = []
    for row in rows:
        if not isinstance(row, list) or len(row) < 6:
            continue
        ts = int(row[0])
        o = safe_float(row[1])
        h = safe_float(row[2])
        l = safe_float(row[3])
        c = safe_float(row[4])
        v = safe_float(row[5])
        if min(o, h, l, c) <= 0:
            continue
        candles.append(
            {"ts": ts, "open": o, "high": h, "low": l, "close": c, "volume": v}
        )
    return candles


def evaluate_signal_v2(symbol: str, candles: List[Dict[str, float]]) -> Tuple[str, Dict[str, Any]]:
    info: Dict[str, Any] = {
        "symbol": symbol,
        "reason": "",
    }

    if len(candles) < MIN_CANDLES_REQUIRED:
        info["reason"] = f"not_enough_candles:{len(candles)}"
        return "no_signal", info

    last = candles[-1]
    prev = candles[-2]
    prev2 = candles[-3]
    prev3 = candles[-4]
    last5 = candles[-5:]

    last_open = last["open"]
    last_high = last["high"]
    last_low = last["low"]
    last_close = last["close"]

    if last_close < MIN_LAST_CLOSE:
        info["reason"] = "last_close_too_small"
        return "no_signal", info

    last_range = max(last_high - last_low, 1e-12)
    last_body = abs(last_close - last_open)
    body_to_range = last_body / last_range

    ret_1 = pct_move(prev["close"], last_close)
    ret_3 = pct_move(prev3["close"], last_close)
    ret_5 = pct_move(last5[0]["close"], last_close)

    higher_low = last_low > prev["low"]
    breakout_above_prev_high = last_close > prev["high"]
    last_green = last_close > last_open

    low_5 = min(c["low"] for c in last5)
    high_5 = max(c["high"] for c in last5)

    dist_from_5_low = pct_move(low_5, last_close)
    pullback_from_5_high = pct_move(high_5, last_close)

    info.update(
        {
            "last_close": last_close,
            "ret_1_pct": round(ret_1, 4),
            "ret_3_pct": round(ret_3, 4),
            "ret_5_pct": round(ret_5, 4),
            "body_to_range": round(body_to_range, 4),
            "higher_low": higher_low,
            "breakout_above_prev_high": breakout_above_prev_high,
            "last_green": last_green,
            "dist_from_5_low_pct": round(dist_from_5_low, 4),
            "pullback_from_5_high_pct": round(pullback_from_5_high, 4),
        }
    )

    # Hard fails only
    if ret_3 < MIN_3BAR_RETURN_PCT:
        info["reason"] = "ret_3_too_small"
        return "no_signal", info

    if ret_5 < MIN_5BAR_RETURN_PCT:
        info["reason"] = "ret_5_too_small"
        return "no_signal", info

    if dist_from_5_low > MAX_DISTANCE_FROM_5BAR_LOW_PCT:
        info["reason"] = "too_extended_from_5bar_low"
        return "no_signal", info

    if pullback_from_5_high < -MAX_PULLBACK_FROM_RECENT_HIGH_PCT:
        info["reason"] = "too_far_below_recent_high"
        return "no_signal", info

    if body_to_range > MAX_LAST_CANDLE_BODY_TO_RANGE_RATIO:
        info["reason"] = "invalid_candle_ratio"
        return "no_signal", info

    # Confidence score
    confidence = 50.0

    # Structure bonuses
    if last_green:
        confidence += 10.0
    if higher_low:
        confidence += 10.0
    if breakout_above_prev_high:
        confidence += 15.0

    # Momentum bonuses
    confidence += clamp(ret_1 * 10.0, -5.0, 12.0)
    confidence += clamp(ret_3 * 8.0, 0.0, 20.0)
    confidence += clamp(ret_5 * 6.0, 0.0, 20.0)

    # Extension / pullback handling
    if dist_from_5_low <= 3.5:
        confidence += 8.0
    elif dist_from_5_low <= 4.5:
        confidence += 3.0
    else:
        confidence -= 6.0

    if pullback_from_5_high >= -0.5:
        confidence += 8.0
    elif pullback_from_5_high >= -1.5:
        confidence += 3.0
    else:
        confidence -= 6.0

    # Candle shape quality
    if body_to_range >= 0.35:
        confidence += 5.0
    elif body_to_range < 0.10:
        confidence -= 4.0

    confidence = round(clamp(confidence, 0.0, 100.0), 2)
    info["confidence"] = confidence

    if confidence >= WOULD_BUY_CONFIDENCE_MIN:
        info["reason"] = "would_buy_signal_v2"
        return "would_buy", info

    if confidence >= LOG_WATCHLIST_CONFIDENCE_MIN:
        info["reason"] = "watchlist_signal_v2"
        return "watchlist", info

    info["reason"] = "confidence_too_low"
    return "no_signal", info


async def fetch_ohlcv_safe(
    exchange: ccxt.kraken, symbol: str, timeframe: str, limit: int
) -> List[Dict[str, float]]:
    try:
        rows = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return parse_ohlcv_rows(rows)
    except Exception as e:
        logger.debug(f"fetch_ohlcv failed for {symbol}: {e}")
        return []


# =========================
# SIGNAL STATE
# =========================
class CooldownMap:
    def __init__(self, seconds: int) -> None:
        self.seconds = seconds
        self.last_ts: Dict[str, float] = {}

    def can_emit(self, key: str, now_ts: float) -> bool:
        prev = self.last_ts.get(key)
        if prev is None:
            return True
        return (now_ts - prev) >= self.seconds

    def mark(self, key: str, now_ts: float) -> None:
        self.last_ts[key] = now_ts


# =========================
# MAIN LOOP
# =========================
async def signal_logger_loop_v2() -> None:
    exchange: Optional[ccxt.kraken] = None
    would_buy_cooldown = CooldownMap(SIGNAL_COOLDOWN_SECONDS)
    watch_cooldown = CooldownMap(WATCH_COOLDOWN_SECONDS)

    try:
        exchange = build_exchange()
        logger.info("Starting Kraken signal logger v2 (read only)")

        server_time = await exchange.fetch_time()
        balance = await exchange.fetch_balance()
        total_keys = len(balance.get("total", {})) if isinstance(balance, dict) else 0

        logger.info(f"Kraken server time: {server_time}")
        logger.info(f"Balance fetched successfully (asset slots: {total_keys})")

        symbols = await load_tradeable_symbols(exchange)
        logger.info(f"Loaded {len(symbols)} tradable spot symbols for scanning")

        if not symbols:
            logger.warning("No symbols loaded. Check market filters.")
            return

        while True:
            cycle_start = time.time()

            try:
                tickers = await fetch_tickers_safe(exchange, symbols)
                ranked = build_ranked_list(tickers)
                log_top_candidates(ranked, TOP_N_SCANNER)

                top_for_candles = ranked[:TOP_N_CANDLE_CHECK]
                logger.info(
                    f"Checking {len(top_for_candles)} symbols with {OHLCV_TIMEFRAME} candles (read only)"
                )

                checked = 0
                would_buy_hits = 0
                watch_hits = 0

                for row in top_for_candles:
                    symbol = row["symbol"]
                    checked += 1

                    candles = await fetch_ohlcv_safe(exchange, symbol, OHLCV_TIMEFRAME, OHLCV_LIMIT)
                    status, details = evaluate_signal_v2(symbol, candles)

                    now_ts = time.time()

                    if status == "would_buy":
                        key = f"would_buy:{symbol}"
                        if would_buy_cooldown.can_emit(key, now_ts):
                            would_buy_cooldown.mark(key, now_ts)
                            would_buy_hits += 1

                            logger.info(
                                "WOULD BUY v2 | "
                                f"{symbol} | "
                                f"conf={details.get('confidence')} | "
                                f"ret1={details.get('ret_1_pct')}% | "
                                f"ret3={details.get('ret_3_pct')}% | "
                                f"ret5={details.get('ret_5_pct')}% | "
                                f"spread={row['spread_pct']:.3f}% | "
                                f"qv={row['quote_vol']:.0f}"
                            )

                            append_jsonl(
                                SIGNAL_LOG_JSONL,
                                {
                                    "ts_utc": now_utc_iso(),
                                    "event": "would_buy_v2",
                                    "symbol": symbol,
                                    "timeframe": OHLCV_TIMEFRAME,
                                    "scanner": {
                                        "score": row["score"],
                                        "change_pct": row["change_pct"],
                                        "spread_pct": row["spread_pct"],
                                        "quote_vol": row["quote_vol"],
                                        "last": row["last"],
                                    },
                                    "signal": details,
                                },
                            )
                        else:
                            logger.info(f"Cooldown active | WOULD BUY v2 | {symbol}")

                    elif status == "watchlist":
                        key = f"watch:{symbol}"
                        if watch_cooldown.can_emit(key, now_ts):
                            watch_cooldown.mark(key, now_ts)
                            watch_hits += 1

                            logger.info(
                                "WATCHLIST v2 | "
                                f"{symbol} | "
                                f"conf={details.get('confidence')} | "
                                f"ret1={details.get('ret_1_pct')}% | "
                                f"ret3={details.get('ret_3_pct')}% | "
                                f"ret5={details.get('ret_5_pct')}%"
                            )

                            append_jsonl(
                                SIGNAL_LOG_JSONL,
                                {
                                    "ts_utc": now_utc_iso(),
                                    "event": "watchlist_v2",
                                    "symbol": symbol,
                                    "timeframe": OHLCV_TIMEFRAME,
                                    "scanner": {
                                        "score": row["score"],
                                        "change_pct": row["change_pct"],
                                        "spread_pct": row["spread_pct"],
                                        "quote_vol": row["quote_vol"],
                                        "last": row["last"],
                                    },
                                    "signal": details,
                                },
                            )
                        else:
                            logger.info(f"Cooldown active | WATCHLIST v2 | {symbol}")

                    else:
                        logger.info(f"NO SIGNAL v2 | {symbol:<12} | {details.get('reason')}")

                    await asyncio.sleep(0.03)

                elapsed = time.time() - cycle_start
                logger.info(
                    f"Cycle done v2 | scanned={len(ranked)} | candle_checked={checked} | "
                    f"watch_hits={watch_hits} | would_buy_hits={would_buy_hits} | elapsed={elapsed:.2f}s"
                )

            except Exception as cycle_error:
                logger.exception(f"Signal logger v2 cycle error: {cycle_error}")

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    except asyncio.CancelledError:
        logger.info("Signal logger v2 cancelled")
        raise
    except KeyboardInterrupt:
        logger.info("Signal logger v2 stopped by user")
    except Exception as e:
        logger.exception(f"Fatal signal logger v2 error: {e}")
    finally:
        if exchange is not None:
            try:
                await exchange.close()
            except Exception:
                pass
        logger.info("Signal logger v2 shutdown complete")


def main() -> None:
    try:
        asyncio.run(signal_logger_loop_v2())
    except KeyboardInterrupt:
        logger.info("Exited")


if __name__ == "__main__":
    main()