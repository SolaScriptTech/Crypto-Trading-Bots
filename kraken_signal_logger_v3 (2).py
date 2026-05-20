import os
import sys
import time
import json
import math
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
TOP_N_CANDLE_CHECK = 20

QUOTE_CURRENCIES = {"USD"}
PREFER_QUOTE = "USD"

MIN_QUOTE_VOLUME_24H = 900000.0
MAX_SPREAD_PCT = 0.55

LOG_FILE = "kraken_signal_logger_v3.log"
SIGNAL_LOG_JSONL = "kraken_signal_events_v3.jsonl"

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
    "XAU",
    "XAG",
    "PAXG",
}

BASE_SUBSTRING_EXCEPTIONS = set()

# Stage 1 momentum scanner tuning
MIN_POSITIVE_CHANGE_PCT = 0.15
RED_COIN_HARD_PENALTY = 30.0
LOW_POSITIVE_PENALTY = 4.0
VOLUME_CAP_M = 20.0
PREFER_USD_BONUS = 0.5

# Stage 2 candle signal checks (read only)
OHLCV_TIMEFRAME = "1m"
OHLCV_LIMIT = 40

MIN_CANDLES_REQUIRED = 20
MIN_LAST_CLOSE = 0.00000001

# Hard fails (keep broad like v2, but not reckless)
MIN_3BAR_RETURN_PCT = 0.24
MIN_5BAR_RETURN_PCT = 0.42
MAX_DISTANCE_FROM_5BAR_LOW_PCT = 4.2
MAX_LAST_CANDLE_BODY_TO_RANGE_RATIO = 0.92
MAX_PULLBACK_FROM_RECENT_HIGH_PCT = 0.95

# Confidence thresholds
WOULD_BUY_CONFIDENCE_MIN = 72.0
LOG_WATCHLIST_CONFIDENCE_MIN = 56.0
V3_STRONG_CONFIDENCE_BUY = 78.0
V3_MIN_BAR_RANGE_PCT = 0.10
V3_MIN_MEDIAN_RANGE_PCT_8 = 0.10
V3_MIN_GREEN_BARS_LAST3 = 2
V3_MIN_GREEN_BARS_LAST5 = 3
V3_MIN_LAST2_VOLUME_BURST = 1.30
V3_MIN_3BAR_VOLUME_RATIO = 1.12
V3_MIN_CLOSE_POSITION_IN_BAR = 0.58
V3_MIN_BREAKOUT_DISTANCE_PCT = 0.03
V3_MAX_BREAKOUT_DISTANCE_PCT = 0.95
V3_MAX_CHASE_IF_HIGH_VOLUME_PCT = 3.20

# Final execution quality gates for would_buy (still read only logger)
V3_MAX_WOULD_BUY_SPREAD_PCT = 0.35
V3_MIN_WOULD_BUY_SCANNER_CHANGE_PCT = 0.20
V3_STRONG_REVERSAL_CONFIDENCE_MIN = 88.0
V3_MIN_RED_COIN_VOLUME_RATIO = 1.25

# Alias names used later in evaluate function
V3_CONFIDENCE_BUY = WOULD_BUY_CONFIDENCE_MIN
V3_CONFIDENCE_WATCH = LOG_WATCHLIST_CONFIDENCE_MIN

# Cooldowns
SIGNAL_COOLDOWN_SECONDS = 180
WATCH_COOLDOWN_SECONDS = 120

# Exit planning defaults (logged only for backtesting, not traded live)
DEFAULT_HARD_STOP_PCT = 1.25
DEFAULT_TIME_STOP_MIN = 10

# Risk tuning for dynamic exit plans
MIN_HARD_STOP_PCT = 0.60
MAX_HARD_STOP_PCT = 2.20
MIN_TRAIL_PCT = 0.35
MAX_TRAIL_PCT = 1.40

# Startup behavior
PERFORM_PRIVATE_BALANCE_CHECK = False  # keep scanner usable with read only public API keys


# =========================
# LOGGING
# =========================
logger = logging.getLogger("kraken_signal_logger_v3")
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
    base_u = base.upper()
    if base_u in EXCLUDED_BASES:
        return True

    for token in EXCLUDED_BASE_SUBSTRINGS:
        if token in base_u and base_u not in BASE_SUBSTRING_EXCEPTIONS:
            return True

    return False


def symbol_is_allowed(market: Dict[str, Any]) -> bool:
    symbol = str(market.get("symbol", "")).upper()
    if not symbol:
        return False

    if symbol in EXCLUDED_SYMBOLS:
        return False

    if not is_spot_market(market):
        return False

    if not is_market_active(market):
        return False

    quote = extract_quote_from_symbol(symbol)
    base = extract_base_from_symbol(symbol)

    if quote not in QUOTE_CURRENCIES:
        return False

    if base_looks_like_stable_or_fiat(base):
        return False

    return True


def quote_pref_bonus(symbol: str) -> float:
    return PREFER_USD_BONUS if extract_quote_from_symbol(symbol) == PREFER_QUOTE else 0.0


def score_candidate(symbol: str, change_pct: float, quote_vol: float, spread_pct: float) -> float:
    """
    Stage 1 scanner ranking score.
    Keep this broad because the goal is to pull in movers, then let candle logic decide.
    """
    score = 0.0

    if change_pct < 0:
        score -= RED_COIN_HARD_PENALTY
    elif change_pct < MIN_POSITIVE_CHANGE_PCT:
        score -= LOW_POSITIVE_PENALTY
    else:
        score += min(change_pct, 15.0) * 2.2

    vol_m = quote_vol / 1_000_000.0
    score += min(vol_m, VOLUME_CAP_M)

    # lower spread is better
    score -= min(max(spread_pct, 0.0), 5.0) * 4.0

    score += quote_pref_bonus(symbol)

    return round(score, 4)


def pct_move(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return ((b - a) / a) * 100.0


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def append_jsonl(filepath: str, row: Dict[str, Any]) -> None:
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def median(values: List[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    mid = n // 2
    if n % 2 == 1:
        return s[mid]
    return (s[mid - 1] + s[mid]) / 2.0


# =========================
# EXCHANGE
# =========================
def build_exchange() -> ccxt.kraken:
    return ccxt.kraken(
        {
            "enableRateLimit": True,
            "timeout": 15000,
            "apiKey": os.getenv("KRAKEN_API_KEY", ""),
            "secret": os.getenv("KRAKEN_API_SECRET", ""),
        }
    )


async def load_tradeable_symbols(exchange: ccxt.kraken) -> List[str]:
    markets = await exchange.load_markets()
    symbols: List[str] = []

    for market in markets.values():
        try:
            if symbol_is_allowed(market):
                symbols.append(str(market["symbol"]))
        except Exception:
            continue

    symbols = sorted(set(symbols))
    return symbols


async def fetch_tickers_safe(
    exchange: ccxt.kraken, symbols: List[str]
) -> Dict[str, Dict[str, Any]]:
    try:
        return await exchange.fetch_tickers(symbols)
    except Exception as e:
        logger.exception(f"fetch_tickers failed: {e}")
        return {}


def build_ranked_list(tickers: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []

    for symbol, ticker in tickers.items():
        try:
            if not isinstance(ticker, dict):
                continue

            last = safe_float(ticker.get("last"))
            if last <= 0:
                continue

            qv = quote_volume_from_ticker(ticker)
            if qv < MIN_QUOTE_VOLUME_24H:
                continue

            spread_pct = spread_pct_from_ticker(ticker)
            if spread_pct > MAX_SPREAD_PCT:
                continue

            change_pct = pct_change_from_ticker(ticker)
            score = score_candidate(symbol, change_pct, qv, spread_pct)

            ranked.append(
                {
                    "symbol": symbol,
                    "last": last,
                    "quote_vol": qv,
                    "spread_pct": spread_pct,
                    "change_pct": change_pct,
                    "score": score,
                }
            )
        except Exception:
            continue

    ranked.sort(key=lambda x: x["score"], reverse=True)
    return ranked


def log_top_candidates(ranked: List[Dict[str, Any]], top_n: int) -> None:
    if not ranked:
        logger.info("Momentum scanner found no candidates after filters")
        return

    top = ranked[:top_n]
    logger.info(f"Top {len(top)} momentum candidates")
    for i, row in enumerate(top, start=1):
        logger.info(
            f"#{i:02d} {row['symbol']:<12} "
            f"score={row['score']:.2f} "
            f"chg={row['change_pct']:.3f}% "
            f"spr={row['spread_pct']:.3f}% "
            f"qv={row['quote_vol']:.0f} "
            f"last={row['last']}"
        )


# =========================
# CANDLES
# =========================
def parse_ohlcv_rows(rows: List[List[Any]]) -> List[Dict[str, float]]:
    candles: List[Dict[str, float]] = []
    for r in rows:
        if not isinstance(r, list) or len(r) < 6:
            continue
        ts, o, h, l, c, v = r[:6]
        candles.append(
            {
                "ts": safe_float(ts),
                "open": safe_float(o),
                "high": safe_float(h),
                "low": safe_float(l),
                "close": safe_float(c),
                "volume": safe_float(v),
            }
        )
    return candles


async def fetch_ohlcv_safe(
    exchange: ccxt.kraken,
    symbol: str,
    timeframe: str,
    limit: int,
    retries: int = 3,
) -> List[Dict[str, float]]:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            rows = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            return parse_ohlcv_rows(rows or [])
        except Exception as e:
            last_err = e
            if attempt < retries:
                await asyncio.sleep(0.6 * attempt)
            else:
                logger.debug(f"fetch_ohlcv failed for {symbol}: {e}")
    if last_err is not None:
        logger.debug(f"fetch_ohlcv giving up for {symbol}: {last_err}")
    return []


def _recent_range_stats(candles: List[Dict[str, float]], lookback: int = 8) -> Dict[str, float]:
    use = candles[-lookback:] if len(candles) >= lookback else candles[:]
    if not use:
        return {
            "median_range_pct": 0.0,
            "avg_range_pct": 0.0,
            "median_body_pct": 0.0,
            "avg_volume": 0.0,
            "last_volume": 0.0,
            "volume_ratio_last_vs_avg": 0.0,
        }

    range_pcts: List[float] = []
    body_pcts: List[float] = []
    vols: List[float] = []

    for c in use:
        c_close = max(c["close"], 1e-12)
        c_range = max(c["high"] - c["low"], 0.0)
        c_body = abs(c["close"] - c["open"])
        range_pcts.append((c_range / c_close) * 100.0)
        body_pcts.append((c_body / c_close) * 100.0)
        vols.append(c["volume"])

    avg_vol = sum(vols) / max(len(vols), 1)
    last_vol = use[-1]["volume"]
    vol_ratio = (last_vol / avg_vol) if avg_vol > 0 else 0.0

    return {
        "median_range_pct": round(median(range_pcts), 4),
        "avg_range_pct": round(sum(range_pcts) / max(len(range_pcts), 1), 4),
        "median_body_pct": round(median(body_pcts), 4),
        "avg_volume": round(avg_vol, 6),
        "last_volume": round(last_vol, 6),
        "volume_ratio_last_vs_avg": round(vol_ratio, 4),
    }


def _close_position_in_bar(candle: Dict[str, float]) -> float:
    """
    Where the close sits inside the bar range.
    0 = closes at low, 1 = closes at high.
    """
    rng = max(candle["high"] - candle["low"], 1e-12)
    return (candle["close"] - candle["low"]) / rng


def build_exit_plan(details: Dict[str, Any]) -> Dict[str, Any]:
    confidence = float(details.get("confidence", 0.0))
    median_range = float(details.get("median_range_pct_8", 0.25))
    volume_burst = float(details.get("volume_burst", 1.0))
    breakout_distance = float(details.get("breakout_distance_pct", 0.0))
    dist_from_5_low = float(details.get("distance_from_5bar_low_pct", 0.0))

    # Base risk comes from actual symbol movement so the backtest is not one size fits all.
    base_risk = clamp(max(0.55, median_range * 1.45), 0.65, 1.60)

    # Tighten if the entry is getting stretched, loosen a little if volume is exceptional.
    if dist_from_5_low > 3.0 or breakout_distance > 0.80:
        base_risk *= 0.90
    if volume_burst >= 1.8:
        base_risk *= 1.08
    base_risk = clamp(base_risk, 0.60, 1.75)

    if confidence >= 84:
        tier = "A+"
        hard_stop = clamp(base_risk * 1.00, 0.70, 1.60)
        tp_trigger = clamp(max(0.90, base_risk * 0.85), 0.90, 1.50)
        trailing_stop = clamp(base_risk * 0.62, 0.50, 1.10)
        time_stop_minutes = 18
        break_even_after = clamp(base_risk * 0.55, 0.45, 0.95)
        momentum_fail_minutes = 3
        momentum_fail_min = 0.16
    elif confidence >= 78:
        tier = "A"
        hard_stop = clamp(base_risk * 0.92, 0.65, 1.40)
        tp_trigger = clamp(max(0.75, base_risk * 0.78), 0.75, 1.25)
        trailing_stop = clamp(base_risk * 0.58, 0.45, 0.90)
        time_stop_minutes = 14
        break_even_after = clamp(base_risk * 0.50, 0.40, 0.85)
        momentum_fail_minutes = 3
        momentum_fail_min = 0.13
    else:
        tier = "B"
        hard_stop = clamp(base_risk * 0.84, 0.60, 1.20)
        tp_trigger = clamp(max(0.60, base_risk * 0.70), 0.60, 1.00)
        trailing_stop = clamp(base_risk * 0.52, 0.38, 0.75)
        time_stop_minutes = 10
        break_even_after = clamp(base_risk * 0.44, 0.32, 0.70)
        momentum_fail_minutes = 2
        momentum_fail_min = 0.10

    return {
        "quality_tier": tier,
        "hard_stop_pct": round(float(hard_stop), 4),
        "take_profit_trigger_pct": round(float(tp_trigger), 4),
        "trailing_stop_pct": round(float(trailing_stop), 4),
        "time_stop_minutes": int(time_stop_minutes),
        "break_even_after_pct": round(float(break_even_after), 4),
        "momentum_fail_if_below_pct_after_minutes": int(momentum_fail_minutes),
        "momentum_fail_min_profit_pct": round(float(momentum_fail_min), 4),
        "max_chase_pct_observed": round(float(dist_from_5_low), 4),
        "notes": "v3 adaptive exit plan from median range, confidence, and extension",
    }

def evaluate_signal_v3(symbol: str, candles: List[Dict[str, float]], scanner_row: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
    if len(candles) < 20:
        return "no_signal", {"reason": "not_enough_candles", "candles": len(candles)}

    closes = [c["close"] for c in candles]
    opens = [c["open"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    vols = [c["volume"] for c in candles]

    last = candles[-1]
    prev = candles[-2]
    c0 = closes[-1]
    c1 = closes[-2]
    c2 = closes[-3]
    c3 = closes[-4]
    c4 = closes[-5]
    o0 = opens[-1]
    h0 = highs[-1]
    l0 = lows[-1]

    if c0 <= 0:
        return "no_signal", {"reason": "bad_price"}

    ret_1 = pct_change(c0, c1)
    ret_3 = pct_change(c0, c3)
    ret_5 = pct_change(c0, c4)

    # price action structure
    last_green = c0 > o0
    higher_low = lows[-1] >= lows[-2]
    breakout_above_prev_high = c0 > highs[-2]
    breakout_above_3bar_high = c0 > max(highs[-4:-1]) if len(highs) >= 4 else False
    recent_5_low = min(lows[-5:])
    recent_5_high = max(highs[-5:])
    dist_from_5_low = ((c0 / recent_5_low) - 1.0) * 100.0 if recent_5_low > 0 else 0.0
    pullback_from_5_high = ((c0 / recent_5_high) - 1.0) * 100.0 if recent_5_high > 0 else 0.0
    breakout_ref = max(highs[-6:-1]) if len(highs) >= 6 else highs[-2]
    breakout_distance_pct = ((c0 / breakout_ref) - 1.0) * 100.0 if breakout_ref > 0 else 0.0

    # candle quality
    bar_range_pct = ((h0 - l0) / c0) * 100.0 if c0 > 0 else 0.0
    body_to_range = abs(c0 - o0) / max(h0 - l0, 1e-12)
    close_pos_in_bar = (c0 - l0) / max(h0 - l0, 1e-12)
    median_range_pct_8 = median_pct_range(candles[-8:]) if len(candles) >= 8 else median_pct_range(candles)

    # volume impulse
    avg_vol_10 = safe_mean(vols[-11:-1]) if len(vols) >= 11 else safe_mean(vols[:-1])
    avg_vol_3_prev = safe_mean(vols[-6:-3]) if len(vols) >= 6 else safe_mean(vols[:-3])
    volume_burst = (safe_mean(vols[-2:]) / avg_vol_10) if avg_vol_10 > 0 else 0.0
    volume_ratio_3 = (safe_mean(vols[-3:]) / avg_vol_3_prev) if avg_vol_3_prev > 0 else 0.0
    volume_last_vs_avg10 = (vols[-1] / avg_vol_10) if avg_vol_10 > 0 else 0.0

    # trend / persistence
    micro_trend_pct = pct_change(safe_mean(closes[-3:]), safe_mean(closes[-8:-5])) if len(closes) >= 8 else ret_5
    green_count_3 = sum(1 for i in range(-3, 0) if closes[i] > opens[i])
    green_count_5 = sum(1 for i in range(-5, 0) if closes[i] > opens[i])
    higher_lows_3 = int(lows[-1] >= lows[-2]) + int(lows[-2] >= lows[-3]) + int(lows[-3] >= lows[-4])
    higher_highs_3 = int(highs[-1] >= highs[-2]) + int(highs[-2] >= highs[-3]) + int(highs[-3] >= highs[-4])
    momentum_persistence = green_count_5 + higher_lows_3 + higher_highs_3  # 0..11

    info = {
        "symbol": symbol,
        "strategy": "v3",
        "ret_1_pct": round(ret_1, 4),
        "ret_3_pct": round(ret_3, 4),
        "ret_5_pct": round(ret_5, 4),
        "last_green": bool(last_green),
        "higher_low": bool(higher_low),
        "breakout_above_prev_high": bool(breakout_above_prev_high),
        "breakout_above_3bar_high": bool(breakout_above_3bar_high),
        "distance_from_5bar_low_pct": round(dist_from_5_low, 4),
        "pullback_from_5bar_high_pct": round(pullback_from_5_high, 4),
        "breakout_distance_pct": round(breakout_distance_pct, 4),
        "bar_range_pct": round(bar_range_pct, 4),
        "body_to_range": round(body_to_range, 4),
        "close_pos_in_bar": round(close_pos_in_bar, 4),
        "median_range_pct_8": round(median_range_pct_8, 4),
        "volume_burst": round(volume_burst, 4),
        "volume_ratio_3": round(volume_ratio_3, 4),
        "volume_last_vs_avg10": round(volume_last_vs_avg10, 4),
        "micro_trend_pct": round(micro_trend_pct, 4),
        "green_count_3": int(green_count_3),
        "green_count_5": int(green_count_5),
        "higher_lows_3": int(higher_lows_3),
        "higher_highs_3": int(higher_highs_3),
        "momentum_persistence": int(momentum_persistence),
        "last_close": round(c0, 8),
        "last_volume": round(vols[-1], 8),
        "ts_utc": now_utc_iso(),
    }

    # hard rejects: tighten quality before scoring
    if ret_3 < MIN_3BAR_RETURN_PCT:
        info["reason"] = "ret_3_too_small"
        return "no_signal", info
    if ret_5 < MIN_5BAR_RETURN_PCT:
        info["reason"] = "ret_5_too_small"
        return "no_signal", info
    if volume_burst < MIN_LAST2_VOLUME_BURST:
        info["reason"] = "low_volume_burst"
        return "no_signal", info
    if volume_ratio_3 < MIN_3BAR_VOLUME_RATIO:
        info["reason"] = "low_volume_ratio_3"
        return "no_signal", info
    if close_pos_in_bar < MIN_CLOSE_POSITION_IN_BAR:
        info["reason"] = "weak_close_in_bar"
        return "no_signal", info
    if body_to_range < 0.35:
        info["reason"] = "small_body_no_commitment"
        return "no_signal", info
    if bar_range_pct < max(V3_MIN_BAR_RANGE_PCT, median_range_pct_8 * 0.70):
        info["reason"] = "bar_range_too_small"
        return "no_signal", info
    if median_range_pct_8 < V3_MIN_MEDIAN_RANGE_PCT_8:
        info["reason"] = "symbol_too_slow"
        return "no_signal", info
    if micro_trend_pct < 0.14:
        info["reason"] = "micro_trend_too_weak"
        return "no_signal", info
    if dist_from_5_low > MAX_DISTANCE_FROM_5BAR_LOW_PCT:
        info["reason"] = "too_extended_from_5bar_low"
        return "no_signal", info
    if dist_from_5_low > V3_MAX_CHASE_IF_HIGH_VOLUME_PCT and volume_burst < 1.60:
        info["reason"] = "too_extended_without_extreme_volume"
        return "no_signal", info
    if pullback_from_5_high < -MAX_PULLBACK_FROM_RECENT_HIGH_PCT:
        info["reason"] = "too_far_below_recent_high"
        return "no_signal", info
    if body_to_range > MAX_LAST_CANDLE_BODY_TO_RANGE_RATIO:
        info["reason"] = "invalid_candle_ratio"
        return "no_signal", info
    if breakout_distance_pct < V3_MIN_BREAKOUT_DISTANCE_PCT:
        info["reason"] = "no_breakout_confirmation"
        return "no_signal", info
    if breakout_distance_pct > V3_MAX_BREAKOUT_DISTANCE_PCT:
        info["reason"] = "breakout_distance_too_large"
        return "no_signal", info
    if green_count_3 < V3_MIN_GREEN_BARS_LAST3:
        info["reason"] = "not_enough_green_bars_last3"
        return "no_signal", info
    if green_count_5 < V3_MIN_GREEN_BARS_LAST5:
        info["reason"] = "not_enough_green_bars_last5"
        return "no_signal", info
    if not breakout_above_prev_high and not breakout_above_3bar_high:
        info["reason"] = "no_recent_breakout"
        return "no_signal", info

    # score only after passing the quality gate
    confidence = 42.0

    # structural points
    if last_green:
        confidence += 8.0
    if higher_low:
        confidence += 6.0
    if breakout_above_prev_high:
        confidence += 12.0
    if breakout_above_3bar_high:
        confidence += 6.0

    # momentum and trend
    confidence += clamp(ret_1 * 9.0, -2.0, 10.0)
    confidence += clamp(ret_3 * 8.5, 0.0, 22.0)
    confidence += clamp(ret_5 * 6.5, 0.0, 20.0)
    confidence += clamp(micro_trend_pct * 6.5, 0.0, 10.0)

    # volume
    confidence += clamp((volume_burst - 1.0) * 13.0, 0.0, 14.0)
    confidence += clamp((volume_ratio_3 - 1.0) * 10.0, 0.0, 10.0)

    # candle quality and persistence
    confidence += clamp((close_pos_in_bar - 0.5) * 20.0, 0.0, 8.0)
    confidence += clamp((body_to_range - 0.35) * 14.0, 0.0, 8.0)
    confidence += clamp((momentum_persistence - 6) * 1.8, -2.0, 9.0)

    # anti chase penalties
    if dist_from_5_low > 2.6:
        confidence -= 6.0
    if dist_from_5_low > 3.2:
        confidence -= 6.0
    if breakout_distance_pct > 0.70:
        confidence -= 4.0
    if breakout_distance_pct > 0.85:
        confidence -= 6.0
    if pullback_from_5_high < -0.40:
        confidence -= 4.0
    if bar_range_pct > max(0.90, median_range_pct_8 * 3.0):
        confidence -= 5.0  # blowoff risk

    confidence = clamp(confidence, 0.0, 100.0)

    # Scanner-aware quality gates (reduces false positives seen in v2.1 such as weak/negative 24h drift names
    # and high-spread microcaps that look good on one candle but are hard to monetize after slippage)
    scanner_change_pct = None
    scanner_spread_pct = None
    if isinstance(scanner_row, dict):
        scanner_change_pct = safe_float(scanner_row.get("change_pct"), 0.0)
        scanner_spread_pct = safe_float(scanner_row.get("spread_pct"), 999.0)
        info["scanner_change_pct"] = round(scanner_change_pct, 4)
        info["scanner_spread_pct"] = round(scanner_spread_pct, 4)

        if scanner_spread_pct > V3_MAX_WOULD_BUY_SPREAD_PCT:
            confidence -= 10.0
            info.setdefault("anti_chase_flags", []).append("high_spread_execution_risk")

        red_coin = scanner_change_pct < V3_MIN_WOULD_BUY_SCANNER_CHANGE_PCT
        if red_coin:
            # allow true reversals only when confidence and volume confirmation are strong
            if confidence < V3_STRONG_REVERSAL_CONFIDENCE_MIN:
                confidence -= 12.0
                info.setdefault("anti_chase_flags", []).append("weak_market_context")
            if volume_ratio_last_vs_avg_8 < V3_MIN_RED_COIN_VOLUME_RATIO:
                confidence -= 10.0
                info.setdefault("anti_chase_flags", []).append("red_coin_no_volume_confirmation")

    confidence = clamp(confidence, 0.0, 100.0)
    info["confidence"] = round(confidence, 2)

    if confidence >= V3_CONFIDENCE_BUY:
        info["quality_tier"] = "A" if confidence >= V3_STRONG_CONFIDENCE_BUY else "B"
        info["exit_plan"] = build_exit_plan(info)
        return "would_buy", info

    if confidence >= V3_CONFIDENCE_WATCH:
        info["quality_tier"] = "watch"
        return "watchlist", info

    info["reason"] = "low_confidence"
    return "no_signal", info


# =========================
# SIGNAL STATE
# =========================
class CooldownMap:
    def __init__(self, seconds: int) -> None:
        self.seconds = int(seconds)
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
async def signal_logger_loop_v3() -> None:
    exchange: Optional[ccxt.kraken] = None
    would_buy_cooldown = CooldownMap(SIGNAL_COOLDOWN_SECONDS)
    watch_cooldown = CooldownMap(WATCH_COOLDOWN_SECONDS)

    try:
        exchange = build_exchange()
        logger.info("Starting Kraken signal logger v3 (read only)")

        # Public time check is safe
        try:
            server_time = await exchange.fetch_time()
            logger.info(f"Kraken server time: {server_time}")
        except Exception as e:
            logger.warning(f"fetch_time failed: {e}")

        # Optional private balance check only if you want to verify credentials
        if PERFORM_PRIVATE_BALANCE_CHECK:
            try:
                balance = await exchange.fetch_balance()
                total_keys = len(balance.get("total", {})) if isinstance(balance, dict) else 0
                logger.info(f"Balance fetched successfully (asset slots: {total_keys})")
            except Exception as e:
                logger.warning(f"Balance check skipped/failed: {e}")
        else:
            logger.info("Private balance check disabled (public scanner mode)")

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
                    status, details = evaluate_signal_v3(symbol, candles, row)

                    now_ts = time.time()

                    if status == "would_buy":
                        key = f"would_buy:{symbol}"
                        if would_buy_cooldown.can_emit(key, now_ts):
                            would_buy_cooldown.mark(key, now_ts)
                            would_buy_hits += 1

                            ep = details.get("exit_plan", {}) if isinstance(details, dict) else {}
                            logger.info(
                                "WOULD BUY v3 | "
                                f"{symbol} | "
                                f"conf={details.get('confidence')} | "
                                f"v1q={details.get('v1_quality_score')} | "
                                f"ret1={details.get('ret_1_pct')}% | "
                                f"ret3={details.get('ret_3_pct')}% | "
                                f"ret5={details.get('ret_5_pct')}% | "
                                f"spr={row['spread_pct']:.3f}% | "
                                f"qv={row['quote_vol']:.0f} | "
                                f"stop={ep.get('hard_stop_pct')}% | "
                                f"trail={ep.get('trailing_stop_pct')}% | "
                                f"time={ep.get('time_stop_minutes')}m"
                            )

                            append_jsonl(
                                SIGNAL_LOG_JSONL,
                                {
                                    "ts_utc": now_utc_iso(),
                                    "event": "would_buy_v3",
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
                            logger.info(f"Cooldown active | WOULD BUY v3 | {symbol}")

                    elif status == "watchlist":
                        key = f"watch:{symbol}"
                        if watch_cooldown.can_emit(key, now_ts):
                            watch_cooldown.mark(key, now_ts)
                            watch_hits += 1

                            logger.info(
                                "WATCHLIST v3 | "
                                f"{symbol} | "
                                f"conf={details.get('confidence')} | "
                                f"v1q={details.get('v1_quality_score')} | "
                                f"ret1={details.get('ret_1_pct')}% | "
                                f"ret3={details.get('ret_3_pct')}% | "
                                f"ret5={details.get('ret_5_pct')}%"
                            )

                            append_jsonl(
                                SIGNAL_LOG_JSONL,
                                {
                                    "ts_utc": now_utc_iso(),
                                    "event": "watch_v3",
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
                            logger.info(f"Cooldown active | WATCHLIST v3 | {symbol}")

                    else:
                        logger.info(f"NO SIGNAL v3 | {symbol:<12} | {details.get('reason')}")

                    await asyncio.sleep(0.03)

                elapsed = time.time() - cycle_start
                logger.info(
                    f"Cycle done v3 | scanned={len(ranked)} | candle_checked={checked} | "
                    f"watch_hits={watch_hits} | would_buy_hits={would_buy_hits} | elapsed={elapsed:.2f}s"
                )

            except Exception as cycle_error:
                logger.exception(f"Signal logger v3 cycle error: {cycle_error}")

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    except asyncio.CancelledError:
        logger.info("Signal logger v3 cancelled")
        raise
    except KeyboardInterrupt:
        logger.info("Signal logger v3 stopped by user")
    except Exception as e:
        logger.exception(f"Fatal signal logger v3 error: {e}")
    finally:
        if exchange is not None:
            try:
                await exchange.close()
            except Exception:
                pass
        logger.info("Signal logger v3 shutdown complete")


def main() -> None:
    try:
        asyncio.run(signal_logger_loop_v3())
    except KeyboardInterrupt:
        logger.info("Exited")


if __name__ == "__main__":
    main()
