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
TOP_N_CANDLE_CHECK = 12

QUOTE_CURRENCIES = {"USD", "USDT"}
PREFER_QUOTE = "USD"

MIN_QUOTE_VOLUME_24H = 250000.0
MAX_SPREAD_PCT = 0.80

LOG_FILE = "kraken_signal_logger_v2_1.log"
SIGNAL_LOG_JSONL = "kraken_signal_events_v2_1.jsonl"

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
OHLCV_LIMIT = 40

MIN_CANDLES_REQUIRED = 20
MIN_LAST_CLOSE = 0.00000001

# Hard fails (keep broad like v2, but not reckless)
MIN_3BAR_RETURN_PCT = 0.05
MIN_5BAR_RETURN_PCT = 0.10
MAX_DISTANCE_FROM_5BAR_LOW_PCT = 6.0
MAX_LAST_CANDLE_BODY_TO_RANGE_RATIO = 1.0
MAX_PULLBACK_FROM_RECENT_HIGH_PCT = 4.5

# Confidence thresholds
WOULD_BUY_CONFIDENCE_MIN = 66.0
LOG_WATCHLIST_CONFIDENCE_MIN = 46.0

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
logger = logging.getLogger("kraken_signal_logger_v2_1")
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
    """
    Dynamic exit plan metadata for backtesting.
    This does not execute trades. It gives the backtester enough info to test exits that fit the signal.
    """
    confidence = safe_float(details.get("confidence"), 0.0)
    v1_quality = safe_float(details.get("v1_quality_score"), 0.0)
    ret1 = safe_float(details.get("ret_1_pct"), 0.0)
    ret3 = safe_float(details.get("ret_3_pct"), 0.0)
    ret5 = safe_float(details.get("ret_5_pct"), 0.0)
    med_range = safe_float(details.get("median_range_pct_8"), 0.0)
    close_pos = safe_float(details.get("close_pos_in_bar"), 0.0)
    vol_ratio = safe_float(details.get("volume_ratio_last_vs_avg_8"), 0.0)
    body_to_range = safe_float(details.get("body_to_range"), 0.0)
    dist_from_5_low = safe_float(details.get("dist_from_5_low_pct"), 0.0)
    pullback_from_5_high = safe_float(details.get("pullback_from_5_high_pct"), 0.0)

    strength_score = 0.0
    strength_score += (confidence - 50.0) * 0.6
    strength_score += v1_quality * 0.5
    strength_score += clamp(ret3, 0.0, 3.0) * 5.0
    strength_score += clamp(ret5, 0.0, 5.0) * 3.0
    strength_score += clamp((close_pos - 0.5) * 20.0, -5.0, 5.0)
    strength_score += clamp((vol_ratio - 1.0) * 4.0, -4.0, 6.0)

    # anti chase adjustment inside exit plan
    if ret1 > 1.20 and body_to_range > 0.75:
        strength_score -= 6.0
    if dist_from_5_low > 4.8:
        strength_score -= 5.0
    if pullback_from_5_high < -2.0:
        strength_score -= 4.0

    # volatility based stop and trail
    # med_range is 1m median range in pct
    if med_range <= 0:
        hard_stop = DEFAULT_HARD_STOP_PCT
        trail_pct = 0.80
    else:
        hard_stop = clamp(max(med_range * 1.35, 0.75), MIN_HARD_STOP_PCT, MAX_HARD_STOP_PCT)
        trail_pct = clamp(max(med_range * 0.95, 0.45), MIN_TRAIL_PCT, MAX_TRAIL_PCT)

    # stronger signals get a little more room and longer time to work
    if strength_score >= 25:
        hard_stop = clamp(hard_stop * 1.10, MIN_HARD_STOP_PCT, MAX_HARD_STOP_PCT)
        trail_pct = clamp(trail_pct * 1.05, MIN_TRAIL_PCT, MAX_TRAIL_PCT)
        time_stop_min = 15
        arm_trailing_after_gain_pct = 0.85
        move_to_be_after_gain_pct = 0.65
        target1_pct = 1.25
        target2_pct = 2.75
    elif strength_score >= 12:
        time_stop_min = 12
        arm_trailing_after_gain_pct = 0.70
        move_to_be_after_gain_pct = 0.50
        target1_pct = 1.00
        target2_pct = 2.25
    else:
        # weaker but still valid momentum entries get tighter management
        hard_stop = clamp(hard_stop * 0.90, MIN_HARD_STOP_PCT, MAX_HARD_STOP_PCT)
        trail_pct = clamp(trail_pct * 0.90, MIN_TRAIL_PCT, MAX_TRAIL_PCT)
        time_stop_min = 8
        arm_trailing_after_gain_pct = 0.55
        move_to_be_after_gain_pct = 0.35
        target1_pct = 0.80
        target2_pct = 1.75

    # momentum failure rules are purposely simple and testable in a bar by bar backtester
    momentum_fail = {
        "enabled": True,
        "check_after_minutes": 2,
        "min_unrealized_gain_pct_to_avoid_fail": 0.15 if strength_score >= 12 else 0.10,
        "max_drawdown_from_peak_pct_early": 0.60 if strength_score >= 12 else 0.45,
        "two_red_closes_exit_after_minutes": 3,
    }

    # optional partials metadata for future trading engine
    partials = {
        "enabled": True,
        "take_25pct_at_target1": True,
        "target1_pct": round(target1_pct, 4),
        "take_50pct_at_target2": False,  # log only for now, can test later
        "target2_pct": round(target2_pct, 4),
    }

    plan = {
        "version": "v2.1_exit_plan",
        "strength_score": round(strength_score, 4),
        "style": "momentum_runner_dynamic",
        "hard_stop_pct": round(hard_stop, 4),
        "move_to_break_even_after_gain_pct": round(move_to_be_after_gain_pct, 4),
        "arm_trailing_after_gain_pct": round(arm_trailing_after_gain_pct, 4),
        "trailing_stop_pct": round(trail_pct, 4),
        "time_stop_minutes": int(time_stop_min),
        "max_hold_minutes": int(max(time_stop_min + 2, 10)),
        "partials": partials,
        "momentum_fail": momentum_fail,
        "inputs": {
            "confidence": round(confidence, 4),
            "v1_quality_score": round(v1_quality, 4),
            "ret_1_pct": round(ret1, 4),
            "ret_3_pct": round(ret3, 4),
            "ret_5_pct": round(ret5, 4),
            "median_range_pct_8": round(med_range, 4),
            "close_pos_in_bar": round(close_pos, 4),
            "volume_ratio_last_vs_avg_8": round(vol_ratio, 4),
            "body_to_range": round(body_to_range, 4),
        },
    }

    return plan


def evaluate_signal_v2_1(symbol: str, candles: List[Dict[str, float]]) -> Tuple[str, Dict[str, Any]]:
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
    last8 = candles[-8:]

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

    # Extra features
    close_pos_in_bar = _close_position_in_bar(last)
    prev_close_pos = _close_position_in_bar(prev)
    last2_green = (last["close"] > last["open"]) and (prev["close"] > prev["open"])
    breakout_2bar = last_close > max(prev["high"], prev2["high"])
    vol_stats = _recent_range_stats(candles, lookback=8)

    # Simple v1 style quality overlay (soft score, not hard fail)
    v1_quality = 0.0
    if last_green:
        v1_quality += 1.0
    if higher_low:
        v1_quality += 1.0
    if breakout_above_prev_high:
        v1_quality += 1.0
    if breakout_2bar:
        v1_quality += 0.75
    if body_to_range >= 0.20:
        v1_quality += 0.5
    if close_pos_in_bar >= 0.60:
        v1_quality += 0.75
    if ret_3 >= 0.20:
        v1_quality += 0.5
    if ret_5 >= 0.30:
        v1_quality += 0.5

    info.update(
        {
            "last_close": last_close,
            "ret_1_pct": round(ret_1, 4),
            "ret_3_pct": round(ret_3, 4),
            "ret_5_pct": round(ret_5, 4),
            "body_to_range": round(body_to_range, 4),
            "higher_low": higher_low,
            "breakout_above_prev_high": breakout_above_prev_high,
            "breakout_2bar": breakout_2bar,
            "last_green": last_green,
            "last2_green": last2_green,
            "dist_from_5_low_pct": round(dist_from_5_low, 4),
            "pullback_from_5_high_pct": round(pullback_from_5_high, 4),
            "close_pos_in_bar": round(close_pos_in_bar, 4),
            "prev_close_pos_in_bar": round(prev_close_pos, 4),
            "v1_quality_score": round(v1_quality, 4),
            "median_range_pct_8": vol_stats["median_range_pct"],
            "avg_range_pct_8": vol_stats["avg_range_pct"],
            "median_body_pct_8": vol_stats["median_body_pct"],
            "volume_ratio_last_vs_avg_8": vol_stats["volume_ratio_last_vs_avg"],
        }
    )

    # Hard fails stay broad enough to preserve v2 behavior
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

    # Confidence score (keep v2 spirit, add v1 quality and anti chase logic)
    confidence = 50.0

    # v2 structure bonuses
    if last_green:
        confidence += 10.0
    if higher_low:
        confidence += 10.0
    if breakout_above_prev_high:
        confidence += 15.0

    # momentum bonuses
    confidence += clamp(ret_1 * 10.0, -5.0, 12.0)
    confidence += clamp(ret_3 * 8.0, 0.0, 20.0)
    confidence += clamp(ret_5 * 6.0, 0.0, 20.0)

    # extension and pullback handling
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

    # candle quality
    if body_to_range >= 0.35:
        confidence += 5.0
    elif body_to_range < 0.10:
        confidence -= 4.0

    # v1 quality overlay
    confidence += clamp(v1_quality * 2.5, 0.0, 12.0)

    # volume confirmation
    vol_ratio = vol_stats["volume_ratio_last_vs_avg"]
    if vol_ratio >= 1.8:
        confidence += 5.0
    elif vol_ratio >= 1.2:
        confidence += 2.5
    elif vol_ratio < 0.7:
        confidence -= 3.5

    # close location in bar
    if close_pos_in_bar >= 0.75:
        confidence += 4.0
    elif close_pos_in_bar < 0.40:
        confidence -= 4.5

    # anti chase penalties (important)
    anti_chase_flags: List[str] = []

    # huge single bar impulse with no pause often mean reversion traps
    if ret_1 > 1.4 and body_to_range > 0.75 and close_pos_in_bar > 0.80:
        confidence -= 9.0
        anti_chase_flags.append("blowoff_last_bar")

    if ret_1 > 2.2:
        confidence -= 10.0
        anti_chase_flags.append("ret1_overheat")

    # too far from recent base
    if dist_from_5_low > 5.0:
        confidence -= 6.0
        anti_chase_flags.append("too_extended")

    # weak close while claiming momentum
    if (ret_3 > 0.6 or ret_5 > 1.0) and close_pos_in_bar < 0.45:
        confidence -= 7.0
        anti_chase_flags.append("weak_close_for_momentum")

    # small extra bonus for controlled continuation
    if ret_1 > 0 and ret_1 < 0.9 and close_pos_in_bar >= 0.65 and body_to_range >= 0.20:
        confidence += 3.0

    info["anti_chase_flags"] = anti_chase_flags
    info["anti_chase_count"] = len(anti_chase_flags)

    confidence = round(clamp(confidence, 0.0, 100.0), 2)
    info["confidence"] = confidence

    # attach exit plan metadata for backtesting
    info["exit_plan"] = build_exit_plan(info)

    if confidence >= WOULD_BUY_CONFIDENCE_MIN:
        info["reason"] = "would_buy_signal_v2_1"
        return "would_buy", info

    if confidence >= LOG_WATCHLIST_CONFIDENCE_MIN:
        info["reason"] = "watchlist_signal_v2_1"
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
async def signal_logger_loop_v2_1() -> None:
    exchange: Optional[ccxt.kraken] = None
    would_buy_cooldown = CooldownMap(SIGNAL_COOLDOWN_SECONDS)
    watch_cooldown = CooldownMap(WATCH_COOLDOWN_SECONDS)

    try:
        exchange = build_exchange()
        logger.info("Starting Kraken signal logger v2.1 (read only)")

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
                    status, details = evaluate_signal_v2_1(symbol, candles)

                    now_ts = time.time()

                    if status == "would_buy":
                        key = f"would_buy:{symbol}"
                        if would_buy_cooldown.can_emit(key, now_ts):
                            would_buy_cooldown.mark(key, now_ts)
                            would_buy_hits += 1

                            ep = details.get("exit_plan", {}) if isinstance(details, dict) else {}
                            logger.info(
                                "WOULD BUY v2.1 | "
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
                                    "event": "would_buy_v2_1",
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
                            logger.info(f"Cooldown active | WOULD BUY v2.1 | {symbol}")

                    elif status == "watchlist":
                        key = f"watch:{symbol}"
                        if watch_cooldown.can_emit(key, now_ts):
                            watch_cooldown.mark(key, now_ts)
                            watch_hits += 1

                            logger.info(
                                "WATCHLIST v2.1 | "
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
                                    "event": "watchlist_v2_1",
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
                            logger.info(f"Cooldown active | WATCHLIST v2.1 | {symbol}")

                    else:
                        logger.info(f"NO SIGNAL v2.1 | {symbol:<12} | {details.get('reason')}")

                    await asyncio.sleep(0.03)

                elapsed = time.time() - cycle_start
                logger.info(
                    f"Cycle done v2.1 | scanned={len(ranked)} | candle_checked={checked} | "
                    f"watch_hits={watch_hits} | would_buy_hits={would_buy_hits} | elapsed={elapsed:.2f}s"
                )

            except Exception as cycle_error:
                logger.exception(f"Signal logger v2.1 cycle error: {cycle_error}")

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    except asyncio.CancelledError:
        logger.info("Signal logger v2.1 cancelled")
        raise
    except KeyboardInterrupt:
        logger.info("Signal logger v2.1 stopped by user")
    except Exception as e:
        logger.exception(f"Fatal signal logger v2.1 error: {e}")
    finally:
        if exchange is not None:
            try:
                await exchange.close()
            except Exception:
                pass
        logger.info("Signal logger v2.1 shutdown complete")


def main() -> None:
    try:
        asyncio.run(signal_logger_loop_v2_1())
    except KeyboardInterrupt:
        logger.info("Exited")


if __name__ == "__main__":
    main()
