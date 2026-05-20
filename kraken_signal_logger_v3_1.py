#!/usr/bin/env python3
import os
import sys
import json
import math
import time
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import ccxt.async_support as ccxt

load_dotenv()

# =========================
# CONFIG v3.1 (fresh rebuild)
# =========================
VERSION = "v3.1"
EVENT_WOULD_BUY = "would_buy_v3_1"
EVENT_WATCHLIST = "watchlist_v3_1"

LOG_FILE = "kraken_signal_logger_v3_1.log"
SIGNAL_LOG_JSONL = "kraken_signal_events_v3_1.jsonl"

SCAN_INTERVAL_SECONDS = 30
TOP_N_SCANNER_LOG = 20
TOP_N_CANDLE_CHECK = 14

OHLCV_TIMEFRAME = "1m"
OHLCV_LIMIT = 60
MIN_CANDLES_REQUIRED = 30

QUOTE_CURRENCIES = {"USD", "USDT"}
PREFER_QUOTE = "USD"

MIN_QUOTE_VOLUME_24H = 250_000.0
MAX_SPREAD_PCT = 0.90

SIGNAL_COOLDOWN_SECONDS = 240
WATCH_COOLDOWN_SECONDS = 150

PERFORM_PRIVATE_BALANCE_CHECK = False  # keep scanner public-only

# Stage 1 scoring
RED_COIN_HARD_PENALTY = 28.0
LOW_POSITIVE_PENALTY = 5.0
MIN_POSITIVE_CHANGE_PCT = 0.05
VOLUME_CAP_M = 20.0
PREFER_USD_BONUS = 0.5
NEGATIVE_CHANGE_SOFT_BLOCK = -1.5  # scanner can still rank negatives, but signal engine will gate them by regime

# Regime guardrails (long-only scanner)
REGIME_TOPN = 12
REGIME_MIN_POSITIVE_COUNT = 3
REGIME_MIN_AVG_CHANGE_PCT = -0.25
REGIME_BAD_SUPPRESSION = True
REGIME_BAD_WOULD_BUY_CONF_BONUS_REQUIRED = 8.0  # stricter in red tape

# Stage 2 thresholds
WOULD_BUY_CONFIDENCE_MIN = 70.0
WATCHLIST_CONFIDENCE_MIN = 52.0

# Hard fails / anti-chase
MIN_3BAR_RETURN_PCT = 0.12
MIN_5BAR_RETURN_PCT = 0.20
MAX_DISTANCE_FROM_8BAR_LOW_PCT = 5.2
MAX_PULLBACK_FROM_8BAR_HIGH_PCT = 3.8
MAX_SINGLE_BAR_PUMP_PCT = 1.85
MIN_LAST_CLOSE = 1e-8

# Optional concurrency for OHLCV on top candidates
OHLCV_CONCURRENCY = 4

EXCLUDED_BASES = {
    "USD", "USDT", "USDC", "USDG", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY"
}
EXCLUDED_SYMBOLS = {
    "USDT/USD", "USDC/USD", "USDG/USD",
    "EUR/USD", "GBP/USD", "AUD/USD", "CAD/USD", "CHF/USD", "JPY/USD",
    "USDC/USDT", "USDT/USDC",
}
EXCLUDED_BASE_SUBSTRINGS = {"USD", "EUR", "GBP", "AUD", "CAD", "CHF", "JPY"}
BASE_SUBSTRING_EXCEPTIONS = set()


# =========================
# LOGGING
# =========================
logger = logging.getLogger("kraken_signal_logger_v3_1")
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


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def pct_move(a: float, b: float) -> float:
    if a <= 0:
        return 0.0
    return ((b - a) / a) * 100.0


def append_jsonl(filepath: str, row: Dict[str, Any]) -> None:
    with open(filepath, "a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")


def median(values: List[float]) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    n = len(s)
    m = n // 2
    if n % 2 == 1:
        return s[m]
    return (s[m - 1] + s[m]) / 2.0


def pct_change_from_ticker(ticker: Dict[str, Any]) -> float:
    # Kraken sometimes provides percentage, sometimes only open/last
    percentage = ticker.get("percentage")
    if percentage is not None:
        return safe_float(percentage, 0.0)
    last_price = safe_float(ticker.get("last"))
    open_price = safe_float(ticker.get("open"))
    return pct_move(open_price, last_price)


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
    base_vol = safe_float(ticker.get("baseVolume"), 0.0)
    last = safe_float(ticker.get("last"), 0.0)
    if base_vol > 0 and last > 0:
        return base_vol * last
    return 0.0


def extract_quote_from_symbol(symbol: str) -> str:
    return symbol.split("/")[-1].upper().strip() if "/" in symbol else ""


def extract_base_from_symbol(symbol: str) -> str:
    return symbol.split("/")[0].upper().strip() if "/" in symbol else symbol.upper().strip()


def is_spot_market(market: Dict[str, Any]) -> bool:
    return bool(market.get("spot") is True or market.get("type") == "spot")


def is_market_active(market: Dict[str, Any]) -> bool:
    active = market.get("active")
    return True if active is None else bool(active)


def base_looks_like_stable_or_fiat(base: str) -> bool:
    b = base.upper()
    if b in EXCLUDED_BASES:
        return True
    for token in EXCLUDED_BASE_SUBSTRINGS:
        if token in b and b not in BASE_SUBSTRING_EXCEPTIONS:
            return True
    return False


def symbol_is_allowed(market: Dict[str, Any]) -> bool:
    symbol = str(market.get("symbol", "")).upper()
    if not symbol or symbol in EXCLUDED_SYMBOLS:
        return False
    if not is_spot_market(market) or not is_market_active(market):
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


# =========================
# STAGE 1 SCANNER
# =========================
def score_candidate(symbol: str, change_pct: float, quote_vol: float, spread_pct: float) -> float:
    score = 0.0

    if change_pct < 0:
        score -= RED_COIN_HARD_PENALTY
        # small recovery for shallow red names only (useful for reversals, but still mostly penalized)
        if change_pct > NEGATIVE_CHANGE_SOFT_BLOCK:
            score += (change_pct + abs(NEGATIVE_CHANGE_SOFT_BLOCK)) * 2.0
    elif change_pct < MIN_POSITIVE_CHANGE_PCT:
        score -= LOW_POSITIVE_PENALTY
    else:
        score += min(change_pct, 15.0) * 2.1

    vol_m = quote_vol / 1_000_000.0
    score += min(vol_m, VOLUME_CAP_M)

    score -= min(max(spread_pct, 0.0), 5.0) * 4.25
    score += quote_pref_bonus(symbol)
    return round(score, 4)


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
                    "spread_pct": round(spread_pct, 6),
                    "change_pct": round(change_pct, 6),
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
            f"#{i:02d} {row['symbol']:<14} "
            f"score={row['score']:.2f} "
            f"chg={row['change_pct']:.3f}% "
            f"spr={row['spread_pct']:.3f}% "
            f"qv={row['quote_vol']:.0f} "
            f"last={row['last']}"
        )


def classify_market_regime(ranked: List[Dict[str, Any]]) -> Dict[str, Any]:
    top = ranked[:REGIME_TOPN]
    if not top:
        return {
            "label": "unknown",
            "top_n": 0,
            "positive_count": 0,
            "avg_change_pct": 0.0,
            "median_change_pct": 0.0,
            "top1_change_pct": 0.0,
            "is_long_friendly": False,
        }
    changes = [safe_float(r.get("change_pct")) for r in top]
    pos_count = sum(1 for x in changes if x > 0)
    avg_chg = sum(changes) / len(changes)
    med_chg = median(changes)

    long_friendly = (pos_count >= REGIME_MIN_POSITIVE_COUNT) and (avg_chg >= REGIME_MIN_AVG_CHANGE_PCT)
    if long_friendly and avg_chg >= 0.30 and pos_count >= max(4, REGIME_MIN_POSITIVE_COUNT):
        label = "strong_long"
    elif long_friendly:
        label = "mixed_long_ok"
    elif pos_count <= 1 and avg_chg < -1.0:
        label = "risk_off_red"
    else:
        label = "weak_mixed"

    return {
        "label": label,
        "top_n": len(top),
        "positive_count": pos_count,
        "avg_change_pct": round(avg_chg, 4),
        "median_change_pct": round(med_chg, 4),
        "top1_change_pct": round(changes[0], 4) if changes else 0.0,
        "is_long_friendly": bool(long_friendly),
    }


# =========================
# CANDLES + FEATURES
# =========================
def parse_ohlcv_rows(rows: List[Any]) -> List[Dict[str, float]]:
    candles: List[Dict[str, float]] = []
    for row in rows:
        if not isinstance(row, (list, tuple)) or len(row) < 6:
            continue
        ts, o, h, l, c, v = row[:6]
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


def close_pos_in_bar(c: Dict[str, float]) -> float:
    rng = max(c["high"] - c["low"], 1e-12)
    return (c["close"] - c["low"]) / rng


def range_stats(candles: List[Dict[str, float]], lookback: int = 8) -> Dict[str, float]:
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


def trend_features(candles: List[Dict[str, float]]) -> Dict[str, Any]:
    last = candles[-1]
    prev = candles[-2]
    prev2 = candles[-3]
    prev3 = candles[-4]
    last5 = candles[-5:]
    last8 = candles[-8:]
    closes = [c["close"] for c in candles]

    last_close = last["close"]
    last_open = last["open"]
    last_high = last["high"]
    last_low = last["low"]

    last_range = max(last_high - last_low, 1e-12)
    last_body = abs(last_close - last_open)
    body_to_range = last_body / last_range

    ret_1 = pct_move(prev["close"], last_close)
    ret_2 = pct_move(prev2["close"], last_close)
    ret_3 = pct_move(prev3["close"], last_close)
    ret_5 = pct_move(last5[0]["close"], last_close)

    low_8 = min(c["low"] for c in last8)
    high_8 = max(c["high"] for c in last8)
    dist_from_8_low = pct_move(low_8, last_close)
    pullback_from_8_high = pct_move(high_8, last_close)

    cpos = close_pos_in_bar(last)
    prev_cpos = close_pos_in_bar(prev)
    prev_body_to_range = abs(prev["close"] - prev["open"]) / max(prev["high"] - prev["low"], 1e-12)

    last_green = last_close > last_open
    prev_green = prev["close"] > prev["open"]
    last2_green = last_green and prev_green
    higher_low = last_low > prev["low"]
    higher_high = last_high > prev["high"]
    breakout_prev_high = last_close > prev["high"]
    breakout_2bar = last_close > max(prev["high"], prev2["high"])

    # micro trend slope proxy from last few closes
    ema_like_fast = sum(closes[-3:]) / 3.0
    ema_like_slow = sum(closes[-8:]) / 8.0 if len(closes) >= 8 else sum(closes) / max(len(closes), 1)
    trend_bias = pct_move(ema_like_slow, ema_like_fast)

    return {
        "last_close": round(last_close, 10),
        "ret_1_pct": round(ret_1, 4),
        "ret_2_pct": round(ret_2, 4),
        "ret_3_pct": round(ret_3, 4),
        "ret_5_pct": round(ret_5, 4),
        "body_to_range": round(body_to_range, 4),
        "prev_body_to_range": round(prev_body_to_range, 4),
        "last_green": last_green,
        "prev_green": prev_green,
        "last2_green": last2_green,
        "higher_low": higher_low,
        "higher_high": higher_high,
        "breakout_above_prev_high": breakout_prev_high,
        "breakout_2bar": breakout_2bar,
        "close_pos_in_bar": round(cpos, 4),
        "prev_close_pos_in_bar": round(prev_cpos, 4),
        "dist_from_8_low_pct": round(dist_from_8_low, 4),
        "pullback_from_8_high_pct": round(pullback_from_8_high, 4),
        "trend_bias_pct": round(trend_bias, 4),
    }


# =========================
# EXIT PLAN (logged only)
# =========================
def build_exit_plan_v3_1(signal: Dict[str, Any], setup_type: str) -> Dict[str, Any]:
    confidence = safe_float(signal.get("confidence"))
    ret1 = safe_float(signal.get("ret_1_pct"))
    ret3 = safe_float(signal.get("ret_3_pct"))
    ret5 = safe_float(signal.get("ret_5_pct"))
    med_range = safe_float(signal.get("median_range_pct_8"))
    vol_ratio = safe_float(signal.get("volume_ratio_last_vs_avg_8"))
    close_pos = safe_float(signal.get("close_pos_in_bar"))
    anti_chase_count = int(safe_float(signal.get("anti_chase_count"), 0))
    scanner_score = safe_float(signal.get("scanner_score"), 0.0)
    regime_label = str(signal.get("regime_label", "unknown"))

    strength = 0.0
    strength += (confidence - 50.0) * 0.55
    strength += clamp(ret3, 0.0, 3.0) * 4.0
    strength += clamp(ret5, 0.0, 5.0) * 2.5
    strength += clamp((vol_ratio - 1.0) * 4.0, -3.0, 8.0)
    strength += clamp((close_pos - 0.5) * 12.0, -4.0, 4.0)
    strength += clamp(scanner_score / 4.0, -6.0, 8.0)
    strength -= anti_chase_count * 3.0

    if regime_label in {"risk_off_red", "weak_mixed"}:
        strength -= 4.0
    if setup_type == "reversal":
        strength -= 2.0  # require tighter management for reversal pops

    if med_range <= 0:
        hard_stop = 1.0
        trail = 0.70
    else:
        hard_stop = clamp(max(med_range * 1.25, 0.65), 0.55, 2.20)
        trail = clamp(max(med_range * 0.90, 0.40), 0.30, 1.40)

    if strength >= 22:
        hard_stop = clamp(hard_stop * 1.05, 0.55, 2.20)
        trail = clamp(trail * 1.05, 0.30, 1.40)
        time_stop = 14
        arm_trail = 0.85
        breakeven = 0.60
        target1 = 1.10
        target2 = 2.40
    elif strength >= 12:
        time_stop = 11
        arm_trail = 0.70
        breakeven = 0.45
        target1 = 0.90
        target2 = 2.00
    else:
        hard_stop = clamp(hard_stop * 0.90, 0.55, 2.20)
        trail = clamp(trail * 0.90, 0.30, 1.40)
        time_stop = 8
        arm_trail = 0.55
        breakeven = 0.30
        target1 = 0.70
        target2 = 1.50

    momentum_fail = {
        "enabled": True,
        "check_after_minutes": 2,
        "min_unrealized_gain_pct_to_avoid_fail": 0.18 if setup_type == "continuation" else 0.12,
        "max_drawdown_from_peak_pct_early": 0.55 if strength >= 12 else 0.40,
        "two_red_closes_exit_after_minutes": 3,
    }

    return {
        "version": "v3_1_exit_plan",
        "style": "long_only_momentum_regime_aware",
        "setup_type": setup_type,
        "strength_score": round(strength, 4),
        "hard_stop_pct": round(hard_stop, 4),
        "move_to_break_even_after_gain_pct": round(breakeven, 4),
        "arm_trailing_after_gain_pct": round(arm_trail, 4),
        "trailing_stop_pct": round(trail, 4),
        "time_stop_minutes": int(time_stop),
        "max_hold_minutes": int(max(10, time_stop + 2)),
        "partials": {
            "enabled": True,
            "take_25pct_at_target1": True,
            "target1_pct": round(target1, 4),
            "take_50pct_at_target2": False,
            "target2_pct": round(target2, 4),
        },
        "momentum_fail": momentum_fail,
        "inputs": {
            "confidence": round(confidence, 4),
            "ret_1_pct": round(ret1, 4),
            "ret_3_pct": round(ret3, 4),
            "ret_5_pct": round(ret5, 4),
            "median_range_pct_8": round(med_range, 4),
            "volume_ratio_last_vs_avg_8": round(vol_ratio, 4),
            "close_pos_in_bar": round(close_pos, 4),
            "scanner_score": round(scanner_score, 4),
            "anti_chase_count": anti_chase_count,
            "regime_label": regime_label,
        },
    }


# =========================
# STAGE 2 SIGNAL ENGINE (fresh logic)
# =========================
def evaluate_signal_v3_1(
    symbol: str,
    candles: List[Dict[str, float]],
    scanner_row: Dict[str, Any],
    regime: Dict[str, Any],
) -> Tuple[str, Dict[str, Any]]:
    info: Dict[str, Any] = {"symbol": symbol, "reason": ""}

    if len(candles) < MIN_CANDLES_REQUIRED:
        info["reason"] = f"not_enough_candles:{len(candles)}"
        return "no_signal", info

    # sanitize candle structure to avoid v3 style list/dict crashes
    for c in candles[-5:]:
        if not isinstance(c, dict) or "close" not in c or "open" not in c or "high" not in c or "low" not in c:
            info["reason"] = "invalid_candle_shape"
            return "no_signal", info

    feat = trend_features(candles)
    stats8 = range_stats(candles, lookback=8)

    last_close = safe_float(feat["last_close"])
    if last_close < MIN_LAST_CLOSE:
        info["reason"] = "last_close_too_small"
        return "no_signal", info

    ret1 = safe_float(feat["ret_1_pct"])
    ret3 = safe_float(feat["ret_3_pct"])
    ret5 = safe_float(feat["ret_5_pct"])
    dist_from_8_low = safe_float(feat["dist_from_8_low_pct"])
    pullback_from_8_high = safe_float(feat["pullback_from_8_high_pct"])
    body_to_range = safe_float(feat["body_to_range"])
    close_pos = safe_float(feat["close_pos_in_bar"])
    trend_bias = safe_float(feat["trend_bias_pct"])
    vol_ratio = safe_float(stats8["volume_ratio_last_vs_avg"])

    # Hard fails: reduce false positives and avoid buying pops in weak conditions
    if ret3 < MIN_3BAR_RETURN_PCT:
        info["reason"] = "ret_3_too_small"
        return "no_signal", info
    if ret5 < MIN_5BAR_RETURN_PCT:
        info["reason"] = "ret_5_too_small"
        return "no_signal", info
    if dist_from_8_low > MAX_DISTANCE_FROM_8BAR_LOW_PCT:
        info["reason"] = "too_extended_from_8bar_low"
        return "no_signal", info
    if pullback_from_8_high < -MAX_PULLBACK_FROM_8BAR_HIGH_PCT:
        info["reason"] = "too_far_below_recent_high"
        return "no_signal", info
    if ret1 > MAX_SINGLE_BAR_PUMP_PCT and body_to_range > 0.80 and close_pos > 0.80:
        info["reason"] = "single_bar_overheat"
        return "no_signal", info

    # Determine setup type (explicitly avoid hidden conflicts from v2.1)
    continuation = bool(feat["breakout_above_prev_high"] and feat["higher_low"] and feat["last_green"])
    reversal = bool(
        (not feat["breakout_above_prev_high"])
        and feat["last_green"]
        and feat["prev_green"]
        and trend_bias > 0
        and vol_ratio >= 1.05
        and pullback_from_8_high > -1.4
    )

    if not continuation and not reversal:
        info["reason"] = "no_valid_setup_type"
        return "no_signal", info

    setup_type = "continuation" if continuation else "reversal"

    scanner_score = safe_float(scanner_row.get("score"), 0.0)
    scanner_change = safe_float(scanner_row.get("change_pct"), 0.0)
    scanner_spread = safe_float(scanner_row.get("spread_pct"), 999.0)
    scanner_qv = safe_float(scanner_row.get("quote_vol"), 0.0)

    # Confidence model, now regime aware and aligned with scanner
    confidence = 42.0

    # scanner alignment
    if scanner_score > 8:
        confidence += 9.0
    elif scanner_score > 3:
        confidence += 4.0
    elif scanner_score < 0:
        confidence -= 5.0

    if scanner_change > 0:
        confidence += clamp(scanner_change * 1.2, 0.0, 8.0)
    else:
        # allow slight negative only for reversal, but penalize heavily for continuation
        confidence += (-2.0 if setup_type == "reversal" else -8.0)

    # candle structure
    if feat["last_green"]:
        confidence += 8.0
    if feat["last2_green"]:
        confidence += 6.0
    if feat["higher_low"]:
        confidence += 6.0
    if feat["higher_high"]:
        confidence += 4.0
    if feat["breakout_above_prev_high"]:
        confidence += 10.0
    if feat["breakout_2bar"]:
        confidence += 6.0

    # momentum / trend
    confidence += clamp(ret1 * 9.0, -4.0, 10.0)
    confidence += clamp(ret3 * 7.0, 0.0, 15.0)
    confidence += clamp(ret5 * 5.0, 0.0, 16.0)
    confidence += clamp(trend_bias * 8.0, -5.0, 8.0)

    # volume and candle quality
    if vol_ratio >= 1.8:
        confidence += 6.0
    elif vol_ratio >= 1.2:
        confidence += 3.0
    elif vol_ratio < 0.75:
        confidence -= 4.0

    if body_to_range >= 0.28:
        confidence += 4.0
    elif body_to_range < 0.10:
        confidence -= 5.0

    if close_pos >= 0.70:
        confidence += 4.0
    elif close_pos < 0.45:
        confidence -= 5.0

    # anti-chase penalties
    anti_chase_flags: List[str] = []
    if ret1 > 1.25 and body_to_range > 0.70 and close_pos > 0.75:
        confidence -= 8.0
        anti_chase_flags.append("blowoff_last_bar")
    if dist_from_8_low > 4.6:
        confidence -= 6.0
        anti_chase_flags.append("too_extended")
    if scanner_spread > 0.45:
        confidence -= 4.0
        anti_chase_flags.append("wider_spread")
    if vol_ratio > 4.0 and ret1 > 1.0:
        confidence -= 5.0
        anti_chase_flags.append("volume_spike_chase_risk")

    # Setup type nudges
    if setup_type == "continuation":
        confidence += 4.0
    else:
        confidence -= 2.0  # reversals need stronger proof

    # Regime gating (this is the main v2.1 profit-protection idea)
    regime_label = str(regime.get("label", "unknown"))
    is_long_friendly = bool(regime.get("is_long_friendly", False))

    if not is_long_friendly:
        confidence -= 8.0
        anti_chase_flags.append("regime_not_long_friendly")

        # In bad regime, suppress continuation entries unless truly strong
        if REGIME_BAD_SUPPRESSION and setup_type == "continuation":
            if scanner_change <= 0 or vol_ratio < 1.25:
                info["reason"] = "regime_suppressed_continuation"
                return "no_signal", info

    confidence = round(clamp(confidence, 0.0, 100.0), 2)

    info.update(feat)
    info.update(
        {
            "setup_type": setup_type,
            "confidence": confidence,
            "anti_chase_flags": anti_chase_flags,
            "anti_chase_count": len(anti_chase_flags),
            "median_range_pct_8": stats8["median_range_pct"],
            "avg_range_pct_8": stats8["avg_range_pct"],
            "median_body_pct_8": stats8["median_body_pct"],
            "volume_ratio_last_vs_avg_8": stats8["volume_ratio_last_vs_avg"],
            "scanner_score": round(scanner_score, 4),
            "scanner_change_pct": round(scanner_change, 4),
            "scanner_spread_pct": round(scanner_spread, 4),
            "scanner_quote_vol": round(scanner_qv, 4),
            "regime_label": regime_label,
            "regime_is_long_friendly": is_long_friendly,
            "regime_avg_change_pct": safe_float(regime.get("avg_change_pct")),
            "regime_positive_count": int(safe_float(regime.get("positive_count"), 0)),
        }
    )

    # attach exit plan metadata for backtesting / future executor
    info["exit_plan"] = build_exit_plan_v3_1(info, setup_type=setup_type)

    # final thresholds
    would_buy_threshold = WOULD_BUY_CONFIDENCE_MIN
    if not is_long_friendly:
        would_buy_threshold += REGIME_BAD_WOULD_BUY_CONF_BONUS_REQUIRED
    if setup_type == "reversal":
        would_buy_threshold += 3.0

    if confidence >= would_buy_threshold:
        info["reason"] = "would_buy_signal_v3_1"
        return "would_buy", info

    if confidence >= WATCHLIST_CONFIDENCE_MIN:
        info["reason"] = "watchlist_signal_v3_1"
        return "watchlist", info

    info["reason"] = "confidence_too_low"
    return "no_signal", info


# =========================
# EXCHANGE WRAPPERS
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
    return sorted(set(symbols))


async def fetch_tickers_safe(exchange: ccxt.kraken, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    try:
        return await exchange.fetch_tickers(symbols)
    except Exception as e:
        logger.exception(f"fetch_tickers failed: {e}")
        return {}


async def fetch_ohlcv_safe(exchange: ccxt.kraken, symbol: str, timeframe: str, limit: int) -> List[Dict[str, float]]:
    try:
        rows = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        return parse_ohlcv_rows(rows)
    except Exception as e:
        logger.debug(f"fetch_ohlcv failed for {symbol}: {e}")
        return []


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


async def fetch_many_ohlcv(
    exchange: ccxt.kraken,
    symbols: List[str],
    timeframe: str,
    limit: int,
    concurrency: int,
) -> Dict[str, List[Dict[str, float]]]:
    sem = asyncio.Semaphore(max(1, int(concurrency)))
    out: Dict[str, List[Dict[str, float]]] = {}

    async def _one(sym: str) -> None:
        async with sem:
            out[sym] = await fetch_ohlcv_safe(exchange, sym, timeframe, limit)
            await asyncio.sleep(0.03)  # small spacing; Kraken + ccxt rate limit already on

    await asyncio.gather(*[_one(s) for s in symbols], return_exceptions=False)
    return out


# =========================
# MAIN LOOP
# =========================
async def signal_logger_loop_v3_1() -> None:
    exchange: Optional[ccxt.kraken] = None
    would_buy_cooldown = CooldownMap(SIGNAL_COOLDOWN_SECONDS)
    watch_cooldown = CooldownMap(WATCH_COOLDOWN_SECONDS)

    try:
        exchange = build_exchange()
        logger.info(f"Starting Kraken signal logger {VERSION} (read only fresh rebuild)")

        # public time check
        try:
            server_time = await exchange.fetch_time()
            logger.info(f"Kraken server time: {server_time}")
        except Exception as e:
            logger.warning(f"fetch_time failed: {e}")

        # optional private balance check
        if PERFORM_PRIVATE_BALANCE_CHECK:
            try:
                bal = await exchange.fetch_balance()
                total_keys = len(bal.get("total", {})) if isinstance(bal, dict) else 0
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

        cycle_num = 0
        while True:
            cycle_num += 1
            cycle_start = time.time()

            try:
                tickers = await fetch_tickers_safe(exchange, symbols)
                ranked = build_ranked_list(tickers)
                if not ranked:
                    logger.info("No ranked candidates this cycle")
                    await asyncio.sleep(SCAN_INTERVAL_SECONDS)
                    continue

                log_top_candidates(ranked, TOP_N_SCANNER_LOG)

                regime = classify_market_regime(ranked)
                logger.info(
                    "Regime | "
                    f"label={regime['label']} | "
                    f"top_n={regime['top_n']} | "
                    f"positive={regime['positive_count']} | "
                    f"avg_change={regime['avg_change_pct']}% | "
                    f"median_change={regime['median_change_pct']}% | "
                    f"long_friendly={regime['is_long_friendly']}"
                )

                top_rows = ranked[:TOP_N_CANDLE_CHECK]
                top_symbols = [r["symbol"] for r in top_rows]

                ohlcv_map = await fetch_many_ohlcv(
                    exchange=exchange,
                    symbols=top_symbols,
                    timeframe=OHLCV_TIMEFRAME,
                    limit=OHLCV_LIMIT,
                    concurrency=OHLCV_CONCURRENCY,
                )

                checked = 0
                watch_hits = 0
                would_buy_hits = 0

                for row in top_rows:
                    symbol = row["symbol"]
                    candles = ohlcv_map.get(symbol, [])
                    checked += 1

                    status, details = evaluate_signal_v3_1(symbol, candles, row, regime)
                    now_ts = time.time()

                    if status == "would_buy":
                        key = f"would_buy:{symbol}"
                        if would_buy_cooldown.can_emit(key, now_ts):
                            would_buy_cooldown.mark(key, now_ts)
                            would_buy_hits += 1

                            logger.info(
                                "WOULD BUY v3.1 | "
                                f"{symbol} | "
                                f"setup={details.get('setup_type')} | "
                                f"conf={details.get('confidence')} | "
                                f"scanner={details.get('scanner_score')} | "
                                f"ret1={details.get('ret_1_pct')}% | "
                                f"ret3={details.get('ret_3_pct')}% | "
                                f"ret5={details.get('ret_5_pct')}% | "
                                f"volr={details.get('volume_ratio_last_vs_avg_8')} | "
                                f"regime={details.get('regime_label')}"
                            )

                            append_jsonl(
                                SIGNAL_LOG_JSONL,
                                {
                                    "ts_utc": now_utc_iso(),
                                    "event": EVENT_WOULD_BUY,
                                    "symbol": symbol,
                                    "timeframe": OHLCV_TIMEFRAME,
                                    "scanner": {
                                        "score": row.get("score"),
                                        "change_pct": row.get("change_pct"),
                                        "spread_pct": row.get("spread_pct"),
                                        "quote_vol": row.get("quote_vol"),
                                        "last": row.get("last"),
                                    },
                                    "regime": regime,
                                    "signal": details,
                                },
                            )
                        else:
                            logger.info(f"Cooldown active | WOULD BUY v3.1 | {symbol}")

                    elif status == "watchlist":
                        key = f"watch:{symbol}"
                        if watch_cooldown.can_emit(key, now_ts):
                            watch_cooldown.mark(key, now_ts)
                            watch_hits += 1

                            logger.info(
                                "WATCHLIST v3.1 | "
                                f"{symbol} | "
                                f"setup={details.get('setup_type')} | "
                                f"conf={details.get('confidence')} | "
                                f"scanner={details.get('scanner_score')} | "
                                f"ret1={details.get('ret_1_pct')}% | "
                                f"ret3={details.get('ret_3_pct')}% | "
                                f"volr={details.get('volume_ratio_last_vs_avg_8')} | "
                                f"regime={details.get('regime_label')}"
                            )

                            append_jsonl(
                                SIGNAL_LOG_JSONL,
                                {
                                    "ts_utc": now_utc_iso(),
                                    "event": EVENT_WATCHLIST,
                                    "symbol": symbol,
                                    "timeframe": OHLCV_TIMEFRAME,
                                    "scanner": {
                                        "score": row.get("score"),
                                        "change_pct": row.get("change_pct"),
                                        "spread_pct": row.get("spread_pct"),
                                        "quote_vol": row.get("quote_vol"),
                                        "last": row.get("last"),
                                    },
                                    "regime": regime,
                                    "signal": details,
                                },
                            )
                        else:
                            logger.info(f"Cooldown active | WATCHLIST v3.1 | {symbol}")
                    else:
                        logger.info(f"NO SIGNAL v3.1 | {symbol:<14} | {details.get('reason')}")

                elapsed = time.time() - cycle_start
                logger.info(
                    f"Cycle done v3.1 | cycle={cycle_num} | scanned={len(ranked)} | "
                    f"candle_checked={checked} | watch_hits={watch_hits} | "
                    f"would_buy_hits={would_buy_hits} | elapsed={elapsed:.2f}s"
                )

            except Exception as cycle_error:
                logger.exception(f"Signal logger v3.1 cycle error: {cycle_error}")

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    except KeyboardInterrupt:
        logger.info("Signal logger v3.1 stopped by user")
        raise
    except Exception as e:
        logger.exception(f"Fatal signal logger v3.1 error: {e}")
    finally:
        if exchange is not None:
            try:
                await exchange.close()
            except Exception:
                pass
        logger.info("Signal logger v3.1 shutdown complete")


def main() -> None:
    try:
        asyncio.run(signal_logger_loop_v3_1())
    except KeyboardInterrupt:
        logger.info("Exited")


if __name__ == "__main__":
    main()
