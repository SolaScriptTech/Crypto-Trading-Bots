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
TOP_N_SCANNER = 40
TOP_N_CANDLE_CHECK = 25

QUOTE_CURRENCIES = {"USD"}
PREFER_QUOTE = "USD"

MIN_QUOTE_VOLUME_24H = 500000.0
MAX_SPREAD_PCT = 0.65

LOG_FILE = "kraken_signal_logger_v3_2.log"
SIGNAL_LOG_JSONL = "kraken_signal_events_v3_2.jsonl"

# New reject logging
REJECT_LOG_TOP_N_PER_LOOP = 5
LOG_REJECTIONS_TO_JSONL = True
REJECT_JSONL_EVENT_NAME = "candidate_rejected_v3_2"

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
    "USD/USD",
    "USDT/USD",
    "USDC/USD",
}

# Pullback continuation model settings
TIMEFRAME = "1m"
OHLCV_LIMIT = 12

# Entry shape filters
MIN_UPTREND_5M_PCT = 1.20
MAX_CHASE_1M_PCT = 0.60
PULLBACK_DEPTH_MIN_PCT = -0.90
PULLBACK_DEPTH_MAX_PCT = -0.10
MIN_RECOVERY_FROM_PULLBACK_PCT = 0.15

# Candle structure filters
MIN_CLOSE_POS_IN_BAR = 0.45
MIN_VOLUME_RATIO_LAST_VS_AVG8 = 0.75
MAX_RANGE_PCT_LAST = 2.80
MIN_RANGE_PCT_LAST = 0.08

# Optional anti spam and anti repeat
COOLDOWN_SECONDS_PER_SYMBOL = 180

# Regime check across current top scanner list
REGIME_TOP_N = 15
MIN_POSITIVE_COUNT_FOR_LONG = 8

# Debug and networking
HTTP_TIMEOUT_MS = 20000
ENABLE_RATE_LIMIT = True


# =========================
# LOGGING
# =========================
logger = logging.getLogger("kraken_v3_2")
logger.setLevel(logging.INFO)

_formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

_file_handler = logging.FileHandler(LOG_FILE)
_file_handler.setFormatter(_formatter)
logger.addHandler(_file_handler)

_stream_handler = logging.StreamHandler(sys.stdout)
_stream_handler.setFormatter(_formatter)
logger.addHandler(_stream_handler)


# =========================
# HELPERS
# =========================
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def pct_change(a: float, b: float) -> float:
    if a == 0:
        return 0.0
    return ((b - a) / a) * 100.0


def close_pos_in_bar(high_: float, low_: float, close_: float) -> float:
    rng = high_ - low_
    if rng <= 0:
        return 0.5
    return (close_ - low_) / rng


def range_pct(high_: float, low_: float, ref_price: float) -> float:
    if ref_price <= 0:
        return 0.0
    return ((high_ - low_) / ref_price) * 100.0


def median(values: List[float]) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    n = len(vals)
    mid = n // 2
    if n % 2 == 1:
        return vals[mid]
    return (vals[mid - 1] + vals[mid]) / 2.0


def jsonl_append(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def choose_preferred_symbol_variants(symbols: List[str]) -> List[str]:
    by_base: Dict[str, List[str]] = {}
    for s in symbols:
        if "/" not in s:
            continue
        base, quote = s.split("/", 1)
        by_base.setdefault(base, []).append(s)

    chosen: List[str] = []
    for base, variants in by_base.items():
        variants_sorted = sorted(
            variants,
            key=lambda s: (0 if s.endswith(f"/{PREFER_QUOTE}") else 1, s),
        )
        chosen.append(variants_sorted[0])

    return sorted(chosen)


# =========================
# KRAKEN CLIENT
# =========================
async def make_exchange() -> ccxt.kraken:
    api_key = os.getenv("KRAKEN_API_KEY", "")
    api_secret = os.getenv("KRAKEN_API_SECRET", "")

    ex = ccxt.kraken(
        {
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": ENABLE_RATE_LIMIT,
            "timeout": HTTP_TIMEOUT_MS,
            "options": {
                "adjustForTimeDifference": True,
            },
        }
    )
    return ex


# =========================
# MARKET UNIVERSE
# =========================
def is_spot_market(m: Dict[str, Any]) -> bool:
    if not isinstance(m, dict):
        return False
    if not m.get("active", True):
        return False
    if m.get("spot") is False:
        return False
    if m.get("swap") or m.get("future") or m.get("option"):
        return False
    return True


def market_symbol_ok(symbol: str) -> bool:
    if symbol in EXCLUDED_SYMBOLS:
        return False
    if "/" not in symbol:
        return False
    base, quote = symbol.split("/", 1)
    if base in EXCLUDED_BASES:
        return False
    if quote not in QUOTE_CURRENCIES:
        return False
    return True


async def get_candidate_symbols(exchange: ccxt.kraken) -> List[str]:
    markets = await exchange.load_markets()

    symbols: List[str] = []
    for sym, m in markets.items():
        if not is_spot_market(m):
            continue
        if not market_symbol_ok(sym):
            continue
        symbols.append(sym)

    symbols = choose_preferred_symbol_variants(symbols)
    return symbols


# =========================
# SCANNER STAGE 1
# =========================
async def fetch_tickers(exchange: ccxt.kraken, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
    results: Dict[str, Dict[str, Any]] = {}
    chunk_size = 40

    for i in range(0, len(symbols), chunk_size):
        chunk = symbols[i:i + chunk_size]
        try:
            tickers = await exchange.fetch_tickers(chunk)
            if isinstance(tickers, dict):
                results.update(tickers)
        except Exception as e:
            logger.warning(f"fetch_tickers chunk failed ({i}-{i+len(chunk)-1}): {e}")
            await asyncio.sleep(0.5)

    return results


def parse_ticker_metrics(symbol: str, t: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(t, dict):
        return None

    bid = safe_float(t.get("bid"))
    ask = safe_float(t.get("ask"))
    last = safe_float(t.get("last"))
    quote_volume = safe_float(t.get("quoteVolume"))
    base_volume = safe_float(t.get("baseVolume"))
    pct = safe_float(t.get("percentage"))

    if last <= 0:
        return None

    if quote_volume <= 0 and base_volume > 0:
        quote_volume = base_volume * last

    spread_pct = 999.0
    if bid > 0 and ask > 0 and ask >= bid:
        spread_pct = ((ask - bid) / max(bid, 1e-12)) * 100.0

    return {
        "symbol": symbol,
        "last": last,
        "bid": bid,
        "ask": ask,
        "spread_pct": spread_pct,
        "quote_vol": quote_volume,
        "change_pct": pct,
    }


def build_stage1_rankings(tickers: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    parsed: List[Dict[str, Any]] = []

    for symbol, t in tickers.items():
        row = parse_ticker_metrics(symbol, t)
        if row is None:
            continue

        if row["quote_vol"] < MIN_QUOTE_VOLUME_24H:
            continue

        if row["spread_pct"] > max(MAX_SPREAD_PCT * 1.4, 0.95):
            continue

        change_pct = row["change_pct"]
        spread_pen = row["spread_pct"] * 2.2
        vol_bonus = min(row["quote_vol"] / 1_000_000.0, 8.0) * 1.1

        if change_pct >= 0:
            change_score = min(change_pct, 18.0) * 1.4
        else:
            change_score = change_pct * 0.35

        row["score"] = round(change_score + vol_bonus - spread_pen, 4)
        parsed.append(row)

    parsed.sort(key=lambda x: x["score"], reverse=True)
    return parsed[:TOP_N_SCANNER]


def build_regime(stage1_top: List[Dict[str, Any]]) -> Dict[str, Any]:
    top = stage1_top[:REGIME_TOP_N]
    changes = [safe_float(x.get("change_pct")) for x in top]
    positive_count = sum(1 for c in changes if c > 0)
    avg_change = sum(changes) / len(changes) if changes else 0.0
    med_change = median(changes)
    top1 = max(changes) if changes else 0.0

    is_long_friendly = positive_count >= MIN_POSITIVE_COUNT_FOR_LONG and avg_change > 0.0

    if is_long_friendly and top1 >= 5 and avg_change >= 1:
        label = "strong_long"
    elif is_long_friendly:
        label = "mixed_long_ok"
    else:
        label = "avoid_long"

    return {
        "label": label,
        "top_n": len(top),
        "positive_count": positive_count,
        "avg_change_pct": round(avg_change, 4),
        "median_change_pct": round(med_change, 4),
        "top1_change_pct": round(top1, 4),
        "is_long_friendly": is_long_friendly,
    }


# =========================
# SCANNER STAGE 2
# =========================
async def fetch_ohlcv_safe(
    exchange: ccxt.kraken,
    symbol: str,
    timeframe: str = TIMEFRAME,
    limit: int = OHLCV_LIMIT,
) -> Optional[List[List[float]]]:
    try:
        data = await exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not data or len(data) < 10:
            return None
        return data
    except Exception as e:
        logger.debug(f"fetch_ohlcv failed for {symbol}: {e}")
        return None


def analyze_pullback_continuation(
    symbol: str,
    ohlcv: List[List[float]],
    scanner_row: Dict[str, Any],
    regime: Dict[str, Any],
) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if len(ohlcv) < 10:
        return None, {
            "symbol": symbol,
            "reject_reason": "insufficient_ohlcv",
            "failed_checks": ["ohlcv_len_too_small"],
            "metrics": {
                "ohlcv_len": len(ohlcv),
            },
        }

    bars = ohlcv[-10:]
    opens = [safe_float(x[1]) for x in bars]
    highs = [safe_float(x[2]) for x in bars]
    lows = [safe_float(x[3]) for x in bars]
    closes = [safe_float(x[4]) for x in bars]
    vols = [safe_float(x[5]) for x in bars]

    last_o, last_h, last_l, last_c, last_v = opens[-1], highs[-1], lows[-1], closes[-1], vols[-1]
    prev_c = closes[-2]
    c_3 = closes[-4]
    c_5 = closes[-6]

    if min(last_c, prev_c, c_3, c_5) <= 0:
        return None, {
            "symbol": symbol,
            "reject_reason": "invalid_prices",
            "failed_checks": ["non_positive_reference_close"],
            "metrics": {
                "last_c": last_c,
                "prev_c": prev_c,
                "c_3": c_3,
                "c_5": c_5,
            },
        }

    ret_1_pct = pct_change(prev_c, last_c)
    ret_3_pct = pct_change(c_3, last_c)
    ret_5_pct = pct_change(c_5, last_c)

    recent_window_high = max(highs[-8:-1])
    recent_window_low = min(lows[-4:-1])
    if recent_window_high <= 0:
        return None, {
            "symbol": symbol,
            "reject_reason": "invalid_pullback_window",
            "failed_checks": ["recent_window_high_non_positive"],
            "metrics": {
                "recent_window_high": recent_window_high,
                "recent_window_low": recent_window_low,
            },
        }

    pullback_depth_pct = pct_change(recent_window_high, recent_window_low)
    recovery_from_pullback_pct = pct_change(recent_window_low, last_c)

    cpib = close_pos_in_bar(last_h, last_l, last_c)
    last_range_pct = range_pct(last_h, last_l, max(last_c, 1e-12))
    avg_vol_8 = sum(vols[-9:-1]) / 8.0 if len(vols[-9:-1]) == 8 else (sum(vols[:-1]) / max(len(vols[:-1]), 1))
    vol_ratio = (last_v / avg_vol_8) if avg_vol_8 > 0 else 0.0

    uptrend_ok = ret_5_pct >= MIN_UPTREND_5M_PCT
    chase_ok = ret_1_pct <= MAX_CHASE_1M_PCT
    pullback_ok = PULLBACK_DEPTH_MIN_PCT <= pullback_depth_pct <= PULLBACK_DEPTH_MAX_PCT
    recovery_ok = recovery_from_pullback_pct >= MIN_RECOVERY_FROM_PULLBACK_PCT
    structure_ok = cpib >= MIN_CLOSE_POS_IN_BAR
    range_ok = MIN_RANGE_PCT_LAST <= last_range_pct <= MAX_RANGE_PCT_LAST
    volume_ok = vol_ratio >= MIN_VOLUME_RATIO_LAST_VS_AVG8
    regime_ok = bool(regime.get("is_long_friendly", False))

    checks = {
        "uptrend_ok": uptrend_ok,
        "chase_ok": chase_ok,
        "pullback_ok": pullback_ok,
        "recovery_ok": recovery_ok,
        "structure_ok": structure_ok,
        "range_ok": range_ok,
        "volume_ok": volume_ok,
        "regime_ok": regime_ok,
    }

    metrics = {
        "ret_1_pct": round(ret_1_pct, 4),
        "ret_3_pct": round(ret_3_pct, 4),
        "ret_5_pct": round(ret_5_pct, 4),
        "pullback_depth_pct": round(pullback_depth_pct, 4),
        "recovery_from_pullback_pct": round(recovery_from_pullback_pct, 4),
        "close_pos_in_bar": round(cpib, 4),
        "last_range_pct": round(last_range_pct, 4),
        "volume_ratio_last_vs_avg_8": round(vol_ratio, 4),
        "last_open": round(last_o, 10),
        "last_high": round(last_h, 10),
        "last_low": round(last_l, 10),
        "last_close": round(last_c, 10),
        "last_volume": round(last_v, 8),
    }

    failed_checks = [k for k, v in checks.items() if not v]

    if failed_checks:
        reject = {
            "symbol": symbol,
            "reject_reason": "failed_strategy_checks",
            "failed_checks": failed_checks,
            "checks": checks,
            "metrics": metrics,
        }
        return None, reject

    scanner_score = safe_float(scanner_row.get("score"))
    score = (
        8.0
        + min(ret_5_pct, 8.0) * 1.1
        + min(recovery_from_pullback_pct, 2.5) * 3.0
        + min(vol_ratio, 3.0) * 2.2
        + max(0.0, cpib - 0.5) * 10.0
        + max(0.0, scanner_score) * 0.10
        - max(0.0, safe_float(scanner_row.get("spread_pct")) - 0.25) * 6.0
    )

    signal = {
        "symbol": symbol,
        "reason": "would_buy_signal_v3_2_pullback_continuation",
        "last_close": round(last_c, 10),
        "ret_1_pct": round(ret_1_pct, 4),
        "ret_3_pct": round(ret_3_pct, 4),
        "ret_5_pct": round(ret_5_pct, 4),
        "score": round(score, 4),
        "plan": {
            "strategy": "v3_2_pullback_continuation",
            "timeframe": TIMEFRAME,
            "entry_style": "dip_into_strength_reclaim",
            "risk_model_hint": {
                "hard_stop_pct": 0.85,
                "time_stop_minutes": 18,
                "momentum_fail": {
                    "enabled": True,
                    "check_after_minutes": 2,
                    "min_unrealized_gain_pct_to_avoid_fail": 0.14,
                    "max_drawdown_from_peak_pct_early": 0.45,
                    "two_red_closes_exit_after_minutes": 3,
                },
                "trailing": {
                    "arm_at_gain_pct": 0.70,
                    "trail_pct": 0.45,
                },
                "partials": [
                    {"take_gain_pct": 0.90, "size_fraction": 0.35},
                    {"take_gain_pct": 1.40, "size_fraction": 0.35},
                ],
            },
            "inputs": {
                "scanner_score": round(scanner_score, 4),
                "scanner_change_pct": round(safe_float(scanner_row.get("change_pct")), 6),
                "scanner_spread_pct": round(safe_float(scanner_row.get("spread_pct")), 6),
                "scanner_quote_vol": round(safe_float(scanner_row.get("quote_vol")), 4),
                "regime_label": regime.get("label"),
                "pullback_depth_pct": round(pullback_depth_pct, 4),
                "recovery_from_pullback_pct": round(recovery_from_pullback_pct, 4),
                "close_pos_in_bar": round(cpib, 4),
                "last_range_pct": round(last_range_pct, 4),
                "volume_ratio_last_vs_avg_8": round(vol_ratio, 4),
            },
            "checks": checks,
        },
    }

    return signal, None


def reject_log_line(symbol: str, failed_checks: List[str], metrics: Dict[str, Any], regime: Dict[str, Any]) -> str:
    return (
        f"REJECT v3_2 | {symbol} | failed={','.join(failed_checks)} | "
        f"r1={metrics.get('ret_1_pct', 'na')} r3={metrics.get('ret_3_pct', 'na')} r5={metrics.get('ret_5_pct', 'na')} | "
        f"pb={metrics.get('pullback_depth_pct', 'na')} rec={metrics.get('recovery_from_pullback_pct', 'na')} | "
        f"cpib={metrics.get('close_pos_in_bar', 'na')} rng={metrics.get('last_range_pct', 'na')} "
        f"vr={metrics.get('volume_ratio_last_vs_avg_8', 'na')} | regime={regime.get('label')}"
    )


# =========================
# MAIN LOOP
# =========================
async def scanner_loop() -> None:
    exchange = await make_exchange()
    last_signal_ts_by_symbol: Dict[str, float] = {}

    logger.info("Starting Kraken signal logger v3_2 (pullback continuation)")
    logger.info(f"Signal JSONL: {SIGNAL_LOG_JSONL}")
    logger.info(f"Log file: {LOG_FILE}")

    try:
        symbols = await get_candidate_symbols(exchange)
        logger.info(f"Universe loaded: {len(symbols)} candidate spot symbols")

        while True:
            loop_started = time.time()

            try:
                tickers = await fetch_tickers(exchange, symbols)
                stage1 = build_stage1_rankings(tickers)

                if not stage1:
                    logger.warning("No stage1 candidates after filtering")
                    await asyncio.sleep(SCAN_INTERVAL_SECONDS)
                    continue

                regime = build_regime(stage1)

                logger.info(
                    "Stage1 top=%d | regime=%s | pos=%d/%d | avg_change=%.2f%%",
                    len(stage1),
                    regime["label"],
                    regime["positive_count"],
                    regime["top_n"],
                    regime["avg_change_pct"],
                )

                stage2_candidates = stage1[:TOP_N_CANDLE_CHECK]
                signals_this_loop = 0
                rejects_logged_this_loop = 0

                for row in stage2_candidates:
                    symbol = row["symbol"]

                    now_ts = time.time()
                    last_ts = last_signal_ts_by_symbol.get(symbol, 0.0)
                    if (now_ts - last_ts) < COOLDOWN_SECONDS_PER_SYMBOL:
                        continue

                    if safe_float(row.get("spread_pct")) > MAX_SPREAD_PCT:
                        if rejects_logged_this_loop < REJECT_LOG_TOP_N_PER_LOOP:
                            rej_obj = {
                                "ts_utc": utc_now_iso(),
                                "event": REJECT_JSONL_EVENT_NAME,
                                "symbol": symbol,
                                "timeframe": TIMEFRAME,
                                "scanner": {
                                    "score": round(safe_float(row.get("score")), 4),
                                    "change_pct": round(safe_float(row.get("change_pct")), 6),
                                    "spread_pct": round(safe_float(row.get("spread_pct")), 6),
                                    "quote_vol": round(safe_float(row.get("quote_vol")), 4),
                                    "last": round(safe_float(row.get("last")), 10),
                                },
                                "regime": regime,
                                "reject": {
                                    "reject_reason": "spread_above_max_stage2",
                                    "failed_checks": ["spread_ok"],
                                    "checks": {"spread_ok": False},
                                    "metrics": {},
                                },
                            }
                            if LOG_REJECTIONS_TO_JSONL:
                                jsonl_append(SIGNAL_LOG_JSONL, rej_obj)
                            logger.info(
                                "REJECT v3_2 | %s | failed=spread_ok | spread=%.3f%% | regime=%s",
                                symbol,
                                safe_float(row.get("spread_pct")),
                                regime.get("label"),
                            )
                            rejects_logged_this_loop += 1
                        continue

                    ohlcv = await fetch_ohlcv_safe(exchange, symbol, timeframe=TIMEFRAME, limit=OHLCV_LIMIT)
                    if ohlcv is None:
                        if rejects_logged_this_loop < REJECT_LOG_TOP_N_PER_LOOP:
                            rej_obj = {
                                "ts_utc": utc_now_iso(),
                                "event": REJECT_JSONL_EVENT_NAME,
                                "symbol": symbol,
                                "timeframe": TIMEFRAME,
                                "scanner": {
                                    "score": round(safe_float(row.get("score")), 4),
                                    "change_pct": round(safe_float(row.get("change_pct")), 6),
                                    "spread_pct": round(safe_float(row.get("spread_pct")), 6),
                                    "quote_vol": round(safe_float(row.get("quote_vol")), 4),
                                    "last": round(safe_float(row.get("last")), 10),
                                },
                                "regime": regime,
                                "reject": {
                                    "reject_reason": "ohlcv_unavailable",
                                    "failed_checks": ["ohlcv_fetch_ok"],
                                    "checks": {"ohlcv_fetch_ok": False},
                                    "metrics": {},
                                },
                            }
                            if LOG_REJECTIONS_TO_JSONL:
                                jsonl_append(SIGNAL_LOG_JSONL, rej_obj)
                            logger.info(
                                "REJECT v3_2 | %s | failed=ohlcv_fetch_ok | regime=%s",
                                symbol,
                                regime.get("label"),
                            )
                            rejects_logged_this_loop += 1
                        continue

                    signal, reject = analyze_pullback_continuation(symbol, ohlcv, row, regime)

                    if signal is None:
                        if reject is not None and rejects_logged_this_loop < REJECT_LOG_TOP_N_PER_LOOP:
                            reject_payload = {
                                "ts_utc": utc_now_iso(),
                                "event": REJECT_JSONL_EVENT_NAME,
                                "symbol": symbol,
                                "timeframe": TIMEFRAME,
                                "scanner": {
                                    "score": round(safe_float(row.get("score")), 4),
                                    "change_pct": round(safe_float(row.get("change_pct")), 6),
                                    "spread_pct": round(safe_float(row.get("spread_pct")), 6),
                                    "quote_vol": round(safe_float(row.get("quote_vol")), 4),
                                    "last": round(safe_float(row.get("last")), 10),
                                },
                                "regime": regime,
                                "reject": {
                                    **reject,
                                    "scanner_score": round(safe_float(row.get("score")), 4),
                                    "scanner_change_pct": round(safe_float(row.get("change_pct")), 6),
                                    "scanner_spread_pct": round(safe_float(row.get("spread_pct")), 6),
                                    "scanner_quote_vol": round(safe_float(row.get("quote_vol")), 4),
                                },
                            }

                            if LOG_REJECTIONS_TO_JSONL:
                                jsonl_append(SIGNAL_LOG_JSONL, reject_payload)

                            metrics = reject.get("metrics", {})
                            failed_checks = reject.get("failed_checks", ["unknown"])
                            logger.info(reject_log_line(symbol, failed_checks, metrics, regime))
                            rejects_logged_this_loop += 1
                        continue

                    event_obj = {
                        "ts_utc": utc_now_iso(),
                        "event": "would_buy_v3_2",
                        "symbol": symbol,
                        "timeframe": TIMEFRAME,
                        "scanner": {
                            "score": round(safe_float(row.get("score")), 4),
                            "change_pct": round(safe_float(row.get("change_pct")), 6),
                            "spread_pct": round(safe_float(row.get("spread_pct")), 6),
                            "quote_vol": round(safe_float(row.get("quote_vol")), 4),
                            "last": round(safe_float(row.get("last")), 10),
                        },
                        "regime": regime,
                        "signal": signal,
                    }

                    jsonl_append(SIGNAL_LOG_JSONL, event_obj)
                    last_signal_ts_by_symbol[symbol] = now_ts
                    signals_this_loop += 1

                    logger.info(
                        "SIGNAL v3_2 | %s | score=%.2f | r1=%.3f r3=%.3f r5=%.3f | spread=%.3f%% | qv=%.0f | regime=%s",
                        symbol,
                        safe_float(signal.get("score")),
                        safe_float(signal.get("ret_1_pct")),
                        safe_float(signal.get("ret_3_pct")),
                        safe_float(signal.get("ret_5_pct")),
                        safe_float(row.get("spread_pct")),
                        safe_float(row.get("quote_vol")),
                        regime.get("label"),
                    )

                elapsed = time.time() - loop_started
                logger.info(
                    "Loop done | stage1=%d | checked=%d | signals=%d | rejects_logged=%d | elapsed=%.2fs",
                    len(stage1),
                    len(stage2_candidates),
                    signals_this_loop,
                    rejects_logged_this_loop,
                    elapsed,
                )

            except Exception as e:
                logger.exception(f"Loop error: {e}")

            sleep_for = max(1.0, SCAN_INTERVAL_SECONDS - (time.time() - loop_started))
            await asyncio.sleep(sleep_for)

    finally:
        try:
            await exchange.close()
        except Exception:
            pass


def main() -> None:
    try:
        asyncio.run(scanner_loop())
    except KeyboardInterrupt:
        logger.info("Stopped by user")


if __name__ == "__main__":
    main()
