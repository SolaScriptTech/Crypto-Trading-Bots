import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from dotenv import load_dotenv
import ccxt.async_support as ccxt

load_dotenv()

# =========================================================
# BOT CONFIGURATION: v3_4 momentum + smarter trailing exits
# =========================================================
QUOTE_CCY = "USD"

MAX_POSITIONS = 3
USD_PER_TRADE = 25.0

# =========================
# DATA DRIVEN FILTERS
# =========================
MIN_QUOTE_VOL = 1_500_000.0
MAX_QUOTE_VOL = 0.0
MAX_SCANNER_CHANGE_PCT = 3.0
MAX_SPREAD_PCT = 0.12
MIN_SCANNER_SCORE = 0.0

# Regime gate
ENABLE_REGIME_GATE = True
REGIME_SAMPLE_TOP_N = 15
REGIME_MIN_POSITIVE_COUNT = 8
REGIME_MIN_AVG_CHANGE_PCT = 0.0

# Setup filters
REQUIRED_SETUP = "continuation"
OHLCV_LIMIT = 15
OHLCV_TIMEFRAME = "1m"

# Pullback continuation checks
MIN_RET_5_PCT = 0.60
MAX_RET_1_PCT = 0.40
MIN_PULLBACK_PCT = -1.20
MAX_PULLBACK_PCT = -0.05
MIN_RECOVERY_FROM_PULLBACK_PCT = 0.10
MIN_CLOSE_POS_IN_BAR = 0.45
MIN_LAST_BAR_RANGE_PCT = 0.05
MAX_LAST_BAR_RANGE_PCT = 2.50
MIN_VOL_RATIO_LAST_VS_AVG8 = 0.80

# =========================
# Position management / risk
# =========================
HARD_STOP_LOSS_PCT = 1.00

# Profit protection activation threshold
TRAIL_ACTIVATION_PCT = 1.00

# Profit secured stop logic
# "within 1% of total profit OR 1% from highest, whichever is first"
# This is implemented as a stop price equal to the HIGHER of:
# 1) peak based stop (1% below peak)
# 2) keep all but 1% of profit from entry to peak
TRAIL_BUFFER_PCT = 1.00
PROFIT_GIVEBACK_PCT = 1.00
MIN_SECURED_PROFIT_PCT = 0.05

# Optional early break even arm
BREAK_EVEN_ARM_PCT = 0.45

# Smarter trailing stop touch handling
TRAIL_TOUCH_GRACE_SECONDS = 20.0
TRAIL_RECLAIM_BUFFER_PCT = 0.12
TRAIL_CONFIRM_MIN_1M_BARS = 4
TRAIL_HOLD_EXTEND_SECONDS = 20.0

# Continuation confirmation thresholds on trail touch
CONT_HOLD_REQUIRE_LAST_1M_GREEN = True
CONT_HOLD_REQUIRE_NOT_LAST2_RED = True
CONT_HOLD_REQUIRE_ABOVE_TRAIL_BUFFER = True
CONT_HOLD_REQUIRE_5M_NOT_BEARISH = False
CONT_HOLD_MIN_VOL_RATIO_LAST2_VS_PRIOR2 = 0.85
CONT_HOLD_MIN_SCORE_TO_HOLD = 2

# Time and momentum fail
MAX_HOLD_MINUTES = 18
MOMENTUM_FAIL_CHECK_AFTER_MIN = 2
MOMENTUM_FAIL_MIN_PROFIT_PCT = 0.10
MOMENTUM_FAIL_MAX_DRAWDOWN_FROM_PEAK_PCT = 0.45

# Polling
SCAN_EVERY_S = 3.0
RISK_LOOP_EVERY_S = 0.5
TICKER_REFRESH_CHUNK = 40

# Timeouts / debug
FETCH_TICKERS_TIMEOUT_S = 25.0
FETCH_OHLCV_TIMEOUT_S = 12.0
FETCH_POS_TICKERS_TIMEOUT_S = 10.0
HEARTBEAT_EVERY_S = 30.0
LOG_SCAN_SKIP_REASONS = True
LOG_SCAN_START_END = True
MAX_REJECTS_PER_SCAN = 10

# Files
STATE_FILE = "kraken_live_state_v3_4.json"
LOG_FILE = "kraken_live_execution_v3_4.log"
EVENTS_JSONL = "kraken_live_events_v3_4.jsonl"
REJECTS_JSONL = "kraken_live_rejects_v3_4.jsonl"

# Rejection logging
LOG_REJECTS = True

# Networking
HTTP_TIMEOUT_MS = 20000
ENABLE_RATE_LIMIT = True

# Cooldowns
DEFAULT_REBUY_COOLDOWN_S = 120.0
TRAIL_SELL_REBUY_COOLDOWN_S = 1800.0
STOPLOSS_SELL_REBUY_COOLDOWN_S = 300.0

# Universe exclusions
EXCLUDED_BASES = {
    "USD", "USDT", "USDC", "USDG",
    "EUR", "GBP", "AUD", "CAD", "CHF", "JPY",
}
EXCLUDED_SYMBOLS = {
    "USD/USD", "USDT/USD", "USDC/USD"
}

# =========================================================
# LOGGING SETUP
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
log = logging.getLogger("KrakenPoC_v3_4")


# =========================================================
# HELPERS
# =========================================================
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


def jsonl_append(path: str, obj: Dict[str, Any]) -> None:
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def ensure_file_exists(path: str) -> None:
    if not os.path.exists(path):
        with open(path, "a", encoding="utf-8"):
            pass


def is_spot_usd_symbol(sym: str) -> bool:
    if not isinstance(sym, str) or "/" not in sym:
        return False
    if sym in EXCLUDED_SYMBOLS:
        return False
    base, quote = sym.split("/", 1)
    if quote != QUOTE_CCY:
        return False
    if base in EXCLUDED_BASES:
        return False
    return True


def candle_green(c: List[float]) -> bool:
    if not c or len(c) < 6:
        return False
    o = safe_float(c[1])
    cl = safe_float(c[4])
    return cl > o


def candle_red(c: List[float]) -> bool:
    if not c or len(c) < 6:
        return False
    o = safe_float(c[1])
    cl = safe_float(c[4])
    return cl < o


# =========================================================
# STATE MANAGEMENT
# =========================================================
@dataclass
class Position:
    symbol: str
    amount: float
    entry_price: float
    entry_time: float
    peak_price: float
    trail_active: bool = False
    break_even_armed: bool = False
    strategy: str = "v3_4_momentum"
    scanner_change_pct: float = 0.0
    scanner_spread_pct: float = 0.0
    scanner_quote_vol: float = 0.0
    entry_signal_score: float = 0.0

    # New smarter trailing exit fields
    profit_secured: bool = False
    secured_stop_price: float = 0.0
    trail_touch_pending: bool = False
    trail_touch_first_ts: float = 0.0
    trail_touch_price: float = 0.0
    last_profit_secured_log_state: bool = False


class KrakenV34Bot:
    def __init__(self) -> None:
        self.ex: Optional[ccxt.kraken] = None
        self.positions: Dict[str, Position] = {}
        self.last_scan_ts: float = 0.0
        self.last_risk_ts: float = 0.0
        self.last_heartbeat_ts: float = 0.0
        self.markets_loaded = False
        self.symbol_universe: List[str] = []

        # Cooldowns by symbol
        self.rebuy_cooldowns_until: Dict[str, float] = {}

        # Runtime counters for visibility
        self.loop_counter: int = 0
        self.scan_counter: int = 0
        self.risk_counter: int = 0
        self.last_scan_summary: Dict[str, Any] = {}
        self.last_risk_summary: Dict[str, Any] = {}
        self.last_progress_note: str = "startup"

        self.load_state()

    # -------------------------
    # State
    # -------------------------
    def load_state(self) -> None:
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            pos_blob = data.get("positions", data if isinstance(data, dict) else {})
            if isinstance(pos_blob, dict):
                for sym, pos_data in pos_blob.items():
                    if isinstance(pos_data, dict) and "symbol" in pos_data:
                        self.positions[sym] = Position(**pos_data)

            rb = data.get("rebuy_cooldowns_until", {})
            if isinstance(rb, dict):
                self.rebuy_cooldowns_until = {k: safe_float(v) for k, v in rb.items()}

            log.info(f"Loaded state | positions={len(self.positions)} | cooldowns={len(self.rebuy_cooldowns_until)}")
        except Exception as e:
            log.error(f"Failed to load state: {e}")

    def save_state(self) -> None:
        try:
            payload = {
                "positions": {sym: asdict(pos) for sym, pos in self.positions.items()},
                "rebuy_cooldowns_until": self.rebuy_cooldowns_until,
            }
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
        except Exception as e:
            log.error(f"Failed to save state: {e}")

    # -------------------------
    # Exchange
    # -------------------------
    async def init_exchange(self) -> None:
        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = os.getenv("KRAKEN_API_SECRET") or os.getenv("KRAKEN_PRIVATE_KEY")

        if not api_key or not api_secret:
            raise RuntimeError("Missing Kraken credentials. Need KRAKEN_API_KEY and KRAKEN_API_SECRET (or KRAKEN_PRIVATE_KEY fallback).")

        self.ex = ccxt.kraken({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": ENABLE_RATE_LIMIT,
            "timeout": HTTP_TIMEOUT_MS,
            "options": {"adjustForTimeDifference": True},
        })

        await self.ex.load_markets()
        self.markets_loaded = True
        self.symbol_universe = self._build_symbol_universe()
        log.info(f"Loaded markets. Universe size: {len(self.symbol_universe)}")

    def _build_symbol_universe(self) -> List[str]:
        if not self.ex or not getattr(self.ex, "markets", None):
            return []
        out: List[str] = []
        for sym, m in self.ex.markets.items():
            try:
                if not m.get("active", True):
                    continue
                if m.get("spot") is False:
                    continue
                if m.get("swap") or m.get("future") or m.get("option"):
                    continue
                if not is_spot_usd_symbol(sym):
                    continue
                out.append(sym)
            except Exception:
                continue
        return sorted(out)

    # -------------------------
    # Logging helpers
    # -------------------------
    def log_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        try:
            jsonl_append(EVENTS_JSONL, {
                "ts_utc": utc_now_iso(),
                "event": event_type,
                **payload
            })
        except Exception as e:
            log.warning(f"Failed writing event jsonl: {e}")

    def log_reject(self, symbol: str, reason: str, info: Dict[str, Any], signal: Optional[Dict[str, Any]] = None) -> None:
        if not LOG_REJECTS:
            return
        obj = {
            "ts_utc": utc_now_iso(),
            "event": "candidate_rejected_v3_4",
            "symbol": symbol,
            "reject_reason": reason,
            "info": info,
        }
        if signal is not None:
            obj["signal_context"] = signal
        try:
            jsonl_append(REJECTS_JSONL, obj)
        except Exception as e:
            log.warning(f"Failed writing reject jsonl: {e}")

    def _cooldown_remaining_s(self, sym: str, now_ts: Optional[float] = None) -> float:
        now_ts = now_ts or time.time()
        until = safe_float(self.rebuy_cooldowns_until.get(sym), 0.0)
        return max(0.0, until - now_ts)

    def _set_rebuy_cooldown(self, sym: str, seconds: float, reason: str) -> None:
        until = time.time() + max(0.0, seconds)
        prev = safe_float(self.rebuy_cooldowns_until.get(sym), 0.0)
        if until > prev:
            self.rebuy_cooldowns_until[sym] = until
            log.info(f"REBUY COOLDOWN | {sym} | until={datetime.fromtimestamp(until, tz=timezone.utc).isoformat()} | reason={reason}")
            self.log_event("rebuy_cooldown_set", {
                "symbol": sym,
                "cooldown_seconds": round(seconds, 3),
                "until_ts": until,
                "reason": reason,
            })

    def log_heartbeat(self) -> None:
        now = time.time()
        if now - self.last_heartbeat_ts < HEARTBEAT_EVERY_S:
            return
        self.last_heartbeat_ts = now

        pos_list = list(self.positions.keys())
        scan_info = self.last_scan_summary or {}
        risk_info = self.last_risk_summary or {}

        pos_lines = []
        for sym, pos in self.positions.items():
            pos_lines.append({
                "symbol": sym,
                "profit_secured": bool(pos.profit_secured),
                "trail_active": bool(pos.trail_active),
                "secured_stop_price": round(pos.secured_stop_price, 10) if pos.secured_stop_price > 0 else 0.0,
                "trail_touch_pending": bool(pos.trail_touch_pending),
            })

        log.info(
            "HEARTBEAT | loops=%s | scans=%s | risks=%s | positions=%s | note=%s | "
            "last_scan={ranked:%s pre_rej:%s setup_rej:%s buys:%s regime:%s} | "
            "last_risk={checked:%s sells:%s} | secured=%s",
            self.loop_counter,
            self.scan_counter,
            self.risk_counter,
            len(pos_list),
            self.last_progress_note,
            scan_info.get("ranked", 0),
            scan_info.get("prefilter_rejects", 0),
            scan_info.get("setup_rejects", 0),
            scan_info.get("buy_attempts", 0),
            scan_info.get("regime", "n/a"),
            risk_info.get("checked", 0),
            risk_info.get("sells", 0),
            sum(1 for p in self.positions.values() if p.profit_secured),
        )

        for sym, pos in self.positions.items():
            log.info(
                "POSITION | %s | Profit Secured: %s | trail_active=%s | pending_touch=%s | secured_stop=%s | entry=%.8f | peak=%.8f",
                sym,
                "True" if pos.profit_secured else "False",
                pos.trail_active,
                pos.trail_touch_pending,
                f"{pos.secured_stop_price:.8f}" if pos.secured_stop_price > 0 else "n/a",
                pos.entry_price,
                pos.peak_price,
            )

        self.log_event("heartbeat", {
            "loop_counter": self.loop_counter,
            "scan_counter": self.scan_counter,
            "risk_counter": self.risk_counter,
            "positions": pos_list,
            "position_state": pos_lines,
            "last_progress_note": self.last_progress_note,
            "last_scan_summary": scan_info,
            "last_risk_summary": risk_info,
        })

    # -------------------------
    # Scanner helpers
    # -------------------------
    async def fetch_tickers_chunked(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        results: Dict[str, Dict[str, Any]] = {}
        if not symbols:
            return results

        for i in range(0, len(symbols), TICKER_REFRESH_CHUNK):
            chunk = symbols[i:i + TICKER_REFRESH_CHUNK]
            try:
                self.last_progress_note = f"fetch_tickers_chunk_{i}_{i+len(chunk)-1}"
                tickers = await asyncio.wait_for(
                    self.ex.fetch_tickers(chunk),
                    timeout=FETCH_TICKERS_TIMEOUT_S
                )
                if isinstance(tickers, dict):
                    results.update(tickers)
            except asyncio.TimeoutError:
                log.warning(f"fetch_tickers chunk timeout {i}-{i+len(chunk)-1}")
            except Exception as e:
                log.warning(f"fetch_tickers chunk failed {i}-{i+len(chunk)-1}: {e}")

            await asyncio.sleep(0.15)

        return results

    def parse_ticker_row(self, sym: str, t: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(t, dict):
            return None

        bid = safe_float(t.get("bid"))
        ask = safe_float(t.get("ask"))
        last = safe_float(t.get("last"))
        quote_vol = safe_float(t.get("quoteVolume"))
        base_vol = safe_float(t.get("baseVolume"))
        pct24 = safe_float(t.get("percentage"))

        if last <= 0:
            return None
        if quote_vol <= 0 and base_vol > 0:
            quote_vol = base_vol * last

        spread_pct = 999.0
        if bid > 0 and ask > 0 and ask >= bid:
            spread_pct = ((ask - bid) / max(bid, 1e-12)) * 100.0

        scanner_score = max(0.0, min(pct24, 8.0)) * 1.2 + min(quote_vol / 1_000_000.0, 6.0) - (spread_pct * 8.0)

        return {
            "symbol": sym,
            "bid": bid,
            "ask": ask,
            "last": last,
            "quote_vol": quote_vol,
            "pct24": pct24,
            "spread_pct": spread_pct,
            "scanner_score": scanner_score,
        }

    def build_regime(self, ranked_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
        top = ranked_rows[:REGIME_SAMPLE_TOP_N]
        changes = [safe_float(r.get("pct24")) for r in top]
        if not top:
            return {
                "label": "unknown",
                "positive_count": 0,
                "avg_change_pct": 0.0,
                "is_long_ok": False
            }

        positive_count = sum(1 for c in changes if c > 0)
        avg_change = sum(changes) / len(changes)
        is_long_ok = (positive_count >= REGIME_MIN_POSITIVE_COUNT) and (avg_change >= REGIME_MIN_AVG_CHANGE_PCT)

        if is_long_ok and positive_count >= 11 and avg_change >= 1.0:
            label = "strong_long"
        elif is_long_ok:
            label = "mixed_long_ok"
        else:
            label = "avoid_long"

        return {
            "label": label,
            "positive_count": positive_count,
            "avg_change_pct": round(avg_change, 4),
            "top_n": len(top),
            "is_long_ok": is_long_ok,
        }

    async def fetch_ohlcv_rows(self, sym: str, timeframe: str = OHLCV_TIMEFRAME, limit: int = OHLCV_LIMIT) -> Optional[List[List[float]]]:
        try:
            self.last_progress_note = f"fetch_ohlcv_{sym}_{timeframe}"
            rows = await asyncio.wait_for(
                self.ex.fetch_ohlcv(sym, timeframe=timeframe, limit=limit),
                timeout=FETCH_OHLCV_TIMEOUT_S
            )
            if not rows or len(rows) < min(10, limit):
                return None
            return rows
        except asyncio.TimeoutError:
            log.warning(f"fetch_ohlcv timeout for {sym} {timeframe}")
            return None
        except Exception as e:
            log.warning(f"fetch_ohlcv failed for {sym} {timeframe}: {e}")
            return None

    def analyze_setup(
        self,
        sym: str,
        row: Dict[str, Any],
        ohlcv: List[List[float]],
        regime: Dict[str, Any]
    ) -> Tuple[Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
        bars = ohlcv[-10:]
        opens = [safe_float(x[1]) for x in bars]
        highs = [safe_float(x[2]) for x in bars]
        lows = [safe_float(x[3]) for x in bars]
        closes = [safe_float(x[4]) for x in bars]
        vols = [safe_float(x[5]) for x in bars]

        last_o, last_h, last_l, last_c, last_v = opens[-1], highs[-1], lows[-1], closes[-1], vols[-1]
        prev_o, prev_c = opens[-2], closes[-2]
        c_3 = closes[-4]
        c_5 = closes[-6]

        if min(last_c, prev_c, c_3, c_5) <= 0:
            return None, {
                "reject_reason": "invalid_prices",
                "failed_checks": ["valid_prices"],
                "metrics": {}
            }

        ret_1_pct = pct_change(prev_c, last_c)
        ret_5_pct = pct_change(c_5, last_c)

        high_8 = max(highs[-8:-1])
        low_recent_pullback = min(lows[-4:-1]) if len(lows) >= 4 else min(lows[:-1])

        pullback_pct = pct_change(high_8, last_c)
        recovery_from_pullback_pct = pct_change(low_recent_pullback, last_c)

        cpib = close_pos_in_bar(last_h, last_l, last_c)
        last_bar_range_pct = range_pct(last_h, last_l, max(last_c, 1e-12))

        avg_vol_8 = sum(vols[-9:-1]) / 8.0 if len(vols[-9:-1]) == 8 else max(sum(vols[:-1]) / max(len(vols[:-1]), 1), 1e-12)
        vol_ratio = (last_v / avg_vol_8) if avg_vol_8 > 0 else 0.0

        is_green = last_c > last_o
        prev_green = prev_c > prev_o
        setup_type = "continuation" if (is_green and prev_green) else "reversal"

        checks = {
            "regime_ok": (not ENABLE_REGIME_GATE) or bool(regime.get("is_long_ok", False)),
            "quote_vol_ok": row["quote_vol"] >= MIN_QUOTE_VOL and (MAX_QUOTE_VOL <= 0 or row["quote_vol"] <= MAX_QUOTE_VOL),
            "spread_ok": row["spread_pct"] <= MAX_SPREAD_PCT,
            "change_ok": row["pct24"] <= MAX_SCANNER_CHANGE_PCT,
            "scanner_score_ok": row["scanner_score"] >= MIN_SCANNER_SCORE,
            "ret_5_ok": ret_5_pct >= MIN_RET_5_PCT,
            "ret_1_ok": ret_1_pct <= MAX_RET_1_PCT,
            "pullback_ok": MIN_PULLBACK_PCT <= pullback_pct <= MAX_PULLBACK_PCT,
            "recovery_ok": recovery_from_pullback_pct >= MIN_RECOVERY_FROM_PULLBACK_PCT,
            "cpib_ok": cpib >= MIN_CLOSE_POS_IN_BAR,
            "range_ok": MIN_LAST_BAR_RANGE_PCT <= last_bar_range_pct <= MAX_LAST_BAR_RANGE_PCT,
            "vol_ratio_ok": vol_ratio >= MIN_VOL_RATIO_LAST_VS_AVG8,
            "setup_ok": setup_type == REQUIRED_SETUP,
        }

        metrics = {
            "ret_1_pct": round(ret_1_pct, 4),
            "ret_5_pct": round(ret_5_pct, 4),
            "pullback_pct": round(pullback_pct, 4),
            "recovery_from_pullback_pct": round(recovery_from_pullback_pct, 4),
            "cpib": round(cpib, 4),
            "last_bar_range_pct": round(last_bar_range_pct, 4),
            "vol_ratio": round(vol_ratio, 4),
            "last_close": round(last_c, 10),
            "setup_type": setup_type,
            "high_8": round(high_8, 10),
        }

        failed = [k for k, v in checks.items() if not v]
        if failed:
            return None, {
                "reject_reason": "failed_checks",
                "failed_checks": failed,
                "checks": checks,
                "metrics": metrics,
            }

        signal_score = (
            5.0
            + min(max(ret_5_pct, 0.0), 5.0) * 1.4
            + min(max(recovery_from_pullback_pct, 0.0), 2.0) * 2.0
            + max(0.0, cpib - 0.5) * 8.0
            + min(vol_ratio, 2.5) * 1.6
            - (row["spread_pct"] * 10.0)
        )

        signal = {
            "symbol": sym,
            "score": round(signal_score, 4),
            "entry_price_hint": last_c,
            "setup_type": setup_type,
            "scanner": {
                "pct24": round(row["pct24"], 4),
                "spread_pct": round(row["spread_pct"], 4),
                "quote_vol": round(row["quote_vol"], 2),
                "scanner_score": round(row["scanner_score"], 4),
            },
            "metrics": metrics,
            "regime": regime,
        }
        return signal, None

    # -------------------------
    # Profit secured and continuation logic
    # -------------------------
    def _compute_secured_stop_price(self, pos: Position) -> float:
        if pos.entry_price <= 0 or pos.peak_price <= 0:
            return 0.0

        peak_based_stop = pos.peak_price * (1.0 - TRAIL_BUFFER_PCT / 100.0)

        peak_pnl_pct = pct_change(pos.entry_price, pos.peak_price)
        keep_profit_pct = max(MIN_SECURED_PROFIT_PCT, peak_pnl_pct - PROFIT_GIVEBACK_PCT)
        profit_based_stop = pos.entry_price * (1.0 + (keep_profit_pct / 100.0))

        # whichever is first on the way down = higher price
        return max(peak_based_stop, profit_based_stop)

    async def _continuation_confirmation(self, sym: str, pos: Position, current_price: float) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "symbol": sym,
            "hold": False,
            "score": 0,
            "checks": {},
            "metrics": {},
            "reason": "unknown",
        }

        # Need recent 1m bars for continuation check
        rows_1m = await self.fetch_ohlcv_rows(sym, timeframe="1m", limit=max(6, TRAIL_CONFIRM_MIN_1M_BARS + 2))
        if not rows_1m or len(rows_1m) < 5:
            result["reason"] = "no_1m_data"
            return result

        # Prefer closed candles for color checks
        last_closed = rows_1m[-2]
        prev_closed = rows_1m[-3]
        prev2_closed = rows_1m[-4]

        last_green = candle_green(last_closed)
        last2_red = candle_red(prev_closed) and candle_red(last_closed)

        trail_buffer_px = pos.secured_stop_price * (1.0 + TRAIL_RECLAIM_BUFFER_PCT / 100.0)
        above_trail_buffer = current_price >= trail_buffer_px if pos.secured_stop_price > 0 else False

        # Volume momentum proxy using last 2 closed 1m bars vs prior 2
        v_last2 = safe_float(prev_closed[5]) + safe_float(last_closed[5])
        v_prior2 = safe_float(prev2_closed[5]) + safe_float(rows_1m[-5][5]) if len(rows_1m) >= 6 else max(1e-12, safe_float(prev2_closed[5]))
        vol_ratio = (v_last2 / v_prior2) if v_prior2 > 0 else 0.0

        score = 0
        if last_green:
            score += 1
        if not last2_red:
            score += 1
        if above_trail_buffer:
            score += 1
        if vol_ratio >= CONT_HOLD_MIN_VOL_RATIO_LAST2_VS_PRIOR2:
            score += 1

        five_m_not_bearish = True
        if CONT_HOLD_REQUIRE_5M_NOT_BEARISH:
            rows_5m = await self.fetch_ohlcv_rows(sym, timeframe="5m", limit=4)
            if not rows_5m or len(rows_5m) < 3:
                five_m_not_bearish = False
            else:
                last_5m_closed = rows_5m[-2]
                o5 = safe_float(last_5m_closed[1])
                c5 = safe_float(last_5m_closed[4])
                five_m_not_bearish = c5 >= o5
            if five_m_not_bearish:
                score += 1

        checks = {
            "last_1m_green": last_green,
            "not_last2_red": (not last2_red),
            "above_trail_buffer": above_trail_buffer,
            "vol_ratio_ok": vol_ratio >= CONT_HOLD_MIN_VOL_RATIO_LAST2_VS_PRIOR2,
            "five_m_not_bearish": five_m_not_bearish,
        }

        required_ok = True
        if CONT_HOLD_REQUIRE_LAST_1M_GREEN and not checks["last_1m_green"]:
            required_ok = False
        if CONT_HOLD_REQUIRE_NOT_LAST2_RED and not checks["not_last2_red"]:
            required_ok = False
        if CONT_HOLD_REQUIRE_ABOVE_TRAIL_BUFFER and not checks["above_trail_buffer"]:
            required_ok = False
        if CONT_HOLD_REQUIRE_5M_NOT_BEARISH and not checks["five_m_not_bearish"]:
            required_ok = False

        hold = required_ok and (score >= CONT_HOLD_MIN_SCORE_TO_HOLD)

        result["hold"] = hold
        result["score"] = score
        result["checks"] = checks
        result["metrics"] = {
            "current_price": current_price,
            "secured_stop_price": pos.secured_stop_price,
            "trail_buffer_px": trail_buffer_px,
            "vol_ratio_last2_vs_prior2": round(vol_ratio, 4),
        }
        result["reason"] = "continuation_strong" if hold else "continuation_weak"
        return result

    # -------------------------
    # Execution
    # -------------------------
    async def can_trade_more(self) -> bool:
        return len(self.positions) < MAX_POSITIONS

    async def buy_symbol(self, signal: Dict[str, Any]) -> bool:
        sym = signal["symbol"]

        if sym in self.positions:
            return False

        cool_rem = self._cooldown_remaining_s(sym)
        if cool_rem > 0:
            log.info(f"BUY BLOCKED cooldown | {sym} | remaining={cool_rem:.1f}s")
            return False

        if not await self.can_trade_more():
            return False

        last_price = safe_float(signal.get("entry_price_hint"))
        if last_price <= 0:
            return False

        try:
            market = self.ex.market(sym)
            raw_amount = USD_PER_TRADE / last_price
            amount_str = self.ex.amount_to_precision(sym, raw_amount)
            amount = float(amount_str)
        except Exception as e:
            log.warning(f"Failed sizing for {sym}: {e}")
            return False

        try:
            limits = (market or {}).get("limits", {}) if isinstance(market, dict) else {}
            min_amt = safe_float(((limits.get("amount") or {}).get("min")), 0.0)
            if min_amt > 0 and amount < min_amt:
                self.log_reject(sym, "order_below_min_amount", {
                    "amount": amount,
                    "min_amount": min_amt
                }, signal=signal)
                return False
        except Exception:
            pass

        try:
            log.info(
                f"BUY {sym} | score={signal['score']:.2f} | "
                f"chg24={signal['scanner']['pct24']:.2f}% | spread={signal['scanner']['spread_pct']:.3f}% | "
                f"qv={signal['scanner']['quote_vol']:.0f}"
            )

            self.last_progress_note = f"create_market_buy_order_{sym}"
            order = await self.ex.create_market_buy_order(sym, amount)
            entry_price = safe_float(order.get("average")) or safe_float(order.get("price")) or last_price

            self.positions[sym] = Position(
                symbol=sym,
                amount=amount,
                entry_price=entry_price,
                entry_time=time.time(),
                peak_price=entry_price,
                trail_active=False,
                break_even_armed=False,
                strategy="v3_4_momentum",
                scanner_change_pct=safe_float(signal["scanner"]["pct24"]),
                scanner_spread_pct=safe_float(signal["scanner"]["spread_pct"]),
                scanner_quote_vol=safe_float(signal["scanner"]["quote_vol"]),
                entry_signal_score=safe_float(signal["score"]),
                profit_secured=False,
                secured_stop_price=0.0,
                trail_touch_pending=False,
                trail_touch_first_ts=0.0,
                trail_touch_price=0.0,
                last_profit_secured_log_state=False,
            )
            self.save_state()

            self.log_event("buy_filled", {
                "symbol": sym,
                "entry_price": entry_price,
                "amount": amount,
                "signal": signal,
                "order": {
                    "id": order.get("id"),
                    "type": order.get("type"),
                    "side": order.get("side"),
                    "status": order.get("status"),
                }
            })

            log.info(f"Entered {sym} at {entry_price:.8f} | amount={amount} | Profit Secured: False")
            return True

        except Exception as e:
            log.warning(f"Buy failed for {sym}: {e}")
            self.log_reject(sym, "buy_failed", {"error": str(e)}, signal=signal)
            return False

    async def sell_symbol(self, sym: str, reason: str, current_price: float) -> bool:
        pos = self.positions.get(sym)
        if not pos:
            return False

        try:
            self.last_progress_note = f"create_market_sell_order_{sym}"
            order = await self.ex.create_market_sell_order(sym, pos.amount)
            exit_price = safe_float(order.get("average")) or safe_float(order.get("price")) or current_price
            pnl_pct = pct_change(pos.entry_price, exit_price)

            log.info(
                f"SELL {sym} | reason={reason} | exit={exit_price:.8f} | pnl={pnl_pct:.3f}% | "
                f"Profit Secured: {'True' if pos.profit_secured else 'False'}"
            )

            self.log_event("sell_filled", {
                "symbol": sym,
                "reason": reason,
                "entry_price": pos.entry_price,
                "exit_price": exit_price,
                "amount": pos.amount,
                "pnl_pct": round(pnl_pct, 5),
                "hold_seconds": round(time.time() - pos.entry_time, 3),
                "position": asdict(pos),
                "order": {
                    "id": order.get("id"),
                    "type": order.get("type"),
                    "side": order.get("side"),
                    "status": order.get("status"),
                }
            })

            # Separate cooldowns by sell reason
            reason_l = reason.lower()
            if "trail" in reason_l:
                self._set_rebuy_cooldown(sym, TRAIL_SELL_REBUY_COOLDOWN_S, reason="trail_sell")
            elif "hard_stop" in reason_l:
                self._set_rebuy_cooldown(sym, STOPLOSS_SELL_REBUY_COOLDOWN_S, reason="stop_loss_sell")
            else:
                self._set_rebuy_cooldown(sym, DEFAULT_REBUY_COOLDOWN_S, reason="default_sell")

            self.positions.pop(sym, None)
            self.save_state()
            return True

        except Exception as e:
            log.error(f"Sell failed for {sym}: {e}")
            self.log_reject(sym, "sell_failed", {"error": str(e), "reason": reason})
            return False

    # -------------------------
    # Core loops
    # -------------------------
    async def scan_and_buy(self) -> None:
        self.scan_counter += 1
        scan_started = time.time()

        summary = {
            "scan_id": self.scan_counter,
            "ranked": 0,
            "candidates_top20": 0,
            "prefilter_rejects": 0,
            "ohlcv_unavailable": 0,
            "setup_rejects": 0,
            "buy_attempts": 0,
            "buy_success": 0,
            "regime": "n/a",
            "elapsed_s": 0.0,
        }

        if not await self.can_trade_more():
            summary["regime"] = "max_positions_block"
            self.last_scan_summary = summary
            if LOG_SCAN_SKIP_REASONS:
                log.info("SCAN SKIP | max positions reached")
            return

        if LOG_SCAN_START_END:
            log.info(f"SCAN START | id={self.scan_counter} | universe={len(self.symbol_universe)} | positions={len(self.positions)}")

        self.last_progress_note = "scan_fetch_tickers"
        tickers = await self.fetch_tickers_chunked(self.symbol_universe)
        if not tickers:
            summary["regime"] = "no_tickers"
            self.last_scan_summary = summary
            log.warning(f"SCAN END | id={self.scan_counter} | no tickers returned")
            return

        ranked: List[Dict[str, Any]] = []
        for sym, t in tickers.items():
            if sym in self.positions:
                continue
            if self._cooldown_remaining_s(sym, scan_started) > 0:
                continue
            if not is_spot_usd_symbol(sym):
                continue
            row = self.parse_ticker_row(sym, t)
            if row is None:
                continue
            ranked.append(row)

        ranked.sort(key=lambda x: x["scanner_score"], reverse=True)
        summary["ranked"] = len(ranked)

        regime = self.build_regime(ranked)
        summary["regime"] = regime.get("label", "unknown")

        if ENABLE_REGIME_GATE and not regime.get("is_long_ok", False):
            log.info(
                f"Regime block | {regime['label']} | pos={regime['positive_count']}/{regime.get('top_n', 0)} "
                f"| avg={regime['avg_change_pct']:.2f}%"
            )
            self.log_event("regime_block", {"regime": regime, "scan_id": self.scan_counter})
            summary["elapsed_s"] = round(time.time() - scan_started, 3)
            self.last_scan_summary = summary
            if LOG_SCAN_START_END:
                log.info(
                    f"SCAN END | id={self.scan_counter} | ranked={summary['ranked']} | regime={summary['regime']} "
                    f"| buys={summary['buy_success']} | elapsed={summary['elapsed_s']:.2f}s"
                )
            return

        candidates = ranked[:20]
        summary["candidates_top20"] = len(candidates)
        rejects_logged = 0

        for row in candidates:
            if not await self.can_trade_more():
                break

            sym = row["symbol"]

            if self._cooldown_remaining_s(sym) > 0:
                continue

            pre_failed = []
            if row["quote_vol"] < MIN_QUOTE_VOL:
                pre_failed.append("quote_vol_ok")
            if MAX_QUOTE_VOL > 0 and row["quote_vol"] > MAX_QUOTE_VOL:
                pre_failed.append("quote_vol_upper_ok")
            if row["spread_pct"] > MAX_SPREAD_PCT:
                pre_failed.append("spread_ok")
            if row["pct24"] > MAX_SCANNER_CHANGE_PCT:
                pre_failed.append("change_ok")
            if row["scanner_score"] < MIN_SCANNER_SCORE:
                pre_failed.append("scanner_score_ok")

            if pre_failed:
                summary["prefilter_rejects"] += 1
                if rejects_logged < MAX_REJECTS_PER_SCAN:
                    self.log_reject(sym, "failed_prefilters", {
                        "failed_checks": pre_failed,
                        "scanner": row,
                        "regime": regime,
                        "scan_id": self.scan_counter,
                    })
                    rejects_logged += 1
                continue

            ohlcv = await self.fetch_ohlcv_rows(sym, timeframe=OHLCV_TIMEFRAME, limit=OHLCV_LIMIT)
            if ohlcv is None:
                summary["ohlcv_unavailable"] += 1
                if rejects_logged < MAX_REJECTS_PER_SCAN:
                    self.log_reject(sym, "ohlcv_unavailable", {
                        "scanner": row,
                        "regime": regime,
                        "scan_id": self.scan_counter,
                    })
                    rejects_logged += 1
                continue

            signal, reject = self.analyze_setup(sym, row, ohlcv, regime)

            if signal is None:
                summary["setup_rejects"] += 1
                if rejects_logged < MAX_REJECTS_PER_SCAN:
                    self.log_reject(sym, reject.get("reject_reason", "unknown_reject"), {
                        "reject": reject,
                        "scanner": row,
                        "regime": regime,
                        "scan_id": self.scan_counter,
                    })
                    rejects_logged += 1
                continue

            summary["buy_attempts"] += 1
            bought = await self.buy_symbol(signal)
            if bought:
                summary["buy_success"] += 1

            await asyncio.sleep(max(0.15, self.ex.rateLimit / 1000.0 if getattr(self.ex, "rateLimit", None) else 0.15))

        summary["elapsed_s"] = round(time.time() - scan_started, 3)
        self.last_scan_summary = summary

        if LOG_SCAN_START_END:
            log.info(
                "SCAN END | id=%s | ranked=%s | top20=%s | pre_rej=%s | ohlcv_miss=%s | setup_rej=%s | "
                "buy_attempts=%s | buys=%s | regime=%s | elapsed=%.2fs",
                summary["scan_id"],
                summary["ranked"],
                summary["candidates_top20"],
                summary["prefilter_rejects"],
                summary["ohlcv_unavailable"],
                summary["setup_rejects"],
                summary["buy_attempts"],
                summary["buy_success"],
                summary["regime"],
                summary["elapsed_s"],
            )

    async def manage_positions(self) -> None:
        self.risk_counter += 1
        summary = {
            "risk_id": self.risk_counter,
            "checked": 0,
            "sells": 0,
            "elapsed_s": 0.0,
        }
        started = time.time()

        if not self.positions:
            self.last_risk_summary = summary
            return

        syms = list(self.positions.keys())
        try:
            self.last_progress_note = "risk_fetch_position_tickers"
            tickers = await asyncio.wait_for(
                self.ex.fetch_tickers(syms),
                timeout=FETCH_POS_TICKERS_TIMEOUT_S
            )
        except asyncio.TimeoutError:
            log.warning("Failed to fetch position tickers: timeout")
            self.last_risk_summary = summary
            return
        except Exception as e:
            log.warning(f"Failed to fetch position tickers: {e}")
            self.last_risk_summary = summary
            return

        now = time.time()

        for sym in list(self.positions.keys()):
            pos = self.positions.get(sym)
            if not pos:
                continue

            t = tickers.get(sym, {})
            current_price = safe_float(t.get("last"), pos.entry_price)
            if current_price <= 0:
                continue

            summary["checked"] += 1
            pnl_pct = pct_change(pos.entry_price, current_price)

            if current_price > pos.peak_price:
                pos.peak_price = current_price

            if (not pos.break_even_armed) and (pnl_pct >= BREAK_EVEN_ARM_PCT):
                pos.break_even_armed = True

            if (not pos.trail_active) and (pnl_pct >= TRAIL_ACTIVATION_PCT):
                pos.trail_active = True
                log.info(f"TRAIL ON {sym} | pnl={pnl_pct:.3f}% | peak={pos.peak_price:.8f}")

            # Profit secured state and stop price update
            if pos.trail_active:
                new_secured_stop = self._compute_secured_stop_price(pos)
                if new_secured_stop > pos.secured_stop_price:
                    pos.secured_stop_price = new_secured_stop

                if not pos.profit_secured and pos.secured_stop_price > pos.entry_price:
                    pos.profit_secured = True
                    log.info(
                        f"PROFIT SECURED ACTIVE | {sym} | Profit Secured: True | pnl={pnl_pct:.3f}% | "
                        f"peak={pos.peak_price:.8f} | secured_stop={pos.secured_stop_price:.8f}"
                    )
                    self.log_event("profit_secured_activated", {
                        "symbol": sym,
                        "entry_price": pos.entry_price,
                        "peak_price": pos.peak_price,
                        "secured_stop_price": pos.secured_stop_price,
                        "pnl_pct": round(pnl_pct, 5),
                    })

            hold_minutes = (now - pos.entry_time) / 60.0
            sell_reason: Optional[str] = None

            if pnl_pct <= -HARD_STOP_LOSS_PCT:
                sell_reason = f"hard_stop_{pnl_pct:.3f}%"

            if sell_reason is None and pos.break_even_armed and pnl_pct <= 0.02:
                sell_reason = "break_even_protect"

            # Smarter trailing stop touch grace check and continuation confirmation
            if sell_reason is None and pos.trail_active and pos.secured_stop_price > 0:
                touched = current_price <= pos.secured_stop_price

                if touched and not pos.trail_touch_pending:
                    pos.trail_touch_pending = True
                    pos.trail_touch_first_ts = now
                    pos.trail_touch_price = current_price
                    log.info(
                        f"TRAIL TOUCH | {sym} | Profit Secured: {'True' if pos.profit_secured else 'False'} | "
                        f"price={current_price:.8f} | secured_stop={pos.secured_stop_price:.8f} | grace={TRAIL_TOUCH_GRACE_SECONDS:.1f}s"
                    )
                    self.log_event("trail_touch_started", {
                        "symbol": sym,
                        "price": current_price,
                        "secured_stop_price": pos.secured_stop_price,
                        "profit_secured": pos.profit_secured,
                        "grace_seconds": TRAIL_TOUCH_GRACE_SECONDS,
                    })

                elif pos.trail_touch_pending:
                    elapsed = now - pos.trail_touch_first_ts
                    if current_price > pos.secured_stop_price:
                        pos.trail_touch_pending = False
                        pos.trail_touch_first_ts = 0.0
                        pos.trail_touch_price = 0.0
                        log.info(
                            f"TRAIL TOUCH CLEARED | {sym} | reclaimed above secured stop | "
                            f"price={current_price:.8f} | stop={pos.secured_stop_price:.8f}"
                        )
                    elif elapsed >= TRAIL_TOUCH_GRACE_SECONDS:
                        cont = await self._continuation_confirmation(sym, pos, current_price)
                        if cont.get("hold", False):
                            pos.trail_touch_pending = False
                            pos.trail_touch_first_ts = 0.0
                            pos.trail_touch_price = 0.0
                            # give it a little space before another touch forces a check
                            pos.trail_touch_first_ts = 0.0
                            log.info(
                                f"TRAIL TOUCH HOLDING | {sym} | continuation strong | score={cont.get('score')} | "
                                f"Profit Secured: {'True' if pos.profit_secured else 'False'} | "
                                f"price={current_price:.8f} | secured_stop={pos.secured_stop_price:.8f} | "
                                f"checks={cont.get('checks')}"
                            )
                            self.log_event("trail_touch_holding", {
                                "symbol": sym,
                                "current_price": current_price,
                                "secured_stop_price": pos.secured_stop_price,
                                "continuation": cont,
                            })
                            # small hold extension by shifting pending off and letting risk loop continue
                            # no explicit timer needed because secured stop can be touched again next loop
                        else:
                            sell_reason = "trail_touch_confirmed_exit"
                            log.info(
                                f"TRAIL TOUCH SELLING | {sym} | continuation weak | score={cont.get('score')} | "
                                f"price={current_price:.8f} | secured_stop={pos.secured_stop_price:.8f} | checks={cont.get('checks')}"
                            )
                            self.log_event("trail_touch_selling", {
                                "symbol": sym,
                                "current_price": current_price,
                                "secured_stop_price": pos.secured_stop_price,
                                "continuation": cont,
                            })

            if sell_reason is None and hold_minutes >= MOMENTUM_FAIL_CHECK_AFTER_MIN and hold_minutes <= 6:
                drawdown_from_peak_pct = pct_change(pos.peak_price, current_price)
                if (pnl_pct < MOMENTUM_FAIL_MIN_PROFIT_PCT) and (drawdown_from_peak_pct <= -MOMENTUM_FAIL_MAX_DRAWDOWN_FROM_PEAK_PCT):
                    sell_reason = "momentum_fail_no_gain"

            if sell_reason is None and hold_minutes >= MAX_HOLD_MINUTES:
                sell_reason = "time_stop"

            if sell_reason:
                sold = await self.sell_symbol(sym, sell_reason, current_price)
                if sold:
                    summary["sells"] += 1
                    continue

            self.save_state()

        summary["elapsed_s"] = round(time.time() - started, 3)
        self.last_risk_summary = summary

    # -------------------------
    # Main
    # -------------------------
    async def run(self) -> None:
        await self.init_exchange()

        ensure_file_exists(EVENTS_JSONL)
        ensure_file_exists(REJECTS_JSONL)

        self.log_event("startup", {
            "bot": "kraken_live_execution_v3_4",
            "version": "v3_4_momentum_smart_trail",
            "config": {
                "max_positions": MAX_POSITIONS,
                "usd_per_trade": USD_PER_TRADE,
                "min_quote_vol": MIN_QUOTE_VOL,
                "max_spread_pct": MAX_SPREAD_PCT,
                "max_scanner_change_pct": MAX_SCANNER_CHANGE_PCT,
                "enable_regime_gate": ENABLE_REGIME_GATE,
                "hard_stop_loss_pct": HARD_STOP_LOSS_PCT,
                "trail_activation_pct": TRAIL_ACTIVATION_PCT,
                "trail_buffer_pct": TRAIL_BUFFER_PCT,
                "profit_giveback_pct": PROFIT_GIVEBACK_PCT,
                "trail_touch_grace_seconds": TRAIL_TOUCH_GRACE_SECONDS,
                "trail_sell_rebuy_cooldown_s": TRAIL_SELL_REBUY_COOLDOWN_S,
            }
        })

        log.info("Kraken Live Bot Started (v3_4 momentum smart trail)")
        log.info(f"Config | max_positions={MAX_POSITIONS} | usd_per_trade={USD_PER_TRADE}")
        log.info(
            f"Gates | qv>={MIN_QUOTE_VOL:.0f} | spread<={MAX_SPREAD_PCT:.3f}% | "
            f"chg24<={MAX_SCANNER_CHANGE_PCT:.2f}%"
        )
        log.info(
            f"Risk | hard_stop={HARD_STOP_LOSS_PCT:.2f}% | trail_on={TRAIL_ACTIVATION_PCT:.2f}% | "
            f"trail_buffer={TRAIL_BUFFER_PCT:.2f}% | profit_giveback={PROFIT_GIVEBACK_PCT:.2f}%"
        )
        log.info(
            f"Trail touch logic | grace={TRAIL_TOUCH_GRACE_SECONDS:.1f}s | reclaim_buffer={TRAIL_RECLAIM_BUFFER_PCT:.2f}% | "
            f"trail_rebuy_cooldown={TRAIL_SELL_REBUY_COOLDOWN_S:.0f}s"
        )

        while True:
            self.loop_counter += 1
            now = time.time()

            try:
                if now - self.last_risk_ts >= RISK_LOOP_EVERY_S:
                    self.last_progress_note = "risk_loop"
                    await self.manage_positions()
                    self.last_risk_ts = now

                if now - self.last_scan_ts >= SCAN_EVERY_S:
                    self.last_progress_note = "scan_loop"
                    await self.scan_and_buy()
                    self.last_scan_ts = now

                self.log_heartbeat()

            except Exception as e:
                log.exception(f"Main loop error: {e}")

            await asyncio.sleep(0.1)

    async def close(self) -> None:
        try:
            if self.ex is not None:
                await self.ex.close()
        except Exception:
            pass


async def _main() -> None:
    bot = KrakenV34Bot()
    try:
        await bot.run()
    finally:
        await bot.close()


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
