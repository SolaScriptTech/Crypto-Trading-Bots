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
# BOT CONFIGURATION: v3_3 HYBRID DEBUG
# =========================================================
QUOTE_CCY = "USD"

MAX_POSITIONS = 3
USD_PER_TRADE = 25.0

# Scanner gates
MIN_QUOTE_VOL = 1_500_000.0
MAX_QUOTE_VOL = 0.0                  # 0 means disabled upper cap
MAX_SCANNER_CHANGE_PCT = 3.0         # anti chase gate
MAX_SPREAD_PCT = 0.12
MIN_SCANNER_SCORE = 0.0

# Time window gate
# Set to False for testing so it scans immediately
ENABLE_TIME_WINDOW_GATE = False
TRADE_START_HOUR_UTC = 8
TRADE_END_HOUR_UTC = 16

# Regime gate
ENABLE_REGIME_GATE = True
REGIME_SAMPLE_TOP_N = 15
REGIME_MIN_POSITIVE_COUNT = 8
REGIME_MIN_AVG_CHANGE_PCT = 0.0

# Setup analysis
REQUIRED_SETUP = "continuation"
OHLCV_LIMIT = 15
OHLCV_TIMEFRAME = "1m"

MIN_RET_5_PCT = 0.60
MAX_RET_1_PCT = 0.40
MIN_PULLBACK_PCT = -1.20
MAX_PULLBACK_PCT = -0.05
MIN_RECOVERY_FROM_PULLBACK_PCT = 0.10
MIN_CLOSE_POS_IN_BAR = 0.45
MIN_LAST_BAR_RANGE_PCT = 0.05
MAX_LAST_BAR_RANGE_PCT = 2.50
MIN_VOL_RATIO_LAST_VS_AVG8 = 0.80

# Risk management
HARD_STOP_LOSS_PCT = 1.20
TRAIL_ACTIVATION_PCT = 0.80
TRAIL_BUFFER_PCT = 0.45
MAX_HOLD_MINUTES = 18
BREAK_EVEN_ARM_PCT = 0.45
MOMENTUM_FAIL_CHECK_AFTER_MIN = 2
MOMENTUM_FAIL_MIN_PROFIT_PCT = 0.10
MOMENTUM_FAIL_MAX_DRAWDOWN_FROM_PEAK_PCT = 0.45

# Polling
SCAN_EVERY_S = 3.0
RISK_LOOP_EVERY_S = 0.5
TICKER_REFRESH_CHUNK = 40

# Timeouts and debug
FETCH_TICKERS_TIMEOUT_S = 25.0
FETCH_OHLCV_TIMEOUT_S = 12.0
FETCH_POS_TICKERS_TIMEOUT_S = 10.0
HEARTBEAT_EVERY_S = 30.0
LOG_SCAN_SKIP_REASONS = True
LOG_SCAN_START_END = True
MAX_REJECTS_PER_SCAN = 10

# Files
STATE_FILE = "kraken_live_state_v3_3.json"
LOG_FILE = "kraken_live_execution_v3_3.log"
EVENTS_JSONL = "kraken_live_events_v3_3.jsonl"
REJECTS_JSONL = "kraken_live_rejects_v3_3.jsonl"

LOG_REJECTS = True

# Exchange client
HTTP_TIMEOUT_MS = 20_000
ENABLE_RATE_LIMIT = True

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
log = logging.getLogger("KrakenLiveV33")


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


def in_trade_window_utc(now_ts: Optional[float] = None) -> bool:
    if not ENABLE_TIME_WINDOW_GATE:
        return True
    dt = datetime.fromtimestamp(now_ts or time.time(), tz=timezone.utc)
    return TRADE_START_HOUR_UTC <= dt.hour < TRADE_END_HOUR_UTC


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


# =========================================================
# STATE
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
    strategy: str = "v3_3_hybrid"
    scanner_change_pct: float = 0.0
    scanner_spread_pct: float = 0.0
    scanner_quote_vol: float = 0.0
    entry_signal_score: float = 0.0


class KrakenV33Bot:
    def __init__(self) -> None:
        self.ex: Optional[ccxt.kraken] = None
        self.positions: Dict[str, Position] = {}

        self.last_scan_ts = 0.0
        self.last_risk_ts = 0.0
        self.last_heartbeat_ts = 0.0

        self.loop_counter = 0
        self.scan_counter = 0
        self.risk_counter = 0

        self.last_scan_summary: Dict[str, Any] = {}
        self.last_risk_summary: Dict[str, Any] = {}
        self.last_progress_note = "startup"

        self.markets_loaded = False
        self.symbol_universe: List[str] = []

        self.load_state()

    # -------------------------
    # State persistence
    # -------------------------
    def load_state(self) -> None:
        if not os.path.exists(STATE_FILE):
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            for sym, pos_data in data.items():
                self.positions[sym] = Position(**pos_data)
            log.info(f"Loaded {len(self.positions)} active positions from state.")
        except Exception as e:
            log.error(f"Failed to load state: {e}")

    def save_state(self) -> None:
        try:
            with open(STATE_FILE, "w", encoding="utf-8") as f:
                json.dump({sym: asdict(pos) for sym, pos in self.positions.items()}, f, indent=2)
        except Exception as e:
            log.error(f"Failed to save state: {e}")

    # -------------------------
    # Exchange setup
    # -------------------------
    async def init_exchange(self) -> None:
        api_key = os.getenv("KRAKEN_API_KEY")
        api_secret = os.getenv("KRAKEN_API_SECRET") or os.getenv("KRAKEN_PRIVATE_KEY")

        if not api_key or not api_secret:
            raise RuntimeError(
                "Missing Kraken credentials. Need KRAKEN_API_KEY and KRAKEN_API_SECRET "
                "or KRAKEN_PRIVATE_KEY"
            )

        self.ex = ccxt.kraken({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": ENABLE_RATE_LIMIT,
            "timeout": HTTP_TIMEOUT_MS,
            "options": {
                "adjustForTimeDifference": True
            },
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
    # JSONL logging helpers
    # -------------------------
    def log_event(self, event_type: str, payload: Dict[str, Any]) -> None:
        try:
            jsonl_append(EVENTS_JSONL, {
                "ts_utc": utc_now_iso(),
                "event": event_type,
                **payload,
            })
        except Exception as e:
            log.warning(f"Failed writing event JSONL: {e}")

    def log_reject(
        self,
        symbol: str,
        reason: str,
        info: Dict[str, Any],
        signal: Optional[Dict[str, Any]] = None
    ) -> None:
        if not LOG_REJECTS:
            return
        obj: Dict[str, Any] = {
            "ts_utc": utc_now_iso(),
            "event": "candidate_rejected_v3_3",
            "symbol": symbol,
            "reject_reason": reason,
            "info": info,
        }
        if signal is not None:
            obj["signal_context"] = signal
        try:
            jsonl_append(REJECTS_JSONL, obj)
        except Exception as e:
            log.warning(f"Failed writing reject JSONL: {e}")

    # -------------------------
    # Heartbeat
    # -------------------------
    def log_heartbeat(self) -> None:
        now = time.time()
        if now - self.last_heartbeat_ts < HEARTBEAT_EVERY_S:
            return
        self.last_heartbeat_ts = now

        scan_info = self.last_scan_summary or {}
        risk_info = self.last_risk_summary or {}

        log.info(
            "HEARTBEAT | loops=%s | scans=%s | risks=%s | positions=%s | note=%s | "
            "last_scan={ranked:%s pre_rej:%s setup_rej:%s buys:%s regime:%s} | "
            "last_risk={checked:%s sells:%s}",
            self.loop_counter,
            self.scan_counter,
            self.risk_counter,
            len(self.positions),
            self.last_progress_note,
            scan_info.get("ranked", 0),
            scan_info.get("prefilter_rejects", 0),
            scan_info.get("setup_rejects", 0),
            scan_info.get("buy_success", 0),
            scan_info.get("regime", "n/a"),
            risk_info.get("checked", 0),
            risk_info.get("sells", 0),
        )

        self.log_event("heartbeat", {
            "loop_counter": self.loop_counter,
            "scan_counter": self.scan_counter,
            "risk_counter": self.risk_counter,
            "positions": list(self.positions.keys()),
            "last_progress_note": self.last_progress_note,
            "last_scan_summary": scan_info,
            "last_risk_summary": risk_info,
        })

    # -------------------------
    # Market fetch helpers
    # -------------------------
    async def fetch_tickers_chunked(self, symbols: List[str]) -> Dict[str, Dict[str, Any]]:
        results: Dict[str, Dict[str, Any]] = {}
        if not symbols:
            return results

        for i in range(0, len(symbols), TICKER_REFRESH_CHUNK):
            chunk = symbols[i:i + TICKER_REFRESH_CHUNK]
            try:
                self.last_progress_note = f"fetch_tickers_chunk_{i}_{i + len(chunk) - 1}"
                tickers = await asyncio.wait_for(
                    self.ex.fetch_tickers(chunk),
                    timeout=FETCH_TICKERS_TIMEOUT_S
                )
                if isinstance(tickers, dict):
                    results.update(tickers)
            except asyncio.TimeoutError:
                log.warning(f"fetch_tickers chunk timeout {i} to {i + len(chunk) - 1}")
            except Exception as e:
                log.warning(f"fetch_tickers chunk failed {i} to {i + len(chunk) - 1}: {e}")

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

        scanner_score = (
            max(0.0, min(pct24, 8.0)) * 1.2
            + min(quote_vol / 1_000_000.0, 6.0)
            - (spread_pct * 8.0)
        )

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
                "top_n": 0,
                "is_long_ok": False,
            }

        positive_count = sum(1 for c in changes if c > 0)
        avg_change = sum(changes) / len(changes)
        is_long_ok = (
            positive_count >= REGIME_MIN_POSITIVE_COUNT and
            avg_change >= REGIME_MIN_AVG_CHANGE_PCT
        )

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

    async def fetch_ohlcv_rows(self, sym: str, limit: int = OHLCV_LIMIT) -> Optional[List[List[float]]]:
        try:
            self.last_progress_note = f"fetch_ohlcv_{sym}"
            rows = await asyncio.wait_for(
                self.ex.fetch_ohlcv(sym, timeframe=OHLCV_TIMEFRAME, limit=limit),
                timeout=FETCH_OHLCV_TIMEOUT_S
            )
            if not rows or len(rows) < 10:
                return None
            return rows
        except asyncio.TimeoutError:
            log.warning(f"fetch_ohlcv timeout for {sym}")
            return None
        except Exception as e:
            log.warning(f"fetch_ohlcv failed for {sym}: {e}")
            return None

    # -------------------------
    # Setup analysis
    # -------------------------
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
        c_5 = closes[-6]

        if min(last_c, prev_c, c_5) <= 0:
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
            "time_window_ok": in_trade_window_utc(),
            "regime_ok": (not ENABLE_REGIME_GATE) or bool(regime.get("is_long_ok", False)),
            "quote_vol_ok": row["quote_vol"] >= MIN_QUOTE_VOL and (MAX_QUOTE_VOL <= 0 or row["quote_vol"] <= MAX_QUOTE_VOL),
            "spread_ok": row["spread_pct"] <= MAX_SPREAD_PCT,
            "change_ok": row["pct24"] <= MAX_SCANNER_CHANGE_PCT,
            "scanner_score_ok": row["scanner_score"] >= MIN_SCANNER_SCORE,
            "ret_5_ok": ret_5_pct >= MIN_RET_5_PCT,
            "ret_1_ok": ret_1_pct <= MAX_RET_1_PCT,
            "pullback_ok": MIN_PULLBACK_PCT <= pullback_pct <= MAX_PULLBACK_PCT,
            "recovery_ok": recovery_from_pullback_PCT <= recovery_from_pullback_pct if False else recovery_from_pullback_pct >= MIN_RECOVERY_FROM_PULLBACK_PCT,
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
    # Trade execution
    # -------------------------
    async def can_trade_more(self) -> bool:
        return len(self.positions) < MAX_POSITIONS

    async def buy_symbol(self, signal: Dict[str, Any]) -> bool:
        sym = signal["symbol"]

        if sym in self.positions:
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
                    "min_amount": min_amt,
                }, signal=signal)
                return False
        except Exception:
            pass

        try:
            log.info(
                f"BUY {sym} | score={signal['score']:.2f} | "
                f"chg24={signal['scanner']['pct24']:.2f}% | "
                f"spread={signal['scanner']['spread_pct']:.3f}% | "
                f"qv={signal['scanner']['quote_vol']:.0f}"
            )

            self.last_progress_note = f"create_market_buy_order_{sym}"
            order = await self.ex.create_market_buy_order(sym, amount)
            entry_price = (
                safe_float(order.get("average")) or
                safe_float(order.get("price")) or
                last_price
            )

            self.positions[sym] = Position(
                symbol=sym,
                amount=amount,
                entry_price=entry_price,
                entry_time=time.time(),
                peak_price=entry_price,
                strategy="v3_3_hybrid",
                scanner_change_pct=safe_float(signal["scanner"]["pct24"]),
                scanner_spread_pct=safe_float(signal["scanner"]["spread_pct"]),
                scanner_quote_vol=safe_float(signal["scanner"]["quote_vol"]),
                entry_signal_score=safe_float(signal["score"]),
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

            log.info(f"Entered {sym} at {entry_price:.8f} | amount={amount}")
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
            exit_price = (
                safe_float(order.get("average")) or
                safe_float(order.get("price")) or
                current_price
            )
            pnl_pct = pct_change(pos.entry_price, exit_price)

            log.info(f"SELL {sym} | reason={reason} | exit={exit_price:.8f} | pnl={pnl_pct:.3f}%")

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

            self.positions.pop(sym, None)
            self.save_state()
            return True

        except Exception as e:
            log.error(f"Sell failed for {sym}: {e}")
            self.log_reject(sym, "sell_failed", {"error": str(e), "reason": reason})
            return False

    # -------------------------
    # Main scan loop
    # -------------------------
    async def scan_and_buy(self) -> None:
        self.scan_counter += 1
        started = time.time()

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

        if not in_trade_window_utc():
            summary["regime"] = "time_window_block"
            self.last_scan_summary = summary
            if LOG_SCAN_SKIP_REASONS:
                log.info("SCAN SKIP | outside trade window UTC")
            return

        if LOG_SCAN_START_END:
            log.info(
                f"SCAN START | id={self.scan_counter} | universe={len(self.symbol_universe)} | "
                f"positions={len(self.positions)}"
            )

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
                f"Regime block | {regime['label']} | pos={regime['positive_count']}/{regime['top_n']} | "
                f"avg={regime['avg_change_pct']:.2f}%"
            )
            self.log_event("regime_block", {
                "scan_id": self.scan_counter,
                "regime": regime,
            })
            summary["elapsed_s"] = round(time.time() - started, 3)
            self.last_scan_summary = summary
            if LOG_SCAN_START_END:
                log.info(
                    f"SCAN END | id={summary['scan_id']} | ranked={summary['ranked']} | regime={summary['regime']} | "
                    f"buys={summary['buy_success']} | elapsed={summary['elapsed_s']:.2f}s"
                )
            return

        candidates = ranked[:20]
        summary["candidates_top20"] = len(candidates)
        rejects_logged = 0

        for row in candidates:
            if not await self.can_trade_more():
                break

            sym = row["symbol"]

            pre_failed: List[str] = []
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
                        "scan_id": self.scan_counter,
                        "failed_checks": pre_failed,
                        "scanner": row,
                        "regime": regime,
                    })
                    rejects_logged += 1
                continue

            ohlcv = await self.fetch_ohlcv_rows(sym, OHLCV_LIMIT)
            if ohlcv is None:
                summary["ohlcv_unavailable"] += 1
                if rejects_logged < MAX_REJECTS_PER_SCAN:
                    self.log_reject(sym, "ohlcv_unavailable", {
                        "scan_id": self.scan_counter,
                        "scanner": row,
                        "regime": regime,
                    })
                    rejects_logged += 1
                continue

            signal, reject = self.analyze_setup(sym, row, ohlcv, regime)
            if signal is None:
                summary["setup_rejects"] += 1
                if rejects_logged < MAX_REJECTS_PER_SCAN:
                    self.log_reject(sym, reject.get("reject_reason", "unknown_reject"), {
                        "scan_id": self.scan_counter,
                        "reject": reject,
                        "scanner": row,
                        "regime": regime,
                    })
                    rejects_logged += 1
                continue

            summary["buy_attempts"] += 1
            bought = await self.buy_symbol(signal)
            if bought:
                summary["buy_success"] += 1

            await asyncio.sleep(
                max(0.15, (self.ex.rateLimit / 1000.0) if getattr(self.ex, "rateLimit", None) else 0.15)
            )

        summary["elapsed_s"] = round(time.time() - started, 3)
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

    # -------------------------
    # Position management loop
    # -------------------------
    async def manage_positions(self) -> None:
        self.risk_counter += 1
        started = time.time()
        summary = {
            "risk_id": self.risk_counter,
            "checked": 0,
            "sells": 0,
            "elapsed_s": 0.0,
        }

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

            if (not pos.break_even_armed) and pnl_pct >= BREAK_EVEN_ARM_PCT:
                pos.break_even_armed = True

            if (not pos.trail_active) and pnl_pct >= TRAIL_ACTIVATION_PCT:
                pos.trail_active = True
                log.info(f"TRAIL ON {sym} | pnl={pnl_pct:.3f}% | peak={pos.peak_price:.8f}")

            self.save_state()

            hold_minutes = (now - pos.entry_time) / 60.0
            sell_reason: Optional[str] = None

            if pnl_pct <= -HARD_STOP_LOSS_PCT:
                sell_reason = f"hard_stop_{pnl_pct:.3f}%"

            if sell_reason is None and pos.break_even_armed and pnl_pct <= 0.02:
                sell_reason = "break_even_protect"

            if sell_reason is None and pos.trail_active:
                trail_stop_price = pos.peak_price * (1.0 - TRAIL_BUFFER_PCT / 100.0)
                if current_price <= trail_stop_price:
                    sell_reason = "trail_stop"

            if sell_reason is None and MOMENTUM_FAIL_CHECK_AFTER_MIN <= hold_minutes <= 6:
                drawdown_from_peak_pct = pct_change(pos.peak_price, current_price)
                if (
                    pnl_pct < MOMENTUM_FAIL_MIN_PROFIT_PCT and
                    drawdown_from_peak_pct <= -MOMENTUM_FAIL_MAX_DRAWDOWN_FROM_PEAK_PCT
                ):
                    sell_reason = "momentum_fail_no_gain"

            if sell_reason is None and hold_minutes >= MAX_HOLD_MINUTES:
                sell_reason = "time_stop"

            if sell_reason:
                sold = await self.sell_symbol(sym, sell_reason, current_price)
                if sold:
                    summary["sells"] += 1

        summary["elapsed_s"] = round(time.time() - started, 3)
        self.last_risk_summary = summary

    # -------------------------
    # Main runner
    # -------------------------
    async def run(self) -> None:
        await self.init_exchange()

        ensure_file_exists(EVENTS_JSONL)
        ensure_file_exists(REJECTS_JSONL)

        self.log_event("startup", {
            "bot": "kraken_live_execution_v3_3",
            "version": "v3_3_hybrid_debug",
            "config": {
                "max_positions": MAX_POSITIONS,
                "usd_per_trade": USD_PER_TRADE,
                "min_quote_vol": MIN_QUOTE_VOL,
                "max_quote_vol": MAX_QUOTE_VOL,
                "max_scanner_change_pct": MAX_SCANNER_CHANGE_PCT,
                "max_spread_pct": MAX_SPREAD_PCT,
                "time_window_enabled": ENABLE_TIME_WINDOW_GATE,
                "trade_window_utc": [TRADE_START_HOUR_UTC, TRADE_END_HOUR_UTC],
                "regime_gate_enabled": ENABLE_REGIME_GATE,
                "required_setup": REQUIRED_SETUP,
            }
        })

        log.info("Kraken Live Bot Started (v3_3 Hybrid PoC DEBUG)")
        log.info(f"Config | max_positions={MAX_POSITIONS} | usd_per_trade={USD_PER_TRADE}")
        log.info(
            f"Gates | qv>={MIN_QUOTE_VOL:.0f} | spread<={MAX_SPREAD_PCT:.3f}% | "
            f"chg24<={MAX_SCANNER_CHANGE_PCT:.2f}% | "
            f"window={TRADE_START_HOUR_UTC}:00-{TRADE_END_HOUR_UTC}:00 UTC"
        )
        log.info(
            f"Debug | heartbeat={HEARTBEAT_EVERY_S:.1f}s | "
            f"ticker_timeout={FETCH_TICKERS_TIMEOUT_S:.1f}s | "
            f"ohlcv_timeout={FETCH_OHLCV_TIMEOUT_S:.1f}s"
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
    bot = KrakenV33Bot()
    try:
        await bot.run()
    finally:
        await bot.close()


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")
