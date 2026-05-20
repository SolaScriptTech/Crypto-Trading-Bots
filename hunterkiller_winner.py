import asyncio
import json
import logging
import os
import time
import random
from dataclasses import dataclass, asdict
from datetime import datetime
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta


TZ_PST = ZoneInfo("America/Los_Angeles")

QUOTE_CCY = "USD"

MAX_POSITIONS = 5
USD_PER_TRADE = 10.0
RESERVED_USD_BUFFER = 2.0

CONCURRENCY = 6

SCAN_EVERY_S = 2.0
RISK_LOOP_EVERY_S = 1.0

STATE_FILE = "hunterkiller_state.json"
LOG_FILE = "hunterkiller_winner.log"

CANDLE_TF = "1m"
CANDLES_LOOKBACK = 160

TOP_BY_24H = 120
TOP_BY_1H = 120

SPREAD_MAX_PCT = 2.50
MIN_DOLLAR_VOL_24H = 150_000

MIN_5M_UP_PCT = 0.15
MAX_5M_UP_PCT = 12.0

VOL_SPIKE_MULT = 1.40
BREAKOUT_LOOKBACK = 20

SCORE_MIN = 3.50

AUDITION_SECONDS = 120
AUDITION_CHECKPOINTS = [45, 75, 120]
AUDITION_MIN_GAIN_PCTS = [0.15, 0.30, 0.45]

HARD_STOP_LOSS_PCT = 1.10
MAX_HOLD_SECONDS = 28 * 60

TRAIL_ARM_PCT = 1.0
TRAIL_PCT_SMALL = 0.8
TRAIL_PCT_BIG = 1.2
TRAIL_BIG_AT_PCT = 4.0

ROTATE_ENABLED = True
ROTATE_MIN_NEW_SCORE = 5.00
ROTATE_SELL_IF_GAIN_BELOW_PCT = 0.15
ROTATE_MIN_AGE_S = 70

API_TIMEOUT_S = 15
HEARTBEAT_S = 10

EVAL_SLICE = 60


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()],
)
log = logging.getLogger("hunterkiller")


@dataclass
class Position:
    symbol: str
    base: str
    quote: str
    amount: float
    entry_price: float
    entry_ts: float
    peak_price: float
    trailing_armed: bool
    last_note: str = ""


def _now_ts() -> float:
    return time.time()


def _pst_now_str() -> str:
    return datetime.now(tz=TZ_PST).strftime("%H:%M:%S")


def atomic_write_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {"positions": {}}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"positions": {}}


def save_state(state: Dict[str, Any]) -> None:
    atomic_write_json(STATE_FILE, state)


def safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def pct(a: float, b: float) -> float:
    if b == 0:
        return 0.0
    return (a / b - 1.0) * 100.0


def is_stable_like(asset: str) -> bool:
    a = asset.upper()
    return a in {"USD", "USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDP"}


async def timed(coro, label: str, timeout_s: int = API_TIMEOUT_S, default=None):
    try:
        return await asyncio.wait_for(coro, timeout=timeout_s)
    except asyncio.TimeoutError:
        log.warning(f"{label} timeout")
        return default
    except Exception as e:
        log.warning(f"{label} failed: {e}")
        return default


async def fetch_ohlcv_df(ex: ccxt.Exchange, symbol: str, limit: int) -> Optional[pd.DataFrame]:
    ohlcv = await timed(ex.fetch_ohlcv(symbol, timeframe=CANDLE_TF, limit=limit), f"fetch_ohlcv {symbol}", default=None)
    if not ohlcv or len(ohlcv) < 40:
        return None
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
    df["close"] = pd.to_numeric(df["close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
    df = df.dropna(subset=["close"]).reset_index(drop=True)
    if len(df) < 40:
        return None
    return df


def compute_features(df: pd.DataFrame) -> Dict[str, float]:
    close = df["close"].astype(float)

    r1 = pct(float(close.iloc[-1]), float(close.iloc[-2]))
    r5 = pct(float(close.iloc[-1]), float(close.iloc[-6])) if len(close) >= 6 else 0.0
    r15 = pct(float(close.iloc[-1]), float(close.iloc[-16])) if len(close) >= 16 else 0.0
    r60 = pct(float(close.iloc[-1]), float(close.iloc[-61])) if len(close) >= 61 else 0.0

    v = df["volume"].fillna(0.0).astype(float)
    base_window = v.iloc[-21:-1] if len(v) >= 21 else v.iloc[:-1]
    v_med = float(base_window.median()) if len(base_window) else 0.0
    v_last = float(v.iloc[-1]) if len(v) else 0.0
    vol_mult = (v_last / v_med) if v_med > 0 else 0.0

    ema9 = ta.ema(close, length=9)
    ema21 = ta.ema(close, length=21)
    ema_trend = 1.0 if (ema9 is not None and ema21 is not None and float(ema9.iloc[-1]) > float(ema21.iloc[-1])) else 0.0

    macd = ta.macd(close, fast=12, slow=26, signal=9)
    hist_now = 0.0
    hist_prev = 0.0
    hist_prev2 = 0.0
    if macd is not None and not macd.empty:
        col = [c for c in macd.columns if "MACDh" in c]
        if col:
            h = macd[col[0]].fillna(0.0).astype(float).tolist()
            if len(h) >= 3:
                hist_prev2, hist_prev, hist_now = h[-3], h[-2], h[-1]

    hist_rising = 1.0 if (hist_now > hist_prev and hist_prev > hist_prev2) else 0.0
    hist_crossing_up = 1.0 if (hist_prev <= 0.0 and hist_now > 0.0) else 0.0

    lb = min(BREAKOUT_LOOKBACK, len(df) - 2)
    breakout = 0.0
    if lb >= 8:
        prior_high = float(df["high"].iloc[-(lb + 2):-2].max())
        breakout = 1.0 if float(close.iloc[-1]) >= prior_high else 0.0

    rsi = ta.rsi(close, length=14)
    rsi_now = 50.0
    if rsi is not None and not rsi.empty:
        x = safe_float(rsi.iloc[-1], 50.0)
        if x == x:
            rsi_now = x

    return {
        "r1": float(r1),
        "r5": float(r5),
        "r15": float(r15),
        "r60": float(r60),
        "vol_mult": float(vol_mult),
        "ema_trend": float(ema_trend),
        "hist_now": float(hist_now),
        "hist_rising": float(hist_rising),
        "hist_crossing_up": float(hist_crossing_up),
        "breakout": float(breakout),
        "rsi": float(rsi_now),
    }


def score_candidate(feat: Dict[str, float]) -> float:
    r5 = feat["r5"]
    r1 = feat["r1"]
    r15 = feat["r15"]
    r60 = feat["r60"]
    vm = feat["vol_mult"]
    br = feat["breakout"]
    et = feat["ema_trend"]
    hr = feat["hist_rising"]
    hx = feat["hist_crossing_up"]
    rsi = feat["rsi"]

    s_r5 = clamp((r5 - MIN_5M_UP_PCT) / (MAX_5M_UP_PCT - MIN_5M_UP_PCT), 0.0, 1.0)
    s_vm = clamp((vm - VOL_SPIKE_MULT) / 6.0, 0.0, 1.0)
    s_r1 = clamp((r1 + 0.2) / 1.7, 0.0, 1.0)
    s_r15 = clamp((r15 + 0.8) / 6.0, 0.0, 1.0)
    s_r60 = clamp((r60 + 0.5) / 10.0, 0.0, 1.0)

    hot_pen = clamp((rsi - 82.0) / 10.0, 0.0, 1.0)

    score = 0.0
    score += 1.6 * br
    score += 2.0 * s_vm
    score += 1.4 * s_r5
    score += 0.9 * s_r1
    score += 0.7 * s_r15
    score += 0.5 * s_r60
    score += 0.8 * et
    score += 1.0 * hr
    score += 0.7 * hx
    score -= 1.7 * hot_pen
    return float(score)


async def get_spread_pct(ex: ccxt.Exchange, symbol: str) -> float:
    ob = await timed(ex.fetch_order_book(symbol, limit=5), f"fetch_order_book {symbol}", default=None)
    if not ob:
        return 999.0
    bids = ob.get("bids") or []
    asks = ob.get("asks") or []
    if not bids or not asks:
        return 999.0
    bid = safe_float(bids[0][0], 0.0)
    ask = safe_float(asks[0][0], 0.0)
    if bid <= 0 or ask <= 0:
        return 999.0
    mid = (bid + ask) / 2.0
    return abs(ask - bid) / mid * 100.0


async def get_free_usd(ex: ccxt.Exchange) -> float:
    bal = await timed(ex.fetch_balance(), "fetch_balance", default=None)
    if not bal:
        return 0.0
    free = bal.get("free") or {}
    return safe_float(free.get(QUOTE_CCY), 0.0)


async def create_market_buy(ex: ccxt.Exchange, symbol: str, usd_cost: float) -> Optional[Tuple[float, float]]:
    if hasattr(ex, "createMarketBuyOrderWithCost"):
        o = await timed(ex.createMarketBuyOrderWithCost(symbol, usd_cost), f"buy_cost {symbol}", default=None)
    else:
        t = await timed(ex.fetch_ticker(symbol), f"fetch_ticker {symbol}", default=None)
        if not t:
            return None
        last = safe_float(t.get("last"), 0.0)
        if last <= 0:
            return None
        amount = usd_cost / last
        amount = float(ex.amount_to_precision(symbol, amount))
        if amount <= 0:
            return None
        o = await timed(ex.create_order(symbol, "market", "buy", amount), f"buy {symbol}", default=None)

    if not o:
        return None

    filled = safe_float(o.get("filled"), 0.0)
    avg = safe_float(o.get("average"), 0.0)
    if avg <= 0:
        t2 = await timed(ex.fetch_ticker(symbol), f"fetch_ticker2 {symbol}", default=None)
        if t2:
            avg = safe_float(t2.get("last"), 0.0)
    return (filled, avg)


async def create_market_sell(ex: ccxt.Exchange, symbol: str, amount: float) -> bool:
    try:
        amount = float(ex.amount_to_precision(symbol, amount))
        if amount <= 0:
            return False
        o = await timed(ex.create_order(symbol, "market", "sell", amount), f"sell {symbol}", default=None)
        return bool(o)
    except Exception as e:
        log.warning(f"SELL failed {symbol}: {e}")
        return False


async def build_universe(ex: ccxt.Exchange) -> List[str]:
    syms: List[str] = []
    for sym, m in ex.markets.items():
        if not m.get("active", True):
            continue
        if m.get("spot") is False:
            continue
        if (m.get("quote") or "").upper() != QUOTE_CCY:
            continue
        base = (m.get("base") or "").upper()
        if not base or is_stable_like(base):
            continue
        if "BULL" in sym or "BEAR" in sym:
            continue
        syms.append(sym)
    return syms


async def fetch_all_tickers(ex: ccxt.Exchange) -> Optional[Dict[str, Any]]:
    return await timed(ex.fetch_tickers(), "fetch_tickers all", timeout_s=API_TIMEOUT_S, default=None)


async def select_candidates(ex: ccxt.Exchange, universe: List[str]) -> List[str]:
    tickers = await fetch_all_tickers(ex)
    if not tickers:
        return []

    rows_24h: List[Tuple[str, float, float]] = []
    for s in universe:
        t = tickers.get(s) or {}
        pct24 = safe_float(t.get("percentage"), 0.0)
        qv = safe_float(t.get("quoteVolume"), 0.0)
        if qv and qv < MIN_DOLLAR_VOL_24H:
            continue
        rows_24h.append((s, pct24, qv))

    rows_24h.sort(key=lambda x: x[1], reverse=True)
    top24 = [s for s, p, qv in rows_24h[:TOP_BY_24H] if p > -10.0]

    rest = [s for s in universe if s not in top24]
    sample_rest = random.sample(rest, k=min(140, len(rest))) if rest else []
    sample = list(dict.fromkeys(top24 + sample_rest))

    sem = asyncio.Semaphore(CONCURRENCY)

    async def one(sym: str):
        async with sem:
            df = await fetch_ohlcv_df(ex, sym, limit=75)
            if df is None or len(df) < 70:
                return None
            c_now = float(df["close"].iloc[-1])
            c_60 = float(df["close"].iloc[-61])
            ch = pct(c_now, c_60)
            return (sym, ch)

    tasks = [asyncio.create_task(one(s)) for s in sample]
    out = await asyncio.gather(*tasks)
    rows_1h = [x for x in out if x is not None]
    rows_1h.sort(key=lambda x: x[1], reverse=True)
    top1h = [s for s, p in rows_1h[:TOP_BY_1H] if p > 0.2]

    merged = list(dict.fromkeys(top1h + top24))
    return merged


class HunterKillerWinner:
    def __init__(self):
        self.ex: Optional[ccxt.Exchange] = None
        self.markets: Dict[str, Any] = {}
        self.state = load_state()
        self.positions: Dict[str, Position] = {}
        self._load_positions()
        self._scan_count = 0
        self._last_heartbeat = 0.0
        self.stage = "init"

    def _load_positions(self):
        raw = self.state.get("positions") or {}
        pos: Dict[str, Position] = {}
        for sym, p in raw.items():
            try:
                pos[sym] = Position(**p)
            except Exception:
                continue
        self.positions = pos

    def _persist(self):
        self.state["positions"] = {s: asdict(p) for s, p in self.positions.items()}
        save_state(self.state)

    async def init_exchange(self):
        api_key = os.getenv("KRAKEN_API_KEY", "").strip()
        api_secret = os.getenv("KRAKEN_API_SECRET", "").strip()
        if not api_key or not api_secret:
            raise RuntimeError("Set KRAKEN_API_KEY and KRAKEN_API_SECRET")

        ex = ccxt.kraken(
            {
                "apiKey": api_key,
                "secret": api_secret,
                "enableRateLimit": True,
                "timeout": API_TIMEOUT_S * 1000,
            }
        )
        self.stage = "load_markets"
        await timed(ex.load_markets(), "load_markets", timeout_s=API_TIMEOUT_S, default=None)
        self.ex = ex
        self.markets = ex.markets
        self.stage = "ready"

    async def close(self):
        try:
            if self.ex:
                await self.ex.close()
        except Exception:
            pass

    def can_open_more(self) -> bool:
        return len(self.positions) < MAX_POSITIONS

    async def scan_and_buy(self):
        if not self.ex:
            return

        self._scan_count += 1
        t0 = time.monotonic()

        diag = {"spread": 0, "r5": 0, "vol": 0, "score": 0, "ok": 0}

        self.stage = "build_universe"
        universe = await build_universe(self.ex)
        if not universe:
            log.info("scan diag universe=0")
            return

        self.stage = "select_candidates"
        candidates = await select_candidates(self.ex, universe)
        if not candidates:
            log.info("scan diag candidates=0")
            return

        candidates = [s for s in candidates if s not in self.positions]
        if not candidates:
            log.info("scan diag candidates=0 after owned filter")
            return

        eval_slice = candidates[:EVAL_SLICE]
        self.stage = f"evaluate_{len(eval_slice)}"

        sem = asyncio.Semaphore(CONCURRENCY)

        async def evaluate(sym: str):
            async with sem:
                spr = await get_spread_pct(self.ex, sym)
                if spr > SPREAD_MAX_PCT:
                    diag["spread"] += 1
                    return None

                df = await fetch_ohlcv_df(self.ex, sym, limit=CANDLES_LOOKBACK)
                if df is None:
                    return None

                feat = compute_features(df)

                if feat["r5"] < MIN_5M_UP_PCT or feat["r5"] > MAX_5M_UP_PCT:
                    diag["r5"] += 1
                    return None

                if feat["vol_mult"] < VOL_SPIKE_MULT and feat["r5"] < 1.2:
                    diag["vol"] += 1
                    return None

                score = score_candidate(feat)
                if score < SCORE_MIN:
                    diag["score"] += 1
                    return None

                diag["ok"] += 1
                last = float(df["close"].iloc[-1])
                return (sym, score, spr, last, feat)

        tasks = [asyncio.create_task(evaluate(s)) for s in eval_slice]
        results = await asyncio.gather(*tasks)

        dt = time.monotonic() - t0
        log.info(
            f"scan diag dt={dt:.1f}s eval={len(eval_slice)} ok={diag['ok']} "
            f"blocked spread={diag['spread']} r5={diag['r5']} vol={diag['vol']} score={diag['score']}"
        )

        picks = [r for r in results if r is not None]
        if not picks:
            self.stage = "scan_done_no_pick"
            return

        picks.sort(key=lambda x: x[1], reverse=True)
        sym, score, spr, last, feat = picks[0]

        self.stage = "balance"
        free_usd = await get_free_usd(self.ex)
        if free_usd < (USD_PER_TRADE + RESERVED_USD_BUFFER):
            log.info(f"skip buy low USD free={free_usd:.2f}")
            self.stage = "scan_done_low_usd"
            return

        m = self.markets.get(sym) or {}
        base = (m.get("base") or "").upper()
        quote = (m.get("quote") or "").upper()
        if not base or not quote:
            self.stage = "scan_done_bad_market"
            return

        log.info(
            f"BUY signal {sym} score={score:.2f} spread={spr:.2f}% "
            f"r5={feat['r5']:.2f}% r1={feat['r1']:.2f}% volx={feat['vol_mult']:.2f} "
            f"hist={feat['hist_now']:.6f} breakout={int(feat['breakout'])} rsi={feat['rsi']:.1f}"
        )

        self.stage = "buy"
        fill = await create_market_buy(self.ex, sym, USD_PER_TRADE)
        if not fill:
            self.stage = "scan_done_buy_fail"
            return

        filled_amt, avg_px = fill
        if filled_amt <= 0 or avg_px <= 0:
            self.stage = "scan_done_buy_zero"
            return

        pos = Position(
            symbol=sym,
            base=base,
            quote=quote,
            amount=filled_amt,
            entry_price=avg_px,
            entry_ts=_now_ts(),
            peak_price=avg_px,
            trailing_armed=False,
            last_note="entered",
        )
        self.positions[sym] = pos
        self._persist()
        self.stage = "scan_done_bought"

    async def manage_positions(self):
        if not self.ex or not self.positions:
            return

        syms = list(self.positions.keys())
        sem = asyncio.Semaphore(CONCURRENCY)

        async def manage_one(sym: str):
            async with sem:
                pos = self.positions.get(sym)
                if not pos:
                    return

                t = await timed(self.ex.fetch_ticker(sym), f"fetch_ticker {sym}", default=None)
                if not t:
                    return
                last = safe_float(t.get("last"), 0.0)
                if last <= 0:
                    return

                if last > pos.peak_price:
                    pos.peak_price = last

                gain_pct = pct(last, pos.entry_price)
                peak_gain_pct = pct(pos.peak_price, pos.entry_price)
                age = _now_ts() - pos.entry_ts

                if gain_pct <= -HARD_STOP_LOSS_PCT:
                    pos.last_note = f"hard stop {gain_pct:.2f}%"
                    await self._sell_position(sym, pos)
                    return

                if age <= AUDITION_SECONDS:
                    for cp, need in zip(AUDITION_CHECKPOINTS, AUDITION_MIN_GAIN_PCTS):
                        if age >= cp and gain_pct < need:
                            pos.last_note = f"audition fail t={int(age)}s g={gain_pct:.2f}% need={need:.2f}%"
                            await self._sell_position(sym, pos)
                            return

                if (not pos.trailing_armed) and gain_pct >= TRAIL_ARM_PCT:
                    pos.trailing_armed = True
                    pos.last_note = "trail armed"

                if age >= MAX_HOLD_SECONDS:
                    pos.last_note = f"max hold g={gain_pct:.2f}%"
                    await self._sell_position(sym, pos)
                    return

                if pos.trailing_armed:
                    trail_pct = TRAIL_PCT_BIG if peak_gain_pct >= TRAIL_BIG_AT_PCT else TRAIL_PCT_SMALL
                    stop_price = pos.peak_price * (1.0 - trail_pct / 100.0)
                    if last <= stop_price and peak_gain_pct >= 0.9:
                        pos.last_note = f"trail hit g={gain_pct:.2f}% peak={peak_gain_pct:.2f}%"
                        await self._sell_position(sym, pos)
                        return

                self.positions[sym] = pos

        self.stage = "manage_positions"
        tasks = [asyncio.create_task(manage_one(s)) for s in syms]
        await asyncio.gather(*tasks)
        self._persist()
        self.stage = "manage_done"

    async def _sell_position(self, sym: str, pos: Position):
        ok = await create_market_sell(self.ex, sym, pos.amount)
        if ok:
            log.info(f"SELL {sym} | {pos.last_note} | entry={pos.entry_price:.10f} peak={pos.peak_price:.10f}")
            self.positions.pop(sym, None)
            self._persist()
        else:
            log.warning(f"SELL failed {sym} will retry next loop")

    async def run(self):
        await self.init_exchange()
        log.info("HunterKiller Winner started")
        log.info(f"Max positions {MAX_POSITIONS}, USD per trade {USD_PER_TRADE}")

        last_scan = 0.0
        last_risk = 0.0

        while True:
            now = _now_ts()

            if now - self._last_heartbeat >= HEARTBEAT_S:
                log.info(f"heartbeat positions={len(self.positions)} time_pst={_pst_now_str()} stage={self.stage}")
                self._last_heartbeat = now

            if now - last_risk >= RISK_LOOP_EVERY_S:
                await self.manage_positions()
                last_risk = now

            if now - last_scan >= SCAN_EVERY_S:
                await self.scan_and_buy()
                last_scan = now

            await asyncio.sleep(0.15)


async def main():
    bot = HunterKillerWinner()
    try:
        await bot.run()
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
