import asyncio
import json
import logging
import math
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Any, Dict, List, Optional, Tuple

import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta


# ==========================================
# Configuration
# ==========================================

TZ_PST = ZoneInfo("America/Los_Angeles")

ENTRY_WINDOW_START_HOUR = 3
ENTRY_WINDOW_END_HOUR = 12

QUOTE_CCY = "USD"

MAX_POSITIONS = 5
MAX_USD_PER_TRADE = 10.0
RESERVED_USD_BUFFER = 2.0  # keep a couple dollars free for fees, rounding, etc

CONCURRENCY = 6
SCAN_EVERY_S = 4.0
RISK_LOOP_EVERY_S = 2.0

STATE_FILE = "hunterkiller_state.json"
LOG_FILE = "hunterkiller_gainers.log"

# Market data windows
CANDLE_TF = "1m"
CANDLES_LOOKBACK = 120  # minutes

# Candidate selection
TOP_BY_24H = 80
TOP_BY_1H = 80
SPREAD_MAX_PCT = 0.45  # skip illiquid
MIN_DOLLAR_VOL_24H = 250_000  # skip tiny markets (approx, based on quoteVolume if available)

# Entry gating
MIN_5M_UP_PCT = 0.7
MAX_5M_UP_PCT = 8.0  # do not chase already exploding
VOL_SPIKE_MULT = 2.0
BREAKOUT_LOOKBACK = 20  # minutes

# Exit logic
HARD_STOP_LOSS_PCT = 1.2
AUDITION_SECONDS = 180  # first 3 minutes are make or break
AUDITION_MIN_GAIN_PCT = 0.35  # must show life quickly
MAX_HOLD_SECONDS = 30 * 60

TRAIL_ARM_PCT = 1.2  # once we are up this much, arm trailing
TRAIL_PCT_SMALL = 0.85
TRAIL_PCT_BIG = 1.25
TRAIL_BIG_AT_PCT = 4.0

# Momentum stall exit (after audition)
STALL_LOOKBACK = 3  # minutes


# ==========================================
# Logging
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("hunterkiller")


# ==========================================
# State
# ==========================================

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


def _pst_now() -> datetime:
    return datetime.now(tz=TZ_PST)


def in_entry_window() -> bool:
    now = _pst_now()
    h = now.hour
    # window: [start, end)
    return (h >= ENTRY_WINDOW_START_HOUR) and (h < ENTRY_WINDOW_END_HOUR)


def atomic_write_json(path: str, data: Any) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)
    os.replace(tmp, path)


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_FILE):
        return {"positions": {}, "last_buy_ts": 0.0}
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {"positions": {}, "last_buy_ts": 0.0}


def save_state(state: Dict[str, Any]) -> None:
    atomic_write_json(STATE_FILE, state)


# ==========================================
# Helpers
# ==========================================

def is_stable_like(asset: str) -> bool:
    a = asset.upper()
    return a in {"USD", "USDT", "USDC", "DAI", "TUSD", "FDUSD", "USDP"} or "USD" in a and a not in {"SAND", "LUSD"}  # cheap guard


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


async def safe_call(coro, label: str, default=None):
    try:
        return await coro
    except Exception as e:
        log.warning(f"{label} failed: {e}")
        return default


async def fetch_ohlcv_df(ex: ccxt.Exchange, symbol: str, limit: int) -> Optional[pd.DataFrame]:
    ohlcv = await safe_call(ex.fetch_ohlcv(symbol, timeframe=CANDLE_TF, limit=limit), f"fetch_ohlcv {symbol}", default=None)
    if not ohlcv or len(ohlcv) < 30:
        return None
    df = pd.DataFrame(ohlcv, columns=["ts", "open", "high", "low", "close", "volume"])
    df["ts"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return df


def compute_features(df: pd.DataFrame) -> Dict[str, float]:
    d = df.copy()
    d["close"] = pd.to_numeric(d["close"], errors="coerce")
    d["volume"] = pd.to_numeric(d["volume"], errors="coerce")

    # Returns
    close = d["close"]
    r1 = pct(close.iloc[-1], close.iloc[-2])
    r3 = pct(close.iloc[-1], close.iloc[-4]) if len(close) >= 4 else 0.0
    r5 = pct(close.iloc[-1], close.iloc[-6]) if len(close) >= 6 else 0.0
    r15 = pct(close.iloc[-1], close.iloc[-16]) if len(close) >= 16 else 0.0

    # Volume spike vs median of prior 20 minutes
    v = d["volume"]
    base_window = v.iloc[-21:-1] if len(v) >= 21 else v.iloc[:-1]
    v_med = float(base_window.median()) if len(base_window) else 0.0
    v_last = float(v.iloc[-1]) if len(v) else 0.0
    vol_mult = (v_last / v_med) if v_med > 0 else 0.0

    # Trend: EMA9 vs EMA21 slope
    d["ema9"] = ta.ema(d["close"], length=9)
    d["ema21"] = ta.ema(d["close"], length=21)
    ema9 = d["ema9"].iloc[-1]
    ema21 = d["ema21"].iloc[-1]
    ema_trend = 1.0 if (ema9 is not None and ema21 is not None and ema9 > ema21) else 0.0

    # MACD histogram rising
    macd = ta.macd(d["close"], fast=12, slow=26, signal=9)
    if macd is None or macd.empty:
        hist = [0.0, 0.0, 0.0]
    else:
        col = [c for c in macd.columns if "MACDh" in c]
        if not col:
            hist = [0.0, 0.0, 0.0]
        else:
            h = macd[col[0]].fillna(0.0).astype(float).tolist()
            hist = h[-3:] if len(h) >= 3 else (h + [0.0, 0.0])[-3:]

    hist_now = float(hist[-1])
    hist_prev = float(hist[-2])
    hist_prev2 = float(hist[-3])

    hist_rising = 1.0 if (hist_now > hist_prev and hist_prev > hist_prev2) else 0.0
    hist_crossing_up = 1.0 if (hist_prev <= 0.0 and hist_now > 0.0) else 0.0

    # Breakout: close above max high of prior N minutes
    lb = min(BREAKOUT_LOOKBACK, len(d) - 2)
    if lb >= 5:
        prior_high = float(d["high"].iloc[-(lb + 2):-2].max())
        breakout = 1.0 if float(close.iloc[-1]) >= prior_high else 0.0
    else:
        breakout = 0.0

    # RSI sanity (avoid already overheated)
    rsi = ta.rsi(d["close"], length=14)
    rsi_now = float(rsi.iloc[-1]) if rsi is not None and not rsi.empty else 50.0

    return {
        "r1": float(r1),
        "r3": float(r3),
        "r5": float(r5),
        "r15": float(r15),
        "vol_mult": float(vol_mult),
        "ema_trend": float(ema_trend),
        "hist_now": float(hist_now),
        "hist_rising": float(hist_rising),
        "hist_crossing_up": float(hist_crossing_up),
        "breakout": float(breakout),
        "rsi": float(rsi_now),
    }


def momentum_score(feat: Dict[str, float]) -> float:
    # Score is designed to catch the first leg of a move:
    # breakout + volume + positive acceleration + histogram rising.
    # It also tries not to chase a coin already in full melt-up.
    r5 = feat["r5"]
    r1 = feat["r1"]
    r15 = feat["r15"]
    vm = feat["vol_mult"]
    br = feat["breakout"]
    ht = feat["ema_trend"]
    hr = feat["hist_rising"]
    hx = feat["hist_crossing_up"]
    rsi = feat["rsi"]

    # Normalize
    s_r5 = clamp((r5 - MIN_5M_UP_PCT) / (MAX_5M_UP_PCT - MIN_5M_UP_PCT), 0.0, 1.0)
    s_vm = clamp((vm - VOL_SPIKE_MULT) / 6.0, 0.0, 1.0)
    s_r1 = clamp((r1 + 0.2) / 1.5, 0.0, 1.0)  # prefer positive 1m
    s_r15 = clamp((r15 + 1.0) / 6.0, 0.0, 1.0)

    # Overheat penalty if RSI is too high already
    hot_pen = clamp((rsi - 78.0) / 10.0, 0.0, 1.0)

    score = 0.0
    score += 2.2 * br
    score += 2.0 * s_vm
    score += 1.6 * s_r5
    score += 1.0 * s_r1
    score += 0.8 * s_r15
    score += 1.0 * ht
    score += 1.0 * hr
    score += 0.8 * hx
    score -= 1.8 * hot_pen
    return float(score)


async def get_spread_pct(ex: ccxt.Exchange, symbol: str) -> float:
    ob = await safe_call(ex.fetch_order_book(symbol, limit=5), f"fetch_order_book {symbol}", default=None)
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
    bal = await safe_call(ex.fetch_balance(), "fetch_balance", default=None)
    if not bal:
        return 0.0
    free = bal.get("free") or {}
    return safe_float(free.get(QUOTE_CCY), 0.0)


async def create_market_buy(ex: ccxt.Exchange, market: Dict[str, Any], symbol: str, usd_cost: float) -> Optional[Tuple[float, float]]:
    # Returns (filled_amount, avg_price)
    # Prefer createMarketBuyOrderWithCost if available, else estimate amount.
    try:
        if hasattr(ex, "createMarketBuyOrderWithCost"):
            o = await ex.createMarketBuyOrderWithCost(symbol, usd_cost)
        else:
            t = await ex.fetch_ticker(symbol)
            last = safe_float(t.get("last"), 0.0)
            if last <= 0:
                return None
            amount = usd_cost / last
            amount = float(ex.amount_to_precision(symbol, amount))
            if amount <= 0:
                return None
            o = await ex.create_order(symbol, "market", "buy", amount)

        filled = safe_float(o.get("filled"), 0.0)
        avg = safe_float(o.get("average"), 0.0)
        if avg <= 0:
            # fallback to last
            t2 = await ex.fetch_ticker(symbol)
            avg = safe_float(t2.get("last"), 0.0)
        return (filled, avg)
    except Exception as e:
        log.warning(f"BUY failed {symbol}: {e}")
        return None


async def create_market_sell(ex: ccxt.Exchange, symbol: str, amount: float) -> bool:
    try:
        amount = float(ex.amount_to_precision(symbol, amount))
        if amount <= 0:
            return False
        await ex.create_order(symbol, "market", "sell", amount)
        return True
    except Exception as e:
        log.warning(f"SELL failed {symbol}: {e}")
        return False


# ==========================================
# Candidate selection
# ==========================================

async def build_universe(ex: ccxt.Exchange) -> List[str]:
    markets = ex.markets
    symbols: List[str] = []
    for sym, m in markets.items():
        if not m.get("active", True):
            continue
        if m.get("spot") is False:
            continue
        if m.get("quote") != QUOTE_CCY:
            continue
        base = (m.get("base") or "").upper()
        quote = (m.get("quote") or "").upper()
        if not base or not quote:
            continue
        if is_stable_like(base):
            continue
        # filter weird stuff
        if ".S" in sym or "BULL" in sym or "BEAR" in sym:
            continue
        symbols.append(sym)
    return symbols


async def top_candidates(ex: ccxt.Exchange, universe: List[str]) -> List[str]:
    # Use two lenses:
    # 1) 24h percent change (Kraken ticker)
    # 2) 1h percent change computed from 1m candles
    tickers = await safe_call(ex.fetch_tickers(universe), "fetch_tickers", default={})
    if not tickers:
        return []

    rows_24h = []
    for s in universe:
        t = tickers.get(s) or {}
        pct24 = safe_float(t.get("percentage"), 0.0)
        qv = safe_float(t.get("quoteVolume"), 0.0)
        # Some markets may not report quoteVolume. Keep them but downrank by volume later.
        if qv and qv < MIN_DOLLAR_VOL_24H:
            continue
        rows_24h.append((s, pct24, qv))

    rows_24h.sort(key=lambda x: x[1], reverse=True)
    top24 = [s for s, p, qv in rows_24h[:TOP_BY_24H] if p > -5.0]

    # 1h: sample candles for the top 24h set plus a random slice of the rest to catch "just starting"
    sample = list(dict.fromkeys(top24 + random.sample(universe, k=min(120, len(universe)))))
    # Limit fetch_ohlcv calls by batching with concurrency
    sem = asyncio.Semaphore(CONCURRENCY)

    async def one(sym: str):
        async with sem:
            df = await fetch_ohlcv_df(ex, sym, limit=70)
            if df is None or len(df) < 65:
                return None
            c_now = float(df["close"].iloc[-1])
            c_60 = float(df["close"].iloc[-61])
            change_1h = pct(c_now, c_60)
            return (sym, change_1h)

    tasks = [asyncio.create_task(one(s)) for s in sample]
    out = await asyncio.gather(*tasks)
    rows_1h = [x for x in out if x is not None]
    rows_1h.sort(key=lambda x: x[1], reverse=True)
    top1h = [s for s, p in rows_1h[:TOP_BY_1H] if p > 0.5]

    # Union and keep order: top1h first, then top24
    merged = list(dict.fromkeys(top1h + top24))
    return merged


# ==========================================
# Core engine
# ==========================================

class HunterKiller:
    def __init__(self):
        self.ex: Optional[ccxt.Exchange] = None
        self.markets: Dict[str, Any] = {}
        self.state: Dict[str, Any] = load_state()
        self.positions: Dict[str, Position] = {}
        self._load_positions_from_state()

    def _load_positions_from_state(self):
        raw = self.state.get("positions") or {}
        out: Dict[str, Position] = {}
        for sym, p in raw.items():
            try:
                out[sym] = Position(**p)
            except Exception:
                continue
        self.positions = out

    def _persist_positions(self):
        self.state["positions"] = {s: asdict(p) for s, p in self.positions.items()}
        save_state(self.state)

    async def init_exchange(self):
        api_key = os.getenv("KRAKEN_API_KEY", "").strip()
        api_secret = os.getenv("KRAKEN_API_SECRET", "").strip()
        if not api_key or not api_secret:
            raise RuntimeError("Set KRAKEN_API_KEY and KRAKEN_API_SECRET in environment variables")

        ex = ccxt.kraken({
            "apiKey": api_key,
            "secret": api_secret,
            "enableRateLimit": True,
        })
        await ex.load_markets()
        self.ex = ex
        self.markets = ex.markets

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
        if not in_entry_window():
            return
        if not self.can_open_more():
            return

        universe = await build_universe(self.ex)
        if not universe:
            return

        candidates = await top_candidates(self.ex, universe)
        if not candidates:
            return

        # Do not re-buy what we already hold
        candidates = [s for s in candidates if s not in self.positions]
        if not candidates:
            return

        sem = asyncio.Semaphore(CONCURRENCY)

        async def evaluate(sym: str):
            async with sem:
                spr = await get_spread_pct(self.ex, sym)
                if spr > SPREAD_MAX_PCT:
                    return None

                df = await fetch_ohlcv_df(self.ex, sym, limit=CANDLES_LOOKBACK)
                if df is None:
                    return None

                feat = compute_features(df)
                # Gate: needs early momentum, not a slow grind
                if feat["r5"] < MIN_5M_UP_PCT or feat["r5"] > MAX_5M_UP_PCT:
                    return None
                if feat["vol_mult"] < VOL_SPIKE_MULT:
                    return None

                score = momentum_score(feat)
                # Strong bias to breakout and rising histogram
                if score < 5.2:
                    return None

                last = float(df["close"].iloc[-1])
                return (sym, score, spr, last, feat)

        # Evaluate top chunk only for speed
        chunk = candidates[:120]
        tasks = [asyncio.create_task(evaluate(s)) for s in chunk]
        results = await asyncio.gather(*tasks)

        picks = [r for r in results if r is not None]
        if not picks:
            return

        picks.sort(key=lambda x: (x[1], -x[2]), reverse=True)
        best = picks[0]
        sym, score, spr, last, feat = best

        # Check USD balance
        free_usd = await get_free_usd(self.ex)
        if free_usd < (MAX_USD_PER_TRADE + RESERVED_USD_BUFFER):
            log.info(f"Skip buy (low USD). free_usd={free_usd:.2f}")
            return

        # Place buy
        m = self.markets.get(sym) or {}
        base = (m.get("base") or "").upper()
        quote = (m.get("quote") or "").upper()
        if not base or not quote:
            return

        log.info(f"BUY signal {sym} score={score:.2f} spread={spr:.3f}% r5={feat['r5']:.2f}% volx={feat['vol_mult']:.2f} hist={feat['hist_now']:.6f}")

        fill = await create_market_buy(self.ex, m, sym, MAX_USD_PER_TRADE)
        if not fill:
            return
        filled_amt, avg_px = fill
        if filled_amt <= 0 or avg_px <= 0:
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
        self._persist_positions()

    async def manage_positions(self):
        if not self.ex or not self.positions:
            return

        # Copy list to avoid mutation during loop
        syms = list(self.positions.keys())
        sem = asyncio.Semaphore(CONCURRENCY)

        async def manage_one(sym: str):
            async with sem:
                pos = self.positions.get(sym)
                if not pos:
                    return

                t = await safe_call(self.ex.fetch_ticker(sym), f"fetch_ticker {sym}", default=None)
                if not t:
                    return
                last = safe_float(t.get("last"), 0.0)
                if last <= 0:
                    return

                # Update peak
                if last > pos.peak_price:
                    pos.peak_price = last

                gain_pct = pct(last, pos.entry_price)
                peak_gain_pct = pct(pos.peak_price, pos.entry_price)
                age = _now_ts() - pos.entry_ts

                # Arm trailing once it moves
                if (not pos.trailing_armed) and gain_pct >= TRAIL_ARM_PCT:
                    pos.trailing_armed = True
                    pos.last_note = "trail armed"

                # HARD stop
                if gain_pct <= -HARD_STOP_LOSS_PCT:
                    pos.last_note = f"hard stop {gain_pct:.2f}%"
                    await self._sell_position(sym, pos)
                    return

                # Audition: if it is not moving quickly, dump it
                if age <= AUDITION_SECONDS:
                    if gain_pct < AUDITION_MIN_GAIN_PCT and age >= 60:
                        pos.last_note = f"audition fail {gain_pct:.2f}%"
                        await self._sell_position(sym, pos)
                        return

                # Max hold
                if age >= MAX_HOLD_SECONDS:
                    pos.last_note = f"max hold {gain_pct:.2f}%"
                    await self._sell_position(sym, pos)
                    return

                # Stall detection (needs candles)
                if age > AUDITION_SECONDS:
                    df = await fetch_ohlcv_df(self.ex, sym, limit=60)
                    if df is not None and len(df) >= 30:
                        d = df.copy()
                        d["ema9"] = ta.ema(d["close"], length=9)
                        ema9 = d["ema9"].fillna(method="ffill").fillna(0.0).astype(float)
                        if len(ema9) >= (STALL_LOOKBACK + 2):
                            slope = float(ema9.iloc[-1] - ema9.iloc[-(STALL_LOOKBACK + 1)])
                        else:
                            slope = 0.0

                        macd = ta.macd(d["close"], fast=12, slow=26, signal=9)
                        hist_decline = False
                        if macd is not None and not macd.empty:
                            col = [c for c in macd.columns if "MACDh" in c]
                            if col:
                                h = macd[col[0]].fillna(0.0).astype(float).tolist()
                                if len(h) >= 3:
                                    hist_decline = (h[-1] < h[-2] < h[-3])

                        # If slope is non positive and histogram is declining, treat as reversal starting
                        if slope <= 0 and hist_decline and peak_gain_pct >= 1.0:
                            pos.last_note = f"stall exit gain={gain_pct:.2f}% peak={peak_gain_pct:.2f}%"
                            await self._sell_position(sym, pos)
                            return

                # Trailing stop
                if pos.trailing_armed:
                    trail_pct = TRAIL_PCT_BIG if peak_gain_pct >= TRAIL_BIG_AT_PCT else TRAIL_PCT_SMALL
                    stop_price = pos.peak_price * (1.0 - trail_pct / 100.0)
                    if last <= stop_price and peak_gain_pct >= 0.9:
                        pos.last_note = f"trail hit gain={gain_pct:.2f}% peak={peak_gain_pct:.2f}%"
                        await self._sell_position(sym, pos)
                        return

                # Persist updated peak and flags occasionally
                self.positions[sym] = pos

        tasks = [asyncio.create_task(manage_one(s)) for s in syms]
        await asyncio.gather(*tasks)
        self._persist_positions()

    async def _sell_position(self, sym: str, pos: Position):
        ok = await create_market_sell(self.ex, sym, pos.amount)
        if ok:
            log.info(f"SELL {sym} | note={pos.last_note} | entry={pos.entry_price:.8f} peak={pos.peak_price:.8f}")
            self.positions.pop(sym, None)
            self._persist_positions()
        else:
            log.warning(f"SELL failed {sym} | keeping position for retry")

    async def run(self):
        await self.init_exchange()
        log.info("HunterKiller gainers started")
        log.info(f"Entry window PST: {ENTRY_WINDOW_START_HOUR}:00 to {ENTRY_WINDOW_END_HOUR}:00")

        last_scan = 0.0
        last_risk = 0.0

        while True:
            now = _now_ts()

            # Always manage risk
            if now - last_risk >= RISK_LOOP_EVERY_S:
                await self.manage_positions()
                last_risk = now

            # Scan and buy in entry window
            if now - last_scan >= SCAN_EVERY_S:
                try:
                    await self.scan_and_buy()
                except Exception as e:
                    log.warning(f"scan_and_buy error: {e}")
                last_scan = now

            await asyncio.sleep(0.25)


async def main():
    bot = HunterKiller()
    try:
        await bot.run()
    finally:
        await bot.close()


if __name__ == "__main__":
    asyncio.run(main())
