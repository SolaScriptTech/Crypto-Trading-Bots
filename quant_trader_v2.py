"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                         quant_trader.py                                      ║
║                    Main Orchestrator — Production Bot                        ║
║                                                                              ║
║  Wires all layers together into a single asyncio loop.                       ║
║                                                                              ║
║  Boot sequence (mandatory order):                                            ║
║   1. Wait 15s NTP sync                                                       ║
║   2. Load state.json — resume positions and cooldowns                        ║
║   3. Connect to Kraken, check ECP margin capability                          ║
║   4. Fetch and log all non-zero balances                                     ║
║   5. Seed candle cache via REST (1.5s apart per symbol)                      ║
║   6. Start asyncio WebSocket watchers per symbol                             ║
║   7. Start main decision loop                                                ║
║                                                                              ║
║  Loop sequence (every candle close):                                         ║
║   PPM.sense() → GTW.concoct_spell() → MCMCClassifier.classify()            ║
║   → SignalRouter.route() → ConvictionScorer.score_and_filter()              ║
║   → RME.evaluate() → process_exits() → process_entries()                   ║
║   → save_state()                                                             ║
║                                                                              ║
║  Infrastructure: asyncio single-loop, ccxt.pro WebSocket, Kraken            ║
║  Deployment: tmux new -s quant_trader → python3 quant_trader.py             ║
║  Emergency stop: touch EMERGENCY_STOP                                        ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import logging
import math
import os
import sys
import time
import datetime

# ── Load .env credentials without dotenv dependency ───────────────────────────
def _load_env(path=".env"):
    if not os.path.exists(path):
        return
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

_load_env()

import ccxt.pro as ccxtpro
import ccxt

# ── Module imports ─────────────────────────────────────────────────────────────
from Lv1_quant_trader import (
    PeterParkerModule, GandalfTheWhiteModule, EWSAlert, MCMCType, SpellName
)
from Lv2_quant_trader import MCMCClassifier, MacroRegime
from Lv3_quant_trader import SignalRouter, ConvictionScorer
from RiskEngine_quant_trader import (
    RiskManagementEngine, OpenPosition, PortfolioState,
    ExitAction, position_from_state
)


# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────────────────────

TIMEFRAME        = "1h"          # Trading timeframe — NEVER 1m
CANDLE_LIMIT     = 200           # Candles to seed on boot
SEED_DELAY_S     = 1.5           # Seconds between REST seed calls (rate limit)
COOLDOWN_MS      = 3 * 3_600_000 # 3 hours cooldown per symbol after exit
MAX_POSITIONS    = 3             # Hard cap — overridden per-MCMC by StrategyConfig
DRY_POWDER_PCT   = 0.20          # Always keep 20% cash
SIZE_HIGH_PCT    = 0.25          # conviction >= 65
SIZE_LOW_PCT     = 0.15          # conviction < 65
MIN_ORDER_USD    = 10.0          # Kraken minimum
LOOP_SLEEP_S     = 5.0           # Fast-sensor tick interval (seconds)
FIXED_TARGET_PCT = 0.025         # 2.5% fixed profit target

STATE_FILE       = "quant_trader_state.json"
STATE_TMP        = "quant_trader_state.json.tmp"
AUDIT_FILE       = "quant_trader_audit.csv"
EVENTS_FILE      = "quant_trader_events.log"
EMERGENCY_FILE   = "EMERGENCY_STOP"

# Trading universe — edit to your targets
SYMBOLS = [
    "BTC/USD", "ETH/USD", "SOL/USD", "XRP/USD",
    "ADA/USD", "DOGE/USD", "HYPE/USD", "SUI/USD",
]

# BTC is always the macro anchor regardless of what else is traded
BTC_SYMBOL = "BTC/USD"


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────

def _setup_logging():
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    log = logging.getLogger("quant_trader")
    log.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(fmt)
    log.addHandler(ch)

    fh = logging.FileHandler(EVENTS_FILE, encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(fmt)
    log.addHandler(fh)

    return log

log = _setup_logging()


def elog(msg: str):
    """Write a timestamped line to the events log and stdout."""
    log.info(msg)


# ─────────────────────────────────────────────────────────────────────────────
# STATE MANAGEMENT — ATOMIC WRITES MANDATORY
# ─────────────────────────────────────────────────────────────────────────────

def _default_state() -> dict:
    now = time.time()
    return {
        "equity":               0.0,
        "peak_equity":          0.0,
        "positions":            {},
        "cooldowns":            {},
        "trade_count":          0,
        "win_count":            0,
        "total_pnl":            0.0,
        "boot_time":            now,
        "last_entry_ms":        0,
        "daily_starting_equity": 0.0,
        "daily_loss_usd":       0.0,
        "daily_date":           datetime.date.today().isoformat(),
        "gross_runtime_start":  now,
        "version":              "QUANT_TRADER_V1",
    }


def load_state() -> dict:
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE) as f:
                s = json.load(f)
            elog(f"[State] Loaded — equity=${s.get('equity',0):.2f} "
                 f"positions={len(s.get('positions',{}))} "
                 f"trades={s.get('trade_count',0)}")
            return s
        except Exception as e:
            elog(f"[State] Corrupt state file: {e} — starting fresh")
    return _default_state()


def save_state(state: dict):
    """Atomic write — temp file then os.replace(). Never write directly."""
    try:
        with open(STATE_TMP, "w") as f:
            json.dump(state, f, indent=2)
        os.replace(STATE_TMP, STATE_FILE)
    except Exception as e:
        log.error(f"[State] WRITE FAILED: {e}")


def _reset_daily_if_needed(state: dict, equity: float):
    """Reset daily loss tracking at UTC midnight."""
    today = datetime.date.today().isoformat()
    if state.get("daily_date") != today:
        state["daily_date"]           = today
        state["daily_starting_equity"] = equity
        state["daily_loss_usd"]        = 0.0
        elog(f"[Daily] New day — starting equity=${equity:.2f}")


# ─────────────────────────────────────────────────────────────────────────────
# EXCHANGE SETUP
# ─────────────────────────────────────────────────────────────────────────────

def _build_exchange() -> ccxtpro.kraken:
    api_key    = os.environ.get("KRAKEN_API_KEY", "")
    api_secret = os.environ.get("KRAKEN_API_SECRET", "")
    if not api_key or not api_secret:
        log.critical("[Boot] KRAKEN_API_KEY / KRAKEN_API_SECRET not set in .env")
        sys.exit(1)
    ex = ccxtpro.kraken({
        "apiKey":    api_key,
        "secret":    api_secret,
        "enableRateLimit": True,
    })
    return ex


def _check_ecp(ex) -> bool:
    """
    Detect non-ECP Kraken accounts (EU retail).
    Non-ECP cannot place margin sell orders — disable shorts.
    Uses validate=True dry run to avoid actual order placement.
    """
    try:
        ex.create_order("BTC/USD", "market", "sell", 0.0001,
                        params={"validate": True})
        return True
    except ccxt.ExchangeError as e:
        if "Non-ECP" in str(e) or "Reduce only" in str(e):
            return False
        return True
    except Exception:
        return True


# ─────────────────────────────────────────────────────────────────────────────
# CANDLE CACHE — shared dict written by WS watchers, read by decision loop
# ─────────────────────────────────────────────────────────────────────────────

# candle_cache[symbol] = list of {o,h,l,c,v,ts} dicts, newest last
candle_cache: dict = {}


def _ohlcv_to_dict(bar) -> dict:
    return {"ts": bar[0], "o": bar[1], "h": bar[2],
            "l": bar[3], "c": bar[4], "v": bar[5]}


async def seed_candle_cache(ex, symbols: list):
    """
    Fetch CANDLE_LIMIT candles per symbol via REST before WS watchers start.
    Without seeding, watch_ohlcv() blocks until the next candle close —
    which on 1h candles could be up to 60 minutes after boot.
    1.5s between calls to stay inside Kraken rate limits.
    """
    elog(f"[Boot] Seeding candle cache for {len(symbols)} symbols...")
    for sym in symbols:
        try:
            raw = ex.fetch_ohlcv(sym, TIMEFRAME, limit=CANDLE_LIMIT)
            candle_cache[sym] = [_ohlcv_to_dict(b) for b in raw]
            elog(f"[Boot]   {sym}: {len(raw)} candles seeded")
        except Exception as e:
            log.warning(f"[Boot]   {sym}: seed failed — {e}")
            candle_cache[sym] = []
        await asyncio.sleep(SEED_DELAY_S)
    elog("[Boot] Candle cache seeded.")


async def watch_symbol(ex, symbol: str):
    """
    WebSocket candle watcher. One task per symbol.
    Keeps candle_cache[symbol] live after the initial REST seed.
    asyncio is single-threaded — no locks needed.
    """
    while True:
        try:
            candles = await ex.watch_ohlcv(symbol, TIMEFRAME)
            if candles:
                candle_cache[symbol] = [_ohlcv_to_dict(b) for b in candles]
        except asyncio.CancelledError:
            break
        except Exception as e:
            log.warning(f"[WS] {symbol} watcher error: {e} — retrying in 5s")
            await asyncio.sleep(5)


# ─────────────────────────────────────────────────────────────────────────────
# LIVE INDICATOR COMPUTATION
# ─────────────────────────────────────────────────────────────────────────────

def _ema(closes: list, period: int) -> list:
    if len(closes) < period:
        return []
    k = 2 / (period + 1)
    r = [sum(closes[:period]) / period]
    for c in closes[period:]:
        r.append(c * k + r[-1] * (1 - k))
    return r


def _macd_hist(closes: list, fast=12, slow=26, signal=9) -> list:
    if len(closes) < slow + signal:
        return []
    ef = _ema(closes, fast)
    es = _ema(closes, slow)
    n  = min(len(ef), len(es))
    ml = [ef[-(n-i)] - es[-(n-i)] for i in range(n)]
    if len(ml) < signal:
        return []
    sl = _ema(ml, signal)
    hl = min(len(ml), len(sl))
    return [ml[-(hl-i)] - sl[-(hl-i)] for i in range(hl)]


def _rsi(closes: list, period=14) -> float:
    if len(closes) < period + 1:
        return 50.0
    d  = [closes[i] - closes[i-1] for i in range(1, len(closes))]
    r  = d[-period:]
    ag = sum(x for x in r if x > 0) / period
    al = sum(-x for x in r if x < 0) / period
    if al == 0:
        return 100.0
    return round(100 - 100 / (1 + ag / al), 2)


def _adx(highs, lows, closes, period=14) -> float:
    if len(closes) < period * 2:
        return 0.0
    trs, pdms, ndms = [], [], []
    for i in range(1, len(closes)):
        h, l, ph, pl = highs[i], lows[i], highs[i-1], lows[i-1]
        trs.append(max(h-l, abs(h-closes[i-1]), abs(l-closes[i-1])))
        pdms.append(max(h-ph, 0) if (h-ph) > (pl-l) else 0)
        ndms.append(max(pl-l, 0) if (pl-l) > (h-ph) else 0)
    def smma(v, p):
        s = sum(v[:p]); r = [s]
        for x in v[p:]: r.append(r[-1] - r[-1]/p + x)
        return r
    st = smma(trs, period); sp = smma(pdms, period); sn = smma(ndms, period)
    dxs = []
    for i in range(len(st)):
        pdi = 100 * sp[i] / st[i] if st[i] > 0 else 0
        ndi = 100 * sn[i] / st[i] if st[i] > 0 else 0
        sm  = pdi + ndi
        dxs.append(100 * abs(pdi-ndi) / sm if sm > 0 else 0)
    return round(sum(dxs[-period:]) / period, 2) if len(dxs) >= period else 0.0


def _bb(closes: list, window=20):
    if len(closes) < window:
        return None, None, None, 0.0
    r   = closes[-window:]
    mid = sum(r) / window
    std = math.sqrt(sum((c-mid)**2 for c in r) / window)
    u   = mid + 2*std
    l   = mid - 2*std
    return u, mid, l, (u-l)/mid if mid > 0 else 0.0


def _mfi(highs, lows, closes, volumes, period=14):
    if len(closes) < period + 1:
        return 50.0
    tp = [(highs[i]+lows[i]+closes[i])/3 for i in range(len(closes))]
    pf = nf = 0.0
    for i in range(-period, 0):
        rf = tp[i] * volumes[i]
        if tp[i] > tp[i-1]: pf += rf
        else:                nf += rf
    if nf == 0: return 100.0
    return round(100 - 100 / (1 + pf/nf), 2)


def _compute_indicators(candles: list) -> dict:
    """
    Compute all indicators needed by the pipeline from a candle list.
    Always uses candles[-2] (penultimate confirmed bar) for signal values.
    """
    if len(candles) < 60:
        return {}
    closes  = [c["c"] for c in candles[:-1]]  # exclude live bar
    highs   = [c["h"] for c in candles[:-1]]
    lows    = [c["l"] for c in candles[:-1]]
    volumes = [c["v"] for c in candles[:-1]]

    bb_u, bb_m, bb_l, bb_w = _bb(closes, 20)
    macd = _macd_hist(closes)
    ema21 = _ema(closes, 21)
    ema55 = _ema(closes, 55)

    return {
        "closes":  closes,
        "highs":   highs,
        "lows":    lows,
        "volumes": volumes,
        "rsi":     _rsi(closes),
        "adx":     _adx(highs, lows, closes),
        "mfi":     _mfi(highs, lows, closes, volumes),
        "macd":    macd,
        "bb_upper": bb_u,
        "bb_mid":   bb_m,
        "bb_lower": bb_l,
        "bb_width": bb_w,
        "ema21":   ema21[-1] if ema21 else None,
        "ema55":   ema55[-1] if ema55 else None,
        "price":   closes[-1],
    }


# ─────────────────────────────────────────────────────────────────────────────
# PROFIT TARGET CALCULATION
# ─────────────────────────────────────────────────────────────────────────────

def _identify_target(price: float, bb_upper, macro_regime: MacroRegime) -> float:
    """
    Identify the best profit target for a new entry.
    Priority: BB_UPPER in BULL → Fixed 2.5% otherwise.
    Target must be identifiable for reduced conviction floors.
    From data: target exits had 100% win rate vs 25-43% for trail-only exits.
    """
    if bb_upper and macro_regime == MacroRegime.BULL:
        dist = (bb_upper - price) / price
        if 0.005 <= dist <= 0.08:   # BB upper is 0.5–8% away — sensible target
            return round(bb_upper, 8)
    return round(price * (1 + FIXED_TARGET_PCT), 8)


# ─────────────────────────────────────────────────────────────────────────────
# COOLDOWN HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _in_cooldown(state: dict, symbol: str) -> bool:
    cd = state["cooldowns"].get(symbol)
    if cd is None:
        return False
    return int(time.time() * 1000) < int(cd)


def _set_cooldown(state: dict, symbol: str):
    state["cooldowns"][symbol] = int(time.time() * 1000) + COOLDOWN_MS
    elog(f"[Cooldown] {symbol} locked for 3h")


# ─────────────────────────────────────────────────────────────────────────────
# POSITION SIZING
# ─────────────────────────────────────────────────────────────────────────────

def _calc_size(equity: float, conviction: int,
               open_sizes: list, size_mult: float = 1.0) -> float:
    """
    deployable = equity × (1 - DRY_POWDER_PCT)
    available  = deployable - sum(open_position_sizes)
    pct        = SIZE_HIGH_PCT if conviction >= 65 else SIZE_LOW_PCT
    size       = min(equity × pct, available) × size_mult
    """
    deployable = equity * (1 - DRY_POWDER_PCT)
    available  = max(0.0, deployable - sum(open_sizes))
    pct        = SIZE_HIGH_PCT if conviction >= 65 else SIZE_LOW_PCT
    size       = min(equity * pct, available) * size_mult
    return round(max(0.0, size), 2)


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY TYPE DETERMINATION
# ─────────────────────────────────────────────────────────────────────────────

def _determine_entry_type(ctx, ews_alert: EWSAlert) -> str:
    """
    ANTICIPATORY: PPM senses Bear→Bull forming (prob > 0.70) while
    macro regime is still BEAR or NEUTRAL.
    These positions hold through adverse conditions by design.
    All other entries are MOMENTUM — they ride current conditions only.
    """
    if (ctx.macro_regime in (MacroRegime.BEAR, MacroRegime.NEUTRAL) and
            ews_alert is not None and
            ews_alert.extreme_fear_building > 0.70):
        return "ANTICIPATORY"
    return "MOMENTUM"


# ─────────────────────────────────────────────────────────────────────────────
# ORDER EXECUTION
# ─────────────────────────────────────────────────────────────────────────────

async def _place_order(ex, symbol: str, side: str,
                       size_usd: float, price: float) -> dict:
    """
    Place a market order. Returns fill details.
    Market orders only — no limit orders. Slippage is knowable.
    Limit order complexity adds rejections and partial fills that
    break position tracking.
    """
    qty = round(size_usd / price, 8)
    if qty <= 0:
        raise ValueError(f"Invalid qty {qty} for {symbol}")

    elog(f"[Order] {side.upper()} {symbol} qty={qty:.8f} est_usd=${size_usd:.2f}")

    order = await ex.create_order(symbol, "market", side, qty)

    fill_price = float(order.get("average") or order.get("price") or price)
    fill_qty   = float(order.get("filled") or qty)
    slippage   = (fill_price - price) / price if price > 0 else 0.0

    elog(f"[Order] FILLED {symbol} fill=${fill_price:.4f} "
         f"slippage={slippage*100:+.3f}%")

    return {
        "fill_price": fill_price,
        "fill_qty":   fill_qty,
        "slippage":   slippage,
        "order_id":   order.get("id", ""),
    }


# ─────────────────────────────────────────────────────────────────────────────
# REGIME CONFIDENCE SCORING (Lv3 job 1 — thesis verification)
# ─────────────────────────────────────────────────────────────────────────────

def _update_regime_confidence(pos: OpenPosition, indic: dict) -> int:
    """
    Score how well current conditions match entry conditions.
    Score starts at pos.regime_confidence (preserved from last candle).
    Applied progressively — degrades or recovers each candle.

    Penalties (deduct):
    - EMA spread narrowing from entry → trend weakening
    - ADX dropped significantly from entry → momentum fading
    - RSI elevated for longs / depressed for shorts
    - Volume exhausting

    Bonuses (add):
    - EMA spread widening → trend strengthening
    - MACD still in profit direction
    - Price making new highs (longs) / new lows (shorts)
    """
    score = pos.regime_confidence
    ema21 = indic.get("ema21")
    ema55 = indic.get("ema55")
    adx   = indic.get("adx", 0)
    rsi   = indic.get("rsi", 50)

    if ema21 and ema55 and ema55 > 0:
        current_spread = (ema21 - ema55) / ema55
        delta = current_spread - pos.ema_spread_at_entry

        if pos.direction == "LONG":
            if delta < -0.005:   score -= 8
            elif delta < -0.002: score -= 4
            elif delta > 0.002:  score += 3
        else:
            if delta > 0.005:    score -= 8
            elif delta > 0.002:  score -= 4
            elif delta < -0.002: score += 3

    if pos.adx_at_entry > 0 and adx > 0:
        adx_drop = pos.adx_at_entry - adx
        if adx_drop > 10:   score -= 6
        elif adx_drop > 5:  score -= 3
        elif adx_drop < -5: score += 2

    if pos.direction == "LONG":
        if rsi >= 78: score -= 5
        elif rsi >= 72: score -= 2
    else:
        if rsi <= 22: score -= 5
        elif rsi <= 28: score -= 2

    macd = indic.get("macd", [])
    if macd and len(macd) >= 2:
        if pos.direction == "LONG":
            score += 2 if macd[-1] > 0 else -3
        else:
            score += 2 if macd[-1] < 0 else -3

    return max(0, min(100, score))


# ─────────────────────────────────────────────────────────────────────────────
# MAIN BOT CLASS
# ─────────────────────────────────────────────────────────────────────────────

class QuantTrader:

    def __init__(self):
        self.ex            = _build_exchange()
        self.state         = load_state()
        self.shorts_enabled = True

        # Instantiate all intelligence layers
        self.ppm    = PeterParkerModule()
        self.gtw    = GandalfTheWhiteModule(timeframe=TIMEFRAME)
        self.mcmc   = MCMCClassifier()
        self.router = SignalRouter()
        self.scorer = ConvictionScorer()
        self.rme    = RiskManagementEngine(AUDIT_FILE)

        # Loop state
        self._last_candle_ts: dict = {}   # {symbol: last processed candle ts}
        self._entry_this_candle = False   # Max 1 entry per candle close
        self._ews_alert  = None
        self._spell      = None
        self._bar_count  = 0

        elog("=" * 68)
        elog("QUANT TRADER V1 — Probabilistic Wave Model")
        elog(f"Symbols: {SYMBOLS}")
        elog(f"Timeframe: {TIMEFRAME}")
        elog("=" * 68)

    # ── Boot sequence ─────────────────────────────────────────────────────────

    async def boot(self):
        # 1. NTP wait — Kraken rejects timestamps > 30s off
        elog("[Boot] Waiting 15s for NTP clock sync...")
        await asyncio.sleep(15)

        # 2. Verify Kraken connectivity
        elog("[Boot] Verifying Kraken API connectivity...")
        try:
            self.ex.fetch_ticker("BTC/USD")
            elog("[Boot] Kraken API reachable.")
        except Exception as e:
            log.critical(f"[Boot] Kraken unreachable: {e}")
            sys.exit(1)

        # 3. Check ECP margin capability
        if not _check_ecp(self.ex):
            self.shorts_enabled = False
            elog("[Boot] ⚠️  Non-ECP account detected — shorts disabled. Longs only.")
        else:
            elog("[Boot] ECP margin confirmed — shorts enabled.")

        # 4. Fetch and log all balances
        try:
            bal = self.ex.fetch_balance()
            equity = float(bal.get("free", {}).get("USD", 0))
            elog(f"[Boot] USD balance: ${equity:.2f}")
            for asset, amount in bal.get("total", {}).items():
                if float(amount or 0) > 0 and asset != "USD":
                    elog(f"[Boot]   {asset}: {amount}")

            # Initialize equity in state if fresh start
            if self.state["equity"] == 0.0:
                self.state["equity"]               = equity
                self.state["peak_equity"]           = equity
                self.state["daily_starting_equity"] = equity
                elog(f"[Boot] Fresh start — equity=${equity:.2f}")
            else:
                elog(f"[Boot] Resuming — state equity=${self.state['equity']:.2f} "
                     f"live USD=${equity:.2f}")
        except Exception as e:
            log.warning(f"[Boot] Balance fetch failed: {e}")

        # 5. Seed candle cache
        await seed_candle_cache(self.ex, SYMBOLS)

        # 6. Resume check — log any positions being resumed
        if self.state["positions"]:
            elog(f"[Boot] Resuming {len(self.state['positions'])} open position(s):")
            for sym, pos_data in self.state["positions"].items():
                elog(f"[Boot]   {sym} {pos_data.get('direction')} "
                     f"entry=${pos_data.get('fill_price',0):.2f} "
                     f"ever_green={pos_data.get('ever_green',False)}")

        elog("[Boot] Boot sequence complete. Entering main loop.")

    # ── Main loop ─────────────────────────────────────────────────────────────

    async def run(self):
        await self.boot()

        # Start WebSocket watchers
        watcher_tasks = [
            asyncio.create_task(watch_symbol(self.ex, sym))
            for sym in SYMBOLS
        ]

        try:
            while True:
                if os.path.exists(EMERGENCY_FILE):
                    elog("[EMERGENCY] EMERGENCY_STOP file detected — halting.")
                    await self._emergency_close_all()
                    break

                await self._tick()
                await asyncio.sleep(LOOP_SLEEP_S)

        except KeyboardInterrupt:
            elog("[Main] KeyboardInterrupt — shutting down gracefully.")
        except Exception as e:
            log.critical(f"[Main] Unhandled exception: {e}", exc_info=True)
        finally:
            for t in watcher_tasks:
                t.cancel()
            save_state(self.state)
            try:
                await self.ex.close()
            except Exception:
                pass
            elog("[Main] State saved. Goodbye.")

    # ── Per-tick logic ────────────────────────────────────────────────────────

    async def _tick(self):
        """
        Runs every LOOP_SLEEP_S seconds.
        Fast sensors run every tick.
        Full pipeline runs only when a new candle has closed.
        """
        now_ms = int(time.time() * 1000)

        # ── Fast sensors (book/spread/CVD) run every tick ─────────────────
        # They can trigger emergency GTW recast regardless of candle state
        await self._run_fast_sensors()

        # ── Detect new candle closes ──────────────────────────────────────
        new_candle_symbols = self._detect_new_candles()

        if not new_candle_symbols:
            return  # No new candles — fast sensors already ran

        # ── New candle(s) closed — run full pipeline ──────────────────────
        self._entry_this_candle = False  # Reset max-1-entry-per-candle gate
        self._bar_count += 1

        # Increment bars_held for all open positions
        for sym in self.state["positions"]:
            self.state["positions"][sym]["bars_held"] = \
                self.state["positions"][sym].get("bars_held", 0) + 1

        # ── 1. PPM — sense what's forming ─────────────────────────────────
        btc_candles = candle_cache.get(BTC_SYMBOL, [])
        if len(btc_candles) < 60:
            log.debug("[Loop] Insufficient BTC candles. Skipping.")
            return

        order_book = await self._fetch_order_book(BTC_SYMBOL)

        alt_candles = {
            sym: candle_cache.get(sym, [])
            for sym in SYMBOLS if sym != BTC_SYMBOL
        }

        self._ews_alert = self.ppm.sense(
            candles_1h   = btc_candles,
            candles_4h   = btc_candles[-20:],  # Use tail as 4h proxy
            order_book   = order_book,
            btc_candles  = btc_candles,
            alt_candles  = alt_candles,
        )

        # ── 2. GTW — cast the spell ───────────────────────────────────────
        self._spell = self.gtw.concoct_spell(self._ews_alert, self._bar_count)

        # ── 3. MCMC — confirm current condition ───────────────────────────
        btc_ctx = self.mcmc.classify(
            candles     = btc_candles,
            btc_candles = btc_candles,
            order_book  = order_book,
            spell       = self._spell,
            symbol      = BTC_SYMBOL,
        )

        macro_regime = btc_ctx.macro_regime

        elog(f"[Loop] Bar={self._bar_count} "
             f"MCMC={btc_ctx.confirmed_mcmc.value} "
             f"MACRO={macro_regime.value} "
             f"SPELL={self._spell.spell_name.value} "
             f"positions={len(self.state['positions'])}")

        # ── 4. Per-symbol pipeline ────────────────────────────────────────
        # Build data dicts for the RME
        live_prices  = {}
        indic_by_sym = {}
        macd_data    = {}
        bb_data      = {}
        rsi_data     = {}
        adx_data     = {}
        ema_data     = {}

        for sym in SYMBOLS:
            candles = candle_cache.get(sym, [])
            if len(candles) < 60:
                continue

            indic = _compute_indicators(candles)
            if not indic:
                continue

            indic_by_sym[sym]  = indic
            live_prices[sym]   = indic["price"]
            macd_data[sym]     = indic["macd"]
            bb_data[sym]       = (indic["bb_upper"], indic["bb_mid"], indic["bb_lower"])
            rsi_data[sym]      = indic["rsi"]
            adx_data[sym]      = indic["adx"]
            ema_data[sym]      = (indic["ema21"], indic["ema55"])

        # Update regime confidence on all open positions
        for sym, pos_data in self.state["positions"].items():
            if sym in indic_by_sym:
                pos_obj = position_from_state(sym, pos_data)
                new_conf = _update_regime_confidence(pos_obj, indic_by_sym[sym])
                self.state["positions"][sym]["regime_confidence"] = new_conf

        # ── 5. RME — evaluate all open positions ──────────────────────────
        open_positions_objs = {
            sym: position_from_state(sym, data)
            for sym, data in self.state["positions"].items()
            if sym in live_prices
        }

        portfolio = PortfolioState(
            equity                = self.state["equity"],
            peak_equity           = self.state["peak_equity"],
            daily_starting_equity = self.state.get("daily_starting_equity", self.state["equity"]),
            daily_loss_usd        = self.state.get("daily_loss_usd", 0.0),
            open_positions        = open_positions_objs,
            macro_regime          = macro_regime,
            current_mcmc          = btc_ctx.confirmed_mcmc,
            active_spell          = self._spell.spell_name,
            ews_alert             = self._ews_alert,
        )

        rme_decisions = self.rme.evaluate(
            portfolio   = portfolio,
            ctx         = btc_ctx,
            live_prices = live_prices,
            candle_data = {sym: candle_cache.get(sym, []) for sym in SYMBOLS},
            macd_data   = macd_data,
            bb_data     = bb_data,
            rsi_data    = rsi_data,
            adx_data    = adx_data,
            ema_data    = ema_data,
        )

        # ── 6. Process exits ──────────────────────────────────────────────
        await self._process_exits(rme_decisions, live_prices, btc_ctx)

        # ── 7. Process entries (one per candle) ───────────────────────────
        if not self._spell.force_exit_existing:
            for sym in new_candle_symbols:
                if self._entry_this_candle:
                    break  # Max 1 entry per candle close
                if sym not in indic_by_sym:
                    continue

                candles = candle_cache.get(sym, [])
                if len(candles) < 60:
                    continue

                sym_ctx = self.mcmc.classify(
                    candles     = candles,
                    btc_candles = btc_candles,
                    order_book  = order_book if sym == BTC_SYMBOL else await self._fetch_order_book(sym),
                    spell       = self._spell,
                    symbol      = sym,
                )

                await self._process_entry(
                    sym, sym_ctx, indic_by_sym[sym], live_prices[sym]
                )

        # ── 8. Reconcile equity and save state ────────────────────────────
        await self._reconcile_equity(live_prices)
        _reset_daily_if_needed(self.state, self.state["equity"])
        save_state(self.state)

    # ── Fast sensor tick ──────────────────────────────────────────────────────

    async def _run_fast_sensors(self):
        """
        Run book depth, spread, CVD sensors between candle closes.
        These can trigger emergency GTW recast if thresholds breached.
        Called every LOOP_SLEEP_S seconds.
        """
        if not candle_cache.get(BTC_SYMBOL):
            return
        try:
            ob = await self._fetch_order_book(BTC_SYMBOL)
            # PPM fast sensor check — update spread and depth readings
            # Emergency recast happens inside GTW if thresholds crossed
            if self._ews_alert and self._spell:
                prev_depths = [ob.get("bids", [[0,0]])[0][1]]
                prev_spreads = []
                if ob.get("bids") and ob.get("asks"):
                    spread = (ob["asks"][0][0] - ob["bids"][0][0]) / ob["bids"][0][0]
                    prev_spreads = [spread]
        except Exception:
            pass  # Fast sensors fail silently — don't interrupt the loop

    # ── Candle close detection ────────────────────────────────────────────────

    def _detect_new_candles(self) -> list:
        """
        Returns list of symbols that have a newly closed candle.
        Compares last processed timestamp against latest candle in cache.
        """
        new = []
        for sym in SYMBOLS:
            candles = candle_cache.get(sym, [])
            if len(candles) < 2:
                continue
            latest_ts = candles[-2]["ts"]  # penultimate = last confirmed
            if latest_ts != self._last_candle_ts.get(sym):
                self._last_candle_ts[sym] = latest_ts
                new.append(sym)
        return new

    # ── Exit processing ───────────────────────────────────────────────────────

    async def _process_exits(self, rme_decisions: dict,
                             live_prices: dict, ctx):
        """Process RME decisions. Close positions that need closing."""
        emergency_triggered = False

        for sym, decision in rme_decisions.items():
            if decision.action == ExitAction.EMERGENCY:
                emergency_triggered = True

            if decision.action in (ExitAction.CLOSE, ExitAction.EMERGENCY):
                await self._execute_exit(sym, decision, live_prices, ctx)

            elif decision.action == ExitAction.TIGHTEN_TRAIL:
                # Update trail stop in state without closing
                if sym in self.state["positions"]:
                    self.state["positions"][sym]["trail_stop_price"] = \
                        decision.updated_trail_stop
                    log.debug(f"[Exit] {sym} trail tightened to "
                              f"{decision.updated_trail_stop:.4f}")

            elif decision.action == ExitAction.HOLD:
                # Update trail stop (ratcheted upward by RME)
                if sym in self.state["positions"] and decision.updated_trail_stop > 0:
                    pos_data = self.state["positions"][sym]
                    direction = pos_data.get("direction", "LONG")
                    current   = float(pos_data.get("trail_stop_price", 0))
                    new_stop  = decision.updated_trail_stop
                    # Only ratchet in profit direction
                    if direction == "LONG":
                        pos_data["trail_stop_price"] = max(new_stop, current)
                    else:
                        pos_data["trail_stop_price"] = (
                            min(new_stop, current) if current > 0 else new_stop
                        )
                    pos_data["peak_price"]    = decision.updated_peak_price
                    pos_data["ever_green"]    = decision.updated_ever_green
                    pos_data["regime_confidence"] = decision.updated_regime_conf

        if emergency_triggered:
            elog("[EMERGENCY] Halting after emergency close.")
            save_state(self.state)
            sys.exit(0)

    async def _execute_exit(self, sym: str, decision, live_prices: dict, ctx):
        """Place the sell order and update state."""
        if sym not in self.state["positions"]:
            return

        pos_data = self.state["positions"][sym]
        direction = pos_data.get("direction", "LONG")
        side      = "sell" if direction == "LONG" else "buy"
        price     = live_prices.get(sym, float(pos_data.get("fill_price", 0)))
        size_qty  = float(pos_data.get("size_qty", 0))

        if size_qty <= 0:
            log.warning(f"[Exit] {sym} size_qty=0 — removing from state")
            del self.state["positions"][sym]
            return

        try:
            order = await self.ex.create_order(sym, "market", side, size_qty)
            fill_price = float(order.get("average") or order.get("price") or price)
        except Exception as e:
            log.error(f"[Exit] {sym} order failed: {e} — using signal price")
            fill_price = price

        # Calculate PnL
        fill_price_entry = float(pos_data.get("fill_price", fill_price))
        if direction == "LONG":
            pnl_pct = (fill_price - fill_price_entry) / fill_price_entry
        else:
            pnl_pct = (fill_price_entry - fill_price) / fill_price_entry
        pnl_usd = float(pos_data.get("size_usd", 0)) * pnl_pct

        # Update state
        self.state["equity"]    = round(self.state["equity"] + pnl_usd, 4)
        self.state["peak_equity"] = max(self.state["peak_equity"], self.state["equity"])
        self.state["trade_count"] += 1
        self.state["total_pnl"]   += pnl_usd

        if pnl_usd > 0:
            self.state["win_count"] += 1
        else:
            self.state["daily_loss_usd"] = \
                self.state.get("daily_loss_usd", 0) - abs(pnl_usd)

        win_rate = (self.state["win_count"] / self.state["trade_count"] * 100
                    if self.state["trade_count"] > 0 else 0)

        elog(f"[Exit] {'✅' if pnl_usd > 0 else '❌'} {sym} {direction} "
             f"reason={decision.exit_reason.value if decision.exit_reason else 'CLOSE'} "
             f"fill=${fill_price:.4f} pnl=${pnl_usd:+.2f} ({pnl_pct*100:+.2f}%) "
             f"equity=${self.state['equity']:.2f} "
             f"WR={win_rate:.0f}% ({self.state['win_count']}/{self.state['trade_count']})")

        # Write audit trail
        pos_obj = position_from_state(sym, pos_data)
        decision.exit_price = fill_price
        decision.pnl_usd    = pnl_usd
        decision.pnl_pct    = pnl_pct * 100
        self.rme.write_audit(pos_obj, decision, ctx)

        # Set cooldown and remove position
        _set_cooldown(self.state, sym)
        del self.state["positions"][sym]

    # ── Entry processing ──────────────────────────────────────────────────────

    async def _process_entry(self, sym: str, sym_ctx,
                             indic: dict, price: float):
        """
        Run signal router and conviction scorer for a single symbol.
        If an entry is approved and all pre-flight checks pass — place order.
        """
        # Pre-flight gate 1: cooldown
        if _in_cooldown(self.state, sym):
            return

        # Pre-flight gate 2: already have a position in this symbol
        if sym in self.state["positions"]:
            return

        # Pre-flight gate 3: max open positions
        max_pos = min(MAX_POSITIONS, sym_ctx.strategy.max_open_positions)
        if len(self.state["positions"]) >= max_pos:
            return

        # Pre-flight gate 4: daily loss limit
        daily_loss_pct = (self.state.get("daily_loss_usd", 0) /
                          self.state.get("daily_starting_equity", 1))
        if abs(daily_loss_pct) >= 0.05:
            log.debug(f"[Entry] {sym} blocked — daily loss limit")
            return

        # Pre-flight gate 5: shorts check
        candles = candle_cache.get(sym, [])
        candidates = self.router.route(
            ctx       = sym_ctx,
            candles   = candles,
            symbol    = sym,
            cooldowns = self.state["cooldowns"],
        )
        if not candidates:
            return

        # Filter shorts if not enabled
        if not self.shorts_enabled:
            candidates = [c for c in candidates if c.direction.value == "LONG"]
        if not candidates:
            return

        # Score candidates
        open_sizes = [
            float(p.get("size_usd", 0))
            for p in self.state["positions"].values()
        ]
        routing_result = self.scorer.score_and_filter(
            candidates          = candidates,
            ctx                 = sym_ctx,
            live_equity         = self.state["equity"],
            open_position_sizes = open_sizes,
        )

        if not routing_result.has_entries():
            return

        # Take the highest-conviction approved entry
        entry = max(routing_result.approved_entries, key=lambda e: e.conviction)

        # Pre-flight gate 6: size
        size_usd = _calc_size(
            self.state["equity"],
            entry.conviction,
            open_sizes,
            self._spell.size_multiplier,
        )
        if size_usd < MIN_ORDER_USD:
            log.debug(f"[Entry] {sym} size ${size_usd:.2f} below minimum ${MIN_ORDER_USD}")
            return

        # Pre-flight gate 7: identify profit target (required for lower floors)
        bb_upper = indic.get("bb_upper")
        target   = _identify_target(price, bb_upper, sym_ctx.macro_regime)

        # Determine entry type (ANTICIPATORY vs MOMENTUM)
        entry_type = _determine_entry_type(sym_ctx, self._ews_alert)

        # Place the order
        side = "buy" if entry.direction.value == "LONG" else "sell"
        try:
            fill = await _place_order(self.ex, sym, side, size_usd, price)
        except Exception as e:
            log.error(f"[Entry] {sym} order failed: {e}")
            return

        fill_price = fill["fill_price"]
        fill_qty   = fill["fill_qty"]
        slippage   = fill["slippage"]

        # Record position in state
        self.state["positions"][sym] = {
            "direction":         entry.direction.value,
            "entry_type":        entry_type,
            "signal_type":       entry.signal_type.value,
            "entry_price":       round(price, 8),
            "fill_price":        round(fill_price, 8),
            "size_usd":          round(size_usd, 2),
            "size_qty":          round(fill_qty, 8),
            "entry_time_ms":     int(time.time() * 1000),
            "conviction":        entry.conviction,
            "mcmc_at_entry":     sym_ctx.confirmed_mcmc.value,
            "spell_at_entry":    self._spell.spell_name.value,
            "regime_at_entry":   sym_ctx.macro_regime.value,
            "peak_price":        round(fill_price, 8),
            "trail_stop_price":  0.0,
            "target_price":      round(target, 8),
            "ever_green":        False,
            "bars_held":         0,
            "regime_confidence": 100,
            "ema_spread_at_entry": (
                (indic["ema21"] - indic["ema55"]) / indic["ema55"]
                if indic.get("ema21") and indic.get("ema55") and indic["ema55"] > 0
                else 0.0
            ),
            "adx_at_entry":      indic.get("adx", 0),
            "rsi_at_entry":      indic.get("rsi", 50),
            "bb_upper_at_entry": round(bb_upper, 8) if bb_upper else 0.0,
            "bb_midband_at_entry": round(indic["bb_mid"], 8) if indic.get("bb_mid") else 0.0,
        }

        self.state["last_entry_ms"] = int(time.time() * 1000)
        self._entry_this_candle     = True   # Block further entries this candle

        elog(f"[Entry] 🟢 {entry_type} {sym} {entry.direction.value} "
             f"signal={entry.signal_type.value} "
             f"conviction={entry.conviction} "
             f"fill=${fill_price:.4f} size=${size_usd:.2f} "
             f"target=${target:.4f} "
             f"slippage={slippage*100:+.3f}% "
             f"MCMC={sym_ctx.confirmed_mcmc.value} "
             f"SPELL={self._spell.spell_name.value}")

    # ── Equity reconciliation ─────────────────────────────────────────────────

    async def _reconcile_equity(self, live_prices: dict):
        """
        Recalculate equity including mark-to-market value of open positions.
        Called at end of every candle to keep equity accurate.
        """
        try:
            bal = self.ex.fetch_balance()
            cash = float(bal.get("free", {}).get("USD", 0))
        except Exception:
            cash = self.state["equity"]  # Fallback to state if fetch fails

        # Mark-to-market open positions
        pos_value = 0.0
        for sym, pos_data in self.state["positions"].items():
            price = live_prices.get(sym, float(pos_data.get("fill_price", 0)))
            qty   = float(pos_data.get("size_qty", 0))
            pos_value += price * qty

        equity = cash + pos_value
        self.state["equity"]      = round(equity, 4)
        self.state["peak_equity"] = max(self.state["peak_equity"], equity)

    # ── Emergency close all ───────────────────────────────────────────────────

    async def _emergency_close_all(self):
        """Close all positions at market immediately."""
        elog("[EMERGENCY] Closing all positions.")
        for sym, pos_data in list(self.state["positions"].items()):
            direction = pos_data.get("direction", "LONG")
            side      = "sell" if direction == "LONG" else "buy"
            qty       = float(pos_data.get("size_qty", 0))
            if qty <= 0:
                continue
            try:
                await self.ex.create_order(sym, "market", side, qty)
                elog(f"[EMERGENCY] Closed {sym} {direction}")
                _set_cooldown(self.state, sym)
                del self.state["positions"][sym]
            except Exception as e:
                log.error(f"[EMERGENCY] Failed to close {sym}: {e}")
        save_state(self.state)

    # ── Order book fetch helper ───────────────────────────────────────────────

    async def _fetch_order_book(self, symbol: str) -> dict:
        """
        Fetch order book. Returns empty book on failure.
        CRITICAL: Kraken ccxt returns [price, size, ts] — always use index, never unpack.
        """
        try:
            return await self.ex.fetch_order_book(symbol, 20)
        except Exception:
            return {"bids": [], "asks": []}


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    bot = QuantTrader()
    await bot.run()


if __name__ == "__main__":
    asyncio.run(main())