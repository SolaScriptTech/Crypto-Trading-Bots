"""
Kraken Autonomous Trading Bot v2
Strategy: Multi-timeframe momentum + mean reversion hybrid
Optimized for $50 account on zero-fee tier
PST timezone aware | Fixed base64 secret decoding
"""

import time
import hmac
import hashlib
import base64
import urllib.parse
import requests
import json
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import deque
import statistics

# ─────────────────────────────────────────────────────────────
# CONFIGURATION — paste your keys here OR set env variables
# ─────────────────────────────────────────────────────────────
API_KEY    = os.environ.get("KRAKEN_API_KEY",    "YOUR_API_KEY_HERE")
API_SECRET = os.environ.get("KRAKEN_API_SECRET", "YOUR_API_SECRET_HERE")

# Capital & risk
STARTING_CAPITAL_USD = 50.0
MAX_TRADE_PCT        = 0.45    # Max 45% of balance per trade (keeps 2-3 slots open)
MIN_TRADE_USD        = 5.0     # Kraken minimum-ish
STOP_LOSS_PCT        = 0.025   # 2.5% trailing stop
TAKE_PROFIT_PCT      = 0.055   # 5.5% take profit  (~2.2:1 R:R)
SLIPPAGE_BUFFER      = 0.0015  # 0.15% limit price buffer
DRAWDOWN_HALT_PCT    = 0.30    # Halt if 30% of starting capital is lost

# Pairs — high volume, tight spreads on Kraken
PAIRS = [
    "XBTUSD",    # BTC/USD
    "ETHUSD",    # ETH/USD
    "SOLUSD",    # SOL/USD
    "XRPUSD",    # XRP/USD
    "ADAUSD",    # ADA/USD
    "LINKUSD",   # LINK/USD
    "DOTUSD",    # DOT/USD
    "MATICUSD",  # MATIC/USD
]

# Scanning
SCAN_INTERVAL_SEC = 45    # seconds between full scans
OHLC_INTERVAL     = 15   # 15-min candles
MIN_CANDLES       = 60   # minimum candles required before scoring

# Active trading windows — PST (bot opens NEW positions only during these hours)
# Manages exits 24/7 regardless
ACTIVE_HOURS_PST = [
    (1,  4),   # London open
    (6,  12),  # NY open overlap (best window)
    (17, 22),  # Asia pre-session
]

MAX_CONCURRENT_POSITIONS = 3

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
log_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
log_file      = logging.FileHandler("bot.log", encoding="utf-8")
log_stdout    = logging.StreamHandler(sys.stdout)
log_file.setFormatter(log_formatter)
log_stdout.setFormatter(log_formatter)

log = logging.getLogger("krakenbot")
log.setLevel(logging.INFO)
log.addHandler(log_file)
log.addHandler(log_stdout)

PST = ZoneInfo("America/Los_Angeles")

# ─────────────────────────────────────────────────────────────
# KRAKEN API WRAPPER  (fixed secret decoding)
# ─────────────────────────────────────────────────────────────
class KrakenAPI:
    BASE = "https://api.kraken.com"

    def __init__(self, key: str, secret: str):
        self.key = key
        # Kraken secrets are base64 strings that may lack padding.
        # Add padding to nearest multiple of 4 before decoding.
        secret = secret.strip()
        missing_padding = len(secret) % 4
        if missing_padding:
            secret += "=" * (4 - missing_padding)
        try:
            self.secret = base64.b64decode(secret)
        except Exception as e:
            raise ValueError(
                f"Could not decode API secret. Make sure you copied it correctly from Kraken.\n"
                f"Original error: {e}"
            )

    def _nonce(self) -> str:
        return str(int(time.time() * 1000))

    def _sign(self, url_path: str, data: dict, nonce: str) -> str:
        post_data = urllib.parse.urlencode(data)
        encoded   = (nonce + post_data).encode("utf-8")
        message   = url_path.encode("utf-8") + hashlib.sha256(encoded).digest()
        mac       = hmac.new(self.secret, message, hashlib.sha512)
        return base64.b64encode(mac.digest()).decode()

    def public(self, endpoint: str, params: dict = None):
        url = f"{self.BASE}/0/public/{endpoint}"
        r   = requests.get(url, params=params or {}, timeout=10)
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            raise Exception(f"Kraken API error [{endpoint}]: {data['error']}")
        return data["result"]

    def private(self, endpoint: str, params: dict = None):
        if params is None:
            params = {}
        url_path        = f"/0/private/{endpoint}"
        nonce           = self._nonce()
        params["nonce"] = nonce
        headers = {
            "API-Key":  self.key,
            "API-Sign": self._sign(url_path, params, nonce),
        }
        r = requests.post(
            f"{self.BASE}{url_path}",
            data=params,
            headers=headers,
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
        if data.get("error"):
            raise Exception(f"Kraken private API error [{endpoint}]: {data['error']}")
        return data["result"]

    # ── Market data ──────────────────────────────────────────
    def get_ohlc(self, pair: str, interval: int = 15):
        result = self.public("OHLC", {"pair": pair, "interval": interval})
        key    = [k for k in result if k != "last"][0]
        return result[key]  # [[time, open, high, low, close, vwap, volume, count], ...]

    def get_ticker(self, pair: str):
        result = self.public("Ticker", {"pair": pair})
        return result[list(result.keys())[0]]

    # ── Account ──────────────────────────────────────────────
    def get_balance(self) -> dict:
        return self.private("Balance")

    # ── Orders ───────────────────────────────────────────────
    def place_limit_order(self, pair: str, side: str, volume: float, price: float):
        params = {
            "pair":      pair,
            "type":      side,
            "ordertype": "limit",
            "price":     f"{price:.10g}",
            "volume":    f"{volume:.10g}",
        }
        return self.private("AddOrder", params)

    def place_market_order(self, pair: str, side: str, volume: float):
        params = {
            "pair":      pair,
            "type":      side,
            "ordertype": "market",
            "volume":    f"{volume:.10g}",
        }
        return self.private("AddOrder", params)

    def cancel_all(self):
        return self.private("CancelAll")


# ─────────────────────────────────────────────────────────────
# TECHNICAL INDICATORS  (pure Python, no extra dependencies)
# ─────────────────────────────────────────────────────────────
def ema(values: list, period: int):
    if len(values) < period:
        return None
    k   = 2.0 / (period + 1)
    val = sum(values[:period]) / period
    for v in values[period:]:
        val = v * k + val * (1 - k)
    return val

def sma(values: list, period: int):
    if len(values) < period:
        return None
    return sum(values[-period:]) / period

def rsi(closes: list, period: int = 14):
    if len(closes) < period + 1:
        return None
    deltas   = [closes[i+1] - closes[i] for i in range(len(closes) - 1)]
    gains    = [max(d, 0.0) for d in deltas]
    losses   = [abs(min(d, 0.0)) for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return 100.0 - (100.0 / (1.0 + avg_gain / avg_loss))

def macd(closes: list, fast: int = 12, slow: int = 26, signal: int = 9):
    if len(closes) < slow + signal:
        return None, None, None
    macd_series = []
    for i in range(slow - 1, len(closes)):
        fe = ema(closes[:i+1], fast)
        se = ema(closes[:i+1], slow)
        if fe is not None and se is not None:
            macd_series.append(fe - se)
    if len(macd_series) < signal:
        return None, None, None
    macd_val   = macd_series[-1]
    signal_val = ema(macd_series, signal)
    hist       = (macd_val - signal_val) if signal_val is not None else None
    return macd_val, signal_val, hist

def bollinger(closes: list, period: int = 20, num_std: float = 2.0):
    if len(closes) < period:
        return None, None, None
    recent = closes[-period:]
    mid    = sum(recent) / period
    std    = statistics.stdev(recent)
    return mid + num_std * std, mid, mid - num_std * std

def atr(highs: list, lows: list, closes: list, period: int = 14):
    if len(closes) < period + 1:
        return None
    trs = []
    for i in range(1, len(closes)):
        tr = max(
            highs[i]  - lows[i],
            abs(highs[i]  - closes[i-1]),
            abs(lows[i]   - closes[i-1])
        )
        trs.append(tr)
    return sum(trs[-period:]) / period

def stoch_rsi(closes: list, rsi_period: int = 14, stoch_period: int = 14):
    rsi_vals = []
    for i in range(rsi_period + 1, len(closes) + 1):
        r = rsi(closes[:i], rsi_period)
        if r is not None:
            rsi_vals.append(r)
    if len(rsi_vals) < stoch_period:
        return None, None
    window = rsi_vals[-stoch_period:]
    lo, hi = min(window), max(window)
    if hi == lo:
        return 50.0, 50.0
    k = (rsi_vals[-1] - lo) / (hi - lo) * 100
    d = sum(rsi_vals[-3:]) / 3 if len(rsi_vals) >= 3 else k
    return k, d

def volume_osc(volumes: list, fast: int = 5, slow: int = 14):
    f = sma(volumes, fast)
    s = sma(volumes, slow)
    if f is None or s is None or s == 0:
        return None
    return (f - s) / s * 100


# ─────────────────────────────────────────────────────────────
# SIGNAL ENGINE
# ─────────────────────────────────────────────────────────────
class SignalEngine:
    """Combines 8 indicators. Score >= 2.5 → BUY candidate."""

    def score(self, candles: list) -> tuple:
        if len(candles) < MIN_CANDLES:
            return 0.0, {}

        opens   = [float(c[1]) for c in candles]
        highs   = [float(c[2]) for c in candles]
        lows    = [float(c[3]) for c in candles]
        closes  = [float(c[4]) for c in candles]
        volumes = [float(c[6]) for c in candles]

        sc   = 0.0
        why  = {}
        last = closes[-1]

        # 1 ── MACD crossover  (weight 2)
        ml,  sl,  hist  = macd(closes)
        ml2, sl2, hist2 = macd(closes[:-1])
        if None not in (ml, sl, ml2, sl2):
            if ml > sl and ml2 <= sl2:
                sc += 2.0;  why["MACD"] = "bullish_cross"
            elif ml < sl and ml2 >= sl2:
                sc -= 2.0;  why["MACD"] = "bearish_cross"
            elif hist and hist > 0:
                sc += 0.8;  why["MACD"] = f"bull_hist"
            elif hist and hist < 0:
                sc -= 0.8;  why["MACD"] = f"bear_hist"

        # 2 ── RSI  (weight 1.5)
        r = rsi(closes)
        if r is not None:
            if r < 30:
                sc += 1.5; why["RSI"] = f"oversold_{r:.1f}"
            elif r > 70:
                sc -= 1.5; why["RSI"] = f"overbought_{r:.1f}"
            elif r < 45:
                sc += 0.5; why["RSI"] = f"low_bull_{r:.1f}"

        # 3 ── Stochastic RSI  (weight 1.5)
        k, d = stoch_rsi(closes)
        if k is not None:
            if k < 20 and d < 20:
                sc += 1.5; why["StochRSI"] = f"oversold_K{k:.1f}"
            elif k > 80 and d > 80:
                sc -= 1.5; why["StochRSI"] = f"overbought_K{k:.1f}"
            elif k > d and k < 55:
                sc += 0.6; why["StochRSI"] = f"bull_cross_K{k:.1f}"

        # 4 ── EMA trend alignment 9/21/55  (weight 1)
        e9  = ema(closes, 9)
        e21 = ema(closes, 21)
        e55 = ema(closes, 55)
        if None not in (e9, e21, e55):
            if e9 > e21 > e55:
                sc += 1.0; why["EMA"] = "bullish_stack"
            elif e9 < e21 < e55:
                sc -= 1.0; why["EMA"] = "bearish_stack"

        # 5 ── Bollinger Bands  (weight 1.5)
        upper, mid, lower = bollinger(closes)
        if None not in (upper, mid, lower):
            bw = (upper - lower) / mid
            if last < lower:
                sc += 1.5; why["BB"] = "below_lower"
            elif last > upper:
                sc -= 1.5; why["BB"] = "above_upper"
            if bw < 0.03:
                sc += 0.5; why["BB_squeeze"] = f"squeeze_{bw:.3f}"

        # 6 ── Volume oscillator  (weight 1)
        vo = volume_osc(volumes)
        if vo is not None:
            if vo > 15:
                sc += 1.0; why["Vol"] = f"rising_{vo:.1f}pct"
            elif vo < -15:
                sc -= 0.5; why["Vol"] = f"falling_{vo:.1f}pct"

        # 7 ── ATR volatility filter (dampener)
        at = atr(highs, lows, closes)
        if at is not None:
            atr_pct = at / last * 100
            if atr_pct < 0.25:
                sc *= 0.5;  why["ATR"] = f"dead_mkt_{atr_pct:.2f}pct"
            elif atr_pct > 10:
                sc *= 0.65; why["ATR"] = f"extreme_vol_{atr_pct:.2f}pct"

        # 8 ── Candle body momentum  (weight 0.5)
        body      = abs(closes[-1] - opens[-1])
        prev_body = abs(closes[-2] - opens[-2])
        if body > prev_body * 1.5:
            if closes[-1] > opens[-1]:
                sc += 0.5; why["Candle"] = "strong_bull"
            else:
                sc -= 0.5; why["Candle"] = "strong_bear"

        return round(sc, 2), why


# ─────────────────────────────────────────────────────────────
# POSITION
# ─────────────────────────────────────────────────────────────
class Position:
    def __init__(self, pair: str, entry: float, volume: float, usd_val: float):
        self.pair          = pair
        self.entry_price   = entry
        self.volume        = volume
        self.usd_val       = usd_val
        self.take_profit   = entry * (1 + TAKE_PROFIT_PCT)
        self.trailing_stop = entry * (1 - STOP_LOSS_PCT)
        self.opened_at     = datetime.now(PST)

    def ratchet(self, current_price: float):
        candidate = current_price * (1 - STOP_LOSS_PCT)
        if candidate > self.trailing_stop:
            self.trailing_stop = candidate

    def pnl_pct(self, price: float) -> float:
        return (price - self.entry_price) / self.entry_price * 100


# ─────────────────────────────────────────────────────────────
# MIN ORDER VOLUMES (Kraken per-pair minimums)
# ─────────────────────────────────────────────────────────────
MIN_VOLUMES = {
    "XBTUSD":   0.0001,
    "ETHUSD":   0.002,
    "SOLUSD":   0.02,
    "XRPUSD":   10.0,
    "ADAUSD":   15.0,
    "LINKUSD":  0.2,
    "DOTUSD":   0.5,
    "MATICUSD": 5.0,
}


# ─────────────────────────────────────────────────────────────
# TRADING BOT
# ─────────────────────────────────────────────────────────────
class TradingBot:

    def __init__(self):
        self._validate_keys()
        self.api        = KrakenAPI(API_KEY, API_SECRET)
        self.engine     = SignalEngine()
        self.positions  = {}
        self.trade_log  = []
        self.errors     = deque(maxlen=30)
        self.total_pnl  = 0.0
        self.wins       = 0
        self.losses     = 0
        self.scan_count = 0

    def _validate_keys(self):
        if API_KEY == "YOUR_API_KEY_HERE" or API_SECRET == "YOUR_API_SECRET_HERE":
            log.error("❌  API keys not set.")
            log.error("    Option A — set environment variables:")
            log.error("      $env:KRAKEN_API_KEY='your_key'")
            log.error("      $env:KRAKEN_API_SECRET='your_secret'")
            log.error("    Option B — edit lines 19-20 in bot.py directly.")
            sys.exit(1)

    def in_active_session(self) -> bool:
        h = datetime.now(PST).hour
        return any(s <= h < e for s, e in ACTIVE_HOURS_PST)

    def usd_balance(self) -> float:
        try:
            bal = self.api.get_balance()
            return float(bal.get("ZUSD", bal.get("USD", 0.0)))
        except Exception as e:
            log.error(f"Balance fetch failed: {e}")
            return 0.0

    def check_drawdown(self) -> bool:
        usd      = self.usd_balance()
        lost_pct = (STARTING_CAPITAL_USD - usd) / STARTING_CAPITAL_USD
        if lost_pct >= DRAWDOWN_HALT_PCT:
            log.warning(f"⛔ DRAWDOWN HALT — lost {lost_pct*100:.1f}%. Balance=${usd:.2f}. Paused.")
            return False
        return True

    def enter(self, pair: str, score: float, reasons: dict):
        if pair in self.positions:
            return

        avail = self.usd_balance()
        if avail < MIN_TRADE_USD:
            log.warning(f"Insufficient balance (${avail:.2f}) to open {pair}")
            return

        try:
            ticker = self.api.get_ticker(pair)
            ask    = float(ticker["a"][0])
            bid    = float(ticker["b"][0])
            spread = (ask - bid) / bid * 100

            if spread > 0.5:
                log.info(f"  ↳ {pair} spread {spread:.3f}% too wide — skip")
                return

            limit_price = round(ask * (1 + SLIPPAGE_BUFFER), 10)
            trade_usd   = avail * MAX_TRADE_PCT
            volume      = trade_usd / limit_price
            min_vol     = MIN_VOLUMES.get(pair, 0.001)

            if volume < min_vol:
                log.info(f"  ↳ {pair} vol {volume:.6g} < min {min_vol} — skip")
                return

            volume    = round(volume, 8)
            trade_usd = round(volume * limit_price, 4)

            log.info(f"🟢 BUY  {pair}  score={score}  ${trade_usd:.2f} @ {limit_price:.6g}")
            log.info(f"   signals={reasons}")

            result = self.api.place_limit_order(pair, "buy", volume, limit_price)
            txid   = result.get("txid", ["?"])[0]

            pos = Position(pair, limit_price, volume, trade_usd)
            self.positions[pair] = pos
            log.info(f"   ✅ txid={txid}  TSL={pos.trailing_stop:.6g}  TP={pos.take_profit:.6g}")

        except Exception as e:
            log.error(f"enter() error {pair}: {e}")
            self.errors.append(str(e))

    def exit(self, pair: str, reason: str, current_price: float):
        pos = self.positions.get(pair)
        if not pos:
            return
        try:
            ticker      = self.api.get_ticker(pair)
            bid         = float(ticker["b"][0])
            limit_price = round(bid * (1 - SLIPPAGE_BUFFER), 10)

            result  = self.api.place_limit_order(pair, "sell", pos.volume, limit_price)
            txid    = result.get("txid", ["?"])[0]

            pnl_pct = pos.pnl_pct(current_price)
            pnl_usd = pos.usd_val * pnl_pct / 100
            self.total_pnl += pnl_usd

            icon = "✅" if pnl_usd >= 0 else "🔴"
            log.info(
                f"{icon} SELL {pair}  {reason}  "
                f"PnL={pnl_pct:+.2f}% (${pnl_usd:+.4f})  "
                f"entry={pos.entry_price:.6g}  exit={current_price:.6g}  txid={txid}"
            )
            log.info(f"   cumulative PnL=${self.total_pnl:+.4f}")

            if pnl_usd >= 0:
                self.wins += 1
            else:
                self.losses += 1

            self.trade_log.append({
                "pair":    pair,
                "entry":   pos.entry_price,
                "exit":    current_price,
                "volume":  pos.volume,
                "pnl_pct": round(pnl_pct, 3),
                "pnl_usd": round(pnl_usd, 5),
                "reason":  reason,
                "opened":  pos.opened_at.isoformat(),
                "closed":  datetime.now(PST).isoformat(),
            })
            del self.positions[pair]

        except Exception as e:
            log.error(f"exit() error {pair}: {e}")
            self.errors.append(str(e))

    def manage_positions(self):
        for pair in list(self.positions.keys()):
            pos = self.positions[pair]
            try:
                ticker = self.api.get_ticker(pair)
                price  = float(ticker["c"][0])
                pos.ratchet(price)

                if price <= pos.trailing_stop:
                    self.exit(pair, "trailing_stop", price)
                elif price >= pos.take_profit:
                    self.exit(pair, "take_profit", price)
                else:
                    log.info(
                        f"  📊 {pair}  price={price:.6g}  "
                        f"PnL={pos.pnl_pct(price):+.2f}%  "
                        f"TSL={pos.trailing_stop:.6g}"
                    )
            except Exception as e:
                log.error(f"manage_positions() error {pair}: {e}")

    def scan(self) -> list:
        signals = []
        for pair in PAIRS:
            try:
                candles = self.api.get_ohlc(pair, OHLC_INTERVAL)
                if not candles or len(candles) < MIN_CANDLES:
                    continue
                score, reasons = self.engine.score(candles)
                if score >= 2.5:
                    ticker  = self.api.get_ticker(pair)
                    price   = float(ticker["c"][0])
                    vol_24h = float(ticker["v"][1])
                    signals.append((pair, score, reasons, price, vol_24h))
            except Exception as e:
                log.error(f"scan() error {pair}: {e}")
                time.sleep(1)
        signals.sort(key=lambda x: x[1], reverse=True)
        return signals

    def print_status(self):
        usd   = self.usd_balance()
        total = self.wins + self.losses
        wr    = self.wins / total * 100 if total > 0 else 0
        now   = datetime.now(PST).strftime("%H:%M:%S PST")
        log.info("═" * 60)
        log.info(f"  💰 Balance=${usd:.2f}   Cum PnL=${self.total_pnl:+.4f}")
        log.info(f"  📈 {self.wins}W / {self.losses}L  ({wr:.0f}% WR)   Scans={self.scan_count}")
        log.info(f"  🔓 Open: {list(self.positions.keys()) or 'none'}")
        log.info(f"  🕐 {now}   Active={self.in_active_session()}")
        log.info("═" * 60)

    def shutdown(self):
        log.info("⏹  Shutting down — closing all positions...")
        try:
            self.api.cancel_all()
        except Exception:
            pass
        for pair in list(self.positions.keys()):
            try:
                ticker = self.api.get_ticker(pair)
                price  = float(ticker["c"][0])
                self.exit(pair, "shutdown", price)
            except Exception as e:
                log.error(f"shutdown exit error {pair}: {e}")
        self.print_status()
        with open("trade_log.json", "w", encoding="utf-8") as f:
            json.dump(self.trade_log, f, indent=2)
        log.info("📋 trade_log.json saved.")

    def run(self):
        log.info("🚀 Kraken Trading Bot v2 starting")
        log.info(f"   Capital=${STARTING_CAPITAL_USD}  SL={STOP_LOSS_PCT*100}%  TP={TAKE_PROFIT_PCT*100}%")
        log.info(f"   Pairs={PAIRS}")

        try:
            bal = self.usd_balance()
            log.info(f"   ✅ API connected — USD balance: ${bal:.2f}")
            if bal < MIN_TRADE_USD:
                log.error(f"   ❌ Balance too low (${bal:.2f}). Need >= ${MIN_TRADE_USD}. Exiting.")
                return
        except Exception as e:
            log.error(f"   ❌ API connection failed: {e}")
            log.error("   Check your keys — ensure 'Query Funds' and 'Create Orders' are enabled on Kraken.")
            return

        try:
            while True:
                self.scan_count += 1

                if not self.check_drawdown():
                    time.sleep(300)
                    continue

                if self.positions:
                    self.manage_positions()

                if not self.in_active_session():
                    if self.scan_count % 20 == 0:
                        log.info("💤 Outside active hours — monitoring only")
                    time.sleep(SCAN_INTERVAL_SEC)
                    continue

                if len(self.positions) < MAX_CONCURRENT_POSITIONS:
                    for pair, score, reasons, price, vol24h in self.scan():
                        if pair not in self.positions and len(self.positions) < MAX_CONCURRENT_POSITIONS:
                            log.info(f"🎯 Signal  {pair}  score={score}  price={price:.6g}  vol24h={vol24h:.0f}")
                            self.enter(pair, score, reasons)
                            time.sleep(1.5)

                if self.scan_count % 10 == 0:
                    self.print_status()

                time.sleep(SCAN_INTERVAL_SEC)

        except KeyboardInterrupt:
            self.shutdown()


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    bot = TradingBot()
    bot.run()
