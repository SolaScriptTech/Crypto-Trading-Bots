import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from typing import Dict, Optional, List

from dotenv import load_dotenv
import ccxt.async_support as ccxt
import pandas as pd

load_dotenv()

# =========================================================
# BOT CONFIGURATION: $80 PROOF OF CONCEPT
# =========================================================
QUOTE_CCY = "USD"
MAX_POSITIONS = 3       
USD_PER_TRADE = 25.0    

# 1. Market & Liquidity Filters
MIN_QUOTE_VOL = 900_000.0
MAX_QUOTE_VOL = 2_200_000.0
MAX_SPREAD_PCT = 0.55          
MIN_SCANNER_SCORE = 8.0

# 2. Regime Gating
REGIME_TOP_N = 15
REGIME_MIN_AVG_CHANGE = 0.5    

# 3. Explicit Pullback & Recovery Logic
REQUIRED_SETUP = "continuation"
MIN_PULLBACK_PCT = -2.5        
MAX_PULLBACK_PCT = -0.01       
MIN_BOUNCE_PCT = 0.1           

# 4. Anti-Chase Filters
MAX_RET_3_PCT = 1.5            
MAX_RET_5_PCT = 2.5            

# Risk Management 
HARD_STOP_LOSS_PCT = 1.5       
TRAIL_ACTIVATION_PCT = 1.0     
TRAIL_BUFFER_PCT = 0.5         

# =========================================================
# KRAKEN RATE LIMIT SAFE TIMINGS (0.33 pts/sec decay)
# =========================================================
SCAN_EVERY_S = 15.0            # Hunt for new coins every 15s
RISK_LOOP_EVERY_S = 4.0        # Check open positions every 4s
HEARTBEAT_EVERY_S = 30.0       # Debug logging
TICKER_CACHE_TTL_S = 3.5       # Share 1 API call across loops for 3.5s

STATE_FILE = "kraken_live_state.json"
LOG_FILE = "kraken_live_execution.log"
REJECT_FILE = "kraken_rejects_live.jsonl"
BUY_FILE = "kraken_buys_live.jsonl"

# =========================================================
# LOGGING SETUP
# =========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler()]
)
log = logging.getLogger("KrakenPoC")

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

class KrakenPoCBot:
    def __init__(self):
        self.ex: Optional[ccxt.kraken] = None
        self.positions: Dict[str, Position] = {}
        
        self.scan_start_time = 0.0
        self.scan_end_time = 0.0
        self.last_risk = 0.0
        
        self.current_regime = "unknown"
        self.last_scan_candidates_count = 0
        
        self.tickers_cache = {}
        self.tickers_last_fetch = 0.0
        
        self.load_state()

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, "r") as f:
                    data = json.load(f)
                    for sym, pos_data in data.items():
                        self.positions[sym] = Position(**pos_data)
                log.info(f"Loaded {len(self.positions)} active positions from state.")
            except Exception as e:
                log.error(f"Failed to load state: {e}")

    def save_state(self):
        with open(STATE_FILE, "w") as f:
            json.dump({sym: asdict(pos) for sym, pos in self.positions.items()}, f, indent=2)

    def log_reject(self, symbol: str, reason: str, extra_data: dict = None):
        event = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": "candidate_rejected",
            "symbol": symbol,
            "regime": self.current_regime,
            "reason": reason,
            "data": extra_data or {}
        }
        with open(REJECT_FILE, "a") as f:
            f.write(json.dumps(event) + "\n")

    def log_buy(self, symbol: str, amount: float, price: float, score: float, pullback: float):
        event = {
            "ts_utc": datetime.now(timezone.utc).isoformat(),
            "event": "buy_executed",
            "symbol": symbol,
            "amount": amount,
            "price": price,
            "score": score,
            "pullback_pct": pullback,
            "regime": self.current_regime
        }
        with open(BUY_FILE, "a") as f:
            f.write(json.dumps(event) + "\n")

    async def init_exchange(self):
        api_secret = os.getenv("KRAKEN_API_SECRET", os.getenv("KRAKEN_PRIVATE_KEY"))
        self.ex = ccxt.kraken({
            "apiKey": os.getenv("KRAKEN_API_KEY"),
            "secret": api_secret,
            "enableRateLimit": True, 
        })
        await self.ex.load_markets()

    async def get_cached_tickers(self) -> dict:
        now = time.time()
        if now - self.tickers_last_fetch > TICKER_CACHE_TTL_S:
            try:
                self.tickers_cache = await self.ex.fetch_tickers()
                self.tickers_last_fetch = now
            except ccxt.RateLimitExceeded:
                log.warning("Rate limit hit fetching global tickers. Serving stale cache.")
            except Exception as e:
                log.warning(f"Failed to fetch tickers: {e}")
        return self.tickers_cache

    # =========================================================
    # DEBUG: ASYNC HEARTBEAT
    # =========================================================
    async def debug_heartbeat(self):
        while True:
            await asyncio.sleep(HEARTBEAT_EVERY_S)
            now = time.time()
            scan_latency = now - self.scan_end_time
            scan_duration = self.scan_end_time - self.scan_start_time if self.scan_end_time > self.scan_start_time else 0.0
            risk_latency = now - self.last_risk
            cache_age = now - self.tickers_last_fetch
            tasks = len(asyncio.all_tasks())
            
            log.info(
                f"💓 [HEARTBEAT] "
                f"Regime: {self.current_regime} | "
                f"Pos: {len(self.positions)}/{MAX_POSITIONS} | "
                f"Scan Dur: {scan_duration:.2f}s | "
                f"Cache Age: {cache_age:.2f}s | "
                f"Tasks: {tasks}"
            )
            
            if self.positions:
                for sym, pos in self.positions.items():
                    curr_price = float(self.tickers_cache.get(sym, {}).get("last", pos.entry_price))
                    prof_pct = ((curr_price - pos.entry_price) / pos.entry_price) * 100
                    log.info(f"   -> [HOLDING] {sym} | PnL: {prof_pct:+.2f}% | Trail: {pos.trail_active}")

    # =========================================================
    # CORE LOGIC: SCANNING & FILTERING
    # =========================================================
    async def scan_and_buy(self):
        self.scan_start_time = time.time()
        
        if len(self.positions) >= MAX_POSITIONS:
            self.scan_end_time = time.time()
            return

        tickers = await self.get_cached_tickers()
        candidates = []

        for sym, t in tickers.items():
            if f"/{QUOTE_CCY}" not in sym or sym in self.positions:
                continue
                
            quote_vol = float(t.get("quoteVolume", 0) or 0)
            if not (MIN_QUOTE_VOL <= quote_vol <= MAX_QUOTE_VOL):
                continue

            last = float(t.get("last", 0) or 0)
            open_24h = float(t.get("open", 0) or 0)
            ask = float(t.get("ask", 0) or 0)
            bid = float(t.get("bid", 0) or 0)
            
            if open_24h <= 0 or last <= 0 or bid <= 0:
                continue

            spread_pct = ((ask - bid) / bid) * 100
            if spread_pct > MAX_SPREAD_PCT:
                self.log_reject(sym, "spread_too_high", {"spread": spread_pct, "limit": MAX_SPREAD_PCT})
                continue

            change_pct = ((last - open_24h) / open_24h) * 100
            scanner_score = (change_pct * 1.5) - (spread_pct * 10) 
            
            if scanner_score < MIN_SCANNER_SCORE:
                self.log_reject(sym, "score_too_low", {"score": scanner_score})
                continue
                
            candidates.append({
                "symbol": sym, 
                "score": scanner_score, 
                "change_pct": change_pct,
                "spread_pct": spread_pct,
                "quote_vol": quote_vol,
                "last": last
            })

        candidates.sort(key=lambda x: x["score"], reverse=True)
        self.last_scan_candidates_count = len(candidates)
        
        top_candidates = candidates[:REGIME_TOP_N]
        if top_candidates:
            avg_change = sum(c["change_pct"] for c in top_candidates) / len(top_candidates)
            if avg_change < REGIME_MIN_AVG_CHANGE:
                self.current_regime = "avoid_long"
                log.info(f"🛑 Regime Gating Active: Top {len(top_candidates)} avg change is {avg_change:.2f}%. Halting buys.")
                self.scan_end_time = time.time()
                return
            else:
                self.current_regime = "strong_long"
        else:
            self.current_regime = "avoid_long"
            self.scan_end_time = time.time()
            return

        candidates_to_check = top_candidates[:3] 
        for cand in candidates_to_check:
            if len(self.positions) >= MAX_POSITIONS:
                break
                
            sym = cand["symbol"]
            try:
                # Explicit sleep to guarantee we don't burst the API rate limit during OHLCV checks
                await asyncio.sleep(1.5)
                
                ohlcv = await self.ex.fetch_ohlcv(sym, timeframe="1m", limit=15)
                if len(ohlcv) < 10:
                    self.log_reject(sym, "insufficient_candles")
                    continue
                    
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df["close"] = df["close"].astype(float)
                df["high"] = df["high"].astype(float)
                df["low"] = df["low"].astype(float)
                df["open"] = df["open"].astype(float)
                
                last_close = df["close"].iloc[-1]
                last_open = df["open"].iloc[-1]
                high_8 = df["high"].tail(8).max()
                recent_low_3 = df["low"].tail(3).min()
                
                close_3m_ago = df["close"].iloc[-4]
                close_5m_ago = df["close"].iloc[-6]
                ret_3_pct = ((last_close - close_3m_ago) / close_3m_ago) * 100
                ret_5_pct = ((last_close - close_5m_ago) / close_5m_ago) * 100

                if ret_3_pct > MAX_RET_3_PCT or ret_5_pct > MAX_RET_5_PCT:
                    self.log_reject(sym, "anti_chase_triggered", {"ret_3": ret_3_pct, "ret_5": ret_5_pct})
                    continue

                pullback_pct = ((last_close - high_8) / high_8) * 100
                is_green = last_close > last_open
                setup_type = "continuation" if is_green and df["close"].iloc[-2] > df["open"].iloc[-2] else "reversal"
                
                if setup_type != REQUIRED_SETUP:
                    self.log_reject(sym, "invalid_setup", {"setup": setup_type})
                    continue
                    
                if not (MIN_PULLBACK_PCT <= pullback_pct <= MAX_PULLBACK_PCT):
                    self.log_reject(sym, "pullback_out_of_bounds", {"pullback": pullback_pct})
                    continue
                    
                bounce_pct = ((last_close - recent_low_3) / recent_low_3) * 100
                has_bounced = bounce_pct >= MIN_BOUNCE_PCT
                recovery_ok = is_green and has_bounced and last_close > recent_low_3
                
                if not recovery_ok:
                    self.log_reject(sym, "failed_recovery_check", {"bounce_pct": bounce_pct, "is_green": is_green})
                    continue

                amount = USD_PER_TRADE / last_close
                market = self.ex.market(sym)
                
                amount = self.ex.amount_to_precision(sym, amount)
                amount_float = float(amount)
                
                if amount_float < market['limits']['amount']['min']:
                    log.warning(f"Order size {amount_float} for {sym} is below Kraken minimum.")
                    self.log_reject(sym, "below_exchange_minimum")
                    continue

                log.info(f"🟢 EXECUTING BUY: {sym} | Score: {cand['score']:.1f} | Pullback: {pullback_pct:.2f}% | Bounce: {bounce_pct:.2f}%")
                
                order = await self.ex.create_market_buy_order(sym, amount_float)
                entry_price = float(order.get('average') or order.get('price') or last_close)
                
                self.positions[sym] = Position(
                    symbol=sym,
                    amount=amount_float,
                    entry_price=entry_price,
                    entry_time=time.time(),
                    peak_price=entry_price
                )
                self.save_state()
                self.log_buy(sym, amount_float, entry_price, cand['score'], pullback_pct)
                log.info(f"Entered {sym} at {entry_price:.6f} | Size: {amount_float}")

            except ccxt.RateLimitExceeded:
                log.warning(f"⚠️ RATE LIMIT HIT fetching OHLCV for {sym}. Forcing penalty box sleep.")
                await asyncio.sleep(15.0)
                break
            except Exception as e:
                log.warning(f"Error executing logic for {sym}: {e}")
                self.log_reject(sym, "execution_error", {"error": str(e)})
            
        self.scan_end_time = time.time()

    # =========================================================
    # CORE LOGIC: RISK MANAGEMENT
    # =========================================================
    async def manage_positions(self):
        if not self.positions:
            self.last_risk = time.time()
            return

        tickers = await self.get_cached_tickers()
        
        for sym, pos in list(self.positions.items()):
            if sym not in tickers:
                continue
                
            current_price = float(tickers[sym].get("last", pos.entry_price))
            profit_pct = ((current_price - pos.entry_price) / pos.entry_price) * 100
            
            if current_price > pos.peak_price:
                pos.peak_price = current_price
                self.save_state()

            if not pos.trail_active and profit_pct >= TRAIL_ACTIVATION_PCT:
                pos.trail_active = True
                log.info(f"🛡️ TRAIL ACTIVATED: {sym} at +{profit_pct:.2f}% profit. Peak: {pos.peak_price}")
                self.save_state()

            sell_reason = None
            
            if pos.trail_active:
                trail_stop_price = pos.peak_price * (1 - (TRAIL_BUFFER_PCT / 100))
                if current_price <= trail_stop_price:
                    actual_profit = ((current_price - pos.entry_price) / pos.entry_price) * 100
                    sell_reason = f"Trailing Stop Hit (Locked in {actual_profit:.2f}%)"
            else:
                if profit_pct <= -HARD_STOP_LOSS_PCT:
                    sell_reason = f"Hard Stop Hit ({profit_pct:.2f}%)"

            if sell_reason:
                log.info(f"🔴 EXECUTING SELL {sym} | {sell_reason}")
                try:
                    await self.ex.create_market_sell_order(sym, pos.amount)
                    self.positions.pop(sym)
                    self.save_state()
                    log.info(f"Successfully closed {sym}.")
                except ccxt.RateLimitExceeded:
                    log.error(f"Rate limit blocked selling {sym}! Will retry.")
                    await asyncio.sleep(5.0)
                except Exception as e:
                    log.error(f"Failed to sell {sym}: {e}")
                    
        self.last_risk = time.time()

    # =========================================================
    # MAIN LOOP
    # =========================================================
    async def run(self):
        await self.init_exchange()
        log.info("🚀 Kraken Live Bot Started ($80 PoC Mode - Rate Limit Fixed)")
        log.info(f"Loop Timings: Scan={SCAN_EVERY_S}s | Risk={RISK_LOOP_EVERY_S}s | Cache TTL={TICKER_CACHE_TTL_S}s")
        
        asyncio.create_task(self.debug_heartbeat())

        try:
            while True:
                now = time.time()

                if now - self.last_risk >= RISK_LOOP_EVERY_S:
                    await self.manage_positions()

                if now - self.scan_start_time >= SCAN_EVERY_S and self.scan_end_time >= self.scan_start_time:
                    await self.scan_and_buy()

                await asyncio.sleep(0.1)
                
        finally:
            log.info("Initiating clean shutdown sequence...")
            if self.ex:
                await self.ex.close()
                log.info("Kraken exchange connection successfully closed. Goodnight.")

if __name__ == "__main__":
    bot = KrakenPoCBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Bot stopped by user via KeyboardInterrupt.")
