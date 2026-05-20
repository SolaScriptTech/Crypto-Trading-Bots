import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, asdict
from typing import Dict, Optional

from dotenv import load_dotenv
import ccxt.async_support as ccxt
import pandas as pd

load_dotenv()

# =========================================================
# BOT CONFIGURATION: $80 PROOF OF CONCEPT
# =========================================================
QUOTE_CCY = "USD"
MAX_POSITIONS = 3       # Limit concurrent trades to avoid draining capital
USD_PER_TRADE = 25.0    # 3 x $25 = $75 used, leaving a $5 buffer for market slippage

# Strict Backtest Filters Derived from Log Data
MIN_QUOTE_VOL = 900_000.0
MAX_QUOTE_VOL = 2_200_000.0
MIN_SCANNER_SCORE = 8.0
MAX_PULLBACK_PCT = -0.01
REQUIRED_SETUP = "continuation"

# Risk Management (Zero-Fee Assumption)
HARD_STOP_LOSS_PCT = 1.5       
TRAIL_ACTIVATION_PCT = 1.0     
TRAIL_BUFFER_PCT = 0.5         

# Ultra-low latency polling (AWS us-east-1 configuration)
SCAN_EVERY_S = 3.0
RISK_LOOP_EVERY_S = 0.5

STATE_FILE = "kraken_live_state.json"
LOG_FILE = "kraken_live_execution.log"

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

    async def init_exchange(self):
        self.ex = ccxt.kraken({
            "apiKey": os.getenv("KRAKEN_API_KEY"),
            "secret": os.getenv("KRAKEN_PRIVATE_KEY"),
            "enableRateLimit": True, 
        })
        await self.ex.load_markets()

    # =========================================================
    # CORE LOGIC: SCANNING & FILTERING
    # =========================================================
    async def scan_and_buy(self):
        if len(self.positions) >= MAX_POSITIONS:
            return

        tickers = await self.ex.fetch_tickers()
        candidates = []

        for sym, t in tickers.items():
            if f"/{QUOTE_CCY}" not in sym or sym in self.positions:
                continue
                
            quote_vol = float(t.get("quoteVolume", 0) or 0)
            
            if not (MIN_QUOTE_VOL <= quote_vol <= MAX_QUOTE_VOL):
                continue

            last = float(t.get("last", 0) or 0)
            open_24h = float(t.get("open", 0) or 0)
            if open_24h <= 0 or last <= 0:
                continue

            change_pct = ((last - open_24h) / open_24h) * 100
            scanner_score = change_pct * 1.5 
            
            if scanner_score < MIN_SCANNER_SCORE:
                continue
                
            candidates.append({"symbol": sym, "score": scanner_score, "last": last})

        candidates.sort(key=lambda x: x["score"], reverse=True)
        candidates = candidates[:3] 

        for cand in candidates:
            if len(self.positions) >= MAX_POSITIONS:
                break
                
            sym = cand["symbol"]
            try:
                ohlcv = await self.ex.fetch_ohlcv(sym, timeframe="1m", limit=15)
                if len(ohlcv) < 10:
                    continue
                    
                df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
                df["close"] = df["close"].astype(float)
                df["high"] = df["high"].astype(float)
                df["open"] = df["open"].astype(float)
                
                last_close = df["close"].iloc[-1]
                last_open = df["open"].iloc[-1]
                high_8 = df["high"].tail(8).max()
                
                pullback_pct = ((last_close - high_8) / high_8) * 100
                is_green = last_close > last_open
                
                setup_type = "continuation" if is_green and df["close"].iloc[-2] > df["open"].iloc[-2] else "reversal"
                
                if setup_type != REQUIRED_SETUP:
                    continue
                    
                if pullback_pct >= MAX_PULLBACK_PCT:
                    continue

                # =========================================================
                # EXECUTE LIVE BUY
                # =========================================================
                amount = USD_PER_TRADE / last_close
                market = self.ex.market(sym)
                
                amount = self.ex.amount_to_precision(sym, amount)
                amount_float = float(amount)
                
                if amount_float < market['limits']['amount']['min']:
                    log.warning(f"Order size {amount_float} for {sym} is below Kraken minimum.")
                    continue

                log.info(f"🟢 EXECUTING BUY: {sym} | Score: {cand['score']:.1f} | Pullback: {pullback_pct:.2f}%")
                
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
                log.info(f"Entered {sym} at {entry_price:.6f} | Size: {amount_float}")

            except Exception as e:
                log.warning(f"Error executing logic for {sym}: {e}")
                
            await asyncio.sleep(self.ex.rateLimit / 1000.0)

    # =========================================================
    # CORE LOGIC: RISK MANAGEMENT
    # =========================================================
    async def manage_positions(self):
        if not self.positions:
            return

        try:
            tickers = await self.ex.fetch_tickers(list(self.positions.keys()))
        except Exception as e:
            log.warning(f"Failed to fetch position tickers: {e}")
            return
        
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
                except Exception as e:
                    log.error(f"Failed to sell {sym}: {e}")

    # =========================================================
    # MAIN LOOP
    # =========================================================
    async def run(self):
        await self.init_exchange()
        log.info("🚀 Kraken Live Bot Started ($80 PoC Mode)")
        log.info(f"Config: {MAX_POSITIONS} positions max, ${USD_PER_TRADE} per trade.")
        
        last_scan = 0.0
        last_risk = 0.0

        while True:
            now = time.time()

            if now - last_risk >= RISK_LOOP_EVERY_S:
                await self.manage_positions()
                last_risk = now

            if now - last_scan >= SCAN_EVERY_S:
                await self.scan_and_buy()
                last_scan = now

            await asyncio.sleep(0.1)

if __name__ == "__main__":
    bot = KrakenPoCBot()
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        log.info("Bot stopped by user.")