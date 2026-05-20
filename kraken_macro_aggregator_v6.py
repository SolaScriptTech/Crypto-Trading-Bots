import asyncio
import os
import time
import logging
import sqlite3
from typing import Tuple

import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta

# ==========================================
# V-SENTRY CONFIGURATION
# ==========================================
WHITELIST = ["BTC/USD", "ETH/USD", "SENT/USD", "XMN/USD", "ZEC/USD", "FUN/USD"]
DB_FILE = "v_sentry_intelligence.db"
POLL_INTERVAL = 30 # High frequency intelligence

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | SENTRY | %(message)s",
    handlers=[logging.FileHandler("v_sentry.log"), logging.StreamHandler()]
)
log = logging.getLogger("VSentry")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS market_intelligence
                 (timestamp REAL, symbol TEXT, spread_pct REAL, 
                  buy_pressure_ratio REAL, bband_width REAL, rsi_1h REAL)''')
    conn.commit()
    return conn

class VSentry:
    def __init__(self):
        self.ex = ccxt.kraken({'enableRateLimit': True})
        self.db = init_db()

    async def get_order_book_pressure(self, symbol: str) -> Tuple[float, float]:
        """Calculates Bid/Ask spread and Buy Pressure (Sum of Bids / Sum of Asks)."""
        book = await self.ex.fetch_order_book(symbol, limit=20)
        best_bid = book['bids'][0][0]
        best_ask = book['asks'][0][0]
        spread = (best_ask - best_bid) / best_bid * 100
        
        sum_bids = sum([b[1] for b in book['bids']])
        sum_asks = sum([a[1] for a in book['asks']])
        pressure = sum_bids / sum_asks if sum_asks > 0 else 1.0
        
        return spread, pressure

    async def log_intelligence(self):
        log.info("📡 Scanning Order Books & Volatility...")
        for symbol in WHITELIST:
            try:
                # 1. Order Book Data
                spread, pressure = await self.get_order_book_pressure(symbol)
                
                # 2. Technical Context (1H)
                ohlcv = await self.ex.fetch_ohlcv(symbol, timeframe='1h', limit=50)
                df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
                df.ta.bbands(length=20, std=2, append=True)
                df.ta.rsi(length=14, append=True)
                
                bb_width = (df['BBU_20_2.0'].iloc[-1] - df['BBL_20_2.0'].iloc[-1]) / df['BBM_20_2.0'].iloc[-1]
                rsi = df['RSI_14'].iloc[-1]

                # Save to Intelligence DB
                c = self.db.cursor()
                c.execute("INSERT INTO market_intelligence VALUES (?, ?, ?, ?, ?, ?)",
                          (time.time(), symbol, spread, pressure, bb_width, rsi))
                self.db.commit()

                log.info(f"📊 {symbol:<10} | Spread: {spread:.3f}% | Pressure: {pressure:.2f} | RSI: {rsi:.1f}")
                
            except Exception as e:
                log.error(f"Error on {symbol}: {e}")
            await asyncio.sleep(1)

    async def run(self):
        log.info("🛡️ V-SENTRY Intelligence Aggregator Online.")
        while True:
            await self.log_intelligence()
            await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    sentry = VSentry()
    asyncio.run(sentry.run())
