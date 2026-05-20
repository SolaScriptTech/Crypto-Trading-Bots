import asyncio
import time
import logging
import sqlite3

import ccxt.async_support as ccxt

# ==========================================
# V6 TAPE SNIFFER CONFIGURATION
# ==========================================
WHITELIST = ["BTC/USD", "ETH/USD", "SENT/USD", "XMN/USD", "ZEC/USD", "FUN/USD"]
DB_FILE = "v6_tape_sniffer.db"
POLL_INTERVAL = 60  # Poll the tape every 60 seconds

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | TAPE | %(message)s",
    handlers=[logging.FileHandler("v6_tape_sniffer.log"), logging.StreamHandler()]
)
log = logging.getLogger("TapeSniffer")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS trade_flow
                 (timestamp REAL, symbol TEXT, total_trades INTEGER, 
                  buy_volume REAL, sell_volume REAL, volume_delta REAL)''')
    conn.commit()
    return conn

class V6TapeSniffer:
    def __init__(self):
        self.ex = ccxt.kraken({'enableRateLimit': True})
        self.db = init_db()

    async def analyze_tape(self, symbol: str):
        """Fetches recent trades and calculates aggressive buy vs sell volume."""
        try:
            # Fetch the last 200 trades
            trades = await self.ex.fetch_trades(symbol, limit=200)
            if not trades:
                return
            
            buy_vol = 0.0
            sell_vol = 0.0
            total_trades = len(trades)

            for t in trades:
                side = t.get('side')
                amount = t.get('amount', 0.0)
                price = t.get('price', 0.0)
                usd_value = amount * price

                if side == 'buy':
                    buy_vol += usd_value
                elif side == 'sell':
                    sell_vol += usd_value

            # Delta is positive if buyers are aggressive, negative if sellers are aggressive
            delta = buy_vol - sell_vol

            # Save to database
            c = self.db.cursor()
            c.execute("INSERT INTO trade_flow VALUES (?, ?, ?, ?, ?, ?)",
                      (time.time(), symbol, total_trades, buy_vol, sell_vol, delta))
            self.db.commit()

            # Format the output for readability
            delta_str = f"+${delta:,.2f}" if delta > 0 else f"-${abs(delta):,.2f}"
            log.info(f"🖨️  {symbol:<10} | Trades: {total_trades:<4} | Buy Vol: ${buy_vol:<9,.0f} | Sell Vol: ${sell_vol:<9,.0f} | Delta: {delta_str}")

        except Exception as e:
            log.error(f"Error reading tape for {symbol}: {e}")

    async def log_trade_flow(self):
        log.info("🔍 Reading the Tape (Actual Filled Orders)...")
        for symbol in WHITELIST:
            await self.analyze_tape(symbol)
            await asyncio.sleep(1) # Respect rate limits

    async def run(self):
        log.info("🕵️‍♂️ V6 TAPE SNIFFER Online. Monitoring aggressive trade flow.")
        while True:
            await self.log_trade_flow()
            await asyncio.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    sniffer = V6TapeSniffer()
    asyncio.run(sniffer.run())
