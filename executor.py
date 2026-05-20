# executor.py
import ccxt
import os
import time
from dotenv import load_dotenv
import database

load_dotenv()

# MINIMUM TRADE SIZE ($)
# Kraken requires about $10 minimum per trade. 
# We set this to $12 to be safe from price fluctuations.
MIN_TRADE_USD = 12.0

class Executor:
    def __init__(self):
        # 1. Connect to Kraken
        try:
            self.api = ccxt.kraken({
                'apiKey': os.getenv("EXCHANGE_KEY"),
                'secret': os.getenv("EXCHANGE_SECRET"),
                'enableRateLimit': True,
            })
            self.api.load_markets()
        except Exception as e:
            print(f"❌ KRAKEN CONNECTION ERROR: {e}")
            exit()

    def get_price(self, symbol):
        """Fetches the current live price."""
        try:
            ticker = self.api.fetch_ticker(symbol)
            return float(ticker['last'])
        except Exception as e:
            print(f"⚠️ Price Fetch Error ({symbol}): {e}")
            return None

    def execute_buy(self, symbol, usd_amount):
        """Buys a crypto symbol with a USD amount."""
        price = self.get_price(symbol)
        if not price: return

        # Calculate Quantity (Crypto Amount)
        # e.g., $15 / $0.50 price = 30 coins
        qty = usd_amount / price
        
        # Safety: Check Minimums
        # (For now we assume $12+ is safe on Kraken)
        
        print(f"🚀 EXECUTING BUY: {symbol} (${usd_amount:.2f} -> {qty:.4f} coins)")

        try:
            # 1. Place Order on Kraken
            order = self.api.create_market_buy_order(symbol, qty)
            
            # 2. Wait for fill (Confirm we actually bought it)
            time.sleep(1) # Give Kraken a second
            
            # 3. Log to Database
            # We record the entry price so we know when to sell later
            fill_price = float(order.get('price') or price) # Fallback to ticker price if immediate fill price is missing
            
            database.update_position(symbol, qty, fill_price)
            database.log("EXECUTOR", f"BOUGHT {symbol} @ ${fill_price:.4f}")
            print(f"✅ SUCCESS: Bought {symbol} at ${fill_price:.4f}")
            
        except Exception as e:
            print(f"❌ BUY FAILED: {e}")
            database.log("EXECUTOR", f"BUY FAIL {symbol}: {e}", "ERROR")

    def execute_sell(self, symbol, reason="Signal"):
        """Sells entire position of a symbol."""
        # 1. Check Database: Do we own it?
        pos = database.get_position(symbol)
        if not pos:
            print(f"⚠️ CANNOT SELL: We don't own {symbol}")
            return

        qty = pos['qty']
        print(f"🚨 EXECUTING SELL: {symbol} ({qty:.4f} coins) | Reason: {reason}")

        try:
            # 2. Place Sell Order
            self.api.create_market_sell_order(symbol, qty)
            
            # 3. Update Database (Remove position)
            database.delete_position(symbol)
            database.log("EXECUTOR", f"SOLD {symbol} ({reason})")
            print(f"✅ SUCCESS: Sold {symbol}")
            
        except Exception as e:
            print(f"❌ SELL FAILED: {e}")
            database.log("EXECUTOR", f"SELL FAIL {symbol}: {e}", "ERROR")

# --- TEST BLOCK ---
# This runs only when you type 'python executor.py'
if __name__ == "__main__":
    print("💪 Testing Executor Muscle...")
    bot = Executor()
    price = bot.get_price("BTC/USD")
    print(f"   Current Bitcoin Price: ${price}")
    print("✅ Executor is Ready.")