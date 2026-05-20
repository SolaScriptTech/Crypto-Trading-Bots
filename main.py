import os
import ccxt
from dotenv import load_dotenv

load_dotenv("/home/ubuntu/bots/kraken_bot/.env")

api_key = os.getenv("KRAKEN_API_KEY")
api_secret = os.getenv("KRAKEN_API_SECRET")

print("API key loaded:", bool(api_key))
print("API secret loaded:", bool(api_secret))

if not api_key or not api_secret:
    raise SystemExit("Missing Kraken API credentials in .env")

exchange = ccxt.kraken({
    "apiKey": api_key,
    "secret": api_secret,
    "enableRateLimit": True,
})

try:
    balance = exchange.fetch_balance()
    print("Connected to Kraken successfully")
    print("Sample balance keys:", list(balance.keys())[:10])
except Exception as e:
    print("Kraken connection failed:")
    print(type(e).__name__, str(e))
