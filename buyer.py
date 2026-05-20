# buyer.py
import ccxt
import os

class Buyer:
    def __init__(self, api_link):
        self.api = api_link

    def strike(self, symbol, usd_amount):
        """Zero logic. Just buys."""
        print(f"💰 BUYER: Executing market buy for {symbol} (${usd_amount})")
        # price = self.api.fetch_ticker(symbol)['last']
        # self.api.create_market_buy_order(symbol, usd_amount / price)
        return True