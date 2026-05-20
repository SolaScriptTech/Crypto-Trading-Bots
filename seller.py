# seller.py
class Seller:
    def __init__(self, api_link):
        self.api = api_link

    def liquidate(self, symbol, reason):
        """Zero logic. Just sells."""
        print(f"📉 SELLER: Closing {symbol}. Reason: {reason}")
        # self.api.create_market_sell_order(symbol, qty)
        return True