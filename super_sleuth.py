import asyncio
import ccxt.pro as ccxt
import pandas as pd
import pandas_ta as ta
import sqlite3
import numpy as np
from datetime import datetime

# --- DATABASE ENGINE ---
def init_db():
    conn = sqlite3.connect('super_sleuth.db', check_same_thread=False)
    conn.execute('PRAGMA journal_mode=WAL;') # Performance for high-speed logging
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sleuth_logs
                 (timestamp TEXT, symbol TEXT, timeframe TEXT, 
                  signal_type TEXT, score INTEGER, layers_hit TEXT)''')
    conn.commit()
    return conn

class SuperSleuth:
    def __init__(self):
        self.exchange = ccxt.kraken({'enableRateLimit': True})
        self.db = init_db()
        # Timeframes for multi-layer stacking
        self.timeframes = ['1m', '15m', '1h', '4h', '12h', '1d', '1w', '1M']

    async def get_data(self, symbol, tf):
        try:
            ohlcv = await self.exchange.fetch_ohlcv(symbol, tf, limit=300)
            df = pd.DataFrame(ohlcv, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            return df
        except Exception:
            return None

    def analyze(self, df):
        """ The Core Multi-Logic Engine """
        # --- Indicators Setup ---
        # Trend: EMAs 20, 50, 200 + ADX + MACD
        df.ta.ema(length=20, append=True)
        df.ta.ema(length=50, append=True)
        df.ta.ema(length=200, append=True)
        df.ta.adx(append=True)
        df.ta.macd(append=True)
        
        # Momentum/Volatility: RSI, BBands, ATR
        df.ta.rsi(length=14, append=True)
        df.ta.bbands(length=20, std=2, append=True)
        df.ta.atr(length=14, append=True)
        
        # Context: VWAP
        df['vwap'] = (df['volume'] * (df['high'] + df['low'] + df['close']) / 3).cumsum() / df['volume'].cumsum()

        # --- Scoring Logic ---
        score = 0
        hits = []
        l = df.iloc[-1]  # Latest candle
        p = df.iloc[-2]  # Previous candle

        # 1. Trend Direction Pattern
        if l['EMA_20'] > l['EMA_50'] > l['EMA_200'] and l['ADX_14'] > 25:
            score += 2
            hits.append("Strong_Trend_Alignment")

        # 2. Candlestick Signals (Engulfing + Volume)
        if l['close'] > p['open'] and l['open'] < p['close'] and l['volume'] > p['volume']:
            score += 3
            hits.append("Bullish_Engulfing_High_Vol")

        # 3. Reversal Patterns (BB Rejection + RSI)
        if l['low'] < l['BBL_20_2.0'] and l['close'] > l['BBL_20_2.0'] and l['RSI_14'] < 30:
            score += 4
            hits.append("Oversold_BB_Rejection")

        # 4. Breakout Structure (BB Squeeze + ATR Rising)
        bb_width = l['BBU_20_2.0'] - l['BBL_20_2.0']
        prev_bb_width = p['BBU_20_2.0'] - p['BBL_20_2.0']
        if bb_width > prev_bb_width and l['ATRr_14'] > p['ATRr_14'] and l['close'] > l['BBU_20_2.0']:
            score += 5
            hits.append("Volatility_Breakout")

        # 5. Context Pattern (VWAP & Support)
        if l['close'] > l['vwap'] and p['close'] < l['vwap']:
            score += 2
            hits.append("VWAP_Cross_Above")

        return score, "|".join(hits)

    async def sleuth_symbol(self, symbol):
        """ Stacks across timeframes to confirm shift in control """
        stack_results = {}
        total_conviction = 0

        for tf in self.timeframes:
            df = await self.get_data(symbol, tf)
            if df is not None and len(df) > 200:
                score, layers = self.analyze(df)
                if score > 0:
                    stack_results[tf] = {"score": score, "layers": layers}
                    total_conviction += score

        # Multi-Timeframe Confirmation Logic
        # A reversal on 1D + 4H is a heavy signal.
        if total_conviction >= 10:
            self.log_signal(symbol, total_conviction, stack_results)
            print(f"[!] {symbol} Detected. Conviction: {total_conviction}")

    def log_signal(self, symbol, total_score, results):
        cursor = self.db.cursor()
        layers_summary = str(results)
        cursor.execute('''INSERT INTO sleuth_logs 
                          (timestamp, symbol, timeframe, signal_type, score, layers_hit) 
                          VALUES (?, ?, ?, ?, ?, ?)''', 
                       (datetime.now().isoformat(), symbol, "MULTI", "Convergence", total_score, layers_summary))
        self.db.commit()

    async def run(self):
        markets = await self.exchange.load_markets()
        # Focusing on high-volume USD & USDT pairs to ensure the technicals are valid
        symbols = [s for s in markets.keys() if '/USD' in s or '/USDT' in s]
        
        while True:
            # Task-based execution to handle processing power requirements
            tasks = [self.sleuth_symbol(s) for s in symbols]
            await asyncio.gather(*tasks)
            await asyncio.sleep(60)

if __name__ == "__main__":
    sleuth = SuperSleuth()
    try:
        asyncio.run(sleuth.run())
    except KeyboardInterrupt:
        print("Shutting down.")