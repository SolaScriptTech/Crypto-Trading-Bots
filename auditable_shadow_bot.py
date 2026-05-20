import ccxt
import pandas as pd
import numpy as np
import time
from datetime import datetime

class MyKrakenIrelandAuditableBot:
    def __init__(self):
        # 1. INFRASTRUCTURE: eu-west-1 (Ireland)
        self.exchange = ccxt.kraken({'enableRateLimit': True})
        self.symbol = 'BTC/USD'
        self.timeframe = '1h'
        self.ms_per_hour = 3600000
        
        # 2. CAPITAL: The $2,000 Starting Block
        self.virtual_usd = 2000.0
        self.virtual_btc = 0.0
        self.initial_capital = 2000.0
        
        # 3. RISK: 15% Max Drawdown Hard-Stop
        self.max_equity = 2000.0
        self.max_dd_limit = 0.15
        
        # 4. MODELING: 10bps Slippage (Covers Spread + Market Impact)
        self.slippage_rate = 0.0010 
        
        self.history = []

    def get_signals(self):
        """Uses the penultimate candle (iloc[-2]) for a confirmed signal."""
        ohlcv = self.exchange.fetch_ohlcv(self.symbol, timeframe=self.timeframe, limit=50)
        df = pd.DataFrame(ohlcv, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        
        # Bollinger Band Setup
        df['sma'] = df['c'].rolling(20).mean()
        df['std'] = df['c'].rolling(20).std()
        df['upper'] = df['sma'] + (df['std'] * 2)
        df['lower'] = df['sma'] - (df['std'] * 2)
        
        closed_candle = df.iloc[-2]
        
        # Delay Calculation: Current Time - Standardized Close Time (T+1h)
        candle_close_ts = closed_candle['ts'] + self.ms_per_hour
        current_ts_ms = time.time() * 1000
        decision_delay = current_ts_ms - candle_close_ts
        
        signal_price = closed_candle['c']
        
        if signal_price < closed_candle['lower']: 
            return "BUY", signal_price, decision_delay
        if signal_price > closed_candle['upper']: 
            return "SELL", signal_price, decision_delay
            
        return "HOLD", signal_price, decision_delay

    def execute_logic(self, action, signal_price, delay):
        exec_price = 0.0
        
        if action == "BUY" and self.virtual_usd > 10:
            # Modeled exec_price includes the slippage 'penalty'
            exec_price = signal_price * (1 + self.slippage_rate)
            self.virtual_btc = self.virtual_usd / exec_price
            self.virtual_usd = 0
            print(f"[{datetime.now().strftime('%H:%M')}] BUY EXEC @ {exec_price:.2f} (Signal: {signal_price:.2f})")
            
        elif action == "SELL" and self.virtual_btc > 0:
            exec_price = signal_price * (1 - self.slippage_rate)
            self.virtual_usd = self.virtual_btc * exec_price
            self.virtual_btc = 0
            print(f"[{datetime.now().strftime('%H:%M')}] SELL EXEC @ {exec_price:.2f} (Signal: {signal_price:.2f})")
            
        return exec_price

    def update_performance(self, current_price, signal_price, exec_price, delay):
        current_equity = self.virtual_usd + (self.virtual_btc * current_price)
        self.max_equity = max(self.max_equity, current_equity)
        drawdown = (self.max_equity - current_equity) / self.max_equity
        
        # CSV Logging with Signal and Exec prices for Auditability
        self.history.append({
            'timestamp': datetime.now(),
            'equity': current_equity,
            'drawdown': drawdown,
            'signal_price': signal_price,
            'exec_price': exec_price,
            'delay_ms': delay
        })
        
        pd.DataFrame(self.history).to_csv('my_kraken_audit_trail.csv', index=False)
        return current_equity, drawdown

    def main(self):
        print(f"--- KRAKEN AUDITABLE SHADOW SYSTEM STARTING ---")
        while True:
            try:
                action, sig_price, delay = self.get_signals()
                exec_price = self.execute_logic(action, sig_price, delay)
                
                # Use current ticker for real-time equity valuation
                ticker = self.exchange.fetch_ticker(self.symbol)
                equity, dd = self.update_performance(ticker['last'], sig_price, exec_price, delay)
                
                print(f"Equity: ${equity:.2f} | DD: {dd*100:.2f}% | Signal: {sig_price:.2f}")
                
                if dd >= self.max_dd_limit:
                    print("STRATEGY TERMINATED: 15% Max Drawdown.")
                    break
                
                # Sleep until top of next hour
                next_hour = (time.time() // 3600 + 1) * 3600
                time.sleep(next_hour - time.time() + 0.5) 
                
            except Exception as e:
                print(f"Runtime Error: {e}")
                time.sleep(10)

if __name__ == "__main__":
    bot = MyKrakenIrelandAuditableBot()
    bot.main()
