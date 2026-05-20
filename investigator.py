import ccxt
import pandas as pd
import numpy as np
import sqlite3
import time
import logging
from datetime import datetime
import sys
import os

# --- Configuration ---
DB_NAME = "sine_signals.db"
MIN_SAFE_DURATION = 3      # The "Guarantee": Must never be < 3 candles
MIN_FLIPS = 10             # Sample Size: Must have flipped at least 10 times to be valid
HISTORY_LIMIT = 720        # Lookback window (approx 720 candles)
API_DELAY = 1.2            # Anti-ban timer

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger()

class Investigator:
    def __init__(self):
        self.exchange = ccxt.kraken()
        self.conn = sqlite3.connect(DB_NAME)
        self.cursor = self.conn.cursor()
        self.setup_database()
        
        # The spectrum of timeframes to scour
        self.timeframes = ['1m', '15m', '1h', '4h', '12h', '1d', '3d', '1w', '1M']

    def setup_database(self):
        """Creates the database table if it doesn't exist."""
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS verified_patterns (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT,
                pair TEXT,
                timeframe TEXT,
                lowest_run_observed INTEGER,
                total_flips INTEGER,
                current_status TEXT
            )
        ''')
        self.conn.commit()

    def fetch_data(self, pair, timeframe):
        """Fetches market data with a sleep timer to avoid Kraken bans."""
        try:
            time.sleep(API_DELAY)
            ohlcv = self.exchange.fetch_ohlcv(pair, timeframe, limit=HISTORY_LIMIT)
            if not ohlcv or len(ohlcv) < 50:
                return None
            
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            return df
        except Exception:
            return None

    def analyze(self, df):
        """
        Strict Binary Logic:
        1. Must have flipped colors at least MIN_FLIPS times.
        2. Must NEVER have a run length < MIN_SAFE_DURATION.
        """
        # 1. Determine Candle Color (1=Green, -1=Red)
        conditions = [
            (df['close'] > df['open']),
            (df['close'] < df['open'])
        ]
        choices = [1, -1]
        df['color'] = np.select(conditions, choices, default=0)
        
        # Filter out flat candles and use .copy() to prevent Pandas warnings
        df = df[df['color'] != 0].copy()

        if df.empty:
            return None

        # 2. Run Length Encoding (RLE)
        # Groups consecutive identical colors to find streak lengths
        df['run_id'] = (df['color'] != df['color'].shift()).cumsum()
        run_lengths = df.groupby('run_id')['color'].count()
        
        # --- FILTER 1: Sample Size ---
        # If the coin is just trending (e.g., all Green) and rarely flips, it's not a "sine wave".
        if len(run_lengths) < MIN_FLIPS:
            return None
            
        # --- FILTER 2: Strict Duration ---
        # We verify that EVERY completed run in the history meets the minimum duration.
        # We exclude the last run because it's still "live" (hasn't failed yet).
        completed_runs = run_lengths.iloc[:-1]
        
        if completed_runs.empty:
            return None

        # Find the absolute floor (lowest number of intervals observed)
        min_observed = completed_runs.min()
        
        # FAIL condition: If we ever saw a run shorter than 3, it's out.
        if min_observed < MIN_SAFE_DURATION:
            return None

        # PASS condition
        current_len = run_lengths.iloc[-1]
        current_code = df['color'].iloc[-1]
        current_color = "Green" if current_code == 1 else "Red"
        
        return {
            'lowest_run': int(min_observed),
            'flips': len(run_lengths),
            'status': f"Currently {current_color} for {current_len} candles"
        }

    def log_finding(self, pair, timeframe, data):
        """Logs the successful find to Console and SQL."""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        print(f"!!! FOUND MATCH !!! {pair} [{timeframe}]")
        print(f"    Lowest Interval: {data['lowest_run']} (Guarantee > 2)")
        print(f"    Total Rotations: {data['flips']} (Sample Size >= {MIN_FLIPS})")
        print(f"    {data['status']}")
        
        self.cursor.execute('''
            INSERT INTO verified_patterns (timestamp, pair, timeframe, lowest_run_observed, total_flips, current_status)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (timestamp, pair, timeframe, data['lowest_run'], data['flips'], data['status']))
        self.conn.commit()

    def run(self):
        print(f"--- Investigator Running ---")
        print(f"Target: Kraken (USD & USDT)")
        print(f"Constraint 1: Must never reverse in < {MIN_SAFE_DURATION} intervals.")
        print(f"Constraint 2: Must have flipped at least {MIN_FLIPS} times.")
        
        try:
            markets = self.exchange.load_markets()
            # Explicitly filtering for USD and USDT pairs
            pairs = [s for s in markets if '/USD' in s or '/USDT' in s]
            
            print(f"Scanning {len(pairs)} pairs...")

            for pair in pairs:
                # Visual heartbeat
                print(".", end="", flush=True)
                
                for tf in self.timeframes:
                    df = self.fetch_data(pair, tf)
                    if df is None:
                        continue
                        
                    result = self.analyze(df)
                    if result:
                        self.log_finding(pair, tf, result)
                        
        except KeyboardInterrupt:
            print("\nStopped by user.")
        finally:
            self.conn.close()
            print("\nDatabase connection closed.")

if __name__ == "__main__":
    bot = Investigator()
    bot.run()