# trader.py
import time
import database
import buyer
import seller
import investigator
import super_sleuth
from datetime import datetime

class Manager:
    def __init__(self):
        # 1. Initialize the Spectrum Investigator first to establish the API connection
        self.investigator = investigator.SpectrumInvestigator()
        shared_api = self.investigator.api 
        
        # 2. Hand the shared API link to the other specialists (Fixes the TypeError)
        self.sleuth = super_sleuth.SuperSleuth(shared_api) 
        self.buyer = buyer.Buyer(shared_api)
        self.seller = seller.Seller(shared_api)
        
        print("👔 MANAGER: Standing by. Intelligence gathering in progress...")

    def deduce_and_act(self):
        print(f"\n🕵️ [{datetime.now().strftime('%H:%M')}] Gathering intelligence reports...")
        
        # A. Command Detectives to scan the Full Spectrum
        self.investigator.hunt()
        self.sleuth.investigate()

        # B. DEDUCE ENTRIES: Review the "Case Files" in the Watchlist
        conn = database.get_connection()
        best_find = conn.execute("SELECT * FROM watchlist ORDER BY pattern_score DESC LIMIT 1").fetchone()
        
        if best_find and best_find['pattern_score'] > 50:
            symbol = best_find['symbol']
            
            # Check if we already own it
            existing = conn.execute("SELECT * FROM positions WHERE symbol=? AND status='OPEN'", (symbol,)).fetchone()
            
            if not existing:
                # Deduction: Determine the mission based on the "est_hold_time" info
                strategy = "SLEUTH"
                info = str(best_find['est_hold_time'])
                
                if "STREAK_MIN" in info:
                    strategy = "STREAK"
                elif "Cycle" in info:
                    strategy = "RHYTHM"

                print(f"🎯 MANAGER: Deducing {strategy} play for {symbol}. Ordering BUY.")
                if self.buyer.strike(symbol, 15.0):
                    database.save_position(symbol, strategy, best_find['timeframe'])

        # C. DEDUCE EXITS: Manage the Portfolio by re-running Detective logic
        positions = conn.execute("SELECT * FROM positions WHERE status='OPEN'").fetchall()
        for pos in positions:
            symbol = pos['symbol']
            strategy = pos['strategy']
            tf = pos['target_data'] or '1h'
            
            # Fetch fresh data for the specific mission
            df = self.investigator.fetch_data(symbol, tf)
            if df is None or df.empty: continue
            
            # Apply the specific pattern-native exit rule
            if strategy == 'STREAK':
                # Rule: Sell on the first RED candle close
                is_red = df['close'].iloc[-1] < df['open'].iloc[-1]
                if is_red:
                    self.seller.liquidate(symbol, "Streak Broken (Red Candle)")
            
            elif strategy == 'RHYTHM':
                # Rule: Sell at the Wavelength Peak (RSI > 70)
                rsi = self.investigator.calculate_rsi(df['close']).iloc[-1]
                if rsi > 70:
                    self.seller.liquidate(symbol, "Rhythm Peak Reached")
            
            else: # SLEUTH Strategy
                # Rule: Trailing Stop Logic or Reversal Stack crumbling
                if df['close'].iloc[-1] < df['low'].rolling(10).min().iloc[-2]:
                    self.seller.liquidate(symbol, "Sleuth Support Broken")

        conn.close()

    def run(self):
        while True:
            try:
                self.deduce_and_act()
                print(f"⏳ Manager taking a break for 15 minutes...")
                time.sleep(900)
            except Exception as e:
                print(f"❌ MANAGER ERROR: {e}")
                time.sleep(60)

if __name__ == "__main__":
    m = Manager()
    m.run()