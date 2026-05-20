import yfinance as yf
import pandas as pd
import numpy as np
import ta
import warnings

# Suppress yfinance warnings for cleaner terminal output
warnings.filterwarnings("ignore")

# List of recognizable tickers extracted from the screenshots
# Mapped to Yahoo Finance ticker format (Symbol-USD)
TICKERS = [
    'MANA-USD', 'CVX-USD', 'GALA-USD', 'BAT-USD', 'COMP-USD', 'RAY-USD', 
    'OP-USD', 'FLOKI-USD', 'GRT-USD', 'TIA-USD', 'JASMY-USD', 'HNT-USD', 
    'LDO-USD', 'ONDO-USD', 'ICP-USD', 'ATOM-USD', 'POL-USD', 'WLD-USD', 
    'QNT-USD', 'ENA-USD', 'KAS-USD', 'ALGO-USD', 'JUP-USD', 'SEI-USD', 
    'STX-USD', 'DASH-USD', 'CAKE-USD', 'XTZ-USD', 'CHZ-USD', 'FET-USD', 
    'ZK-USD', 'STRK-USD', 'ENS-USD', 'WIF-USD', 'SAND-USD', 'AXS-USD', 
    'FLR-USD', 'RENDER-USD', 'XDC-USD', 'FIL-USD', 'APT-USD', 'YFI-USD', 
    'APE-USD', 'QTUM-USD', 'RSR-USD', 'SC-USD', 'ZETA-USD', 'ASTR-USD', 
    'FXS-USD', 'ARKM-USD', 'TON-USD', 'CRO-USD', 'PAXG-USD', 'DOT-USD', 
    'UNI-USD', 'MNT-USD', 'AAVE-USD', 'PEPE-USD', 'KAVA-USD', 'BLUR-USD', 
    'SUSHI-USD', 'DAG-USD', 'REQ-USD'
]

# --- Analysis Parameters ---
HISTORY_PERIOD = "2y"       # Look back 2 years to establish volume and price patterns
DOWN_DAY_THRESHOLD = -0.05  # A daily drop of 5% or more qualifies as a "down day"
REBOUND_WINDOW = 3          # Number of days to look ahead for the maximum rebound peak

def process_ticker(symbol):
    try:
        # Fetch historical daily data silently
        data = yf.download(symbol, period=HISTORY_PERIOD, progress=False)
        
        # Skip if coin lacks sufficient history
        if data.empty or len(data) < 30:
            return None
            
        # Ensure single-level columns if yfinance returns a multi-index dataframe
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.droplevel(1)
            
        # --- Calculate Technical Indicators using the 'ta' library ---
        
        # 1. RSI (Relative Strength Index)
        data['RSI'] = ta.momentum.RSIIndicator(close=data['Close'], window=14).rsi()
        
        # 2. MACD (Moving Average Convergence Divergence)
        macd = ta.trend.MACD(close=data['Close'])
        data['MACD'] = macd.macd()
        data['MACD_Signal'] = macd.macd_signal()
        data['MACD_Hist'] = macd.macd_diff()
        
        # 3. Bollinger Bands (%B) - Measures where the price is relative to the bands
        bb = ta.volatility.BollingerBands(close=data['Close'], window=20, window_dev=2)
        data['BB_pband'] = bb.bollinger_pband()
        
        # --- Pattern Recognition & Rebound Calculation ---
        
        # Calculate daily percentage returns
        data['Daily_Return'] = data['Close'].pct_change()
        
        # Identify historical "down days"
        data['Is_Down_Day'] = data['Daily_Return'] <= DOWN_DAY_THRESHOLD
        
        # For each day, find the maximum high reached over the next 3 days
        data['High_T1'] = data['High'].shift(-1)
        data['High_T2'] = data['High'].shift(-2)
        data['High_T3'] = data['High'].shift(-3)
        data['Max_High_Next_3'] = data[['High_T1', 'High_T2', 'High_T3']].max(axis=1)
        
        # Calculate the potential rebound percentage if bought exactly at the down day's close
        data['Rebound_Pct'] = (data['Max_High_Next_3'] / data['Close']) - 1
        
        # Filter down to only the days that crashed to analyze subsequent action
        down_days = data[data['Is_Down_Day']]
        
        if down_days.empty:
            return None
            
        # Aggregate historical statistics
        avg_rebound = down_days['Rebound_Pct'].mean() * 100
        max_rebound = down_days['Rebound_Pct'].max() * 100
        win_rate = (down_days['Rebound_Pct'] > 0).mean() * 100
        total_down_days = len(down_days)
        
        # Grab the absolute latest available metrics (today)
        current = data.iloc[-1]
        
        return {
            'Symbol': symbol,
            'Historical_Down_Days': total_down_days,
            'Avg_Rebound_%': round(avg_rebound, 2),
            'Max_Rebound_%': round(max_rebound, 2),
            'Rebound_Win_Rate_%': round(win_rate, 2),
            'Current_Return_%': round(current['Daily_Return'] * 100, 2),
            'Current_RSI': round(current['RSI'], 2),
            'Current_MACD_Hist': round(current['MACD_Hist'], 4),
            'Current_BB_%B': round(current['BB_pband'], 2)
        }
        
    except Exception:
        # Fails silently for unavailable or untrackable obscure tokens
        return None

def main():
    print("Fetching historical data and calculating indicator patterns. This may take a minute...\n")
    results = []
    
    for ticker in TICKERS:
        res = process_ticker(ticker)
        if res is not None:
            results.append(res)
            
    if not results:
        print("Failed to retrieve data for the provided symbols. Check network or tickers.")
        return
        
    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(results)
    
    # Sort by the highest average historical rebound to rank robust reversal candidates
    df = df.sort_values(by='Avg_Rebound_%', ascending=False)
    
    print("--- TOP 10 COINS BY HISTORICAL REBOUND MAGNITUDE ---")
    print("These assets historically bounce back the hardest within 3 days following a 5%+ daily drop.\n")
    print(df.head(10).to_string(index=False))
    
    print("\n\n--- TODAY'S MOST OVERSOLD CANDIDATES ---")
    print("Filtering the top historical rebounders for coins that are CURRENTLY showing oversold conditions (RSI < 40).\n")
    
    # Filter strictly for coins showing heavily oversold signals today
    oversold = df[df['Current_RSI'] < 40]
    
    if oversold.empty:
        print("No coins from the list are currently registering an RSI below 40.")
    else:
        # Sort these candidates by how oversold they are
        oversold = oversold.sort_values(by='Current_RSI', ascending=True)
        columns_to_show = ['Symbol', 'Current_RSI', 'Current_BB_%B', 'Avg_Rebound_%', 'Current_Return_%']
        print(oversold[columns_to_show].head(10).to_string(index=False))

if __name__ == "__main__":
    main()