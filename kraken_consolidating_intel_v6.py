import sqlite3
import pandas as pd
import os
from datetime import datetime

# ==========================================
# CONSOLIDATION CONFIGURATION
# ==========================================
DB_HUNTER = "macro_hunter_v6_research.db"
DB_SENTRY = "v_sentry_intelligence.db"
DB_TAPE = "v6_tape_sniffer.db"
OUTPUT_FILE = "master_backtest_analysis.csv"

def consolidate_data():
    print("🔄 Initializing Data Consolidation Engine...")
    
    # 1. Verify databases exist
    if not all([os.path.exists(DB_HUNTER), os.path.exists(DB_SENTRY), os.path.exists(DB_TAPE)]):
        print("❌ ERROR: One or more databases are missing. Let the bots run longer to generate files.")
        return

    # 2. Extract Trade History (The 'What')
    try:
        conn_hunter = sqlite3.connect(DB_HUNTER)
        df_trades = pd.read_sql_query("SELECT * FROM trade_history", conn_hunter)
        conn_hunter.close()
    except Exception as e:
        print(f"❌ ERROR reading Hunter DB: {e}")
        return

    if df_trades.empty:
        print("⚠️ No completed trades found in history yet. Wait for positions to hit a stop or take-profit.")
        return

    # 3. Extract Sentry Intelligence (The 'Context')
    conn_sentry = sqlite3.connect(DB_SENTRY)
    df_sentry = pd.read_sql_query("SELECT * FROM market_intelligence", conn_sentry)
    conn_sentry.close()

    # 4. Extract Tape Flow (The 'Truth')
    conn_tape = sqlite3.connect(DB_TAPE)
    df_tape = pd.read_sql_query("SELECT * FROM trade_flow", conn_tape)
    conn_tape.close()

    print(f"📊 Extracted: {len(df_trades)} Trades | {len(df_sentry)} Sentry Logs | {len(df_tape)} Tape Logs")

    # 5. Time-Series Alignment (The Magic)
    # Sort everything by timestamp to allow pandas to merge "as of" a specific time
    df_trades = df_trades.sort_values('entry_ts')
    df_sentry = df_sentry.sort_values('timestamp')
    df_tape = df_tape.sort_values('timestamp')

    # Merge the Order Book Data (Find the closest Sentry log BEFORE the trade entry)
    merged_1 = pd.merge_asof(
        df_trades, 
        df_sentry, 
        left_on='entry_ts', 
        right_on='timestamp', 
        by='symbol', 
        direction='backward',
        tolerance=300 # Look back up to 5 minutes (300 seconds) for context
    )

    # Merge the Tape Data
    final_df = pd.merge_asof(
        merged_1,
        df_tape,
        left_on='entry_ts',
        right_on='timestamp',
        by='symbol',
        direction='backward',
        tolerance=300,
        suffixes=('', '_tape')
    )

    # 6. Formatting and Cleanup
    # Convert Unix timestamps to human-readable datetime
    final_df['entry_time'] = pd.to_datetime(final_df['entry_ts'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    final_df['exit_time'] = pd.to_datetime(final_df['exit_ts'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')
    
    # Round metrics for clean CSV output
    final_df['pnl_pct'] = final_df['pnl_pct'].round(2)
    final_df['spread_pct'] = final_df['spread_pct'].round(3)
    final_df['buy_pressure_ratio'] = final_df['buy_pressure_ratio'].round(2)
    final_df['bband_width'] = final_df['bband_width'].round(4)
    final_df['rsi_1h'] = final_df['rsi_1h'].round(1)
    final_df['volume_delta'] = final_df['volume_delta'].round(2)

    # Select the columns we actually care about
    cols_to_keep = [
        'id', 'symbol', 'mode', 'entry_time', 'exit_time', 'entry_price', 'exit_price', 
        'pnl_pct', 'exit_reason', 'spread_pct', 'buy_pressure_ratio', 'bband_width', 
        'rsi_1h', 'volume_delta'
    ]
    
    # Filter only columns that exist (in case the 5-min tolerance missed a log)
    existing_cols = [c for c in cols_to_keep if c in final_df.columns]
    final_df = final_df[existing_cols]

    # 7. Export to CSV
    final_df.to_csv(OUTPUT_FILE, index=False)
    print(f"\n✅ Data consolidation complete! Exported to: {OUTPUT_FILE}")
    
    # Quick Terminal Printout
    print("\n📈 QUICK PREVIEW (First 5 Trades):")
    preview_cols = ['symbol', 'pnl_pct', 'buy_pressure_ratio', 'volume_delta']
    print(final_df[[c for c in preview_cols if c in final_df.columns]].head())

if __name__ == "__main__":
    consolidate_data()