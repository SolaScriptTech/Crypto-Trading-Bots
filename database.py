# database.py
import sqlite3
import time
from typing import Optional

DB_FILE = "trading.db"

def get_connection():
    conn = sqlite3.connect(DB_FILE, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_connection()
    cursor = conn.cursor()
    
    # 1. POSITIONS (What we own)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS positions (
        symbol TEXT PRIMARY KEY,
        qty REAL,
        entry_price REAL,
        current_price REAL,
        stop_loss REAL,
        take_profit REAL,
        peak_price REAL,
        entry_time INTEGER,
        status TEXT
    )
    ''')

    # 2. ORDERS (History)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS orders (
        order_id TEXT PRIMARY KEY,
        symbol TEXT,
        side TEXT,
        qty REAL,
        price REAL,
        timestamp INTEGER,
        reason TEXT
    )
    ''')
    
    # 3. WATCHLIST (The Investigator's Findings) <--- NEW!
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS watchlist (
        symbol TEXT PRIMARY KEY,
        timeframe TEXT,
        pattern_score REAL,
        est_hold_time TEXT,
        last_updated INTEGER
    )
    ''')
    
    # 4. LOGS
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp INTEGER,
        module TEXT,
        level TEXT,
        message TEXT
    )
    ''')

    conn.commit()
    conn.close()
    print("✅ Database initialized (trading.db)")

def log(module: str, message: str, level: str = "INFO"):
    try:
        conn = get_connection()
        conn.execute("INSERT INTO logs (timestamp, module, level, message) VALUES (?, ?, ?, ?)",
                     (int(time.time()), module, level, message))
        conn.commit()
        conn.close()
        print(f"[{module}] {message}")
    except Exception as e:
        print(f"❌ LOGGING ERROR: {e}")

# --- WATCHLIST MANAGEMENT ---
def update_watchlist(symbol, timeframe, score, hold_time):
    conn = get_connection()
    conn.execute('''
        INSERT INTO watchlist (symbol, timeframe, pattern_score, est_hold_time, last_updated)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(symbol) DO UPDATE SET
            timeframe=excluded.timeframe,
            pattern_score=excluded.pattern_score,
            est_hold_time=excluded.est_hold_time,
            last_updated=excluded.last_updated
    ''', (symbol, timeframe, score, hold_time, int(time.time())))
    conn.commit()
    conn.close()

# Initialize immediately
init_db()