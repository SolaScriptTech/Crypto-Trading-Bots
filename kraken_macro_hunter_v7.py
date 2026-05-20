import asyncio
import os
import time
import logging
import sqlite3
import sys
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from dotenv import load_dotenv
import ccxt.async_support as ccxt
import pandas as pd
import pandas_ta as ta

# ==========================================
# RESEARCH CONFIGURATION (V7-DRY-RUN)
# ==========================================
load_dotenv()

USD_PER_TRADE = 15.0
MAX_POSITIONS = 6
WHITELIST = ["BTC/USD", "ETH/USD", "SENT/USD", "XMN/USD", "ZEC/USD", "FUN/USD"]

TF_MACRO, TF_SETUP, TF_CONFIRM = "1d", "4h", "1h"
STRICT_TIME_STOP_MINUTES = 180 
DB_FILE = "research_v7.db"
HEARTBEAT_INTERVAL_S = 60

# ==========================================
# LOGGING & DB
# ==========================================
def setup_logger(mode_name: str):
    log_file = f"research_{mode_name.lower()}.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        handlers=[logging.FileHandler(log_file), logging.StreamHandler()]
    )
    return logging.getLogger(f"Research_{mode_name}")

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS virtual_positions
                 (symbol TEXT, mode TEXT, qty REAL, entry_price REAL, 
                  peak_price REAL, stop_loss REAL, entry_ts REAL, status TEXT,
                  PRIMARY KEY (symbol, mode))''')
    conn.commit()
    return conn

class ResearchBot:
    def __init__(self, mode: str):
        self.mode = mode.upper()
        self.log = setup_logger(self.mode)
        self.ex = ccxt.kraken({'enableRateLimit': True})
        self.db = init_db()
        self.positions = {}
        self._last_heartbeat = 0

    def load_db_positions(self) -> Dict[str, dict]:
        c = self.db.cursor()
        c.execute("SELECT symbol, qty, entry_price, peak_price, stop_loss, entry_ts FROM virtual_positions WHERE status='OPEN' AND mode=?", (self.mode,))
        return {row[0]: {"qty": row[1], "entry_price": row[2], "peak_price": row[3], "stop_loss": row[4], "entry_ts": row[5]} for row in c.fetchall()}

    def save_position(self, symbol: str, data: dict):
        c = self.db.cursor()
        c.execute('''INSERT INTO virtual_positions (symbol, mode, qty, entry_price, peak_price, stop_loss, entry_ts, status)
                     VALUES (?, ?, ?, ?, ?, ?, ?, 'OPEN')
                     ON CONFLICT(symbol, mode) DO UPDATE SET peak_price=excluded.peak_price, stop_loss=excluded.stop_loss''',
                  (symbol, self.mode, data['qty'], data['entry_price'], data['peak_price'], data['stop_loss'], data['entry_ts']))
        self.db.commit()

    def remove_position(self, symbol: str):
        c = self.db.cursor()
        c.execute("UPDATE virtual_positions SET status='CLOSED' WHERE symbol=? AND mode=?", (symbol, self.mode))
        self.db.commit()
        if symbol in self.positions: del self.positions[symbol]

    async def print_heartbeat(self):
        now = time.time()
        if now - self._last_heartbeat < HEARTBEAT_INTERVAL_S: return
        self._last_heartbeat = now
        if not self.positions:
            self.log.info(f"🧪 [DRY-RUN] {self.mode}: No active trades. Scanning market...")
            return

        self.log.info(f"📊 [DRY-RUN] {self.mode} --- VIRTUAL PnL ---")
        tickers = await self.ex.fetch_tickers(list(self.positions.keys()))
        for symbol, data in self.positions.items():
            current = tickers[symbol]['last']
            pnl = (current - data['entry_price']) / data['entry_price'] * 100
            self.log.info(f"  {symbol:<10} | PnL: {pnl:>+6.2f}% | SL: ${data['stop_loss']:,.4f} | Peak: ${data['peak_price']:,.4f}")

    async def run(self):
        self.log.info(f"🔬 RESEARCH MODE: {self.mode} Initialized. No live orders will be placed.")
        await self.ex.load_markets()
        self.positions = self.load_db_positions()
        while True:
            try:
                await self.print_heartbeat()
                # (Logic for manage_positions and scan_market remains identical to V7 but calls virtual execute_buy/sell)
                await asyncio.sleep(10)
            except KeyboardInterrupt: break
        await self.ex.close()

# Note: Full Logic for analyze_macro and calculate_dynamic_stop remains unchanged to maintain research integrity.
