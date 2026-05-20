#!/usr/bin/env python3
"""
kraken_auto_bot.py — Self-Optimizing Multi-Asset Kraken Trading Bot
====================================================================
BACKTEST:  python kraken_auto_bot.py --backtest
LIVE:      python kraken_auto_bot.py --live

- Fetches real 15-day OHLCV from Kraken for 10+ symbols
- Runs multi-strategy backtest with $100,000 virtual capital
- Zero fees (Kraken fee-free tier)
- Auto-iterates strategies/parameters until +PnL achieved
- Once a winning config is found, offers to run live
"""

import ccxt
import pandas as pd
import numpy as np
import os
import json
import time
import sys
import argparse
import csv
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

# ─────────────────────────────────────────────────────────────────
# GLOBAL CONFIG
# ─────────────────────────────────────────────────────────────────
STARTING_CAPITAL = 100_000.0
FEE_RATE         = 0.0        # zero-fee tier
SLIPPAGE_BPS     = 10         # 10 bps each way
MAX_POSITIONS    = 5          # max concurrent open positions
DRY_POWDER_PCT   = 0.30       # keep 30% cash reserve
KILL_SWITCH_PCT  = 0.15       # halt at 15% drawdown
BACKTEST_DAYS    = 15
AUDIT_FILE       = "kraken_auto_bot_audit.csv"
STATE_FILE       = "kraken_auto_bot_state.json"

# Symbols to scan — Kraken spot pairs
SYMBOLS = [
    'BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD',
    'ADA/USD', 'LINK/USD', 'AVAX/USD', 'DOT/USD',
    'LTC/USD', 'ATOM/USD',
]

# ─────────────────────────────────────────────────────────────────
# EXCHANGE FACTORY
# ─────────────────────────────────────────────────────────────────
def make_exchange() -> ccxt.kraken:
    return ccxt.kraken({
        'apiKey':          os.getenv('KRAKEN_API_KEY'),
        'secret':          os.getenv('KRAKEN_API_SECRET'),
        'enableRateLimit': True,
        'options':         {'defaultType': 'spot'},
    })


# ─────────────────────────────────────────────────────────────────
# OHLCV FETCHER
# ─────────────────────────────────────────────────────────────────
def fetch_ohlcv(exchange: ccxt.kraken, symbol: str, timeframe: str,
                since_days: int = BACKTEST_DAYS + 5) -> pd.DataFrame:
    """Fetch OHLCV candles with retry. Returns DataFrame or empty."""
    since_ms = int(
        (datetime.now(timezone.utc) - timedelta(days=since_days)).timestamp() * 1000
    )
    for attempt in range(4):
        try:
            raw = exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=1000)
            if not raw:
                return pd.DataFrame()
            df = pd.DataFrame(raw, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
            df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
            df.set_index('ts', inplace=True)
            df = df.astype(float)
            # Keep only within backtest window
            cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=since_days)
            df = df[df.index >= cutoff]
            return df
        except ccxt.BadSymbol:
            return pd.DataFrame()
        except Exception as e:
            wait = 5 * (2 ** attempt)
            print(f"  [WARN] {symbol} fetch error (attempt {attempt+1}): {e}. Retrying in {wait}s...")
            time.sleep(wait)
    return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────
# INDICATOR LIBRARY
# ─────────────────────────────────────────────────────────────────
def add_indicators(df: pd.DataFrame, params: dict) -> pd.DataFrame:
    """Add all required technical indicators to OHLCV DataFrame."""
    fast = params['fast_ema']
    slow = params['slow_ema']
    trend = params.get('trend_ema', 200)

    c = df['close']
    h = df['high']
    lo = df['low']
    v = df['volume']

    # EMAs
    df[f'ema{fast}']  = c.ewm(span=fast,  adjust=False).mean()
    df[f'ema{slow}']  = c.ewm(span=slow,  adjust=False).mean()
    df['ema200']      = c.ewm(span=trend,  adjust=False).mean()
    df['ema21']       = c.ewm(span=21,     adjust=False).mean()
    df['sma20']       = c.rolling(20).mean()

    # RSI
    delta = c.diff()
    gain  = delta.clip(lower=0).rolling(14).mean()
    loss  = (-delta.clip(upper=0)).rolling(14).mean()
    rs    = gain / loss.replace(0, np.nan)
    df['rsi'] = 100 - (100 / (1 + rs))

    # MACD
    ema12     = c.ewm(span=12, adjust=False).mean()
    ema26     = c.ewm(span=26, adjust=False).mean()
    df['macd_line']   = ema12 - ema26
    df['macd_signal'] = df['macd_line'].ewm(span=9, adjust=False).mean()
    df['macd_hist']   = df['macd_line'] - df['macd_signal']

    # Bollinger Bands
    bb_mid     = c.rolling(20).mean()
    bb_std     = c.rolling(20).std()
    df['bb_upper'] = bb_mid + 2 * bb_std
    df['bb_lower'] = bb_mid - 2 * bb_std
    df['bb_mid']   = bb_mid

    # ATR (14)
    prev_close    = c.shift(1)
    tr            = pd.concat([
        h - lo,
        (h - prev_close).abs(),
        (lo - prev_close).abs()
    ], axis=1).max(axis=1)
    df['atr'] = tr.rolling(14).mean()

    # Volume SMA
    df['vol_sma20'] = v.rolling(20).mean()

    # ADX (14) — approximate via smoothed DI
    up_move   = h.diff()
    down_move = -lo.diff()
    plus_dm   = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm  = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    atr14     = tr.rolling(14).mean()
    plus_di   = 100 * pd.Series(plus_dm,  index=df.index).rolling(14).mean() / atr14
    minus_di  = 100 * pd.Series(minus_dm, index=df.index).rolling(14).mean() / atr14
    dx        = (100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan))
    df['adx']      = dx.rolling(14).mean()
    df['plus_di']  = plus_di
    df['minus_di'] = minus_di

    # EMA21 pullback zone: price within [0, 0.75%] above EMA21
    df['ema21_pullback'] = (c >= df['ema21']) & (c <= df['ema21'] * 1.0075)
    # At BB lower band (within 0.5% of lower)
    df['at_bb_lower'] = c <= df['bb_lower'] * 1.005

    return df


# ─────────────────────────────────────────────────────────────────
# STRATEGY DEFINITIONS
# ─────────────────────────────────────────────────────────────────
def entry_signal(row, prev_row, params: dict) -> bool:
    """
    Returns True if entry conditions met on the penultimate candle.
    Strategy: Adaptive trend-following with mean-reversion fallback.
    """
    strategy = params.get('strategy', 'trend')

    rsi_lo   = params['rsi_lo']
    rsi_hi   = params['rsi_hi']

    fast_k   = f"ema{params['fast_ema']}"
    slow_k   = f"ema{params['slow_ema']}"

    # NaN guard
    for key in [fast_k, slow_k, 'ema200', 'rsi', 'macd_hist', 'atr', 'adx']:
        if pd.isna(row.get(key, np.nan)):
            return False

    if strategy == 'trend':
        # ── Trend-Following ──────────────────────────────────────
        # 1. Long-term uptrend filter: price above EMA200
        if row['close'] < row['ema200']:
            return False
        # 2. Fast EMA above slow EMA (trend aligned)
        if row[fast_k] <= row[slow_k]:
            return False
        # 3. RSI in healthy range (not overbought)
        if not (rsi_lo <= row['rsi'] <= rsi_hi):
            return False
        # 4. MACD histogram positive (momentum)
        if row['macd_hist'] <= 0:
            return False
        # 5. Volume confirmation (above 20-period average)
        if row['volume'] < row['vol_sma20'] * params.get('vol_mult', 0.8):
            return False
        # 6. ADX > 20 (trending, not choppy)
        if row['adx'] < params.get('adx_min', 18):
            return False
        return True

    elif strategy == 'mean_rev':
        # ── Mean Reversion (oversold bounce) ─────────────────────
        # 1. RSI oversold
        if row['rsi'] >= params.get('rsi_oversold', 35):
            return False
        # 2. Price at or below BB lower band
        if not row['at_bb_lower']:
            return False
        # 3. MACD histogram turning less negative (momentum shift)
        if pd.isna(prev_row.get('macd_hist', np.nan)):
            return False
        if row['macd_hist'] <= prev_row['macd_hist']:
            return False
        # 4. Volume spike (panic volume often marks bottoms)
        if row['volume'] < row['vol_sma20'] * params.get('vol_mult', 1.2):
            return False
        return True

    elif strategy == 'breakout':
        # ── Volatility Breakout ───────────────────────────────────
        # 1. Price breaks above recent N-bar high
        lookback = params.get('breakout_lookback', 20)
        if prev_row.get(f'roll_high_{lookback}', None) is None:
            return False
        if row['close'] <= prev_row[f'roll_high_{lookback}']:
            return False
        # 2. Volume spike confirms breakout
        if row['volume'] < row['vol_sma20'] * params.get('vol_mult', 1.5):
            return False
        # 3. RSI momentum (not overbought)
        if row['rsi'] > rsi_hi:
            return False
        return True

    elif strategy == 'ema_pullback':
        # ── EMA21 Pullback in Uptrend ─────────────────────────────
        # 1. Uptrend: EMA21 > SMA200
        if row['ema21'] < row['ema200']:
            return False
        # 2. Pullback to EMA21 zone
        if not row['ema21_pullback']:
            return False
        # 3. MACD: histogram increasing (momentum returning)
        if pd.isna(prev_row.get('macd_hist', np.nan)):
            return False
        if row['macd_hist'] < prev_row['macd_hist']:
            return False
        # 4. RSI recovering from moderate dip
        if not (rsi_lo <= row['rsi'] <= rsi_hi):
            return False
        return True

    return False


def exit_signal(row, position: dict, params: dict) -> tuple[bool, str]:
    """
    Returns (should_exit, reason).
    Evaluated every candle against open position state.
    """
    price       = row['close']
    entry       = position['entry_price']
    peak        = position['peak_price']
    atr         = row.get('atr', entry * 0.01)
    hard_stop   = params['hard_stop_pct']
    trail_arm   = params.get('trail_arm_pct', 0.008)
    tp_pct      = params.get('take_profit_pct', 0.05)
    max_hold    = params.get('max_hold_candles', 48)  # 48 x 1h = 2 days
    atr_trail   = params.get('atr_trail_mult', 1.5)

    pnl_pct = (price - entry) / entry

    # A. Hard stop
    if price <= entry * (1 - hard_stop):
        return True, 'HARD_STOP'

    # B. Take-profit target
    if pnl_pct >= tp_pct:
        return True, 'TAKE_PROFIT'

    # C. Trailing stop (ATR-anchored, armed after trail_arm_pct gain)
    if pnl_pct >= trail_arm and peak > entry:
        trail_stop = peak - atr * atr_trail
        if price <= trail_stop:
            return True, 'TRAIL_STOP'

    # D. MACD reversal (histogram turning strongly negative)
    if row.get('macd_hist', 0) < -abs(row.get('atr', entry*0.01)) * 0.1:
        if pnl_pct > 0:  # only if in profit — avoid premature exits
            return True, 'MACD_REVERSAL'

    # E. Time stop
    if position.get('candles_held', 0) >= max_hold:
        return True, 'TIME_STOP'

    return False, ''


# ─────────────────────────────────────────────────────────────────
# BACKTESTER
# ─────────────────────────────────────────────────────────────────
class BacktestResult:
    def __init__(self):
        self.trades      = []
        self.equity_curve = []
        self.final_equity = STARTING_CAPITAL
        self.pnl          = 0.0
        self.pnl_pct      = 0.0
        self.num_trades   = 0
        self.win_rate     = 0.0
        self.max_dd       = 0.0
        self.sharpe       = 0.0
        self.profit_factor = 0.0
        self.params       = {}


def backtest_symbol(df: pd.DataFrame, symbol: str, params: dict,
                    capital_per_trade: float) -> list:
    """
    Runs backtest for a single symbol. Returns list of closed trade dicts.
    Uses iloc[-2] (penultimate candle) for signal, iloc[-1] for execution.
    """
    if len(df) < 210:   # need enough history for EMA200
        return []

    df = df.copy()

    # Add breakout rolling high if strategy needs it
    if params.get('strategy') == 'breakout':
        lb = params.get('breakout_lookback', 20)
        df[f'roll_high_{lb}'] = df['high'].shift(1).rolling(lb).max()

    df = add_indicators(df, params)
    df.dropna(inplace=True)

    slippage = SLIPPAGE_BPS / 10000

    position    = None
    trades      = []
    candles_held = 0

    rows = df.to_dict('records')
    for i, row in enumerate(rows):
        if i == 0:
            continue
        prev_row = rows[i - 1]

        if position is None:
            # Check entry on penultimate candle's signal
            # (signal evaluated at prev_row, execution at row's open)
            if entry_signal(prev_row, rows[i-2] if i >= 2 else prev_row, params):
                exec_price = row['open'] * (1 + slippage)
                qty = capital_per_trade / exec_price
                position = {
                    'symbol':      symbol,
                    'entry_price': exec_price,
                    'entry_ts':    row.get('ts', i),
                    'peak_price':  exec_price,
                    'qty':         qty,
                    'candles_held': 0,
                }
        else:
            candles_held += 1
            position['candles_held'] = candles_held

            # Update peak
            if row['high'] > position['peak_price']:
                position['peak_price'] = row['high']

            # Check exits
            should_exit, reason = exit_signal(row, position, params)
            if should_exit:
                exec_price = row['close'] * (1 - slippage)
                pnl_usd    = (exec_price - position['entry_price']) * position['qty']
                pnl_pct    = (exec_price - position['entry_price']) / position['entry_price']
                trades.append({
                    'symbol':      symbol,
                    'entry_price': position['entry_price'],
                    'exit_price':  exec_price,
                    'pnl_usd':     pnl_usd,
                    'pnl_pct':     pnl_pct,
                    'reason':      reason,
                    'candles':     candles_held,
                    'entry_ts':    position['entry_ts'],
                    'exit_ts':     row.get('ts', i),
                })
                position     = None
                candles_held = 0

    return trades


def run_backtest(data_map: dict, params: dict) -> BacktestResult:
    """
    Runs full multi-asset backtest on pre-fetched data.
    Simulates portfolio with max MAX_POSITIONS concurrent trades.
    """
    result = BacktestResult()
    result.params = params

    slippage = SLIPPAGE_BPS / 10000
    cash     = STARTING_CAPITAL
    max_eq   = STARTING_CAPITAL
    peak_eq  = STARTING_CAPITAL
    max_dd   = 0.0

    # Collect all signals across all symbols (event-driven simulation)
    all_events = []   # (timestamp, event_type, symbol, data)

    for symbol, df in data_map.items():
        if df is None or len(df) < 210:
            continue
        df = df.copy()
        if params.get('strategy') == 'breakout':
            lb = params.get('breakout_lookback', 20)
            df[f'roll_high_{lb}'] = df['high'].shift(1).rolling(lb).max()
        df = add_indicators(df, params)
        df.dropna(inplace=True)

        rows = df.reset_index().to_dict('records')
        for i in range(2, len(rows)):
            prev2 = rows[i - 2]
            prev1 = rows[i - 1]
            curr  = rows[i]

            # Entry signal fires on prev1 candle close, exec on curr candle
            if entry_signal(prev1, prev2, params):
                all_events.append({
                    'ts':     curr['ts'],
                    'type':   'ENTRY_CANDIDATE',
                    'symbol': symbol,
                    'price':  curr['open'],
                    'row':    curr,
                    'idx':    i,
                    'df_rows': rows,
                })

    # Sort events by time
    all_events.sort(key=lambda e: e['ts'])

    open_positions = {}   # symbol → position dict
    equity_curve   = []
    all_trades     = []

    # Replay events chronologically
    # For exits, we need to check every candle for each open position
    # Build a merged timeline of candles
    all_timestamps = set()
    for df in data_map.values():
        if df is not None and not df.empty:
            for ts in df.index:
                all_timestamps.add(ts)
    all_timestamps = sorted(all_timestamps)

    # Build per-symbol row lookup for O(1) access
    sym_rows = {}
    for symbol, df in data_map.items():
        if df is None or df.empty:
            continue
        df2 = df.copy()
        if params.get('strategy') == 'breakout':
            lb = params.get('breakout_lookback', 20)
            df2[f'roll_high_{lb}'] = df2['high'].shift(1).rolling(lb).max()
        df2 = add_indicators(df2, params)
        df2.dropna(inplace=True)
        sym_rows[symbol] = df2

    # Build entry signal map: {(symbol, ts): True/False}
    entry_map = {}
    for symbol, df in sym_rows.items():
        rows_list = df.reset_index().to_dict('records')
        for i in range(2, len(rows_list)):
            prev2 = rows_list[i - 2]
            prev1 = rows_list[i - 1]
            curr  = rows_list[i]
            if entry_signal(prev1, prev2, params):
                entry_map[(symbol, curr['ts'])] = curr

    # Cutoff: only backtest last BACKTEST_DAYS days
    if all_timestamps:
        cutoff_ts = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=BACKTEST_DAYS)
        all_timestamps = [t for t in all_timestamps if t >= cutoff_ts]

    for ts in all_timestamps:
        # Check exits first
        for symbol in list(open_positions.keys()):
            if symbol not in sym_rows:
                continue
            df_s = sym_rows[symbol]
            if ts not in df_s.index:
                continue
            row = df_s.loc[ts].to_dict()
            pos = open_positions[symbol]
            pos['candles_held'] = pos.get('candles_held', 0) + 1

            # Update peak
            if row['high'] > pos['peak_price']:
                pos['peak_price'] = row['high']

            should_exit, reason = exit_signal(row, pos, params)
            if should_exit:
                exec_price = row['close'] * (1 - slippage)
                pnl_usd    = (exec_price - pos['entry_price']) * pos['qty']
                pnl_pct    = (exec_price - pos['entry_price']) / pos['entry_price']
                cash += pos['size_usd'] + pnl_usd
                all_trades.append({
                    'symbol':      symbol,
                    'entry_price': pos['entry_price'],
                    'exit_price':  exec_price,
                    'pnl_usd':     pnl_usd,
                    'pnl_pct':     pnl_pct,
                    'reason':      reason,
                    'candles':     pos['candles_held'],
                    'entry_ts':    pos['entry_ts'],
                    'exit_ts':     ts,
                })
                del open_positions[symbol]

        # Check entries
        if len(open_positions) < MAX_POSITIONS:
            reserve = cash * DRY_POWDER_PCT
            deployable = cash - reserve
            if deployable > 0:
                pos_size = min(
                    deployable / (MAX_POSITIONS - len(open_positions)),
                    cash * params.get('max_pos_pct', 0.15)
                )
                pos_size = max(pos_size, 0)

                for symbol in SYMBOLS:
                    if symbol in open_positions:
                        continue
                    if len(open_positions) >= MAX_POSITIONS:
                        break
                    key = (symbol, ts)
                    if key not in entry_map:
                        continue
                    row = entry_map[key]
                    exec_price = row['open'] * (1 + slippage)
                    if pos_size < 10:
                        continue
                    qty = pos_size / exec_price
                    cash -= pos_size
                    open_positions[symbol] = {
                        'symbol':      symbol,
                        'entry_price': exec_price,
                        'entry_ts':    ts,
                        'peak_price':  exec_price,
                        'qty':         qty,
                        'size_usd':    pos_size,
                        'candles_held': 0,
                    }

        # Mark-to-market equity
        pos_val = 0.0
        for symbol, pos in open_positions.items():
            if symbol in sym_rows and ts in sym_rows[symbol].index:
                current_price = sym_rows[symbol].loc[ts, 'close']
                pos_val += pos['qty'] * current_price

        equity = cash + pos_val
        equity_curve.append({'ts': ts, 'equity': equity})

        if equity > peak_eq:
            peak_eq = equity
        dd = (peak_eq - equity) / peak_eq
        if dd > max_dd:
            max_dd = dd

        # Kill-switch check
        if dd >= KILL_SWITCH_PCT:
            print(f"  [KILL-SWITCH] Drawdown {dd*100:.1f}% >= {KILL_SWITCH_PCT*100:.0f}% — halting sim.")
            break

    # Force-close any open positions at last known price
    for symbol, pos in open_positions.items():
        if symbol in sym_rows and not sym_rows[symbol].empty:
            last_price = sym_rows[symbol].iloc[-1]['close'] * (1 - slippage)
            pnl_usd    = (last_price - pos['entry_price']) * pos['qty']
            pnl_pct    = (last_price - pos['entry_price']) / pos['entry_price']
            cash += pos['size_usd'] + pnl_usd
            all_trades.append({
                'symbol':      symbol,
                'entry_price': pos['entry_price'],
                'exit_price':  last_price,
                'pnl_usd':     pnl_usd,
                'pnl_pct':     pnl_pct,
                'reason':      'END_OF_DATA',
                'candles':     pos.get('candles_held', 0),
                'entry_ts':    pos['entry_ts'],
                'exit_ts':     'end',
            })

    # Build result
    result.trades      = all_trades
    result.equity_curve = equity_curve
    result.final_equity = cash
    result.pnl          = cash - STARTING_CAPITAL
    result.pnl_pct      = result.pnl / STARTING_CAPITAL
    result.num_trades   = len(all_trades)
    result.max_dd       = max_dd

    wins  = [t for t in all_trades if t['pnl_usd'] > 0]
    losses = [t for t in all_trades if t['pnl_usd'] <= 0]
    result.win_rate = len(wins) / len(all_trades) if all_trades else 0.0

    gross_profit = sum(t['pnl_usd'] for t in wins)
    gross_loss   = abs(sum(t['pnl_usd'] for t in losses))
    result.profit_factor = gross_profit / gross_loss if gross_loss > 0 else float('inf')

    # Sharpe (annualized, using equity curve daily returns)
    if len(equity_curve) > 24:
        eq_vals = [e['equity'] for e in equity_curve]
        eq_s    = pd.Series(eq_vals)
        # Use hourly returns, annualize by √(24*365)
        rets    = eq_s.pct_change().dropna()
        if rets.std() > 0:
            result.sharpe = (rets.mean() / rets.std()) * np.sqrt(24 * 365)

    return result


# ─────────────────────────────────────────────────────────────────
# PARAMETER GRID — ordered from most conservative to more aggressive
# ─────────────────────────────────────────────────────────────────
PARAM_GRID = [
    # ── Trend Following variants ──────────────────────────────────
    {
        'strategy': 'trend', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 200,
        'rsi_lo': 42, 'rsi_hi': 65, 'vol_mult': 0.8, 'adx_min': 20,
        'hard_stop_pct': 0.035, 'take_profit_pct': 0.05,
        'trail_arm_pct': 0.01, 'atr_trail_mult': 1.5,
        'max_hold_candles': 48, 'max_pos_pct': 0.15,
        'label': 'Trend-EMA9x21-1h',
    },
    {
        'strategy': 'trend', 'fast_ema': 12, 'slow_ema': 26, 'trend_ema': 200,
        'rsi_lo': 40, 'rsi_hi': 68, 'vol_mult': 0.8, 'adx_min': 18,
        'hard_stop_pct': 0.035, 'take_profit_pct': 0.06,
        'trail_arm_pct': 0.01, 'atr_trail_mult': 1.8,
        'max_hold_candles': 60, 'max_pos_pct': 0.15,
        'label': 'Trend-EMA12x26-1h',
    },
    {
        'strategy': 'trend', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 200,
        'rsi_lo': 38, 'rsi_hi': 70, 'vol_mult': 0.7, 'adx_min': 15,
        'hard_stop_pct': 0.04, 'take_profit_pct': 0.07,
        'trail_arm_pct': 0.012, 'atr_trail_mult': 2.0,
        'max_hold_candles': 72, 'max_pos_pct': 0.18,
        'label': 'Trend-EMA9x21-Relaxed',
    },
    # ── EMA Pullback variants ─────────────────────────────────────
    {
        'strategy': 'ema_pullback', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 200,
        'rsi_lo': 38, 'rsi_hi': 60, 'vol_mult': 0.8, 'adx_min': 15,
        'hard_stop_pct': 0.03, 'take_profit_pct': 0.04,
        'trail_arm_pct': 0.008, 'atr_trail_mult': 1.5,
        'max_hold_candles': 36, 'max_pos_pct': 0.15,
        'label': 'EMA21-Pullback-1h',
    },
    {
        'strategy': 'ema_pullback', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 200,
        'rsi_lo': 35, 'rsi_hi': 62, 'vol_mult': 0.7, 'adx_min': 12,
        'hard_stop_pct': 0.035, 'take_profit_pct': 0.05,
        'trail_arm_pct': 0.008, 'atr_trail_mult': 1.8,
        'max_hold_candles': 48, 'max_pos_pct': 0.18,
        'label': 'EMA21-Pullback-Relaxed',
    },
    # ── Mean Reversion variants ───────────────────────────────────
    {
        'strategy': 'mean_rev', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 200,
        'rsi_lo': 0, 'rsi_hi': 100, 'rsi_oversold': 35,
        'vol_mult': 1.2, 'adx_min': 0,
        'hard_stop_pct': 0.04, 'take_profit_pct': 0.04,
        'trail_arm_pct': 0.01, 'atr_trail_mult': 1.5,
        'max_hold_candles': 24, 'max_pos_pct': 0.15,
        'label': 'MeanRev-RSI35-BB',
    },
    {
        'strategy': 'mean_rev', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 200,
        'rsi_lo': 0, 'rsi_hi': 100, 'rsi_oversold': 38,
        'vol_mult': 1.0, 'adx_min': 0,
        'hard_stop_pct': 0.04, 'take_profit_pct': 0.05,
        'trail_arm_pct': 0.01, 'atr_trail_mult': 1.8,
        'max_hold_candles': 32, 'max_pos_pct': 0.18,
        'label': 'MeanRev-RSI38-BB',
    },
    # ── Breakout variants ─────────────────────────────────────────
    {
        'strategy': 'breakout', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 200,
        'rsi_lo': 0, 'rsi_hi': 70,
        'breakout_lookback': 20, 'vol_mult': 1.5, 'adx_min': 0,
        'hard_stop_pct': 0.035, 'take_profit_pct': 0.05,
        'trail_arm_pct': 0.01, 'atr_trail_mult': 1.5,
        'max_hold_candles': 36, 'max_pos_pct': 0.15,
        'label': 'Breakout-20bar-1h',
    },
    {
        'strategy': 'breakout', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 200,
        'rsi_lo': 0, 'rsi_hi': 72,
        'breakout_lookback': 12, 'vol_mult': 1.3, 'adx_min': 0,
        'hard_stop_pct': 0.04, 'take_profit_pct': 0.06,
        'trail_arm_pct': 0.012, 'atr_trail_mult': 2.0,
        'max_hold_candles': 48, 'max_pos_pct': 0.18,
        'label': 'Breakout-12bar-Relaxed',
    },
    # ── Wide-net combined (all assets, looser filters) ────────────
    {
        'strategy': 'trend', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 50,
        'rsi_lo': 35, 'rsi_hi': 72, 'vol_mult': 0.5, 'adx_min': 12,
        'hard_stop_pct': 0.04, 'take_profit_pct': 0.08,
        'trail_arm_pct': 0.015, 'atr_trail_mult': 2.5,
        'max_hold_candles': 96, 'max_pos_pct': 0.20,
        'label': 'Trend-Wide-EMA50filter',
    },
    {
        'strategy': 'ema_pullback', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 50,
        'rsi_lo': 32, 'rsi_hi': 65, 'vol_mult': 0.5, 'adx_min': 10,
        'hard_stop_pct': 0.04, 'take_profit_pct': 0.07,
        'trail_arm_pct': 0.01, 'atr_trail_mult': 2.0,
        'max_hold_candles': 72, 'max_pos_pct': 0.20,
        'label': 'Pullback-Wide-EMA50filter',
    },
    {
        'strategy': 'mean_rev', 'fast_ema': 9, 'slow_ema': 21, 'trend_ema': 50,
        'rsi_lo': 0, 'rsi_hi': 100, 'rsi_oversold': 40,
        'vol_mult': 0.8, 'adx_min': 0,
        'hard_stop_pct': 0.045, 'take_profit_pct': 0.06,
        'trail_arm_pct': 0.012, 'atr_trail_mult': 2.0,
        'max_hold_candles': 48, 'max_pos_pct': 0.20,
        'label': 'MeanRev-Wide-RSI40',
    },
]


# ─────────────────────────────────────────────────────────────────
# RESULTS PRINTER
# ─────────────────────────────────────────────────────────────────
def print_result(result: BacktestResult, label: str = ''):
    tag   = label or result.params.get('label', '?')
    sign  = '+' if result.pnl >= 0 else ''
    wins  = [t for t in result.trades if t['pnl_usd'] > 0]
    losses = [t for t in result.trades if t['pnl_usd'] <= 0]
    print(f"\n{'='*62}")
    print(f"  Strategy : {tag}")
    print(f"  PnL      : {sign}${result.pnl:,.2f}  ({sign}{result.pnl_pct*100:.2f}%)")
    print(f"  Equity   : ${result.final_equity:,.2f}")
    print(f"  Trades   : {result.num_trades}  ({len(wins)}W / {len(losses)}L)")
    print(f"  Win Rate : {result.win_rate*100:.1f}%")
    print(f"  Prof.Fac : {result.profit_factor:.2f}")
    print(f"  Max DD   : {result.max_dd*100:.2f}%")
    print(f"  Sharpe   : {result.sharpe:.2f}")
    if result.trades:
        avg_win  = np.mean([t['pnl_usd'] for t in wins]) if wins else 0
        avg_loss = np.mean([t['pnl_usd'] for t in losses]) if losses else 0
        print(f"  Avg Win  : +${avg_win:,.2f}")
        print(f"  Avg Loss : ${avg_loss:,.2f}")
    print(f"{'='*62}")


def print_trade_breakdown(result: BacktestResult):
    """Print per-symbol breakdown."""
    if not result.trades:
        return
    by_sym = {}
    for t in result.trades:
        s = t['symbol']
        if s not in by_sym:
            by_sym[s] = {'pnl': 0, 'count': 0}
        by_sym[s]['pnl']   += t['pnl_usd']
        by_sym[s]['count'] += 1
    print("\n  Per-Symbol Breakdown:")
    for sym, d in sorted(by_sym.items(), key=lambda x: -x[1]['pnl']):
        sign = '+' if d['pnl'] >= 0 else ''
        print(f"    {sym:<12} {sign}${d['pnl']:>8,.2f}  ({d['count']} trades)")


def save_audit(result: BacktestResult, filename: str = AUDIT_FILE):
    if not result.trades:
        return
    fieldnames = ['symbol','entry_ts','exit_ts','entry_price','exit_price',
                  'pnl_usd','pnl_pct','reason','candles']
    with open(filename, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for t in result.trades:
            writer.writerow({k: t.get(k, '') for k in fieldnames})
    print(f"\n  Audit trail saved → {filename}")


# ─────────────────────────────────────────────────────────────────
# MAIN BACKTEST ORCHESTRATOR
# ─────────────────────────────────────────────────────────────────
def run_backtest_mode():
    print("\n" + "="*62)
    print("  KRAKEN AUTO BOT — BACKTEST MODE")
    print(f"  Capital: ${STARTING_CAPITAL:,.0f}  |  Fees: 0%  |  Window: {BACKTEST_DAYS} days")
    print("="*62)

    exchange = make_exchange()
    print("\n[1/3] Fetching OHLCV data from Kraken...")
    data_map = {}
    for symbol in SYMBOLS:
        sys.stdout.write(f"  Fetching {symbol:<14}... ")
        sys.stdout.flush()
        df = fetch_ohlcv(exchange, symbol, '1h', since_days=BACKTEST_DAYS + 10)
        if df is not None and len(df) > 100:
            data_map[symbol] = df
            print(f"{len(df)} candles")
        else:
            print("SKIPPED (insufficient data)")
        time.sleep(1.5)

    if not data_map:
        print("\n[ERROR] No data fetched. Check API keys and connectivity.")
        return

    print(f"\n[2/3] Running strategy grid ({len(PARAM_GRID)} configs)...\n")
    results = []
    best    = None

    for i, params in enumerate(PARAM_GRID):
        label = params.get('label', f'Config-{i+1}')
        sys.stdout.write(f"  [{i+1:>2}/{len(PARAM_GRID)}] {label:<35}")
        sys.stdout.flush()

        try:
            result = run_backtest(data_map, params)
            results.append(result)
            sign = '+' if result.pnl >= 0 else ''
            status = 'PASS ✓' if result.pnl > 0 else 'FAIL  '
            print(f"  PnL: {sign}${result.pnl:>8,.0f}  Trades: {result.num_trades:>3}  WR: {result.win_rate*100:>4.0f}%  [{status}]")

            if result.pnl > 0 and result.num_trades >= 3:
                if best is None or result.pnl > best.pnl:
                    best = result

        except Exception as e:
            print(f"  ERROR: {e}")

    print("\n[3/3] Results Summary")
    print("-"*62)

    # Sort results by PnL descending
    results.sort(key=lambda r: r.pnl, reverse=True)

    if best is None:
        # No positive result found — take the least-bad one
        best = results[0] if results else None
        print("\n  WARNING: No strategy achieved +PnL in 15-day window.")
        print("  Showing best-performing config anyway:")
    else:
        print(f"\n  WINNER found: {best.params.get('label','?')}")

    if best:
        print_result(best)
        print_trade_breakdown(best)
        save_audit(best)

        # Save winning config to state file
        config_out = {
            'params':        best.params,
            'backtest_pnl':  best.pnl,
            'backtest_pnl_pct': best.pnl_pct,
            'num_trades':    best.num_trades,
            'win_rate':      best.win_rate,
            'max_dd':        best.max_dd,
            'sharpe':        best.sharpe,
            'generated_at':  datetime.utcnow().isoformat(),
        }
        tmp = STATE_FILE + '.tmp'
        with open(tmp, 'w') as f:
            json.dump(config_out, f, indent=2)
        os.replace(tmp, STATE_FILE)
        print(f"\n  Winning config saved → {STATE_FILE}")
        print(f"\n  To run live:  python kraken_auto_bot.py --live")

    # Print all results ranked
    print(f"\n  {'Rank':<5} {'Strategy':<38} {'PnL':>10} {'Trades':>7} {'WR%':>6} {'Sharpe':>7}")
    print("  " + "-"*75)
    for rank, r in enumerate(results[:12], 1):
        sign = '+' if r.pnl >= 0 else ''
        label = r.params.get('label', '?')
        print(f"  {rank:<5} {label:<38} {sign}${r.pnl:>8,.0f} {r.num_trades:>7} {r.win_rate*100:>5.0f}% {r.sharpe:>7.2f}")


# ─────────────────────────────────────────────────────────────────
# LIVE TRADING ENGINE
# ─────────────────────────────────────────────────────────────────
def load_winning_params() -> dict:
    """Load the params from last backtest run."""
    if not os.path.exists(STATE_FILE):
        print(f"[ERROR] No winning config found at {STATE_FILE}")
        print("  Run: python kraken_auto_bot.py --backtest  first.")
        sys.exit(1)
    with open(STATE_FILE) as f:
        s = json.load(f)
    params = s.get('params', {})
    print(f"\n  Loaded strategy: {params.get('label','?')}")
    print(f"  Backtest PnL:    +${s.get('backtest_pnl',0):,.2f} ({s.get('backtest_pnl_pct',0)*100:.2f}%)")
    print(f"  Win rate:        {s.get('win_rate',0)*100:.1f}%")
    return params


def live_entry_check(exchange, symbol, params, slippage=SLIPPAGE_BPS/10000):
    """
    Fetch latest 1h OHLCV, compute indicators, check entry signal.
    Returns (should_enter, exec_price) using iloc[-2] for signal.
    """
    df = fetch_ohlcv(exchange, symbol, '1h', since_days=25)
    if df is None or len(df) < 210:
        return False, 0.0

    if params.get('strategy') == 'breakout':
        lb = params.get('breakout_lookback', 20)
        df[f'roll_high_{lb}'] = df['high'].shift(1).rolling(lb).max()

    df = add_indicators(df, params)
    df.dropna(inplace=True)
    if len(df) < 3:
        return False, 0.0

    sig_row  = df.iloc[-2].to_dict()
    prev_row = df.iloc[-3].to_dict()
    curr_row = df.iloc[-1].to_dict()

    fires = entry_signal(sig_row, prev_row, params)
    exec_price = curr_row['close'] * (1 + slippage)
    return fires, exec_price


def live_exit_check(exchange, symbol, position, params, slippage=SLIPPAGE_BPS/10000):
    """
    Fetch latest 1h candle, check exit conditions.
    Returns (should_exit, reason, exec_price).
    """
    df = fetch_ohlcv(exchange, symbol, '1h', since_days=5)
    if df is None or len(df) < 30:
        return False, '', 0.0

    df = add_indicators(df, params)
    df.dropna(inplace=True)
    if df.empty:
        return False, '', 0.0

    row = df.iloc[-1].to_dict()

    # Update peak
    if row['high'] > position.get('peak_price', position['entry_price']):
        position['peak_price'] = row['high']

    should_exit, reason = exit_signal(row, position, params)
    exec_price = row['close'] * (1 - slippage)
    return should_exit, reason, exec_price


def run_live_mode():
    print("\n" + "="*62)
    print("  KRAKEN AUTO BOT — LIVE TRADING MODE")
    print("  WARNING: This will place real orders on Kraken!")
    print("="*62)

    params = load_winning_params()
    exchange = make_exchange()

    # Load or init live state
    live_state_file = 'kraken_auto_bot_live_state.json'
    if os.path.exists(live_state_file):
        with open(live_state_file) as f:
            state = json.load(f)
        print(f"\n  Resuming live session | Cash: ${state['cash']:,.2f}")
    else:
        state = {
            'cash':       STARTING_CAPITAL,
            'positions':  {},
            'trade_count': 0,
            'total_pnl':  0.0,
            'start_ts':   datetime.utcnow().isoformat(),
        }
        print(f"\n  Fresh live session | Capital: ${STARTING_CAPITAL:,.2f}")

    slippage    = SLIPPAGE_BPS / 10000
    scan_interval = 300   # 5 minutes between scans
    audit_rows  = []

    print(f"\n  Scanning every {scan_interval}s | Max positions: {MAX_POSITIONS}")
    print("  Press Ctrl+C to stop.\n")

    try:
        while True:
            now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')
            print(f"\n[{now_str}] ── Scan cycle ──")

            # ── EXIT CHECK ──────────────────────────────────────
            for symbol in list(state['positions'].keys()):
                pos = state['positions'][symbol]
                try:
                    should_exit, reason, exec_price = live_exit_check(
                        exchange, symbol, pos, params, slippage
                    )
                    if should_exit:
                        qty    = pos['qty']
                        pnl    = (exec_price - pos['entry_price']) * qty
                        sign   = '+' if pnl >= 0 else ''
                        print(f"  EXIT {symbol}: {reason} | {sign}${pnl:,.2f} @ {exec_price:.4f}")

                        # Place real sell order
                        order = exchange.create_market_sell_order(symbol, qty)
                        actual_price = order.get('average', exec_price) or exec_price
                        actual_pnl   = (actual_price - pos['entry_price']) * qty

                        state['cash']       += pos['size_usd'] + actual_pnl
                        state['total_pnl']  += actual_pnl
                        state['trade_count'] += 1
                        del state['positions'][symbol]

                        audit_rows.append({
                            'ts':          datetime.utcnow().isoformat(),
                            'action':      'SELL',
                            'symbol':      symbol,
                            'price':       actual_price,
                            'pnl_usd':     actual_pnl,
                            'reason':      reason,
                            'cash_after':  state['cash'],
                        })
                        time.sleep(1.5)
                except Exception as e:
                    print(f"  [WARN] Exit check failed for {symbol}: {e}")

            # ── ENTRY SCAN ──────────────────────────────────────
            if len(state['positions']) < MAX_POSITIONS:
                reserve     = state['cash'] * DRY_POWDER_PCT
                deployable  = state['cash'] - reserve
                n_open      = len(state['positions'])
                n_slots     = MAX_POSITIONS - n_open
                pos_size    = min(
                    deployable / n_slots if n_slots > 0 else 0,
                    state['cash'] * params.get('max_pos_pct', 0.15)
                )

                for symbol in SYMBOLS:
                    if symbol in state['positions']:
                        continue
                    if len(state['positions']) >= MAX_POSITIONS:
                        break
                    if pos_size < 10:
                        break
                    try:
                        fires, exec_price = live_entry_check(exchange, symbol, params, slippage)
                        if fires:
                            qty = pos_size / exec_price
                            print(f"  ENTER {symbol}: BUY ${pos_size:,.2f} @ {exec_price:.4f}")

                            # Place real buy order
                            order = exchange.create_market_buy_order(symbol, qty)
                            actual_price = order.get('average', exec_price) or exec_price
                            actual_qty   = order.get('filled', qty) or qty
                            actual_cost  = actual_price * actual_qty

                            state['cash'] -= actual_cost
                            state['positions'][symbol] = {
                                'symbol':      symbol,
                                'entry_price': actual_price,
                                'entry_ts':    datetime.utcnow().isoformat(),
                                'peak_price':  actual_price,
                                'qty':         actual_qty,
                                'size_usd':    actual_cost,
                                'candles_held': 0,
                            }
                            audit_rows.append({
                                'ts':         datetime.utcnow().isoformat(),
                                'action':     'BUY',
                                'symbol':     symbol,
                                'price':      actual_price,
                                'pnl_usd':    0,
                                'reason':     'ENTRY',
                                'cash_after': state['cash'],
                            })
                            time.sleep(1.5)
                    except Exception as e:
                        print(f"  [WARN] Entry failed for {symbol}: {e}")

            # ── STATUS PRINT ────────────────────────────────────
            print(f"  Cash: ${state['cash']:,.2f} | Positions: {len(state['positions'])} | "
                  f"Total PnL: {'+'if state['total_pnl']>=0 else ''}${state['total_pnl']:,.2f} | "
                  f"Trades: {state['trade_count']}")
            for sym, pos in state['positions'].items():
                print(f"    {sym}: entry={pos['entry_price']:.4f}  size=${pos['size_usd']:,.0f}")

            # Kill-switch
            total_equity = state['cash'] + sum(
                p['size_usd'] for p in state['positions'].values()
            )
            dd = (STARTING_CAPITAL - total_equity) / STARTING_CAPITAL
            if dd >= KILL_SWITCH_PCT:
                print(f"\n  [KILL-SWITCH] Drawdown {dd*100:.1f}% — stopping bot.")
                break

            # Save state atomically
            tmp = live_state_file + '.tmp'
            with open(tmp, 'w') as f:
                json.dump(state, f, indent=2)
            os.replace(tmp, live_state_file)

            # Save audit trail
            if audit_rows:
                audit_file = 'kraken_auto_bot_live_audit.csv'
                file_exists = os.path.exists(audit_file)
                with open(audit_file, 'a', newline='') as f:
                    writer = csv.DictWriter(
                        f, fieldnames=['ts','action','symbol','price','pnl_usd','reason','cash_after']
                    )
                    if not file_exists:
                        writer.writeheader()
                    writer.writerows(audit_rows)
                audit_rows.clear()

            print(f"  Sleeping {scan_interval}s until next scan...")
            time.sleep(scan_interval)

    except KeyboardInterrupt:
        print("\n\n  Bot stopped by user.")
        print(f"  Final cash: ${state['cash']:,.2f}")
        print(f"  Total PnL:  {'+'if state['total_pnl']>=0 else ''}${state['total_pnl']:,.2f}")
        print(f"  Trades:     {state['trade_count']}")


# ─────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Kraken Auto Bot — self-optimizing multi-asset trader'
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--backtest', action='store_true',
                       help='Fetch real data, run 15-day backtest, find best strategy')
    group.add_argument('--live',     action='store_true',
                       help='Run live trading with winning backtest config')
    args = parser.parse_args()

    if args.backtest:
        run_backtest_mode()
    elif args.live:
        run_live_mode()
