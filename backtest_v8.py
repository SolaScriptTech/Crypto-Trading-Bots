"""
backtest_v8.py — Kraken V8 Strategy Backtest
=============================================
Backtests the exact V8 signal stack against 90 days of live
Kraken OHLCV data across the top liquid USD pairs.

Signal:
  Entry  — MACD(12,26,9) histogram crosses zero (neg → pos)
             + volume > 1.5× 20-bar rolling average
             + regime != BEAR
  Exit   — MACD histogram flips negative (primary)
             + dynamic capital-scaled trailing stop (secondary)
             + 5% hard stop (always active)

Capital: $100,000 virtual
Sizing:  15% per trade (conviction >= 60), 10% otherwise
Reserve: 20% dry powder
Max pos: 5 simultaneous

Run: python3 backtest_v8.py
Outputs:
  backtest_v8_summary.json
  backtest_v8_trades.csv
  backtest_v8_equity.csv
"""

import ccxt
import pandas as pd
import numpy as np
import json
import os
import time
from datetime import datetime, timezone, timedelta

# ─────────────────────────────────────────────────────────────
# CONFIG — mirrors kraken_v8.py exactly
# ─────────────────────────────────────────────────────────────
STARTING_CAPITAL  = 100_000.0
MAX_POSITIONS     = 5
DRY_POWDER_PCT    = 0.20
HARD_STOP_PCT     = 0.05
SLIPPAGE          = 0.0010
VOL_SPIKE_MULT    = 1.5
MIN_HISTORY_BARS  = 60
SIZE_HIGH_PCT     = 0.15
SIZE_LOW_PCT      = 0.10
MIN_CONVICTION    = 40
COOLDOWN_BARS     = 2        # 2 x 1h candles cooldown after exit
DAYS              = 90

# Pairs to backtest — mirrors V8 universe
PAIRS = [
    'BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'TAO/USD',
    'DOGE/USD', 'HYPE/USD', 'SUI/USD', 'ADA/USD', 'ZEC/USD'
]

# ─────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────
def calc_macd_histogram(closes: pd.Series) -> pd.Series:
    ema12  = closes.ewm(span=12, adjust=False).mean()
    ema26  = closes.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd - signal

def calc_regime(closes: pd.Series) -> pd.Series:
    ema21 = closes.ewm(span=21, adjust=False).mean()
    ema55 = closes.ewm(span=55, adjust=False).mean()
    regime = pd.Series('NEUTRAL', index=closes.index)
    regime[ema21 > ema55] = 'BULL'
    regime[ema21 < ema55] = 'BEAR'
    # Price must be above EMA21 for BULL
    regime[(ema21 > ema55) & (closes < ema21)] = 'NEUTRAL'
    return regime

def calc_rsi(closes: pd.Series, period=14) -> pd.Series:
    delta = closes.diff()
    gain  = delta.clip(lower=0).ewm(com=period-1, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period-1, adjust=False).mean()
    return 100 - 100 / (1 + gain / (loss + 1e-9))

def calc_conviction(hist_val: float, vol_ratio: float, regime: str, rsi_val: float) -> int:
    score = 0
    if hist_val > 0:          score += 30
    if vol_ratio >= 2.5:      score += 30
    elif vol_ratio >= 2.0:    score += 25
    elif vol_ratio >= 1.5:    score += 20
    else:                     score += 10
    if regime == 'BULL':      score += 25
    elif regime == 'NEUTRAL': score += 15
    if rsi_val < 40:          score += 15
    elif rsi_val < 50:        score += 10
    elif rsi_val < 60:        score += 5
    return min(score, 100)

# ─────────────────────────────────────────────────────────────
# DYNAMIC CAPITAL-SCALED TRAILING STOP
# ─────────────────────────────────────────────────────────────
def evaluate_exit(entry_price: float, current_price: float,
                  peak_price: float, size_usd: float) -> tuple:
    """
    Returns (should_exit: bool, reason: str).
    Exact mirror of kraken_v8.py evaluate_tiered_exit().
    """
    loss_pct = (entry_price - current_price) / entry_price
    if loss_pct >= HARD_STOP_PCT:
        return True, f'HARD_STOP_5PCT'

    pnl_pct     = (current_price - entry_price) / entry_price
    current_pnl = size_usd * pnl_pct
    peak_pnl    = size_usd * ((peak_price - entry_price) / entry_price)

    if peak_pnl <= 0:
        return False, 'HOLD_FLAT'

    one_pct_usd = size_usd * 0.01

    if one_pct_usd >= 1000:
        keep_pct, tier = 0.90, 'LARGE'
    elif one_pct_usd >= 600:
        keep_pct, tier = 0.88, 'MID_HIGH'
    elif one_pct_usd >= 300:
        keep_pct, tier = 0.85, 'MID'
    elif one_pct_usd >= 100:
        keep_pct, tier = 0.75, 'SMALL'
    else:
        keep_pct, tier = 0.65, 'MICRO'

    floor_pnl = peak_pnl * keep_pct
    if current_pnl < floor_pnl:
        return True, f'TRAIL_FLOOR_{tier}'

    return False, f'HOLD_{tier}'

# ─────────────────────────────────────────────────────────────
# DATA FETCHER
# ─────────────────────────────────────────────────────────────
def fetch_ohlcv(exchange, symbol: str, days: int = 90) -> pd.DataFrame | None:
    """Fetch days of 1h candles from Kraken public API."""
    since = exchange.parse8601(
        (datetime.now(timezone.utc) - timedelta(days=days + 5)).strftime('%Y-%m-%dT%H:%M:%SZ')
    )
    try:
        print(f"  Fetching {symbol}...", end=' ', flush=True)
        raw = exchange.fetch_ohlcv(symbol, '1h', since=since, limit=days * 24 + 200)
        if raw is None or len(raw) < MIN_HISTORY_BARS:
            print(f"insufficient data ({len(raw) if raw else 0} bars)")
            return None
        df = pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        df['dt'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
        df = df.set_index('dt').sort_index()
        # Trim to exactly 90 days
        cutoff = pd.Timestamp.now(tz='UTC') - pd.Timedelta(days=days)
        df = df[df.index >= cutoff]
        print(f"{len(df)} bars ({df.index[0].strftime('%Y-%m-%d')} → {df.index[-1].strftime('%Y-%m-%d')})")
        time.sleep(1.5)   # rate limit
        return df
    except Exception as e:
        print(f"ERROR: {e}")
        return None

# ─────────────────────────────────────────────────────────────
# SIGNAL PRECOMPUTE
# ─────────────────────────────────────────────────────────────
def precompute_signals(df: pd.DataFrame) -> pd.DataFrame:
    """Add all indicator columns to the dataframe."""
    df = df.copy()
    df['hist']    = calc_macd_histogram(df['c'])
    df['regime']  = calc_regime(df['c'])
    df['rsi']     = calc_rsi(df['c'])
    df['vol_avg'] = df['v'].rolling(20).mean()
    df['vol_ratio'] = df['v'] / (df['vol_avg'] + 1e-9)

    # Entry signal flags — use iloc[-2] equivalent: shift(1) for vectorised
    # hist_prev = histogram one bar before signal bar
    df['hist_prev'] = df['hist'].shift(1)

    # MACD flip: current bar hist > 0, previous bar hist <= 0
    df['macd_flip'] = (df['hist'] > 0) & (df['hist_prev'] <= 0)

    # Volume spike
    df['vol_spike'] = df['vol_ratio'] >= VOL_SPIKE_MULT

    # Combined entry signal (all gates)
    df['entry_signal'] = (
        df['macd_flip'] &
        df['vol_spike'] &
        (df['regime'] != 'BEAR')
    )

    # MACD exit: histogram flips negative
    df['macd_exit'] = df['hist'] < 0

    return df

# ─────────────────────────────────────────────────────────────
# BACKTESTER
# ─────────────────────────────────────────────────────────────
def run_backtest(all_data: dict) -> dict:
    """
    Walk-forward simulation across all pairs simultaneously.
    Respects max positions, dry powder, and cooldowns.
    """
    print("\n" + "=" * 60)
    print("Running walk-forward simulation...")
    print("=" * 60)

    # Build unified timeline of all bars
    all_timestamps = sorted(set(
        ts for df in all_data.values() for ts in df.index
    ))

    cash        = STARTING_CAPITAL
    max_equity  = STARTING_CAPITAL
    positions   = {}   # symbol → {entry_price, size_usd, peak_price, entry_ts, conviction}
    cooldowns   = {}   # symbol → bar index when cooldown expires
    trades      = []
    equity_curve = []

    for bar_idx, ts in enumerate(all_timestamps):

        # ── Compute current equity ────────────────────────────
        pos_val = 0.0
        for sym, pos in positions.items():
            if sym in all_data and ts in all_data[sym].index:
                curr_price = all_data[sym].loc[ts, 'c']
                pos_val += pos['size_usd'] * (curr_price / pos['entry_price'])
            else:
                pos_val += pos['size_usd']   # use cost basis if no price

        equity = cash + pos_val
        max_equity = max(max_equity, equity)
        dd = (max_equity - equity) / max_equity if max_equity > 0 else 0

        equity_curve.append({
            'ts':       ts.strftime('%Y-%m-%dT%H:%M:%S+00:00'),
            'equity':   round(equity, 2),
            'cash':     round(cash, 2),
            'drawdown': round(dd, 4),
            'positions': len(positions),
        })

        # ── Kill switch ───────────────────────────────────────
        if dd >= 0.15:
            print(f"  KILL SWITCH at {ts} — drawdown {dd*100:.1f}%")
            for sym in list(positions.keys()):
                pos = positions.pop(sym)
                if sym in all_data and ts in all_data[sym].index:
                    cp = all_data[sym].loc[ts, 'c']
                else:
                    cp = pos['entry_price']
                ep   = cp * (1 - SLIPPAGE)
                pnl  = pos['size_usd'] * ((ep - pos['entry_price']) / pos['entry_price'])
                cash += pos['size_usd'] * (1 + (ep - pos['entry_price']) / pos['entry_price'])
                trades.append(_trade_row(sym, pos, ep, ts, pnl, 'KILL_SWITCH', bar_idx))
            break

        # ── EXIT EVALUATION ───────────────────────────────────
        for sym in list(positions.keys()):
            if sym not in all_data or ts not in all_data[sym].index:
                continue

            pos        = positions[sym]
            row        = all_data[sym].loc[ts]
            curr_price = row['c']

            # Update peak price
            pos['peak_price'] = max(pos.get('peak_price', pos['entry_price']), curr_price)

            exit_reason = None

            # 1. MACD flip negative → exit
            if row['macd_exit']:
                exit_reason = 'MACD_FLIP_NEGATIVE'

            # 2. Dynamic trailing stop
            if exit_reason is None:
                should_exit, tier_reason = evaluate_exit(
                    pos['entry_price'], curr_price,
                    pos['peak_price'], pos['size_usd']
                )
                if should_exit:
                    exit_reason = tier_reason

            if exit_reason:
                ep   = curr_price * (1 - SLIPPAGE)
                pnl  = pos['size_usd'] * ((ep - pos['entry_price']) / pos['entry_price'])
                cash += pos['size_usd'] * (1 + (ep - pos['entry_price']) / pos['entry_price'])
                trades.append(_trade_row(sym, pos, ep, ts, pnl, exit_reason, bar_idx))
                positions.pop(sym)
                cooldowns[sym] = bar_idx + COOLDOWN_BARS

        # ── ENTRY SCAN ────────────────────────────────────────
        deployed = sum(p['size_usd'] for p in positions.values())
        total_capital = cash + deployed
        max_deploy    = total_capital * (1 - DRY_POWDER_PCT)
        available     = max(0.0, max_deploy - deployed)

        if len(positions) < MAX_POSITIONS and available >= 500:
            candidates = []

            for sym, df in all_data.items():
                if sym in positions:
                    continue
                if cooldowns.get(sym, 0) > bar_idx:
                    continue
                if ts not in df.index:
                    continue

                row = df.loc[ts]

                # Use penultimate candle logic:
                # in walk-forward, the 'current' bar IS the signal bar
                # so entry_signal already uses shifted values
                if not row['entry_signal']:
                    continue

                conviction = calc_conviction(
                    row['hist'], row['vol_ratio'],
                    row['regime'], row['rsi']
                )
                if conviction < MIN_CONVICTION:
                    continue

                candidates.append((conviction, sym, row))

            # Sort by conviction, take best
            candidates.sort(key=lambda x: x[0], reverse=True)

            for conviction, sym, row in candidates:
                if len(positions) >= MAX_POSITIONS:
                    break

                deployed_now = sum(p['size_usd'] for p in positions.values())
                avail_now    = max(0.0, max_deploy - deployed_now)
                if avail_now < 500:
                    break

                pct  = SIZE_HIGH_PCT if conviction >= 60 else SIZE_LOW_PCT
                size = min(total_capital * pct, avail_now)
                if size < 100:
                    continue

                entry_price = row['c'] * (1 + SLIPPAGE)
                positions[sym] = {
                    'entry_price': entry_price,
                    'peak_price':  entry_price,
                    'size_usd':    size,
                    'conviction':  conviction,
                    'entry_ts':    ts,
                    'regime':      row['regime'],
                    'vol_ratio':   round(row['vol_ratio'], 2),
                    'hist':        round(row['hist'], 6),
                }
                cash -= size

    # ── Close any remaining open positions at last price ─────
    last_ts = all_timestamps[-1]
    for sym in list(positions.keys()):
        pos = positions.pop(sym)
        if sym in all_data and last_ts in all_data[sym].index:
            cp = all_data[sym].loc[last_ts, 'c']
        else:
            cp = pos['entry_price']
        ep  = cp * (1 - SLIPPAGE)
        pnl = pos['size_usd'] * ((ep - pos['entry_price']) / pos['entry_price'])
        cash += pos['size_usd'] * (1 + (ep - pos['entry_price']) / pos['entry_price'])
        trades.append(_trade_row(sym, pos, ep, last_ts, pnl, 'END_OF_DATA', len(all_timestamps)-1))

    return {
        'trades':       trades,
        'equity_curve': equity_curve,
        'final_equity': cash,
        'max_equity':   max_equity,
    }

def _trade_row(sym, pos, exit_price, exit_ts, pnl_usd, reason, bar_idx):
    peak_pnl = pos['size_usd'] * ((pos['peak_price'] - pos['entry_price']) / pos['entry_price'])
    return {
        'symbol':      sym,
        'entry_time':  pos['entry_ts'].strftime('%Y-%m-%dT%H:%M:%S+00:00') if hasattr(pos['entry_ts'], 'strftime') else str(pos['entry_ts']),
        'exit_time':   exit_ts.strftime('%Y-%m-%dT%H:%M:%S+00:00') if hasattr(exit_ts, 'strftime') else str(exit_ts),
        'entry_price': round(pos['entry_price'], 6),
        'exit_price':  round(exit_price, 6),
        'size_usd':    round(pos['size_usd'], 2),
        'pnl_usd':     round(pnl_usd, 2),
        'pnl_pct':     round((exit_price - pos['entry_price']) / pos['entry_price'] * 100, 4),
        'peak_pnl_usd': round(peak_pnl, 2),
        'conviction':  pos['conviction'],
        'regime':      pos['regime'],
        'vol_ratio':   pos['vol_ratio'],
        'reason':      reason,
    }

# ─────────────────────────────────────────────────────────────
# SUMMARY STATS
# ─────────────────────────────────────────────────────────────
def compute_summary(trades: list, equity_curve: list,
                    final_equity: float) -> dict:
    if not trades:
        return {'error': 'no trades'}

    df       = pd.DataFrame(trades)
    wins     = df[df['pnl_usd'] > 0]
    losses   = df[df['pnl_usd'] <= 0]
    eq       = pd.DataFrame(equity_curve)['equity']
    returns  = eq.pct_change().dropna()
    sharpe   = (returns.mean() / (returns.std() + 1e-9)) * np.sqrt(24 * 365) if len(returns) > 1 else 0

    # Max drawdown
    roll_max = eq.cummax()
    dd_series = (roll_max - eq) / roll_max
    max_dd   = dd_series.max() * 100

    # Profit factor
    gross_profit = wins['pnl_usd'].sum() if len(wins) else 0
    gross_loss   = abs(losses['pnl_usd'].sum()) if len(losses) else 1
    pf           = gross_profit / gross_loss if gross_loss > 0 else 0

    # Exit reason breakdown
    exit_counts = df['reason'].value_counts().to_dict()

    # Per-symbol breakdown
    by_symbol = {}
    for sym in df['symbol'].unique():
        sdf = df[df['symbol'] == sym]
        by_symbol[sym] = {
            'trades':      len(sdf),
            'wins':        int((sdf['pnl_usd'] > 0).sum()),
            'total_pnl':   round(sdf['pnl_usd'].sum(), 2),
            'win_rate':    round((sdf['pnl_usd'] > 0).mean() * 100, 1),
            'best_trade':  round(sdf['pnl_usd'].max(), 2),
            'worst_trade': round(sdf['pnl_usd'].min(), 2),
        }

    total_return = (final_equity - STARTING_CAPITAL) / STARTING_CAPITAL * 100

    return {
        'period_days':      DAYS,
        'starting_capital': STARTING_CAPITAL,
        'final_equity':     round(final_equity, 2),
        'total_pnl_usd':    round(final_equity - STARTING_CAPITAL, 2),
        'total_return_pct': round(total_return, 3),
        'trades':           len(df),
        'wins':             len(wins),
        'losses':           len(losses),
        'win_rate_pct':     round(len(wins) / len(df) * 100, 1) if len(df) else 0,
        'avg_win_usd':      round(wins['pnl_usd'].mean(), 2) if len(wins) else 0,
        'avg_loss_usd':     round(losses['pnl_usd'].mean(), 2) if len(losses) else 0,
        'largest_win_usd':  round(df['pnl_usd'].max(), 2),
        'largest_loss_usd': round(df['pnl_usd'].min(), 2),
        'profit_factor':    round(pf, 3),
        'sharpe_ratio':     round(float(sharpe), 3),
        'max_drawdown_pct': round(float(max_dd), 3),
        'exit_reasons':     exit_counts,
        'by_symbol':        by_symbol,
        'signal':           'MACD(12,26,9) histogram flip + 1.5x volume spike',
        'exit':             'Dynamic capital-scaled trail + MACD flip negative + 5% hard stop',
    }

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("KRAKEN V8 BACKTEST")
    print(f"Signal: MACD(12,26,9) flip + {VOL_SPIKE_MULT}x volume spike")
    print(f"Exit:   Dynamic capital-scaled trailing stop")
    print(f"Period: {DAYS} days | 1h candles")
    print(f"Capital: ${STARTING_CAPITAL:,.0f} | Max positions: {MAX_POSITIONS}")
    print("=" * 60)

    # Load API credentials from .env if present
    api_key    = os.getenv('KRAKEN_API_KEY', '')
    api_secret = os.getenv('KRAKEN_API_SECRET', '')

    exchange = ccxt.kraken({
        'apiKey':    api_key,
        'secret':    api_secret,
        'enableRateLimit': True,
    })

    # ── Fetch data ────────────────────────────────────────────
    print(f"\nFetching {DAYS} days of 1h OHLCV from Kraken...")
    all_data = {}
    for symbol in PAIRS:
        df = fetch_ohlcv(exchange, symbol, DAYS)
        if df is not None and len(df) >= MIN_HISTORY_BARS:
            all_data[symbol] = precompute_signals(df)

    if not all_data:
        print("ERROR: No data fetched. Check your internet connection.")
        return

    print(f"\nLoaded {len(all_data)} pairs successfully.")

    # ── Count signals before running ──────────────────────────
    total_signals = sum(df['entry_signal'].sum() for df in all_data.values())
    print(f"Total entry signals across all pairs: {int(total_signals)}")
    for sym, df in all_data.items():
        n = int(df['entry_signal'].sum())
        if n > 0:
            print(f"  {sym}: {n} signals")

    # ── Run backtest ──────────────────────────────────────────
    results      = run_backtest(all_data)
    trades       = results['trades']
    equity_curve = results['equity_curve']
    final_equity = results['final_equity']

    # ── Compute summary ───────────────────────────────────────
    summary = compute_summary(trades, equity_curve, final_equity)

    # ── Print results ─────────────────────────────────────────
    print("\n" + "=" * 60)
    print("BACKTEST RESULTS")
    print("=" * 60)
    print(f"Total return:    {summary['total_return_pct']:+.2f}%  (${summary['total_pnl_usd']:+,.2f})")
    print(f"Trades:          {summary['trades']}  |  Wins: {summary['wins']}  |  Losses: {summary['losses']}")
    print(f"Win rate:        {summary['win_rate_pct']:.1f}%")
    print(f"Profit factor:   {summary['profit_factor']:.3f}")
    print(f"Sharpe ratio:    {summary['sharpe_ratio']:.3f}")
    print(f"Max drawdown:    {summary['max_drawdown_pct']:.2f}%")
    print(f"Avg win:         ${summary['avg_win_usd']:,.2f}")
    print(f"Avg loss:        ${summary['avg_loss_usd']:,.2f}")
    print(f"Largest win:     ${summary['largest_win_usd']:,.2f}")
    print(f"Largest loss:    ${summary['largest_loss_usd']:,.2f}")
    print("\nExit reasons:")
    for reason, count in summary['exit_reasons'].items():
        print(f"  {reason}: {count}")
    print("\nBy symbol:")
    for sym, stats in summary['by_symbol'].items():
        print(f"  {sym:<15} trades={stats['trades']}  wins={stats['wins']}  "
              f"win_rate={stats['win_rate']}%  pnl=${stats['total_pnl']:+,.2f}")

    # ── Save outputs ──────────────────────────────────────────
    base = os.path.dirname(os.path.abspath(__file__))

    summary_path = os.path.join(base, 'backtest_v8_summary.json')
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    print(f"\nSummary  → {summary_path}")

    trades_path = os.path.join(base, 'backtest_v8_trades.csv')
    if trades:
        pd.DataFrame(trades).to_csv(trades_path, index=False)
        print(f"Trades   → {trades_path}")

    equity_path = os.path.join(base, 'backtest_v8_equity.csv')
    pd.DataFrame(equity_curve).to_csv(equity_path, index=False)
    print(f"Equity   → {equity_path}")

    print("\n" + "=" * 60)
    print("BACKTEST COMPLETE")
    print("=" * 60)


if __name__ == '__main__':
    main()
