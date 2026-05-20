"""
backtest_goat_v1_2.py
90-day backtest of goat_funded_v1_2.py strategy
$100,000 virtual capital | BTC/USD + ETH/USD 1h candles | 15% max drawdown kill switch

Faithful implementation of all v1.2 logic:
  - BULL/BEAR/NEUTRAL regime (ETH decoupled, BEAR = both bearish, ADX > 15)
  - Full signal suite in NEUTRAL (not just BB_LOWER)
  - Idle guard as independent check (not dead-code elif)
  - V4.1 tiered trail stop + profit floor
  - 10bps slippage both sides
  - Hard stop = 3.5% from peak (not entry)
  - Intra-candle stop checking via candle H/L (sub-loop approximation)
"""

import ccxt
import pandas as pd
import numpy as np
import os
import time
from datetime import datetime, timezone, timedelta

# ── Constants (identical to goat_funded_v1_2.py) ──────────────
STARTING_CAPITAL     = 100_000.0
SLIPPAGE             = 0.0010        # 10bps
STOP_PCT             = 0.035         # 3.5% from peak
MAX_DD_PCT           = 0.15          # 15% kill switch
BB_MULT_BULL         = 1.5
BB_MULT_NEUTRAL      = 2.0
ADX_THRESHOLD        = 15
IDLE_HOURS           = 8

TRAIL_TIERS = [
    (0.012, 0.003),   # gain > 1.2% → 0.3% trail
    (0.007, 0.005),   # gain > 0.7% → 0.5% trail
    (0.003, 0.008),   # gain > 0.3% → 0.8% trail
    (0.000, 0.013),   # gain < 0.3% → 1.3% trail
]
PROFIT_FLOOR_TRIGGER = 0.003
PROFIT_FLOOR_LEVEL   = 0.001

BACKTEST_DAYS        = 90
OUTPUT_DIR           = os.path.dirname(os.path.abspath(__file__))


# ── Helpers ────────────────────────────────────────────────────
def sf(v, d=0.0):
    try:
        f = float(v)
        return d if (np.isnan(f) or np.isinf(f)) else f
    except Exception:
        return d


def calc_adx(df, period=14):
    h, l, c = df['h'], df['l'], df['c']
    pdm = h.diff().clip(lower=0)
    ndm = (-l.diff()).clip(lower=0)
    tr  = pd.concat([(h - l), (h - c.shift()).abs(), (l - c.shift()).abs()],
                    axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    pdi = 100 * pdm.ewm(span=period, adjust=False).mean() / (atr + 1e-9)
    ndi = 100 * ndm.ewm(span=period, adjust=False).mean() / (atr + 1e-9)
    dx  = (pdi - ndi).abs() / (pdi + ndi + 1e-9) * 100
    return dx.ewm(span=period, adjust=False).mean()


def add_indicators(df_btc, df_eth, bb_mult):
    """Compute all indicators in-place and return copies."""
    b = df_btc.copy()
    c = b['c']

    b['sma20'] = c.rolling(20).mean()
    b['std20'] = c.rolling(20).std()
    b['upper'] = b['sma20'] + bb_mult * b['std20']
    b['lower'] = b['sma20'] - bb_mult * b['std20']
    b['ema21'] = c.ewm(span=21, adjust=False).mean()
    b['ema55'] = c.ewm(span=55, adjust=False).mean()

    delta    = c.diff()
    gain     = delta.clip(lower=0).ewm(com=13, adjust=False).mean()
    loss     = (-delta.clip(upper=0)).ewm(com=13, adjust=False).mean()
    b['rsi'] = 100 - 100 / (1 + gain / (loss + 1e-9))
    b['adx'] = calc_adx(df_btc)

    e = df_eth.copy()
    e['ema21'] = e['c'].ewm(span=21, adjust=False).mean()
    e['ema55'] = e['c'].ewm(span=55, adjust=False).mean()
    return b, e


def get_regime(b, e, i):
    """Regime at row i using live candle logic (iloc[-1] equivalent)."""
    btc_ema21 = sf(b['ema21'].iat[i])
    btc_ema55 = sf(b['ema55'].iat[i])
    btc_close = sf(b['c'].iat[i])
    eth_ema21 = sf(e['ema21'].iat[i])
    eth_ema55 = sf(e['ema55'].iat[i])
    adx_val   = sf(b['adx'].iat[i])

    btc_bull = btc_ema21 > btc_ema55 and btc_close > btc_ema21
    btc_bear = btc_ema21 < btc_ema55
    eth_bear = eth_ema21 < eth_ema55

    if btc_bull and adx_val > ADX_THRESHOLD:
        return 'BULL'
    elif btc_bear and eth_bear:
        return 'BEAR'
    return 'NEUTRAL'


def get_entry_signals(b, i):
    """Entry signals from last CLOSED candle (row i = iloc[-2] equivalent)."""
    close    = sf(b['c'].iat[i])
    prev_c   = sf(b['c'].iat[i - 1])
    lower    = sf(b['lower'].iat[i])
    sma20    = sf(b['sma20'].iat[i])
    ema21    = sf(b['ema21'].iat[i])
    ema55    = sf(b['ema55'].iat[i])
    prev_e21 = sf(b['ema21'].iat[i - 1])
    rsi      = sf(b['rsi'].iat[i], 50.0)

    sig_bb    = (not np.isnan(lower))  and (close < lower)
    sig_cross = (prev_c < prev_e21)    and (close > ema21)
    sig_pull  = (not np.isnan(ema21)   and close < ema21
                 and close >= ema21 * 0.9925 and rsi < 52)
    sig_sma   = (not np.isnan(sma20)   and close < sma20 and rsi < 50)
    sig_rsi   = (not np.isnan(ema55)   and rsi < 42 and close > ema55)
    return sig_bb, sig_cross, sig_pull, sig_sma, sig_rsi


def calc_trail_stop(entry_price, peak_price, ever_floor):
    """Returns (stop_price, ever_floor) — identical to live bot logic."""
    gain_pct  = (peak_price - entry_price) / entry_price
    trail_pct = TRAIL_TIERS[-1][1]
    for threshold, pct in TRAIL_TIERS:
        if gain_pct >= threshold:
            trail_pct = pct
            break
    trail_stop = peak_price * (1 - trail_pct)

    if ever_floor or gain_pct >= PROFIT_FLOOR_TRIGGER:
        floor = entry_price * (1 + PROFIT_FLOOR_LEVEL)
        if trail_stop < floor:
            return floor, True
        return trail_stop, True

    return trail_stop, ever_floor


# ── Data fetcher ───────────────────────────────────────────────
def fetch_ohlcv(exchange, symbol, days=95, rl_sleep=1.5):
    """Fetch full historical 1h OHLCV from Kraken public API."""
    since_ms = int((datetime.now(timezone.utc) - timedelta(days=days)).timestamp() * 1000)
    all_candles = []
    while True:
        time.sleep(rl_sleep)
        candles = exchange.fetch_ohlcv(symbol, '1h', since=since_ms, limit=720)
        if not candles:
            break
        all_candles.extend(candles)
        if len(candles) < 720:
            break
        since_ms = candles[-1][0] + 1
    df = pd.DataFrame(all_candles, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
    df = df.drop_duplicates('ts').sort_values('ts').reset_index(drop=True)
    df['dt'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    return df


# ── Main backtest ──────────────────────────────────────────────
def run_backtest():
    exchange = ccxt.kraken({'enableRateLimit': False})

    print("Fetching BTC/USD 1h candles from Kraken…")
    df_btc_raw = fetch_ohlcv(exchange, 'BTC/USD', days=95)
    print(f"  {len(df_btc_raw)} BTC candles fetched")

    print("Fetching ETH/USD 1h candles from Kraken…")
    df_eth_raw = fetch_ohlcv(exchange, 'ETH/USD', days=95)
    print(f"  {len(df_eth_raw)} ETH candles fetched")

    # Align timestamps
    btc_ts = set(df_btc_raw['ts'])
    eth_ts = set(df_eth_raw['ts'])
    common = sorted(btc_ts & eth_ts)
    df_btc_raw = df_btc_raw[df_btc_raw['ts'].isin(common)].reset_index(drop=True)
    df_eth_raw = df_eth_raw[df_eth_raw['ts'].isin(common)].reset_index(drop=True)

    # Determine backtest start (90 days back) with enough warmup for indicators
    cutoff_ms = int((datetime.now(timezone.utc) - timedelta(days=BACKTEST_DAYS)).timestamp() * 1000)
    cutoff_row = df_btc_raw[df_btc_raw['ts'] >= cutoff_ms].index[0]
    warmup = 60
    slice_start = max(0, cutoff_row - warmup)

    df_btc_raw = df_btc_raw.iloc[slice_start:].reset_index(drop=True)
    df_eth_raw = df_eth_raw.iloc[slice_start:].reset_index(drop=True)

    # Precompute indicators for both BB multipliers
    print("Computing indicators…")
    b_bull, e_ind = add_indicators(df_btc_raw, df_eth_raw, BB_MULT_BULL)
    b_neut, _     = add_indicators(df_btc_raw, df_eth_raw, BB_MULT_NEUTRAL)

    # True backtest start index (after warmup)
    bt_start = warmup  # first candle we actually evaluate

    # ── State ──────────────────────────────────────────────────
    virtual_usd   = STARTING_CAPITAL
    virtual_btc   = 0.0
    max_equity    = STARTING_CAPITAL
    peak_price    = 0.0
    entry_price   = 0.0
    ever_floor    = False
    trade_count   = 0
    last_sell_idx = bt_start - IDLE_HOURS - 1   # start as already idle
    entry_ts      = None
    entry_signal  = ''

    trades        = []
    equity_curve  = []
    killed        = False

    print(f"Running backtest: {df_btc_raw['dt'].iloc[bt_start].strftime('%Y-%m-%d')} → "
          f"{df_btc_raw['dt'].iloc[-2].strftime('%Y-%m-%d')}…")

    # Iterate: candle i is the "live" candle; candle i-1 is the last closed candle
    for i in range(bt_start, len(df_btc_raw) - 1):
        row    = df_btc_raw.iloc[i]
        close  = sf(row['c'])
        high   = sf(row['h'])
        low    = sf(row['l'])
        dt     = row['dt']

        # Choose indicators based on regime (BULL uses tighter BB)
        regime_bull = get_regime(b_bull, e_ind, i)
        if regime_bull == 'BULL':
            b_use = b_bull
        else:
            b_use = b_neut

        regime = get_regime(b_use, e_ind, i)

        # Entry signals from last closed candle (i-1)
        sig_bb, sig_cross, sig_pull, sig_sma, sig_rsi = get_entry_signals(b_use, i - 1)

        # BB upper / EMA21 for exit (live candle i)
        live_upper = sf(b_use['upper'].iat[i])
        live_ema21 = sf(b_use['ema21'].iat[i])
        bb_upper_exit = (not np.isnan(live_upper)) and (close > live_upper)

        action     = 'HOLD'
        exec_price = 0.0

        # ── Sub-loop approximation: intra-candle trail / hard stop ─
        if virtual_btc > 0:
            # Update peak with candle high (optimistic — price went up first)
            peak_price = max(peak_price, high)

            hard_stop  = peak_price * (1 - STOP_PCT)
            trail_stop, ever_floor = calc_trail_stop(entry_price, peak_price, ever_floor)
            effective_stop = max(hard_stop, trail_stop)  # whichever is higher protects more

            # Check if low breached stop
            if low <= hard_stop:
                exec_price = hard_stop * (1 - SLIPPAGE)
                action = 'SELL_STOP'
            elif low <= trail_stop:
                exec_price = trail_stop * (1 - SLIPPAGE)
                action = 'SELL_TRAIL'

        # ── Hourly exit checks (if not yet stopped out) ────────────
        if virtual_btc > 0 and action == 'HOLD':
            if regime == 'BEAR':
                exec_price = close * (1 - SLIPPAGE)
                action = 'SELL_REGIME_FLIP'
            elif bb_upper_exit:
                # BULL hold filter: don't sell if price still riding EMA21
                if regime == 'BULL' and close < live_ema21 * 1.01:
                    action = 'HOLD'
                else:
                    exec_price = close * (1 - SLIPPAGE)
                    action = 'SELL_TARGET'

        # ── Hourly entry checks ────────────────────────────────────
        if virtual_btc == 0 and action == 'HOLD':
            idle = (i - last_sell_idx) > IDLE_HOURS

            if regime != 'BEAR':
                if sig_bb:
                    action = 'BUY'; entry_signal = 'BB_LOWER' + ('' if regime == 'BULL' else '_NEUTRAL')
                elif sig_cross:
                    action = 'BUY'; entry_signal = 'EMA21_CROSS' + ('' if regime == 'BULL' else '_NEUTRAL')
                elif sig_pull:
                    action = 'BUY'; entry_signal = 'EMA21_PULL' + ('' if regime == 'BULL' else '_NEUTRAL')
                elif sig_sma:
                    action = 'BUY'; entry_signal = 'SMA20_TOUCH' + ('' if regime == 'BULL' else '_NEUTRAL')
                elif sig_rsi:
                    action = 'BUY'; entry_signal = 'RSI_OVERSOLD' + ('' if regime == 'BULL' else '_NEUTRAL')
                # Idle guard — independent check, not elif
                if action == 'HOLD' and idle:
                    if sig_pull or sig_sma or sig_rsi:
                        action = 'BUY'; entry_signal = 'IDLE_GUARD' + ('' if regime == 'BULL' else '_NEUTRAL')

        # ── Execute ────────────────────────────────────────────────
        if action == 'BUY' and virtual_btc == 0:
            exec_price  = close * (1 + SLIPPAGE)
            virtual_btc = virtual_usd / exec_price
            virtual_usd = 0.0
            peak_price  = exec_price
            entry_price = exec_price
            ever_floor  = False
            trade_count += 1
            entry_ts    = dt

        elif action.startswith('SELL') and virtual_btc > 0:
            if exec_price == 0.0:
                exec_price = close * (1 - SLIPPAGE)
            pnl_usd     = virtual_btc * exec_price - virtual_btc * entry_price
            pnl_pct     = (exec_price - entry_price) / entry_price * 100
            virtual_usd = virtual_btc * exec_price
            virtual_btc = 0.0
            last_sell_idx = i

            trades.append({
                'entry_ts':    entry_ts,
                'exit_ts':     dt,
                'signal':      entry_signal,
                'reason':      action,
                'entry_price': round(entry_price, 2),
                'exec_price':  round(exec_price, 2),
                'pnl_usd':     round(pnl_usd, 2),
                'pnl_pct':     round(pnl_pct, 4),
                'trade_num':   trade_count,
            })

            entry_price  = 0.0
            peak_price   = 0.0
            ever_floor   = False
            entry_ts     = None
            entry_signal = ''

        # ── Equity + drawdown ──────────────────────────────────────
        equity     = virtual_usd + virtual_btc * close
        max_equity = max(max_equity, equity)
        dd         = (max_equity - equity) / max_equity if max_equity > 0 else 0.0

        equity_curve.append({
            'timestamp': dt,
            'price':     round(close, 2),
            'equity':    round(equity, 2),
            'drawdown':  round(dd, 4),
            'regime':    regime,
            'position':  'LONG' if virtual_btc > 0 else 'FLAT',
        })

        # Kill switch
        if dd >= MAX_DD_PCT:
            print(f"\n!!! KILL SWITCH TRIGGERED @ {dt.strftime('%Y-%m-%d %H:%M')} | "
                  f"Drawdown: {dd*100:.1f}%")
            killed = True
            break

    return trades, equity_curve, killed, trade_count


# ── Report ─────────────────────────────────────────────────────
def print_report(trades, equity_curve, killed, trade_count):
    if not equity_curve:
        print("No equity data.")
        return pd.DataFrame(), pd.DataFrame()

    df_eq = pd.DataFrame(equity_curve)
    df_tr = pd.DataFrame(trades) if trades else pd.DataFrame()

    end_eq   = df_eq['equity'].iloc[-1]
    max_eq   = df_eq['equity'].max()
    max_dd   = df_eq['drawdown'].max()
    net_pnl  = end_eq - STARTING_CAPITAL
    ret_pct  = net_pnl / STARTING_CAPITAL * 100

    # Annualized Sharpe from hourly equity returns
    df_eq['ret'] = df_eq['equity'].pct_change()
    sharpe = (df_eq['ret'].mean() / (df_eq['ret'].std() + 1e-12)) * np.sqrt(8760)

    bar = "=" * 64

    print(f"\n{bar}")
    print(f"  GOAT FUNDED — 90-DAY BACKTEST RESULTS")
    print(f"  goat_funded_v1_2.py | $100K virtual capital | 15% kill switch")
    print(bar)
    print(f"  Period:           {df_eq['timestamp'].iloc[0].strftime('%Y-%m-%d')} → "
          f"{df_eq['timestamp'].iloc[-1].strftime('%Y-%m-%d')}")
    print(f"  Starting Capital: ${STARTING_CAPITAL:>12,.2f}")
    print(f"  Ending Equity:    ${end_eq:>12,.2f}")
    print(f"  Net P/L:          ${net_pnl:>+12,.2f}  ({ret_pct:+.2f}%)")
    print(f"  Peak Equity:      ${max_eq:>12,.2f}")
    print(f"  Max Drawdown:     {max_dd*100:.2f}%  (limit: {MAX_DD_PCT*100:.0f}%)")
    print(f"  Kill Switch:      {'*** TRIGGERED ***' if killed else 'Not triggered'}")
    print(f"  Sharpe Ratio:     {sharpe:.2f}  (annualized)")
    print(f"  {'─' * 62}")
    print(f"  Total Trades:     {trade_count}")

    if not df_tr.empty:
        wins      = df_tr[df_tr['pnl_usd'] > 0]
        losses    = df_tr[df_tr['pnl_usd'] <= 0]
        win_rate  = len(wins) / len(df_tr) * 100
        avg_win   = wins['pnl_usd'].mean()   if not wins.empty   else 0.0
        avg_loss  = losses['pnl_usd'].mean() if not losses.empty else 0.0
        best      = df_tr['pnl_usd'].max()
        worst     = df_tr['pnl_usd'].min()
        g_profit  = wins['pnl_usd'].sum()    if not wins.empty   else 0.0
        g_loss    = losses['pnl_usd'].sum()  if not losses.empty else 0.0
        pf        = abs(g_profit / g_loss)   if g_loss != 0      else float('inf')

        print(f"  Win Rate:         {win_rate:.1f}%  ({len(wins)}W / {len(losses)}L)")
        print(f"  Avg Win:          ${avg_win:>+,.2f}")
        print(f"  Avg Loss:         ${avg_loss:>+,.2f}")
        print(f"  Best Trade:       ${best:>+,.2f}")
        print(f"  Worst Trade:      ${worst:>+,.2f}")
        print(f"  Profit Factor:    {pf:.2f}")
        print(f"  {'─' * 62}")

        print("  Signal Breakdown:")
        for sig, grp in df_tr.groupby('signal'):
            wr  = (grp['pnl_usd'] > 0).mean() * 100
            tot = grp['pnl_usd'].sum()
            print(f"    {sig:<34}  {len(grp):>3} trades  WR:{wr:>5.1f}%  P/L:${tot:>+,.0f}")

        print(f"  {'─' * 62}")
        print("  Exit Reason Breakdown:")
        for reason, grp in df_tr.groupby('reason'):
            wr  = (grp['pnl_usd'] > 0).mean() * 100
            tot = grp['pnl_usd'].sum()
            print(f"    {reason:<34}  {len(grp):>3} trades  WR:{wr:>5.1f}%  P/L:${tot:>+,.0f}")

    print(f"  {'─' * 62}")
    print("  Regime Distribution:")
    for reg, cnt in df_eq['regime'].value_counts().items():
        pct = cnt / len(df_eq) * 100
        print(f"    {reg:<12}  {cnt:>5} candles  ({pct:.1f}%)")

    print(bar + "\n")
    return df_eq, df_tr


# ── Entry point ────────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 64)
    print("  GOAT FUNDED — 90-DAY BACKTEST")
    print("  goat_funded_v1_2.py | $100K | 15% kill switch")
    print("=" * 64)

    trades, equity_curve, killed, trade_count = run_backtest()
    df_eq, df_tr = print_report(trades, equity_curve, killed, trade_count)

    out_equity = os.path.join(OUTPUT_DIR, 'backtest_goat_v1_2_equity.csv')
    out_trades = os.path.join(OUTPUT_DIR, 'backtest_goat_v1_2_trades.csv')

    df_eq.to_csv(out_equity, index=False)
    print(f"Equity curve → {out_equity}")

    if not df_tr.empty:
        df_tr.to_csv(out_trades, index=False)
        print(f"Trades log   → {out_trades}")
