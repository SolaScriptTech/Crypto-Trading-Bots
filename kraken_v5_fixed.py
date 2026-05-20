"""
backtest_v5_fixed.py — V5 backtest with three targeted fixes
=============================================================
Fixes vs backtest_v5.py (diagnosed from 90-day run results):

  FIX 1 — BB_LOWER_NEUTRAL gated (was -$8,480 on 8 trades, WR 12%)
    Now requires: ADX > 20 AND RSI < 40 AND price > EMA55
    Previously fired on BB touch alone in NEUTRAL — caught every falling knife.

  FIX 2 — EMA21_PULLBACK RSI gate tightened: < 52 → < 45
    Best signal by P&L (+$1,010) but WR only 40% due to overbought entries.
    Tighter RSI filters weak pullbacks still in overbought territory.

  FIX 3 — Minimum 3-bar hold before trail stop can fire
    12 of 22 trades previously exited in ≤3h, losing -$7,553 combined.
    Hard stop still fires immediately. Only trail stop is gated.

Capital: $100,000  |  Max DD: 15%  |  Slippage: 0.10%
"""

import ccxt
import pandas as pd
import numpy as np
import math
import time
from datetime import datetime, timezone

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
STARTING_CAPITAL = 100_000.0
SLIPPAGE         = 0.0010
STOP_PCT         = 0.035        # 3.5% hard stop from peak
MAX_DD           = 0.15         # 15% portfolio kill switch
BULL_TRAIL       = 0.013        # 1.3% trailing stop in bull
BEAR_TRAIL       = 0.020        # 2.0% trailing stop in neutral
BB_MULT_BULL     = 1.5
BB_MULT_NEUTRAL  = 2.0
ADX_THRESHOLD    = 20
BACKTEST_DAYS    = 90
PAGE_SIZE        = 720

OUT_TRADES = 'backtest_v5_fixed_trades.csv'
OUT_EQUITY = 'backtest_v5_fixed_equity.csv'

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def sf(v, d=0.0):
    try:
        f = float(v)
        return d if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return d

def ts_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')

def ema_series(c, span):
    return c.ewm(span=span, adjust=False).mean()

def calc_rsi(c, period=14):
    delta = c.diff()
    gain  = delta.clip(lower=0).ewm(com=period-1, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period-1, adjust=False).mean()
    return 100 - 100 / (1 + gain / (loss + 1e-9))

def calc_adx(df, period=14):
    h, l, c = df['h'], df['l'], df['c']
    pdm = h.diff().clip(lower=0)
    ndm = (-l.diff()).clip(lower=0)
    tr  = pd.concat([(h-l), (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(span=period, adjust=False).mean()
    pdi = 100 * pdm.ewm(span=period, adjust=False).mean() / (atr + 1e-9)
    ndi = 100 * ndm.ewm(span=period, adjust=False).mean() / (atr + 1e-9)
    dx  = (abs(pdi - ndi) / (pdi + ndi + 1e-9)) * 100
    return dx.ewm(span=period, adjust=False).mean()

# ─────────────────────────────────────────────────────────────
# DATA FETCH — paginated
# ─────────────────────────────────────────────────────────────
def fetch(exchange, sym, days, min_bars=120):
    need  = days * 24 + min_bars + 10
    end   = int(time.time() * 1000)
    rows  = []
    seen  = set()
    pages = math.ceil(need / PAGE_SIZE) + 3
    print(f"  Fetching {sym} 1h ({days}d, need {need} bars)...", end='', flush=True)
    for _ in range(pages):
        since = end - PAGE_SIZE * 3600 * 1000
        try:
            raw = exchange.fetch_ohlcv(sym, '1h', since=since, limit=PAGE_SIZE)
            time.sleep(1.5)
        except Exception as e:
            print(f" ERR({e})")
            break
        if not raw:
            break
        new = [r for r in raw if r[0] not in seen]
        if not new:
            break
        for r in new:
            seen.add(r[0])
        rows = new + rows
        end  = raw[0][0]
        if len(rows) >= need:
            break
    if len(rows) < min_bars:
        print(f" SKIP ({len(rows)} bars)")
        return None
    df = pd.DataFrame(rows, columns=['ts','o','h','l','c','v'])
    df.drop_duplicates('ts', inplace=True)
    df.sort_values('ts', inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(f" OK ({len(df)} bars)")
    return df

# ─────────────────────────────────────────────────────────────
# REGIME DETECTION — faithful V5 RegimeEngine
# BTC: EMA21 > EMA55 AND price > EMA21
# ETH: EMA21 > EMA55
# ADX(BTC) > 20
# All three required for BULL
# ─────────────────────────────────────────────────────────────
def detect_regime(df_btc_sub, df_eth_sub):
    if len(df_btc_sub) < 60 or len(df_eth_sub) < 60:
        return 'NEUTRAL'
    btc_e21 = sf(ema_series(df_btc_sub['c'], 21).iloc[-1])
    btc_e55 = sf(ema_series(df_btc_sub['c'], 55).iloc[-1])
    eth_e21 = sf(ema_series(df_eth_sub['c'], 21).iloc[-1])
    eth_e55 = sf(ema_series(df_eth_sub['c'], 55).iloc[-1])
    btc_px  = sf(df_btc_sub['c'].iloc[-1])
    adx_val = sf(calc_adx(df_btc_sub).iloc[-1])

    btc_bull = btc_e21 > btc_e55 and btc_px > btc_e21
    eth_bull = eth_e21 > eth_e55
    btc_bear = btc_e21 < btc_e55
    eth_bear = eth_e21 < eth_e55
    strong   = adx_val > ADX_THRESHOLD

    if btc_bull and eth_bull and strong:
        return 'BULL'
    if btc_bear or eth_bear:
        return 'BEAR'
    return 'NEUTRAL'

# ─────────────────────────────────────────────────────────────
# V5 TIERED TRAILING STOP — from V4.1
# ─────────────────────────────────────────────────────────────
def tiered_trail(entry_price, peak_price, base_trail):
    gain_pct = (peak_price - entry_price) / entry_price if entry_price > 0 else 0
    if gain_pct < 0.003:
        trail = 0.013
    elif gain_pct < 0.007:
        trail = 0.008
    elif gain_pct < 0.012:
        trail = 0.005
    else:
        trail = 0.003
    trail = min(trail, base_trail)
    stop  = peak_price * (1 - trail)
    # Profit floor: once ever above 0.3% gain, stop never below entry*1.001
    if gain_pct > 0.003:
        floor = entry_price * 1.001
        stop  = max(stop, floor)
    return stop, trail

# ─────────────────────────────────────────────────────────────
# BACKTEST ENGINE
# ─────────────────────────────────────────────────────────────
def run_backtest(df_btc, df_eth):
    # Align on BTC bars for the backtest window
    cutoff     = BACKTEST_DAYS * 24
    btc_bars   = df_btc.tail(cutoff + 60).reset_index(drop=True)
    start_idx  = 60   # need history for indicators

    cash       = STARTING_CAPITAL
    position   = None   # dict when in trade
    max_equity = STARTING_CAPITAL
    killed     = False

    trades  = []
    eq_log  = []
    n_tr    = 0
    wins    = 0
    tot_pnl = 0.0

    last_sell_bar = 0   # for idleness guard (bar index)

    print(f"\nRunning backtest: {len(btc_bars)-start_idx} hourly bars...\n")

    for i in range(start_idx, len(btc_bars)):
        if killed:
            break

        ts       = int(btc_bars['ts'].iloc[i])
        price    = sf(btc_bars['c'].iloc[i])
        if price <= 0:
            continue

        # Slices up to and including current bar (use iloc[-2] for signal = confirmed bar)
        btc_sub  = btc_bars.iloc[:i+1].copy()
        eth_ts   = df_eth['ts'].values
        eth_end  = np.searchsorted(eth_ts, ts, side='right')
        eth_sub  = df_eth.iloc[max(0, eth_end-100):eth_end].copy() if eth_end > 60 else df_eth.iloc[:eth_end].copy()

        # ── REGIME ───────────────────────────────────────────
        regime = detect_regime(btc_sub, eth_sub)

        # ── INDICATORS on confirmed bar (iloc[-2]) ───────────
        bb_mult  = BB_MULT_BULL if regime == 'BULL' else BB_MULT_NEUTRAL
        c        = btc_sub['c']
        sma20    = c.rolling(20).mean()
        std20    = c.rolling(20).std()
        upper    = sma20 + bb_mult * std20
        lower    = sma20 - bb_mult * std20
        e21      = ema_series(c, 21)
        e55      = ema_series(c, 55)
        rsi      = calc_rsi(c)

        # Use iloc[-2] (confirmed candle) for signals
        last     = sf(c.iloc[-2])
        low_b    = sf(lower.iloc[-2])
        up_b     = sf(upper.iloc[-2])
        sma20_v  = sf(sma20.iloc[-2])
        ema21_v  = sf(e21.iloc[-2])
        ema21_p  = sf(e21.iloc[-3]) if len(e21) > 3 else ema21_v
        ema55_v  = sf(e55.iloc[-2])
        rsi_v    = sf(rsi.iloc[-2])
        prev_c   = sf(c.iloc[-3]) if len(c) > 3 else last

        # ── EXIT CHECK (position open) ────────────────────────
        if position is not None:
            peak       = max(position['peak'], price)
            position['peak'] = peak
            base_trail = BULL_TRAIL if position['regime'] == 'BULL' else BEAR_TRAIL
            stop_price, trail_pct = tiered_trail(position['entry'], peak, base_trail)
            hard_stop  = peak * (1 - STOP_PCT)
            bars_held  = i - position['bar']  # FIX 3: track bars in trade

            exit_reason = None
            exit_price  = price

            # Hard stop — always fires regardless of hold time
            if price <= hard_stop:
                exit_reason = 'HARD_STOP_3PCT'
            # Trailing stop — FIX 3: minimum 3 bars before trail can fire
            elif price <= stop_price and bars_held >= 3:
                exit_reason = f'TRAIL_STOP({trail_pct*100:.1f}%)'
            # BB upper band exit (respect BULL hold rule)
            elif last > up_b and not math.isnan(up_b):
                if not (regime == 'BULL' and last < ema21_v * 1.01):
                    exit_reason = 'BB_UPPER'
            # Regime flip to BEAR
            elif regime == 'BEAR':
                exit_reason = 'REGIME_BEAR'

            if exit_reason:
                ep      = exit_price * (1 - SLIPPAGE)
                pnl     = cash  # will recalc
                pnl_val = (position['size'] / position['entry']) * ep - position['size']
                cash   += position['size'] + pnl_val
                tot_pnl += pnl_val
                n_tr   += 1
                if pnl_val > 0:
                    wins += 1
                last_sell_bar = i
                trades.append({
                    'entry_time':   ts_str(position['ts']),
                    'exit_time':    ts_str(ts),
                    'entry_price':  round(position['entry'], 2),
                    'exit_price':   round(ep, 2),
                    'size_usd':     round(position['size'], 2),
                    'pnl_usd':      round(pnl_val, 2),
                    'pnl_pct':      round(pnl_val / position['size'] * 100, 3),
                    'peak_price':   round(peak, 2),
                    'regime':       position['regime'],
                    'signal':       position['signal'],
                    'reason':       exit_reason,
                    'bars_held':    i - position['bar'],
                })
                position = None

        # ── ENTRY CHECK (flat) ────────────────────────────────
        if position is None and regime != 'BEAR' and not killed:
            action = None
            signal = None

            if regime == 'BULL':
                crossover = (prev_c < ema21_p) and (last > ema21_v)
                sig_bb    = (not math.isnan(low_b)) and last < low_b
                sig_cross = crossover
                sig_pull  = (not math.isnan(ema21_v) and
                             last < ema21_v and
                             last >= ema21_v * 0.9925 and
                             rsi_v < 45)  # FIX 2: tightened from <52 to <45
                sig_sma   = (not math.isnan(sma20_v) and
                             last < sma20_v and
                             rsi_v < 50)
                sig_rsi   = (not math.isnan(ema55_v) and
                             rsi_v < 42 and
                             last > ema55_v)

                # Idleness guard: >8h flat in BULL = 8 bars
                idle_8h   = (i - last_sell_bar) > 8

                if sig_bb:
                    action = 'BUY'; signal = 'BB_LOWER'
                elif sig_cross:
                    action = 'BUY'; signal = 'EMA21_CROSS'
                elif sig_pull:
                    action = 'BUY'; signal = 'EMA21_PULLBACK'
                elif sig_sma:
                    action = 'BUY'; signal = 'SMA20_TOUCH'
                elif sig_rsi:
                    action = 'BUY'; signal = 'RSI_OVERSOLD'
                elif idle_8h and (sig_pull or sig_sma or sig_rsi):
                    action = 'BUY'; signal = 'IDLE_GUARD'

            elif regime == 'NEUTRAL':
                # FIX 1: Gate BB_LOWER_NEUTRAL — must have ADX>20 + RSI<40 + price above EMA55
                # Previously fired on BB touch alone in any NEUTRAL — caught every falling knife
                adx_neutral = sf(calc_adx(btc_sub).iloc[-2])
                if ((not math.isnan(low_b)) and last < low_b and
                        adx_neutral > 20 and
                        rsi_v < 40 and
                        (not math.isnan(ema55_v)) and last > ema55_v):
                    action = 'BUY'; signal = 'BB_LOWER_NEUTRAL'

            if action == 'BUY':
                ep   = last * (1 + SLIPPAGE)
                size = cash   # all-in (single asset, V5 style)
                cash = 0.0
                position = {
                    'entry': ep,
                    'peak':  ep,
                    'size':  size,
                    'ts':    ts,
                    'bar':   i,
                    'regime': regime,
                    'signal': signal,
                }

        # ── EQUITY + KILL SWITCH ──────────────────────────────
        pos_val = 0.0
        if position is not None:
            pos_val = (position['size'] / position['entry']) * price
        equity  = cash + pos_val
        max_equity = max(max_equity, equity)
        dd      = (max_equity - equity) / max_equity if max_equity > 0 else 0.0

        eq_log.append({
            'ts':     ts_str(ts),
            'equity': round(equity, 2),
            'regime': regime,
            'dd_pct': round(dd * 100, 3),
        })

        if dd >= MAX_DD:
            print(f"\n  [!] KILL SWITCH bar={i}  DD={dd*100:.1f}%  equity=${equity:,.0f}")
            if position is not None:
                ep      = price * (1 - SLIPPAGE)
                pnl_val = (position['size'] / position['entry']) * ep - position['size']
                cash   += position['size'] + pnl_val
                tot_pnl += pnl_val
                n_tr   += 1
                if pnl_val > 0: wins += 1
                trades.append({
                    'entry_time':  ts_str(position['ts']),
                    'exit_time':   ts_str(ts),
                    'entry_price': round(position['entry'], 2),
                    'exit_price':  round(ep, 2),
                    'size_usd':    round(position['size'], 2),
                    'pnl_usd':     round(pnl_val, 2),
                    'pnl_pct':     round(pnl_val / position['size'] * 100, 3),
                    'peak_price':  round(position['peak'], 2),
                    'regime':      position['regime'],
                    'signal':      position['signal'],
                    'reason':      'KILL_SWITCH',
                    'bars_held':   i - position['bar'],
                })
                position = None
            killed = True
            break

    # Close any residual open position at end of data
    if position is not None:
        last_bar = btc_bars.iloc[-1]
        ep       = sf(last_bar['c']) * (1 - SLIPPAGE)
        pnl_val  = (position['size'] / position['entry']) * ep - position['size']
        cash    += position['size'] + pnl_val
        tot_pnl += pnl_val
        n_tr    += 1
        if pnl_val > 0: wins += 1
        trades.append({
            'entry_time':  ts_str(position['ts']),
            'exit_time':   ts_str(int(last_bar['ts'])),
            'entry_price': round(position['entry'], 2),
            'exit_price':  round(ep, 2),
            'size_usd':    round(position['size'], 2),
            'pnl_usd':     round(pnl_val, 2),
            'pnl_pct':     round(pnl_val / position['size'] * 100, 3),
            'peak_price':  round(position['peak'], 2),
            'regime':      position['regime'],
            'signal':      position['signal'],
            'reason':      'END_OF_DATA',
            'bars_held':   len(btc_bars) - 1 - position['bar'],
        })

    return trades, eq_log, n_tr, wins, tot_pnl, max_equity, cash

# ─────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────
def report(trades, eq_log, n_tr, wins, tot_pnl, max_equity, final_cash):
    tdf = pd.DataFrame(trades) if trades else pd.DataFrame()
    edf = pd.DataFrame(eq_log) if eq_log else pd.DataFrame()

    net   = final_cash - STARTING_CAPITAL
    net_p = net / STARTING_CAPITAL * 100
    wr    = wins / n_tr * 100 if n_tr > 0 else 0.0

    max_dd = 0.0
    if not edf.empty:
        eq = edf['equity'].values.astype(float)
        pk = np.maximum.accumulate(eq)
        max_dd = float(((pk - eq) / (pk + 1e-9)).max()) * 100

    avg_w = avg_l = best = worst = avg_b = pf = 0.0
    reasons = {}; by_sig = {}; by_regime = {}

    if not tdf.empty:
        wt    = tdf[tdf['pnl_usd'] > 0]
        lt    = tdf[tdf['pnl_usd'] <= 0]
        avg_w = float(wt['pnl_usd'].mean()) if len(wt) > 0 else 0.0
        avg_l = float(lt['pnl_usd'].mean()) if len(lt) > 0 else 0.0
        best  = float(tdf['pnl_usd'].max())
        worst = float(tdf['pnl_usd'].min())
        avg_b = float(tdf['bars_held'].mean())
        gw    = float(wt['pnl_usd'].sum()) if len(wt) > 0 else 0.0
        gl    = abs(float(lt['pnl_usd'].sum())) if len(lt) > 0 else 0.0
        pf    = gw / (gl + 1e-9)
        reasons  = tdf['reason'].value_counts().to_dict()
        by_sig   = tdf.groupby('signal')['pnl_usd'].agg(['count','sum','mean']).to_dict('index')
        by_regime = tdf.groupby('regime')['pnl_usd'].agg(['count','sum']).to_dict('index')

    S = '=' * 65
    print(f"\n{S}")
    print(f"  KRAKEN V5 FIXED — 90-DAY BACKTEST RESULTS")
    print(f"  Fix1: BB_NEUTRAL gated | Fix2: RSI<45 | Fix3: 3-bar min hold")
    print(S)
    print(f"  Starting equity:     ${STARTING_CAPITAL:>12,.2f}")
    print(f"  Final equity:        ${final_cash:>12,.2f}")
    print(f"  Net P&L:             ${net:>+12,.2f}  ({net_p:+.2f}%)")
    print(f"  Max equity:          ${max_equity:>12,.2f}")
    print(f"  Max drawdown:        {max_dd:>11.2f}%  (limit {MAX_DD*100:.0f}%)")
    print('-' * 65)
    print(f"  Total trades:        {n_tr}")
    print(f"  Wins:                {wins}  ({wr:.1f}%)")
    print(f"  Losses:              {n_tr - wins}")
    print(f"  Avg win:             ${avg_w:>+10,.2f}")
    print(f"  Avg loss:            ${avg_l:>+10,.2f}")
    print(f"  Profit factor:       {pf:>10.2f}")
    print(f"  Best trade:          ${best:>+10,.2f}")
    print(f"  Worst trade:         ${worst:>+10,.2f}")
    print(f"  Avg hold (1h bars):  {avg_b:>9.1f}h")
    print('-' * 65)
    if by_sig:
        print("  By entry signal:")
        for sig, r in sorted(by_sig.items()):
            wr2 = 0
            if not tdf.empty:
                sub = tdf[tdf['signal']==sig]
                wr2 = len(sub[sub['pnl_usd']>0]) / len(sub) * 100
            print(f"    {sig:<22}  {int(r['count']):>3} trades  "
                  f"WR {wr2:.0f}%  P&L ${float(r['sum']):>+10,.2f}")
    print('-' * 65)
    if by_regime:
        print("  By entry regime:")
        for reg, r in sorted(by_regime.items()):
            print(f"    {reg:<10}  {int(r['count']):>3} trades  P&L ${float(r['sum']):>+10,.2f}")
    print('-' * 65)
    if reasons:
        print("  Exit reasons:")
        for r, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {r:<30}  {cnt:>4}")
    print(S)

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  KRAKEN V5 FIXED — 90-Day Backtest")
    print("  Fix1: BB_NEUTRAL gated  Fix2: RSI<45  Fix3: 3-bar min hold")
    print("  Regime:   EMA21/EMA55 + ADX>20, BTC+ETH confirmation")
    print("  Capital:  $100,000  |  Max DD: 15%  |  Slippage: 0.10%")
    print("=" * 65)

    exchange = ccxt.kraken({'enableRateLimit': True})

    print("\nFetching data...")
    df_btc = fetch(exchange, 'BTC/USD', BACKTEST_DAYS)
    df_eth = fetch(exchange, 'ETH/USD', BACKTEST_DAYS)

    if df_btc is None or df_eth is None:
        print("ERROR: Could not fetch required data.")
        return

    trades, eq_log, n_tr, wins, tot_pnl, max_equity, final_cash = \
        run_backtest(df_btc, df_eth)

    if trades:
        pd.DataFrame(trades).to_csv(OUT_TRADES, index=False)
        print(f"\nTrade log  -> {OUT_TRADES}  ({len(trades)} trades)")
    if eq_log:
        pd.DataFrame(eq_log).to_csv(OUT_EQUITY, index=False)
        print(f"Equity log -> {OUT_EQUITY}")

    report(trades, eq_log, n_tr, wins, tot_pnl, max_equity, final_cash)

if __name__ == '__main__':
    main()
