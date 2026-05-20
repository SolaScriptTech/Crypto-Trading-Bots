"""
btc_trader_backtest_v2.py — 90-Day Backtest
=============================================
Faithful replication of btc_trader.py v2 strategy logic.

ENTRY (exact mirror of live bot):
  PRIMARY — all three required:
    1. MACD(12,26,9) histogram flipped <=0 → >0 within last 2 closed bars
    2. Volume > 1.2x 20-bar rolling average on the flip bar
    3. Regime != BEAR (EMA21 < EMA55)
  SECONDARY — idleness catch-all:
    4. Flat > 24h AND RSI(14) < 40 AND regime != BEAR

EXIT LADDER (exact mirror of live bot):
  0a. Never-green pain: loss >= $150, never went positive → cut
  0b. Chop detection: was green, loss >= $150 → cut
  1.  Break-even floor: once green, stop never below entry
  2.  Tiered trail:
        Tier 1 (peak profit < $100): 5% hard stop from entry
        Tier 2 (peak profit >= $100): floor at 75% of peak profit
  3.  MACD histogram flips negative on closed bar → exit
  4.  5% hard stop from entry — always active

Capital:   $2,000
Slippage:  0.10% per side
Kill switch: 15% max drawdown

Usage:
    python3 btc_trader_backtest_v2.py
    python3 btc_trader_backtest_v2.py --days 90 --capital 2000
"""

import argparse
import math
import time
import sys
from datetime import datetime, timezone

import pandas as pd
import numpy as np

try:
    import ccxt
except ImportError:
    print("ERROR: pip install ccxt pandas numpy")
    sys.exit(1)

# ─────────────────────────────────────────────────────────────
# CONFIG — exact mirrors of btc_trader.py v2 constants
# ─────────────────────────────────────────────────────────────
STARTING_CAPITAL   = 2_000.0
SLIPPAGE           = 0.0010
HARD_STOP_PCT      = 0.05
MAX_DD_PCT         = 0.15
VOL_SPIKE_MULT     = 1.2
PAIN_THRESHOLD     = 150.0
TIER2_THRESH       = 100.0
TIER2_FLOOR_PCT    = 0.75
IDLE_ENTRY_HOURS   = 24
IDLE_RSI_THRESH    = 40
BACKTEST_DAYS      = 90
PAGE_SIZE          = 720

OUT_TRADES = 'btc_trader_v2_trades.csv'
OUT_EQUITY = 'btc_trader_v2_equity.csv'

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

# ─────────────────────────────────────────────────────────────
# DATA FETCH — paginated, same as backtest_v5.py pattern
# ─────────────────────────────────────────────────────────────
def fetch(exchange, days, min_bars=120):
    need  = days * 24 + min_bars + 10
    end   = int(time.time() * 1000)
    rows  = []
    seen  = set()
    pages = math.ceil(need / PAGE_SIZE) + 3
    print(f"  Fetching BTC/USD 1h ({days}d, need {need} bars)...", end='', flush=True)
    for _ in range(pages):
        since = end - PAGE_SIZE * 3600 * 1000
        try:
            raw = exchange.fetch_ohlcv('BTC/USD', '1h', since=since, limit=PAGE_SIZE)
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
    df = pd.DataFrame(rows, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
    df.drop_duplicates('ts', inplace=True)
    df.sort_values('ts', inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(f" OK ({len(df)} bars)")
    return df

# ─────────────────────────────────────────────────────────────
# INDICATORS — exact mirrors of btc_trader.py v2
# ─────────────────────────────────────────────────────────────
def calc_macd_histogram(closes):
    ema12  = closes.ewm(span=12, adjust=False).mean()
    ema26  = closes.ewm(span=26, adjust=False).mean()
    macd   = ema12 - ema26
    signal = macd.ewm(span=9, adjust=False).mean()
    return macd - signal

def calc_regime(closes):
    ema21 = closes.ewm(span=21, adjust=False).mean()
    ema55 = closes.ewm(span=55, adjust=False).mean()
    if ema21.iloc[-1] < ema55.iloc[-1]:
        return 'BEAR'
    if ema21.iloc[-1] > ema55.iloc[-1] and closes.iloc[-1] > ema21.iloc[-1]:
        return 'BULL'
    return 'NEUTRAL'

def calc_rsi(closes, period=14):
    delta = closes.diff()
    gain  = delta.clip(lower=0).ewm(com=period - 1, adjust=False).mean()
    loss  = (-delta.clip(upper=0)).ewm(com=period - 1, adjust=False).mean()
    return 100 - 100 / (1 + gain / (loss + 1e-9))

# ─────────────────────────────────────────────────────────────
# ENTRY SIGNAL — exact mirror of check_entry() in btc_trader v2
# iloc[-2] = last fully closed candle; iloc[-3] = one before
# ─────────────────────────────────────────────────────────────
def check_entry(df_sub, hours_since_last_entry):
    """
    Returns (fired, signal_type, detail)
    df_sub: slice of data up to and including current bar
    """
    if len(df_sub) < 60:
        return False, '', {}

    hist   = calc_macd_histogram(df_sub['c'])
    regime = calc_regime(df_sub['c'])

    if regime == 'BEAR':
        return False, '', {'reason': 'BEAR'}

    # Volume rolling average (shift(1) to use only prior bars — no look-ahead)
    vol_avg = df_sub['v'].shift(1).rolling(20).mean()

    # PRIMARY: 2-bar MACD flip window
    for offset in [-2, -3]:
        curr_h = sf(hist.iloc[offset])
        prev_h = sf(hist.iloc[offset - 1])
        if curr_h > 0 and prev_h <= 0:
            vol_curr  = sf(df_sub['v'].iloc[offset])
            vol_mean  = sf(vol_avg.iloc[offset])
            vol_ratio = vol_curr / (vol_mean + 1e-9)
            if vol_ratio >= VOL_SPIKE_MULT:
                return True, 'MACD_FLIP', {
                    'regime': regime,
                    'curr_hist': round(curr_h, 6),
                    'vol_ratio': round(vol_ratio, 2),
                    'flip_bar': offset,
                }
            else:
                # Flipped but vol too low — don't check the other bar
                return False, '', {
                    'reason': f'MACD flip bar {offset} vol too low ({vol_ratio:.2f}x)'
                }

    # SECONDARY: RSI idle guard
    if hours_since_last_entry >= IDLE_ENTRY_HOURS:
        rsi   = calc_rsi(df_sub['c'])
        rsi_v = sf(rsi.iloc[-2])
        if rsi_v < IDLE_RSI_THRESH:
            return True, 'RSI_IDLE_GUARD', {
                'regime': regime,
                'rsi': round(rsi_v, 1),
                'hours_flat': round(hours_since_last_entry, 1),
            }

    return False, '', {'reason': 'no signal'}

# ─────────────────────────────────────────────────────────────
# EXIT EVALUATION — exact mirror of evaluate_exit() in live bot
# ─────────────────────────────────────────────────────────────
def evaluate_exit(pos, current_price, capital_deployed):
    """
    pos: dict with entry_price, peak_price, ever_green,
         peak_profit_usd, tier2_armed
    Returns (should_exit, reason, updated_pos)
    """
    entry       = pos['entry_price']
    peak        = pos['peak_price']
    ever_green  = pos['ever_green']
    peak_profit = pos['peak_profit_usd']
    tier2_armed = pos['tier2_armed']

    # Update peak price
    if current_price > peak:
        pos['peak_price'] = current_price
        peak = current_price

    pnl_usd = (current_price - entry) / entry * capital_deployed

    # Update peak profit
    if pnl_usd > peak_profit:
        pos['peak_profit_usd'] = pnl_usd
        peak_profit = pnl_usd

    # Arm Tier 2
    if peak_profit >= TIER2_THRESH:
        pos['tier2_armed'] = True
        tier2_armed = True

    # Mark ever-green
    if pnl_usd > 0 and not ever_green:
        pos['ever_green'] = True
        ever_green = True

    # 0a. Never-green pain
    if not ever_green and pnl_usd <= -PAIN_THRESHOLD:
        return True, 'FAILED_SIGNAL_CUT', pos

    # 0b. Chop detection
    if ever_green and pnl_usd <= -PAIN_THRESHOLD:
        return True, 'CHOP_DETECTED', pos

    # 1. Break-even floor
    if ever_green and current_price < entry:
        return True, 'BREAK_EVEN_FLOOR', pos

    # 4. Hard stop — always active
    if current_price <= entry * (1 - HARD_STOP_PCT):
        return True, 'HARD_STOP_5PCT', pos

    # 2. Tiered trail
    if tier2_armed:
        floor_usd   = TIER2_FLOOR_PCT * peak_profit
        floor_price = entry * (1 + floor_usd / capital_deployed)
        if current_price <= floor_price:
            return True, 'TRAIL_FLOOR_T2', pos

    return False, 'HOLD', pos

# ─────────────────────────────────────────────────────────────
# BACKTEST ENGINE
# ─────────────────────────────────────────────────────────────
def run_backtest(df, starting_capital):
    cutoff    = BACKTEST_DAYS * 24
    bars      = df.tail(cutoff + 60).reset_index(drop=True)
    start_idx = 60   # indicator warmup

    capital    = starting_capital
    position   = None
    max_equity = starting_capital
    killed     = False

    trades  = []
    eq_log  = []
    n_tr    = 0
    wins    = 0

    # Track idle time using bar index (1 bar = 1 hour)
    last_entry_bar = -IDLE_ENTRY_HOURS  # seed so idle guard can fire from start

    print(f"\nRunning backtest on {len(bars) - start_idx} hourly bars...\n")

    for i in range(start_idx, len(bars)):
        if killed:
            break

        ts    = int(bars['ts'].iloc[i])
        price = sf(bars['c'].iloc[i])
        if price <= 0:
            continue

        df_sub = bars.iloc[:i + 1].copy()

        # ── MACD histogram for exit check ──────────────────
        hist_series = calc_macd_histogram(df_sub['c'])
        macd_negative = sf(hist_series.iloc[-2]) < 0

        # ── EXIT ───────────────────────────────────────────
        if position is not None:
            capital_deployed = position['capital_deployed']
            should_exit, reason, position = evaluate_exit(position, price, capital_deployed)

            # MACD flip negative exit (mirrors 5-min check in live bot)
            if not should_exit and macd_negative:
                should_exit = True
                reason      = 'MACD_FLIP_NEGATIVE'

            if should_exit:
                ep      = price * (1 - SLIPPAGE)
                pnl_usd = (ep - position['entry_price']) / position['entry_price'] * capital_deployed
                capital  = capital_deployed + pnl_usd
                n_tr    += 1
                if pnl_usd > 0:
                    wins += 1
                last_entry_bar = -IDLE_ENTRY_HOURS  # reset after exit so idle clock restarts
                trades.append({
                    'entry_time':   ts_str(position['ts']),
                    'exit_time':    ts_str(ts),
                    'entry_price':  round(position['entry_price'], 2),
                    'exit_price':   round(ep, 2),
                    'capital':      round(capital_deployed, 2),
                    'pnl_usd':      round(pnl_usd, 2),
                    'pnl_pct':      round(pnl_usd / capital_deployed * 100, 3),
                    'peak_profit':  round(position['peak_profit_usd'], 2),
                    'ever_green':   position['ever_green'],
                    'tier2_armed':  position['tier2_armed'],
                    'regime':       position['regime'],
                    'signal':       position['signal'],
                    'reason':       reason,
                    'bars_held':    i - position['bar'],
                })
                position = None

        # ── EQUITY + KILL SWITCH ────────────────────────────
        equity     = capital if position is None else (
            capital + (position['capital_deployed'] / position['entry_price']) * price
            - position['capital_deployed']
            + position['capital_deployed']
        )
        # Cleaner equity calc:
        if position is not None:
            btc_held = position['capital_deployed'] / position['entry_price']
            equity   = btc_held * price
        else:
            equity   = capital

        max_equity = max(max_equity, equity)
        dd         = (max_equity - equity) / max_equity if max_equity > 0 else 0.0

        eq_log.append({
            'ts':       ts_str(ts),
            'equity':   round(equity, 2),
            'price':    round(price, 2),
            'regime':   calc_regime(df_sub['c']),
            'position': 'LONG' if position else 'FLAT',
            'dd_pct':   round(dd * 100, 3),
        })

        if dd >= MAX_DD_PCT:
            print(f"\n  [!] KILL SWITCH bar={i}  DD={dd*100:.1f}%  equity=${equity:,.0f}")
            if position is not None:
                ep      = price * (1 - SLIPPAGE)
                pnl_usd = (ep - position['entry_price']) / position['entry_price'] * position['capital_deployed']
                capital  = position['capital_deployed'] + pnl_usd
                n_tr    += 1
                if pnl_usd > 0:
                    wins += 1
                trades.append({
                    'entry_time':  ts_str(position['ts']),
                    'exit_time':   ts_str(ts),
                    'entry_price': round(position['entry_price'], 2),
                    'exit_price':  round(ep, 2),
                    'capital':     round(position['capital_deployed'], 2),
                    'pnl_usd':     round(pnl_usd, 2),
                    'pnl_pct':     round(pnl_usd / position['capital_deployed'] * 100, 3),
                    'peak_profit': round(position['peak_profit_usd'], 2),
                    'ever_green':  position['ever_green'],
                    'tier2_armed': position['tier2_armed'],
                    'regime':      position['regime'],
                    'signal':      position['signal'],
                    'reason':      'KILL_SWITCH',
                    'bars_held':   i - position['bar'],
                })
                position = None
            killed = True
            break

        # ── ENTRY ───────────────────────────────────────────
        if position is None and not killed:
            hours_since_last = (i - last_entry_bar)  # 1 bar = 1 hour
            fired, signal, detail = check_entry(df_sub, hours_since_last)

            if fired:
                ep = price * (1 + SLIPPAGE)
                position = {
                    'entry_price':     ep,
                    'peak_price':      ep,
                    'capital_deployed': capital,
                    'ever_green':      False,
                    'peak_profit_usd': 0.0,
                    'tier2_armed':     False,
                    'ts':              ts,
                    'bar':             i,
                    'regime':          detail.get('regime', '?'),
                    'signal':          signal,
                }
                capital        = 0.0
                last_entry_bar = i

    # Close any open position at end of data
    if position is not None:
        last_price = sf(bars['c'].iloc[-1])
        ep         = last_price * (1 - SLIPPAGE)
        pnl_usd    = (ep - position['entry_price']) / position['entry_price'] * position['capital_deployed']
        capital     = position['capital_deployed'] + pnl_usd
        n_tr       += 1
        if pnl_usd > 0:
            wins += 1
        trades.append({
            'entry_time':  ts_str(position['ts']),
            'exit_time':   ts_str(int(bars['ts'].iloc[-1])),
            'entry_price': round(position['entry_price'], 2),
            'exit_price':  round(ep, 2),
            'capital':     round(position['capital_deployed'], 2),
            'pnl_usd':     round(pnl_usd, 2),
            'pnl_pct':     round(pnl_usd / position['capital_deployed'] * 100, 3),
            'peak_profit': round(position['peak_profit_usd'], 2),
            'ever_green':  position['ever_green'],
            'tier2_armed': position['tier2_armed'],
            'regime':      position['regime'],
            'signal':      position['signal'],
            'reason':      'END_OF_DATA',
            'bars_held':   len(bars) - 1 - position['bar'],
        })

    return trades, eq_log, n_tr, wins, capital, max_equity

# ─────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────
def report(trades, eq_log, n_tr, wins, final_capital, max_equity, starting_capital):
    tdf = pd.DataFrame(trades) if trades else pd.DataFrame()
    edf = pd.DataFrame(eq_log)  if eq_log  else pd.DataFrame()

    net   = final_capital - starting_capital
    net_p = net / starting_capital * 100
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
        avg_w = float(wt['pnl_usd'].mean()) if len(wt) else 0.0
        avg_l = float(lt['pnl_usd'].mean()) if len(lt) else 0.0
        best  = float(tdf['pnl_usd'].max())
        worst = float(tdf['pnl_usd'].min())
        avg_b = float(tdf['bars_held'].mean())
        gw    = float(wt['pnl_usd'].sum()) if len(wt) else 0.0
        gl    = abs(float(lt['pnl_usd'].sum())) if len(lt) else 0.0
        pf    = gw / (gl + 1e-9)
        reasons   = tdf['reason'].value_counts().to_dict()
        by_sig    = tdf.groupby('signal')['pnl_usd'].agg(['count','sum']).to_dict('index')
        by_regime = tdf.groupby('regime')['pnl_usd'].agg(['count','sum']).to_dict('index')

    S = '=' * 65
    print(f"\n{S}")
    print(f"  BTC_TRADER v2 — 90-DAY BACKTEST RESULTS")
    print(f"  MACD flip (2-bar) | Vol 1.2x | RSI idle guard | $2,000")
    print(S)
    print(f"  Starting equity:     ${starting_capital:>10,.2f}")
    print(f"  Final equity:        ${final_capital:>10,.2f}")
    print(f"  Net P&L:             ${net:>+10,.2f}  ({net_p:+.2f}%)")
    print(f"  Max equity:          ${max_equity:>10,.2f}")
    print(f"  Max drawdown:        {max_dd:>9.2f}%  (limit {MAX_DD_PCT*100:.0f}%)")
    print('-' * 65)
    print(f"  Total trades:        {n_tr}")
    print(f"  Wins:                {wins}  ({wr:.1f}%)")
    print(f"  Losses:              {n_tr - wins}")
    print(f"  Avg win:             ${avg_w:>+8,.2f}")
    print(f"  Avg loss:            ${avg_l:>+8,.2f}")
    print(f"  Profit factor:       {pf:>8.2f}")
    print(f"  Best trade:          ${best:>+8,.2f}")
    print(f"  Worst trade:         ${worst:>+8,.2f}")
    print(f"  Avg hold (hours):    {avg_b:>7.1f}h")
    print('-' * 65)
    if by_sig:
        print("  By signal:")
        for sig, r in sorted(by_sig.items()):
            sub = tdf[tdf['signal'] == sig] if not tdf.empty else pd.DataFrame()
            wr2 = len(sub[sub['pnl_usd'] > 0]) / len(sub) * 100 if len(sub) else 0
            print(f"    {sig:<22}  {int(r['count']):>3} trades  "
                  f"WR {wr2:.0f}%  P&L ${float(r['sum']):>+8,.2f}")
    print('-' * 65)
    if by_regime:
        print("  By entry regime:")
        for reg, r in sorted(by_regime.items()):
            print(f"    {reg:<10}  {int(r['count']):>3} trades  "
                  f"P&L ${float(r['sum']):>+8,.2f}")
    print('-' * 65)
    if reasons:
        print("  Exit reasons:")
        for r, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {r:<28}  {cnt:>4}")
    print(S)

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--days',    type=int,   default=BACKTEST_DAYS)
    parser.add_argument('--capital', type=float, default=STARTING_CAPITAL)
    args = parser.parse_args()

    print('=' * 65)
    print('  BTC_TRADER v2 — 90-Day Backtest')
    print('  Entry: MACD flip (2-bar) + Vol 1.2x | RSI idle guard')
    print('  Exit:  Pain cut | Break-even | Tiered trail | MACD flip')
    print(f'  Capital: ${args.capital:,.0f}  |  Max DD: {MAX_DD_PCT*100:.0f}%  |  Slippage: {SLIPPAGE*100:.1f}bps')
    print('=' * 65)

    exchange = ccxt.kraken({'enableRateLimit': True})

    print('\nFetching data...')
    df = fetch(exchange, args.days)
    if df is None:
        print('ERROR: Could not fetch data.')
        return

    trades, eq_log, n_tr, wins, final_capital, max_equity = \
        run_backtest(df, args.capital)

    if trades:
        pd.DataFrame(trades).to_csv(OUT_TRADES, index=False)
        print(f'\nTrade log  -> {OUT_TRADES}  ({len(trades)} trades)')
    if eq_log:
        pd.DataFrame(eq_log).to_csv(OUT_EQUITY, index=False)
        print(f'Equity log -> {OUT_EQUITY}')

    report(trades, eq_log, n_tr, wins, final_capital, max_equity, args.capital)

if __name__ == '__main__':
    main()
