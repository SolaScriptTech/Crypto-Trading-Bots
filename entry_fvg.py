#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ICT Structure Bot -- 15m candle analysis
Detects Break of Structure, Change of Character, Trend Lines, Fair Value Gaps
and generates an entry setup at the FVG Consequential Encroachment line.

Usage:
    python ict_structure_bot.py BTC/USD
    python ict_structure_bot.py ETH/USD SOL/USD ADA/USD
"""

import sys
import io

# Force UTF-8 output on Windows so all characters render correctly
if hasattr(sys.stdout, 'buffer'):
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import ccxt
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from dotenv import load_dotenv
import os
import time

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────
TIMEFRAME      = '15m'
CANDLE_COUNT   = 96          # 24 hours of 15m data
SWING_LOOKBACK = 3           # candles each side to confirm a swing point
MIN_BOS_TREND  = 2           # minimum same-direction BOS to confirm trend
TP_RISK_RATIO  = 4.0         # take-profit = entry +/- (risk * ratio)
SLIPPAGE_BPS   = 10          # 10bps slippage model on entry
STOP_BUFFER    = 0.002       # 0.2% buffer outside the stop anchor


# ── Exchange ──────────────────────────────────────────────────────────────────
def get_exchange():
    return ccxt.kraken({
        'apiKey':          os.getenv('KRAKEN_API_KEY', ''),
        'secret':          os.getenv('KRAKEN_API_SECRET', ''),
        'enableRateLimit': True,
    })


def fetch_candles(exchange, symbol: str) -> pd.DataFrame:
    raw = exchange.fetch_ohlcv(symbol, TIMEFRAME, limit=CANDLE_COUNT + 10)
    df  = pd.DataFrame(raw, columns=['ts', 'open', 'high', 'low', 'close', 'volume'])
    df['ts'] = pd.to_datetime(df['ts'], unit='ms', utc=True)
    # Drop the last (live, unclosed) candle; evaluate only confirmed bars
    df = df.iloc[:-1].tail(CANDLE_COUNT).reset_index(drop=True)
    return df


# ── Swing Detection ───────────────────────────────────────────────────────────
def find_swings(df: pd.DataFrame, n: int = SWING_LOOKBACK):
    """
    Swing high: candle whose high >= all highs within n bars on each side.
    Swing low:  candle whose low  <= all lows  within n bars on each side.
    """
    highs, lows = [], []
    for i in range(n, len(df) - n):
        h = df['high'].iloc[i]
        l = df['low'].iloc[i]
        if all(h >= df['high'].iloc[i - j] for j in range(1, n + 1)) and \
           all(h >= df['high'].iloc[i + j] for j in range(1, n + 1)):
            highs.append(i)
        if all(l <= df['low'].iloc[i - j] for j in range(1, n + 1)) and \
           all(l <= df['low'].iloc[i + j] for j in range(1, n + 1)):
            lows.append(i)
    return highs, lows


# ── Break of Structure ────────────────────────────────────────────────────────
def find_bos(df: pd.DataFrame, swing_highs: list, swing_lows: list) -> list:
    """
    BOS = a candle that CLOSES beyond a prior confirmed swing point.
      Bearish BOS: close < prior swing low  (lower low confirmed)
      Bullish BOS: close > prior swing high (higher high confirmed)
    Each swing point generates at most one BOS event.
    """
    events = []

    for sl_idx in swing_lows:
        level = df['low'].iloc[sl_idx]
        for i in range(sl_idx + 1, len(df)):
            if df['close'].iloc[i] < level:
                events.append({
                    'type': 'BOS', 'direction': 'bearish',
                    'bar_idx': i, 'level': level,
                    'swing_idx': sl_idx,
                    'ts': df['ts'].iloc[i],
                    'close': df['close'].iloc[i],
                })
                break

    for sh_idx in swing_highs:
        level = df['high'].iloc[sh_idx]
        for i in range(sh_idx + 1, len(df)):
            if df['close'].iloc[i] > level:
                events.append({
                    'type': 'BOS', 'direction': 'bullish',
                    'bar_idx': i, 'level': level,
                    'swing_idx': sh_idx,
                    'ts': df['ts'].iloc[i],
                    'close': df['close'].iloc[i],
                })
                break

    events.sort(key=lambda x: x['bar_idx'])
    return events


def determine_trend(bos_events: list) -> str:
    """
    Trend confirmed when the most recent consecutive run of BOS events has
    >= MIN_BOS_TREND in the same direction with no opposing BOS after.
    Returns 'bearish', 'bullish', or 'undefined'.
    """
    if not bos_events:
        return 'undefined'

    last_dir = bos_events[-1]['direction']
    count = 0
    for b in reversed(bos_events):
        if b['direction'] == last_dir:
            count += 1
        else:
            break

    return last_dir if count >= MIN_BOS_TREND else 'undefined'


# ── Change of Character ───────────────────────────────────────────────────────
def find_choch(df, bos_events, trend, swing_highs, swing_lows):
    """
    CHoCH = the first candle closing AGAINST the prevailing trend, breaking
    a swing in the opposing direction after the last trend-direction BOS.

    Bearish trend -> CHoCH = close above a swing HIGH (fails new LL, breaks LH)
    Bullish trend -> CHoCH = close below a swing LOW  (fails new HH, breaks HL)
    """
    if trend == 'undefined' or not bos_events:
        return None

    trend_bos = [b for b in bos_events if b['direction'] == trend]
    if not trend_bos:
        return None

    search_from = trend_bos[-1]['bar_idx']

    if trend == 'bearish':
        for sh_idx in [i for i in swing_highs if i > search_from]:
            level = df['high'].iloc[sh_idx]
            for i in range(sh_idx + 1, len(df)):
                if df['close'].iloc[i] > level:
                    return {
                        'type': 'CHoCH', 'direction': 'bullish',
                        'bar_idx': i, 'level': level,
                        'swing_idx': sh_idx,
                        'ts': df['ts'].iloc[i],
                        'close': df['close'].iloc[i],
                    }
    else:
        for sl_idx in [i for i in swing_lows if i > search_from]:
            level = df['low'].iloc[sl_idx]
            for i in range(sl_idx + 1, len(df)):
                if df['close'].iloc[i] < level:
                    return {
                        'type': 'CHoCH', 'direction': 'bearish',
                        'bar_idx': i, 'level': level,
                        'swing_idx': sl_idx,
                        'ts': df['ts'].iloc[i],
                        'close': df['close'].iloc[i],
                    }
    return None


# ── Trend Line ────────────────────────────────────────────────────────────────
def build_trendline(df, swing_indices, trend):
    """
    Bearish: line through swing HIGHs (descending resistance).
    Bullish: line through swing LOWs  (ascending support).
    Linear regression over up to 5 most-recent swing points.
    """
    if len(swing_indices) < 2:
        return None

    pts    = swing_indices[-5:]
    prices = [df['high'].iloc[i] if trend == 'bearish' else df['low'].iloc[i]
              for i in pts]

    x = np.array(pts, dtype=float)
    y = np.array(prices, dtype=float)
    slope, intercept = np.polyfit(x, y, 1)

    touches = []
    for i in pts:
        line_val = slope * i + intercept
        actual   = df['high'].iloc[i] if trend == 'bearish' else df['low'].iloc[i]
        if abs(actual - line_val) / line_val < 0.0015:
            touches.append({
                'idx': i,
                'price': actual,
                'line_val': round(line_val, 6),
                'ts': df['ts'].iloc[i],
            })

    return {'slope': slope, 'intercept': intercept, 'touches': touches, 'pts': pts}


def find_trendline_break(df, trendline, trend, start_idx=0):
    """
    First candle CLOSING outside the trendline after start_idx.
    Bearish line break: close > line value.
    Bullish line break: close < line value.
    """
    if trendline is None:
        return None

    s, b = trendline['slope'], trendline['intercept']
    for i in range(start_idx, len(df)):
        line_val = s * i + b
        close    = df['close'].iloc[i]
        if trend == 'bearish' and close > line_val:
            return {'bar_idx': i, 'close': close,
                    'line_val': round(line_val, 6), 'ts': df['ts'].iloc[i]}
        if trend == 'bullish' and close < line_val:
            return {'bar_idx': i, 'close': close,
                    'line_val': round(line_val, 6), 'ts': df['ts'].iloc[i]}
    return None


# ── Fair Value Gap ─────────────────────────────────────────────────────────────
def find_fvgs(df, after_idx=0) -> list:
    """
    3-candle FVG pattern:
      Bullish FVG: candle[i-1].high < candle[i+1].low
      Bearish FVG: candle[i-1].low  > candle[i+1].high

    CE (Consequential Encroachment) = 50% midpoint of the gap.
    'filled' = subsequent price fully re-entered the gap.
    """
    fvgs = []

    for i in range(max(1, after_idx + 1), len(df) - 1):
        c0_high = df['high'].iloc[i - 1]
        c0_low  = df['low'].iloc[i - 1]
        c2_low  = df['low'].iloc[i + 1]
        c2_high = df['high'].iloc[i + 1]

        if c0_high < c2_low:
            gap_low, gap_high = c0_high, c2_low
            fvgs.append({'type': 'bullish', 'bar_idx': i,
                         'gap_low': gap_low, 'gap_high': gap_high,
                         'ce': (gap_low + gap_high) / 2,
                         'size': gap_high - gap_low,
                         'ts': df['ts'].iloc[i], 'filled': False})
        elif c0_low > c2_high:
            gap_high, gap_low = c0_low, c2_high
            fvgs.append({'type': 'bearish', 'bar_idx': i,
                         'gap_low': gap_low, 'gap_high': gap_high,
                         'ce': (gap_low + gap_high) / 2,
                         'size': gap_high - gap_low,
                         'ts': df['ts'].iloc[i], 'filled': False})

    for fvg in fvgs:
        for j in range(fvg['bar_idx'] + 2, len(df)):
            if fvg['type'] == 'bullish' and df['low'].iloc[j] <= fvg['gap_low']:
                fvg['filled'] = True
                break
            if fvg['type'] == 'bearish' and df['high'].iloc[j] >= fvg['gap_high']:
                fvg['filled'] = True
                break

    return fvgs


# ── Entry Setup ───────────────────────────────────────────────────────────────
def generate_setup(df, choch, fvgs, swing_highs, swing_lows):
    """
    After CHoCH:
      1. Find the most recent unfilled FVG in the new direction.
      2. Entry  = CE line (+/- slippage).
      3. Stop   = outside the most extreme swing point in the wrong direction.
      4. TP     = entry +/- risk * TP_RISK_RATIO.
    """
    if choch is None:
        return None

    choch_bar = choch['bar_idx']
    new_dir   = choch['direction']

    candidates = [
        f for f in fvgs
        if f['bar_idx'] >= choch_bar
        and f['type'] == new_dir
        and not f['filled']
    ]
    if not candidates:
        return None

    fvg   = candidates[-1]
    entry = fvg['ce']

    if new_dir == 'bullish':
        exec_entry = entry * (1 + SLIPPAGE_BPS / 10000)
        anchors    = [df['low'].iloc[i] for i in swing_lows if i >= choch_bar]
        anchor     = min(anchors) if anchors else choch['level']
        stop       = anchor * (1 - STOP_BUFFER)
        risk       = exec_entry - stop
        tp         = exec_entry + risk * TP_RISK_RATIO
    else:
        exec_entry = entry * (1 - SLIPPAGE_BPS / 10000)
        anchors    = [df['high'].iloc[i] for i in swing_highs if i >= choch_bar]
        anchor     = max(anchors) if anchors else choch['level']
        stop       = anchor * (1 + STOP_BUFFER)
        risk       = stop - exec_entry
        tp         = exec_entry - risk * TP_RISK_RATIO

    if risk <= 0:
        return None

    return {
        'direction':   new_dir,
        'entry':       round(exec_entry, 8),
        'stop':        round(stop, 8),
        'tp':          round(tp, 8),
        'risk_pct':    round(risk / exec_entry * 100, 4),
        'rr_ratio':    TP_RISK_RATIO,
        'fvg':         fvg,
        'ce_line':     round(fvg['ce'], 8),
        'choch_level': choch['level'],
    }


# ── Report ────────────────────────────────────────────────────────────────────
def print_report(symbol, df, swing_highs, swing_lows,
                 bos_events, trend, choch, trendline, tl_break, fvgs, setup):

    SEP     = '-' * 62
    current = df['close'].iloc[-1]
    now     = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')

    print(f'\n{SEP}')
    print(f'  ICT STRUCTURE ANALYSIS  |  {symbol}  |  {TIMEFRAME}')
    print(f'  {now}')
    print(SEP)
    print(f'\n  Price (last confirmed bar): {current:,.8g}')
    print(f'  Range: {df["ts"].iloc[0].strftime("%m-%d %H:%M")} -> '
          f'{df["ts"].iloc[-1].strftime("%m-%d %H:%M")}  ({len(df)} candles)')
    print(f'  Swing highs: {len(swing_highs)}   Swing lows: {len(swing_lows)}')

    # --- TREND ---
    bear_bos = [b for b in bos_events if b['direction'] == 'bearish']
    bull_bos = [b for b in bos_events if b['direction'] == 'bullish']
    print(f'\n  TREND: {trend.upper()}')
    print(f'  BOS count -- bearish: {len(bear_bos)}  bullish: {len(bull_bos)}')
    if bos_events:
        print('  Recent BOS:')
        for b in bos_events[-5:]:
            marker = 'v' if b['direction'] == 'bearish' else '^'
            print(f'    [{marker}] bar {b["bar_idx"]:>3}  '
                  f'level={b["level"]:,.6g}  close={b["close"]:,.6g}  '
                  f'@ {b["ts"].strftime("%m-%d %H:%M")}')

    # --- CHOCH ---
    print('\n  CHANGE OF CHARACTER: ', end='')
    if choch:
        label = '[^] BULLISH' if choch['direction'] == 'bullish' else '[v] BEARISH'
        print(f'{label} confirmed')
        print(f'    bar {choch["bar_idx"]}  '
              f'broke level={choch["level"]:,.6g}  '
              f'close={choch["close"]:,.6g}  '
              f'@ {choch["ts"].strftime("%m-%d %H:%M")}')
    else:
        print('not detected')

    # --- TREND LINE ---
    print('\n  TREND LINE: ', end='')
    if trendline:
        print(f'slope={trendline["slope"]:+.6f}/bar  '
              f'{len(trendline["touches"])} confirmed touches')
        for t in trendline['touches']:
            print(f'    bar {t["idx"]:>3}  '
                  f'price={t["price"]:,.6g}  '
                  f'line={t["line_val"]:,.6g}  '
                  f'@ {t["ts"].strftime("%m-%d %H:%M")}')
        if tl_break:
            print(f'    *** BREAK  bar {tl_break["bar_idx"]}  '
                  f'close={tl_break["close"]:,.6g}  '
                  f'line={tl_break["line_val"]:,.6g}  '
                  f'@ {tl_break["ts"].strftime("%m-%d %H:%M")}')
        else:
            print('    (not yet broken)')
    else:
        print('insufficient swing points')

    # --- FAIR VALUE GAPS ---
    unfilled = [f for f in fvgs if not f['filled']]
    print(f'\n  FAIR VALUE GAPS: {len(fvgs)} total  /  {len(unfilled)} unfilled')
    if unfilled:
        for f in unfilled[-5:]:
            tag = '[^]' if f['type'] == 'bullish' else '[v]'
            print(f'    {tag} bar {f["bar_idx"]:>3}  '
                  f'gap={f["gap_low"]:,.6g} to {f["gap_high"]:,.6g}  '
                  f'CE={f["ce"]:,.6g}  '
                  f'size={f["size"]:,.4g}  '
                  f'@ {f["ts"].strftime("%m-%d %H:%M")}')

    # --- SETUP ---
    print(f'\n{SEP}')
    if setup:
        direction_label = 'LONG' if setup['direction'] == 'bullish' else 'SHORT'
        print(f'  SETUP: {direction_label}')
        print(f'\n  Entry  (CE + slippage)  {setup["entry"]:>18,.8g}')
        print(f'  Stop loss               {setup["stop"]:>18,.8g}')
        print(f'  Take profit  ({TP_RISK_RATIO}R)     {setup["tp"]:>18,.8g}')
        print(f'  Risk                    {setup["risk_pct"]:>17.3f}%')

        fvg = setup['fvg']
        print(f'\n  FVG range    {fvg["gap_low"]:,.6g} to {fvg["gap_high"]:,.6g}')
        print(f'  CE line      {setup["ce_line"]:,.8g}')
        print(f'  CHoCH level  {setup["choch_level"]:,.6g}')

        dist_pct = abs(current - setup['entry']) / current * 100
        side     = 'above' if setup['direction'] == 'bullish' else 'below'
        if dist_pct < 0.01:
            print('\n  !! Entry is AT current price -- monitor closely')
        else:
            print(f'\n  Entry is {dist_pct:.3f}% {side} current price')
    else:
        print('  NO SETUP -- conditions not met:')
        if trend == 'undefined':
            print(f'  - Trend undefined (need >={MIN_BOS_TREND} BOS in same direction)')
        if choch is None:
            print('  - No Change of Character detected')
        if choch is not None and not [
                f for f in fvgs
                if f['bar_idx'] >= choch['bar_idx']
                and f['type'] == choch['direction']
                and not f['filled']]:
            print('  - No unfilled FVG in new direction after CHoCH')
    print(SEP)


# ── Main ──────────────────────────────────────────────────────────────────────
def analyse(symbol: str):
    print(f'\nFetching {CANDLE_COUNT}x{TIMEFRAME} candles for {symbol}...')
    exchange = get_exchange()
    df = fetch_candles(exchange, symbol)

    swing_highs, swing_lows = find_swings(df)
    bos_events              = find_bos(df, swing_highs, swing_lows)
    trend                   = determine_trend(bos_events)

    choch     = find_choch(df, bos_events, trend, swing_highs, swing_lows)
    tl_pts    = swing_highs if trend == 'bearish' else swing_lows
    trendline = build_trendline(df, tl_pts, trend) if trend != 'undefined' else None
    tl_start  = choch['bar_idx'] if choch else 0
    tl_break  = find_trendline_break(df, trendline, trend, tl_start)

    fvg_start = choch['bar_idx'] if choch else 0
    fvgs      = find_fvgs(df, after_idx=fvg_start)

    setup = generate_setup(df, choch, fvgs, swing_highs, swing_lows)

    print_report(symbol, df, swing_highs, swing_lows,
                 bos_events, trend, choch, trendline, tl_break, fvgs, setup)
    return setup


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print('Usage: python ict_structure_bot.py BTC/USD')
        print('       python ict_structure_bot.py ETH/USD SOL/USD ADA/USD')
        sys.exit(1)

    symbols = sys.argv[1:]
    results = {}

    for sym in symbols:
        try:
            results[sym] = analyse(sym)
        except Exception as e:
            print(f'\nERROR analysing {sym}: {e}')
            results[sym] = None
        if len(symbols) > 1:
            time.sleep(1.5)

    if len(symbols) > 1:
        SEP = '-' * 62
        print(f'\n{SEP}')
        print('  MULTI-SYMBOL SUMMARY')
        print(SEP)
        for sym, s in results.items():
            if s:
                label = 'LONG ' if s['direction'] == 'bullish' else 'SHORT'
                print(f'  {label}  {sym:<14}  entry={s["entry"]:,.6g}  '
                      f'stop={s["stop"]:,.6g}  tp={s["tp"]:,.6g}  '
                      f'risk={s["risk_pct"]}%')
            else:
                print(f'  ----  {sym:<14}  no setup')
        print(SEP)
