"""
╔══════════════════════════════════════════════════════════════════════════╗
║              KRAKEN SCOUT V1 — Entry Scout + Telegram Alerts            ║
╠══════════════════════════════════════════════════════════════════════════╣
║  WHAT THIS BOT DOES:                                                     ║
║    1. Scans top 15 liquid Kraken USD pairs + 7 core pairs (deduped)      ║
║    2. Scores each asset 0-100 for reversal / uptrend probability         ║
║    3. Enters a position if score >= 55 and market conditions allow       ║
║    4. Sends you a Telegram message instantly with full breakdown          ║
║    5. You manually decide when to exit — YOU are the exit strategy       ║
║    6. The ONLY auto-exit: price drops 5% of position USD → cut + alert  ║
║                                                                          ║
║  WHAT THIS BOT DOES NOT DO:                                              ║
║    - Manage exits (that's your job after reviewing the Telegram alert)   ║
║    - Chase momentum without a reversal or trend confirmation signal      ║
║    - Enter anything when BTC is in a confirmed bear regime               ║
║                                                                          ║
║  SCORING SYSTEM (0-100):                                                 ║
║    Reversal signals  → RSI divergence, MACD turning, BB lower band,     ║
║                        volume spike at low, EQL proximity               ║
║    Trend signals     → EMA21 > EMA55, price > EMA21, ADX rising,        ║
║                        MACD histogram positive + growing                ║
║    Market multiplier → BULL ×1.25 | NEUTRAL ×1.0 | BEAR ×0.5           ║
║                                                                          ║
║  SCORE → POSITION SIZE ($500 capital):                                   ║
║    85-100  → $120  (high conviction + market aligned)                    ║
║    70-84   → $90   (strong signal)                                       ║
║    55-69   → $60   (valid signal, smaller size)                          ║
║    < 55    → no entry                                                    ║
║                                                                          ║
║  EMERGENCY CUT: 5% of position USD lost → instant sell + Telegram       ║
║    $120 position → cuts at -$6.00 loss                                   ║
║    $90  position → cuts at -$4.50 loss                                   ║
║    $60  position → cuts at -$3.00 loss                                   ║
╚══════════════════════════════════════════════════════════════════════════╝

SETUP (5 minutes):
  1. pip install ccxt pandas numpy requests
  2. Create a Telegram bot:
       → Open Telegram, search @BotFather
       → Send /newbot, follow prompts, copy the token
       → Send any message to your new bot
       → Visit: https://api.telegram.org/bot<TOKEN>/getUpdates
       → Find "chat":{"id": <NUMBER>} — that is your CHAT_ID
  3. Fill in the config section below
  4. Set PAPER_TRADING = False when ready for real orders
  5. python3 kraken_scout_v1.py

RUNNING IN TMUX (survives disconnects):
  tmux new -s scout
  python3 kraken_scout_v1.py
  Ctrl+B then D   ← detach (bot keeps running)
  tmux attach -t scout  ← reattach

EMERGENCY STOP:
  touch EMERGENCY_STOP   ← bot detects this, closes all positions, exits
"""

import ccxt
import pandas as pd
import numpy as np
import json
import csv
import os
import sys
import math
import time
import signal
import traceback
import requests
from datetime import datetime, timezone
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════
# ██  CONFIG — Fill this in before running
# ══════════════════════════════════════════════════════════════════════════

API_KEY    = '/AL34kjaYAnSVw0cMAiGj62vGLAD93UjGuusEQg1K6QglQez62/t0Wip'
API_SECRET = 'ToIeXiQivgB8pmFmlX5gDKCSCNhDj+WuJ/05Pc+bNxxAzO92H7q7Yi5olJ1TaAVc7qDau6jP42Uvuc8+oURAbg=='

M_CHAT_ID   = '183920471'

PAPER_TRADING = True   # ← set False when ready for real money

# Capital and risk
STARTING_CAPITAL  = 500.0
MAX_POSITIONS     = 4          # max simultaneous open positions
DRY_POWDER_PCT    = 0.20       # always keep 20% cash in reserve
EMERGENCY_CUT_PCT = 0.05       # cut position if 5% of entry USD is lost
                                # $90 position → cut at -$4.50

# Entry score thresholds → position sizes
SCORE_TIERS = [
    (85, 120.0),   # score 85-100 → $120
    (70,  90.0),   # score 70-84  → $90
    (55,  60.0),   # score 55-69  → $60
]
MIN_SCORE    = 55      # below this → no entry
MIN_POS_USD  = 15.0    # Kraken minimum order floor

# Core pairs — always in the scan regardless of volume rank
CORE_PAIRS = [
    'BTC/USD', 'ETH/USD', 'SOL/USD',
    'XRP/USD', 'DOGE/USD', 'ADA/USD', 'DOT/USD',
]

# How many top-volume pairs to fetch from Kraken dynamically
TOP_N_LIQUID = 15

COOLDOWN_SECS  = 4 * 3600    # 4h cooldown per symbol after any exit
LOOP_SLEEP_SEC = 60           # scan every 60 seconds
SLIPPAGE_BPS   = 10           # 10 bps slippage model

# Files
EMERGENCY_STOP_FILE = 'EMERGENCY_STOP'
STATE_FILE          = 'scout_state.json'
AUDIT_FILE          = 'scout_audit.csv'
LOG_FILE            = 'scout_events.log'

# ══════════════════════════════════════════════════════════════════════════
# ██  GLOBALS
# ══════════════════════════════════════════════════════════════════════════

_shutdown      = False
_start_wall    = time.time()
_last_api_call = 0.0

# ══════════════════════════════════════════════════════════════════════════
# ██  SIGNAL HANDLERS
# ══════════════════════════════════════════════════════════════════════════

def _handle_signal(sig, frame):
    global _shutdown
    _shutdown = True
    _log('SHUTDOWN', f'Signal {sig} — finishing cycle then stopping')

signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT,  _handle_signal)

# ══════════════════════════════════════════════════════════════════════════
# ██  UTILITIES
# ══════════════════════════════════════════════════════════════════════════

def sf(v, d=0.0):
    try:
        f = float(v)
        return d if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return d

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')

def now_ts() -> float:
    return time.time()

def _log(tag: str, msg: str):
    line = f"[{utc_now()}] [{tag:14s}] {msg}"
    print(line)
    try:
        with open(LOG_FILE, 'a') as f:
            f.write(line + '\n')
    except Exception:
        pass

def _write_audit(row: dict):
    exists = os.path.exists(AUDIT_FILE)
    with open(AUDIT_FILE, 'a', newline='') as f:
        w = csv.DictWriter(f, fieldnames=list(row.keys()))
        if not exists:
            w.writeheader()
        w.writerow(row)

# ══════════════════════════════════════════════════════════════════════════
# ██  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════

def _tg(message: str, silent: bool = False):
    """Send a Telegram message. Fails gracefully — never crashes the bot."""
    if not TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN == 'YOUR_TELEGRAM_BOT_TOKEN':
        _log('TELEGRAM', 'Token not set — skipping notification')
        return
    url = f'https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage'
    payload = {
        'chat_id':                  TELEGRAM_CHAT_ID,
        'text':                     message,
        'parse_mode':               'HTML',
        'disable_notification':     silent,
        'disable_web_page_preview': True,
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.status_code != 200:
            _log('TELEGRAM', f'Send failed: {r.status_code} {r.text[:120]}')
    except Exception as e:
        _log('TELEGRAM', f'Error: {e}')


def _tg_entry(sym: str, score: int, size_usd: float, entry_price: float,
              signals: list, regime: str, raw_score: int,
              score_breakdown: dict, emergency_cut_usd: float):
    """Full entry notification — formatted for mobile reading."""
    base = sym.replace('/USD', '')
    chart_url = f'https://www.tradingview.com/chart/?symbol=KRAKEN:{base}USD'
    kraken_url = f'https://www.kraken.com/prices/{base}'

    # Score bar visual
    filled  = int(score / 10)
    bar     = '█' * filled + '░' * (10 - filled)

    # Signal list
    sig_lines = '\n'.join(f'  ✅ {s}' for s in signals) if signals else '  (none logged)'

    # Score breakdown
    bd_lines = '\n'.join(
        f'  {k}: +{v}' for k, v in score_breakdown.items() if v > 0
    )

    regime_emoji = {'BULL': '🟢', 'NEUTRAL': '🟡', 'BEAR': '🔴'}.get(regime, '⚪')

    msg = (
        f"🎯 <b>SCOUT ENTRY — {sym}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 Score: <b>{score}/100</b> (raw {raw_score} × regime mult)\n"
        f"[{bar}]\n\n"
        f"{regime_emoji} Market regime: <b>{regime}</b>\n\n"
        f"💵 Entry price:  <b>${entry_price:,.4f}</b>\n"
        f"💰 Position:     <b>${size_usd:.2f}</b>\n"
        f"🛑 Emergency cut at: <b>-${emergency_cut_usd:.2f}</b> "
        f"(price = ${entry_price * (1 - EMERGENCY_CUT_PCT):.4f})\n\n"
        f"📡 Signals fired:\n{sig_lines}\n\n"
        f"🔢 Score breakdown:\n{bd_lines}\n\n"
        f"<b>⚠️ ACTION REQUIRED: Review this trade.</b>\n"
        f"You are managing the exit.\n"
        f"Bot will only auto-sell if emergency cut triggers.\n\n"
        f"📈 <a href='{chart_url}'>TradingView Chart</a>\n"
        f"🏦 <a href='{kraken_url}'>Kraken</a>"
    )
    _tg(msg)
    _log('TELEGRAM', f'Entry alert sent for {sym}')


def _tg_emergency_cut(sym: str, entry_price: float, exit_price: float,
                      loss_usd: float, size_usd: float):
    msg = (
        f"🚨 <b>EMERGENCY CUT — {sym}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Price dropped 5% of position before review.\n\n"
        f"Entry:  ${entry_price:,.4f}\n"
        f"Exit:   ${exit_price:,.4f}\n"
        f"Loss:   <b>-${abs(loss_usd):.2f}</b> of ${size_usd:.2f} position\n\n"
        f"Position closed automatically. No action needed."
    )
    _tg(msg)
    _log('TELEGRAM', f'Emergency cut alert sent for {sym}')


def _tg_heartbeat(equity: float, positions: dict, regime: str,
                  scan_count: int, pairs_count: int):
    """Periodic silent heartbeat so you know the bot is alive."""
    pos_lines = ''
    for sym, p in positions.items():
        gain_pct = (p.get('current_price', p['entry_price']) - p['entry_price']) \
                   / p['entry_price'] * 100
        pos_lines += f"\n  {sym}: ${p['size_usd']:.0f} @ ${p['entry_price']:.4f} ({gain_pct:+.2f}%)"

    regime_emoji = {'BULL': '🟢', 'NEUTRAL': '🟡', 'BEAR': '🔴'}.get(regime, '⚪')
    mode = 'PAPER' if PAPER_TRADING else 'LIVE'

    msg = (
        f"💓 <b>Scout Heartbeat [{mode}]</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"🕐 {utc_now()}\n"
        f"{regime_emoji} Regime: {regime}\n"
        f"💰 Equity: ${equity:.2f}\n"
        f"📊 Scans completed: {scan_count}\n"
        f"🔍 Pairs watched: {pairs_count}\n"
        f"📂 Open positions: {len(positions)}{pos_lines}\n\n"
        f"Bot is running normally."
    )
    _tg(msg, silent=True)


def _tg_shutdown(reason: str, equity: float, trade_count: int):
    msg = (
        f"🔴 <b>Scout Bot Shutdown</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n"
        f"Reason: {reason}\n"
        f"Final equity: ${equity:.2f}\n"
        f"Trades taken: {trade_count}\n"
        f"Time: {utc_now()}"
    )
    _tg(msg)

# ══════════════════════════════════════════════════════════════════════════
# ██  STATE
# ══════════════════════════════════════════════════════════════════════════

def _default_state() -> dict:
    return {
        'equity':      STARTING_CAPITAL,
        'positions':   {},
        'cooldowns':   {},
        'trade_count': 0,
        'win_count':   0,
        'total_pnl':   0.0,
        'boot_time':   now_ts(),
        'scan_count':  0,
        'version':     'SCOUT_V1',
    }

def load_state() -> dict:
    if not os.path.exists(STATE_FILE):
        _log('BOOT', 'No state file — fresh start')
        return _default_state()
    try:
        with open(STATE_FILE) as f:
            s = json.load(f)
        _log('BOOT', f'State loaded — equity=${s["equity"]:.2f} positions={len(s["positions"])}')
        return s
    except Exception as e:
        _log('STATE', f'Load failed ({e}) — fresh start')
        return _default_state()

def save_state(state: dict):
    tmp = STATE_FILE + '.tmp'
    with open(tmp, 'w') as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp, STATE_FILE)

# ══════════════════════════════════════════════════════════════════════════
# ██  RATE-LIMITED API CALLS
# ══════════════════════════════════════════════════════════════════════════

def api_call(fn, *args, **kwargs):
    global _last_api_call
    gap = time.time() - _last_api_call
    if gap < 1.5:
        time.sleep(1.5 - gap)
    for attempt in range(5):
        try:
            result = fn(*args, **kwargs)
            _last_api_call = time.time()
            return result
        except ccxt.RateLimitExceeded:
            wait = 10 * (2 ** attempt)
            _log('RATE_LIMIT', f'429 — backoff {wait}s')
            time.sleep(wait)
        except (ccxt.NetworkError, ccxt.RequestTimeout) as e:
            if attempt == 4:
                _log('API_ERR', f'Network error: {e}')
                return None
            time.sleep(5 * (2 ** attempt))
        except Exception as e:
            _log('API_ERR', f'{e}')
            return None
    return None

# ══════════════════════════════════════════════════════════════════════════
# ██  DYNAMIC PAIR LIST
# ══════════════════════════════════════════════════════════════════════════

def build_pair_list(exchange) -> list:
    """
    Fetch top N liquid USD pairs by 24h volume from Kraken,
    union with CORE_PAIRS, deduplicate, return sorted list.
    Always returns at least len(CORE_PAIRS) pairs.
    """
    try:
        tickers = api_call(exchange.fetch_tickers)
        if tickers is None:
            return CORE_PAIRS[:]

        usd_pairs = {
            sym: t for sym, t in tickers.items()
            if sym.endswith('/USD') and sf(t.get('quoteVolume', 0)) > 0
        }
        ranked = sorted(usd_pairs.items(),
                        key=lambda x: sf(x[1].get('quoteVolume', 0)),
                        reverse=True)

        top_n = [sym for sym, _ in ranked[:TOP_N_LIQUID]]

        # Union with core pairs, deduplicated, preserve order
        combined = list(dict.fromkeys(top_n + CORE_PAIRS))
        _log('PAIRS', f'Scan list: {len(combined)} pairs '
                      f'({len(top_n)} top-volume + {len(CORE_PAIRS)} core, deduped)')
        return combined

    except Exception as e:
        _log('PAIRS', f'Failed to build dynamic list ({e}) — using core pairs')
        return CORE_PAIRS[:]

# ══════════════════════════════════════════════════════════════════════════
# ██  DATA FETCH
# ══════════════════════════════════════════════════════════════════════════

def fetch_ohlcv(exchange, sym: str, tf: str, limit: int = 210):
    try:
        raw = api_call(exchange.fetch_ohlcv, sym, tf, limit=limit)
        if raw is None or len(raw) < 50:
            return None
        df = pd.DataFrame(raw, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
        df.drop_duplicates('ts', inplace=True)
        df.sort_values('ts', inplace=True)
        df.reset_index(drop=True, inplace=True)
        for col in ['o', 'h', 'l', 'c', 'v']:
            df[col] = df[col].astype(float)
        return df
    except Exception as e:
        _log('FETCH', f'{sym}/{tf}: {e}')
        return None

# ══════════════════════════════════════════════════════════════════════════
# ██  INDICATORS
# ══════════════════════════════════════════════════════════════════════════

def ind_ema(c: pd.Series, span: int) -> pd.Series:
    return c.ewm(span=span, adjust=False).mean()

def ind_macd(c: pd.Series, fast=12, slow=26, sig=9):
    ml = c.ewm(span=fast, adjust=False).mean() - c.ewm(span=slow, adjust=False).mean()
    sl = ml.ewm(span=sig, adjust=False).mean()
    return ml, sl, ml - sl

def ind_rsi(c: pd.Series, p=14) -> pd.Series:
    d  = c.diff()
    g  = d.clip(lower=0).ewm(com=p-1, adjust=False).mean()
    ls = (-d.clip(upper=0)).ewm(com=p-1, adjust=False).mean()
    return 100 - 100 / (1 + g / (ls + 1e-9))

def ind_adx(h, l, c, p=14):
    up  = h.diff()
    dn  = -l.diff()
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr  = pd.concat([h - l,
                     (h - c.shift()).abs(),
                     (l - c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/p, adjust=False).mean()
    pdi = 100 * pd.Series(pdm, index=c.index).ewm(alpha=1/p, adjust=False).mean() / (atr + 1e-9)
    mdi = 100 * pd.Series(mdm, index=c.index).ewm(alpha=1/p, adjust=False).mean() / (atr + 1e-9)
    dx  = 100 * (pdi - mdi).abs() / (pdi + mdi + 1e-9)
    return dx.ewm(alpha=1/p, adjust=False).mean(), atr

def ind_bb(c: pd.Series, p=20, mult=2.0):
    mid = c.rolling(p).mean()
    std = c.rolling(p).std()
    return mid - mult * std, mid, mid + mult * std

def ind_mfi(h, l, c, v, p=14) -> pd.Series:
    tp  = (h + l + c) / 3
    rmf = tp * v
    pos = rmf.where(tp > tp.shift(1), 0.0).rolling(p).sum()
    neg = rmf.where(tp < tp.shift(1), 0.0).rolling(p).sum()
    return 100 - 100 / (1 + pos / (neg + 1e-9))

# ══════════════════════════════════════════════════════════════════════════
# ██  REGIME DETECTION (BTC as market anchor)
# ══════════════════════════════════════════════════════════════════════════

def detect_regime(df_btc_1h) -> str:
    if df_btc_1h is None or len(df_btc_1h) < 60:
        return 'NEUTRAL'
    c   = df_btc_1h['c']
    e21 = sf(ind_ema(c, 21).iloc[-2])
    e55 = sf(ind_ema(c, 55).iloc[-2])
    p   = sf(c.iloc[-2])
    if   e21 > e55 and p > e21:  return 'BULL'
    elif e21 < e55:               return 'BEAR'
    return 'NEUTRAL'

# ══════════════════════════════════════════════════════════════════════════
# ██  EQUAL LOWS DETECTION
# ══════════════════════════════════════════════════════════════════════════

def find_equal_lows(df1h, tolerance=0.003, lookback=40) -> float | None:
    """
    Finds the most recent Equal Low cluster — two or more swing lows
    within tolerance% of each other on 1h candles.
    Returns the EQL price level if found, None otherwise.
    Used to add conviction when price is sitting at a known liquidity pool.
    """
    if df1h is None or len(df1h) < lookback + 5:
        return None
    sub = df1h.tail(lookback + 4)
    lv  = sub['l'].values

    # Find swing lows (lower than 2 candles each side)
    swing_lows = []
    for i in range(2, len(lv) - 2):
        if (lv[i] < lv[i-1] and lv[i] < lv[i-2]
                and lv[i] < lv[i+1] and lv[i] < lv[i+2]):
            swing_lows.append(float(lv[i]))

    if len(swing_lows) < 2:
        return None

    # Check if the two most recent swing lows are within tolerance
    lo1, lo2 = swing_lows[-2], swing_lows[-1]
    if abs(lo1 - lo2) / (lo2 + 1e-9) <= tolerance:
        return (lo1 + lo2) / 2   # return average of the cluster

    return None

# ══════════════════════════════════════════════════════════════════════════
# ██  RSI DIVERGENCE DETECTION
# ══════════════════════════════════════════════════════════════════════════

def detect_rsi_divergence(df5: pd.DataFrame, lookback=40) -> bool:
    """
    Bullish RSI divergence: price making lower lows while RSI makes higher lows.
    This is one of the highest-probability reversal signals in technical analysis.
    Confirms the selling is exhausted — momentum is turning before price does.
    """
    if df5 is None or len(df5) < lookback + 5:
        return False

    c        = df5['c']
    rsi_vals = ind_rsi(c)
    cv       = c.values[-lookback:]
    rv       = rsi_vals.values[-lookback:]

    # Find price swing lows
    price_lows = [(i, cv[i]) for i in range(2, len(cv)-2)
                  if cv[i] < cv[i-1] and cv[i] < cv[i-2]
                  and cv[i] < cv[i+1] and cv[i] < cv[i+2]]

    if len(price_lows) < 2:
        return False

    (i1, p1), (i2, p2) = price_lows[-2], price_lows[-1]

    # Price lower low required
    if p2 >= p1:
        return False

    # RSI must be higher at the second low (divergence)
    rsi1, rsi2 = float(rv[i1]), float(rv[i2])
    return rsi2 > rsi1 and rsi2 < 50   # RSI still in oversold territory

# ══════════════════════════════════════════════════════════════════════════
# ██  MACD HISTOGRAM REVERSAL
# ══════════════════════════════════════════════════════════════════════════

def detect_macd_turning(df5: pd.DataFrame) -> tuple:
    """
    Checks if MACD histogram is turning from negative toward zero.
    This is the 2-bar confirmation pattern that got 3/4 on the BTC backtest.

    Returns (is_turning: bool, current_hist: float, direction: str)
    """
    if df5 is None or len(df5) < 50:
        return False, 0.0, ''

    _, _, hist = ind_macd(df5['c'])
    h_now  = sf(hist.iloc[-2])
    h_prev = sf(hist.iloc[-3])
    h_pp   = sf(hist.iloc[-4])

    # Turning up from negative: getting less negative over 2 bars
    if h_now < 0 and h_prev < 0 and h_now > h_prev > h_pp:
        return True, h_now, 'TURNING_UP'

    # Already crossed to positive with 2-bar confirmation
    if h_now > 0 and h_prev > 0 and h_pp <= 0:
        return True, h_now, 'CROSSED_POSITIVE'

    return False, h_now, ''

# ══════════════════════════════════════════════════════════════════════════
# ██  VOLUME SPIKE DETECTION
# ══════════════════════════════════════════════════════════════════════════

def detect_volume_spike(df5: pd.DataFrame, threshold=1.8) -> tuple:
    """
    Checks for an above-average volume on recent candles.
    A volume spike at a low = absorption candle = sellers exhausted.
    Returns (spike_detected: bool, volume_ratio: float)
    """
    if df5 is None or len(df5) < 25:
        return False, 0.0
    v     = df5['v']
    avg   = sf(v.rolling(20).mean().iloc[-2])
    curr  = sf(v.iloc[-2])
    ratio = curr / (avg + 1e-9)
    return ratio >= threshold, round(ratio, 2)

# ══════════════════════════════════════════════════════════════════════════
# ██  TREND CONFIRMATION
# ══════════════════════════════════════════════════════════════════════════

def detect_trend_signals(df1h, df5: pd.DataFrame) -> dict:
    """
    Checks for established uptrend signals on 1h candles.
    Returns dict of signal_name → points_value.
    """
    signals = {}

    if df1h is None or len(df1h) < 60:
        return signals

    c   = df1h['c']
    e21 = ind_ema(c, 21)
    e55 = ind_ema(c, 55)

    e21_now  = sf(e21.iloc[-2])
    e55_now  = sf(e55.iloc[-2])
    e21_prev = sf(e21.iloc[-3])
    e55_prev = sf(e55.iloc[-3])
    price    = sf(c.iloc[-2])

    # EMA21 crossed above EMA55 recently (within last 5 candles)
    recently_crossed = False
    for i in range(-6, -1):
        try:
            if e21.iloc[i] > e55.iloc[i] and e21.iloc[i-1] <= e55.iloc[i-1]:
                recently_crossed = True
                break
        except Exception:
            pass

    if recently_crossed:
        signals['EMA21_CROSSED_ABOVE_EMA55_1H'] = 18

    # EMA21 > EMA55 (established uptrend on 1h)
    if e21_now > e55_now and price > e21_now:
        signals['PRICE_ABOVE_EMA21_EMA55_1H'] = 14

    # EMA21 trending up (slope positive)
    if e21_now > e21_prev:
        signals['EMA21_SLOPE_UP'] = 6

    # ADX rising from low (trend building, not exhausted)
    adx_s, _ = ind_adx(df1h['h'], df1h['l'], c)
    adx_now  = sf(adx_s.iloc[-2])
    adx_prev = sf(adx_s.iloc[-5])
    if 12 <= adx_now <= 35 and adx_now > adx_prev:
        signals['ADX_RISING'] = 8

    # Price reclaimed EMA21 on 5m (micro confirmation)
    if df5 is not None and len(df5) >= 30:
        c5   = df5['c']
        e21_5 = ind_ema(c5, 21)
        if sf(c5.iloc[-2]) > sf(e21_5.iloc[-2]) and sf(c5.iloc[-3]) <= sf(e21_5.iloc[-3]):
            signals['PRICE_RECLAIMED_EMA21_5M'] = 10

    return signals

# ══════════════════════════════════════════════════════════════════════════
# ██  MASTER SCORER
# ══════════════════════════════════════════════════════════════════════════

def score_asset(sym: str, df5, df1h, regime: str) -> tuple:
    """
    Scores an asset 0-100 for probability of upward move.

    Returns:
        (final_score: int, raw_score: int, signals_fired: list,
         breakdown: dict, signal_price: float)

    The score is built from two categories:
      - Reversal signals (bottomed out + turning)
      - Trend signals (already moving up)

    Then multiplied by a market regime factor.

    Philosophy: the bot should fire on EITHER a reversal OR a trend —
    not both required. A clear reversal at the lows with RSI divergence
    is just as valid as a clean EMA21/EMA55 cross on established trend.
    """
    if df5 is None or len(df5) < 80:
        return 0, 0, [], {}, 0.0

    I = -2   # penultimate candle — no look-ahead
    c, h, l, v = df5['c'], df5['h'], df5['l'], df5['v']
    price = sf(c.iloc[I])
    if price <= 0:
        return 0, 0, [], {}, 0.0

    signals_fired = []
    breakdown     = {}

    # ── REVERSAL SIGNALS ────────────────────────────────────────────────

    # 1. RSI divergence (strongest reversal signal — momentum turning first)
    rsi_s   = ind_rsi(c)
    rsi_now = sf(rsi_s.iloc[I])

    if detect_rsi_divergence(df5):
        pts = 22
        breakdown['RSI_DIVERGENCE'] = pts
        signals_fired.append(f'RSI Divergence (RSI={rsi_now:.1f})')
    elif rsi_now < 28:
        pts = 14
        breakdown['RSI_EXTREME_OVERSOLD'] = pts
        signals_fired.append(f'RSI Extreme Oversold ({rsi_now:.1f})')
    elif rsi_now < 35:
        pts = 8
        breakdown['RSI_OVERSOLD'] = pts
        signals_fired.append(f'RSI Oversold ({rsi_now:.1f})')

    # 2. MACD histogram turning from negative toward zero
    macd_turning, hist_val, macd_dir = detect_macd_turning(df5)
    if macd_turning:
        if macd_dir == 'CROSSED_POSITIVE':
            pts = 20
            breakdown['MACD_CROSSED_POSITIVE'] = pts
            signals_fired.append(f'MACD Crossed Zero (hist={hist_val:.6f})')
        else:
            pts = 12
            breakdown['MACD_TURNING_UP'] = pts
            signals_fired.append(f'MACD Histogram Turning Up (hist={hist_val:.6f})')

    # 3. Bollinger Band lower band touch (mean reversion setup)
    bb_lo, bb_mid, bb_hi = ind_bb(c)
    bb_lo_v  = sf(bb_lo.iloc[I])
    bb_mid_v = sf(bb_mid.iloc[I])

    if price <= bb_lo_v * 1.005:
        pts = 14
        breakdown['BB_LOWER_BAND'] = pts
        pct_from_mid = (bb_mid_v - price) / price * 100
        signals_fired.append(f'At BB Lower Band (midband {pct_from_mid:.1f}% above)')

    # 4. Volume spike at recent low (absorption — sellers exhausted)
    vol_spike, vol_ratio = detect_volume_spike(df5)
    if vol_spike:
        pts = 10
        breakdown['VOLUME_SPIKE'] = pts
        signals_fired.append(f'Volume Spike ({vol_ratio:.1f}x average)')

    # 5. Equal Low proximity (liquidity pool — smart money buys here)
    eql = find_equal_lows(df1h)
    if eql is not None:
        dist_pct = abs(price - eql) / eql * 100
        if dist_pct <= 0.5:
            pts = 14
            breakdown['EQL_PROXIMITY'] = pts
            signals_fired.append(f'At Equal Low cluster (EQL=${eql:.4f}, dist={dist_pct:.2f}%)')
        elif dist_pct <= 1.5:
            pts = 6
            breakdown['EQL_NEARBY'] = pts
            signals_fired.append(f'Near Equal Low cluster (EQL=${eql:.4f}, dist={dist_pct:.2f}%)')

    # 6. MFI oversold (money flow confirming exit of sellers)
    mfi_s   = ind_mfi(h, l, c, v)
    mfi_now = sf(mfi_s.iloc[I])
    if mfi_now < 25:
        pts = 10
        breakdown['MFI_OVERSOLD'] = pts
        signals_fired.append(f'MFI Oversold ({mfi_now:.1f})')
    elif mfi_now < 35:
        pts = 5
        breakdown['MFI_LOW'] = pts
        signals_fired.append(f'MFI Low ({mfi_now:.1f})')

    # ── TREND SIGNALS ────────────────────────────────────────────────────

    trend_sigs = detect_trend_signals(df1h, df5)
    for sig_name, pts in trend_sigs.items():
        breakdown[sig_name] = pts
    if trend_sigs:
        total_trend = sum(trend_sigs.values())
        signals_fired.append(f'Trend signals: {", ".join(trend_sigs.keys())} (+{total_trend}pts)')

    # ── HARD BLOCKS — disqualify regardless of score ──────────────────────
    # RSI too high = momentum exhausted at entry
    if rsi_now > 72:
        return 0, 0, [], {}, price

    # MACD histogram negative AND getting more negative = still falling
    _, _, hist_series = ind_macd(c)
    h_now  = sf(hist_series.iloc[I])
    h_prev = sf(hist_series.iloc[I-1])
    if h_now < 0 and h_now < h_prev and rsi_now > 50:
        return 0, 0, [], {}, price

    # ── RAW SCORE ────────────────────────────────────────────────────────
    raw_score = sum(breakdown.values())

    # ── REGIME MULTIPLIER ─────────────────────────────────────────────────
    # This is the market condition adjustment.
    # BULL: everything gets a tailwind — lower threshold, higher score
    # NEUTRAL: you're on your own — score stands as-is
    # BEAR: you need a very strong signal to justify buying into headwind
    mult = {'BULL': 1.25, 'NEUTRAL': 1.0, 'BEAR': 0.50}.get(regime, 1.0)
    final_score = min(100, int(raw_score * mult))

    return final_score, raw_score, signals_fired, breakdown, price

# ══════════════════════════════════════════════════════════════════════════
# ██  POSITION SIZING
# ══════════════════════════════════════════════════════════════════════════

def score_to_size(score: int, equity: float, positions: dict) -> float:
    """
    Score → USD position size, respecting dry powder and max positions.
    """
    deployed  = sum(p['size_usd'] for p in positions.values())
    available = max(0.0, equity * (1 - DRY_POWDER_PCT) - deployed)
    if available < MIN_POS_USD * 1.5:
        return 0.0

    for threshold, size in SCORE_TIERS:
        if score >= threshold:
            actual = min(size, available)
            return actual if actual >= MIN_POS_USD else 0.0

    return 0.0

# ══════════════════════════════════════════════════════════════════════════
# ██  ORDER EXECUTION
# ══════════════════════════════════════════════════════════════════════════

def execute_buy(exchange, sym: str, size_usd: float, price: float):
    slip     = SLIPPAGE_BPS / 10000
    exec_p   = price * (1 + slip)
    if PAPER_TRADING:
        return {'price': exec_p, 'qty': size_usd / exec_p, 'mode': 'PAPER'}
    try:
        qty   = size_usd / price
        order = api_call(exchange.create_market_buy_order, sym, qty)
        if order is None:
            return None
        fill  = sf(order.get('average', exec_p))
        return {'price': fill, 'qty': qty, 'mode': 'LIVE', 'id': order.get('id')}
    except Exception as e:
        _log('ORDER', f'BUY {sym} failed: {e}')
        return None

def execute_sell(exchange, sym: str, pos: dict, price: float):
    slip   = SLIPPAGE_BPS / 10000
    exec_p = price * (1 - slip)
    if PAPER_TRADING:
        pnl = pos['size_usd'] * (exec_p - pos['entry_price']) / pos['entry_price']
        return {'price': exec_p, 'pnl': pnl, 'mode': 'PAPER'}
    try:
        qty   = pos.get('qty', pos['size_usd'] / pos['entry_price'])
        order = api_call(exchange.create_market_sell_order, sym, qty)
        if order is None:
            return None
        fill  = sf(order.get('average', exec_p))
        pnl   = pos['size_usd'] * (fill - pos['entry_price']) / pos['entry_price']
        return {'price': fill, 'pnl': pnl, 'mode': 'LIVE', 'id': order.get('id')}
    except Exception as e:
        _log('ORDER', f'SELL {sym} failed: {e}')
        return None

# ══════════════════════════════════════════════════════════════════════════
# ██  EMERGENCY CUT CHECK
# ══════════════════════════════════════════════════════════════════════════

def check_emergency_cut(pos: dict, current_price: float) -> bool:
    """
    The ONLY auto-exit logic in this bot.
    If the position has lost 5% of the USD amount invested → cut it.
    Simple. No exceptions. No regime checks.

    $120 position: cut at -$6.00
    $90  position: cut at -$4.50
    $60  position: cut at -$3.00
    """
    loss_usd = pos['size_usd'] * (current_price - pos['entry_price']) / pos['entry_price']
    threshold = -(pos['size_usd'] * EMERGENCY_CUT_PCT)
    return loss_usd <= threshold

# ══════════════════════════════════════════════════════════════════════════
# ██  HEARTBEAT DISPLAY
# ══════════════════════════════════════════════════════════════════════════

def print_heartbeat(state: dict, regime: str, pairs: list,
                    bar_n: int, prices: dict):
    equity = state['equity']
    pos    = state.get('positions', {})

    unrealized = sum(
        p['size_usd'] * (prices.get(sym, p['entry_price']) - p['entry_price'])
        / p['entry_price']
        for sym, p in pos.items()
    )
    live_eq  = equity + unrealized
    pnl_tot  = live_eq - STARTING_CAPITAL
    pnl_pct  = pnl_tot / STARTING_CAPITAL * 100
    uptime_h = (now_ts() - state.get('boot_time', _start_wall)) / 3600
    mode_tag = '[PAPER]' if PAPER_TRADING else '[LIVE ]'

    W  = 72
    ln = '═' * W
    ln2 = '─' * W

    regime_icon = {'BULL': '🟢 BULL', 'NEUTRAL': '🟡 NEUTRAL', 'BEAR': '🔴 BEAR'}.get(regime, regime)

    print(f'\n{ln}')
    print(f'  KRAKEN SCOUT V1 ♥ {mode_tag}  |  Scan #{bar_n}')
    print(f'  {utc_now()}  |  Uptime: {uptime_h:.1f}h  |  Regime: {regime_icon}')
    print(f'  Scanning {len(pairs)} pairs  |  Trades: {state["trade_count"]}  '
          f'|  Scans: {state["scan_count"]}')
    print(ln2)
    print(f'  Starting capital:  ${STARTING_CAPITAL:>10,.2f}')
    print(f'  Cash equity:       ${equity:>10,.2f}')
    print(f'  Unrealized P&L:    ${unrealized:>+10,.2f}')
    print(f'  ► Live equity:     ${live_eq:>10,.2f}   ({pnl_tot:>+.2f}, {pnl_pct:>+.2f}%)')
    print(f'  Realized P&L:      ${state["total_pnl"]:>+10.2f}')
    print(ln2)

    if pos:
        print(f'  OPEN POSITIONS (you are managing exits):')
        for sym, p in pos.items():
            price    = prices.get(sym, p['entry_price'])
            gain_pct = (price - p['entry_price']) / p['entry_price'] * 100
            gain_usd = p['size_usd'] * (price - p['entry_price']) / p['entry_price']
            cut_px   = p['entry_price'] * (1 - EMERGENCY_CUT_PCT)
            cut_usd  = p['size_usd'] * EMERGENCY_CUT_PCT
            g_sym    = '+' if gain_usd >= 0 else ''
            print(f'  ┌ {sym:<12} [Score={p.get("score",0):>3}] '
                  f'[{p.get("strategy","?")}] [{p.get("regime","?")} at entry]')
            print(f'  │  Entry: ${p["entry_price"]:>12,.4f}  |  Now: ${price:>12,.4f}')
            print(f'  │  P&L:   {g_sym}${gain_usd:>9,.2f} ({gain_pct:>+.2f}%)  '
                  f'|  Size: ${p["size_usd"]:.2f}')
            print(f'  └  Emergency cut: ${cut_px:.4f}  (at -${cut_usd:.2f})')
    else:
        print(f'  No open positions — scanning for entries')
        print(f'  Cooldowns active: '
              f'{sum(1 for t in state.get("cooldowns",{}).values() if float(t) > now_ts())}')

    print(ln)
    print()

# ══════════════════════════════════════════════════════════════════════════
# ██  MAIN LOOP
# ══════════════════════════════════════════════════════════════════════════

def main():
    global _shutdown

    print('╔══════════════════════════════════════════════════════════════════════╗')
    print('║           KRAKEN SCOUT V1 — Entry Scout + Telegram Alerts           ║')
    print('║   Finds reversals + uptrends → enters → notifies → you decide exit  ║')
    print(f'║   Mode: {"PAPER TRADING":^60} ║' if PAPER_TRADING else
          f'║   Mode: {"LIVE TRADING (real money)":^60} ║')
    print('╚══════════════════════════════════════════════════════════════════════╝')
    print()

    if not PAPER_TRADING and API_KEY == 'YOUR_KRAKEN_API_KEY':
        print('ERROR: Live mode selected but API keys not set. Exiting.')
        sys.exit(1)

    if TELEGRAM_BOT_TOKEN == 'YOUR_TELEGRAM_BOT_TOKEN':
        print('WARNING: Telegram not configured — notifications will be skipped.')
        print('         See setup instructions at top of file.\n')

    exchange = ccxt.kraken({
        'apiKey':          API_KEY,
        'secret':          API_SECRET,
        'enableRateLimit': True,
    })

    state     = load_state()
    positions = state.get('positions', {})
    cooldowns = {k: float(v) for k, v in state.get('cooldowns', {}).items()}
    state['boot_time'] = now_ts()

    # Build initial pair list
    scan_pairs = build_pair_list(exchange)

    bar_n          = 0
    prices         = {}
    last_tg_hb     = 0.0    # last Telegram heartbeat
    last_pair_refresh = 0.0  # last dynamic pair list refresh

    _log('BOOT', f'Scout started | pairs={len(scan_pairs)} | '
                 f'equity=${state["equity"]:.2f} | mode={"PAPER" if PAPER_TRADING else "LIVE"}')

    _tg(f'🚀 <b>Kraken Scout V1 Started</b>\n'
        f'Mode: {"PAPER" if PAPER_TRADING else "LIVE"}\n'
        f'Capital: ${STARTING_CAPITAL:.2f}\n'
        f'Watching {len(scan_pairs)} pairs\n'
        f'Emergency cut: 5% of position USD\n'
        f'{utc_now()}')

    while not _shutdown:

        # ── Emergency stop file ───────────────────────────────────────────
        if os.path.exists(EMERGENCY_STOP_FILE):
            _log('EMERGENCY', 'EMERGENCY_STOP file detected — shutting down')
            _tg_shutdown('EMERGENCY_STOP file detected', state['equity'], state['trade_count'])
            _shutdown = True
            break

        bar_n       += 1
        loop_start   = now_ts()
        state['scan_count'] = state.get('scan_count', 0) + 1

        try:
            # ── Refresh pair list every 4 hours ──────────────────────────
            if now_ts() - last_pair_refresh > 4 * 3600:
                scan_pairs        = build_pair_list(exchange)
                last_pair_refresh = now_ts()

            # ── BTC 1h for market regime ──────────────────────────────────
            btc_1h        = fetch_ohlcv(exchange, 'BTC/USD', '1h', limit=80)
            market_regime = detect_regime(btc_1h)

            # ── Fetch current prices for all open positions first ─────────
            for sym in list(positions.keys()):
                df5 = fetch_ohlcv(exchange, sym, '5m', limit=10)
                if df5 is not None:
                    prices[sym] = sf(df5['c'].iloc[-1])

            # ── Emergency cut check on ALL open positions ─────────────────
            for sym in list(positions.keys()):
                if sym not in prices:
                    continue
                pos   = positions[sym]
                price = prices[sym]

                if check_emergency_cut(pos, price):
                    result  = execute_sell(exchange, sym, pos, price)
                    pnl     = result['pnl'] if result else 0.0
                    exit_px = result['price'] if result else price
                    loss_usd = pos['size_usd'] * (exit_px - pos['entry_price']) / pos['entry_price']

                    state['equity']      += pnl
                    state['total_pnl']   += pnl
                    state['trade_count'] += 1

                    cooldowns[sym] = now_ts() + COOLDOWN_SECS

                    _write_audit({
                        'time':        utc_now(),
                        'symbol':      sym,
                        'strategy':    pos.get('strategy', '?'),
                        'score':       pos.get('score', 0),
                        'entry_time':  pos.get('entry_time', '?'),
                        'entry_price': round(pos['entry_price'], 6),
                        'exit_price':  round(exit_px, 6),
                        'size_usd':    round(pos['size_usd'], 2),
                        'pnl_usd':     round(pnl, 4),
                        'pnl_pct':     round(pnl / pos['size_usd'] * 100, 3),
                        'reason':      'EMERGENCY_CUT_5PCT',
                        'regime':      market_regime,
                        'mode':        'PAPER' if PAPER_TRADING else 'LIVE',
                    })

                    _tg_emergency_cut(sym, pos['entry_price'], exit_px,
                                      loss_usd, pos['size_usd'])
                    _log('EMERGENCY_CUT', f'{sym} | loss=${pnl:.2f} | '
                                          f'equity=${state["equity"]:.2f}')
                    del positions[sym]

            # ── Heartbeat display every loop ──────────────────────────────
            print_heartbeat(state, market_regime, scan_pairs, bar_n, prices)

            # ── Telegram heartbeat every 6 hours ─────────────────────────
            if now_ts() - last_tg_hb > 6 * 3600:
                _tg_heartbeat(state['equity'], positions,
                              market_regime, state['scan_count'], len(scan_pairs))
                last_tg_hb = now_ts()

            # ── BEAR regime — no new entries ──────────────────────────────
            if market_regime == 'BEAR':
                _log('SCAN', f'Market regime BEAR — skipping entry scan (cash preservation)')
                save_state({**state, 'positions': positions,
                            'cooldowns': {k: str(v) for k, v in cooldowns.items()}})
                elapsed = now_ts() - loop_start
                time.sleep(max(0, LOOP_SLEEP_SEC - elapsed))
                continue

            # ── ENTRY SCAN ────────────────────────────────────────────────
            if len(positions) < MAX_POSITIONS:
                _log('SCAN', f'Scanning {len(scan_pairs)} pairs '
                             f'({MAX_POSITIONS - len(positions)} slots free, regime={market_regime})')

                for sym in scan_pairs:
                    if _shutdown:
                        break
                    if sym in positions:
                        continue
                    if len(positions) >= MAX_POSITIONS:
                        break
                    if cooldowns.get(sym, 0) > now_ts():
                        continue

                    # Fetch data for this symbol
                    df5  = fetch_ohlcv(exchange, sym, '5m', limit=210)
                    df1h = fetch_ohlcv(exchange, sym, '1h', limit=80)

                    if df5 is not None:
                        prices[sym] = sf(df5['c'].iloc[-1])

                    # Score the asset
                    score, raw_score, signals, breakdown, sig_price = \
                        score_asset(sym, df5, df1h, market_regime)

                    if score < MIN_SCORE:
                        continue

                    # Size the position
                    unrealized_now = sum(
                        p['size_usd'] * (prices.get(s, p['entry_price']) - p['entry_price'])
                        / p['entry_price']
                        for s, p in positions.items()
                    )
                    current_eq = state['equity'] + unrealized_now
                    size = score_to_size(score, current_eq, positions)

                    if size < MIN_POS_USD:
                        _log('SCAN', f'{sym}: score={score} but insufficient capital')
                        continue

                    # Execute entry
                    price  = prices.get(sym, sig_price)
                    result = execute_buy(exchange, sym, size, price)
                    if result is None:
                        _log('ENTRY_FAIL', f'{sym} — order failed')
                        continue

                    exec_price = result['price']
                    cut_usd    = size * EMERGENCY_CUT_PCT

                    # Determine strategy label
                    if 'RSI_DIVERGENCE' in breakdown or 'MACD_TURNING_UP' in breakdown:
                        strategy = 'REVERSAL'
                    elif 'EMA21_CROSSED_ABOVE_EMA55_1H' in breakdown:
                        strategy = 'TREND_BREAK'
                    elif 'BB_LOWER_BAND' in breakdown:
                        strategy = 'MEAN_REV'
                    else:
                        strategy = 'CONFLUENCE'

                    positions[sym] = {
                        'entry_price': exec_price,
                        'size_usd':    size,
                        'qty':         result.get('qty', size / exec_price),
                        'score':       score,
                        'raw_score':   raw_score,
                        'strategy':    strategy,
                        'regime':      market_regime,
                        'signals':     signals,
                        'breakdown':   breakdown,
                        'entry_time':  utc_now(),
                    }

                    # Notify via Telegram immediately
                    _tg_entry(
                        sym          = sym,
                        score        = score,
                        size_usd     = size,
                        entry_price  = exec_price,
                        signals      = signals,
                        regime       = market_regime,
                        raw_score    = raw_score,
                        score_breakdown = breakdown,
                        emergency_cut_usd = cut_usd,
                    )

                    _log('ENTRY', (
                        f'{sym} | score={score} (raw={raw_score}) | '
                        f'strategy={strategy} | regime={market_regime} | '
                        f'size=${size:.2f} | price=${exec_price:.4f} | '
                        f'cut_at=-${cut_usd:.2f} | '
                        f'mode={"PAPER" if PAPER_TRADING else "LIVE"}'
                    ))

                    _write_audit({
                        'time':        utc_now(),
                        'symbol':      sym,
                        'side':        'BUY',
                        'strategy':    strategy,
                        'score':       score,
                        'raw_score':   raw_score,
                        'regime':      market_regime,
                        'entry_price': round(exec_price, 6),
                        'size_usd':    round(size, 2),
                        'signals':     ' | '.join(signals),
                        'mode':        'PAPER' if PAPER_TRADING else 'LIVE',
                    })

            # ── Save state ────────────────────────────────────────────────
            save_state({**state, 'positions': positions,
                        'cooldowns': {k: str(v) for k, v in cooldowns.items()}})

        except Exception as e:
            _log('LOOP_ERR', f'Unhandled error: {e}')
            traceback.print_exc()

        # ── Sleep ─────────────────────────────────────────────────────────
        elapsed = now_ts() - loop_start
        sleep_t = max(0, LOOP_SLEEP_SEC - elapsed)
        if not _shutdown:
            time.sleep(sleep_t)

    # ── Clean shutdown ────────────────────────────────────────────────────
    save_state({**state, 'positions': positions,
                'cooldowns': {k: str(v) for k, v in cooldowns.items()}})
    _tg_shutdown('Clean shutdown', state['equity'], state['trade_count'])
    _log('SHUTDOWN', f'Done. Equity=${state["equity"]:.2f} | Trades={state["trade_count"]}')
    print('\n[DONE] Scout shut down cleanly.')


if __name__ == '__main__':
    main()
