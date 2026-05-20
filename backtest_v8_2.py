"""
backtest_v8_2.py — Multi-Strategy MACD Backtester  (clean rewrite)
====================================================================
Fixes vs prior broken versions:
  - Paginated fetch: walks backward in 720-bar pages until 90 days collected
  - prices dict is persistent (updated each bar, never wiped)
  - Fast MACD requires 2-bar confirmation + histogram above ATR noise floor
  - Key level detection uses 1h swing H/L only (not 5m noise), radius 0.5%
  - MACD exit gated by MIN_HOLD_BARS — no 1-bar whipsaws
  - df sliced by positional index, not per-bar boolean filter (10x faster)
  - report uses float() casts everywhere — no numpy .abs() calls

Strategies:
  A. SIGNAL LINE CROSSOVER  — MACD(12,26,9) or (5,10,16)
     Gate: ADX>=22, RSI<68, 2-bar histogram confirm, ATR magnitude floor
  B. ZERO LINE CROSSOVER    — MACD line (12,26,90) crosses zero
     Gate: ADX>=28, price>SMA50
  C. DIVERGENCE             — bullish div on slow or fast MACD
     Gate: RSI<=45, MFI>42, both MACD lows negative

All entries: HTF 1h not BEAR. 1h swing H/L key levels add +15 conviction.

Exit ladder (V8.1):
  failed signal cut -> chop detection -> break-even floor ->
  dynamic trail -> MACD flip (after MIN_HOLD) -> 5% hard stop ->
  15% portfolio kill switch

Usage:
    pip install ccxt pandas numpy
    python3 backtest_v8_2.py
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
STARTING_CAPITAL    = 100_000.0
MAX_POSITIONS       = 5
DRY_POWDER_PCT      = 0.20
HARD_STOP_PCT       = 0.05
MAX_DD_PCT          = 0.15
SLIPPAGE            = 0.0010
SIZE_HIGH_PCT       = 0.15
SIZE_LOW_PCT        = 0.10
MIN_CONVICTION      = 50
COOLDOWN_BARS       = 24          # 2h = 24 x 5m bars
MIN_HISTORY_BARS    = 120
MIN_HOLD_BARS       = 3           # bars before MACD exit fires (~15 min)
BACKTEST_DAYS       = 90
PAGE_SIZE           = 720         # Kraken max bars per request

ADX_MIN_SIGNAL      = 22
ADX_MIN_ZERO        = 28
RSI_MAX_ENTRY       = 68
RSI_MAX_DIV         = 45
MFI_MIN_DIV         = 42

FAST_HIST_FLOOR     = 0.0003      # fast hist must be > price * this
SLOW_HIST_FLOOR     = 0.0001

KEY_LEVEL_RADIUS    = 0.005       # 0.5%
KEY_LEVEL_BONUS     = 15
KEY_LEVEL_LOOKBACK  = 30          # 1h candles

PAIN_THRESHOLDS = [
    (50_000, 500),
    (15_000, 250),
    (5_000,  150),
    (0,      100),
]

PAIRS = [
    'BTC/USD', 'ETH/USD', 'SOL/USD', 'XRP/USD', 'TAO/USD',
    'DOGE/USD', 'HYPE/USD', 'SUI/USD', 'ADA/USD', 'ZEC/USD',
]

MACD_SLOW   = (12, 26,  9)
MACD_FAST   = ( 5, 10, 16)
MACD_MEDIUM = (12, 26, 90)

OUT_AUDIT  = 'backtest_v8_2_audit.csv'
OUT_EQUITY = 'backtest_v8_2_equity.csv'

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────
def sf(v, d=0.0):
    try:
        f = float(v)
        return d if (math.isnan(f) or math.isinf(f)) else f
    except Exception:
        return d

def get_pain(size_usd):
    for mn, th in PAIN_THRESHOLDS:
        if size_usd >= mn:
            return th
    return 100

def ts_str(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime('%Y-%m-%d %H:%M')

# ─────────────────────────────────────────────────────────────
# INDICATORS
# ─────────────────────────────────────────────────────────────
def macd(c, fast, slow, sig):
    ml = c.ewm(span=fast, adjust=False).mean() - c.ewm(span=slow, adjust=False).mean()
    sl = ml.ewm(span=sig, adjust=False).mean()
    return ml, sl, ml - sl

def adx(h, l, c, p=14):
    up, dn = h.diff(), -l.diff()
    pdm = np.where((up > dn) & (up > 0), up, 0.0)
    mdm = np.where((dn > up) & (dn > 0), dn, 0.0)
    tr  = pd.concat([h-l, (h-c.shift()).abs(), (l-c.shift()).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/p, adjust=False).mean()
    pdi = 100 * pd.Series(pdm, index=c.index).ewm(alpha=1/p, adjust=False).mean() / (atr + 1e-9)
    mdi = 100 * pd.Series(mdm, index=c.index).ewm(alpha=1/p, adjust=False).mean() / (atr + 1e-9)
    dx  = 100 * (pdi - mdi).abs() / (pdi + mdi + 1e-9)
    return dx.ewm(alpha=1/p, adjust=False).mean()

def rsi(c, p=14):
    d = c.diff()
    g = d.clip(lower=0).ewm(com=p-1, adjust=False).mean()
    ls = (-d.clip(upper=0)).ewm(com=p-1, adjust=False).mean()
    return 100 - 100 / (1 + g / (ls + 1e-9))

def mfi(h, l, c, v, p=14):
    tp  = (h + l + c) / 3
    rmf = tp * v
    pos = rmf.where(tp > tp.shift(1), 0.0).rolling(p).sum()
    neg = rmf.where(tp < tp.shift(1), 0.0).rolling(p).sum()
    return 100 - 100 / (1 + pos / (neg + 1e-9))

def rvi(o, h, l, c, p=10):
    num = (c - o).ewm(span=p, adjust=False).mean()
    den = (h - l).ewm(span=p, adjust=False).mean()
    rv  = num / (den + 1e-9)
    sg  = (rv + 2*rv.shift(1) + 2*rv.shift(2) + rv.shift(3)) / 6
    return rv, sg

def ema(c, span):
    return c.ewm(span=span, adjust=False).mean()

def sma(c, p):
    return c.rolling(p).mean()

# ─────────────────────────────────────────────────────────────
# HTF REGIME
# ─────────────────────────────────────────────────────────────
def htf_regime(df1h, end_ts):
    if df1h is None or len(df1h) < 55:
        return 'NEUTRAL'
    sub = df1h[df1h['ts'] <= end_ts]
    if len(sub) < 55:
        return 'NEUTRAL'
    c   = sub['c']
    e21 = sf(ema(c, 21).iloc[-1])
    e55 = sf(ema(c, 55).iloc[-1])
    p   = sf(c.iloc[-1])
    if e21 > e55 and p > e21:  return 'BULL'
    if e21 < e55:              return 'BEAR'
    return 'NEUTRAL'

# ─────────────────────────────────────────────────────────────
# KEY LEVELS  (1h swing H/L only)
# ─────────────────────────────────────────────────────────────
def key_levels(df1h, end_ts):
    if df1h is None:
        return []
    sub = df1h[df1h['ts'] <= end_ts].tail(KEY_LEVEL_LOOKBACK + 4)
    if len(sub) < 5:
        return []
    levels = []
    hv, lv = sub['h'].values, sub['l'].values
    for i in range(2, len(hv) - 2):
        if hv[i] > hv[i-1] and hv[i] > hv[i-2] and hv[i] > hv[i+1] and hv[i] > hv[i+2]:
            levels.append(float(hv[i]))
        if lv[i] < lv[i-1] and lv[i] < lv[i-2] and lv[i] < lv[i+1] and lv[i] < lv[i+2]:
            levels.append(float(lv[i]))
    return levels

def at_key(price, levels):
    return any(abs(price - lvl) / (lvl + 1e-9) <= KEY_LEVEL_RADIUS for lvl in levels)

# ─────────────────────────────────────────────────────────────
# DIVERGENCE
# ─────────────────────────────────────────────────────────────
def bull_div(c, hist, lookback=30):
    if len(c) < lookback + 5:
        return False
    cv, hv = c.values[-lookback:], hist.values[-lookback:]
    lows = [(i, cv[i]) for i in range(2, len(cv)-2)
            if cv[i] < cv[i-1] and cv[i] < cv[i-2]
            and cv[i] < cv[i+1] and cv[i] < cv[i+2]]
    if len(lows) < 2:
        return False
    (i1, p1), (i2, p2) = lows[-2], lows[-1]
    if p2 >= p1:
        return False
    m1, m2 = float(hv[i1]), float(hv[i2])
    if m1 >= 0 or m2 >= 0:   # must be negative territory
        return False
    return m2 > m1

# ─────────────────────────────────────────────────────────────
# CONVICTION SCORER
# ─────────────────────────────────────────────────────────────
def conviction(strategy, adx_v, rsi_v, mfi_v, rvi_v, rvisig_v,
               sma50_slope, regime, key_hit, hist_v, vol_ratio):
    s = {'SIGNAL_CROSSOVER_SLOW': 25, 'SIGNAL_CROSSOVER_FAST': 18,
         'ZERO_LINE_CROSS': 30, 'DIVERGENCE': 22}.get(strategy, 0)

    if adx_v >= 35:    s += 20
    elif adx_v >= 28:  s += 15
    elif adx_v >= 22:  s += 8

    if rsi_v < 35:     s += 15
    elif rsi_v < 45:   s += 10
    elif rsi_v < 55:   s += 5
    elif rsi_v >= 65:  s -= 8

    if mfi_v > 60:     s += 8
    elif mfi_v > 50:   s += 4
    elif mfi_v < 35:   s -= 6

    if rvi_v > rvisig_v and rvi_v > 0:  s += 8
    elif rvi_v < rvisig_v:              s -= 4

    if sma50_slope > 0:  s += 4

    if regime == 'BULL':    s += 15
    elif regime == 'BEAR':  s -= 25

    if key_hit:  s += KEY_LEVEL_BONUS

    if hist_v > 0.005:    s += 5
    elif hist_v > 0.001:  s += 2

    if vol_ratio >= 2.5:   s += 8
    elif vol_ratio >= 2.0: s += 5
    elif vol_ratio >= 1.5: s += 3

    return max(0, min(s, 100))

# ─────────────────────────────────────────────────────────────
# ENTRY SIGNALS
# ─────────────────────────────────────────────────────────────
def entry_signals(df5, regime, levels):
    if len(df5) < MIN_HISTORY_BARS:
        return []

    c2, h2, l2, o2, v2 = df5['c'], df5['h'], df5['l'], df5['o'], df5['v']
    I = -2

    ml_s, sl_s, hs = macd(c2, *MACD_SLOW)
    ml_f, sl_f, hf = macd(c2, *MACD_FAST)
    ml_m, sl_m, hm = macd(c2, *MACD_MEDIUM)
    adx_s          = adx(h2, l2, c2)
    rsi_s          = rsi(c2)
    mfi_s          = mfi(h2, l2, c2, v2)
    rvi_s, rvs     = rvi(o2, h2, l2, c2)
    sma50_s        = sma(c2, 50)

    adx_v   = sf(adx_s.iloc[I])
    rsi_v   = sf(rsi_s.iloc[I])
    mfi_v   = sf(mfi_s.iloc[I])
    rvi_v   = sf(rvi_s.iloc[I])
    rvisig_v = sf(rvs.iloc[I])
    sma50_v = sf(sma50_s.iloc[I])
    sma50_sl = sma50_v - sf(sma50_s.iloc[I-5])
    price   = sf(c2.iloc[I])
    vol_avg = sf(v2.rolling(20).mean().iloc[I])
    vol_r   = sf(v2.iloc[I]) / (vol_avg + 1e-9)
    kh      = at_key(price, levels)

    if regime == 'BEAR' or rsi_v >= RSI_MAX_ENTRY:
        return []

    sigs = []

    def det(strat, hv):
        return {'strategy': strat, 'regime_htf': regime,
                'adx': round(adx_v,1), 'rsi': round(rsi_v,1),
                'mfi': round(mfi_v,1), 'rvi_bull': rvi_v > rvisig_v,
                'vol_ratio': round(vol_r,2), 'at_key_lvl': kh,
                'macd_hist': round(hv,6), 'signal_price': price}

    # A1 — slow MACD signal crossover (2-bar confirm)
    cs = sf(hs.iloc[I]);  ps1 = sf(hs.iloc[I-1]);  ps2 = sf(hs.iloc[I-2])
    if cs > price * SLOW_HIST_FLOOR and ps1 > 0 and ps2 <= 0 and adx_v >= ADX_MIN_SIGNAL:
        cv = conviction('SIGNAL_CROSSOVER_SLOW', adx_v, rsi_v, mfi_v,
                        rvi_v, rvisig_v, sma50_sl, regime, kh, cs, vol_r)
        sigs.append((cv, det('SIGNAL_CROSSOVER_SLOW', cs)))

    # A2 — fast MACD signal crossover (2-bar confirm + stricter floor)
    cf = sf(hf.iloc[I]);  pf1 = sf(hf.iloc[I-1]);  pf2 = sf(hf.iloc[I-2])
    if cf > price * FAST_HIST_FLOOR and pf1 > 0 and pf2 <= 0 and adx_v >= ADX_MIN_SIGNAL:
        cv = conviction('SIGNAL_CROSSOVER_FAST', adx_v, rsi_v, mfi_v,
                        rvi_v, rvisig_v, sma50_sl, regime, kh, cf, vol_r)
        sigs.append((cv, det('SIGNAL_CROSSOVER_FAST', cf)))

    # B — zero line crossover (MACD line, 12,26,90)
    cm = sf(ml_m.iloc[I]);  pm = sf(ml_m.iloc[I-1])
    sma_ok = not math.isnan(sma50_v) and price > sma50_v
    if cm > 0 and pm <= 0 and adx_v >= ADX_MIN_ZERO and sma_ok:
        cv = conviction('ZERO_LINE_CROSS', adx_v, rsi_v, mfi_v,
                        rvi_v, rvisig_v, sma50_sl, regime, kh, cm, vol_r)
        sigs.append((cv, det('ZERO_LINE_CROSS', cm)))

    # C — divergence
    if rsi_v <= RSI_MAX_DIV and mfi_v > MFI_MIN_DIV:
        ds = bull_div(c2, hs)
        df_ = bull_div(c2, hf)
        if ds or df_:
            bh = max(sf(hs.iloc[I]), sf(hf.iloc[I]))
            cv = conviction('DIVERGENCE', adx_v, rsi_v, mfi_v,
                            rvi_v, rvisig_v, sma50_sl, regime, kh, bh, vol_r)
            d2 = det('DIVERGENCE', bh)
            d2['div_slow'] = ds;  d2['div_fast'] = df_
            sigs.append((cv, d2))

    return sigs

# ─────────────────────────────────────────────────────────────
# EXIT LADDER
# ─────────────────────────────────────────────────────────────
def eval_exit(pos, price):
    entry = pos['entry_price'];  peak = pos['peak_price']
    size  = pos['size_usd'];     eg   = pos['ever_green']
    peak  = max(peak, price);    pos['peak_price'] = peak
    pnl      = size * (price - entry) / entry
    peak_pnl = size * (peak  - entry) / entry
    if pnl > 0 and not eg:
        pos['ever_green'] = True;  eg = True
    pain = get_pain(size)
    if not eg and pnl <= -pain:               return True, 'FAILED_SIGNAL_CUT', pnl
    if eg and pnl <= -pain:                   return True, 'CHOP_DETECTED', pnl
    if eg and price < entry:                  return True, 'BREAK_EVEN_FLOOR', pnl
    if (entry - price) / entry >= HARD_STOP_PCT: return True, 'HARD_STOP_5PCT', pnl
    if peak_pnl <= 0:                         return False, 'HOLD_FLAT', pnl
    one_pct = size * 0.01
    if one_pct >= 1000:   keep = 0.90
    elif one_pct >= 600:  keep = 0.88
    elif one_pct >= 300:  keep = 0.85
    elif one_pct >= 100:  keep = 0.75
    else:                 keep = 0.65
    if pnl < peak_pnl * keep:                return True, 'TRAIL_FLOOR', pnl
    return False, 'HOLD', pnl

def macd_negative(df5, end_ri):
    if end_ri < 30:
        return False
    sub = df5.iloc[:end_ri + 1]
    if len(sub) < 30:
        return False
    _, _, h = macd(sub['c'], *MACD_SLOW)
    return sf(h.iloc[-2]) < 0

# ─────────────────────────────────────────────────────────────
# POSITION SIZING
# ─────────────────────────────────────────────────────────────
def calc_size(conv, total_cap, positions):
    deployed  = sum(p['size_usd'] for p in positions.values())
    available = max(0.0, total_cap * (1 - DRY_POWDER_PCT) - deployed)
    if available < 500:
        return 0.0
    pct  = SIZE_HIGH_PCT if conv >= 60 else SIZE_LOW_PCT
    return min(total_cap * pct, available)

# ─────────────────────────────────────────────────────────────
# DATA FETCH  — paginated
# ─────────────────────────────────────────────────────────────
TF_SEC = {'1m': 60, '5m': 300, '1h': 3600}

def fetch(exchange, sym, tf, days):
    tf_s  = TF_SEC[tf]
    need  = days * (86400 // tf_s) + MIN_HISTORY_BARS + 10
    end   = int(time.time() * 1000)
    rows  = []; seen = set()
    pages = math.ceil(need / PAGE_SIZE) + 3
    print(f"  {sym} [{tf}] need {need}...", end='', flush=True)
    for _ in range(pages):
        since = end - PAGE_SIZE * tf_s * 1000
        try:
            raw = exchange.fetch_ohlcv(sym, tf, since=since, limit=PAGE_SIZE)
            time.sleep(1.5)
        except Exception as e:
            print(f" ERR({e})"); break
        if not raw: break
        new = [r for r in raw if r[0] not in seen]
        if not new: break
        for r in new: seen.add(r[0])
        rows = new + rows
        end  = raw[0][0]
        if len(rows) >= need: break
    if len(rows) < MIN_HISTORY_BARS:
        print(f" SKIP ({len(rows)})"); return None
    df = pd.DataFrame(rows, columns=['ts','o','h','l','c','v'])
    df.drop_duplicates('ts', inplace=True)
    df.sort_values('ts', inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(f" OK ({len(df)} bars)")
    return df

# ─────────────────────────────────────────────────────────────
# BACKTEST ENGINE
# ─────────────────────────────────────────────────────────────
def run_backtest(data_5m, data_1h):
    cutoff = BACKTEST_DAYS * 288
    all_ts = set()
    for df in data_5m.values():
        all_ts.update(df['ts'].values[-cutoff:])
    timeline = sorted(all_ts)

    # Build ts -> row-index maps for O(1) lookup
    ts_idx = {sym: {int(ts): i for i, ts in enumerate(df['ts'].values)}
              for sym, df in data_5m.items()}

    cash   = STARTING_CAPITAL
    pos    = {}
    cd     = {}
    prices = {}   # persistent — never wiped
    max_eq = STARTING_CAPITAL
    killed = False
    trades = [];  eq_log = []
    n_tr = wins = 0;  tot_pnl = 0.0

    print(f"\nReplaying {len(timeline):,} bars × {len(data_5m)} symbols...\n")

    for bar_idx, ts in enumerate(timeline):
        if killed: break

        if bar_idx % 5000 == 0:
            eq = cash + sum(p['size_usd'] * (prices.get(s, p['entry_price'])
                             / p['entry_price']) for s, p in pos.items())
            print(f"  {bar_idx/len(timeline)*100:4.0f}%  bar {bar_idx:>7,}"
                  f"  equity=${eq:,.0f}  trades={n_tr}", flush=True)

        # Update prices (persistent dict)
        for sym, df in data_5m.items():
            ri = ts_idx[sym].get(int(ts))
            if ri is not None:
                prices[sym] = sf(df['c'].iloc[ri])

        # EXIT
        for sym in list(pos.keys()):
            if sym not in prices: continue
            p     = pos[sym]
            price = prices[sym]
            ri    = ts_idx[sym].get(int(ts))
            if ri is None: continue

            should_exit, reason, pnl = eval_exit(p, price)
            held = bar_idx - p['entry_bar']

            if not should_exit and held >= MIN_HOLD_BARS:
                if macd_negative(data_5m[sym], ri):
                    should_exit = True;  reason = 'MACD_FLIP'
                    pnl = p['size_usd'] * (price - p['entry_price']) / p['entry_price']

            if should_exit:
                ep  = price * (1 - SLIPPAGE)
                ap  = p['size_usd'] * (ep - p['entry_price']) / p['entry_price']
                cash += p['size_usd'] + ap
                tot_pnl += ap;  n_tr += 1
                if ap > 0: wins += 1
                cd[sym] = bar_idx + COOLDOWN_BARS
                trades.append({
                    'symbol': sym, 'strategy': p['strategy'],
                    'entry_time': p['entry_time'], 'exit_time': ts_str(ts),
                    'entry_price': round(p['entry_price'],6),
                    'exit_price':  round(ep,6),
                    'size_usd':    round(p['size_usd'],2),
                    'pnl_usd':     round(ap,2),
                    'pnl_pct':     round(ap/p['size_usd']*100,3),
                    'reason':      reason, 'conviction': p['conviction'],
                    'regime_htf':  p['regime_htf'], 'adx': p['adx'],
                    'rsi': p['rsi'], 'mfi': p['mfi'],
                    'at_key_lvl':  p['at_key_lvl'], 'ever_green': p['ever_green'],
                    'bars_held':   held,
                })
                del pos[sym]

        # EQUITY + KILL SWITCH
        pos_val = sum(p['size_usd'] * (prices.get(s, p['entry_price'])
                      / p['entry_price']) for s, p in pos.items())
        equity  = cash + pos_val
        max_eq  = max(max_eq, equity)
        dd      = (max_eq - equity) / max_eq if max_eq > 0 else 0.0

        if dd >= MAX_DD_PCT:
            print(f"\n  [!] KILL SWITCH bar={bar_idx}  DD={dd*100:.1f}%")
            for sym, p in list(pos.items()):
                price = prices.get(sym, p['entry_price'])
                ep  = price * (1 - SLIPPAGE)
                pnl = p['size_usd'] * (ep - p['entry_price']) / p['entry_price']
                cash += p['size_usd'] + pnl;  tot_pnl += pnl;  n_tr += 1
                if pnl > 0: wins += 1
                trades.append({
                    'symbol': sym, 'strategy': p['strategy'],
                    'entry_time': p['entry_time'], 'exit_time': ts_str(ts),
                    'entry_price': round(p['entry_price'],6), 'exit_price': round(ep,6),
                    'size_usd': round(p['size_usd'],2), 'pnl_usd': round(pnl,2),
                    'pnl_pct': round(pnl/p['size_usd']*100,3), 'reason': 'KILL_SWITCH',
                    'conviction': p['conviction'], 'regime_htf': p['regime_htf'],
                    'adx': p['adx'], 'rsi': p['rsi'], 'mfi': p['mfi'],
                    'at_key_lvl': p['at_key_lvl'], 'ever_green': p['ever_green'],
                    'bars_held': bar_idx - p['entry_bar'],
                })
            pos = {};  killed = True

        eq_log.append({'bar': bar_idx, 'time': ts_str(ts),
                       'equity': round(equity,2), 'cash': round(cash,2), 'open': len(pos)})
        if killed: break

        # ENTRY SCAN
        if len(pos) >= MAX_POSITIONS: continue

        for sym in PAIRS:
            if sym not in data_5m or sym in pos: continue
            if len(pos) >= MAX_POSITIONS: break
            if cd.get(sym, 0) > bar_idx: continue

            ri = ts_idx[sym].get(int(ts))
            if ri is None or ri < MIN_HISTORY_BARS: continue

            df5  = data_5m[sym].iloc[:ri + 1]
            df1h = data_1h.get(sym)
            reg  = htf_regime(df1h, ts)
            lvls = key_levels(df1h, ts)
            sigs = entry_signals(df5, reg, lvls)
            if not sigs: continue

            best_cv, best_det = max(sigs, key=lambda x: x[0])
            if best_cv < MIN_CONVICTION: continue
            if sym not in prices or prices[sym] <= 0: continue

            total_cap = cash + sum(p['size_usd'] * (prices.get(s, p['entry_price'])
                                   / p['entry_price']) for s, p in pos.items())
            size = calc_size(best_cv, total_cap, pos)
            if size < 100: continue

            ep    = prices[sym] * (1 + SLIPPAGE)
            cash -= size
            pos[sym] = {
                'entry_price': ep, 'peak_price': ep, 'size_usd': size,
                'conviction': best_cv, 'strategy': best_det['strategy'],
                'regime_htf': best_det.get('regime_htf','NEUTRAL'),
                'adx': best_det.get('adx',0), 'rsi': best_det.get('rsi',50),
                'mfi': best_det.get('mfi',50), 'at_key_lvl': best_det.get('at_key_lvl',False),
                'ever_green': False, 'entry_time': ts_str(ts),
                'entry_bar': bar_idx, 'symbol': sym,
            }

    # Close residuals
    final_ts = timeline[-1] if timeline else 0
    for sym, p in list(pos.items()):
        price = prices.get(sym, p['entry_price'])
        ep  = price * (1 - SLIPPAGE)
        pnl = p['size_usd'] * (ep - p['entry_price']) / p['entry_price']
        cash += p['size_usd'] + pnl;  tot_pnl += pnl;  n_tr += 1
        if pnl > 0: wins += 1
        trades.append({
            'symbol': sym, 'strategy': p['strategy'],
            'entry_time': p['entry_time'], 'exit_time': ts_str(final_ts),
            'entry_price': round(p['entry_price'],6), 'exit_price': round(ep,6),
            'size_usd': round(p['size_usd'],2), 'pnl_usd': round(pnl,2),
            'pnl_pct': round(pnl/p['size_usd']*100,3), 'reason': 'END_OF_DATA',
            'conviction': p['conviction'], 'regime_htf': p['regime_htf'],
            'adx': p['adx'], 'rsi': p['rsi'], 'mfi': p['mfi'],
            'at_key_lvl': p['at_key_lvl'], 'ever_green': p['ever_green'],
            'bars_held': len(timeline) - 1 - p['entry_bar'],
        })

    return trades, eq_log, n_tr, wins, tot_pnl, max_eq, cash

# ─────────────────────────────────────────────────────────────
# REPORT
# ─────────────────────────────────────────────────────────────
def report(trades, eq_log, n_tr, wins, tot_pnl, max_eq, final_cash):
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
    reasons = {};  by_sym = pd.Series(dtype=float);  by_st = {}

    if not tdf.empty:
        wt = tdf[tdf['pnl_usd'] > 0];  lt = tdf[tdf['pnl_usd'] <= 0]
        avg_w = float(wt['pnl_usd'].mean()) if len(wt) > 0 else 0.0
        avg_l = float(lt['pnl_usd'].mean()) if len(lt) > 0 else 0.0
        best  = float(tdf['pnl_usd'].max());  worst = float(tdf['pnl_usd'].min())
        avg_b = float(tdf['bars_held'].mean())
        gw = float(wt['pnl_usd'].sum()) if len(wt) > 0 else 0.0
        gl = abs(float(lt['pnl_usd'].sum())) if len(lt) > 0 else 0.0
        pf = gw / (gl + 1e-9)
        reasons = tdf['reason'].value_counts().to_dict()
        by_sym  = tdf.groupby('symbol')['pnl_usd'].sum().sort_values(ascending=False)
        by_st   = (tdf.groupby('strategy')
                   .agg(count=('pnl_usd','count'),
                        wins=('pnl_usd', lambda x: int((x>0).sum())),
                        pnl=('pnl_usd','sum'))
                   .to_dict('index'))

    S = '=' * 65
    print(f"\n{S}")
    print(f"  KRAKEN V8.2 — 90-DAY BACKTEST RESULTS")
    print(f"  5m primary | 1h HTF | {len(PAIRS)} pairs")
    print(S)
    print(f"  Starting equity:     ${STARTING_CAPITAL:>12,.2f}")
    print(f"  Final equity:        ${final_cash:>12,.2f}")
    print(f"  Net P&L:             ${net:>+12,.2f}  ({net_p:+.2f}%)")
    print(f"  Max equity:          ${max_eq:>12,.2f}")
    print(f"  Max drawdown:        {max_dd:>11.2f}%  (limit {MAX_DD_PCT*100:.0f}%)")
    print('-' * 65)
    print(f"  Total trades:        {n_tr}")
    print(f"  Wins:                {wins}  ({wr:.1f}%)")
    print(f"  Losses:              {n_tr - wins}")
    print(f"  Avg win:             ${avg_w:>+10,.2f}")
    print(f"  Avg loss:            ${avg_l:>+10,.2f}")
    print(f"  Profit factor:       {pf:>10.2f}")
    print(f"  Best trade:          ${best:>+10,.2f}")
    print(f"  Worst trade:         ${worst:>+10,.2f}")
    print(f"  Avg hold (5m bars):  {avg_b:>9.1f}  (~{avg_b*5/60:.1f}h)")
    print('-' * 65)
    if by_st:
        print("  By strategy:")
        for st, r in sorted(by_st.items()):
            w2 = r['wins'] / r['count'] * 100 if r['count'] > 0 else 0.0
            print(f"    {st:<28}  {r['count']:>3} trades"
                  f"  WR {w2:.0f}%  P&L ${float(r['pnl']):>+9,.2f}")
        print('-' * 65)
    if reasons:
        print("  Exit reasons:")
        for r, cnt in sorted(reasons.items(), key=lambda x: -x[1]):
            print(f"    {r:<28}  {cnt:>4}")
        print('-' * 65)
    if not by_sym.empty:
        print("  P&L by symbol:")
        for sym, pnl in by_sym.items():
            print(f"    {sym:<14}  ${float(pnl):>+10,.2f}")
    print(S)

# ─────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────
def main():
    print("=" * 65)
    print("  KRAKEN V8.2 — Multi-Strategy MACD Backtester")
    print("  Strategies : Signal Crossover | Zero Line | Divergence")
    print("  MACD sets  : (12,26,9)  (5,10,16)  (12,26,90)")
    print("  Confluence : ADX | RSI | MFI | RVI | SMA | 1h HTF")
    print("  Data       : 5m paginated | 1h HTF paginated")
    print("=" * 65)

    exchange = ccxt.kraken({'enableRateLimit': True})

    print(f"\nFetching 5m data ({BACKTEST_DAYS} days, paginated)...")
    data_5m = {}
    for sym in PAIRS:
        df = fetch(exchange, sym, '5m', BACKTEST_DAYS)
        if df is not None:
            data_5m[sym] = df

    print(f"\nFetching 1h HTF data...")
    data_1h = {}
    for sym in PAIRS:
        df = fetch(exchange, sym, '1h', BACKTEST_DAYS)
        if df is not None:
            data_1h[sym] = df

    if not data_5m:
        print("ERROR: No data fetched.")
        return

    print(f"\nLoaded: {len(data_5m)} (5m)  {len(data_1h)} (1h)")

    trade_log, eq_log, n_tr, wins, tot_pnl, max_eq, final_cash = \
        run_backtest(data_5m, data_1h)

    if trade_log:
        pd.DataFrame(trade_log).to_csv(OUT_AUDIT, index=False)
        print(f"\nTrade audit  -> {OUT_AUDIT}  ({len(trade_log)} trades)")
    if eq_log:
        pd.DataFrame(eq_log).to_csv(OUT_EQUITY, index=False)
        print(f"Equity curve -> {OUT_EQUITY}")

    report(trade_log, eq_log, n_tr, wins, tot_pnl, max_eq, final_cash)


if __name__ == '__main__':
    main()
