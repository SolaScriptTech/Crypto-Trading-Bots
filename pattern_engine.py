"""
pattern_engine.py — Historical Fingerprint Pattern Matcher
Runs at boot + every 6h as subprocess. Never blocks main loop.
Writes pattern_lookup.json for kraken_v8_1.py to read.
"""
import ccxt, pandas as pd, numpy as np, json, os, time, math
from datetime import datetime, timezone, timedelta

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
OUTPUT_FILE = os.path.join(BASE_DIR, 'pattern_lookup.json')
LOG_FILE    = os.path.join(BASE_DIR, 'kraken_v8_1_events.log')

LOOKBACK_DAYS=365; EXTENDED_DAYS=1095; MIN_MATCHES=3; FORWARD_BARS=4
UP_THRESHOLD=0.70; DOWN_THRESHOLD=0.70
TRAIL_LOOSEN=1.20; TRAIL_TIGHTEN=0.80; TRAIL_STANDARD=1.00
TOL_MACD_HIST_PCT=0.15; TOL_GAIN_PCT=0.003; TOL_VOL_RATIO=0.30; TOL_RSI=5.0

def log(msg):
    line=f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [PatternEngine] {msg}"
    print(line,flush=True)
    try:
        with open(LOG_FILE,'a') as f: f.write(line+"\n")
    except: pass

def sf(v,d=0.0):
    try:
        f=float(v); return d if (math.isnan(f) or math.isinf(f)) else f
    except: return d

def compute_indicators(df):
    df=df.copy()
    e12=df['c'].ewm(span=12,adjust=False).mean()
    e26=df['c'].ewm(span=26,adjust=False).mean()
    macd=e12-e26; sig=macd.ewm(span=9,adjust=False).mean()
    df['hist']=macd-sig
    df['hist_dir']=np.where(df['hist']>df['hist'].shift(1),'rising','falling')
    d=df['c'].diff()
    g=d.clip(lower=0).ewm(com=13,adjust=False).mean()
    l=(-d.clip(upper=0)).ewm(com=13,adjust=False).mean()
    df['rsi']=100-100/(1+g/(l+1e-9))
    df['vol_avg']=df['v'].rolling(20).mean()
    df['vol_ratio']=df['v']/(df['vol_avg']+1e-9)
    e21=df['c'].ewm(span=21,adjust=False).mean()
    e55=df['c'].ewm(span=55,adjust=False).mean()
    df['regime']='NEUTRAL'
    df.loc[(e21>e55)&(df['c']>e21),'regime']='BULL'
    df.loc[e21<e55,'regime']='BEAR'
    s20=df['c'].rolling(20).mean(); std=df['c'].rolling(20).std()
    df['bb_zone']='MID'
    df.loc[df['c']<s20-2*std,'bb_zone']='BELOW_LOWER'
    df.loc[df['c']>s20+2*std,'bb_zone']='ABOVE_UPPER'
    df['rolling_gain']=df['c'].pct_change(4)
    return df.dropna()

def is_similar(fp,row):
    ch=fp['hist']; hh=sf(row['hist'])
    if abs(ch)>1e-8:
        if abs(hh-ch)/abs(ch)>TOL_MACD_HIST_PCT: return False
    elif abs(hh)>1e-6: return False
    if row['hist_dir']!=fp['hist_dir']: return False
    if abs(sf(row['vol_ratio'])-fp['vol_ratio'])>TOL_VOL_RATIO: return False
    if abs(sf(row['rsi'])-fp['rsi'])>TOL_RSI: return False
    if row['regime']!=fp['regime']: return False
    if row['bb_zone']!=fp['bb_zone']: return False
    if abs(sf(row['rolling_gain'])-fp['gain_pct'])>TOL_GAIN_PCT: return False
    return True

def eval_outcome(df,idx):
    if idx+FORWARD_BARS>=len(df): return None
    return 'UP' if df.iloc[idx+FORWARD_BARS]['c']>df.iloc[idx]['c'] else 'DOWN'

def calc_modifier(matches,up,down):
    if matches==0: return TRAIL_STANDARD,'NONE',0.0
    up_pct=up/matches*100
    conf='HIGH' if matches>=MIN_MATCHES else ('MED' if matches==2 else 'LOW')
    cm=1.0 if conf=='HIGH' else 0.5
    if up_pct>=UP_THRESHOLD*100: mod=TRAIL_STANDARD+(TRAIL_LOOSEN-TRAIL_STANDARD)*cm
    elif (100-up_pct)>=DOWN_THRESHOLD*100: mod=TRAIL_STANDARD-(TRAIL_STANDARD-TRAIL_TIGHTEN)*cm
    else: mod=TRAIL_STANDARD
    return round(mod,3),conf,round(up_pct,1)

def fetch_history(exchange,rl_last,symbol,days):
    since=exchange.parse8601((datetime.now(timezone.utc)-timedelta(days=days+5)).strftime('%Y-%m-%dT%H:%M:%SZ'))
    try:
        gap=1.5-(time.time()-rl_last[0])
        if gap>0: time.sleep(gap)
        rl_last[0]=time.time()
        all_bars=[]; fs=since
        while True:
            raw=exchange.fetch_ohlcv(symbol,'1h',since=fs,limit=720)
            if not raw: break
            all_bars.extend(raw)
            if len(raw)<720: break
            fs=raw[-1][0]+3600000; time.sleep(1.5)
        if len(all_bars)<60: return None
        df=pd.DataFrame(all_bars,columns=['ts','o','h','l','c','v'])
        df['dt']=pd.to_datetime(df['ts'],unit='ms',utc=True)
        df=df.set_index('dt').sort_index().drop_duplicates()
        cutoff=pd.Timestamp.now(tz='UTC')-pd.Timedelta(days=days)
        return df[df.index>=cutoff]
    except Exception as e:
        log(f"  {symbol} fetch error: {e}"); return None

def analyse_asset(exchange,rl_last,symbol,fp):
    log(f"  Analysing {symbol}...")
    df=fetch_history(exchange,rl_last,symbol,LOOKBACK_DAYS)
    if df is None or len(df)<100:
        log(f"  {symbol}: extending to 3yr...")
        df=fetch_history(exchange,rl_last,symbol,EXTENDED_DAYS)
    if df is None or len(df)<60:
        log(f"  {symbol}: no history")
        return {'matches':0,'up_pct':0,'confidence':'NONE','trail_modifier':TRAIL_STANDARD,
                'last_updated':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),'bars_analysed':0}
    df=compute_indicators(df); bars=len(df)
    log(f"  {symbol}: {bars} bars")
    if fp is None:
        return {'matches':0,'up_pct':50.0,'confidence':'NONE','trail_modifier':TRAIL_STANDARD,
                'last_updated':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),'bars_analysed':bars}
    sdf=df.iloc[:-FORWARD_BARS]
    matches=up=down=0; dates=[]
    for i in range(len(sdf)):
        if is_similar(fp,sdf.iloc[i]):
            o=eval_outcome(df,i)
            if o is None: continue
            matches+=1
            if o=='UP': up+=1
            else: down+=1
            dates.append(str(sdf.index[i])[:10])
    mod,conf,up_pct=calc_modifier(matches,up,down)
    log(f"  {symbol}: {matches} matches | {up_pct:.0f}% up | {conf} | modifier={mod}")
    return {'matches':matches,'up_pct':up_pct,'up_count':up,'down_count':down,
            'confidence':conf,'trail_modifier':mod,
            'last_updated':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
            'bars_analysed':bars,'match_dates':dates[-5:]}

def load_fingerprints():
    sf_path=os.path.join(BASE_DIR,'kraken_v8_1_state.json')
    if not os.path.exists(sf_path): return {}
    try:
        with open(sf_path) as f: s=json.load(f)
        fps={}
        for sym,pos in s.get('positions',{}).items():
            fps[sym]={'hist':sf(pos.get('curr_hist',0)),
                      'hist_dir':'rising' if sf(pos.get('curr_hist',0))>0 else 'falling',
                      'gain_pct':0.0,'vol_ratio':sf(pos.get('vol_ratio',1.0)),
                      'rsi':50.0,'regime':pos.get('regime','NEUTRAL'),'bb_zone':'MID'}
        return fps
    except Exception as e:
        log(f"fingerprint load error: {e}"); return {}

def load_universe():
    wf=os.path.join(BASE_DIR,'watchlist.json')
    if os.path.exists(wf):
        try:
            with open(wf) as f: d=json.load(f)
            u=list(set(d.get('watchlist',[])+d.get('cold_universe',[])+d.get('positions',[])))
            if u: return u
        except: pass
    return ['BTC/USD','ETH/USD','SOL/USD','XRP/USD','TAO/USD',
            'DOGE/USD','HYPE/USD','SUI/USD','ADA/USD','ZEC/USD']

def run():
    log("="*50); log("Pattern engine starting...")
    exchange=ccxt.kraken({'enableRateLimit':False}); rl_last=[0.0]
    universe=load_universe(); fps=load_fingerprints()
    log(f"Universe: {len(universe)} | Positions: {list(fps.keys())}")
    results={}
    for sym in universe:
        try: results[sym]=analyse_asset(exchange,rl_last,sym,fps.get(sym))
        except Exception as e:
            log(f"  {sym} error: {e}")
            results[sym]={'matches':0,'up_pct':50.0,'confidence':'NONE',
                          'trail_modifier':TRAIL_STANDARD,
                          'last_updated':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S'),
                          'error':str(e)}
    tmp=OUTPUT_FILE+'.tmp'
    with open(tmp,'w') as f: json.dump(results,f,indent=2)
    os.replace(tmp,OUTPUT_FILE)
    log(f"Written → {OUTPUT_FILE}"); log("Pattern engine complete."); log("="*50)

if __name__=='__main__':
    run()
