import ccxt, pandas as pd, pandas_ta as ta

QUOTE="USD"
MIN_5M_UP_PCT=0.6
MAX_5M_UP_PCT=7.0
VOL_SPIKE_MULT=2.2
SPREAD_MAX_PCT=0.55
BREAKOUT_LOOKBACK=20

def pct(a,b):
    return 0.0 if b==0 else (a/b-1)*100.0

def clamp(x,lo,hi):
    return max(lo,min(hi,x))

def is_stable(a):
    return a.upper() in {"USD","USDT","USDC","DAI","TUSD","FDUSD","USDP"}

def compute(df):
    close=df["close"].astype(float)
    r1=pct(close.iloc[-1],close.iloc[-2])
    r5=pct(close.iloc[-1],close.iloc[-6]) if len(close)>=6 else 0.0
    v=df["volume"].astype(float)
    base=v.iloc[-21:-1] if len(v)>=21 else v.iloc[:-1]
    v_med=float(base.median()) if len(base) else 0.0
    v_last=float(v.iloc[-1])
    vol_mult=(v_last/v_med) if v_med>0 else 0.0

    ema9=ta.ema(close, length=9)
    ema21=ta.ema(close, length=21)
    ema_trend=1.0 if (ema9 is not None and ema21 is not None and float(ema9.iloc[-1])>float(ema21.iloc[-1])) else 0.0

    macd=ta.macd(close, fast=12, slow=26, signal=9)
    hist_now=hist_prev=hist_prev2=0.0
    if macd is not None and not macd.empty:
        col=[c for c in macd.columns if "MACDh" in c]
        if col:
            h=macd[col[0]].fillna(0.0).astype(float).tolist()
            if len(h)>=3: hist_prev2,hist_prev,hist_now=h[-3],h[-2],h[-1]
    hist_rising=1.0 if (hist_now>hist_prev and hist_prev>hist_prev2) else 0.0
    hist_cross=1.0 if (hist_prev<=0.0 and hist_now>0.0) else 0.0

    lb=min(BREAKOUT_LOOKBACK, len(df)-2)
    breakout=0.0
    if lb>=8:
        prior_high=float(df["high"].iloc[-(lb+2):-2].max())
        breakout=1.0 if float(close.iloc[-1])>=prior_high else 0.0

    rsi=ta.rsi(close, length=14)
    rsi_now=float(rsi.iloc[-1]) if rsi is not None and not rsi.empty else 50.0

    s_r5=clamp((r5-MIN_5M_UP_PCT)/(MAX_5M_UP_PCT-MIN_5M_UP_PCT),0.0,1.0)
    s_vm=clamp((vol_mult-VOL_SPIKE_MULT)/6.0,0.0,1.0)
    s_r1=clamp((r1+0.2)/1.7,0.0,1.0)
    hot_pen=clamp((rsi_now-80.0)/10.0,0.0,1.0)

    score=0.0
    score += 2.4*breakout
    score += 2.2*s_vm
    score += 1.7*s_r5
    score += 1.0*s_r1
    score += 0.9*ema_trend
    score += 1.1*hist_rising
    score += 0.8*hist_cross
    score -= 2.0*hot_pen

    return {"r1":r1,"r5":r5,"vol_mult":vol_mult,"breakout":breakout,"hist_now":hist_now,"rsi":rsi_now,"score":score}

ex=ccxt.kraken({"enableRateLimit": True})
ex.load_markets()

universe=[]
for sym,m in ex.markets.items():
    if not m.get("active", True): 
        continue
    if m.get("spot") is False:
        continue
    if (m.get("quote") or "").upper()!=QUOTE:
        continue
    base=(m.get("base") or "").upper()
    if not base or is_stable(base):
        continue
    universe.append(sym)

print("universe:", len(universe))

tickers=ex.fetch_tickers()
rows=[]
for s in universe:
    t=tickers.get(s) or {}
    qv=float(t.get("quoteVolume") or 0.0)
    pct24=float(t.get("percentage") or 0.0)
    if qv>0:
        rows.append((s,pct24,qv))
rows.sort(key=lambda x: x[1], reverse=True)
cands=[s for s,_,_ in rows[:60]]
print("candidates:", len(cands))

out=[]
for s in cands[:20]:
    try:
        ob=ex.fetch_order_book(s, limit=5)
        bid=ob["bids"][0][0] if ob["bids"] else 0.0
        ask=ob["asks"][0][0] if ob["asks"] else 0.0
        if bid<=0 or ask<=0:
            continue
        mid=(bid+ask)/2.0
        spread=abs(ask-bid)/mid*100.0
        ohlcv=ex.fetch_ohlcv(s, timeframe="1m", limit=160)
        df=pd.DataFrame(ohlcv, columns=["ts","open","high","low","close","volume"])
        f=compute(df)
        out.append((s,f["score"],spread,f["r5"],f["vol_mult"],f["breakout"],f["hist_now"],f["rsi"]))
    except Exception:
        continue

out.sort(key=lambda x: x[1], reverse=True)
for row in out[:10]:
    s,score,spread,r5,vm,br,hist,rsi=row
    gates=[]
    if spread>SPREAD_MAX_PCT: gates.append("spread")
    if r5<MIN_5M_UP_PCT or r5>MAX_5M_UP_PCT: gates.append("r5")
    if vm<VOL_SPIKE_MULT: gates.append("vol")
    if score<6.0: gates.append("score")
    print(f"{s:14s} score={score:5.2f} spread={spread:4.2f}% r5={r5:5.2f}% volx={vm:4.2f} br={int(br)} hist={hist: .6f} rsi={rsi:5.1f} blocked={','.join(gates) or 'none'}")
