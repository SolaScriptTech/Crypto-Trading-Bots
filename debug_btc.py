from candles import get_candles
from bot2 import analyze, build_recommendation_combined
from datetime import datetime, timezone

c15 = get_candles('BTC/USD', '15m', 720)
c5  = get_candles('BTC/USD', '5m',  720)

for hour in [13, 14, 15, 16, 17, 18]:
    target_ts = int(datetime(2026, 5, 18, hour, 0, 0, tzinfo=timezone.utc).timestamp())
    snap15 = [c for c in c15 if c['time'] < target_ts]
    snap5  = [c for c in c5  if c['time'] < target_ts]
    if len(snap15) < 50 or len(snap5) < 50:
        continue

    a15 = analyze(snap15, '15m')
    a5  = analyze(snap5,  '5m')

    recs15 = build_recommendation_combined(a15, 'both')
    recs5  = build_recommendation_combined(a5,  'both')

    price = snap15[-1]['close']
    w15 = a15['wave_count_before_last_choc']['wave_count']
    ws15 = a15['wave_count_since_last_choc']['wave_count']
    w5  = a5['wave_count_before_last_choc']['wave_count']
    ws5 = a5['wave_count_since_last_choc']['wave_count']

    fired = False
    for r_h, r_l in zip(recs15, recs5):
        if r_h.get('verdict') == 'TRADE' and r_l.get('verdict') == 'TRADE' and r_h.get('side') == r_l.get('side'):
            fired = True
            print(f"\n{'='*60}")
            print(f"SIGNAL at {hour:02d}:00 UTC  price={price}")
            print(f"  side={r_h['side']}  entry={r_h.get('entry')}  SL={r_h.get('sl')}  TP={r_h.get('tp')}")
            print(f"  waves_before_choc 15m={w15}  since={ws15}")
            print(f"  waves_before_choc  5m={w5}   since={ws5}")
            break

    if not fired:
        reject15 = [str(r) for rec in recs15 for r in rec.get('reasons_against', [])]
        reject5  = [str(r) for rec in recs5  for r in rec.get('reasons_against', [])]
        all_r    = list(dict.fromkeys(reject15 + reject5))[:3]
        print(f"{hour:02d}:00 UTC  price={price}  trend15={a15['trend']}  waves_before={w15}  waves_since={ws15}  NO TRADE: {' | '.join(all_r)}")
