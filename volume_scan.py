"""Rank all 43 coins by recent trading volume (last 4h of 15m candles)."""
import time
from candles import get_candles, SYMBOL_MAP

results = []
symbols = list(SYMBOL_MAP.keys())
print(f"Scanning {len(symbols)} coins for volume...\n")

for sym in symbols:
    try:
        candles = get_candles(sym, "15m", 48)  # 48 x 15m = 12 hours
        if not candles or len(candles) < 4:
            continue
        recent = candles[-16:]   # last 4 hours (16 x 15m)
        full   = candles[-48:]   # last 12 hours
        vol_4h  = sum(c["volume"] for c in recent)
        vol_12h = sum(c["volume"] for c in full)
        last_price = candles[-1]["close"]
        notional_4h = vol_4h * last_price
        notional_12h = vol_12h * last_price
        # Volume acceleration: 4h vs prior 4h
        prior = candles[-32:-16] if len(candles) >= 32 else candles[:16]
        vol_prior = sum(c["volume"] for c in prior)
        accel = (vol_4h / vol_prior) if vol_prior > 0 else 1.0
        results.append({
            "symbol": sym,
            "price": last_price,
            "vol_4h": vol_4h,
            "notional_4h": notional_4h,
            "notional_12h": notional_12h,
            "accel": accel,
        })
        print(f"  {sym:<14} price={last_price:<12} notional_4h=${notional_4h:>14,.0f}  accel={accel:.2f}x")
        time.sleep(0.25)
    except Exception as e:
        print(f"  {sym}: ERROR {e}")

print("\n" + "="*70)
print("TOP 10 BY 4H NOTIONAL VOLUME")
print("="*70)
results.sort(key=lambda x: x["notional_4h"], reverse=True)
print(f"{'#':<3} {'Symbol':<14} {'Price':>12} {'4h Vol ($)':>16} {'12h Vol ($)':>16} {'Accel':>7}")
print("-"*70)
for i, r in enumerate(results[:10], 1):
    flag = " <-- SURGING" if r["accel"] >= 1.5 else ""
    print(f"{i:<3} {r['symbol']:<14} {r['price']:>12.6g} ${r['notional_4h']:>15,.0f} ${r['notional_12h']:>15,.0f} {r['accel']:>6.2f}x{flag}")

print("\nTOP 10 BY VOLUME ACCELERATION (last 4h vs prior 4h)")
print("="*70)
results.sort(key=lambda x: x["accel"], reverse=True)
print(f"{'#':<3} {'Symbol':<14} {'Price':>12} {'4h Vol ($)':>16} {'Accel':>7}")
print("-"*70)
for i, r in enumerate(results[:10], 1):
    print(f"{i:<3} {r['symbol']:<14} {r['price']:>12.6g} ${r['notional_4h']:>15,.0f} {r['accel']:>6.2f}x")
