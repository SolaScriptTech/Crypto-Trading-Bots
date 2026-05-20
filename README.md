# Prop Bot System — Setup & Deployment Guide

Multi-strategy trading bot system designed to pass a prop firm evaluation:
- **Profit target**: 3–7% over 90 days (bot aims for 5%, locks down at 6%)
- **Max drawdown**: 15% hard limit (bot halts at 12% as safety buffer)

---

## File Structure

```
prop_bot_system/
├── config.py            ← All parameters (edit this, not the bot)
├── risk_manager.py      ← Prop firm risk controls
├── prop_bot_system.py   ← Main bot (run this)
├── backtest.py          ← Offline validator
└── README.md
```

---

## Quick Start

### 1. Install dependencies
```bash
pip install ccxt[pro] pandas pandas_ta
```

### 2. Set Kraken API keys
```bash
export KRAKEN_API_KEY="your_key"
export KRAKEN_SECRET="your_secret"
```
Or add directly to `prop_bot_system.py` in the `ccxtpro.kraken({...})` call:
```python
self.exchange = ccxtpro.kraken({
    "apiKey":  "your_key",
    "secret":  "your_secret",
    "enableRateLimit": True,
})
```

### 3. Backtest first (always)
```bash
python3 backtest.py --days 90 --equity 10000
```
Check that output shows ✅ PASS on both metrics before going live.

### 4. Deploy in tmux
```bash
tmux new -s prop_bot
python3 prop_bot_system.py
```
Detach: `Ctrl+B D` | Re-attach: `tmux attach -t prop_bot`

---

## Monitoring

```bash
# Live log
tail -f prop_events.log

# Trade history
cat prop_audit.csv

# Current state
python3 -c "import json; d=json.load(open('prop_state.json')); print(json.dumps(d['risk_status'], indent=2))"
```

---

## Emergency Stop

```bash
touch EMERGENCY_STOP
```
Bot detects the file on the next loop, closes all positions, and exits cleanly.
Remove the file before restarting: `rm EMERGENCY_STOP`

---

## Strategies

| Bot | Signal | Regime | Entry Gate |
|-----|--------|--------|-----------|
| TREND_FOLLOW | MACD slow crossover (12,26,9) | BULL + NEUTRAL | ADX ≥ 25, vol ≥ 1.5× |
| TREND_FOLLOW | MACD fast crossover (5,10,16) | BULL + NEUTRAL | ADX ≥ 25, histogram ≥ 0.0002 |
| MOMENTUM | Zero-line cross (12,26,90) | BULL only | ADX ≥ 22 |
| MEAN_REVERSION | BB lower band touch | NEUTRAL only | ADX < 28, RSI < 40 |
| BEAR_SHORT | MACD bear crossover | BEAR only | RSI ≥ 40 |
| BEAR_SHORT | BB upper rejection | BEAR only | RSI ≥ 60 |

---

## Risk Architecture

```
Per trade:    never risk > 0.8% of account
Hard stop:    3% loss on longs, 1.5% on shorts  
Daily loss:   halt at 1.5% daily loss (resumes next UTC day)
Drawdown:     halt at 12% total DD (buffer before 15% prop limit)
Profit lock:  at 6% profit, all position sizes cut 50%
Dry powder:   always keep 20% in cash
Max positions: 4 concurrent
Cooldown:     3h per pair after any exit
```

---

## Prop Firm Parameters Explained

The 90-day evaluation window requires **consistency**, not aggression:

- **Why 5% target, not 7%?**  
  Aiming for 7% creates pressure to overtrade in unfavorable conditions.
  5% is achievable at ~0.055%/day with only 3–4 trades per week.

- **Why halt at 12% DD, not 15%?**  
  A 3% buffer absorbs slippage, spread, and overnight gaps so the bot
  never accidentally triggers the prop firm's hard limit.

- **Why the profit lock at 6%?**  
  Once you're in profit, capital preservation becomes the primary goal.
  Half sizing means the bot can still trade but can't blow the account
  on a late-stage losing streak.

---

## Tuning After Backtest

If backtest shows PnL too low (< 3%):
- Lower `MIN_CONVICTION` from 62 → 58
- Lower `ADX_MIN_TREND` from 25 → 22
- Lower `VOL_RATIO_MIN` from 1.5 → 1.3

If backtest shows drawdown too high (> 12%):
- Raise `HARD_STOP_PCT` from 3% → 2.5%
- Lower `SIZE_HIGH_PCT` from 20% → 15%
- Raise `MIN_CONVICTION` from 62 → 65
