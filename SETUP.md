# Kraken Trading Bot — Setup Guide

## Requirements
- Python 3.11+
- VS Code (or any terminal)
- Kraken account with API key (Trade permissions enabled)

---

## Step 1 — Install dependencies
```bash
pip install -r requirements.txt
```

---

## Step 2 — Set your Kraken API keys

**Option A (recommended): Environment variables**
```bash
# Mac/Linux terminal:
export KRAKEN_API_KEY="your_key_here"
export KRAKEN_API_SECRET="your_secret_here"

# Windows PowerShell:
$env:KRAKEN_API_KEY="your_key_here"
$env:KRAKEN_API_SECRET="your_secret_here"
```

**Option B:** Edit bot.py lines 28–29 directly (less secure):
```python
API_KEY    = "your_key_here"
API_SECRET = "your_secret_here"
```

---

## Step 3 — Kraken API Key permissions needed
When creating your key on Kraken:
✅ Query Funds
✅ Query Open Orders & Trades
✅ Create & Modify Orders
✅ Cancel/Close Orders
❌ DO NOT enable: Withdrawals

---

## Step 4 — Run the bot
```bash
python bot.py
```

To stop: `Ctrl+C` — the bot will close all open positions cleanly before exiting.

---

## Active trading hours (PST)
The bot only opens NEW positions during high-volatility windows:
- **01:00–04:00 PST** — London open (good crypto moves)
- **06:00–12:00 PST** — NY open overlap (highest volume)
- **17:00–22:00 PST** — Asia pre-session

It will still MANAGE existing positions (trailing stop, take profit) 24/7.

---

## Output files
- `bot.log` — Full trading log (created in same directory)
- `trade_log.json` — All completed trades with PnL

---

## Strategy overview
**Multi-signal momentum + mean reversion hybrid**
- MACD crossover detection (weight: 2)
- RSI oversold/overbought (weight: 1.5)
- Stochastic RSI confirmation (weight: 1.5)
- EMA trend alignment 9/21/55 (weight: 1)
- Bollinger Band position + squeeze (weight: 1.5)
- Volume oscillator confirmation (weight: 1)
- ATR volatility filter (dampens signals in extremes)
- Candle body strength (weight: 0.5)

**Enters only when combined score ≥ 2.5**
**Exits on: trailing stop loss (2.5%) or take profit (5.5%)**
**Max 3 concurrent positions**
**Halts if 30% drawdown hit**
