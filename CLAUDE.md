# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a cryptocurrency trading bot ecosystem for the Kraken exchange. It contains multiple bot versions (v1–v7) representing an evolution from simple to sophisticated strategies. Two bots are currently live on AWS EC2 (eu-west-1 Ireland) in **tmux sessions**:
- **`btc_trader.py`** — running in tmux session `pitch_deck` (BTC single-asset, pitch deck audit trail)
- **`kraken_v7.py`** — running in tmux session `kraken_v7` (multi-asset scanner, $100K virtual)

All bots operate in **shadow/paper trading mode** — virtual capital, no real money — to build a 90-day auditable track record for a pitch deck.

## Environment Setup

```bash
python3 -m venv venv
source venv/bin/activate
pip install ccxt pandas python-dotenv requests aiohttp pandas-ta numpy peewee yfinance pytz websockets
```

Set API credentials via environment variables or a `.env` file:
```
KRAKEN_API_KEY=your_key_here
KRAKEN_API_SECRET=your_secret_here
```

Required Kraken API permissions: Query Funds, Query Open/Closed Orders & Trades, Create & Modify Orders, Cancel Orders. **Withdrawals must remain disabled.**

## Running Bots

```bash
# Local development
python kraken_v3_5.py      # Shadow bot: Bollinger Band mean-reversion, BTC/USD 1h
python kraken_v6.py        # Multi-asset scanner with conviction scoring
python kraken_v4_2.py      # Modular conductor: regime + BB signal + trail + book gate
python trader.py           # Investigator chain: multi-module momentum manager
python hunterkiller.py     # Whale hunter: volume-spike momentum scalper

# Verify API connectivity
python verify_kraken.py
```

## Production (AWS EC2 — tmux sessions)

Both production bots run in **tmux**, not systemd.

```bash
# Attach to a running session
tmux attach -t pitch_deck     # btc_trader.py
tmux attach -t kraken_v7      # kraken_v7.py

# Start a session if it doesn't exist
tmux new -s kraken_v7
python3 kraken_v7.py

# Watch audit trails
tail -f btc_trader_audit_trail.csv
tail -f kraken_v7_audit_trail.csv
```

There is no automated test suite. Verification is manual via `verify_kraken.py`, log inspection, and audit trail CSV review.

## Architecture

### Bot Versions & Their Role

| File | Strategy | Status |
|---|---|---|
| `btc_trader.py` | BTC single-asset, pitch deck audit trail (tmux: `pitch_deck`) | **PRODUCTION** |
| `kraken_v7.py` | Multi-asset scanner, 3-thread architecture, $100K virtual (tmux: `kraken_v7`) | **PRODUCTION** |
| `kraken_v6.py` | Multi-asset scanner, single-threaded Conductor, $100K virtual | Superseded by v7 |
| `pitch_deck/kraken_v3_5.py` | Bollinger Band mean-reversion, BTC/USD 1h, $2K virtual | Reference/Archive |
| `kraken_v4_2.py` | Modular conductor: RegimeEngine + SignalEngine + TrailEngine + OrderBookEngine | Reference |
| `trader.py` | Manager orchestrating Investigator → SuperSleuth → Buyer/Seller chain | Multi-module |
| `hunterkiller.py` + variants | Whale/volume-spike scalper, 1m candles, 3am–12pm PST | Scalper |
| `SolaScript_v4_Unicorn.py` | Day/Night dual-personality engine (latest) | Scalper |
| `SolaScript*.py` | Earlier momentum scalper iterations (v1–v3, Hybrid) | Scalper/Archive |

### V7 Architecture (Production Multi-Asset Bot)

Three decoupled threads running concurrently:

```
Thread 1 · EXIT LOOP (every 20 seconds)
  · Fetches live ticker for every open position
  · Evaluates stop ladder: hard stop → break-even floor → trailing stop → MACD exit → regime flip → 12h time stop
  · Writes audit CSV row on every exit
  · Registers closed symbol in CooldownRegistry (2h lockout)

Thread 2 · ENTRY SCAN (every 5 minutes)
  · Scores universe symbols via 1h OHLCV signal stack
  · PINK_1 (single shrinking MACD bar) explicitly rejected — only PINK (2+ bars) allowed
  · Blocks entry on any symbol in CooldownRegistry
  · Deducts cash and registers position atomically under lock

Thread 3 · HEARTBEAT (every 5 minutes, offset 30s)
  · Prints portfolio summary to stdout
  · Saves state.json atomically
  · Checks 15% portfolio kill-switch
```

State/output files: `kraken_v7_state.json`, `kraken_v7_audit_trail.csv`, `kraken_v7_events.log`

### btc_trader.py Architecture (Production Pitch Deck Bot)

BTC single-asset mean-reversion on 1h candles, $2,000 virtual capital. Lives only on the server at `/home/ubuntu/kraken/pitch_deck/btc_trader.py` — not present in the local repo.

**Entry signals (any one qualifies):**
- `BB_LOWER` — close at Bollinger Band lower band
- `EMA21_PULLBACK` / `EMA21_PULLBACK_IDLE` — pullback to EMA21 (0–0.75%)
- `SMA20_TOUCH` / `SMA20_TOUCH_IDLE` — price touches SMA20
- `EMA21_CROSS` — EMA21 crossover

**Exit reasons:**
- `SELL_TARGET` — take-profit target reached
- `SELL_TRAIL` — trailing stop hit (2.8% in NEUTRAL regime, ATR-based)
- `SELL_REGIME_FLIP` — regime changes (e.g. NEUTRAL → BEAR)

**Key parameters:** `neutral_trail=2.8%`, `bear_confirm=2 bars`, `atr_filter=ON`

**Server paths:**
- Script: `/home/ubuntu/kraken/pitch_deck/btc_trader.py`
- Cached OHLCV: `/home/ubuntu/kraken/pitch_deck/data/btc_1h.csv`
- Backtest outputs: `backtest_trades.csv`, `backtest_equity_curve.csv`, `backtest_summary.json`

### Standard Bot Execution Loop (single-threaded pattern)

Every single-threaded bot version follows this general pattern:
1. Wake at hour (or N-minute) boundary
2. Fetch OHLCV candles; evaluate **penultimate candle (`iloc[-2]`)** — never the last — to prevent look-ahead bias
3. Calculate indicators (Bollinger Bands, MACD, RSI, ATR, etc.)
4. Generate signal: BUY / SELL_TARGET / SELL_STOP / SELL_TRAIL / HOLD
5. Apply **10bps slippage model**: `buy_exec = signal_price * 1.001`, `sell_exec = signal_price * 0.999`
6. Update state JSON atomically
7. Append one row to the CSV audit trail
8. Check 15% account-wide kill-switch (terminates bot if breached)
9. Sleep to next boundary

### V4.2 Engine Architecture (Modular Pattern)

```
RateLimiter (1.5s spacing, exponential backoff)
  └─ StateManager (atomic state.json, CSV audit, events.log)
       ├─ RegimeEngine (EMA/ADX → BULL/SIDEWAYS/BEAR, hourly)
       ├─ SignalEngine (Bollinger Band entry/exit, hourly)
       ├─ TrailEngine (analog fingerprinting + tiered stops, every 5 min)
       └─ OrderBookEngine (4-check gate: imbalance/walls/depth/tape, every 5 min)
```

External modules: `order_book_v4_2.py`, `trail_engine_v4_2.py`

### V6 Engine Architecture (Multi-Asset)

```
Conductor (5-min heartbeat)
  ├─ UniverseManager: top-30 by 24h volume (≥$5M), refreshed hourly
  ├─ ScanEngine: MACD + RSI + BB + EMA21 + SMA20 + order book → conviction 0–100
  ├─ ConvictionEngine: score → position size (6% / 12% / 20% of capital)
  ├─ TrailEngine: ATR-calibrated adaptive stop (tightens as profit grows)
  ├─ OrderBookEngine: 4-check gate (VETO blocks entry/exit)
  └─ PortfolioManager: max 5 positions, 40% dry-powder reserve, 15% kill-switch
```

### Shared Core Modules

- **`botlib.py`** (in `C:/users/matti/desktop/crypto_bots/`) — Central utility: `read_state()` / `write_state_atomic()` (write to `.tmp` then `os.replace()`), `append_outbox()` / `drain_outbox()` (JSONL message queue), `clamp_qty_to_precision()`, `get_sold_until()` / `set_sold_until()` (cooldown registry). Used by newer bots.
- **`database.py`** — SQLite via raw `sqlite3`. Tables: `positions`, `orders`, `watchlist`, `logs`. Used by trader.py chain.
- **`executor.py`** — CCXT Kraken connector. `execute_buy(symbol, usd_amount)` / `execute_sell(symbol, reason)`. Min trade: $12.
- **`investigator.py`** — Binary run-length encoding on candle streaks (green/red). Requires ≥10 flips, ≥3 candles per run. Feeds watchlist. Output stored in `sine_signals.db`.
- **`super_sleuth.py`** — Async multi-timeframe scorer (1m→1M): trend (EMA 20/50/200 + ADX>25=+2), bullish engulfing+vol (+3), BB rejection+RSI<30 (+4), BB squeeze+ATR rising (+5), VWAP cross (+2). Output stored in `super_sleuth.db`.
- **`buyer.py` / `seller.py`** — Thin command wrappers used by trader.py; `Buyer.strike()` / `Seller.liquidate()`.
- **`kraken_macro_aggregator_v6.py`** (`Crypto Project/`) — V-Sentry Intelligence Aggregator. Polls bid/ask spread, buy-pressure ratio (level-20 order book), BB width, RSI(14) every 30s for a whitelisted set of pairs. Stores to `v_sentry_intelligence.db`.

### State & Output Files

| File | Purpose |
|---|---|
| `kraken_v7_state.json` | V7 multi-asset positions, universe, capital (atomic writes) |
| `kraken_v7_audit_trail.csv` | V7 audit trail (one row per exit) |
| `kraken_v7_events.log` | V7 rate limit events, API errors, signal evaluations |
| `kraken_v3_5_state.json` | V3.5 position, peak price, trade count |
| `kraken_v6_state.json` | V6 multi-asset positions, universe, capital |
| `kraken_auditable_shadow_bot_v3_5_audit_trail.csv` | V3.5 audit trail (one row/hour) |
| `trading.db` | SQLite for trader.py chain (positions, orders, watchlist, logs) |
| `whale_hunter.db` | SQLite for hunterkiller.py |
| `super_sleuth.db` | Multi-timeframe signal scores from super_sleuth.py |
| `sine_signals.db` | Verified candle-streak patterns from investigator.py |
| `v_sentry_intelligence.db` | Order-book pressure + BB + RSI from macro aggregator |
| `trade_state.json` | SolaScript live state |

### SolaScript v4 Unicorn (Dual-Personality Scalper)

Switches mode based on time of day:

| Phase | Hours (PST) | Nickname | Positions | Size | Entry Gate |
|---|---|---|---|---|---|
| Phase 1 | 03:00–12:00 | Unicorn Hunter | 8 | $7 | RVOL > 3.0x + BB squeeze |
| Phase 2 | 12:00–03:00 | Deep Researcher | 6 | $10 | 4H trend UP + 5m RSI < 30 |

Key exit logic: once +10% gain, tighten trail to 1% below current. Once +1.5% profit, switches from loose to tight trailing stop. Uses async/await + TokenBucket rate limiting for high-concurrency scanning.

### HunterKiller Variants

- **`hunterkiller.py`** — Core whale hunter (RVOL > 2.0x, 03:00–12:00 PST, audition rules: +0.6% in 60s / +1.2% in 120s or dump)
- **`hunterkiller_gainers.py`** — Focuses entry on top 24h gainers list
- **`hunterkiller_winner.py`** — Focuses on current session winners
- **`hunterkiller_winner_anytime.py`** — 24/7 version without PST time gate

All variants share the same stall detection (120s no new high + profit < 2% → exit) and use `whale_hunter.db`.

### Crypto Project Subfolder

`Crypto Project/` contains the backtesting and signal infrastructure:
- **`kraken_signal_backtester_v2_1.py`** — Historical signal validation; outputs Sharpe, Profit Factor, equity curve
- **`kraken_signal_logger_v3_2.py`** — Audit trail logger (the reference implementation for timestamp/equity/drawdown/signal_price/exec_price/delay_ms)
- **`kraken_macro_aggregator_v6.py`** — V-Sentry order-book intelligence (30s poll)
- **`kraken_macro_hunter_v7.py`** — Macro-level asset hunter
- **`crypto_trader_v5.py`** — Earlier all-in-one trading engine

## Mandatory Audit Trail Standard

Every production bot row **must** contain these exact columns:
```
timestamp, equity, drawdown, signal_price, exec_price, delay_ms
```
- `timestamp`: UTC, microsecond precision
- `equity`: portfolio value marked to live ticker
- `drawdown`: rolling max drawdown from starting equity
- `signal_price`: raw price from indicator at candle close
- `exec_price`: modeled price including 10bps slippage
- `delay_ms`: milliseconds between candle close timestamp and bot evaluation

## Risk Management Constants (Shared Across Versions)

- **Slippage model:** 10 basis points (0.10%) on every entry and exit
- **Hard stop per position:** 3.5% from entry
- **Trailing stop:** 2.0% from peak (tightens with ATR in v6)
- **Account kill-switch:** 15% rolling max drawdown → bot terminates
- **Look-ahead prevention:** always evaluate `iloc[-2]`, never `iloc[-1]`

## V7 Signal Stack

**Entry (all must pass):**
1. MACD histogram: PINK state (≥2 consecutive shrinking negative bars) — PINK_1 blocked
2. RSI(14) < 52
3. Price at BB lower band OR EMA21 pullback (0–0.75%) OR SMA20 touch OR RSI < 42 above EMA55
4. Order book verdict ≠ VETO
5. Asset regime ≠ BEAR
6. Conviction score ≥ 40
7. Symbol NOT in CooldownRegistry

**Exit ladder (first trigger wins):**
- A. Hard stop: entry × (1 − 3.5%)
- B. Break-even: once +0.8%, stop floor = entry price
- C. Trailing stop: 1h-ATR anchored, armed at +0.8%, tiers 1.0→0.7→0.4→0.2×ATR
- D. MACD LIGHT_GREEN (2+ shrinking positive bars) → collapse trail to 0.2×ATR
- E. Regime flips BEAR on held asset
- F. Time stop: 12h maximum hold

## Key Infrastructure Notes

- **Exchange access:** always via CCXT library (`ccxt.kraken()` or `ccxt.krakenpro()`)
- **Rate limiting:** all API calls gated through a shared RateLimiter (1.5s min spacing, exponential backoff on 429/520)
- **Atomicity:** state JSON is always written to a `.tmp` file then renamed — never written in-place
- **Timezone:** logs in UTC; trading window logic in `America/Los_Angeles`; server timezone set to PST
- **EC2 protections:** stop protection + termination protection must remain enabled on the production instance
