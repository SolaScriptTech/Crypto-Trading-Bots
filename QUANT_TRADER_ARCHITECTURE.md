# QUANT_TRADER_ARCHITECTURE.md
## Master Architecture Document — Authoritative Reference

> This document is the single source of truth for the quant_trader system.
> Every design decision, every rule, every parameter is recorded here.
> If the code does something this document doesn't describe, the code is wrong.
> If you are starting a new session, read this document first before writing anything.

---

## THE CORE PHILOSOPHY

This system is built on one insight that separates it from every retail bot:
**The market is always moving between conditions. Edge comes from sensing what
condition is forming BEFORE it arrives, positioning for it, and managing the
exit BEFORE it ends. Most bots react. This one anticipates.**

The asymmetry that makes this system structurally safe:
- **When PPM is right:** Full upside captured. Entered early, rode the move, exited near the top.
- **When PPM is wrong:** Zero capital loss. Only missed opportunity cost. Lv3 corrects and the engine resumes.

Wrong predictions cost unrealized gain. Never real money. That is the moat.

---

## THE SYSTEM ARCHITECTURE

```
┌──────────────────────────────────────────────────────────────────┐
│  Lv1 — PETER PARKER MODULE (PPM)                                 │
│  "My spidey sense is tingling."                                   │
│  Senses what market condition is FORMING on the horizon.         │
│  Reads 12 sensors. Outputs 7 pre-condition probabilities (0-1).  │
│  Runs on every candle close. Fast sensors run every loop tick.   │
├──────────────────────────────────────────────────────────────────┤
│  Lv1 — GANDALF THE WHITE MODULE (GTW)                            │
│  "A wizard is never late."                                        │
│  Receives PPM alert. Casts a strategic spell.                    │
│  Spell = complete configuration of trailing stops, signal        │
│  permissions, size multipliers, and exit behavior.               │
│  DOES NOT cause trades. Only adjusts parameters.                 │
├──────────────────────────────────────────────────────────────────┤
│  Lv2 — MCMC CLASSIFIER                                           │
│  Confirms what the market condition ACTUALLY IS right now.       │
│  Macro regime: BULL / BEAR / NEUTRAL (EMA21 vs EMA55).          │
│  Micro conditions: price structure, MACD state, volume,          │
│  session, key levels. Loads per-MCMC strategy config.            │
│  Applies GTW spell on top of base config.                        │
├──────────────────────────────────────────────────────────────────┤
│  Lv3 — SIGNAL ROUTER + CONVICTION SCORER                         │
│  Current-condition layer. Two jobs:                              │
│  Job 1: Verify whether Lv1/2 prediction is materializing.        │
│         Manage open positions accordingly.                        │
│  Job 2: Detect and score momentum signals in current condition.  │
│  Outputs: RoutingResult with approved entries + risk/reward score│
├──────────────────────────────────────────────────────────────────┤
│  RISK/REWARD TOLERANCE ENGINE                                    │
│  NOT binary gates. A ratio.                                      │
│  Risk score (everything saying don't buy) vs                     │
│  Reward score (everything saying the move is real).              │
│  Tolerance threshold set by Lv3 CONFIRMED CURRENT CONDITION.    │
│  Never by prediction. Never by PPM probability.                  │
├──────────────────────────────────────────────────────────────────┤
│  RISK MANAGEMENT ENGINE (RME)                                    │
│  Runs every loop. Manages open positions only. Never opens.      │
│  Exit ladder in strict priority order.                           │
│  Absolute floors never change regardless of any prediction.      │
├──────────────────────────────────────────────────────────────────┤
│  EXECUTION ENGINE                                                │
│  Places orders. Manages state. Writes audit trail.               │
│  Operates within whatever parameters it has been given.          │
│  Does not know or care whether params came from prediction        │
│  or Lv3 correction. Just executes within the boundaries.        │
└──────────────────────────────────────────────────────────────────┘
```

---

## LAYER 1 — PETER PARKER MODULE (PPM)

### What It Does
Senses pre-conditions BEFORE the MCMC manifests. Like the spiders on the
asphalt heading uphill before the rain — not reacting to rain, sensing
the pressure drop and humidity shift hours before.

### What It Does NOT Do
- Does not cause trades
- Does not approve entries
- Does not manage positions
- Does not know what assets are held

### The 12 Sensor Channels
Each returns a probability 0.0–1.0:

| Sensor | What It Reads | Why It's Leading |
|--------|--------------|-----------------|
| `_sense_macd_thrust` | Histogram bars shrinking progressively | Momentum fading before the cross |
| `_sense_volume_drying` | Volume declining below rolling average | Spring compressing |
| `_sense_bb_compression` | BB width at multi-period low percentile | Coil tightening |
| `_sense_atr_contraction` | ATR declining for N consecutive bars | Volatility collapsing |
| `_sense_book_depth` | Total bid/ask depth draining | Market makers retreating |
| `_sense_spread` | Bid-ask spread widening from baseline | Liquidity cost rising |
| `_sense_cvd_divergence` | CVD vs price direction | Hidden buying/selling |
| `_sense_swing_compression` | Each swing range smaller than the last | Energy coiling |
| `_sense_rsi_exhaustion` | RSI extreme + diverging from price | Momentum lying |
| `_sense_mfi_divergence` | MFI turning against price | Capital leaving quietly |
| `_sense_session_risk` | UTC clock + weekend flag | Calendar IS a sensor |
| `_sense_altseason` | Alt/BTC performance decoupling | Rotation assembling |

### The 7 Macro Pre-Condition Outputs
Synthesized from the 12 sensors:
- `consolidation_forming` (0.0–1.0)
- `fakeout_conditions` (0.0–1.0)
- `vol_spike_coiling` (0.0–1.0)
- `altseason_assembling` (0.0–1.0)
- `thin_book_approaching` (0.0–1.0)
- `extreme_greed_building` (0.0–1.0)
- `extreme_fear_building` (0.0–1.0)

### Run Frequency
- **Slow sensors** (MACD thrust, BB compression, ATR, volume, swing, RSI, MFI, altseason):
  Run every candle close on the trading timeframe. Never on unconfirmed candles.
- **Fast sensors** (book depth, spread, CVD, session clock):
  Run every loop tick. Can change between candle closes.
- **Emergency recast threshold**: Book depth ≥ 80%, Spread ≥ 80%, CVD ≥ 85%
  → Bypass confirmation counter, immediate GTW recast.

---

## LAYER 1 — GANDALF THE WHITE MODULE (GTW)

### What It Does
Receives EWSAlert from PPM. Casts a strategic spell — a complete
StrategySpell configuration object that downstream layers consume.

### What It Does NOT Do
- Does not cause trades
- Does not approve entries
- Does not manage positions

### Confirmation Counter (Timeframe-Adaptive)
GTW does not recast on a single candle. Requirements:
- **Minute candles**: 3 consecutive candles above threshold
- **Hour candles**: 2 consecutive candles above threshold
- **Day candles**: 1 candle sufficient

Fast sensors (book depth, spread, CVD) are **exempt** — they bypass the
counter and trigger immediate emergency recast if critical thresholds breached.

### The 9 Named Spells

| Spell | Condition | Posture |
|-------|-----------|---------|
| GLAMDRING | Clean trend | Full offense — all signals |
| THE_GREY_PILGRIM | Consolidation forming | BB mean rev only, tighter trails |
| CIRCLE_OF_WARDS | Fakeout trap detected | Block breakout, arm reversal |
| SHADOWFAX_SPRINT | Vol spike coiling | Arm breakout mode, wider stops |
| PALANTIR_VISION | Altseason assembling | Rotate to alts, 4 positions |
| MITHRIL_VEST | Thin book approaching | Half size, widest stops |
| SHIELD_OF_GONDOR | Extreme greed building | No longs, arm short |
| LIGHT_OF_EARENDIL | Extreme fear building | High-conviction reversal long |
| YOU_SHALL_NOT_PASS | Vol + thin book combined | Entry freeze, protect existing |

### Dual-Condition Handler
Certain combinations override single-condition spells:
- Vol spike + Thin book → YOU_SHALL_NOT_PASS (hardest freeze)
- Extreme fear + Fakeout → LIGHT_OF_EARENDIL (bear trap — maximum conviction reversal)
- Consolidation + Altseason → PALANTIR_VISION (BTC flat, alts assembling)
- Extreme greed + Vol spike → SHIELD_OF_GONDOR with force_exit_existing=True

### What A Spell Actually Changes (In Trading Terms)
- Conviction floor shifts (e.g. 62 → 72 → nothing fires below 72)
- Signal whitelist changes (MACD on/off, BB on/off)
- Position size multiplier (1.0× → 0.5× → 0.0×)
- Trailing stop tier percentages reconfigured on ALL open positions
- Entry freeze flag (size_multiplier=0.0, conviction_floor=999)
- Rotation mode flag (prioritize alt pairs over BTC)
- Tighten existing stops flag

---

## THE CRITICAL RULE: WHAT PPM/GTW CAN AND CANNOT DO

### PPM Predictions Affect EXISTING POSITIONS ONLY — with one exception.

**Normal behavior (all condition changes except Bear→Bull):**
- PPM senses condition changing
- GTW casts appropriate spell
- Spell tightens or adjusts exit parameters on open positions
- Spell may stop new entries into certain signals
- **NEW ASSET PURCHASES are governed exclusively by Lv3 confirmed current condition**
- PPM prediction does NOT authorize new entries

**The one exception — Bear or Flat transitioning to Bull:**
- This is the highest-value transition in crypto
- Missing the bottom of a bull run is the single biggest opportunity cost
- When PPM senses Bear→Bull forming, GTW is allowed to relax entry parameters
  for anticipatory long entries that Lv3 would not yet approve on its own
- If PPM is wrong: Lv3 corrects, positions managed defensively, no capital lost
  — only unrealized gain that didn't materialize. No real loss.
- This is the ONLY case where a prediction influences new entries

**Why this rule is safe:**
Wrong predictions cost only missed opportunity, never real money. The risk
is asymmetric — correct calls capture full upside, wrong calls cost zero.
The absolute risk floors (hard stops, kill switch, max DD) never change
regardless of any prediction.

---

## LAYER 2 — MCMC CLASSIFIER

### The 6 Macro Crypto Market Conditions

**MCMC-1: Sideways / Consolidation**
- ADX < 22, EMA21 ≈ EMA55, BB width < 30th percentile
- Volume below 20-period average
- Strategy: BB_MEAN_REV primary, MACD disabled, tight trails, BB midband target

**MCMC-2: Liquidity Traps / Fakeouts**
- Price breaches key level 0.3–1.5% then closes back inside
- Volume spike on breach candle, RSI divergence, book imbalance flips
- Strategy: Wait for close-back, enter reversal. Fakeout IS the signal.

**MCMC-3: Volatility Spikes**
- ATR > 2× 20-period average, BB width > 0.8%, Volume ratio > 3.0
- Strategy: No new entries. Tighten trail to 0.30%. Wait for ATR normalization.

**MCMC-4: Altseason / Whisper Markets**
- BTC ADX < 20 (flat), alts showing independent BULL, BTC dominance declining
- Strategy: Rotate capital to alts. Wider trails. Up to 4 positions.

**MCMC-5: Thin Order Books**
- Book depth < 50th percentile, spread > 2×, book imbalance oscillating
- Strategy: Size × 0.5, widen stops, consider entry freeze.

**MCMC-6: Extreme Fear / Greed**
- Greed: RSI > 78, MFI > 80, volume declining on up moves
- Fear: RSI < 25, MFI < 20, volume spike on down move
- Strategy: Greed = no new longs, arm short. Fear = high-conviction reversal long.

### Macro Regime Detection
```
BULL    = EMA21 > EMA55 AND price > EMA21 AND ADX >= 15
BEAR    = EMA21 < EMA55 AND ADX >= 15
NEUTRAL = everything else
```
**ADX threshold is 15, NOT 20.** The 20 threshold blocked BULL detection
precisely when the strategy had edge. This was a real loss in production.

**BEAR trigger asymmetry (from v5 source code analysis):**
Old versions: BULL required triple lock (BTC + ETH + ADX). BEAR fired on
ANY ONE condition (ETH OR BTC). This caused more BEAR labels than BULL
in range-bound markets. New architecture requires symmetric confirmation.

**Always use candle[-2] — the penultimate confirmed bar.**
Never act on the live unfinished candle. This prevents whipsaws on
candles that haven't closed yet.

### Micro Conditions
Five independent sub-conditions within each MCMC:
1. Price structure (UPTREND/DOWNTREND/RANGING via HH/HL or LH/LL)
2. MACD state (6 states: RISING_POSITIVE, FALLING_POSITIVE, CROSSING_UP, etc.)
3. Volume character (CONFIRMING/EXHAUSTING/CAPITULATING/ACCUMULATING/FLAT)
4. Session (LONDON_OPEN/NY_OPEN/NY_LONDON/ASIA/OFF_HOURS)
5. Key level proximity (swing high/low or round number within 0.5%)

Key level proximity adds +12 to conviction score when at a swing high/low.

---

## LAYER 3 — SIGNAL ROUTER + CONVICTION SCORER

### Lv3's Two Jobs

**Job 1 — Prediction Verification (runs on every open position every candle):**
- Compare current confirmed conditions against conditions at entry
- Output: Regime Confidence Score (0–100) per open position
- High score → hold with normal or wider trail (prediction materializing)
- Falling score → tighten trail (conditions degrading)
- Below 40 → close regardless of PnL (prediction failed)

**Job 2 — Current Momentum Signal Detection:**
- Route signals based on confirmed current MCMC and micro conditions
- Score each signal candidate for conviction
- Output approved entries with sizes to execution engine

### Signal Suite

**Long signals (6):**
- MACD_SLOW_CROSS: (12,26,9) PINK pattern, ADX ≥ 15, vol ≥ floor
- MACD_FAST_CROSS: (5,10,16) PINK pattern, magnitude ≥ 0.0002, ADX ≥ 15
- ZERO_LINE_CROSS: (12,26,90) line crosses zero, ADX ≥ 22, BULL only
- BB_MEAN_REV: price at/below lower band, ADX < 28, RSI < 40, BB width ≥ 0.004
- REVERSAL_LONG: fakeout sweep + close-back confirmed with volume
- BREAKOUT_LONG: vol spike breakout with ADX + volume confirmation

**Short signals (5):**
- MACD_BEAR_CROSS: slow histogram crosses negative, RSI ≥ 58
- MACD_BEAR_FAST: fast histogram crosses negative, RSI ≥ 62
- BB_UPPER_REJECT: price rejected at upper band, RSI ≥ 60
- REVERSAL_SHORT: fakeout high + close-back below resistance
- BREAKOUT_SHORT: vol spike breakdown with confirmation

### PINK-Only MACD Rule (NEVER VIOLATE)
```
PINK cross = current > 0 AND prev > 0 AND bar_before <= 0
```
DARK_GREEN MACD entries (trend-following with mean-reversion exit) are a
structural mismatch. Only PINK histogram crossovers are valid entries.
This was explicitly called out in the skill as a failure mode.

### RSI Floor for Shorts: 40, NOT 50
In a slow grind-down bear, RSI hovers 40–55 and never reaches 50.
The 50 floor blocks every short in exactly the conditions shorts are
designed for. This was a calibrated lesson from production.

### Conviction Scoring (0–100)
Base scores by signal type:
- ZERO_LINE_CROSS: 30
- MACD_SLOW_CROSS: 25
- REVERSAL_LONG/SHORT: 24
- BB_MEAN_REV: 22
- BREAKOUT_LONG/SHORT: 22
- MACD_FAST_CROSS: 18

Bonuses (long):
- ADX > 35: +12, ADX > 30: +8, ADX > 22: +4
- RSI ≤ 25: +10, RSI ≤ 30: +6, RSI ≤ 35: +4
- MFI ≥ 60: +7, MFI ≥ 50: +5
- Vol ratio ≥ 3.0: +8, ≥ 2.0: +5, ≥ 1.5: +2
- Session peak (London/NY): +4
- Regime aligned (asset BULL + macro BULL): +6
- Key level (SWING_LOW on long): +12
- MCMC match (BB_MEAN_REV in SIDEWAYS): +5

Short scoring is fully inverted: high RSI = good, low MFI = good.

### Conviction Floors (Context-Sensitive, NOT Universal)

| MCMC | Confirmation | Floor |
|------|-------------|-------|
| TRENDING/BULL clean | Lv3 confirmed, target identified | 68 |
| TRENDING/BULL | Standard | 72 |
| SIDEWAYS | BB_MEAN_REV with target | 70 |
| FAKEOUT | Reversal confirmed | 72 |
| VOL_SPIKE | Post-breakout | 75 |
| ALTSEASON | Alt with decoupling | 65 |
| THIN_BOOK | Highly selective | 80 |
| EXTREME_FEAR | Reversal confirmed | 72 |
| EXTREME_GREED | Short only | 68 |
| Bear→Bull anticipatory | PPM > 0.70 probability | 70 |

**Context rule:** In confirmed BULL with BB_UPPER target identified,
floor can be as low as 68. In NEUTRAL with no target, floor must be 80+.
The existence of a profit target at entry is a pre-condition for lower floors.

---

## RISK/REWARD TOLERANCE ENGINE

### The Fundamental Principle
The decision is never "is there risk?" — there is ALWAYS risk.
The decision is always: **does the expected reward justify this specific
bundle of risks at this specific moment?**

This is a ratio, not a binary gate. Every risk signal becomes an input
to the risk score. Every opportunity signal becomes an input to the reward
score. The ratio of reward/risk is compared against a tolerance threshold.

### Risk Score Inputs (higher = more risk)
- Volatility elevation (current ATR vs baseline)
- Order book health (spread, depth, imbalance)
- RSI distance from ideal entry level
- Volume confirmation weakness
- Regime classification uncertainty
- Session liquidity risk
- How late in the current condition we are

### Reward Score Inputs (higher = better opportunity)
- PPM signal strength for incoming condition
- Lv3 condition confirmation confidence
- Entry signal quality and recency
- Expected move size based on confirmed MCMC
- How early we are relative to the full move
- Asset's historical performance in this MCMC

### Tolerance Threshold
**Set by Lv3 CONFIRMED CURRENT CONDITION. Never by prediction.**

| Confirmed Current Condition | Tolerance (ratio required) |
|----------------------------|---------------------------|
| BEAR | 2.5× (very high bar — fighting headwind) |
| NEUTRAL/SIDEWAYS | 1.8× (moderate — specific setup required) |
| BULL | 1.4× (lower bar — tailwind confirmed) |
| EXTREME_FEAR (reversal) | 1.6× (elevated despite opportunity) |
| ALTSEASON | 1.5× (alts running, lower bar for alt entries) |
| THIN_BOOK | 3.0× (very high — liquidity risk) |

**Bear→Bull exception**: When PPM senses Bear→Bull and probability > 0.70,
tolerance for anticipatory entries drops to 1.6× even though Lv3 still
shows BEAR. This is the one and only case where prediction influences
entry tolerance.

---

## RISK MANAGEMENT ENGINE (RME)

### Exit Ladder — Strict Priority Order (top wins, no skipping)

1. **EMERGENCY_STOP file** — File called `EMERGENCY_STOP` on disk.
   Close everything at market. Log. Shut down. No exceptions.

2. **HARD_STOP** — Longs: 3% below entry. Shorts: 1.5% above entry.
   Absolute floor. No override from any spell or config.

3. **BEAR_REGIME_EXIT** — Macro regime flips to BEAR.
   Close ALL longs unconditionally. Not "profitable ones only."
   Not "wait for green." Every long, regardless of PnL.
   A position down 2% in BEAR gets closed. This is non-negotiable.

4. **FAILED_SIGNAL_CUT** — Position never touched profit AND
   loss exceeds pain threshold (default 1.0%, tighter than 1.5%).
   If it never went green, it was a bad read. Cut before hard stop.

5. **ZOMBIE_KILL** — Open 48h and still negative. No exceptions.

6. **STAGNATION_KILL** — Open 24h, never touched profit, still negative.

7. **BB_TARGET_HIT** — BB_MEAN_REV entries only.
   Exit when price reaches the midband. Mean reversion trades are
   precision instruments, not trend rides. Don't hold past the target.

8. **BB_UPPER_TARGET** — All entries in BULL regime.
   Exit when price reaches upper BB band. From data: 100% win rate
   on BB_UPPER exits across all backtest versions. This is the primary
   profit-taking mechanism.

9. **PROFIT_TARGET_2.5PCT** — Fixed target for non-BB entries.
   If BB_UPPER is not within range, use 2.5% fixed target.
   Exits based on defined targets had 100% win rate in backtest.
   Trail exits had 25–43% win rate. Targets first, trails second.

10. **BREAK_EVEN_FLOOR** — Once peak gain ≥ 0.3%, trail stop can
    never drop below entry × 1.001. Locks in breakeven + buffer.
    Only ever ratchets upward, never back.

11. **TIERED_TRAIL** — Gain-adaptive. Only ratchets in profit direction.

**Data-derived trail tiers (from v5 analysis):**
| Gain Level | Trail % | Old % | Why Changed |
|-----------|---------|-------|-------------|
| < 0.3% | 1.50% | 1.30% | Trail efficiency only 55% of peak |
| 0.3–0.7% | 1.20% | 0.80% | **0.80% had 0% win rate** in v5 |
| 0.7–1.2% | 0.50% | 0.50% | Unchanged |
| > 1.2% | 0.30% | 0.30% | Unchanged |

12. **MACD_FLIP** — Histogram flips direction. ONLY fires after
    MIN_HOLD_BARS (10 bars minimum = 50 min on 1h candles).
    **Critical rule: MACD_FLIP only fires if ever_green=True.**
    If position never went green, FAILED_SIGNAL_CUT fires first.
    The min hold bar gate is non-negotiable — source of 51% whipsaws
    when set to 3 bars in production.

### Short Exit Ladder (inverted)
Same structure but:
- Hard stop: 1.5% ABOVE entry (bear rallies are violent)
- Trail ratchets DOWN (stop is a ceiling not a floor)
- Regime flip to BULL closes immediately

### Portfolio-Level Risk Overlays
- **Max drawdown kill switch**: 15% from peak equity. Stop all new
  entries, begin closing in reverse conviction order (lowest first).
- **Daily loss limit**: 5% of starting-day equity. Go to cash.
  Pause until next UTC day.
- **Cooldown per symbol**: 3 hours after any exit.
  Written to state.json — must survive restarts.
- **Max concurrent positions**: Set by StrategyConfig per MCMC.
  Never bypassed.

---

## EXECUTION ENGINE

### Pre-Flight Checks (in order, any failure = no entry)
1. EMERGENCY_STOP file not present
2. Max open positions not exceeded
3. Available capital > Kraken minimum order (~$10)
4. Symbol not in active cooldown
5. No existing open position in same symbol same direction
6. GTW force_exit_existing flag is False
7. Daily loss limit not breached
8. Profit target identifiable (required for reduced conviction floors)
9. **Risk/Reward ratio clears current-condition tolerance threshold**
10. **Max 1 new entry per candle close** (prevents correlated drawdowns)
    — From data: March 10 v8 disaster: BTC + ETH + TAO same candle = -$1,187

### Order Placement
- Market orders only. No limit orders.
  Slippage is knowable. Limit complexity breaks position tracking.
- Size from ApprovedEntry.size_usd, converted to asset quantity at current price.
- Record actual fill price vs signal price. Log slippage to audit CSV.
- Set cooldown immediately after entry.

### Position Sizing
```python
deployable = equity × (1 - DRY_POWDER_PCT)   # keep 20% cash always
available  = deployable - sum(open_position_sizes)
pct        = SIZE_HIGH_PCT if conviction >= 65 else SIZE_LOW_PCT
size       = min(equity × pct, available)
```
Size multiplier from GTW spell applied on top of base calculation.

### State Management — Atomic Writes (MANDATORY)
```python
# Write to temp first, then atomic replace
with open('quant_trader_state.json.tmp', 'w') as f:
    json.dump(state, f)
os.replace('quant_trader_state.json.tmp', 'quant_trader_state.json')
```
An AWS reboot mid-write corrupts the state file. Never write directly.

### State File Contents
```json
{
  "equity": float,
  "peak_equity": float,
  "positions": {
    "SYMBOL/USD": {
      "entry_price": float,
      "fill_price": float,
      "size_usd": float,
      "size_qty": float,
      "direction": "LONG|SHORT",
      "signal_type": str,
      "conviction": int,
      "mcmc_at_entry": str,
      "spell_at_entry": str,
      "entry_time_ms": int,
      "peak_price": float,
      "ever_green": bool,
      "bars_held": int,
      "trail_stop_price": float,
      "target_price": float,
      "regime_confidence_at_entry": int,
      "entry_type": "ANTICIPATORY|MOMENTUM"
    }
  },
  "cooldowns": {"SYMBOL/USD": expiry_ms},
  "trade_count": int,
  "win_count": int,
  "total_pnl": float,
  "boot_time": float,
  "last_entry_ms": int,
  "daily_starting_equity": float,
  "daily_loss_usd": float
}
```

### Audit CSV Columns
```
symbol, direction, entry_type, signal_type, entry_time, exit_time,
entry_price, fill_price, exit_price, size_usd, pnl_usd, pnl_pct,
peak_price, target_price, ever_green, bars_held, exit_reason,
conviction, rr_ratio, tolerance_threshold, mcmc, spell, adx, rsi,
mfi, vol_ratio, slippage_pct, regime_confidence_at_entry,
regime_confidence_at_exit
```

### Boot Sequence (MANDATORY ORDER)
1. Wait 15s for NTP clock sync (Kraken rejects timestamps > 30s off)
2. Load state.json — resume positions and cooldowns
3. Check Kraken margin capability (non-ECP detection)
   — If non-ECP: SHORTS_ENABLED=False, log warning, continue
4. Fetch and log all non-zero account balances
5. Seed candle cache via REST (1.5s apart per symbol — rate limit safe)
6. Start asyncio WebSocket watchers per symbol
7. Start main decision loop

### Infrastructure Rules (NEVER VIOLATE)
- **tmux only** — no systemd
- **t3.small** — not larger
- **Single asyncio loop** — no threading
- **Emergency stop**: `touch EMERGENCY_STOP`

---

## DATA-DERIVED PARAMETERS (FROM BACKTEST ANALYSIS)

### Backtest Performance Summary
| Version | Trades | Win Rate | Return | Max DD | Key Finding |
|---------|--------|----------|--------|--------|------------|
| v_early | 16 | 37.5% | -3.67% | 8.71% | No regime gating = losing |
| btc_v4 first | 16 | 37.5% | -3.67% | 8.71% | Same params, same result |
| btc_v4 fixed | 18 | 55.6% | +6.29% | 7.96% | Target exits + regime discipline |
| v8 multi-asset | 29 | 37.9% | +0.95% | 2.20% | Trail only, no targets |
| v5 fixed | 12 | 75.0% | +7.72% | 0.93% | BB_UPPER target: best system |

### Critical Data Findings
1. **Target exits = 100% win rate** (BB_UPPER: 3/3, SELL_TARGET: 7/7)
   Trail exits = 25–43% win rate. Targets first, trails second.

2. **0.8% trail tier = 0% win rate** across 4 trades in v5.
   Fires at 0.3–0.7% gain. Too tight for noise at that gain level.
   Changed to 1.20%.

3. **Never-green trades**: 6 trades in v8, all MACD_FLIP exits, all losses.
   Total damage: -$2,219. Fix: FAILED_SIGNAL_CUT fires before MACD_FLIP
   if ever_green=False. Pain threshold tightened to 1.0%.

4. **BEAR regime entries**: 8 trades in btc_v4, 25% win rate, -$165 PnL.
   BULL regime: 100% win rate, +$279. Gate is non-negotiable.

5. **Correlated multi-asset entries**: March 10 v8 — BTC, ETH, TAO
   same candle → all flipped MACD_FLIP same session → -$1,187.
   Rule: max 1 new entry per candle.

6. **Vol ratio on wins: 3.50 avg, losses: 2.65 avg.**
   Best winners: ADA 4.95, DOGE 7.61, BTC 3.91.
   Floor: 2.5 alts, 2.0 BTC.

7. **Regime label mismatch**: Trades entered in NEUTRAL, labeled BEAR
   at exit because ETH weakened during hold. Old BEAR trigger fired
   on ETH alone. New architecture requires symmetric confirmation.

8. **Trail efficiency**: Winners captured only 55% of peak profit.
   Trail tiers widened in early stages to fix this.

---

## FILE STRUCTURE

```
quant_trader/
├── Lv1_quant_trader.py          # PPM + GTW — prediction and parameter management
├── Lv2_quant_trader.py          # MCMC Classifier — current condition confirmation
├── Lv3_quant_trader.py          # Signal Router + Conviction Scorer
├── RiskEngine_quant_trader.py   # Risk Management Engine — probabilistic wave model
├── quant_trader_v2.py           # Main orchestrator — boot, loop, orders, state
├── .env                         # KRAKEN_API_KEY and KRAKEN_API_SECRET
├── QUANT_TRADER_ARCHITECTURE.md # This document
├── quant_trader_state.json      # Atomic state file
├── quant_trader_state.json.tmp  # Atomic write buffer (never touch directly)
├── quant_trader_audit.csv       # Every trade with full metadata
├── quant_trader_events.log      # Every decision, timestamped
└── EMERGENCY_STOP               # Create this file to halt the bot immediately
```

**Deployment:**
```bash
tmux new -s quant_trader
python3 quant_trader_v2.py

# Monitor
tail -f quant_trader_events.log

# Emergency stop
touch EMERGENCY_STOP
```

---

## IMPORT CHAIN
```python
# Lv2 imports from Lv1
from Lv1_quant_trader import MCMCType, SpellName, StrategySpell

# Lv3 imports from Lv1 and Lv2
from Lv1_quant_trader import MCMCType, SpellName
from Lv2_quant_trader import MCMCContext, MacroRegime, MicroContext, Session

# RiskEngine imports from Lv2 and Lv3
from Lv2_quant_trader import MCMCContext, MacroRegime, StrategyConfig
from Lv3_quant_trader import ApprovedEntry, SignalType

# quant_trader_v2.py imports everything
from Lv1_quant_trader import PeterParkerModule, GandalfTheWhiteModule
from Lv2_quant_trader import MCMCClassifier
from Lv3_quant_trader import SignalRouter, ConvictionScorer
from RiskEngine_quant_trader import RiskManagementEngine
```

All files must sit in the same directory on EC2.

---

## WHAT TO CHECK IF THE BOT IS NOT FIRING

In order of likelihood:
1. Is the market in confirmed BEAR? No longs will fire. Correct behavior.
2. Is vol_ratio below floor? (2.5 alts, 2.0 BTC)
3. Is conviction below the MCMC-specific floor?
4. Is a profit target not identifiable? Required for lower floors.
5. Is risk/reward ratio below the current-condition tolerance?
6. Is a symbol in cooldown? Check state.json cooldowns dict.
7. Is GTW spell YOU_SHALL_NOT_PASS active? (size_multiplier=0.0)
8. Is daily loss limit breached for today?
9. Is max open positions reached?
10. Is max 1 entry per candle already used this candle?

If none of the above — the bot is probably correct to not fire.
The system is designed to be selective, not silent. Check the events log.

---

## WHAT TO CHECK IF THE BOT IS LOSING MONEY

In order of likelihood:
1. Check the regime at entry vs regime at exit. Did they differ?
   (The historic BEAR label mismatch problem)
2. Check ever_green on losing trades. Did MACD_FLIP fire on never-green?
   Should have been FAILED_SIGNAL_CUT at 1.0% pain threshold.
3. Check vol_ratio on losing trades. Below 2.0 = questionable entry.
4. Check whether multiple assets entered the same candle.
5. Check conviction band. Below 72 = marginal entries.
6. Check trail tier 2 (0.3–0.7% gain). If 0.8% still set, that's the bug.
7. Check whether profit target was identified at entry.

---

*Last updated: End of architecture design session*
*All parameters derived from actual backtest data, not estimates.*
*Do not change parameters without corresponding backtest evidence.*
