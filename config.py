"""
config.py — Prop Firm Bot System Configuration
All tunable parameters in one place. Calibrated for a 90-day evaluation:
  - Profit target: 3–7% (aim for 5%, stop pushing after 6%)
  - Max drawdown: 15% hard limit (bot kills at 12% as safety buffer)
"""

# ─────────────────────────────────────────────
# EXCHANGE
# ─────────────────────────────────────────────
EXCHANGE_ID       = "kraken"
TRADE_PAIRS = [
    "BTC/USD", "ETH/USD", "SOL/USD",
    "AVAX/USD", "LINK/USD", "DOT/USD",
]
TIMEFRAME         = "1h"
CANDLE_LIMIT      = 120      # candles fetched on boot per pair

# ─────────────────────────────────────────────
# PROP FIRM RISK LIMITS (NON-NEGOTIABLE)
# ─────────────────────────────────────────────
PROP_MAX_DRAWDOWN_PCT       = 0.12   # halt all trading at 12% DD (prop limit is 15%)
PROP_DAILY_LOSS_LIMIT_PCT   = 0.015  # 1.5% daily loss halts trading until next UTC day
PROP_PROFIT_LOCK_PCT        = 0.06   # at 6% total profit: reduce all sizing by 50%
PROP_PROFIT_TARGET_PCT      = 0.05   # 5% target — within the 3-7% window
PROP_MAX_RISK_PER_TRADE_PCT = 0.008  # never risk more than 0.8% of account on a single trade

# ─────────────────────────────────────────────
# CAPITAL ALLOCATION
# ─────────────────────────────────────────────
DRY_POWDER_PCT    = 0.20    # always keep 20% in cash
MAX_POSITIONS     = 4       # max concurrent open positions
SIZE_HIGH_PCT     = 0.20    # high-conviction trade = 20% of deployable
SIZE_LOW_PCT      = 0.12    # low-conviction trade = 12% of deployable

# ─────────────────────────────────────────────
# STRATEGY ENABLE FLAGS
# ─────────────────────────────────────────────
ENABLE_TREND_FOLLOW   = True   # MACD slow/fast cross (BULL + NEUTRAL)
ENABLE_MEAN_REVERSION = True   # BB mean reversion (NEUTRAL only)
ENABLE_MOMENTUM       = True   # Zero-line cross momentum (BULL only)
ENABLE_BEAR_SHORTS    = True   # Short entries in BEAR (overridden if non-ECP)

# ─────────────────────────────────────────────
# REGIME DETECTION
# ─────────────────────────────────────────────
EMA_FAST          = 21
EMA_SLOW          = 55
ADX_PERIOD        = 14
ADX_MIN_TREND     = 25       # minimum ADX for MACD entries (backtest-calibrated)
ADX_MAX_RANGING   = 28       # max ADX for BB mean reversion (ranging market only)
REGIME_ANCHOR     = "BTC/USD"  # market-wide regime anchor

# ─────────────────────────────────────────────
# SIGNAL PARAMETERS
# ─────────────────────────────────────────────
# MACD Slow (trend)
MACD_SLOW_FAST    = 12
MACD_SLOW_SLOW    = 26
MACD_SLOW_SIGNAL  = 9

# MACD Fast (momentum)
MACD_FAST_FAST    = 5
MACD_FAST_SLOW    = 10
MACD_FAST_SIGNAL  = 16

# MACD Zero-line
MACD_ZERO_FAST    = 12
MACD_ZERO_SLOW    = 26
MACD_ZERO_SIGNAL  = 90

# Bollinger Bands
BB_PERIOD         = 20
BB_STDDEV         = 2.0
BB_WIDTH_MIN      = 0.004    # don't trade ultra-tight bands

# RSI
RSI_PERIOD        = 14
RSI_OVERSOLD      = 40       # long entry gate
RSI_OVERBOUGHT    = 60       # short entry gate
RSI_BEAR_SHORT_MIN = 40      # short RSI floor (bear grind)

# Volume
VOL_RATIO_MIN     = 1.5      # current volume must be 1.5× 20-bar avg

# MACD histogram threshold
MACD_FAST_THRESHOLD = 0.0002  # fast MACD histogram minimum (backtest-calibrated)

# ─────────────────────────────────────────────
# CONVICTION SCORING
# ─────────────────────────────────────────────
MIN_CONVICTION    = 62       # minimum score to enter a trade

# Strategy base scores
SCORE_ZERO_LINE   = 30
SCORE_MACD_SLOW   = 25
SCORE_BB_MEAN_REV = 22
SCORE_MACD_FAST   = 18

# Bonus modifiers (added on top of base)
SCORE_REGIME_BULL_BONUS   = 10
SCORE_ADX_STRONG_BONUS    = 8    # ADX > 35
SCORE_RSI_OVERSOLD_BONUS  = 7    # RSI < 35
SCORE_MFI_BONUS           = 6    # MFI < 30
SCORE_VOLUME_BONUS        = 5    # vol_ratio > 2.5
SCORE_KEY_LEVEL_BONUS     = 12   # price within 0.5% of swing high/low

# ─────────────────────────────────────────────
# EXIT PARAMETERS
# ─────────────────────────────────────────────
HARD_STOP_PCT         = 0.030    # 3% hard stop (absolute floor)
SHORT_HARD_STOP_PCT   = 0.015    # 1.5% hard stop for shorts (bear rallies are violent)

# Tiered trailing stop (gain → trail distance)
TRAIL_TIERS = [
    (0.000, 0.010),   # 0.0–0.3%  gain → 1.0% trail
    (0.003, 0.008),   # 0.3–0.7%  gain → 0.8% trail
    (0.007, 0.005),   # 0.7–1.2%  gain → 0.5% trail
    (0.012, 0.003),   # > 1.2%    gain → 0.3% trail
]
PROFIT_FLOOR_TRIGGER  = 0.003    # once >= 0.3% peak, stop never below entry
PROFIT_FLOOR_BUFFER   = 0.001    # floor = entry × (1 + this)

MIN_HOLD_BARS         = 10       # minimum bars before MACD_FLIP exit allowed
FAILED_SIGNAL_BARS    = 6        # never went green after N bars → cut
FAILED_SIGNAL_PAIN    = -0.008   # loss threshold for failed signal cut
ZOMBIE_KILL_HOURS     = 48       # close after 48h if still negative
STAGNATION_HOURS      = 24       # close after 24h if never touched profit

# ─────────────────────────────────────────────
# COOLDOWNS & TIMING
# ─────────────────────────────────────────────
COOLDOWN_MS           = 3 * 3600 * 1000   # 3h cooldown per pair after exit
LOOP_INTERVAL_SEC     = 30               # decision loop cadence
NTP_WAIT_SEC          = 15               # wait for NTP on boot
REST_SEED_SPACING_SEC = 1.5              # spacing between REST calls on boot

# ─────────────────────────────────────────────
# INFRASTRUCTURE
# ─────────────────────────────────────────────
STATE_FILE        = "prop_state.json"
AUDIT_FILE        = "prop_audit.csv"
LOG_FILE          = "prop_events.log"
EMERGENCY_FILE    = "EMERGENCY_STOP"
