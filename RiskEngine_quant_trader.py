"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    RiskEngine_quant_trader.py                               ║
║                  PROBABILISTIC WAVE MODEL — v2                              ║
║                                                                              ║
║  No binary exit gates (except the 3% hard floor).                           ║
║  Every factor contributes a pressure score to the wave.                     ║
║  The ratio of exit_pressure / (exit + hold) determines action.             ║
║                                                                              ║
║  EXIT PRESSURE CHANNELS (each 0-100):                                        ║
║   trail · target · regime · time · ppm · health · momentum · position       ║
║                                                                              ║
║  HOLD PRESSURE CHANNELS (each 0-100):                                        ║
║   anticipatory · ppm_bull · trend · profit · target_approach                ║
║                                                                              ║
║  Thresholds set by confirmed current condition (Lv3), not prediction.       ║
║  ANTICIPATORY longs hold through BEAR because anticipatory_bonus and         ║
║  ppm_bull_signal counter the regime_pressure — by design.                   ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import os, csv, time, logging, datetime
from dataclasses import dataclass, field
from typing import Optional
from enum import Enum

from Lv1_quant_trader import MCMCType, SpellName, EWSAlert
from Lv2_quant_trader import MCMCContext, MacroRegime, StrategyConfig
from Lv3_quant_trader import SignalType

log = logging.getLogger("quant_trader")


# ── Enums ─────────────────────────────────────────────────────────────────────

class ExitReason(str, Enum):
    HARD_STOP          = "HARD_STOP"
    PROFIT_TARGET      = "PROFIT_TARGET"
    BB_UPPER_TARGET    = "BB_UPPER_TARGET"
    BB_MIDBAND_TARGET  = "BB_MIDBAND_TARGET"
    WAVE_CLOSE         = "WAVE_CLOSE"
    WAVE_TIGHTEN       = "WAVE_TIGHTEN"
    EMERGENCY_STOP     = "EMERGENCY_STOP"
    MAX_DRAWDOWN       = "MAX_DRAWDOWN"
    DAILY_LOSS_LIMIT   = "DAILY_LOSS_LIMIT"

class ExitAction(str, Enum):
    HOLD          = "HOLD"
    TIGHTEN_TRAIL = "TIGHTEN_TRAIL"
    CLOSE         = "CLOSE"
    EMERGENCY     = "EMERGENCY"


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class OpenPosition:
    symbol:               str
    direction:            str       # LONG | SHORT
    entry_type:           str       # ANTICIPATORY | MOMENTUM
    signal_type:          str
    entry_price:          float
    fill_price:           float
    size_usd:             float
    size_qty:             float
    entry_time_ms:        int
    conviction:           int
    mcmc_at_entry:        str
    spell_at_entry:       str
    regime_at_entry:      str
    peak_price:           float
    trail_stop_price:     float
    target_price:         float
    ever_green:           bool
    bars_held:            int
    regime_confidence:    int       # 0-100 live score from Lv3
    ema_spread_at_entry:  float = 0.0
    adx_at_entry:         float = 0.0
    rsi_at_entry:         float = 0.0
    bb_upper_at_entry:    float = 0.0
    bb_midband_at_entry:  float = 0.0


@dataclass
class WaveScore:
    """Full probability wave breakdown — every channel visible in audit trail."""
    # Exit pressure (0-100 each)
    trail_pressure:    float = 0.0
    target_pressure:   float = 0.0
    regime_pressure:   float = 0.0
    time_pressure:     float = 0.0
    ppm_pressure:      float = 0.0
    health_pressure:   float = 0.0
    momentum_pressure: float = 0.0
    position_pressure: float = 0.0
    # Hold pressure (0-100 each)
    anticipatory_bonus: float = 0.0
    ppm_bull_signal:    float = 0.0
    trend_confirmation: float = 0.0
    profit_protection:  float = 0.0
    target_approach:    float = 0.0
    # Totals
    total_exit:        float = 0.0
    total_hold:        float = 0.0
    ratio:             float = 0.0
    action_threshold:  float = 0.0

    def compute(self):
        self.total_exit = (self.trail_pressure + self.target_pressure +
            self.regime_pressure + self.time_pressure + self.ppm_pressure +
            self.health_pressure + self.momentum_pressure + self.position_pressure)
        self.total_hold = (self.anticipatory_bonus + self.ppm_bull_signal +
            self.trend_confirmation + self.profit_protection + self.target_approach)
        total = self.total_exit + self.total_hold
        self.ratio = self.total_exit / total if total > 0 else 0.0

    def summary(self) -> str:
        return (f"EXIT={self.total_exit:.1f} HOLD={self.total_hold:.1f} "
                f"ratio={self.ratio:.3f}/thr={self.action_threshold:.3f} | "
                f"trail={self.trail_pressure:.0f} regime={self.regime_pressure:.0f} "
                f"time={self.time_pressure:.0f} momentum={self.momentum_pressure:.0f} "
                f"position={self.position_pressure:.0f} | "
                f"anticip={self.anticipatory_bonus:.0f} ppm_bull={self.ppm_bull_signal:.0f} "
                f"profit={self.profit_protection:.0f} trend={self.trend_confirmation:.0f}")


@dataclass
class RMEDecision:
    symbol:              str
    action:              ExitAction        = ExitAction.HOLD
    exit_reason:         Optional[ExitReason] = None
    exit_price:          float             = 0.0
    pnl_usd:             float             = 0.0
    pnl_pct:             float             = 0.0
    peak_pct:            float             = 0.0
    bars_held:           int               = 0
    updated_trail_stop:  float             = 0.0
    updated_peak_price:  float             = 0.0
    updated_ever_green:  bool              = False
    updated_regime_conf: int               = 100
    wave:                Optional[WaveScore] = None
    reason_detail:       str               = ""


@dataclass
class PortfolioState:
    equity:                float
    peak_equity:           float
    daily_starting_equity: float
    daily_loss_usd:        float
    open_positions:        dict
    macro_regime:          MacroRegime
    current_mcmc:          MCMCType
    active_spell:          SpellName
    ews_alert:             Optional[EWSAlert] = None
    candle_close_ms:       int = 0


# ── Risk Management Engine ────────────────────────────────────────────────────

class RiskManagementEngine:
    """
    Probabilistic wave model. No single data point has veto power.
    Ratio of exit_pressure / (exit + hold) vs threshold determines action.
    """

    # The one true binary — physical floor
    LONG_HARD_STOP_PCT   = 0.030
    SHORT_HARD_STOP_PCT  = 0.015

    # Profit targets
    FIXED_TARGET_PCT     = 0.025

    # Break-even floor
    BREAK_EVEN_TRIGGER   = 0.003
    BREAK_EVEN_BUFFER    = 0.001

    # Trail tiers (data-derived — 0.8% tier had 0% WR, fixed to 1.2%)
    TRAIL_T1  = 0.015   # gain < 0.3%
    TRAIL_T2  = 0.012   # gain 0.3-0.7%
    TRAIL_T3  = 0.005   # gain 0.7-1.2%
    TRAIL_T4  = 0.003   # gain > 1.2%

    # Wave thresholds per confirmed regime
    THRESHOLDS = {
        MacroRegime.BULL:    0.68,
        MacroRegime.NEUTRAL: 0.60,
        MacroRegime.BEAR:    0.52,
    }
    TIGHTEN_OFFSET   = 0.15
    MIN_HOLD_BARS    = 10

    # Portfolio kill switches
    MAX_DRAWDOWN_PCT     = 0.15
    DAILY_LOSS_LIMIT_PCT = 0.05
    EMERGENCY_FILE       = "EMERGENCY_STOP"

    def __init__(self, audit_csv_path="quant_trader_audit.csv"):
        self._audit_path        = audit_csv_path
        self._audit_initialized = False
        log.info("[RME] ⚛️  Risk Management Engine — Probabilistic Wave Model armed.")

    # ── Public entry point ────────────────────────────────────────────────

    def evaluate(self, portfolio, ctx, live_prices, candle_data,
                 macd_data, bb_data, rsi_data, adx_data, ema_data) -> dict:
        decisions = {}

        if os.path.exists(self.EMERGENCY_FILE):
            log.critical("[RME] 🚨 EMERGENCY_STOP detected.")
            for sym, pos in portfolio.open_positions.items():
                price = live_prices.get(sym, pos.fill_price)
                pnl   = self._pnl(pos, price)
                decisions[sym] = RMEDecision(sym, ExitAction.EMERGENCY,
                    ExitReason.EMERGENCY_STOP, price, pnl[0], pnl[1],
                    self._peak_pct(pos), pos.bars_held,
                    reason_detail="EMERGENCY_STOP file present")
            return decisions

        kill = self._portfolio_kill(portfolio)

        for sym, pos in portfolio.open_positions.items():
            price = live_prices.get(sym)
            if not price or price <= 0:
                decisions[sym] = RMEDecision(sym, reason_detail="No live price")
                continue
            self._update_live_state(pos, price)
            decisions[sym] = self._evaluate_position(
                pos, price, ctx, portfolio,
                macd_data.get(sym, []),
                bb_data.get(sym, (None, None, None)),
                rsi_data.get(sym),
                adx_data.get(sym),
                ema_data.get(sym, (None, None)),
                kill,
            )
            self._log_decision(decisions[sym], pos)

        return decisions

    # ── Position evaluation ───────────────────────────────────────────────

    def _evaluate_position(self, pos, price, ctx, portfolio,
                           macd_hist, bb, rsi, adx, emas, kill):
        bb_upper, bb_mid, bb_lower = bb
        ema21, ema55 = emas

        # 1. Hard stop — only true binary
        if self._hard_stop_hit(pos, price):
            pnl = self._pnl(pos, price)
            return RMEDecision(pos.symbol, ExitAction.CLOSE, ExitReason.HARD_STOP,
                price, pnl[0], pnl[1], self._peak_pct(pos), pos.bars_held,
                reason_detail=f"Hard stop hit. fill={pos.fill_price:.2f} price={price:.2f}")

        # 2. Portfolio kill
        if kill:
            pnl = self._pnl(pos, price)
            return RMEDecision(pos.symbol, ExitAction.CLOSE, kill,
                price, pnl[0], pnl[1], self._peak_pct(pos), pos.bars_held,
                reason_detail=f"Portfolio kill: {kill.value}")

        # 3. Hard profit targets — always beneficial, fire immediately
        t = self._check_targets(pos, price, bb_upper, bb_mid)
        if t:
            return t

        # 4. Build the probability wave
        w = WaveScore()
        threshold = self.THRESHOLDS.get(ctx.macro_regime, 0.60)
        w.action_threshold = threshold

        # Exit pressure channels
        trail_stop = self._compute_trail_stop(pos, price, ctx.strategy)
        w.trail_pressure    = self._p_trail(pos, price, trail_stop)
        w.target_pressure   = self._p_target(pos, price, bb_upper)
        w.regime_pressure   = self._p_regime(pos, ctx, ema21, ema55, adx, portfolio.macro_regime)
        w.time_pressure     = self._p_time(pos, price)
        w.ppm_pressure      = self._p_ppm_exit(pos, portfolio.ews_alert)
        w.health_pressure   = self._p_health(ctx)
        w.momentum_pressure = self._p_momentum(pos, macd_hist, rsi)
        w.position_pressure = self._p_position(pos)

        # Hold pressure channels
        w.anticipatory_bonus = self._h_anticipatory(pos, portfolio.ews_alert)
        w.ppm_bull_signal    = self._h_ppm_bull(pos, portfolio.ews_alert)
        w.trend_confirmation = self._h_trend(pos, ema21, ema55, adx, macd_hist)
        w.profit_protection  = self._h_profit(pos, price)
        w.target_approach    = self._h_target_approach(pos, price, bb_upper)

        w.compute()

        new_trail = self._ratchet_trail(pos, trail_stop)

        if w.ratio >= threshold:
            pnl = self._pnl(pos, price)
            return RMEDecision(pos.symbol, ExitAction.CLOSE, ExitReason.WAVE_CLOSE,
                price, pnl[0], pnl[1], self._peak_pct(pos), pos.bars_held,
                wave=w, reason_detail=f"Wave close: {w.summary()}")

        elif w.ratio >= threshold - self.TIGHTEN_OFFSET:
            tight = self._tighten_one_tier(pos, price, ctx.strategy)
            return RMEDecision(pos.symbol, ExitAction.TIGHTEN_TRAIL,
                ExitReason.WAVE_TIGHTEN,
                updated_trail_stop=tight, updated_peak_price=pos.peak_price,
                updated_ever_green=pos.ever_green, updated_regime_conf=pos.regime_confidence,
                bars_held=pos.bars_held, wave=w,
                reason_detail=f"Wave tighten: {w.summary()}")

        else:
            return RMEDecision(pos.symbol, ExitAction.HOLD,
                updated_trail_stop=new_trail, updated_peak_price=pos.peak_price,
                updated_ever_green=pos.ever_green, updated_regime_conf=pos.regime_confidence,
                bars_held=pos.bars_held, wave=w,
                reason_detail=f"Hold: {w.summary()}")

    # ── Exit pressure scorers (0-100 each) ────────────────────────────────

    def _p_trail(self, pos, price, trail_stop) -> float:
        if trail_stop <= 0: return 0.0
        if pos.direction == "LONG":
            if price <= trail_stop: return 100.0
            dist = (price - trail_stop) / price
        else:
            if price >= trail_stop: return 100.0
            dist = (trail_stop - price) / price
        return round(max(0.0, min(100.0, (1 - dist / 0.02) * 80)), 1)

    def _p_target(self, pos, price, bb_upper) -> float:
        if pos.target_price <= 0 or pos.direction != "LONG": return 0.0
        over = (price - pos.target_price) / pos.target_price
        if over >= 0.01: return min(100.0, over * 3000)
        return 0.0

    def _p_regime(self, pos, ctx, ema21, ema55, adx, macro_regime) -> float:
        p = 0.0
        conf = pos.regime_confidence
        if conf < 40:   p += 60.0
        elif conf < 65: p += 35.0
        elif conf < 80: p += 15.0

        if ema21 and ema55 and ema55 > 0:
            spread = (ema21 - ema55) / ema55
            delta  = spread - pos.ema_spread_at_entry
            if pos.direction == "LONG":
                if delta < -0.005: p += 25.0
                elif delta < -0.002: p += 12.0

        if adx and pos.adx_at_entry > 0:
            drop = pos.adx_at_entry - adx
            if drop > 8: p += 15.0
            elif drop > 4: p += 7.0

        # Regime is pressure — but NOT a veto.
        # ANTICIPATORY longs counter this via h_anticipatory.
        if pos.direction == "LONG" and macro_regime == MacroRegime.BEAR:
            p += 20.0
        elif (pos.direction == "LONG" and macro_regime == MacroRegime.NEUTRAL
              and pos.regime_at_entry == "BULL"):
            p += 8.0

        return round(min(100.0, p), 1)

    def _p_time(self, pos, price) -> float:
        hours = (time.time() * 1000 - pos.entry_time_ms) / 3_600_000
        gain  = self._gain_pct(pos, price)
        p = 0.0
        if hours >= 48 and gain < 0:           p += 80.0
        elif hours >= 24 and not pos.ever_green and gain < 0: p += 55.0
        elif hours >= 12 and gain < 0.001:     p += 20.0
        elif hours >= 6 and not pos.ever_green: p += 10.0
        return round(min(100.0, p), 1)

    def _p_ppm_exit(self, pos, ews) -> float:
        if ews is None: return 0.0
        p = 0.0
        if pos.direction == "LONG":
            if ews.extreme_greed_building > 0.70: p += ews.extreme_greed_building * 40
            elif ews.extreme_greed_building > 0.50: p += ews.extreme_greed_building * 20
            if ews.thin_book_approaching > 0.70: p += ews.thin_book_approaching * 25
            if ews.vol_spike_coiling > 0.75: p += ews.vol_spike_coiling * 20
        else:
            if ews.extreme_fear_building > 0.70: p += ews.extreme_fear_building * 35
        return round(min(100.0, p), 1)

    def _p_health(self, ctx) -> float:
        from Lv2_quant_trader import Session, VolumeCharacter
        p, micro = 0.0, ctx.micro
        if micro.volume_character == VolumeCharacter.EXHAUSTING:   p += 20.0
        elif micro.volume_character == VolumeCharacter.CAPITULATING: p += 35.0
        if micro.session == Session.ASIA: p += 10.0
        if micro.adx and micro.adx < 15: p += 15.0
        return round(min(100.0, p), 1)

    def _p_momentum(self, pos, macd_hist, rsi) -> float:
        p = 0.0
        if (pos.ever_green and pos.bars_held >= self.MIN_HOLD_BARS
                and macd_hist and len(macd_hist) >= 3):
            curr, prev, pprev = macd_hist[-1], macd_hist[-2], macd_hist[-3]
            if pos.direction == "LONG":
                if curr < 0 and prev < 0 and pprev >= 0: p += 55.0
                elif curr < 0 and abs(curr) > abs(prev): p += 25.0
            else:
                if curr > 0 and prev > 0 and pprev <= 0: p += 55.0
                elif curr > 0 and abs(curr) > abs(prev): p += 25.0

        if rsi and pos.direction == "LONG":
            if rsi >= 78: p += 30.0
            elif rsi >= 72: p += 15.0
        if rsi and pos.direction == "SHORT":
            if rsi <= 22: p += 30.0
            elif rsi <= 28: p += 15.0
        return round(min(100.0, p), 1)

    def _p_position(self, pos) -> float:
        p = 0.0
        if not pos.ever_green:
            loss = (pos.fill_price - pos.peak_price) / pos.fill_price
            if pos.direction == "SHORT": loss = (pos.peak_price - pos.fill_price) / pos.fill_price
            if loss < -0.005: p += min(70.0, abs(loss) * 5000)
            elif loss < -0.002: p += 20.0
        if pos.conviction < 70 and not pos.ever_green: p += 15.0
        if pos.regime_confidence < 50: p += 20.0
        elif pos.regime_confidence < 65: p += 8.0
        return round(min(100.0, p), 1)

    # ── Hold pressure scorers (0-100 each) ────────────────────────────────

    def _h_anticipatory(self, pos, ews) -> float:
        """
        ANTICIPATORY positions entered for Bear→Bull get significant hold bonus.
        This counteracts regime_pressure — by design. The architecture requires this.
        PPM authorized the entry precisely because BEAR conditions were expected.
        """
        if pos.entry_type != "ANTICIPATORY": return 0.0
        bonus = 40.0
        if ews and ews.extreme_fear_building > 0.50:
            bonus += ews.extreme_fear_building * 20
        hours = (time.time() * 1000 - pos.entry_time_ms) / 3_600_000
        if hours < 24: bonus += 20.0
        elif hours < 48: bonus += 8.0
        return round(min(100.0, bonus), 1)

    def _h_ppm_bull(self, pos, ews) -> float:
        if ews is None or pos.direction == "SHORT": return 0.0
        b = 0.0
        if ews.altseason_assembling > 0.60:     b += ews.altseason_assembling * 40
        if ews.extreme_fear_building > 0.65:    b += ews.extreme_fear_building * 35
        if ews.fakeout_conditions > 0.60:       b += ews.fakeout_conditions * 20
        if ews.vol_spike_coiling > 0.60 and ews.consolidation_forming < 0.40:
            b += ews.vol_spike_coiling * 15
        return round(min(100.0, b), 1)

    def _h_trend(self, pos, ema21, ema55, adx, macd_hist) -> float:
        b = 0.0
        if ema21 and ema55 and ema55 > 0:
            if pos.direction == "LONG" and ema21 > ema55:
                b += min(30.0, (ema21 - ema55) / ema55 * 3000)
            elif pos.direction == "SHORT" and ema21 < ema55:
                b += min(30.0, (ema55 - ema21) / ema55 * 3000)
        if adx and adx >= 20: b += min(20.0, (adx - 20) * 0.5)
        if macd_hist and len(macd_hist) >= 2:
            curr, prev = macd_hist[-1], macd_hist[-2]
            if pos.direction == "LONG":
                if curr > 0 and curr >= prev: b += 20.0
                elif curr > 0: b += 8.0
            else:
                if curr < 0 and curr <= prev: b += 20.0
                elif curr < 0: b += 8.0
        return round(min(100.0, b), 1)

    def _h_profit(self, pos, price) -> float:
        gain = self._gain_pct(pos, price)
        if gain <= 0: return 0.0
        b = min(60.0, gain * 3000)
        if pos.ever_green: b += 15.0
        if gain >= self.BREAK_EVEN_TRIGGER: b += 10.0
        return round(min(100.0, b), 1)

    def _h_target_approach(self, pos, price, bb_upper) -> float:
        b = 0.0
        if bb_upper and pos.direction == "LONG":
            dist = (bb_upper - price) / price
            if 0 < dist <= 0.02: b += min(50.0, (1 - dist / 0.02) * 50)
        if pos.target_price > 0 and pos.direction == "LONG":
            dist = (pos.target_price - price) / price
            if 0 < dist <= 0.02: b += min(40.0, (1 - dist / 0.02) * 40)
        return round(min(100.0, b), 1)

    # ── Profit targets ────────────────────────────────────────────────────

    def _check_targets(self, pos, price, bb_upper, bb_mid):
        if (pos.signal_type == SignalType.BB_MEAN_REV.value and
                bb_mid and pos.direction == "LONG" and price >= bb_mid):
            pnl = self._pnl(pos, price)
            return RMEDecision(pos.symbol, ExitAction.CLOSE, ExitReason.BB_MIDBAND_TARGET,
                price, pnl[0], pnl[1], self._peak_pct(pos), pos.bars_held,
                reason_detail=f"BB midband {bb_mid:.2f} hit")

        if bb_upper and pos.direction == "LONG" and price >= bb_upper:
            pnl = self._pnl(pos, price)
            return RMEDecision(pos.symbol, ExitAction.CLOSE, ExitReason.BB_UPPER_TARGET,
                price, pnl[0], pnl[1], self._peak_pct(pos), pos.bars_held,
                reason_detail=f"BB upper {bb_upper:.2f} hit (100% WR in backtest)")

        if pos.target_price > 0 and pos.direction == "LONG" and price >= pos.target_price:
            pnl = self._pnl(pos, price)
            return RMEDecision(pos.symbol, ExitAction.CLOSE, ExitReason.PROFIT_TARGET,
                price, pnl[0], pnl[1], self._peak_pct(pos), pos.bars_held,
                reason_detail=f"Fixed target {pos.target_price:.2f} hit")
        return None

    # ── Trail mechanics ───────────────────────────────────────────────────

    def _compute_trail_stop(self, pos, price, cfg) -> float:
        if pos.direction == "LONG":
            peak  = pos.peak_price
            gain  = (peak - pos.fill_price) / pos.fill_price if pos.fill_price > 0 else 0
            trail = self._trail_pct(gain, cfg)
            stop  = peak * (1 - trail)
            if gain >= self.BREAK_EVEN_TRIGGER:
                stop = max(stop, pos.fill_price * (1 + self.BREAK_EVEN_BUFFER))
        else:
            peak  = pos.peak_price if pos.peak_price > 0 else price
            gain  = (pos.fill_price - peak) / pos.fill_price if pos.fill_price > 0 else 0
            trail = self._trail_pct(gain, cfg)
            stop  = peak * (1 + trail)
            if gain >= self.BREAK_EVEN_TRIGGER:
                stop = min(stop, pos.fill_price * (1 - self.BREAK_EVEN_BUFFER))
        return round(stop, 8)

    def _ratchet_trail(self, pos, new_stop) -> float:
        if pos.direction == "LONG":
            return max(new_stop, pos.trail_stop_price)
        if pos.trail_stop_price <= 0: return new_stop
        return min(new_stop, pos.trail_stop_price)

    def _tighten_one_tier(self, pos, price, cfg) -> float:
        gain = self._gain_pct(pos, price)
        if gain < 0.003:   t = cfg.trail_tier_2_pct / 100
        elif gain < 0.007: t = cfg.trail_tier_3_pct / 100
        else:              t = cfg.trail_tier_4_pct / 100
        if pos.direction == "LONG":
            stop = pos.peak_price * (1 - t)
            if gain >= self.BREAK_EVEN_TRIGGER:
                stop = max(stop, pos.fill_price * (1 + self.BREAK_EVEN_BUFFER))
            return max(stop, pos.trail_stop_price)
        else:
            stop = pos.peak_price * (1 + t)
            if gain >= self.BREAK_EVEN_TRIGGER:
                stop = min(stop, pos.fill_price * (1 - self.BREAK_EVEN_BUFFER))
            return min(stop, pos.trail_stop_price) if pos.trail_stop_price > 0 else stop

    def _trail_pct(self, gain, cfg) -> float:
        if gain < 0.003:   return cfg.trail_tier_1_pct / 100
        elif gain < 0.007: return cfg.trail_tier_2_pct / 100
        elif gain < 0.012: return cfg.trail_tier_3_pct / 100
        else:              return cfg.trail_tier_4_pct / 100

    # ── Portfolio overlays ────────────────────────────────────────────────

    def _portfolio_kill(self, p):
        if p.peak_equity > 0:
            dd = (p.peak_equity - p.equity) / p.peak_equity
            if dd >= self.MAX_DRAWDOWN_PCT:
                log.critical(f"[RME] 🚨 MAX DD {dd*100:.1f}%")
                return ExitReason.MAX_DRAWDOWN
        if p.daily_starting_equity > 0:
            loss_pct = p.daily_loss_usd / p.daily_starting_equity
            if loss_pct >= self.DAILY_LOSS_LIMIT_PCT:
                log.critical(f"[RME] 🚨 DAILY LOSS {loss_pct*100:.1f}%")
                return ExitReason.DAILY_LOSS_LIMIT
        return None

    # ── Utilities ─────────────────────────────────────────────────────────

    def _hard_stop_hit(self, pos, price) -> bool:
        if pos.direction == "LONG":
            return price <= pos.fill_price * (1 - self.LONG_HARD_STOP_PCT)
        return price >= pos.fill_price * (1 + self.SHORT_HARD_STOP_PCT)

    def _update_live_state(self, pos, price):
        if pos.direction == "LONG":
            pos.peak_price = max(pos.peak_price, price)
            if price > pos.fill_price: pos.ever_green = True
        else:
            if pos.peak_price == 0: pos.peak_price = price
            pos.peak_price = min(pos.peak_price, price)
            if price < pos.fill_price: pos.ever_green = True

    def _gain_pct(self, pos, price) -> float:
        if pos.fill_price <= 0: return 0.0
        if pos.direction == "LONG":
            return (price - pos.fill_price) / pos.fill_price
        return (pos.fill_price - price) / pos.fill_price

    def _peak_pct(self, pos) -> float:
        if pos.fill_price <= 0: return 0.0
        if pos.direction == "LONG":
            return round((pos.peak_price - pos.fill_price) / pos.fill_price * 100, 3)
        return round((pos.fill_price - pos.peak_price) / pos.fill_price * 100, 3)

    def _pnl(self, pos, exit_price) -> tuple:
        if pos.fill_price <= 0 or pos.size_usd <= 0: return 0.0, 0.0
        pct = self._gain_pct(pos, exit_price)
        return round(pos.size_usd * pct, 4), round(pct * 100, 4)

    def _log_decision(self, d, pos):
        if d.action == ExitAction.CLOSE:
            log.info(f"[RME] 🔴 CLOSE {pos.symbol} {pos.direction} "
                     f"reason={d.exit_reason.value} pnl=${d.pnl_usd:+.2f}({d.pnl_pct:+.2f}%) "
                     f"peak={d.peak_pct:+.2f}% bars={d.bars_held} "
                     f"ever_green={pos.ever_green} entry={pos.entry_type}")
            if d.wave: log.debug(f"[RME]   {d.wave.summary()}")
        elif d.action == ExitAction.TIGHTEN_TRAIL:
            log.info(f"[RME] ⚠️  TIGHTEN {pos.symbol} stop→{d.updated_trail_stop:.2f}")
            if d.wave: log.debug(f"[RME]   {d.wave.summary()}")
        else:
            log.debug(f"[RME] ✅ HOLD {pos.symbol}")

    # ── Audit trail ───────────────────────────────────────────────────────

    def write_audit(self, pos, decision, ctx):
        fieldnames = [
            "symbol","direction","entry_type","signal_type","entry_time","exit_time",
            "entry_price","fill_price","exit_price","size_usd","pnl_usd","pnl_pct",
            "peak_price","peak_pct","target_price","ever_green","bars_held","exit_reason",
            "conviction","mcmc_at_entry","mcmc_at_exit","spell_at_entry","spell_at_exit",
            "regime_at_entry","regime_at_exit","regime_confidence",
            "wave_ratio","wave_threshold",
            "w_trail","w_target","w_regime","w_time","w_ppm_p","w_health","w_momentum","w_position",
            "w_anticipatory","w_ppm_bull","w_trend","w_profit","w_approach","reason_detail",
        ]
        w = decision.wave
        row = {
            "symbol": pos.symbol, "direction": pos.direction,
            "entry_type": pos.entry_type, "signal_type": pos.signal_type,
            "entry_time": datetime.datetime.fromtimestamp(pos.entry_time_ms/1000).strftime("%Y-%m-%d %H:%M:%S"),
            "exit_time": datetime.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"),
            "entry_price": round(pos.entry_price,4), "fill_price": round(pos.fill_price,4),
            "exit_price": round(decision.exit_price,4), "size_usd": round(pos.size_usd,2),
            "pnl_usd": round(decision.pnl_usd,4), "pnl_pct": round(decision.pnl_pct,4),
            "peak_price": round(pos.peak_price,4), "peak_pct": round(decision.peak_pct,3),
            "target_price": round(pos.target_price,4), "ever_green": pos.ever_green,
            "bars_held": pos.bars_held,
            "exit_reason": decision.exit_reason.value if decision.exit_reason else "",
            "conviction": pos.conviction, "mcmc_at_entry": pos.mcmc_at_entry,
            "mcmc_at_exit": ctx.confirmed_mcmc.value, "spell_at_entry": pos.spell_at_entry,
            "spell_at_exit": ctx.active_spell.value, "regime_at_entry": pos.regime_at_entry,
            "regime_at_exit": ctx.macro_regime.value, "regime_confidence": pos.regime_confidence,
            "wave_ratio": round(w.ratio,4) if w else "",
            "wave_threshold": round(w.action_threshold,4) if w else "",
            "w_trail": round(w.trail_pressure,1) if w else "",
            "w_target": round(w.target_pressure,1) if w else "",
            "w_regime": round(w.regime_pressure,1) if w else "",
            "w_time": round(w.time_pressure,1) if w else "",
            "w_ppm_p": round(w.ppm_pressure,1) if w else "",
            "w_health": round(w.health_pressure,1) if w else "",
            "w_momentum": round(w.momentum_pressure,1) if w else "",
            "w_position": round(w.position_pressure,1) if w else "",
            "w_anticipatory": round(w.anticipatory_bonus,1) if w else "",
            "w_ppm_bull": round(w.ppm_bull_signal,1) if w else "",
            "w_trend": round(w.trend_confirmation,1) if w else "",
            "w_profit": round(w.profit_protection,1) if w else "",
            "w_approach": round(w.target_approach,1) if w else "",
            "reason_detail": decision.reason_detail[:200],
        }
        write_header = not self._audit_initialized or not os.path.exists(self._audit_path)
        with open(self._audit_path, "a", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            if write_header: writer.writeheader()
            writer.writerow(row)
        self._audit_initialized = True


# ── State loader ──────────────────────────────────────────────────────────────

def position_from_state(symbol: str, data: dict) -> OpenPosition:
    return OpenPosition(
        symbol=symbol, direction=data.get("direction","LONG"),
        entry_type=data.get("entry_type","MOMENTUM"),
        signal_type=data.get("signal_type","MACD_SLOW_CROSS"),
        entry_price=float(data.get("entry_price",0)),
        fill_price=float(data.get("fill_price",0)),
        size_usd=float(data.get("size_usd",0)),
        size_qty=float(data.get("size_qty",0)),
        entry_time_ms=int(data.get("entry_time_ms",0)),
        conviction=int(data.get("conviction",0)),
        mcmc_at_entry=data.get("mcmc_at_entry","UNKNOWN"),
        spell_at_entry=data.get("spell_at_entry","GLAMDRING"),
        regime_at_entry=data.get("regime_at_entry","NEUTRAL"),
        peak_price=float(data.get("peak_price", data.get("fill_price",0))),
        trail_stop_price=float(data.get("trail_stop_price",0)),
        target_price=float(data.get("target_price",0)),
        ever_green=bool(data.get("ever_green",False)),
        bars_held=int(data.get("bars_held",0)),
        regime_confidence=int(data.get("regime_confidence",100)),
        ema_spread_at_entry=float(data.get("ema_spread_at_entry",0)),
        adx_at_entry=float(data.get("adx_at_entry",0)),
        rsi_at_entry=float(data.get("rsi_at_entry",0)),
        bb_upper_at_entry=float(data.get("bb_upper_at_entry",0)),
        bb_midband_at_entry=float(data.get("bb_midband_at_entry",0)),
    )


# ── Self-test ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys, random
    sys.path.insert(0, ".")
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s | %(levelname)s | %(message)s")
    from Lv1_quant_trader import PeterParkerModule, GandalfTheWhiteModule
    from Lv2_quant_trader import MCMCClassifier

    log.info("=" * 70)
    log.info("RiskEngine — Probabilistic Wave Model Self-Test")
    log.info("=" * 70)

    rme = RiskManagementEngine("/tmp/wave_audit.csv")
    random.seed(42)
    base = 85_000.0

    def mkc(n, trend=0.0):
        candles, p = [], base
        for _ in range(n):
            o=p; c=o*(1+random.gauss(trend,0.005))
            h=max(o,c)*1.002; l=min(o,c)*0.998; v=random.uniform(10,50)
            candles.append({"o":o,"h":h,"l":l,"c":c,"v":v,"ts":int(time.time()*1000)})
            p=c
        return candles

    candles = mkc(80, 0.0003)
    ob = {"bids":[[84900-i*10,0.5,0] for i in range(10)],
          "asks":[[85100+i*10,0.5,0] for i in range(10)]}

    ppm  = PeterParkerModule()
    gtw  = GandalfTheWhiteModule(timeframe="1h")
    mcmc = MCMCClassifier()
    alert = ppm.sense(candles_1h=candles, candles_4h=mkc(20), order_book=ob,
                      btc_candles=candles, alt_candles={})
    spell = gtw.concoct_spell(alert, 1)
    ctx   = mcmc.classify(candles=candles, btc_candles=candles, order_book=ob,
                          spell=spell, symbol="BTC/USD")
    price = candles[-1]["c"]

    log.info(f"Context: {ctx.confirmed_mcmc.value} | {ctx.macro_regime.value}")

    def mkpos(sym, direction, entry_type, fill_mult, peak_mult, ever_green,
              bars, conf, regime_at_entry, conviction=75, trail=0.0):
        return OpenPosition(
            symbol=sym, direction=direction, entry_type=entry_type,
            signal_type="MACD_SLOW_CROSS",
            entry_price=price*fill_mult, fill_price=price*fill_mult,
            size_usd=1000.0, size_qty=0.01,
            entry_time_ms=int(time.time()*1000)-bars*3_600_000,
            conviction=conviction, mcmc_at_entry="TRENDING",
            spell_at_entry="GLAMDRING", regime_at_entry=regime_at_entry,
            peak_price=price*peak_mult,
            trail_stop_price=trail if trail else price*fill_mult*0.97,
            target_price=price*fill_mult*1.025,
            ever_green=ever_green, bars_held=bars,
            regime_confidence=conf, adx_at_entry=20,
            ema_spread_at_entry=0.002 if direction=="LONG" else -0.002,
        )

    positions = {
        # Winning momentum long — should HOLD
        "BTC/USD": mkpos("BTC/USD","LONG","MOMENTUM",0.97,1.01,True,12,85,"BULL",78),
        # Dead momentum long never green — should CLOSE via wave
        "ETH/USD": mkpos("ETH/USD","LONG","MOMENTUM",1.02,1.02,False,8,55,"BULL",70),
        # ANTICIPATORY long in BEAR conditions — should HOLD (anticip bonus)
        "SOL/USD": mkpos("SOL/USD","LONG","ANTICIPATORY",0.98,0.995,False,4,60,"BEAR",75),
        # Degrading regime confidence — should TIGHTEN
        "ADA/USD": mkpos("ADA/USD","LONG","MOMENTUM",0.99,1.005,True,8,52,"NEUTRAL",74),
    }

    portfolio = PortfolioState(
        equity=10_000.0, peak_equity=10_200.0,
        daily_starting_equity=10_000.0, daily_loss_usd=0.0,
        open_positions=positions,
        macro_regime=ctx.macro_regime,
        current_mcmc=ctx.confirmed_mcmc,
        active_spell=ctx.active_spell,
        ews_alert=alert,
    )

    live_prices = {"BTC/USD":price, "ETH/USD":price*0.985,
                   "SOL/USD":price*0.990, "ADA/USD":price*1.003}
    macd_data   = {"BTC/USD":[0.012,0.015,0.013,0.010],
                   "ETH/USD":[-0.003,-0.006,-0.009],
                   "SOL/USD":[0.002,0.001,-0.001],
                   "ADA/USD":[0.008,0.006,0.004]}
    bb_data     = {s:(price*1.03,price,price*0.97) for s in live_prices}
    rsi_data    = {"BTC/USD":58.0,"ETH/USD":42.0,"SOL/USD":38.0,"ADA/USD":55.0}
    adx_data    = {"BTC/USD":22.0,"ETH/USD":15.0,"SOL/USD":18.0,"ADA/USD":19.0}
    ema_data    = {"BTC/USD":(price*1.002,price*0.998),"ETH/USD":(price*0.995,price*1.005),
                   "SOL/USD":(price*0.993,price*1.005),"ADA/USD":(price*1.001,price*0.999)}

    decisions = rme.evaluate(portfolio, ctx, live_prices,
        {s:candles for s in live_prices}, macd_data, bb_data,
        rsi_data, adx_data, ema_data)

    log.info("\n" + "=" * 70)
    log.info("WAVE DECISIONS")
    log.info("=" * 70)
    for sym, d in decisions.items():
        pos = positions[sym]
        w   = d.wave
        print(f"\n  {sym:12s} [{pos.entry_type:13s}] → {d.action.value:14s}"
              f"  {'('+d.exit_reason.value+')' if d.exit_reason else ''}")
        if w:
            print(f"    EXIT  trail={w.trail_pressure:.0f} regime={w.regime_pressure:.0f} "
                  f"time={w.time_pressure:.0f} momentum={w.momentum_pressure:.0f} "
                  f"position={w.position_pressure:.0f} → {w.total_exit:.1f}")
            print(f"    HOLD  anticip={w.anticipatory_bonus:.0f} ppm_bull={w.ppm_bull_signal:.0f} "
                  f"trend={w.trend_confirmation:.0f} profit={w.profit_protection:.0f} "
                  f"→ {w.total_hold:.1f}")
            print(f"    RATIO {w.ratio:.3f} vs thr={w.action_threshold:.3f}")

    log.info("\n✅ Probabilistic Wave Model self-test complete.")