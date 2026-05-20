"""
risk_manager.py — Prop Firm Evaluation Risk Controller

Enforces all prop firm constraints with hard gates:
  - Max drawdown: 12% bot halt (15% is prop limit)
  - Daily loss limit: 1.5% halts trading until next UTC day
  - Profit lock: at 6% gain, reduce all sizing by 50%
  - Per-trade risk cap: 0.8% of account max
  - Scaling rules: reduce exposure as profit builds

This module is the FINAL GATE before any order is placed.
"""

import json
import logging
import os
from datetime import datetime, timezone
from typing import Optional
import config as C

logger = logging.getLogger("RiskManager")


class PropRiskManager:
    """
    Tracks account-level P&L and enforces prop firm evaluation rules.
    All limits are loaded from config.py — change them there, not here.
    """

    def __init__(self, starting_equity: float):
        self.starting_equity     = starting_equity
        self.peak_equity         = starting_equity
        self.current_equity      = starting_equity

        # Daily tracking (resets at UTC midnight)
        self._day_start_equity   = starting_equity
        self._current_day        = self._utc_day()

        # Drawdown / halt state
        self.trading_halted      = False
        self.halt_reason         = ""

        # Sizing modifier (applied when profit lock triggered)
        self._sizing_modifier    = 1.0

        # Statistics
        self.total_trades        = 0
        self.winning_trades      = 0
        self.total_realized_pnl  = 0.0
        self.daily_realized_pnl  = 0.0

        logger.info(
            f"RiskManager initialized | starting_equity=${starting_equity:,.2f} | "
            f"max_dd={C.PROP_MAX_DRAWDOWN_PCT:.1%} | daily_loss={C.PROP_DAILY_LOSS_LIMIT_PCT:.1%}"
        )

    # ─────────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────────

    def update_equity(self, new_equity: float) -> None:
        """Call after every candle / on balance fetch. Triggers halt checks."""
        self._roll_day_if_needed()
        self.current_equity = new_equity

        if new_equity > self.peak_equity:
            self.peak_equity = new_equity

        self._check_drawdown()
        self._check_daily_loss()
        self._update_sizing_modifier()

    def record_trade(self, pnl: float, is_win: bool) -> None:
        """Called after each trade closes."""
        self.total_trades       += 1
        self.total_realized_pnl += pnl
        self.daily_realized_pnl += pnl
        if is_win:
            self.winning_trades += 1

    def can_trade(self) -> tuple[bool, str]:
        """
        Returns (allowed: bool, reason: str).
        Final gate — check this before EVERY entry.
        """
        if os.path.exists(C.EMERGENCY_FILE):
            return False, "EMERGENCY_STOP file detected"

        if self.trading_halted:
            return False, self.halt_reason

        if self._daily_loss_pct() <= -C.PROP_DAILY_LOSS_LIMIT_PCT:
            return False, f"Daily loss limit hit ({self._daily_loss_pct():.2%})"

        if self._total_drawdown_pct() <= -C.PROP_MAX_DRAWDOWN_PCT:
            return False, f"Max drawdown limit hit ({self._total_drawdown_pct():.2%})"

        return True, "ok"

    def size_trade(self, base_size: float) -> float:
        """Apply the sizing modifier (reduced when profit lock is active)."""
        return base_size * self._sizing_modifier

    def max_risk_per_trade(self) -> float:
        """Max dollar loss allowed on a single trade (0.8% of current equity)."""
        return self.current_equity * C.PROP_MAX_RISK_PER_TRADE_PCT

    def stop_price_from_risk(
        self,
        entry_price: float,
        side: str,
        position_size_usd: float,
    ) -> float:
        """
        Compute a hard stop price such that total loss never exceeds per-trade risk cap.
        stop = entry ± (max_risk_usd / position_qty)
        """
        max_loss_usd = self.max_risk_per_trade()
        qty = position_size_usd / entry_price
        if qty == 0:
            return entry_price * (0.97 if side == "long" else 1.03)
        price_distance = max_loss_usd / qty
        if side == "long":
            return entry_price - price_distance
        else:
            return entry_price + price_distance

    def status_report(self) -> dict:
        """Full state snapshot for logging and heartbeat."""
        return {
            "equity":           round(self.current_equity, 2),
            "peak_equity":      round(self.peak_equity, 2),
            "starting_equity":  round(self.starting_equity, 2),
            "total_pnl_pct":    round(self._total_pnl_pct() * 100, 3),
            "drawdown_pct":     round(self._total_drawdown_pct() * 100, 3),
            "daily_pnl_pct":    round(self._daily_loss_pct() * 100, 3),
            "sizing_modifier":  round(self._sizing_modifier, 2),
            "trading_halted":   self.trading_halted,
            "halt_reason":      self.halt_reason,
            "total_trades":     self.total_trades,
            "win_rate":         round(self.winning_trades / max(self.total_trades, 1) * 100, 1),
            "realized_pnl":     round(self.total_realized_pnl, 2),
            "prop_target_met":  self._total_pnl_pct() >= C.PROP_PROFIT_TARGET_PCT,
        }

    def resume_if_safe(self) -> bool:
        """
        Attempt to resume trading if conditions have recovered.
        Called once per loop when halted.
        Returns True if trading was resumed.
        """
        if not self.trading_halted:
            return False

        can, reason = self.can_trade()
        if can:
            self.trading_halted = False
            self.halt_reason    = ""
            logger.info("Trading RESUMED — conditions recovered")
            return True
        return False

    # ─────────────────────────────────────────────
    # PRIVATE HELPERS
    # ─────────────────────────────────────────────

    def _check_drawdown(self) -> None:
        dd = self._total_drawdown_pct()
        if dd <= -C.PROP_MAX_DRAWDOWN_PCT:
            if not self.trading_halted:
                self.trading_halted = True
                self.halt_reason    = f"DRAWDOWN_HALT: {dd:.2%} (limit {C.PROP_MAX_DRAWDOWN_PCT:.1%})"
                logger.critical(f"⛔  TRADING HALTED — {self.halt_reason}")

        elif dd <= -(C.PROP_MAX_DRAWDOWN_PCT * 0.75):
            # Early warning at 75% of drawdown limit
            logger.warning(
                f"⚠️  Drawdown warning: {dd:.2%} "
                f"(limit {C.PROP_MAX_DRAWDOWN_PCT:.1%}, "
                f"at {abs(dd)/C.PROP_MAX_DRAWDOWN_PCT:.0%} of limit)"
            )

    def _check_daily_loss(self) -> None:
        dl = self._daily_loss_pct()
        if dl <= -C.PROP_DAILY_LOSS_LIMIT_PCT:
            if not self.trading_halted:
                self.trading_halted = True
                self.halt_reason    = f"DAILY_LOSS_HALT: {dl:.2%} (limit {C.PROP_DAILY_LOSS_LIMIT_PCT:.1%})"
                logger.warning(f"⛔  Daily loss limit hit — {self.halt_reason}")

    def _update_sizing_modifier(self) -> None:
        pnl = self._total_pnl_pct()
        if pnl >= C.PROP_PROFIT_LOCK_PCT:
            new_mod = 0.5
        elif pnl >= C.PROP_PROFIT_LOCK_PCT * 0.75:
            new_mod = 0.65   # taper early
        else:
            new_mod = 1.0

        if new_mod != self._sizing_modifier:
            logger.info(
                f"Sizing modifier changed: {self._sizing_modifier:.2f} → {new_mod:.2f} "
                f"(total PnL: {pnl:.2%})"
            )
            self._sizing_modifier = new_mod

    def _roll_day_if_needed(self) -> None:
        today = self._utc_day()
        if today != self._current_day:
            logger.info(
                f"Day roll: {self._current_day} → {today} | "
                f"daily PnL was {self._daily_loss_pct():.2%}"
            )
            self._current_day      = today
            self._day_start_equity = self.current_equity
            self.daily_realized_pnl = 0.0
            # Resume from daily-loss halt on new day
            if self.trading_halted and "DAILY_LOSS" in self.halt_reason:
                self.trading_halted = False
                self.halt_reason    = ""
                logger.info("Daily loss halt CLEARED — new trading day")

    def _total_pnl_pct(self) -> float:
        return (self.current_equity - self.starting_equity) / self.starting_equity

    def _total_drawdown_pct(self) -> float:
        return (self.current_equity - self.peak_equity) / self.peak_equity

    def _daily_loss_pct(self) -> float:
        return (self.current_equity - self._day_start_equity) / self._day_start_equity

    @staticmethod
    def _utc_day() -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")
