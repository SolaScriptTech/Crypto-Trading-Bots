"""mss_bot.py — Liquidity Hunt & Market Structure Shift Bot.

Implements the exact algorithmic blueprint from 'agentic strategy.txt':

  Step 1 — Trend Exhaustion     : 5+ wave bearish expansion establishes sell bias
  Step 2 — Liquidity Sweep      : violent wick below prior swing lows (stop hunt)
                                  → marks the Validated Swing Low
  Step 3 — CHoCH + Displacement : price closes above the Lower High that led to
                                  the sweep → confirms bullish MSS
  Step 4 — OTE Zone mapping     : FVG created by displacement candle +
                                  0.618–0.786 Fibonacci retracement
  Step 5 — Entry                : Limit BUY at FVG top or 0.618 fib (whichever
                                  is higher). SL = 0.3% below Validated Swing
                                  Low. TP = prior range high liquidity pool.
                                  Must clear 1:3 R:R or setup is discarded.

Usage:
    python mss_bot.py BTC/USD
    python mss_bot.py BTC/USD --tf 15m --candles 200
    python mss_bot.py ETH/USD --json eth_mss.json --no-chart
"""

import argparse
import json
import sys
import webbrowser
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from candles import get_candles

# ── Tunables ──────────────────────────────────────────────────────────────────
LOOKBACK_CANDLES    = 100     # operational trading range window
SWING_LEFT          = 3       # bars left of pivot to confirm swing
SWING_RIGHT         = 3       # bars right of pivot to confirm swing
MIN_WAVES           = 5       # minimum bearish waves to qualify exhaustion
SWEEP_WICK_RATIO    = 0.4     # lower wick must be >= 40% of candle range
SWEEP_CLOSE_FACTOR  = 0.6     # close must be in upper 60% of candle range (rejection)
SWEEP_LOOKBACK      = 80      # candles to look back when hunting for a sweep
SL_BUFFER_PCT       = 0.003   # SL = validated_swing_low × (1 - 0.003)
MIN_RR              = 3.0     # minimum reward:risk to confirm setup
FIB_618             = 0.618
FIB_786             = 0.786
MIN_CANDLES         = 60      # minimum candles needed to run analysis


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class Swing:
    index: int
    price: float
    kind:  str    # 'H' or 'L'


@dataclass
class LiquiditySweep:
    index:               int
    candle:              dict
    validated_swing_low: float    # the wick low — ultimate invalidation level
    swept_level:         float    # the prior swing low that was swept
    wick_size:           float
    rejection_strength:  float    # how strongly it closed back up (0–1)


@dataclass
class CHoCH:
    index:            int
    candle:           dict
    lower_high_level: float    # the LH broken by the CHoCH candle
    displacement_peak: float   # high of the displacement move
    displacement_candles: list = field(default_factory=list)


@dataclass
class OTEZone:
    fib_618:   float
    fib_786:   float
    fvg_low:   float
    fvg_high:  float
    entry:     float    # final entry price = max(fib_618, fvg_high)
    source:    str      # 'fvg+fib', 'fib_only', 'fvg_only'


@dataclass
class TradeSetup:
    status:              str     # TRADE or NO_TRADE
    reason:              str
    symbol:              str
    timeframe:           str
    current_price:       float
    # Validated levels
    validated_swing_low: float   = 0.0
    displacement_peak:   float   = 0.0
    lower_high_level:    float   = 0.0
    range_high:          float   = 0.0   # TP target = prior range high
    # Entry plan
    entry:               float   = 0.0
    stop_loss:           float   = 0.0
    take_profit:         float   = 0.0
    sl_distance:         float   = 0.0
    ote_zone_low:        float   = 0.0
    ote_zone_high:       float   = 0.0
    fib_618:             float   = 0.0
    fib_786:             float   = 0.0
    risk_reward:         float   = 0.0
    # Context
    wave_count:          int     = 0
    sweep_index:         int     = 0
    choch_index:         int     = 0
    context_log:         list    = field(default_factory=list)


# ── Main bot class ────────────────────────────────────────────────────────────

class MarketStructureV2Bot:

    def __init__(
        self,
        candles:    list[dict],
        symbol:     str = "UNKNOWN",
        timeframe:  str = "15m",
        lookback:   int = LOOKBACK_CANDLES,
    ):
        if len(candles) < MIN_CANDLES:
            raise ValueError(f"need at least {MIN_CANDLES} candles, got {len(candles)}")
        self.candles   = candles
        self.symbol    = symbol
        self.timeframe = timeframe
        self.lookback  = min(lookback, len(candles))
        self.window    = candles[-self.lookback:]   # operational range window
        self.log       = []

    # ── helpers ───────────────────────────────────────────────────────────────

    def _note(self, msg: str) -> None:
        self.log.append(msg)

    def _find_swings(self, candles: list[dict], left: int = SWING_LEFT, right: int = SWING_RIGHT) -> list[Swing]:
        swings = []
        n = len(candles)
        for i in range(left, n - right):
            hi = candles[i]["high"]
            lo = candles[i]["low"]
            if all(candles[j]["high"] < hi for j in range(i-left, i)) and \
               all(candles[j]["high"] < hi for j in range(i+1, i+right+1)):
                swings.append(Swing(i, hi, "H"))
            if all(candles[j]["low"] > lo for j in range(i-left, i)) and \
               all(candles[j]["low"] > lo for j in range(i+1, i+right+1)):
                swings.append(Swing(i, lo, "L"))
        swings.sort(key=lambda s: s.index)
        return swings

    def _range_high_low(self) -> tuple[float, float]:
        highs = [c["high"] for c in self.window]
        lows  = [c["low"]  for c in self.window]
        return max(highs), min(lows)

    # ── Step 1: Trend exhaustion ──────────────────────────────────────────────

    def _detect_trend_exhaustion(self) -> tuple[bool, int]:
        """
        Count bearish waves (alternating LL/LH sequence) in the window.
        Returns (exhausted, wave_count).
        A 'wave' = one swing Low followed by one swing High (LH) in downtrend.
        """
        swings = self._find_swings(self.window)
        if not swings:
            return False, 0

        # Walk swings and count consecutive bearish waves
        last_h = last_l = None
        bear_waves = 0
        labeled = []
        for s in swings:
            if s.kind == "H":
                tag = "HH" if (last_h is None or s.price > last_h.price) else "LH"
                last_h = s
            else:
                tag = "LL" if (last_l is None or s.price < last_l.price) else "HL"
                last_l = s
            labeled.append((s, tag))

        # Count consecutive LL/LH pairs from the most recent sequence
        # Walk backwards to find the most recent bearish run
        run = 0
        for s, tag in reversed(labeled):
            if tag in ("LL", "LH"):
                run += 1
            else:
                break

        exhausted = run >= MIN_WAVES
        self._note(f"Trend exhaustion: {run} bearish waves (need {MIN_WAVES}) → {'YES' if exhausted else 'NO'}")
        return exhausted, run

    # ── Step 2: Liquidity sweep ───────────────────────────────────────────────

    def _detect_liquidity_sweep(self) -> Optional[LiquiditySweep]:
        """
        Find the most recent sell-side liquidity sweep:
        - Candle wick goes below a prior swing low
        - Close is in the upper portion of the candle (rejection)
        - Strong lower wick relative to total range
        Works on the most recent SWEEP_LOOKBACK candles.
        """
        search = self.window[-SWEEP_LOOKBACK:]
        n      = len(search)
        swings = self._find_swings(search)
        lows   = [s for s in swings if s.kind == "L"]

        best_sweep: Optional[LiquiditySweep] = None

        for i in range(SWING_RIGHT + 1, n):
            c = search[i]
            candle_range = c["high"] - c["low"]
            if candle_range == 0:
                continue

            lower_wick  = c["close"] - c["low"] if c["close"] > c["open"] else c["open"] - c["low"]
            upper_close = (c["close"] - c["low"]) / candle_range

            # Must have significant lower wick and close high in its range
            if lower_wick / candle_range < SWEEP_WICK_RATIO:
                continue
            if upper_close < SWEEP_CLOSE_FACTOR:
                continue

            # Must sweep (go below) at least one prior swing low
            for sl in lows:
                if sl.index >= i:
                    continue
                if c["low"] < sl.price and c["close"] > sl.price:
                    sweep = LiquiditySweep(
                        index               = i,
                        candle              = c,
                        validated_swing_low = c["low"],
                        swept_level         = sl.price,
                        wick_size           = sl.price - c["low"],
                        rejection_strength  = upper_close,
                    )
                    # Prefer the most recent sweep
                    if best_sweep is None or i > best_sweep.index:
                        best_sweep = sweep

        if best_sweep:
            self._note(
                f"Liquidity sweep: idx={best_sweep.index}  "
                f"swept={best_sweep.swept_level:.4f}  "
                f"wick_low={best_sweep.validated_swing_low:.4f}  "
                f"rejection={best_sweep.rejection_strength:.2f}"
            )
        else:
            self._note("No qualifying liquidity sweep found in window.")

        return best_sweep

    # ── Step 3: CHoCH + displacement ─────────────────────────────────────────

    def _find_choch(self, sweep: LiquiditySweep) -> Optional[CHoCH]:
        """
        After the sweep candle, look for the CHoCH:
        - Find the last Lower High (LH) BEFORE the sweep (the LH that led to the drop)
        - Find the first candle AFTER the sweep that CLOSES above that LH
        - The displacement peak = highest high of the displacement move (up to 5 bars after CHoCH)
        """
        search      = self.window
        sweep_idx   = sweep.index
        swings      = self._find_swings(search)

        # Find LHs before the sweep
        lhs_before = [s for s in swings if s.kind == "H" and s.index < sweep_idx]
        if not lhs_before:
            self._note("CHoCH: no swing highs found before sweep.")
            return None

        # The LH immediately preceding the sweep is the CHoCH level
        choch_level = lhs_before[-1].price

        # Find the first candle after the sweep that closes above choch_level
        n = len(search)
        choch_candle = None
        choch_idx    = None
        for i in range(sweep_idx + 1, min(sweep_idx + 30, n)):
            if search[i]["close"] > choch_level:
                choch_candle = search[i]
                choch_idx    = i
                break

        if choch_candle is None:
            self._note(f"CHoCH: no candle closed above LH level {choch_level:.4f} within 30 bars of sweep.")
            return None

        # Displacement peak = highest high from sweep to 5 bars after CHoCH
        disp_candles = search[sweep_idx: min(choch_idx + 6, n)]
        disp_peak    = max(c["high"] for c in disp_candles)

        self._note(
            f"CHoCH confirmed: idx={choch_idx}  "
            f"broke LH={choch_level:.4f}  "
            f"displacement_peak={disp_peak:.4f}"
        )
        return CHoCH(
            index             = choch_idx,
            candle            = choch_candle,
            lower_high_level  = choch_level,
            displacement_peak = disp_peak,
            displacement_candles = disp_candles,
        )

    # ── Step 4: OTE zone ─────────────────────────────────────────────────────

    def _calculate_fib_levels(self, swing_low: float, peak: float) -> dict:
        rng = peak - swing_low
        return {
            "fib_236": peak - rng * 0.236,
            "fib_382": peak - rng * 0.382,
            "fib_500": peak - rng * 0.500,
            "fib_618": peak - rng * FIB_618,
            "fib_786": peak - rng * FIB_786,
        }

    def _find_fvg_in_displacement(self, sweep: LiquiditySweep, choch: CHoCH) -> Optional[dict]:
        """
        Find bullish FVGs created during the displacement (sweep → CHoCH) move.
        A bullish FVG = candle[i-2].high < candle[i].low  (gap between bodies).
        Return the most recent unchallenged one.
        """
        disp = choch.displacement_candles
        n    = len(disp)
        fvgs = []
        for i in range(2, n):
            c1, c3 = disp[i-2], disp[i]
            if c1["high"] < c3["low"]:
                fvg_low  = c1["high"]
                fvg_high = c3["low"]
                # Check unchallenged: no subsequent candle in the full window has low <= fvg_high
                start_idx = choch.index + (i - len(disp) + 1)
                challenged = any(
                    c["low"] <= fvg_high
                    for c in self.window[start_idx + 1:]
                )
                if not challenged:
                    fvgs.append({"low": fvg_low, "high": fvg_high, "mid": (fvg_low + fvg_high) / 2})

        if fvgs:
            best = fvgs[-1]   # most recent unchallenged
            self._note(f"Displacement FVG: low={best['low']:.4f}  high={best['high']:.4f}  mid={best['mid']:.4f}")
            return best
        self._note("No unchallenged FVG found in displacement candles.")
        return None

    def _build_ote_zone(self, fibs: dict, fvg: Optional[dict], current_price: float) -> Optional[OTEZone]:
        fib_618 = fibs["fib_618"]
        fib_786 = fibs["fib_786"]

        if fvg:
            # Entry = top of FVG (fvg_high) if it's inside the OTE zone, else fib_618
            if fib_786 <= fvg["high"] <= fib_618 + (fib_618 - fib_786) * 0.5:
                entry  = fvg["high"]
                source = "fvg+fib"
            else:
                entry  = max(fib_618, fvg["high"])
                source = "fvg_priority"
        else:
            entry  = fib_618
            source = "fib_only"

        # Entry must be BELOW current price (price hasn't yet retraced to OTE)
        # OR at/just above current price (price is in the zone right now)
        if entry > current_price * 1.005:
            self._note(f"OTE entry {entry:.4f} is above current price {current_price:.4f} by >0.5% — not a valid retracement entry.")
            return None

        self._note(f"OTE zone: fib_618={fib_618:.4f}  fib_786={fib_786:.4f}  entry={entry:.4f}  source={source}")
        return OTEZone(
            fib_618  = fib_618,
            fib_786  = fib_786,
            fvg_low  = fvg["low"]  if fvg else 0.0,
            fvg_high = fvg["high"] if fvg else 0.0,
            entry    = entry,
            source   = source,
        )

    # ── Step 5: R:R validation and range high ────────────────────────────────

    def _find_range_high(self, sweep: LiquiditySweep) -> float:
        """
        TP target = the internal range high liquidity pool = the highest swing high
        formed BEFORE the CHoCH displacement sequence, within the operational range.
        """
        swings   = self._find_swings(self.window)
        highs    = [s for s in swings if s.kind == "H" and s.index < sweep.index]
        if not highs:
            return max(c["high"] for c in self.window[:sweep.index + 1])
        return max(s.price for s in highs)

    def _validate_risk_reward(self, entry: float, sl: float, tp: float) -> tuple[bool, float]:
        risk   = abs(entry - sl)
        reward = abs(tp - entry)
        if risk == 0:
            return False, 0.0
        rr = reward / risk
        ok = rr >= MIN_RR
        self._note(f"R:R check: entry={entry:.4f}  SL={sl:.4f}  TP={tp:.4f}  R:R=1:{rr:.2f}  {'PASS' if ok else 'FAIL (need 1:3)'}")
        return ok, rr

    # ── Main execution ────────────────────────────────────────────────────────

    def analyze(self) -> TradeSetup:
        self.log = []
        current_price = self.window[-1]["close"]
        range_high, range_low = self._range_high_low()

        self._note(f"Symbol: {self.symbol}  TF: {self.timeframe}  price: {current_price}")
        self._note(f"Operational range: high={range_high:.4f}  low={range_low:.4f}  ({self.lookback} candles)")

        def no_trade(reason: str) -> TradeSetup:
            return TradeSetup(
                status        = "NO_TRADE",
                reason        = reason,
                symbol        = self.symbol,
                timeframe     = self.timeframe,
                current_price = current_price,
                context_log   = self.log.copy(),
            )

        # ── Step 1 ────────────────────────────────────────────────────────────
        exhausted, wave_count = self._detect_trend_exhaustion()
        if not exhausted:
            return no_trade(f"Trend not exhausted: only {wave_count} bearish waves (need {MIN_WAVES})")

        # ── Step 2 ────────────────────────────────────────────────────────────
        sweep = self._detect_liquidity_sweep()
        if sweep is None:
            return no_trade("No qualifying sell-side liquidity sweep detected")

        # ── Step 3 ────────────────────────────────────────────────────────────
        choch = self._find_choch(sweep)
        if choch is None:
            return no_trade("No CHoCH confirmed after liquidity sweep")

        # ── Step 4 ────────────────────────────────────────────────────────────
        fibs = self._calculate_fib_levels(sweep.validated_swing_low, choch.displacement_peak)
        fvg  = self._find_fvg_in_displacement(sweep, choch)
        ote  = self._build_ote_zone(fibs, fvg, current_price)
        if ote is None:
            return no_trade("OTE zone not reachable at current price or invalid")

        # ── Step 5 ────────────────────────────────────────────────────────────
        sl        = sweep.validated_swing_low * (1 - SL_BUFFER_PCT)
        tp        = self._find_range_high(sweep)
        entry     = ote.entry
        sl_dist   = entry - sl

        rr_ok, rr = self._validate_risk_reward(entry, sl, tp)
        if not rr_ok:
            return no_trade(f"R:R too low: 1:{rr:.2f} (need 1:{MIN_RR:.0f})")

        self._note(f"Setup VALID — LONG  entry={entry:.4f}  SL={sl:.4f}  TP={tp:.4f}  R:R=1:{rr:.2f}")

        return TradeSetup(
            status               = "TRADE",
            reason               = "All conditions met: exhaustion → sweep → CHoCH → OTE → R:R ≥ 1:3",
            symbol               = self.symbol,
            timeframe            = self.timeframe,
            current_price        = current_price,
            validated_swing_low  = sweep.validated_swing_low,
            displacement_peak    = choch.displacement_peak,
            lower_high_level     = choch.lower_high_level,
            range_high           = tp,
            entry                = round(entry, 6),
            stop_loss            = round(sl, 6),
            take_profit          = round(tp, 6),
            sl_distance          = round(sl_dist, 6),
            ote_zone_low         = round(ote.fib_786, 6),
            ote_zone_high        = round(ote.fib_618, 6),
            fib_618              = round(fibs["fib_618"], 6),
            fib_786              = round(fibs["fib_786"], 6),
            risk_reward          = round(rr, 2),
            wave_count           = wave_count,
            sweep_index          = sweep.index,
            choch_index          = choch.index,
            context_log          = self.log.copy(),
        )


# ── CLI ───────────────────────────────────────────────────────────────────────

def format_report(setup: TradeSetup) -> str:
    lines = []
    lines.append("=" * 65)
    lines.append(f"MSS BOT — {setup.symbol}  [{setup.timeframe}]")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}")
    lines.append("=" * 65)

    if setup.status == "TRADE":
        lines.append(f"\n  *** TAKE THE TRADE  (LONG / BUY) ***\n")
        lines.append(f"  Entry (Limit)    : {setup.entry}")
        lines.append(f"  Stop Loss        : {setup.stop_loss}  ({setup.sl_distance:.6f} / unit)")
        lines.append(f"  Take Profit      : {setup.take_profit}  (range high liquidity)")
        lines.append(f"  R:R              : 1:{setup.risk_reward}")
        lines.append("")
        lines.append(f"  OTE zone         : {setup.ote_zone_low} – {setup.ote_zone_high}")
        lines.append(f"  Fib 61.8%        : {setup.fib_618}")
        lines.append(f"  Fib 78.6%        : {setup.fib_786}")
        lines.append(f"  Validated SL     : {setup.validated_swing_low}  (sweep wick low)")
        lines.append(f"  Displacement peak: {setup.displacement_peak}")
        lines.append(f"  Prior range high : {setup.range_high}  (TP target)")
        lines.append(f"  CHoCH level      : {setup.lower_high_level}")
        lines.append("")
        lines.append(f"  Bearish waves    : {setup.wave_count}  (exhaustion confirmed)")
        lines.append(f"  Current price    : {setup.current_price}")
        lines.append(f"  Distance to entry: {abs(setup.current_price - setup.entry) / setup.current_price * 100:.2f}%  "
                     f"({'above entry — wait for retracement' if setup.current_price > setup.entry else 'AT OR BELOW entry — fill may be immediate'})")
    else:
        lines.append(f"\n  NO TRADE\n")
        lines.append(f"  Reason: {setup.reason}")
        lines.append(f"  Price : {setup.current_price}")

    lines.append("")
    lines.append("── Context log ──")
    for note in setup.context_log:
        lines.append(f"  {note}")
    lines.append("=" * 65)
    return "\n".join(lines)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol",           help="e.g. BTC/USD")
    ap.add_argument("--tf",             default="15m",  help="timeframe (default 15m)")
    ap.add_argument("--candles",        type=int, default=200, help="candles to fetch (default 200)")
    ap.add_argument("--lookback",       type=int, default=LOOKBACK_CANDLES)
    ap.add_argument("--json",           default=None,   help="also write JSON to this path")
    ap.add_argument("--no-chart",       action="store_true")
    args = ap.parse_args()

    symbol = args.symbol.upper()
    print(f"Fetching {symbol} {args.tf} candles...")
    candles = get_candles(symbol, args.tf, args.candles)
    print(f"  {len(candles)} candles loaded  (last close: {candles[-1]['close']})\n")

    bot   = MarketStructureV2Bot(candles, symbol=symbol, timeframe=args.tf, lookback=args.lookback)
    setup = bot.analyze()

    print(format_report(setup))

    if args.json:
        Path(args.json).write_text(json.dumps(asdict(setup), indent=2, default=str))
        print(f"\nJSON written to {args.json}")

    return 0 if setup.status == "TRADE" else 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\naborted.")
        sys.exit(130)
