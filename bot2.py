"""Bot 2 — Signal Filtration Bot.

Pulls 15m and 5m candles from Kraken for a user-specified symbol and reports:
- Trend direction + trendline touch count (liquidity inflection levels)
- Swing structure (HH/HL/LH/LL)
- Most recent BOS and CHOC
- Post-CHOC structure: first BOS then swing point per spec
- All FVGs in lookback window (~3 days), highlighting unchallenged + most recent
- Support/resistance zones
- Liquidity pockets (ceiling/floor)
- Failed breakouts (wick rejections at prior swings)
- "5 wave pattern" check on most recent CHOC trendline

Usage:
    python bot2.py BTC/USD
    python bot2.py DOGE/USD --json out.json
"""
import argparse
import json
import sys
import webbrowser
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path

from candles import get_candles

LOOKBACK_DAYS = 3
SWING_LEFT = 2
SWING_RIGHT = 2
ZONE_CLUSTER_PCT = 0.0012
EQUAL_LEVEL_PCT = 0.0007
TRENDLINE_TOUCH_PCT = 0.002
SR_SWING_LOOKBACK = 40
LIQ_SWING_LOOKBACK = 30
LIQ_MIN_BARS_APART = 5


@dataclass
class Swing:
    index: int
    time: int
    price: float
    kind: str

    def label(self) -> str:
        return self.kind


def compute_swings(candles, left=SWING_LEFT, right=SWING_RIGHT):
    swings = []
    n = len(candles)
    for i in range(left, n - right):
        hi = candles[i]["high"]
        lo = candles[i]["low"]
        if all(candles[j]["high"] < hi for j in range(i - left, i)) and \
           all(candles[j]["high"] < hi for j in range(i + 1, i + right + 1)):
            swings.append(Swing(i, candles[i]["time"], hi, "H"))
        if all(candles[j]["low"] > lo for j in range(i - left, i)) and \
           all(candles[j]["low"] > lo for j in range(i + 1, i + right + 1)):
            swings.append(Swing(i, candles[i]["time"], lo, "L"))
    swings.sort(key=lambda s: s.index)
    return swings


def classify_swings(swings):
    """Label swings as HH/HL/LH/LL relative to the prior same-kind swing."""
    last_h = None
    last_l = None
    labeled = []
    for s in swings:
        if s.kind == "H":
            tag = "HH" if (last_h is None or s.price > last_h.price) else "LH"
            last_h = s
        else:
            tag = "LL" if (last_l is None or s.price < last_l.price) else "HL"
            last_l = s
        labeled.append((s, tag))
    return labeled


def current_trend(labeled_swings, lookback=6):
    """Look at the last N labeled swings; recency-weighted bias = trend.

    Weighting: the last 3 swings count 2x, swings 4-6 count 1x. This prevents
    a fresh impulsive reversal (where only 1-2 new opposite-direction swings
    have printed) from being outvoted by an older cluster of same-direction
    swings still inside the lookback window.
    """
    if not labeled_swings:
        return "undefined"
    recent = labeled_swings[-lookback:]
    bullish = 0.0
    bearish = 0.0
    n = len(recent)
    for i, (_, tag) in enumerate(recent):
        # position from end: 0 = most recent
        from_end = (n - 1) - i
        weight = 2.0 if from_end < 3 else 1.0
        if tag in ("HH", "HL"):
            bullish += weight
        elif tag in ("LH", "LL"):
            bearish += weight
    if bullish > bearish:
        return "up"
    if bearish > bullish:
        return "down"
    return "range"


def detect_bos_choc(candles, labeled_swings):
    """Walk forward through candles; whenever close breaks a prior swing,
    emit BOS (same direction as trend) or CHOC (opposite)."""
    events = []
    trend = None
    pending_h = None
    pending_l = None
    swing_iter = iter(labeled_swings)

    def next_at_or_before(idx):
        return [(s, t) for s, t in labeled_swings if s.index <= idx]

    last_swing_h = None
    last_swing_l = None
    for i, c in enumerate(candles):
        for s, t in labeled_swings:
            if s.index == i:
                if s.kind == "H":
                    last_swing_h = s
                else:
                    last_swing_l = s
                if t in ("HH", "HL"):
                    trend = trend or "up"
                elif t in ("LH", "LL"):
                    trend = trend or "down"
                continue
        close = c["close"]
        if last_swing_h and close > last_swing_h.price and i > last_swing_h.index:
            kind = "BOS" if trend == "up" else "CHOC"
            events.append({"index": i, "time": c["time"], "type": kind,
                           "direction": "up", "broken_price": last_swing_h.price,
                           "broken_at_index": last_swing_h.index})
            trend = "up"
            last_swing_h = None
        if last_swing_l and close < last_swing_l.price and i > last_swing_l.index:
            kind = "BOS" if trend == "down" else "CHOC"
            events.append({"index": i, "time": c["time"], "type": kind,
                           "direction": "down", "broken_price": last_swing_l.price,
                           "broken_at_index": last_swing_l.index})
            trend = "down"
            last_swing_l = None
    return events


def detect_fvgs(candles):
    fvgs = []
    n = len(candles)
    for i in range(2, n):
        c1, c2, c3 = candles[i - 2], candles[i - 1], candles[i]
        if c1["high"] < c3["low"]:
            fvg = {"type": "bullish", "i_start": i - 2, "i_end": i,
                   "time": c2["time"], "low": c1["high"], "high": c3["low"]}
            challenged_idx = None
            for j in range(i + 1, n):
                if candles[j]["low"] <= fvg["high"] and candles[j]["high"] >= fvg["low"]:
                    challenged_idx = j
                    break
            fvg["challenged"] = challenged_idx is not None
            fvg["challenged_at_index"] = challenged_idx
            fvgs.append(fvg)
        if c1["low"] > c3["high"]:
            fvg = {"type": "bearish", "i_start": i - 2, "i_end": i,
                   "time": c2["time"], "low": c3["high"], "high": c1["low"]}
            challenged_idx = None
            for j in range(i + 1, n):
                if candles[j]["low"] <= fvg["high"] and candles[j]["high"] >= fvg["low"]:
                    challenged_idx = j
                    break
            fvg["challenged"] = challenged_idx is not None
            fvg["challenged_at_index"] = challenged_idx
            fvgs.append(fvg)
    return fvgs


def detect_sr_zones(swings, threshold_pct=ZONE_CLUSTER_PCT):
    recent = swings[-SR_SWING_LOOKBACK:]
    highs = sorted([s.price for s in recent if s.kind == "H"])
    lows = sorted([s.price for s in recent if s.kind == "L"])

    def cluster(prices):
        zones = []
        if not prices:
            return zones
        bucket = [prices[0]]
        for p in prices[1:]:
            lo, hi = min(bucket), max(bucket)
            if (max(hi, p) - min(lo, p)) / max(hi, p) <= threshold_pct:
                bucket.append(p)
            else:
                if len(bucket) >= 2:
                    zones.append({"low": min(bucket), "high": max(bucket), "touches": len(bucket)})
                bucket = [p]
        if len(bucket) >= 2:
            zones.append({"low": min(bucket), "high": max(bucket), "touches": len(bucket)})
        return zones

    return {"resistance": cluster(highs), "support": cluster(lows)}


def detect_liquidity_pockets(swings):
    recent = swings[-LIQ_SWING_LOOKBACK:]
    highs = [s for s in recent if s.kind == "H"]
    lows = [s for s in recent if s.kind == "L"]

    def equal_pairs(points):
        pockets = []
        for i in range(len(points)):
            for j in range(i + 1, len(points)):
                a, b = points[i], points[j]
                if b.index - a.index < LIQ_MIN_BARS_APART:
                    continue
                if abs(a.price - b.price) / max(a.price, b.price) <= EQUAL_LEVEL_PCT:
                    pockets.append({"a_index": a.index, "b_index": b.index,
                                    "a_price": a.price, "b_price": b.price,
                                    "level": (a.price + b.price) / 2,
                                    "bars_apart": b.index - a.index})
        pockets.sort(key=lambda p: max(p["a_index"], p["b_index"]), reverse=True)
        return pockets

    return {"ceiling": equal_pairs(highs), "floor": equal_pairs(lows)}


def detect_failed_breakouts(candles, swings):
    failures = []
    for s in swings:
        for j in range(s.index + 1, len(candles)):
            c = candles[j]
            if s.kind == "H" and c["high"] > s.price and c["close"] < s.price:
                failures.append({"swing_index": s.index, "swing_price": s.price,
                                 "fail_index": j, "fail_time": c["time"], "direction": "above"})
                break
            if s.kind == "L" and c["low"] < s.price and c["close"] > s.price:
                failures.append({"swing_index": s.index, "swing_price": s.price,
                                 "fail_index": j, "fail_time": c["time"], "direction": "below"})
                break
    return failures


def _fit_trendline(pts):
    """Fit the best-touch trendline through a list of same-kind swings.

    Tries every (anchor, end) pair among the last 5 same-kind swings and
    returns the line with the most touches. This handles cases where the
    very first swing is an outlier above/below the dominant trendline
    (e.g. a spike that started the move) and a tighter line forms below it.
    """
    if len(pts) < 2:
        return None
    best = None
    for i in range(len(pts)):
        for j in range(i + 1, len(pts)):
            a, b = pts[i], pts[j]
            if b.index == a.index:
                continue
            slope = (b.price - a.price) / (b.index - a.index)
            intercept = a.price - slope * a.index
            touches = 0
            for s in pts:
                proj = slope * s.index + intercept
                if proj == 0:
                    continue
                if abs(s.price - proj) / proj <= TRENDLINE_TOUCH_PCT:
                    touches += 1
            if best is None or touches > best["touches"]:
                best = {
                    "line": {"slope": slope, "intercept": intercept,
                             "from_index": a.index, "to_index": b.index,
                             "from_price": a.price, "to_price": b.price},
                    "touches": touches,
                    "points": [{"i": s.index, "p": s.price} for s in pts],
                }
    return best


def count_trendline_touches(swings, trend, prefer_direction=None):
    """Evaluate trendlines on BOTH swing-highs (resistance) and swing-lows (support)
    and return the dominant one.

    The old logic only looked at one kind based on `trend`, which is a laggy
    label — a fresh impulsive reversal can leave `trend` saying "up" while the
    actually-controlling line is a descending resistance through the recent
    swing highs. This version computes both lines and returns the one with
    more touches. Ties are broken by `prefer_direction` if supplied (e.g. the
    direction of the most recent CHOC), else by the legacy `trend` label.

    Return shape matches the original (line/touches/points) so downstream
    callers don't change. Adds `direction` ("up"|"down") and `alternate` for
    callers that want to see the other line too.
    """
    highs = [s for s in swings if s.kind == "H"][-5:]
    lows  = [s for s in swings if s.kind == "L"][-5:]

    resist_line = _fit_trendline(highs)    # descending if down-trending, flat in range
    support_line = _fit_trendline(lows)    # ascending if up-trending, flat in range

    empty = {"line": None, "touches": 0, "points": []}
    if resist_line is None and support_line is None:
        return {**empty, "direction": None, "alternate": None}

    candidates = []
    if resist_line is not None:
        candidates.append(("down", resist_line))   # swing-high line = down/resistance
    if support_line is not None:
        candidates.append(("up", support_line))    # swing-low line = up/support

    # Sort by touches desc, then by tie-breakers
    def tiebreak_key(item):
        direction, info = item
        touches = info["touches"]
        # higher touches first; then prefer the requested direction; then legacy trend match
        pref_match = 1 if (prefer_direction is not None and direction == prefer_direction) else 0
        legacy_match = 1 if (trend == direction) else 0
        return (touches, pref_match, legacy_match)

    candidates.sort(key=tiebreak_key, reverse=True)
    winner_dir, winner = candidates[0]
    loser = candidates[1][1] if len(candidates) > 1 else None
    loser_dir = candidates[1][0] if len(candidates) > 1 else None

    return {
        "line": winner["line"],
        "touches": winner["touches"],
        "points": winner["points"],
        "direction": winner_dir,
        "alternate": {
            "direction": loser_dir,
            "line": loser["line"] if loser else None,
            "touches": loser["touches"] if loser else 0,
            "points": loser["points"] if loser else [],
        } if loser else None,
    }


def count_waves_since_last_choc(events, swings):
    """Count swings after the most recent CHOC event."""
    chocs = [e for e in events if e["type"] == "CHOC"]
    if not chocs:
        return {"choc": None, "wave_count": 0}
    last_choc = chocs[-1]
    waves = [s for s in swings if s.index >= last_choc["index"]]
    return {"choc": last_choc, "wave_count": len(waves)}


def count_waves_before_last_choc(events, swings):
    """Count swings in the trend that PRECEDED the most recent CHOC.

    That trend runs from the prior CHOC (or start of data) up to the most
    recent CHOC. Used by the FVG strategy to verify the reversal is
    meaningful — the trend being reversed must have had >=5 wave patterns.
    """
    chocs = [e for e in events if e["type"] == "CHOC"]
    if not chocs:
        return {"choc": None, "prior_choc": None, "wave_count": 0}
    last_choc = chocs[-1]
    prior_choc = chocs[-2] if len(chocs) >= 2 else None
    lo = prior_choc["index"] if prior_choc else -1
    hi = last_choc["index"]
    waves = [s for s in swings if lo < s.index < hi]
    return {"choc": last_choc, "prior_choc": prior_choc, "wave_count": len(waves)}


def post_choc_structure(events, swings):
    """After most recent CHOC, find first BOS and the swing point after it."""
    chocs = [e for e in events if e["type"] == "CHOC"]
    if not chocs:
        return None
    last_choc = chocs[-1]
    later_bos = [e for e in events if e["type"] == "BOS" and e["index"] > last_choc["index"]]
    first_bos = later_bos[0] if later_bos else None
    swing_after = None
    if first_bos:
        later_swings = [s for s in swings if s.index > first_bos["index"]]
        if later_swings:
            swing_after = later_swings[0]
    return {"choc": last_choc, "first_bos_after": first_bos,
            "first_swing_after_bos": asdict(swing_after) if swing_after else None}


def build_recommendation(a):
    """Apply Bot 1's entry rules to a single timeframe's analysis.

    Rules (from strategy file):
      - Entry = middle of most recent UNCHALLENGED FVG following a CHOC and newest BOS
      - Direction = trend coming out of the last CHOC
      - That CHOC must follow >=5 waves and the trendline must have >=2 touches
      - SL = bottom of the FVG (for bullish FVG = its low; for bearish FVG = its high)
      - TP = 1:4 R:R from entry
    """
    reasons_for = []
    reasons_against = []

    pc = a["post_choc_structure"]
    if not pc:
        return {"verdict": "NO TRADE", "reason": "no CHOC found in window", "details": None}
    if not pc["first_bos_after"]:
        reasons_against.append("no BOS confirmed after most recent CHOC")
    else:
        reasons_for.append(f"BOS confirmed after CHOC at idx {pc['first_bos_after']['index']}")

    waves = a["wave_count_before_last_choc"]["wave_count"]
    if waves < 5:
        reasons_against.append(f"prior trend had only {waves} waves (need >=5 to qualify the reversal)")
    else:
        reasons_for.append(f"prior trend had {waves} waves before CHOC")

    touches = a["trendline"]["touches"]
    if touches < 2:
        reasons_against.append(f"trendline has {touches} touches (need >=2)")
    else:
        reasons_for.append(f"trendline has {touches} touches")

    direction = pc["choc"]["direction"]
    side = "BUY" if direction == "up" else "SELL"
    expected_fvg_type = "bullish" if side == "BUY" else "bearish"

    fvg = a["fvg_most_recent_unchallenged"]
    if not fvg:
        reasons_against.append("no unchallenged FVG available for entry")
    elif fvg["type"] != expected_fvg_type:
        reasons_against.append(f"most recent unchallenged FVG is {fvg['type']}, "
                               f"does not match {side} bias")
    else:
        if pc["first_bos_after"] and fvg["i_end"] < pc["first_bos_after"]["index"]:
            reasons_against.append("unchallenged FVG predates the post-CHOC BOS")
        else:
            reasons_for.append(f"{fvg['type']} FVG aligns with {side} bias")

    if reasons_against or not fvg:
        return {"verdict": "NO TRADE",
                "side": side,
                "reasons_for": reasons_for,
                "reasons_against": reasons_against,
                "details": None}

    entry = (fvg["high"] + fvg["low"]) / 2
    if side == "BUY":
        sl = fvg["low"]
        risk = entry - sl
        tp = entry + 4 * risk
    else:
        sl = fvg["high"]
        risk = sl - entry
        tp = entry - 4 * risk

    last = a["last_price"]
    distance_pct = abs(last - entry) / entry * 100

    return {
        "verdict": "TRADE",
        "side": side,
        "entry": entry,
        "stop_loss": sl,
        "take_profit": tp,
        "risk_per_unit": risk,
        "rr": 4.0,
        "fvg": {"type": fvg["type"], "low": fvg["low"], "high": fvg["high"]},
        "current_price": last,
        "distance_to_entry_pct": distance_pct,
        "reasons_for": reasons_for,
        "reasons_against": reasons_against,
    }


def build_recommendation_trendline(a):
    """Strategy 2 — continuation: ride a trend already in progress.

    Rules:
      - There must be a post-CHOC BOS confirming the trend.
      - >=3 trendline touches (current strategy already counts these).
      - <=3 waves since the most recent CHOC (early in the new trend).
      - Entry = projected trendline price at the most recent bar.
      - SL = beyond the last opposite-side swing (gives the trendline room).
      - TP = 1:4 R:R.
    """
    reasons_for = []
    reasons_against = []

    pc = a["post_choc_structure"]
    if not pc:
        return {"verdict": "NO TRADE", "strategy": "trendline",
                "reason": "no CHOC found in window", "details": None}
    if not pc["first_bos_after"]:
        reasons_against.append("no BOS confirmed after most recent CHOC")
    else:
        reasons_for.append("post-CHOC BOS confirms trend")

    waves = a["wave_count_since_last_choc"]["wave_count"]
    if waves > 3:
        reasons_against.append(f"{waves} waves since CHOC (need <=3 for fresh continuation)")
    else:
        reasons_for.append(f"only {waves} waves since CHOC (fresh trend)")

    tl = a["trendline"]
    if not tl["line"] or tl["touches"] < 2:
        reasons_against.append(f"trendline has {tl['touches']} touches (need >=2)")
        return {"verdict": "NO TRADE", "strategy": "trendline",
                "reasons_for": reasons_for, "reasons_against": reasons_against,
                "details": None}
    reasons_for.append(f"trendline has {tl['touches']} touches")

    direction = pc["choc"]["direction"]
    side = "BUY" if direction == "up" else "SELL"

    last_idx = a["candle_count"] - 1
    slope = tl["line"]["slope"]
    intercept = tl["line"]["intercept"]
    entry = slope * last_idx + intercept

    last_price = a["last_price"]
    if side == "BUY" and last_price < entry:
        reasons_against.append("price already below trendline (broken)")
    if side == "SELL" and last_price > entry:
        reasons_against.append("price already above trendline (broken)")

    swing_lows = [s["p"] for s in a["labeled_swings_tail"] if s["kind"] == "L"]
    swing_highs = [s["p"] for s in a["labeled_swings_tail"] if s["kind"] == "H"]
    if side == "BUY":
        if not swing_lows:
            reasons_against.append("no recent swing low for SL")
        else:
            sl = min(swing_lows)
            if sl >= entry:
                reasons_against.append("SL swing low is above entry (invalid)")
    else:
        if not swing_highs:
            reasons_against.append("no recent swing high for SL")
        else:
            sl = max(swing_highs)
            if sl <= entry:
                reasons_against.append("SL swing high is below entry (invalid)")

    if reasons_against:
        return {"verdict": "NO TRADE", "strategy": "trendline", "side": side,
                "reasons_for": reasons_for, "reasons_against": reasons_against,
                "details": None}

    if side == "BUY":
        risk = entry - sl
        tp = entry + 4 * risk
    else:
        risk = sl - entry
        tp = entry - 4 * risk

    distance_pct = abs(last_price - entry) / entry * 100
    return {
        "verdict": "TRADE",
        "strategy": "trendline",
        "side": side,
        "entry": entry,
        "stop_loss": sl,
        "take_profit": tp,
        "risk_per_unit": risk,
        "rr": 4.0,
        "trendline": tl["line"],
        "current_price": last_price,
        "distance_to_entry_pct": distance_pct,
        "reasons_for": reasons_for,
        "reasons_against": reasons_against,
    }


def build_recommendation_combined(a, strategy="both"):
    """Dispatch to one or both strategies. Returns list of recommendations.

    Also stamps `waves_since_choc` on each rec so the formatter can label
    the trade type as Reversion (catching the flip, <=2 waves since CHOC)
    or Momentum (riding it, >=3 waves since CHOC).
    """
    waves_since = a.get("wave_count_since_last_choc", {}).get("wave_count", 0)
    out = []
    if strategy in ("fvg", "both"):
        r = build_recommendation(a)
        r["strategy"] = "fvg"
        r["waves_since_choc"] = waves_since
        out.append(r)
    if strategy in ("trendline", "both"):
        r = build_recommendation_trendline(a)
        r["waves_since_choc"] = waves_since
        out.append(r)
    return out


def score_signal(rec_15m, rec_5m, a_15m, a_5m):
    """Score a multi-timeframe trade signal 0-100 with per-component breakdown.

    Should only be called when both rec_15m and rec_5m have verdict == 'TRADE'
    and matching sides. Returns full transparency on WHY the score is what it is.

    Returns dict:
      simple_score:   "N/M conditions met (XX%)"   <-- 15m primary, counts gates
      weighted_score: int 0-100                    <-- weighted sum below
      verdict:        "HIGH" / "OK" / "WEAK"
      components:     dict of name -> {points, max, note}
      timeframes_used: ["15m", "5m"]

    Weight table (sum = 100):
       25  multi-TF agreement (must be true to even call this function)
       25  all required gates passed on 15m primary
       15  trendline touches (3 pts each, capped at 15 / 5+ touches)
       10  prior trend depth before CHOC (FVG strategy only, n/a for trendline)
       10  trend freshness (waves since CHOC, sweet spot 1-3)
       10  FVG quality (entry distance + gap width, FVG strategy only)
        5  clean path to TP (no opposing FVGs ahead)
    """
    components = {}

    # 1. Multi-TF agreement (25 pts)
    tf_agree = (rec_15m.get("verdict") == "TRADE"
                and rec_5m.get("verdict") == "TRADE"
                and rec_15m.get("side") == rec_5m.get("side"))
    components["tf_agreement"] = {
        "points": 25 if tf_agree else 0,
        "max": 25,
        "note": ("15m and 5m both fired same direction" if tf_agree
                 else "timeframes disagree — should not have been scored"),
    }

    # 2. All gates met on 15m (25 pts)
    reasons_for_15 = rec_15m.get("reasons_for", [])
    reasons_against_15 = rec_15m.get("reasons_against", [])
    all_met = len(reasons_against_15) == 0 and len(reasons_for_15) > 0
    components["all_gates_met"] = {
        "points": 25 if all_met else 0,
        "max": 25,
        "note": (f"all {len(reasons_for_15)} conditions met on 15m" if all_met
                 else f"{len(reasons_against_15)} failed: " + "; ".join(reasons_against_15)),
    }

    # 3. Trendline touches (15 pts max) — 3 pts per touch, cap at 5 touches
    touches = a_15m.get("trendline", {}).get("touches", 0)
    tl_pts = min(15, max(0, touches * 3))
    components["trendline_touches"] = {
        "points": tl_pts,
        "max": 15,
        "note": f"{touches} touches on 15m trendline",
    }

    # 4. Prior trend depth (10 pts max) — FVG strategy only
    waves_prior = a_15m.get("wave_count_before_last_choc", {}).get("wave_count", 0)
    if rec_15m.get("strategy") == "fvg":
        prior_pts = min(10, max(0, (waves_prior - 4) * 2)) if waves_prior >= 5 else 0
        prior_note = f"{waves_prior} waves before CHOC (need >=5, sweet spot 7+)"
    else:
        prior_pts = 10
        prior_note = "n/a for trendline strategy (full credit)"
    components["prior_trend_depth"] = {
        "points": prior_pts,
        "max": 10,
        "note": prior_note,
    }

    # 5. Trend freshness (10 pts max) — waves since CHOC
    waves_since = rec_15m.get("waves_since_choc", 0)
    if waves_since == 0:
        fresh_pts = 4
    elif 1 <= waves_since <= 3:
        fresh_pts = 10
    elif waves_since == 4:
        fresh_pts = 6
    elif waves_since == 5:
        fresh_pts = 3
    else:
        fresh_pts = 0
    components["trend_freshness"] = {
        "points": fresh_pts,
        "max": 10,
        "note": f"{waves_since} waves since CHOC (sweet spot 1-3)",
    }

    # 6. FVG quality (10 pts max) — FVG strategy only
    if rec_15m.get("strategy") == "fvg" and rec_15m.get("fvg"):
        fvg = rec_15m["fvg"]
        last_price = a_15m.get("last_price", 0) or 1
        entry = rec_15m.get("entry", last_price)
        distance_pct = abs(last_price - entry) / entry * 100 if entry else 100
        fvg_height = abs(fvg["high"] - fvg["low"])
        height_pct = fvg_height / last_price * 100 if last_price else 100

        # Entry proximity sub-score (max 6)
        if distance_pct < 0.5:
            dist_pts, dist_note = 6, f"entry {distance_pct:.2f}% away (close)"
        elif distance_pct < 1.0:
            dist_pts, dist_note = 4, f"entry {distance_pct:.2f}% away (moderate)"
        elif distance_pct < 2.0:
            dist_pts, dist_note = 2, f"entry {distance_pct:.2f}% away (far)"
        else:
            dist_pts, dist_note = 0, f"entry {distance_pct:.2f}% away (very far)"

        # FVG tightness sub-score (max 4)
        if height_pct < 0.5:
            height_pts, height_note = 4, f"tight FVG ({height_pct:.2f}%)"
        elif height_pct < 1.0:
            height_pts, height_note = 2, f"normal FVG ({height_pct:.2f}%)"
        else:
            height_pts, height_note = 0, f"wide FVG ({height_pct:.2f}%)"

        fvg_pts = min(10, dist_pts + height_pts)
        fvg_note = f"{dist_note}, {height_note}"
    else:
        fvg_pts = 10
        fvg_note = "n/a for trendline strategy (full credit)"
    components["fvg_quality"] = {
        "points": fvg_pts,
        "max": 10,
        "note": fvg_note,
    }

    # 7. Clean path to TP (5 pts max)
    trend_env = a_15m.get("trend_environment", {}) or {}
    opposing_ahead = trend_env.get("opposing_ahead_count", 0)
    if opposing_ahead == 0:
        path_pts, path_note = 5, "no opposing FVGs ahead — clean path"
    elif opposing_ahead == 1:
        path_pts, path_note = 3, "1 opposing FVG ahead — minor obstacle"
    elif opposing_ahead == 2:
        path_pts, path_note = 1, "2 opposing FVGs ahead — choppy path"
    else:
        path_pts, path_note = 0, f"{opposing_ahead} opposing FVGs ahead — congested"
    components["clean_path"] = {
        "points": path_pts,
        "max": 5,
        "note": path_note,
    }

    # Aggregate
    weighted = sum(c["points"] for c in components.values())
    max_total = sum(c["max"] for c in components.values())

    if weighted >= 75:
        verdict = "HIGH"
    elif weighted >= 50:
        verdict = "OK"
    else:
        verdict = "WEAK"

    # Simple score: gate count on 15m
    gates_total = len(reasons_for_15) + len(reasons_against_15)
    gates_met = len(reasons_for_15)
    if gates_total > 0:
        simple_pct = int(round(gates_met / gates_total * 100))
        simple_score = f"{gates_met}/{gates_total} conditions met ({simple_pct}%)"
    else:
        simple_score = "no gate data"

    return {
        "simple_score": simple_score,
        "weighted_score": weighted,
        "max_score": max_total,
        "verdict": verdict,
        "tf_agreement": tf_agree,
        "components": components,
        "timeframes_used": [a_15m.get("timeframe", "15m"), a_5m.get("timeframe", "5m")],
    }


def format_score(score):
    """Pretty-print a score dict for console display."""
    lines = []
    lines.append("=" * 70)
    lines.append(f"SIGNAL CONFIDENCE SCORE")
    lines.append("=" * 70)
    lines.append(f"Timeframes used : {score['timeframes_used'][0]} (primary) + {score['timeframes_used'][1]} (confirm)")
    lines.append(f"Simple score    : {score['simple_score']}")
    lines.append(f"Weighted score  : {score['weighted_score']}/{score['max_score']} — {score['verdict']}")
    lines.append("")
    lines.append("Component breakdown:")
    for name, c in score["components"].items():
        label = name.replace("_", " ")
        bar_filled = int(c["points"] / c["max"] * 10) if c["max"] else 0
        bar = "█" * bar_filled + "░" * (10 - bar_filled)
        lines.append(f"  {label:<22} {bar}  {c['points']:>2}/{c['max']:<2}  {c['note']}")
    return "\n".join(lines)


def format_recommendation(symbol, rec, timeframe_label):
    lines = []
    lines.append("=" * 70)
    strat = rec.get("strategy", "fvg").upper()
    lines.append(f"TRADE RECOMMENDATION [{strat}] ({timeframe_label}) — {symbol}")
    lines.append("=" * 70)
    # Type is determined by how fresh the new (post-CHOC) trend is.
    # <=2 waves since CHOC = catching the flip = Reversion
    # >=3 waves since CHOC = riding the new trend = Momentum
    waves_since = rec.get("waves_since_choc", 0)
    strat_type = "Reversion" if waves_since <= 2 else "Momentum"
    if rec["verdict"] == "NO TRADE":
        lines.append("VERDICT       : DO NOT TRADE")
        lines.append(f"Type          : {strat_type}")
        if rec.get("side"):
            lines.append(f"Direction     : {rec['side']} (conditions not met)")
        if rec.get("reasons_for"):
            lines.append("Conditions met:")
            for r in rec["reasons_for"]:
                lines.append(f"   + {r}")
        if rec.get("reasons_against"):
            lines.append("Conditions failed:")
            for r in rec["reasons_against"]:
                lines.append(f"   - {r}")
        return "\n".join(lines)

    lines.append(f"VERDICT       : TAKE TRADE")
    lines.append(f"Type          : {strat_type}")
    lines.append(f"Direction     : {rec['side']}")
    if rec.get("fvg"):
        lines.append(f"Entry         : {rec['entry']:.6f}   (middle of {rec['fvg']['type']} FVG "
                     f"{rec['fvg']['low']:.6f}..{rec['fvg']['high']:.6f})")
    else:
        lines.append(f"Entry         : {rec['entry']:.6f}   (projected trendline)")
    lines.append(f"Stop Loss     : {rec['stop_loss']:.6f}")
    lines.append(f"Take Profit   : {rec['take_profit']:.6f}   (1:{rec['rr']:.0f} R:R)")
    lines.append(f"Risk / unit   : {rec['risk_per_unit']:.6f}")
    lines.append(f"Current price : {rec['current_price']:.6f}  "
                 f"({rec['distance_to_entry_pct']:.2f}% from entry)")
    lines.append("Conditions met:")
    for r in rec["reasons_for"]:
        lines.append(f"   + {r}")
    if rec["reasons_against"]:
        lines.append("Caveats:")
        for r in rec["reasons_against"]:
            lines.append(f"   - {r}")
    return "\n".join(lines)


def compute_trend_environment(fvgs, post_choc, last_price):
    """Side-aware counts of FVGs in the trade environment.

    Returns counts and lists for:
      - opposing_ahead: unchallenged opposite-type FVGs in the path of the trend
        (resistance for BUY, support for SELL). High count = wall of supply/demand.
      - opposing_violated: opposite-type FVGs that have been challenged AFTER the
        most recent CHOC. High count = trend is eating resistance = strength.
      - supporting_behind: unchallenged same-type FVGs behind price (support
        shelves for BUY, resistance shelves for SELL). These are pullback re-entries.
    """
    if not post_choc or not post_choc.get("choc"):
        return None
    direction = post_choc["choc"]["direction"]
    side = "BUY" if direction == "up" else "SELL"
    opposing = "bearish" if side == "BUY" else "bullish"
    supporting = "bullish" if side == "BUY" else "bearish"
    choc_idx = post_choc["choc"]["index"]

    if side == "BUY":
        opposing_ahead = [f for f in fvgs if f["type"] == opposing
                          and not f["challenged"] and f["low"] > last_price]
        supporting_behind = [f for f in fvgs if f["type"] == supporting
                             and not f["challenged"] and f["high"] < last_price]
    else:
        opposing_ahead = [f for f in fvgs if f["type"] == opposing
                          and not f["challenged"] and f["high"] < last_price]
        supporting_behind = [f for f in fvgs if f["type"] == supporting
                             and not f["challenged"] and f["low"] > last_price]

    opposing_violated = [f for f in fvgs if f["type"] == opposing
                         and f["challenged"] and f["challenged_at_index"] is not None
                         and f["challenged_at_index"] >= choc_idx]

    return {
        "side": side,
        "opposing_ahead_count": len(opposing_ahead),
        "opposing_violated_count": len(opposing_violated),
        "supporting_behind_count": len(supporting_behind),
        "opposing_ahead": sorted(opposing_ahead,
                                 key=lambda f: abs(((f["low"] + f["high"]) / 2) - last_price))[:5],
        "supporting_behind": sorted(supporting_behind,
                                    key=lambda f: abs(((f["low"] + f["high"]) / 2) - last_price))[:5],
    }


def analyze(candles, timeframe_label):
    swings = compute_swings(candles)
    labeled = classify_swings(swings)
    trend = current_trend(labeled)
    events = detect_bos_choc(candles, labeled)
    fvgs = detect_fvgs(candles)
    sr = detect_sr_zones(swings)
    liq = detect_liquidity_pockets(swings)
    fails = detect_failed_breakouts(candles, swings)
    waves = count_waves_since_last_choc(events, swings)
    waves_prior = count_waves_before_last_choc(events, swings)
    post_choc = post_choc_structure(events, swings)
    # Prefer the trendline that aligns with the most recent CHOC direction —
    # that's the structurally relevant line for a continuation trade.
    pref_dir = None
    if post_choc and post_choc.get("choc"):
        pref_dir = post_choc["choc"].get("direction")
    tl = count_trendline_touches(swings, trend, prefer_direction=pref_dir)
    trend_env = compute_trend_environment(fvgs, post_choc, candles[-1]["close"])

    last_c = candles[-1]
    unchallenged_fvgs = [f for f in fvgs if not f["challenged"]]

    return {
        "timeframe": timeframe_label,
        "candle_count": len(candles),
        "last_price": last_c["close"],
        "last_time": last_c["time"],
        "swing_count": len(swings),
        "labeled_swings_tail": [{"i": s.index, "p": s.price, "kind": s.kind, "tag": t}
                                for s, t in labeled[-8:]],
        "trend": trend,
        "events_tail": events[-8:],
        "fvg_total": len(fvgs),
        "fvg_unchallenged": len(unchallenged_fvgs),
        "fvg_most_recent": fvgs[-1] if fvgs else None,
        "fvg_most_recent_unchallenged": unchallenged_fvgs[-1] if unchallenged_fvgs else None,
        "sr_zones": sr,
        "liquidity": liq,
        "failed_breakouts_tail": fails[-5:],
        "trendline": tl,
        "wave_count_since_last_choc": waves,
        "wave_count_before_last_choc": waves_prior,
        "post_choc_structure": post_choc,
        "trend_environment": trend_env,
    }


def format_report(symbol, a15, a5):
    lines = []
    lines.append("=" * 70)
    lines.append(f"BOT 2 — SIGNAL REPORT — {symbol}")
    lines.append(f"Generated: {datetime.now(timezone.utc).isoformat()}  source: Kraken public OHLC")
    lines.append("=" * 70)
    lines.append("")
    for a, label in ((a15, "15m"), (a5, "5m")):
        lines.append(f"=== {label} timeframe  (last price {a['last_price']:.6f}, {a['candle_count']} candles) ===")
        lines.append(f"Trend          : {a['trend'].upper()}")
        lines.append(f"Swings (total) : {a['swing_count']}")
        lines.append("Recent swings  :")
        for s in a["labeled_swings_tail"]:
            lines.append(f"   idx {s['i']:>4}  {s['kind']}  {s['tag']:>2}  @ {s['p']:.6f}")
        if a["trendline"]["line"]:
            tl = a["trendline"]
            lines.append(f"Trendline      : from idx {tl['line']['from_index']} @ {tl['line']['from_price']:.6f}"
                         f" -> idx {tl['line']['to_index']} @ {tl['line']['to_price']:.6f}")
            lines.append(f"Touches        : {tl['touches']} of {len(tl['points'])} candidate points within {TRENDLINE_TOUCH_PCT*100:.2f}%")
        else:
            lines.append("Trendline      : not enough same-kind swings")
        lines.append(f"Waves since last CHOC: {a['wave_count_since_last_choc']['wave_count']}")
        if a["events_tail"]:
            lines.append("Recent BOS/CHOC events:")
            for e in a["events_tail"]:
                lines.append(f"   idx {e['index']:>4}  {e['type']:<4}  {e['direction']}  broke {e['broken_price']:.6f} (swing idx {e['broken_at_index']})")
        else:
            lines.append("Events         : no BOS/CHOC detected in window")

        pc = a["post_choc_structure"]
        if pc:
            ch = pc["choc"]
            lines.append(f"Post-CHOC      : last CHOC at idx {ch['index']} (broke {ch['broken_price']:.6f}, dir {ch['direction']})")
            if pc["first_bos_after"]:
                fb = pc["first_bos_after"]
                lines.append(f"   first BOS after : idx {fb['index']} (broke {fb['broken_price']:.6f}, dir {fb['direction']})")
                if pc["first_swing_after_bos"]:
                    sa = pc["first_swing_after_bos"]
                    lines.append(f"   swing after BOS : idx {sa['index']} {sa['kind']} @ {sa['price']:.6f}")
                    last_idx = a["candle_count"] - 1
                    bars_since = last_idx - sa["index"]
                    lines.append(f"   bars since swing: {bars_since}")
            else:
                lines.append("   no BOS yet after this CHOC")
        else:
            lines.append("Post-CHOC      : no CHOC in window")

        lines.append(f"FVGs           : {a['fvg_total']} total, {a['fvg_unchallenged']} unchallenged")
        if a["fvg_most_recent_unchallenged"]:
            f = a["fvg_most_recent_unchallenged"]
            mid = (f["high"] + f["low"]) / 2
            lines.append(f"   most recent UNCHALLENGED FVG ({f['type']}):")
            lines.append(f"     range : {f['low']:.6f}  ..  {f['high']:.6f}")
            lines.append(f"     mid   : {mid:.6f}    height: {f['high']-f['low']:.6f}")
        if a["fvg_most_recent"] and (not a["fvg_most_recent_unchallenged"] or
                                     a["fvg_most_recent"]["i_end"] != a["fvg_most_recent_unchallenged"]["i_end"]):
            f = a["fvg_most_recent"]
            lines.append(f"   most recent FVG ({f['type']}, {'challenged' if f['challenged'] else 'UNCHALLENGED'}):")
            lines.append(f"     range : {f['low']:.6f}  ..  {f['high']:.6f}")

        if a["sr_zones"]["resistance"]:
            lines.append("Resistance zones (clustered swing highs):")
            for z in a["sr_zones"]["resistance"][-3:]:
                lines.append(f"   {z['low']:.6f} .. {z['high']:.6f}   touches: {z['touches']}")
        if a["sr_zones"]["support"]:
            lines.append("Support zones (clustered swing lows):")
            for z in a["sr_zones"]["support"][-3:]:
                lines.append(f"   {z['low']:.6f} .. {z['high']:.6f}   touches: {z['touches']}")

        if a["liquidity"]["ceiling"]:
            lines.append(f"Liquidity ceiling (equal highs, {len(a['liquidity']['ceiling'])} pair(s)):")
            for p in a["liquidity"]["ceiling"][-3:]:
                lines.append(f"   level ~{p['level']:.6f}  (idx {p['a_index']}+{p['b_index']})")
        if a["liquidity"]["floor"]:
            lines.append(f"Liquidity floor (equal lows, {len(a['liquidity']['floor'])} pair(s)):")
            for p in a["liquidity"]["floor"][-3:]:
                lines.append(f"   level ~{p['level']:.6f}  (idx {p['a_index']}+{p['b_index']})")

        if a["failed_breakouts_tail"]:
            lines.append("Failed breakouts (wick rejections at prior swings):")
            for f in a["failed_breakouts_tail"]:
                lines.append(f"   {f['direction']:>5}  swing @ {f['swing_price']:.6f}  failed at idx {f['fail_index']}")

        env = a.get("trend_environment")
        if env:
            opposing_word = "BEARISH above" if env["side"] == "BUY" else "BULLISH below"
            supporting_word = "BULLISH below" if env["side"] == "BUY" else "BEARISH above"
            warn = "  ⚠️ STACKED" if env["opposing_ahead_count"] >= 2 else ""
            lines.append(f"Trend environment ({env['side']} bias):")
            lines.append(f"   Unchallenged {opposing_word} (resistance) : {env['opposing_ahead_count']}{warn}")
            lines.append(f"   Opposing FVGs violated during trend       : {env['opposing_violated_count']}")
            lines.append(f"   Unchallenged {supporting_word} (support shelves) : {env['supporting_behind_count']}")
            if env["opposing_ahead"]:
                lines.append("   Nearest opposing FVGs (resistance ahead):")
                for f in env["opposing_ahead"][:3]:
                    mid = (f["low"] + f["high"]) / 2
                    lines.append(f"      {f['low']:.6f} .. {f['high']:.6f}  (mid {mid:.6f})")
            if env["supporting_behind"]:
                lines.append("   Nearest supporting FVGs (pullback re-entry shelves):")
                for f in env["supporting_behind"][:3]:
                    mid = (f["low"] + f["high"]) / 2
                    lines.append(f"      {f['low']:.6f} .. {f['high']:.6f}  (mid {mid:.6f})")

        lines.append("")
    return "\n".join(lines)


def format_environment_conflict(a15, a5):
    """Print a warning if one timeframe has stacked opposing FVGs and the other doesn't."""
    e15 = a15.get("trend_environment")
    e5 = a5.get("trend_environment")
    if not e15 or not e5:
        return None
    if e15["side"] != e5["side"]:
        return None
    threshold = 2
    high_15 = e15["opposing_ahead_count"] >= threshold
    high_5 = e5["opposing_ahead_count"] >= threshold
    if high_15 and high_5:
        return (f"⚠️  ENVIRONMENT CONFLICT — BOTH timeframes show stacked opposing FVGs "
                f"(15m={e15['opposing_ahead_count']}, 5m={e5['opposing_ahead_count']}). "
                f"Heavy supply ahead. Veto recommended.")
    if high_15 != high_5:
        worse_tf = "15m" if high_15 else "5m"
        worse_n = e15["opposing_ahead_count"] if high_15 else e5["opposing_ahead_count"]
        return (f"⚠️  ENVIRONMENT CONFLICT — {worse_tf} shows {worse_n} unchallenged opposing FVGs ahead "
                f"while the other timeframe looks clear. Single-TF lane is a trap. Veto recommended.")
    return None


VALID_TIMEFRAMES = ["1m", "5m", "15m", "30m", "1h", "4h", "1d"]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol", nargs="?", default=None,
                    help="symbol to analyse (e.g. BTC/USD); omit for interactive prompt")
    ap.add_argument("--json", metavar="PATH", help="also write structured JSON to this path")
    ap.add_argument("--candles-15m", type=int, default=288,
                    help="number of higher-TF candles (default 288)")
    ap.add_argument("--candles-5m", type=int, default=720,
                    help="number of lower-TF candles (default 720)")
    ap.add_argument("--tf-high", default=None, help="higher timeframe (default 15m)")
    ap.add_argument("--tf-low", default=None, help="lower timeframe (default 5m)")
    ap.add_argument("--strategy", choices=["fvg", "trendline", "both"], default="both",
                    help="which strategy to run (default: both)")
    ap.add_argument("--no-chart", action="store_true",
                    help="skip rendering and opening the chart in the browser")
    ap.add_argument("--chart-out", default=None,
                    help="output HTML path for chart (default <symbol>.html)")
    args = ap.parse_args()

    # --- interactive prompt if no symbol supplied ---
    if args.symbol is None:
        print("=" * 62)
        print("BOT 2 — Signal Filtration Bot")
        print("=" * 62)
        raw = input("Symbol (e.g. BTC/USD, DOGE/USD): ").strip().upper()
        if not raw:
            print("No symbol entered. Aborted.")
            return 1
        args.symbol = raw

        if args.tf_high is None:
            tf_high_raw = input(f"Higher timeframe [{', '.join(VALID_TIMEFRAMES)}] (default 15m): ").strip().lower()
            args.tf_high = tf_high_raw if tf_high_raw in VALID_TIMEFRAMES else "15m"
            if tf_high_raw and tf_high_raw not in VALID_TIMEFRAMES:
                print(f"  '{tf_high_raw}' not recognised — using 15m")

        if args.tf_low is None:
            tf_low_raw = input(f"Lower timeframe [{', '.join(VALID_TIMEFRAMES)}] (default 5m): ").strip().lower()
            args.tf_low = tf_low_raw if tf_low_raw in VALID_TIMEFRAMES else "5m"
            if tf_low_raw and tf_low_raw not in VALID_TIMEFRAMES:
                print(f"  '{tf_low_raw}' not recognised — using 5m")

        strat_raw = input("Strategy [fvg / trendline / both] (default both): ").strip().lower()
        if strat_raw in ("fvg", "trendline", "both"):
            args.strategy = strat_raw

    # apply defaults for non-interactive path
    if args.tf_high is None:
        args.tf_high = "15m"
    if args.tf_low is None:
        args.tf_low = "5m"

    symbol = args.symbol.upper()

    print(f"fetching {symbol} candles from Kraken...")
    c15 = get_candles(symbol, args.tf_high, args.candles_15m)
    c5 = get_candles(symbol, args.tf_low, args.candles_5m)
    print(f"  got {len(c15)} x {args.tf_high}, {len(c5)} x {args.tf_low}")

    a15 = analyze(c15, args.tf_high)
    a5 = analyze(c5, args.tf_low)
    report = format_report(symbol, a15, a5)
    print(report)

    conflict = format_environment_conflict(a15, a5)
    if conflict:
        print(conflict)
        print()

    recs15 = build_recommendation_combined(a15, args.strategy)
    recs5 = build_recommendation_combined(a5, args.strategy)
    for r15, r5 in zip(recs15, recs5):
        print(format_recommendation(symbol, r15, args.tf_high))
        print()
        print(format_recommendation(symbol, r5, args.tf_low))
        print()
        strat = r15.get("strategy", "fvg").upper()
        if r15["verdict"] == "TRADE" and r5["verdict"] == "TRADE" and r15["side"] == r5["side"]:
            print(f">>> [{strat}] FINAL ADVICE: TAKE THE TRADE ({r15['side']}) — {args.tf_high} and {args.tf_low} agree.")
        elif r15["verdict"] == "TRADE":
            print(f">>> [{strat}] FINAL ADVICE: {args.tf_high} says {r15['side']}, {args.tf_low} disagrees. Wait.")
        elif r5["verdict"] == "TRADE":
            print(f">>> [{strat}] FINAL ADVICE: {args.tf_low} says {r5['side']}, {args.tf_high} disagrees. Blocked.")
        else:
            print(f">>> [{strat}] FINAL ADVICE: DO NOT TRADE.")
        print()

    if args.json:
        out = {"symbol": symbol, "generated_utc": datetime.now(timezone.utc).isoformat(),
               "tf15": a15, "tf5": a5,
               "recommendations_15m": recs15, "recommendations_5m": recs5}
        Path(args.json).write_text(json.dumps(out, indent=2, default=str))
        print(f"wrote JSON report to {args.json}")

    if not args.no_chart:
        try:
            from bot2_chart import render as render_chart
            chart_path = Path(args.chart_out) if args.chart_out else Path(symbol.replace("/", "") + ".html")
            print("rendering chart...")
            render_chart(symbol, c15, c5, chart_path, args.tf_high, args.tf_low)
            print(f"wrote chart to {chart_path.resolve()}")
            webbrowser.open(chart_path.resolve().as_uri())
        except ImportError as e:
            print(f"chart skipped: {e} (install plotly to enable: pip install plotly)")
        except Exception as e:
            print(f"chart skipped due to error: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())