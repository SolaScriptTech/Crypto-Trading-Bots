"""Render Bot 2 SMC analysis as an interactive Plotly HTML chart.

Usage:
    python bot2_chart.py BTC/USD
    python bot2_chart.py DOGE/USD --out doge.html --open

Requires: pip install plotly
"""
import argparse
import sys
import webbrowser
from datetime import datetime, timezone
from pathlib import Path

try:
    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
except ImportError:
    print("error: plotly is not installed.\n  install with:  pip install plotly")
    sys.exit(1)

from candles import get_candles
from bot2 import (
    compute_swings, classify_swings, current_trend, detect_bos_choc,
    detect_fvgs, detect_sr_zones, detect_liquidity_pockets,
    detect_failed_breakouts, count_trendline_touches,
)

TAG_COLOR = {"HH": "#16a34a", "HL": "#22c55e", "LH": "#dc2626", "LL": "#b91c1c"}
FVG_COLOR_BULL = "rgba(34,197,94,{a})"
FVG_COLOR_BEAR = "rgba(220,38,38,{a})"
BOS_COLOR = "#0ea5e9"
CHOC_COLOR = "#f59e0b"
SR_RES_COLOR = "rgba(220,38,38,0.10)"
SR_SUP_COLOR = "rgba(34,197,94,0.10)"
LIQ_CEIL_COLOR = "#dc2626"
LIQ_FLOOR_COLOR = "#16a34a"
TRENDLINE_COLOR = "#a855f7"


def to_dt(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)


def add_candles(fig, candles, row, name):
    times = [to_dt(c["time"]) for c in candles]
    fig.add_trace(go.Candlestick(
        x=times,
        open=[c["open"] for c in candles],
        high=[c["high"] for c in candles],
        low=[c["low"] for c in candles],
        close=[c["close"] for c in candles],
        name=name,
        increasing_line_color="#16a34a",
        decreasing_line_color="#dc2626",
        showlegend=False,
    ), row=row, col=1)


def add_swings(fig, candles, labeled, row):
    for s, tag in labeled:
        t = to_dt(candles[s.index]["time"])
        color = TAG_COLOR.get(tag, "#888")
        offset = 1.0015 if s.kind == "H" else 0.9985
        fig.add_trace(go.Scatter(
            x=[t], y=[s.price * offset],
            mode="text",
            text=[tag],
            textfont=dict(color=color, size=10, family="monospace"),
            showlegend=False,
            hoverinfo="text",
            hovertext=f"{tag} @ {s.price:.6f}  idx {s.index}",
        ), row=row, col=1)


def add_bos_choc(fig, candles, events, row):
    n = len(candles)
    for e in events:
        if e["index"] >= n:
            continue
        t_break = to_dt(candles[e["broken_at_index"]]["time"])
        t_event = to_dt(candles[e["index"]]["time"])
        color = BOS_COLOR if e["type"] == "BOS" else CHOC_COLOR
        fig.add_shape(
            type="line",
            x0=t_break, x1=t_event,
            y0=e["broken_price"], y1=e["broken_price"],
            line=dict(color=color, width=1.5, dash="dot"),
            row=row, col=1,
        )
        fig.add_trace(go.Scatter(
            x=[t_event], y=[e["broken_price"]],
            mode="markers+text",
            marker=dict(symbol="triangle-down" if e["direction"] == "down" else "triangle-up",
                        size=10, color=color),
            text=[e["type"]],
            textposition="top center" if e["direction"] == "up" else "bottom center",
            textfont=dict(color=color, size=9),
            showlegend=False,
            hoverinfo="text",
            hovertext=f"{e['type']} {e['direction']} broke {e['broken_price']:.6f}",
        ), row=row, col=1)


def add_fvgs(fig, candles, fvgs, row, max_show=30):
    """Draw FVG rectangles. Unchallenged FVGs extend to last candle; challenged stop at challenge."""
    n = len(candles)
    last_t = to_dt(candles[-1]["time"])
    recent = fvgs[-max_show:]
    for f in recent:
        if f["i_start"] >= n:
            continue
        t0 = to_dt(candles[f["i_start"]]["time"])
        if f["challenged"] and f["challenged_at_index"] is not None and f["challenged_at_index"] < n:
            t1 = to_dt(candles[f["challenged_at_index"]]["time"])
            alpha = 0.08
        else:
            t1 = last_t
            alpha = 0.28
        color_tpl = FVG_COLOR_BULL if f["type"] == "bullish" else FVG_COLOR_BEAR
        fillcolor = color_tpl.format(a=alpha)
        fig.add_shape(
            type="rect",
            x0=t0, x1=t1,
            y0=f["low"], y1=f["high"],
            fillcolor=fillcolor,
            line=dict(width=0),
            layer="below",
            row=row, col=1,
        )


def add_sr_zones(fig, candles, sr, row):
    first_t = to_dt(candles[0]["time"])
    last_t = to_dt(candles[-1]["time"])
    for z in sr["resistance"]:
        fig.add_shape(type="rect", x0=first_t, x1=last_t,
                      y0=z["low"], y1=z["high"],
                      fillcolor=SR_RES_COLOR, line=dict(width=0),
                      layer="below", row=row, col=1)
    for z in sr["support"]:
        fig.add_shape(type="rect", x0=first_t, x1=last_t,
                      y0=z["low"], y1=z["high"],
                      fillcolor=SR_SUP_COLOR, line=dict(width=0),
                      layer="below", row=row, col=1)


def add_liquidity(fig, candles, liq, row, max_show=8):
    first_t = to_dt(candles[0]["time"])
    last_t = to_dt(candles[-1]["time"])
    for p in liq["ceiling"][:max_show]:
        fig.add_shape(type="line", x0=first_t, x1=last_t,
                      y0=p["level"], y1=p["level"],
                      line=dict(color=LIQ_CEIL_COLOR, width=1, dash="dash"),
                      row=row, col=1)
        fig.add_annotation(x=last_t, y=p["level"],
                           text=f"  LIQ {p['level']:.4f}",
                           xanchor="left", showarrow=False,
                           font=dict(color=LIQ_CEIL_COLOR, size=9),
                           row=row, col=1)
    for p in liq["floor"][:max_show]:
        fig.add_shape(type="line", x0=first_t, x1=last_t,
                      y0=p["level"], y1=p["level"],
                      line=dict(color=LIQ_FLOOR_COLOR, width=1, dash="dash"),
                      row=row, col=1)
        fig.add_annotation(x=last_t, y=p["level"],
                           text=f"  LIQ {p['level']:.4f}",
                           xanchor="left", showarrow=False,
                           font=dict(color=LIQ_FLOOR_COLOR, size=9),
                           row=row, col=1)


def add_trendline(fig, candles, tl, row):
    line = tl.get("line")
    if not line:
        return
    t0 = to_dt(candles[line["from_index"]]["time"])
    t1 = to_dt(candles[line["to_index"]]["time"])
    fig.add_shape(type="line",
                  x0=t0, x1=t1,
                  y0=line["from_price"], y1=line["to_price"],
                  line=dict(color=TRENDLINE_COLOR, width=2),
                  row=row, col=1)
    fig.add_annotation(x=t1, y=line["to_price"],
                       text=f"  TL ({tl['touches']} touches)",
                       xanchor="left", showarrow=False,
                       font=dict(color=TRENDLINE_COLOR, size=10),
                       row=row, col=1)


def render(symbol, c15, c5, out_path: Path, tf_high="15m", tf_low="5m"):
    a15 = {
        "candles": c15,
        "swings": compute_swings(c15),
    }
    a15["labeled"] = classify_swings(a15["swings"])
    a15["events"] = detect_bos_choc(c15, a15["labeled"])
    a15["fvgs"] = detect_fvgs(c15)
    a15["sr"] = detect_sr_zones(a15["swings"])
    a15["liq"] = detect_liquidity_pockets(a15["swings"])
    a15["tl"] = count_trendline_touches(a15["swings"], current_trend(a15["labeled"]))
    a15["trend"] = current_trend(a15["labeled"])

    a5 = {
        "candles": c5,
        "swings": compute_swings(c5),
    }
    a5["labeled"] = classify_swings(a5["swings"])
    a5["events"] = detect_bos_choc(c5, a5["labeled"])
    a5["fvgs"] = detect_fvgs(c5)
    a5["sr"] = detect_sr_zones(a5["swings"])
    a5["liq"] = detect_liquidity_pockets(a5["swings"])
    a5["tl"] = count_trendline_touches(a5["swings"], current_trend(a5["labeled"]))
    a5["trend"] = current_trend(a5["labeled"])

    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=False,
        vertical_spacing=0.06,
        subplot_titles=(
            f"{symbol} — {tf_high}  (trend {a15['trend'].upper()})",
            f"{symbol} — {tf_low}  (trend {a5['trend'].upper()})",
        ),
    )

    for a, row in ((a15, 1), (a5, 2)):
        add_sr_zones(fig, a["candles"], a["sr"], row)
        add_fvgs(fig, a["candles"], a["fvgs"], row)
        add_candles(fig, a["candles"], row, f"{symbol} candles")
        add_swings(fig, a["candles"], a["labeled"], row)
        add_bos_choc(fig, a["candles"], a["events"], row)
        add_liquidity(fig, a["candles"], a["liq"], row)
        add_trendline(fig, a["candles"], a["tl"], row)

    fig.update_layout(
        height=1100,
        template="plotly_dark",
        showlegend=False,
        title=dict(text=f"{symbol} — SMC Report  ({datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')})"),
        margin=dict(l=40, r=120, t=80, b=40),
    )
    for r in (1, 2):
        fig.update_xaxes(rangeslider_visible=False, row=r, col=1)
        fig.update_yaxes(autorange=True, fixedrange=False, row=r, col=1)

    fig.write_html(str(out_path), include_plotlyjs="cdn")
    return out_path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("symbol")
    ap.add_argument("--out", default=None, help="output HTML path (default <symbol>.html)")
    ap.add_argument("--candles-15m", type=int, default=288)
    ap.add_argument("--candles-5m", type=int, default=720)
    ap.add_argument("--open", action="store_true", help="open the file in the default browser when done")
    args = ap.parse_args()

    symbol = args.symbol.upper()
    out = Path(args.out) if args.out else Path(symbol.replace("/", "") + ".html")

    print(f"fetching {symbol} candles from Kraken...")
    c15 = get_candles(symbol, "15m", args.candles_15m)
    c5 = get_candles(symbol, "5m", args.candles_5m)
    print(f"  got {len(c15)} x 15m, {len(c5)} x 5m")
    print("rendering chart...")
    render(symbol, c15, c5, out)
    print(f"wrote {out.resolve()}")
    if args.open:
        webbrowser.open(out.resolve().as_uri())
    return 0


if __name__ == "__main__":
    sys.exit(main())
