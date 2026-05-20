"""research.py — Research Routine.

Collects per-cycle market data:
  - OHLCV candles (Kraken public, two timeframes)
  - Order book depth / spread (Kraken public)
  - Fear & Greed index (Alternative.me, free)
  - BTC dominance (CoinGecko, free)

Returns a ResearchPacket consumed by signal_engine.py.
"""
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib import request as urlrequest
from urllib.error import URLError

from candles import get_candles

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"


@dataclass
class ResearchPacket:
    timestamp:      datetime
    fear_greed:     dict          # {"value": int, "classification": str}
    btc_dominance:  float         # e.g. 54.3
    candles_high:   dict          # symbol -> list[candle]
    candles_low:    dict          # symbol -> list[candle]
    orderbooks:     dict          # symbol -> {"bid": float, "ask": float, "spread_pct": float}
    fetch_errors:   list          # list of (symbol, error_str)


def _get(url: str, timeout: int = 10) -> dict | None:
    req = urlrequest.Request(url, headers={"User-Agent": UA, "Accept": "application/json"})
    try:
        with urlrequest.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception:
        return None


def fetch_fear_greed() -> dict:
    data = _get("https://api.alternative.me/fng/?limit=1")
    if data and data.get("data"):
        d = data["data"][0]
        return {"value": int(d["value"]), "classification": d["value_classification"]}
    return {"value": -1, "classification": "unavailable"}


def fetch_btc_dominance() -> float:
    data = _get("https://api.coingecko.com/api/v3/global")
    if data and "data" in data:
        return round(data["data"].get("market_cap_percentage", {}).get("btc", 0.0), 2)
    return 0.0


def fetch_orderbook(symbol: str, depth: int = 5) -> dict:
    from candles import SYMBOL_MAP, _kraken_pair
    pair = _kraken_pair(symbol)
    url = f"https://api.kraken.com/0/public/Depth?pair={pair}&count={depth}"
    data = _get(url)
    if not data or data.get("error"):
        return {"bid": 0.0, "ask": 0.0, "spread_pct": 0.0, "bid_depth": 0.0, "ask_depth": 0.0}
    result = data.get("result", {})
    book = next(iter(result.values()), {})
    bids = book.get("bids", [])
    asks = book.get("asks", [])
    if not bids or not asks:
        return {"bid": 0.0, "ask": 0.0, "spread_pct": 0.0, "bid_depth": 0.0, "ask_depth": 0.0}
    best_bid = float(bids[0][0])
    best_ask = float(asks[0][0])
    spread_pct = (best_ask - best_bid) / best_bid * 100 if best_bid > 0 else 0.0
    bid_depth = sum(float(b[1]) for b in bids)
    ask_depth = sum(float(a[1]) for a in asks)
    return {
        "bid": best_bid,
        "ask": best_ask,
        "spread_pct": round(spread_pct, 4),
        "bid_depth": round(bid_depth, 4),
        "ask_depth": round(ask_depth, 4),
    }


def collect(
    symbols: list[str],
    tf_high: str = "15m",
    tf_low: str  = "5m",
    candles_high: int = 288,
    candles_low:  int = 720,
    fetch_books:  bool = True,
    verbose:      bool = True,
) -> ResearchPacket:

    now = datetime.now(timezone.utc)

    if verbose:
        print(f"[research] {now.strftime('%H:%M:%S UTC')} — collecting data for {len(symbols)} symbols")

    # Macro data first (quick)
    fg   = fetch_fear_greed()
    dom  = fetch_btc_dominance()

    if verbose:
        fg_str = f"{fg['value']} ({fg['classification']})" if fg["value"] >= 0 else "unavailable"
        dom_str = f"{dom:.1f}%" if dom else "unavailable"
        print(f"  Fear & Greed: {fg_str}  |  BTC dominance: {dom_str}")

    c_high  = {}
    c_low   = {}
    books   = {}
    errors  = []

    for i, sym in enumerate(symbols, 1):
        if verbose:
            print(f"  [{i:02d}/{len(symbols)}] {sym}...", end=" ", flush=True)
        try:
            c_high[sym] = get_candles(sym, tf_high, candles_high)
            time.sleep(0.25)
            c_low[sym]  = get_candles(sym, tf_low,  candles_low)
            time.sleep(0.25)
            if fetch_books:
                books[sym] = fetch_orderbook(sym)
                time.sleep(0.2)
            if verbose:
                last = c_high[sym][-1]["close"]
                spread = books.get(sym, {}).get("spread_pct", 0)
                spread_str = f"  spread={spread:.3f}%" if fetch_books else ""
                print(f"ok  price={last}{spread_str}")
        except Exception as e:
            errors.append((sym, str(e)))
            if verbose:
                print(f"ERROR: {e}")

    if verbose:
        print(f"  Done. {len(c_high)} symbols loaded, {len(errors)} errors.")

    return ResearchPacket(
        timestamp     = now,
        fear_greed    = fg,
        btc_dominance = dom,
        candles_high  = c_high,
        candles_low   = c_low,
        orderbooks    = books,
        fetch_errors  = errors,
    )
