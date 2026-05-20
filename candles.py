"""Generic candle fetcher backed by Kraken public OHLC (no auth needed).

Usage:
    python candles.py BTC/USD 1h 100
    python candles.py DOGE/USD 5m 50
"""
import json
import sys
import time
from urllib import request as urlrequest
from urllib.parse import urlencode

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

INTERVAL_MIN = {
    "1m": 1, "5m": 5, "15m": 15, "30m": 30,
    "1h": 60, "4h": 240, "1d": 1440, "1w": 10080, "15d": 21600,
}

SYMBOL_MAP = {
    "BTC/USD": "XBTUSD",
    "ETH/USD": "ETHUSD",
    "DOGE/USD": "DOGEUSD",
    "LTC/USD": "LTCUSD",
    "XMR/USD": "XMRUSD",
    "SOL/USD": "SOLUSD",
    "ADA/USD": "ADAUSD",
    "AVAX/USD": "AVAXUSD",
    "LINK/USD": "LINKUSD",
    "DOT/USD": "DOTUSD",
    "POL/USD": "POLUSD",
    "TRUMP/USD": "TRUMPUSD",
    "SHIB/USD": "SHIBUSD",
    "XRP/USD": "XRPUSD",
    "ATOM/USD": "ATOMUSD",
    "NEAR/USD": "NEARUSD",
    "FIL/USD": "FILUSD",
    "AAVE/USD": "AAVEUSD",
    "UNI/USD": "UNIUSD",
    "APT/USD": "APTUSD",
    "ARB/USD": "ARBUSD",
    "OP/USD": "OPUSD",
    "INJ/USD": "INJUSD",
    "SUI/USD": "SUIUSD",
    "BCH/USD": "BCHUSD",
    "ETC/USD": "ETCUSD",
    "ALGO/USD": "ALGOUSD",
    "XLM/USD": "XLMUSD",
    "MANA/USD": "MANAUSD",
    "SAND/USD": "SANDUSD",
    "CRV/USD": "CRVUSD",
    "COMP/USD": "COMPUSD",
    "ZEC/USD": "ZECUSD",
    "SUSHI/USD": "SUSHIUSD",
    "BAT/USD": "BATUSD",
    "ICP/USD": "ICPUSD",
    "HBAR/USD": "HBARUSD",
    "LDO/USD": "LDOUSD",
    "TIA/USD": "TIAUSD",
    "GRT/USD": "GRTUSD",
    "IMX/USD": "IMXUSD",
    "SNX/USD": "SNXUSD",
    "FET/USD": "FETUSD",
    "PEPE/USD": "PEPEUSD",
    # Round 4 — added 2026-05-18
    "STX/USD":   "STXUSD",
    "RUNE/USD":  "RUNEUSD",
    "WIF/USD":   "WIFUSD",
    "BONK/USD":  "BONKUSD",
    "JUP/USD":   "JUPUSD",
    "PYTH/USD":  "PYTHUSD",
    "ENS/USD":   "ENSUSD",
    "FLOW/USD":  "FLOWUSD",
    "CHZ/USD":   "CHZUSD",
    "ANKR/USD":  "ANKRUSD",
    "OCEAN/USD": "OCEANUSD",
    "LPT/USD":   "LPTUSD",
    "JASMY/USD": "JASMYUSD",
    "KAVA/USD":  "KAVAUSD",
    "AUDIO/USD": "AUDIOUSD",
    "STORJ/USD": "STORJUSD",
    "DASH/USD":  "DASHUSD",
    "EGLD/USD":  "EGLDUSD",
    "FLR/USD":   "FLRUSD",
    "AXS/USD":   "AXSUSD",
}


def _kraken_pair(symbol: str) -> str:
    if symbol in SYMBOL_MAP:
        return SYMBOL_MAP[symbol]
    return symbol.replace("/", "").replace("BTC", "XBT")


def get_candles(symbol: str, timeframe: str = "1h", count: int = 100) -> list[dict]:
    """Return last `count` OHLCV candles for `symbol` at `timeframe`.

    Returns list of dicts: {time, open, high, low, close, vwap, volume}.
    Newest candle last.
    """
    if timeframe not in INTERVAL_MIN:
        raise ValueError(f"unsupported timeframe {timeframe!r}; pick from {list(INTERVAL_MIN)}")

    pair = _kraken_pair(symbol)
    interval = INTERVAL_MIN[timeframe]
    since = int(time.time()) - (count + 5) * interval * 60

    url = "https://api.kraken.com/0/public/OHLC?" + urlencode({
        "pair": pair, "interval": interval, "since": since,
    })
    req = urlrequest.Request(url, headers={"User-Agent": UA})
    with urlrequest.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    if data.get("error"):
        raise RuntimeError(f"Kraken error for {symbol} -> {pair}: {data['error']}")

    result = data["result"]
    rows = next(v for k, v in result.items() if k != "last")
    out = []
    for r in rows[-count:]:
        out.append({
            "time": int(r[0]),
            "open": float(r[1]),
            "high": float(r[2]),
            "low": float(r[3]),
            "close": float(r[4]),
            "vwap": float(r[5]),
            "volume": float(r[6]),
        })
    return out


def _cli() -> int:
    if len(sys.argv) < 2:
        print("usage: python candles.py SYMBOL [TIMEFRAME] [COUNT]")
        print(f"  timeframes: {list(INTERVAL_MIN)}")
        return 1
    symbol = sys.argv[1].upper()
    timeframe = sys.argv[2] if len(sys.argv) > 2 else "1h"
    count = int(sys.argv[3]) if len(sys.argv) > 3 else 100

    candles = get_candles(symbol, timeframe, count)
    print(f"{symbol} {timeframe} -> {len(candles)} candles via Kraken")
    print(f"first: {candles[0]}")
    print(f"last:  {candles[-1]}")
    last_close = candles[-1]["close"]
    first_close = candles[0]["close"]
    pct = (last_close / first_close - 1) * 100
    print(f"change over window: {pct:+.2f}%")
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
