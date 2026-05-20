"""Drill into Candle eventType — fromTime is required inside the object."""
import json
from pathlib import Path

from dxtrade_client import DXtradeClient, load_env

HERE = Path(__file__).parent


def show(label, status, body):
    snippet = json.dumps(body)[:1500] if isinstance(body, (dict, list)) else str(body)[:600]
    print(f"[{status}] {label}\n  {snippet}\n")


def main() -> int:
    env = load_env(HERE / ".env")
    c = DXtradeClient(env)
    c.login()

    from_iso = "2026-05-10T00:00:00Z"
    to_iso = "2026-05-12T00:00:00Z"

    bodies = [
        {"symbols": ["BTC/USD"], "eventTypes": [{"type": "Candle", "fromTime": from_iso}]},
        {"symbols": ["BTC/USD"], "eventTypes": [{"type": "Candle", "fromTime": from_iso, "toTime": to_iso}]},
        {"symbols": ["BTC/USD"], "eventTypes": [{"type": "Candle", "fromTime": from_iso, "toTime": to_iso, "candleType": "H1"}]},
        {"symbols": ["BTC/USD"], "eventTypes": [{"type": "Candle", "fromTime": from_iso, "toTime": to_iso, "period": "H1"}]},
        {"symbols": ["BTC/USD"], "eventTypes": [{"type": "Candle", "fromTime": from_iso, "toTime": to_iso, "candleType": "HOUR_1"}]},
        {"symbols": ["BTC/USD"], "eventTypes": [{"type": "Candle", "fromTime": from_iso, "toTime": to_iso, "candleType": "1h"}]},
        {"symbols": ["BTC/USD"], "eventTypes": [{"type": "Candle", "fromTime": from_iso, "toTime": to_iso, "candleType": "1H"}]},
        {"symbols": ["BTC/USD"], "eventTypes": [{"type": "Candle", "fromTime": from_iso, "toTime": to_iso, "candleType": "h1"}]},
    ]
    for b in bodies:
        status, body = c.post("/dxsca-web/marketdata", b)
        show(json.dumps(b), status, body)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
