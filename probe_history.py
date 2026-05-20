"""Probe DXtrade endpoints to find trade/position history."""
import json
from pathlib import Path
from dxtrade_client import DXtradeClient, load_env

env = load_env(Path('.env'))
client = DXtradeClient(env)
client.login()

endpoints = [
    '/orders',
    '/orders?limit=200',
    '/orders?status=CLOSED',
    '/orders?status=CANCELLED',
    '/orders?status=ALL',
    '/closedPositions',
    '/closedPositions?limit=200',
    '/trades',
    '/trades?limit=200',
    '/history',
    '/history/orders',
    '/history/trades',
    '/positions/closed',
    '/deals',
    '/deals?limit=200',
    '/transactions',
]

for ep in endpoints:
    path = client.account_path(ep)
    status, data = client.get(path)
    if isinstance(data, dict):
        keys = list(data.keys())
        # count items in any list values
        counts = {k: len(v) if isinstance(v, list) else v for k, v in data.items() if not isinstance(v, dict)}
        print(f"{status}  {ep:<35} keys={keys}  counts={counts}")
        # if there's actual data, print first item
        for k, v in data.items():
            if isinstance(v, list) and len(v) > 0:
                print(f"       First item: {json.dumps(v[0], indent=2)[:500]}")
    else:
        print(f"{status}  {ep:<35} raw: {str(data)[:100]}")
