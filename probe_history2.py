"""Probe more DXtrade endpoints for closed trade / position history."""
import json
from pathlib import Path
from dxtrade_client import DXtradeClient, load_env
from urllib.parse import quote

env = load_env(Path('.env'))
client = DXtradeClient(env)
client.login()
acct = env["DXTRADE_ACCOUNT"]

# Try non-account-scoped paths and alternate account paths
endpoints = [
    # Account-scoped variations
    (client.account_path('/positions'), 'acc positions'),
    (client.account_path('/positions?status=CLOSED'), 'acc positions CLOSED'),
    (client.account_path('/positions?type=CLOSED'), 'acc positions type=CLOSED'),
    (client.account_path('/positions/history'), 'acc positions/history'),
    (client.account_path('/orders?finalStatus=true'), 'acc orders finalStatus=true'),
    (client.account_path('/orders?status=CANCELLED,FILLED,EXPIRED'), 'acc orders multi-status'),
    (client.account_path('/cashTransactions'), 'acc cashTransactions'),
    (client.account_path('/statements'), 'acc statements'),
    (client.account_path('/reports'), 'acc reports'),
    # Global (non-account) paths
    ('/dxsca-web/positions', 'global positions'),
    ('/dxsca-web/orders', 'global orders'),
    (f'/dxsca-web/accounts/{quote(acct, safe="")}/closedPositions', 'closedPositions alt'),
    ('/dxsca-web/executions', 'global executions'),
    (f'/dxsca-web/accounts/{quote(acct, safe="")}/executions', 'acc executions'),
]

for path, label in endpoints:
    status, data = client.get(path)
    if isinstance(data, dict):
        keys = list(data.keys())
        has_data = {k: len(v) for k, v in data.items() if isinstance(v, list) and len(v) > 0}
        print(f"{status}  [{label}]  keys={keys}  data={has_data}")
        for k, v in data.items():
            if isinstance(v, list) and len(v) > 0:
                print(f"     first: {json.dumps(v[0], indent=2)[:600]}")
    elif status == 200:
        print(f"{status}  [{label}]  raw: {str(data)[:200]}")
    else:
        print(f"{status}  [{label}]")
