"""Try date-ranged and report-based endpoints for trade history."""
import json
from pathlib import Path
from dxtrade_client import DXtradeClient, load_env

env = load_env(Path('.env'))
client = DXtradeClient(env)
client.login()

# Try adding date range params
date_endpoints = [
    client.account_path('/orders?from=2026-05-01T00:00:00Z'),
    client.account_path('/orders?fromDate=2026-05-01&toDate=2026-05-14'),
    client.account_path('/orders?startDate=2026-05-01'),
    client.account_path('/positions?from=2026-05-01T00:00:00Z'),
    client.account_path('/trades?from=2026-05-01T00:00:00Z'),
]

for path in date_endpoints:
    status, data = client.get(path)
    if isinstance(data, dict):
        has_data = {k: len(v) for k, v in data.items() if isinstance(v, list) and len(v) > 0}
        print(f"{status}  {path[-60:]}  has_data={has_data}")
        for k, v in data.items():
            if isinstance(v, list) and len(v) > 0:
                print(f"     first: {json.dumps(v[0])[:400]}")
    else:
        print(f"{status}  {path[-60:]}  raw: {str(data)[:100]}")

# Try the report/statement API - different base
print("\n--- trying report API ---")
report_paths = [
    '/dxreport-web/report',
    '/dxreport-web/statements',
    '/dxsca-web/report',
    '/api/report',
]
for path in report_paths:
    status, data = client.get(path)
    print(f"{status}  {path}  -> {str(data)[:150]}")

# Try posting to marketdata-style endpoint for trade history
print("\n--- trying POST to history-like endpoints ---")
payload = {"account": env["DXTRADE_ACCOUNT"], "from": "2026-05-01T00:00:00Z", "to": "2026-05-14T23:59:59Z"}
for path in ['/dxsca-web/history', '/dxsca-web/tradeHistory', '/dxsca-web/orderHistory']:
    status, data = client.post(path, payload)
    print(f"{status}  {path}  -> {str(data)[:200]}")
