from dxtrade_client import DXtradeClient, load_env
from pathlib import Path
import json
env = load_env(Path('.env'))
client = DXtradeClient(env)
client.login()
status, data = client.get(client.account_path('/orders?status=FILLED&limit=50'))
print(json.dumps(data, indent=2))
