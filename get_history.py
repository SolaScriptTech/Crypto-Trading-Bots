"""Pull complete trade history + fees using the correct DXtrade endpoints."""
import json
from pathlib import Path
from dxtrade_client import DXtradeClient, load_env

env = load_env(Path('.env'))
client = DXtradeClient(env)
client.login()
print("Logged in OK\n")

# --- Account metrics ---
metrics = client.metrics()
print("=== ACCOUNT METRICS ===")
print(f"  Equity:    ${float(metrics['equity']):,.2f}")
print(f"  Balance:   ${float(metrics['balance']):,.2f}")
print(f"  Total P&L: ${float(metrics['totalPL']):,.2f}")
print(f"  Open P&L:  ${float(metrics['openPL']):,.2f}")
print()

# --- Order history (completed + cancelled + all final states) ---
print("=== ORDER HISTORY (/orders/history) ===")
status, data = client.get(client.account_path('/orders/history?transaction-from=2026-01-01T00:00:00Z'))
print(f"HTTP {status}")
if isinstance(data, dict):
    orders = data.get('orders', [])
    print(f"Orders returned: {len(orders)}")
    if orders:
        for o in orders:
            inst   = o.get('instrument', '?')
            side   = o.get('side', '?')
            otype  = o.get('type', '?')
            ostatus= o.get('status', '?')
            issued = o.get('issueTime', '?')[:19]
            txtime = o.get('transactionTime', '?')[:19]
            legs   = o.get('legs', [{}])
            qty    = legs[0].get('filledQuantity', 0) if legs else 0
            avg    = legs[0].get('averagePrice', 0) if legs else 0
            cash   = o.get('cashTransactions', [])
            pnl    = sum(float(t.get('value', 0)) for t in cash if t.get('type') in ('SETTLEMENT', 'DEPOSIT', 'WITHDRAWAL'))
            fees   = sum(float(t.get('value', 0)) for t in cash if t.get('type') == 'COMMISSION')
            print(f"  {issued} | {inst:<12} {side:<5} {otype:<6} {ostatus:<12} qty={qty} avg={avg} pnl={pnl:+.2f} fees={fees:.4f}")
    else:
        print("  (no orders in response)")
        print("  Raw keys:", list(data.keys()))
        if data.get('nextPageTransactionTime'):
            print("  nextPageTransactionTime:", data['nextPageTransactionTime'])
else:
    print("  Raw:", str(data)[:500])

print()

# --- Cash transfers (commissions, settlements) ---
print("=== CASH TRANSFERS (/transfers) ===")
status2, data2 = client.get(client.account_path('/transfers?transaction-from=2026-01-01T00:00:00Z'))
print(f"HTTP {status2}")
if isinstance(data2, dict):
    transfers = data2.get('cashTransfers', [])
    print(f"Transfers returned: {len(transfers)}")
    total_commission = 0.0
    total_settlement = 0.0
    for t in transfers:
        txtime = t.get('transactionTime', '?')[:19]
        for ct in t.get('cashTransactions', []):
            ctype = ct.get('type', '?')
            val   = float(ct.get('value', 0))
            sym   = ct.get('positionCode', ct.get('orderCode', '?'))
            print(f"  {txtime} | {ctype:<12} {val:+.4f}  ref={sym}")
            if ctype == 'COMMISSION':
                total_commission += val
            elif ctype == 'SETTLEMENT':
                total_settlement += val
    print(f"\n  Total commissions: {total_commission:.4f}")
    print(f"  Total settlements: {total_settlement:.4f}")
    print(f"  Net (settle+comm): {total_settlement + total_commission:.4f}")
else:
    print("  Raw:", str(data2)[:500])
