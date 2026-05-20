from dxtrade_client import DXtradeClient, load_env
from pathlib import Path

env = load_env(Path('.env'))
client = DXtradeClient(env)
client.login()

metrics = client.metrics()
positions = client.positions()

equity  = float(metrics['equity'])
balance = float(metrics['balance'])
open_pl = float(metrics.get('openPL', 0))
day_start = balance - open_pl

hwm         = 100000.0
floor       = hwm - 6000.0
daily_floor = day_start - 3000.0

print("=" * 50)
print("ACCOUNT SNAPSHOT")
print("=" * 50)
print(f"Equity:            ${equity:>10,.2f}")
print(f"Balance:           ${balance:>10,.2f}")
print(f"Open P&L:          ${open_pl:>+10,.2f}")
print()
print(f"Max DD floor:      ${floor:>10,.2f}")
print(f"Max DD remaining:  ${equity - floor:>10,.2f}   {'OK' if equity - floor > 500 else '*** WARNING ***'}")
print(f"Daily DD remaining:${equity - daily_floor:>10,.2f}   {'OK' if equity - daily_floor > 250 else '*** WARNING ***'}")
print()
print(f"Open positions: {metrics['openPositionsCount']}")
print("-" * 50)

total_risk = 0.0
for p in positions:
    entry    = float(p['openPrice'])
    sl       = float(p.get('stopLossPrice') or 0)
    tp       = float(p.get('takeProfitPrice') or 0)
    qty      = float(p['quantity'])
    side     = p['side']
    sym      = p['symbol']
    opened   = p['openTime']

    if sl:
        risk_unit   = abs(sl - entry)
        risk_dollar = risk_unit * qty
        total_risk += risk_dollar
    else:
        risk_dollar = 0
        risk_unit   = 0

    reward_unit   = abs(tp - entry) if tp else 0
    reward_dollar = reward_unit * qty if tp else 0
    rr = reward_unit / risk_unit if risk_unit else 0

    sl_str  = f"SL={sl}" if sl else "NO SL !!!"
    tp_str  = f"TP={tp}" if tp else "no TP"
    print(f"{sym} {side}  entry={entry}  {sl_str}  {tp_str}")
    print(f"  qty={qty:,.0f}  max_risk=${risk_dollar:,.0f}  max_reward=${reward_dollar:,.0f}  R:R=1:{rr:.2f}  opened={opened}")
    print()

print(f"Total max risk if ALL SLs hit: ${total_risk:,.0f}")
print("=" * 50)
