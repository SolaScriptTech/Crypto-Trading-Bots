"""Risk envelope monitor for Tradeify Crypto funded account.

Tracks:
- Max DD floor (HWM ratchets at 22:00 UTC on closed balance; breach checked live on equity).
- Daily DD (loss from day-start balance, $3K hard limit; day resets at 22:00 UTC to match HWM cadence).

Emits an allow/block verdict and distance-to-limit margins. Does NOT trade.
"""
import json
import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

from dxtrade_client import DXtradeClient, load_env

STARTING_BALANCE = 100_000.0
MAX_DD = 6_000.0
DAILY_DD = 3_000.0
BUFFER_MAX_DD = 500.0
BUFFER_DAILY_DD = 250.0

ROLL_HOUR_UTC = 22

HERE = Path(__file__).parent
STATE_PATH = HERE / "state" / "risk_state.json"


def trading_day(now: datetime) -> str:
    """Return ISO date of the current Tradeify trading day (rolls at 22:00 UTC)."""
    anchored = now - timedelta(hours=ROLL_HOUR_UTC)
    return anchored.date().isoformat()


def load_state() -> dict:
    if STATE_PATH.exists():
        return json.loads(STATE_PATH.read_text())
    return {
        "hwm": STARTING_BALANCE,
        "last_ratchet_day": None,
        "day_start_balance": STARTING_BALANCE,
        "day_start_day": None,
    }


def save_state(state: dict) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = STATE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2))
    os.replace(tmp, STATE_PATH)


def evaluate(state: dict, metrics: dict, now: datetime) -> dict:
    equity = float(metrics["equity"])
    balance = float(metrics["balance"])
    open_pl = float(metrics.get("openPL", 0.0))

    today = trading_day(now)

    if state["last_ratchet_day"] != today:
        state["hwm"] = max(state["hwm"], balance)
        state["last_ratchet_day"] = today

    if state["day_start_day"] != today:
        state["day_start_balance"] = balance - open_pl
        state["day_start_day"] = today

    floor = state["hwm"] - MAX_DD
    daily_floor = state["day_start_balance"] - DAILY_DD

    dist_max = equity - floor
    dist_daily = equity - daily_floor

    reasons = []
    if dist_max <= 0:
        reasons.append(f"MAX DD BREACH: equity {equity:.2f} <= floor {floor:.2f}")
    elif dist_max < BUFFER_MAX_DD:
        reasons.append(f"max DD buffer: {dist_max:.2f} < {BUFFER_MAX_DD:.2f}")

    if dist_daily <= 0:
        reasons.append(f"DAILY DD BREACH: equity {equity:.2f} <= daily floor {daily_floor:.2f}")
    elif dist_daily < BUFFER_DAILY_DD:
        reasons.append(f"daily DD buffer: {dist_daily:.2f} < {BUFFER_DAILY_DD:.2f}")

    breached = dist_max <= 0 or dist_daily <= 0
    blocked = breached or bool(reasons)

    return {
        "now_utc": now.isoformat(),
        "trading_day": today,
        "equity": equity,
        "balance": balance,
        "open_pl": open_pl,
        "hwm": state["hwm"],
        "max_dd_floor": floor,
        "max_dd_used": state["hwm"] - equity,
        "max_dd_remaining": dist_max,
        "day_start_balance": state["day_start_balance"],
        "daily_floor": daily_floor,
        "daily_pl": equity - state["day_start_balance"],
        "daily_dd_remaining": dist_daily,
        "verdict": "BREACHED" if breached else ("BLOCKED" if blocked else "ALLOWED"),
        "reasons": reasons,
    }


def main() -> int:
    env = load_env(HERE / ".env")
    client = DXtradeClient(env)
    client.login()
    metrics = client.metrics()
    state = load_state()
    now = datetime.now(timezone.utc)
    verdict = evaluate(state, metrics, now)
    save_state(state)

    print(json.dumps(verdict, indent=2))
    return 0 if verdict["verdict"] != "BREACHED" else 2


if __name__ == "__main__":
    sys.exit(main())
