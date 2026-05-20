"""trailing_stop.py — Dynamic trailing stop manager for open Bot 1 positions.

Run after bot1.py places a live order. Connects to DXtrade via WebSocket,
monitors the position in real-time, and tightens the stop loss progressively.

Phases:
  0  Watching   — price has not reached 3R profit yet
  1  Breakeven  — 3R hit, SL moved to true breakeven (entry + fees + slippage)
  2  Trailing   — trail starts at 100% of original SL distance, tightens 7%
                  per new price extreme, accelerates near FVG via proximity²
                  curve (up to 27% at FVG). Floor: 10% of original SL distance.

Usage:
    pip install websockets
    python trailing_stop.py \\
        --symbol MANA/USD \\
        --side short \\
        --entry 0.098090 \\
        --sl-dist 0.001236 \\
        --sl-client-id b1s1778633096639 \\
        --lot 202238 \\
        --fvg-target 0.093145
"""

import argparse
import asyncio
import json
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import request as urlreq
from urllib.error import HTTPError
from urllib.parse import quote

try:
    import websockets
except ImportError:
    raise SystemExit("Install websockets first:  pip install websockets")

from dxtrade_client import load_env, DXtradeClient

HERE = Path(__file__).parent

# ── Trailing stop constants ──────────────────────────────────────────────────
BREAKEVEN_R      = 3.0    # R profit required before breakeven move
BASE_TIGHTEN     = 0.07   # 7% tighter per new extreme (baseline)
MAX_TIGHTEN      = 0.27   # 27% tighter at full FVG proximity
FLOOR_PCT        = 0.10   # minimum trail = 10% of original SL distance
SLIPPAGE_TICKS   = 3      # ticks of slippage buffer in breakeven calc
FEE_PER_SIDE     = 0.0004 # Tradeify taker 0.04%
MIN_SL_MOVE      = 0.000010  # minimum SL improvement before firing a PUT
PING_INTERVAL    = 30     # WebSocket keepalive interval (seconds)
# ─────────────────────────────────────────────────────────────────────────────


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def req_id() -> str:
    return str(int(time.time() * 1000))


class TrailingStopManager:

    def __init__(self, args, env: dict, token: str):
        self.symbol          = args.symbol.upper()
        self.side            = args.side.lower()
        self.entry           = args.entry
        self.sl_dist         = args.sl_dist
        self.sl_client_id    = args.sl_client_id
        self.lot             = args.lot
        self.fvg_target      = args.fvg_target
        self.price_increment = args.price_increment
        self.account         = env["DXTRADE_ACCOUNT"]
        self.token           = token
        self.rest_host       = env["DXTRADE_HOST"].rstrip("/")
        self.ws_host         = (
            self.rest_host
            .replace("https://", "wss://")
            .replace("http://", "ws://")
        )

        # Derived
        self.fee_per_unit = self.entry * FEE_PER_SIDE * 2  # round-trip per unit
        self.slippage     = SLIPPAGE_TICKS * self.price_increment

        # State
        self.phase            = 0
        self.current_sl       = (
            self.entry + self.sl_dist if self.side == "short"
            else self.entry - self.sl_dist
        )
        self.trail_dist       = self.sl_dist
        self.watermark        = None
        self.filled           = False
        self.stop_order_code  = None
        self.position_code    = None
        self._price           = None
        self._lock            = asyncio.Lock()

    # ── Price helpers ────────────────────────────────────────────────────────

    def _unrealized_r(self) -> float:
        if self._price is None:
            return 0.0
        if self.side == "short":
            return (self.entry - self._price) / self.sl_dist
        return (self._price - self.entry) / self.sl_dist

    def _is_better(self, a: float, b: float) -> bool:
        """True if price a is a more favourable extreme than b."""
        return a < b if self.side == "short" else a > b

    def _breakeven_sl(self) -> float:
        buf = self.fee_per_unit + self.slippage
        return self.entry + buf if self.side == "short" else self.entry - buf

    def _tighten_rate(self) -> float:
        """7% baseline; accelerates near FVG via proximity² curve."""
        if self.fvg_target is None or self._price is None:
            return BASE_TIGHTEN
        total = abs(self.fvg_target - self.entry)
        if total <= 0:
            return BASE_TIGHTEN
        remaining  = abs(self.fvg_target - self._price)
        proximity  = max(0.0, min(1.0, 1.0 - remaining / total))
        return BASE_TIGHTEN + (MAX_TIGHTEN - BASE_TIGHTEN) * (proximity ** 2)

    def _trail_sl(self) -> float | None:
        if self.watermark is None:
            return None
        return (
            self.watermark + self.trail_dist if self.side == "short"
            else self.watermark - self.trail_dist
        )

    def _sl_improves(self, new_sl: float) -> bool:
        """True if new_sl is tighter (better for us) than current."""
        if self.side == "short":
            return new_sl < self.current_sl - MIN_SL_MOVE
        return new_sl > self.current_sl + MIN_SL_MOVE

    # ── Core logic ───────────────────────────────────────────────────────────

    async def on_price(self, bid: float, ask: float):
        price = bid if self.side == "short" else ask
        async with self._lock:
            self._price = price
            if not self.filled:
                return

            r = self._unrealized_r()

            # ── Phase 0 → 1: move to breakeven at 3R ────────────────────────
            if self.phase == 0 and r >= BREAKEVEN_R:
                be = round(self._breakeven_sl(), 6)
                ok = await self._move_sl(be, f"breakeven @ {r:.2f}R")
                if ok:
                    self.phase      = 1
                    self.trail_dist = self.sl_dist
                    self.watermark  = price
                    self.current_sl = be

            # ── Phase 1/2: trail ─────────────────────────────────────────────
            if self.phase >= 1 and self.watermark is not None:
                if self._is_better(price, self.watermark):
                    # New extreme → tighten
                    rate            = self._tighten_rate()
                    floor           = self.sl_dist * FLOOR_PCT
                    self.trail_dist = max(self.trail_dist * (1 - rate), floor)
                    self.watermark  = price
                    self.phase      = 2

                if self.phase == 2:
                    new_sl = self._trail_sl()
                    if new_sl and self._sl_improves(new_sl):
                        new_sl = round(new_sl, 6)
                        ok = await self._move_sl(
                            new_sl,
                            f"trail r={r:.2f}R dist={self.trail_dist:.6f} "
                            f"({self.trail_dist/self.sl_dist*100:.0f}% of orig)"
                        )
                        if ok:
                            self.current_sl = new_sl

    async def on_portfolio(self, portfolio: dict):
        if self.filled:
            return
        for pos in portfolio.get("positions", []):
            if pos.get("symbol") == self.symbol:
                self.filled       = True
                self.position_code = pos.get("positionCode")
                print(
                    f"[{utcnow()}] Fill confirmed — {self.symbol} {self.side.upper()} "
                    f"positionCode={self.position_code}"
                )
                return

    # ── SL modification (REST) ───────────────────────────────────────────────

    async def _move_sl(self, new_price: float, reason: str = "") -> bool:
        print(f"[{utcnow()}] SL → {new_price:.6f}  ({reason})")
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, self._do_modify_sl, new_price)
        except Exception as exc:
            print(f"  SL modify error: {exc}")
            return False

    def _do_modify_sl(self, new_price: float) -> bool:
        """Synchronous: GET open orders → find stop order + ETag → PUT."""
        acct_enc = quote(self.account, safe="")
        base_url = f"{self.rest_host}/dxsca-web/accounts/{acct_enc}/orders"
        auth     = {
            "Authorization": f"DXAPI {self.token}",
            "Accept":        "application/json",
            "User-Agent":    "TradifyBot/1.0",
        }

        # ── GET: find stop order and ETag ────────────────────────────────────
        get_req = urlreq.Request(base_url, headers=auth)
        try:
            with urlreq.urlopen(get_req, timeout=10) as resp:
                etag = resp.headers.get("ETag", "")
                data = json.loads(resp.read())
        except HTTPError as e:
            print(f"  GET orders {e.code}: {e.read().decode(errors='replace')[:200]}")
            return False

        orders     = data.get("orders", [])
        stop_order = next(
            (o for o in orders if o.get("clientOrderId") == self.sl_client_id),
            None,
        )
        if not stop_order:
            print(f"  Stop order {self.sl_client_id} not found — may have triggered already")
            return False

        order_code = stop_order["orderCode"]
        if not self.stop_order_code:
            self.stop_order_code = order_code
        if not self.position_code:
            leg = (stop_order.get("legs") or [{}])[0]
            self.position_code = leg.get("positionCode")

        # ── PUT: update stopPrice ─────────────────────────────────────────────
        body = {
            "orderCode":    order_code,
            "type":         "STOP",
            "instrument":   self.symbol,
            "side":         "BUY" if self.side == "short" else "SELL",
            "stopPrice":    new_price,
            "tif":          "GTC",
            "positionEffect": "CLOSE",
        }
        if self.position_code:
            body["positionCode"] = self.position_code

        put_req = urlreq.Request(
            base_url,
            data=json.dumps(body).encode(),
            headers={
                **auth,
                "Content-Type": "application/json",
                "If-Match":     etag,
            },
            method="PUT",
        )
        try:
            with urlreq.urlopen(put_req, timeout=10) as resp:
                return resp.status in (200, 204)
        except HTTPError as e:
            print(f"  PUT {e.code}: {e.read().decode(errors='replace')[:300]}")
            return False

    # ── WebSocket loops ──────────────────────────────────────────────────────

    async def _run_market_data(self):
        url = f"{self.ws_host}/dxsca-web/md?format=json"
        while True:
            try:
                async with websockets.connect(url, ping_interval=PING_INTERVAL) as ws:
                    print(f"[{utcnow()}] Market data WS connected")
                    await ws.send(json.dumps({
                        "type":      "MarketDataSubscriptionRequest",
                        "requestId": req_id(),
                        "timestamp": utcnow(),
                        "session":   self.token,
                        "payload": {
                            "eventTypes": [{"type": "Quote"}],
                            "symbols":    [self.symbol],
                        },
                    }))
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("type") == "MarketData":
                            for ev in msg.get("payload", {}).get("events", []):
                                if ev.get("type") == "Quote" and ev.get("symbol") == self.symbol:
                                    await self.on_price(float(ev["bid"]), float(ev["ask"]))
                        elif msg.get("type") == "PingRequest":
                            await ws.send(json.dumps({
                                "type":      "Ping",
                                "timestamp": utcnow(),
                                "session":   self.token,
                            }))
            except (websockets.ConnectionClosed, OSError) as exc:
                print(f"[{utcnow()}] Market data WS closed ({exc}), reconnecting in 3s…")
                await asyncio.sleep(3)

    async def _run_portfolio(self):
        url = f"{self.ws_host}/dxsca-web?format=json"
        while True:
            try:
                async with websockets.connect(url, ping_interval=PING_INTERVAL) as ws:
                    print(f"[{utcnow()}] Portfolio WS connected")
                    await ws.send(json.dumps({
                        "type":      "AccountPortfoliosSubscriptionRequest",
                        "requestId": req_id(),
                        "timestamp": utcnow(),
                        "session":   self.token,
                        "payload": {
                            "requestType": "LIST",
                            "accounts":    [self.account],
                        },
                    }))
                    async for raw in ws:
                        msg = json.loads(raw)
                        if msg.get("type") == "AccountPortfolios":
                            for port in msg.get("payload", {}).get("portfolios", []):
                                await self.on_portfolio(port)
                        elif msg.get("type") == "PingRequest":
                            await ws.send(json.dumps({
                                "type":      "Ping",
                                "timestamp": utcnow(),
                                "session":   self.token,
                            }))
            except (websockets.ConnectionClosed, OSError) as exc:
                print(f"[{utcnow()}] Portfolio WS closed ({exc}), reconnecting in 3s…")
                await asyncio.sleep(3)

    async def run(self):
        fvg_str = f"{self.fvg_target:.6f}" if self.fvg_target else "not set"
        print("=" * 62)
        print(f"TRAILING STOP — {self.symbol} {self.side.upper()}")
        print("=" * 62)
        print(f"  Entry        : {self.entry:.6f}")
        print(f"  SL distance  : {self.sl_dist:.6f}  (initial 100% trail)")
        print(f"  Breakeven at : {BREAKEVEN_R}R  ({self._breakeven_sl():.6f})")
        print(f"  FVG target   : {fvg_str}")
        print(f"  Trail floor  : {self.sl_dist * FLOOR_PCT:.6f}  (10% of orig)")
        print(f"  Tighten rate : 7% → 27% (proximity² to FVG)")
        print()
        print("Waiting for fill confirmation…")
        await asyncio.gather(
            self._run_market_data(),
            self._run_portfolio(),
        )


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Dynamic trailing stop manager")
    ap.add_argument("--symbol",          required=True,              help="e.g. MANA/USD")
    ap.add_argument("--side",            required=True,              choices=["long", "short"])
    ap.add_argument("--entry",           required=True, type=float,  help="Entry price")
    ap.add_argument("--sl-dist",         required=True, type=float,  dest="sl_dist",
                    help="Original SL distance from bot1 plan output")
    ap.add_argument("--sl-client-id",    required=True,              dest="sl_client_id",
                    help="clientOrderId of the stop order (b1s... from bot1 JSON output)")
    ap.add_argument("--lot",             required=True, type=float,  help="Lot size from bot1 plan")
    ap.add_argument("--fvg-target",      type=float,    default=None, dest="fvg_target",
                    help="Price of next unchallenged opposing FVG from bot2 output")
    ap.add_argument("--price-increment", type=float,    default=0.000001, dest="price_increment",
                    help="Min price increment for slippage calc (default 0.000001)")
    args = ap.parse_args()

    env    = load_env(HERE / ".env")
    client = DXtradeClient(env)
    client.login()

    mgr = TrailingStopManager(args, env, client.token)
    try:
        asyncio.run(mgr.run())
    except KeyboardInterrupt:
        print("\nTrailing stop manager stopped.")


if __name__ == "__main__":
    main()
