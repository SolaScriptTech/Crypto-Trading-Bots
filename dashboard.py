"""
Kraken Bot — Live Heartbeat Dashboard v2
Reads bot.log in real time and renders a live terminal UI.
Run in a second terminal alongside bot.py.

Usage:
    python dashboard.py
"""

import os
import re
import time
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
from collections import deque

PST      = ZoneInfo("America/Los_Angeles")
LOG_FILE = "bot.log"

# ── ANSI codes ──────────────────────────────────────────────
R      = "\033[0m"
BOLD   = "\033[1m"
DIM    = "\033[2m"
FW     = "\033[97m"   # bright white
FC     = "\033[96m"   # cyan
FG     = "\033[92m"   # green
FR     = "\033[91m"   # red
FY     = "\033[93m"   # yellow
FB     = "\033[94m"   # blue
FM     = "\033[95m"   # magenta
FGRAY  = "\033[90m"

WIDTH  = 76

def cls():
    os.system("cls" if os.name == "nt" else "clear")

def hline(c="─", col=FGRAY):
    print(f"{col}{c * WIDTH}{R}")

def cpnl(v):
    s = f"${v:+.4f}"
    return f"{FG}{BOLD}{s}{R}" if v > 0 else (f"{FR}{BOLD}{s}{R}" if v < 0 else f"{FGRAY}{s}{R}")

def cpct(v):
    s = f"{v:+.2f}%"
    return f"{FG}{s}{R}" if v > 0 else (f"{FR}{s}{R}" if v < 0 else f"{FGRAY}{s}{R}")

def cscore(v):
    try:
        f = float(v)
        return f"{FG}{BOLD}{v}{R}" if f >= 3.5 else (f"{FY}{v}{R}" if f >= 2.5 else f"{FGRAY}{v}{R}")
    except:
        return v

# ── State ───────────────────────────────────────────────────
class State:
    def __init__(self):
        self.balance        = 0.0
        self.raw_balances   = {}
        self.cum_pnl        = 0.0
        self.wins           = 0
        self.losses         = 0
        self.scan_count     = 0
        self.active         = False
        self.status         = "STARTING"
        self.positions      = {}       # pair → {entry, tsl, tp, pnl_pct, opened, usd}
        self.closed_trades  = deque(maxlen=8)
        self.signals        = deque(maxlen=8)
        self.pair_scores    = {}       # pair → score (from scan lines)
        self.thoughts       = deque(maxlen=14)
        self.errors         = deque(maxlen=6)
        self.last_scan_time = "—"
        self.started_at     = datetime.now(PST)

    def ingest(self, line: str):
        line = line.strip()
        if not line:
            return
        m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+ \[(\w+)\] (.+)", line)
        if not m:
            return
        ts, level, msg = m.group(1)[-8:], m.group(2), m.group(3)

        if level == "ERROR":
            self.errors.append(f"{FGRAY}{ts}{R} {FR}{msg}{R}")

        # Raw balances
        rb = re.search(r"Raw balances: (\{.+\})", msg)
        if rb:
            try:
                import ast
                self.raw_balances = ast.literal_eval(rb.group(1))
            except:
                pass

        # Balance
        bm = re.search(r"balance: \$?([\d.]+)", msg)
        if bm:
            v = float(bm.group(1))
            if v > 0:
                self.balance = v

        bm2 = re.search(r"Balance=\$?([\d.]+)", msg)
        if bm2:
            v = float(bm2.group(1))
            if v > 0:
                self.balance = v

        # Cum PnL
        pm = re.search(r"[Cc]um(?:ulative)? PnL=\$?([\+\-]?[\d.]+)", msg)
        if pm:
            self.cum_pnl = float(pm.group(1))

        # W/L
        wm = re.search(r"(\d+)W / (\d+)L", msg)
        if wm:
            self.wins   = int(wm.group(1))
            self.losses = int(wm.group(2))

        # Scan count + time
        sm = re.search(r"Scans=(\d+)", msg)
        if sm:
            self.scan_count    = int(sm.group(1))
            self.last_scan_time = ts

        # Active session
        if "Active session=True" in msg:
            self.active = True
        if "Active session=False" in msg or "Outside active hours" in msg:
            self.active = False

        # Status
        if "API connected" in msg:
            self.status = "RUNNING"
        if "DRAWDOWN HALT" in msg:
            self.status = "HALTED"
        if "Outside active hours" in msg:
            self.status = "MONITORING"
        if "Shutting down" in msg:
            self.status = "STOPPED"

        # Per-pair score from scan line
        # "  📡 XBTUSD  score=+1.50  signals={...}"
        scan_line = re.match(r"\s*📡 (\w+)\s+score=([+\-\d.]+)", msg)
        if scan_line:
            self.pair_scores[scan_line.group(1)] = scan_line.group(2)

        # Signal detected
        sig = re.match(r"🎯 Signal\s+(\w+)\s+score=([\d.]+)\s+price=([\S]+)", msg)
        if sig:
            self.signals.appendleft({"t": ts, "pair": sig.group(1), "score": sig.group(2), "price": sig.group(3)})

        # BUY entry
        buy = re.match(r"🟢 BUY\s+(\w+)\s+score=([\S]+)\s+\$([\d.]+) @ ([\S]+)", msg)
        if buy:
            pair = buy.group(1)
            ep   = float(buy.group(4))
            self.positions[pair] = {
                "entry":   ep,
                "usd":     float(buy.group(3)),
                "tsl":     ep * (1 - 0.025),
                "tp":      ep * (1 + 0.055),
                "pnl_pct": 0.0,
                "opened":  ts,
            }
            self.status = "TRADING"

        # Position monitor update
        pos_m = re.match(r"\s*📊 (\w+)\s+price=[\S]+\s+PnL=([+\-\d.]+)%\s+TSL=([\S]+)", msg)
        if pos_m:
            pair = pos_m.group(1)
            if pair in self.positions:
                self.positions[pair]["pnl_pct"] = float(pos_m.group(2))
                self.positions[pair]["tsl"]     = float(pos_m.group(3))

        # txid / TP / TSL confirmation
        txid_m = re.match(r"\s*✅ txid=\S+\s+TSL=([\S]+)\s+TP=([\S]+)", msg)
        if txid_m:
            # update last added position
            for pair, pos in reversed(list(self.positions.items())):
                pos["tsl"] = float(txid_m.group(1))
                pos["tp"]  = float(txid_m.group(2))
                break

        # SELL exit
        sell = re.match(r"[✅🔴] SELL (\w+)\s+(\S+)\s+PnL=([+\-\d.]+)%\s+\(\$([\+\-\d.]+)\)", msg)
        if sell:
            pair = sell.group(1)
            self.closed_trades.appendleft({
                "t":       ts,
                "pair":    pair,
                "reason":  sell.group(2),
                "pnl_pct": float(sell.group(3)),
                "pnl_usd": float(sell.group(4)),
            })
            self.positions.pop(pair, None)
            if not self.positions and self.status == "TRADING":
                self.status = "RUNNING"

        # Cumulative from sell line
        cpnl_m = re.search(r"cumulative PnL=\$([\+\-][\d.]+)", msg)
        if cpnl_m:
            self.cum_pnl = float(cpnl_m.group(1))

        # Thought log — skip noisy lines
        skip = ["═", "Capital=", "Pairs=", "Kraken Trading Bot",
                "API connected", "Balance=", "Win/Loss", "Open:", "PST",
                "Raw balances", "trade_log.json", "Shutting down",
                "Outside active hours"]
        if not any(p in msg for p in skip):
            clean = re.sub(r"\033\[[0-9;]*m", "", msg)
            if len(clean) > 2:
                self.thoughts.append(f"{FGRAY}{ts}{R} {msg}")


# ── RENDER ──────────────────────────────────────────────────
def render(s: State):
    cls()
    now    = datetime.now(PST).strftime("%H:%M:%S")
    up_s   = int((datetime.now(PST) - s.started_at).total_seconds())
    h, rem = divmod(up_s, 3600)
    mi, sc = divmod(rem, 60)
    uptime = f"{h:02d}:{mi:02d}:{sc:02d}"

    scol = {
        "RUNNING":    FG,
        "TRADING":    FC,
        "MONITORING": FY,
        "HALTED":     FR,
        "STOPPED":    FR,
        "STARTING":   FGRAY,
    }.get(s.status, FW)

    # ── Header ─────────────────────────────────────────────
    print(f"{FC}{BOLD}{'  ⚡ KRAKEN BOT  —  LIVE DASHBOARD':^{WIDTH}}{R}")
    print(
        f"  {FGRAY}PST {FW}{now}  "
        f"{FGRAY}uptime {FW}{uptime}  "
        f"{FGRAY}status {scol}{BOLD}{s.status}{R}  "
        f"{FGRAY}scans {FW}{s.scan_count}{R}"
    )
    hline("═", FC)

    # ── Stats ──────────────────────────────────────────────
    total = s.wins + s.losses
    wr    = s.wins / total * 100 if total > 0 else 0.0
    sess  = f"{FG}● ACTIVE{R}" if s.active else f"{FY}○ WAITING{R}"
    print(
        f"  {FGRAY}Balance {FW}{BOLD}${s.balance:<8.2f}{R}"
        f"  {FGRAY}Cum PnL {cpnl(s.cum_pnl)}"
        f"  {FGRAY}W/L {FG}{s.wins}{FGRAY}/{FR}{s.losses}{R}"
        f"  {FGRAY}({FW}{wr:.0f}%{FGRAY})"
        f"  {sess}"
    )
    print(f"  {FGRAY}Last scan {FW}{s.last_scan_time}{R}")
    hline()

    # ── Open Positions ─────────────────────────────────────
    print(f"  {FC}{BOLD}OPEN POSITIONS{R}")
    if not s.positions:
        print(f"  {FGRAY}  — none —{R}")
    else:
        print(f"  {FGRAY}{'PAIR':<10} {'ENTRY':>11} {'PnL':>8} {'TSL':>12} {'TP':>12} {'SINCE':>8}{R}")
        for pair, p in s.positions.items():
            print(
                f"  {FW}{BOLD}{pair:<10}{R}"
                f" {FGRAY}{p['entry']:>11.6g}{R}"
                f" {cpct(p['pnl_pct']):>8}"
                f" {FY}{p['tsl']:>12.6g}{R}"
                f" {FG}{p['tp']:>12.6g}{R}"
                f" {FGRAY}{p['opened']:>8}{R}"
            )
    hline()

    # ── Pair Scores ────────────────────────────────────────
    print(f"  {FC}{BOLD}LAST SCAN — PAIR SCORES{R}")
    if not s.pair_scores:
        print(f"  {FGRAY}  — waiting for first scan...{R}")
    else:
        items = sorted(s.pair_scores.items(), key=lambda x: float(x[1]) if x[1] else 0, reverse=True)
        row = ""
        for i, (pair, score) in enumerate(items):
            entry = f"{FW}{pair:<10}{R} {cscore(score):>6}   "
            row  += entry
            if (i + 1) % 4 == 0:
                print(f"  {row}")
                row = ""
        if row:
            print(f"  {row}")
    hline()

    # ── Recent Signals ─────────────────────────────────────
    print(f"  {FC}{BOLD}SIGNALS DETECTED (score ≥ 2.5){R}")
    if not s.signals:
        print(f"  {FGRAY}  — none yet{R}")
    else:
        for sig in list(s.signals)[:5]:
            print(
                f"  {FGRAY}{sig['t']}  "
                f"{FW}{BOLD}{sig['pair']:<10}{R}"
                f"  score {cscore(sig['score'])}"
                f"  {FGRAY}@ {FW}{sig['price']}{R}"
            )
    hline()

    # ── Closed Trades ──────────────────────────────────────
    print(f"  {FC}{BOLD}CLOSED TRADES{R}")
    if not s.closed_trades:
        print(f"  {FGRAY}  — no closed trades yet{R}")
    else:
        print(f"  {FGRAY}{'TIME':>8}  {'PAIR':<10} {'REASON':<16} {'PnL%':>7} {'PnL$':>10}{R}")
        for t in s.closed_trades:
            print(
                f"  {FGRAY}{t['t']:>8}  "
                f"{FW}{BOLD}{t['pair']:<10}{R}"
                f"  {FGRAY}{t['reason']:<16}{R}"
                f"  {cpct(t['pnl_pct']):>7}"
                f"  {cpnl(t['pnl_usd']):>10}"
            )
    hline()

    # ── Activity Log ───────────────────────────────────────
    print(f"  {FC}{BOLD}BOT ACTIVITY LOG{R}")
    thoughts = list(s.thoughts)
    if not thoughts:
        print(f"  {FGRAY}  — waiting for activity...{R}")
    else:
        for line in thoughts[-10:]:
            plain = re.sub(r"\033\[[0-9;]*m", "", line)
            if len(plain) > WIDTH - 2:
                line = line[:WIDTH + 30] + f"…{R}"
            print(f"  {line}")
    hline()

    # ── Errors ─────────────────────────────────────────────
    if s.errors:
        print(f"  {FR}{BOLD}ERRORS{R}")
        for e in s.errors:
            print(f"  {e}")
        hline()

    # ── Raw Balances (debug) ───────────────────────────────
    if s.raw_balances:
        nonzero = {k: v for k, v in s.raw_balances.items() if float(v) > 0}
        if nonzero:
            print(f"  {FGRAY}Kraken balances: {FW}{nonzero}{R}")
            hline()

    print(f"  {FGRAY}Ctrl+C exits dashboard  •  Bot keeps running in the other terminal{R}")


# ── MAIN ────────────────────────────────────────────────────
def main():
    state        = State()
    REFRESH      = 1.5
    last_render  = 0.0

    if not os.path.exists(LOG_FILE):
        print(f"{FY}Waiting for {LOG_FILE}... (is bot.py running?){R}")

    # Seed from existing log
    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                state.ingest(line)
    except FileNotFoundError:
        pass

    try:
        while True:
            try:
                with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
                    f.seek(0, 2)
                    while True:
                        line = f.readline()
                        if line:
                            state.ingest(line)
                        now = time.time()
                        if now - last_render >= REFRESH:
                            render(state)
                            last_render = now
                        if not line:
                            time.sleep(0.3)
            except FileNotFoundError:
                time.sleep(2)
            except Exception:
                time.sleep(2)
    except KeyboardInterrupt:
        cls()
        print(f"\n{FC}Dashboard closed. Bot is still running in the other terminal.{R}\n")
        sys.exit(0)

if __name__ == "__main__":
    main()