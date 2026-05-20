"""
order_book.py — OrderBookEngine
Four-check order book gate: imbalance, wall authenticity, liquidity depth,
tape confirmation. Returns CONFIRM / WARN / VETO on every decision.
"""

import time
import collections
from datetime import datetime


class OrderBookEngine:
    def __init__(self, exchange, rate_limiter, symbol='BTC/USD'):
        self.exchange     = exchange
        self.rl           = rate_limiter
        self.symbol       = symbol

        # Rolling history for wall authenticity (keyed by price level)
        # Each entry: {price: {'bid'|'ask', size, cycle_count}}
        self.wall_history  = {}
        self.cycle_count   = 0

        # Rolling liquidity depth for 20-period average
        self.depth_history = collections.deque(maxlen=20)

        # Last snapshot for logging
        self.last_snapshot = {}

    # ─────────────────────────────────────────────────────────
    def fetch(self):
        """Fetch order book and recent trades with rate limiting."""
        book   = self.rl.call(self.exchange.fetch_order_book, self.symbol, 100)
        trades = self.rl.call(self.exchange.fetch_trades,     self.symbol, None, 100)
        return book, trades

    # ─────────────────────────────────────────────────────────
    def _check_imbalance(self, book, mid):
        """
        Check 1: Bid/Ask volume imbalance within 0.5% of mid price.
        Ratio > 1.5 = buy pressure. < 0.67 = sell pressure.
        """
        band   = mid * 0.005
        bids   = sum(sz for px, sz in book['bids'] if px >= mid - band)
        asks   = sum(sz for px, sz in book['asks'] if px <= mid + band)
        ratio  = bids / asks if asks > 0 else 999
        if ratio > 1.5:
            verdict, label = 'CONFIRM', f"buy pressure ({ratio:.2f})"
        elif ratio < 0.67:
            verdict, label = 'WARN',    f"sell pressure ({ratio:.2f})"
        else:
            verdict, label = 'CONFIRM', f"neutral ({ratio:.2f})"
        return verdict, ratio, label

    # ─────────────────────────────────────────────────────────
    def _check_walls(self, book, mid):
        """
        Check 2: Wall authenticity. Large walls that just appeared = spoofing risk.
        A wall must be present for 3+ cycles to be considered authentic.
        """
        self.cycle_count += 1
        band    = mid * 0.02   # look within 2% of mid
        current_walls = {}

        # Find largest bid wall within band
        bid_walls = [(px, sz) for px, sz in book['bids'] if px >= mid - band]
        if bid_walls:
            best_bid = max(bid_walls, key=lambda x: x[1])
            current_walls[round(best_bid[0], 0)] = ('bid', best_bid[1])

        # Find largest ask wall within band
        ask_walls = [(px, sz) for px, sz in book['asks'] if px <= mid + band]
        if ask_walls:
            best_ask = max(ask_walls, key=lambda x: x[1])
            current_walls[round(best_ask[0], 0)] = ('ask', best_ask[1])

        # Update wall history
        new_history = {}
        for price, (side, size) in current_walls.items():
            prev = self.wall_history.get(price)
            if prev:
                new_history[price] = (side, size, prev[2] + 1)  # increment cycle count
            else:
                new_history[price] = (side, size, 1)             # new wall, cycle 1
        self.wall_history = new_history

        # Evaluate
        verdict = 'CONFIRM'
        details = []
        for price, (side, size, cycles) in self.wall_history.items():
            dist_pct = abs(price - mid) / mid * 100
            if cycles < 3 and dist_pct < 0.5:
                # New wall very close to price — spoofing risk
                verdict = 'WARN'
                details.append(
                    f"{'Bid' if side=='bid' else 'Ask'} wall @"
                    f"${price:,.0f}: CAUTION (appeared this cycle)"
                )
            else:
                auth = 'AUTHENTIC' if cycles >= 3 else f'new ({cycles} cycles)'
                details.append(
                    f"{'Bid' if side=='bid' else 'Ask'} wall @"
                    f"${price:,.0f}: {auth}"
                )

        if not details:
            details = ['No significant walls detected']
        return verdict, details

    # ─────────────────────────────────────────────────────────
    def _check_liquidity(self, book, mid):
        """
        Check 3: Liquidity depth vs 20-period rolling average.
        Below 60% of average = rug pull / liquidity withdrawal warning.
        """
        band  = mid * 0.01   # 1% of mid
        depth = (sum(sz for px, sz in book['bids'] if px >= mid - band) +
                 sum(sz for px, sz in book['asks'] if px <= mid + band))
        self.depth_history.append(depth)

        avg = sum(self.depth_history) / len(self.depth_history)
        pct = (depth / avg * 100) if avg > 0 else 100

        if pct < 60:
            verdict = 'VETO'
            label   = f"{pct:.0f}% of avg — LIQUIDITY WITHDRAWAL"
        elif pct < 80:
            verdict = 'WARN'
            label   = f"{pct:.0f}% of avg — thinning"
        else:
            verdict = 'CONFIRM'
            label   = f"{pct:.0f}% of avg (healthy)"
        return verdict, pct, label

    # ─────────────────────────────────────────────────────────
    def _check_tape(self, trades, mid):
        """
        Check 4: Tape confirmation. Are recent trades backing the book?
        Buyers lifting asks = confirmed buying.
        Sellers hitting bids despite apparent buy walls = likely spoofing.
        """
        if not trades:
            return 'CONFIRM', 50.0, 'no tape data'

        recent = trades[-50:]
        buy_vol  = sum(t['amount'] for t in recent if t['side'] == 'buy')
        sell_vol = sum(t['amount'] for t in recent if t['side'] == 'sell')
        total    = buy_vol + sell_vol
        buy_pct  = (buy_vol / total * 100) if total > 0 else 50

        if buy_pct >= 60:
            verdict = 'CONFIRM'
            label   = f"BUYING ({buy_pct:.0f}% of last {len(recent)} trades hit ask)"
        elif buy_pct <= 40:
            verdict = 'WARN'
            label   = f"SELLING ({100-buy_pct:.0f}% of last {len(recent)} trades hit bid)"
        else:
            verdict = 'CONFIRM'
            label   = f"MIXED ({buy_pct:.0f}% buy)"
        return verdict, buy_pct, label

    # ─────────────────────────────────────────────────────────
    def _aggregate_verdict(self, v1, v2, v3, v4):
        """
        Aggregate four check verdicts into one gate output.
        One VETO = VETO. Two or more WARNs = WARN. Otherwise CONFIRM.
        """
        verdicts = [v1, v2, v3, v4]
        if 'VETO' in verdicts:
            return 'VETO'
        if verdicts.count('WARN') >= 2:
            return 'WARN'
        if 'WARN' in verdicts:
            return 'WARN'
        return 'CONFIRM'

    # ─────────────────────────────────────────────────────────
    def evaluate(self):
        """
        Run all four checks. Returns (verdict, snapshot_dict).
        verdict: 'CONFIRM' | 'WARN' | 'VETO'
        """
        try:
            book, trades = self.fetch()
            if not book['bids'] or not book['asks']:
                return 'CONFIRM', {'error': 'empty book — defaulting CONFIRM'}

            mid = (book['bids'][0][0] + book['asks'][0][0]) / 2

            v1, ratio,    imbal_label  = self._check_imbalance(book, mid)
            v2, wall_details           = self._check_walls(book, mid)
            v3, depth_pct, depth_label = self._check_liquidity(book, mid)
            v4, buy_pct,   tape_label  = self._check_tape(trades, mid)

            final = self._aggregate_verdict(v1, v2, v3, v4)

            snapshot = {
                'timestamp':       datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'mid_price':       round(mid, 2),
                'imbalance_ratio': round(ratio, 3),
                'imbalance_label': imbal_label,
                'walls':           wall_details,
                'depth_pct':       round(depth_pct, 1),
                'depth_label':     depth_label,
                'tape_buy_pct':    round(buy_pct, 1),
                'tape_label':      tape_label,
                'verdict':         final,
                'checks':          [v1, v2, v3, v4],
            }
            self.last_snapshot = snapshot
            return final, snapshot

        except Exception as e:
            # On any error default to CONFIRM so the bot doesn't freeze
            err = {'error': str(e), 'verdict': 'CONFIRM'}
            self.last_snapshot = err
            return 'CONFIRM', err

    # ─────────────────────────────────────────────────────────
    def print_snapshot(self, snapshot):
        """Print the order book snapshot in health-check style."""
        print(f" {'─'*58}")
        print(f" ORDER BOOK | {snapshot.get('timestamp','')}")
        if 'error' in snapshot:
            print(f" Status: {snapshot['error']}")
            print(f" {'─'*58}")
            return
        print(f" Mid Price:         ${snapshot['mid_price']:,.2f}")
        print(f" Bid/Ask Imbalance: {snapshot['imbalance_label']}")
        for wall in snapshot['walls']:
            print(f"   {wall}")
        print(f" Liquidity Depth:   {snapshot['depth_label']}")
        print(f" Tape Direction:    {snapshot['tape_label']}")
        checks = snapshot.get('checks', [])
        print(f" Checks [imbal|wall|depth|tape]: "
              f"[{' | '.join(checks)}]")
        print(f" Book Verdict:      {snapshot['verdict']}")
        print(f" {'─'*58}")
