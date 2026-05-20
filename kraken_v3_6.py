import ccxt
import pandas as pd
import numpy as np
import time
import os
from datetime import datetime, timezone

class MyKrakenIrelandAuditableBotV3_6:
    def __init__(self):
        # 1. INFRASTRUCTURE: eu-west-1 (Ireland)
        self.exchange = ccxt.kraken({'enableRateLimit': True})
        self.symbol = 'BTC/USD'
        self.eth_symbol = 'ETH/USD'
        self.timeframe = '1h'

        # 2. CAPITAL & RISK (The $2,000 Starting Block)
        self.virtual_usd = 2000.0
        self.virtual_btc = 0.0
        self.initial_capital = 2000.0
        self.max_equity = 2000.0
        self.max_dd_limit = 0.15      # 15% Account-Wide Kill Switch

        # 3. INDIVIDUAL TRADE SAFETY (The Fail-Safes)
        self.stop_loss_pct = 0.035    # Hard stop: exit if price drops 3.5% from entry/peak
        self.trailing_stop_pct = 0.02 # Trail winners by 2% to lock in profit
        self.peak_price = 0.0         # Tracks highest price seen during a trade

        # 4. MODELING: 10bps Slippage
        self.slippage_rate = 0.0010

        # 5. REGIME DETECTION PARAMETERS
        # Bull regime: EMA21 > EMA55 AND price above EMA21 AND ADX > 20
        # Bear regime: EMA21 < EMA55 — bot goes flat, no new buys
        self.ema_fast = 21
        self.ema_slow = 55
        self.adx_period = 14

        # 6. AGGRESSIVE BULL MODE PARAMETERS
        # In strong bull trends, widen the BB entry to catch more momentum moves,
        # hold longer by requiring price > EMA21 before selling at upper band,
        # and use a tighter trail to ride the wave.
        self.bull_bb_multiplier = 1.5   # Tighter bands = more frequent entries in bull
        self.bear_bb_multiplier = 2.0   # Standard wide bands (defensive)
        self.bull_trail_pct = 0.015     # Tighter trail in bull (ride it longer)
        self.bear_trail_pct = 0.02      # Wider trail in bear/neutral

        self.history = []
        self.trade_count = 0
        self.log_file = 'kraken_auditable_shadow_bot_v3_6_audit_trail.csv'
        self.current_regime = 'NEUTRAL'

    def print_health_check(self, current_equity, current_dd):
        total_profit = current_equity - self.initial_capital
        profit_pct = (total_profit / self.initial_capital) * 100
        print("\n" + "="*55)
        print(f" PITCH DECK HEALTH CHECK | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f" MARKET REGIME: {self.current_regime}")
        print("-" * 55)
        print(f" Total Trades:    {self.trade_count}")
        print(f" Current Equity:  ${current_equity:,.2f}")
        print(f" Running P/L:     ${total_profit:,.2f} ({profit_pct:.2f}%)")
        print(f" Max Drawdown:    {current_dd*100:.2f}% (Limit: {self.max_dd_limit*100}%)")
        print(f" Position:        {'LONG' if self.virtual_btc > 0 else 'FLAT'}")
        if self.virtual_btc > 0:
            trail_pct = self.bull_trail_pct if self.current_regime == 'BULL' else self.bear_trail_pct
            print(f" Entry/Peak:      ${self.peak_price:,.2f}")
            print(f" Trail Stop @:    ${self.peak_price * (1 - trail_pct):,.2f}")
            print(f" Hard Stop @:     ${self.peak_price * (1 - self.stop_loss_pct):,.2f}")
        print("="*55 + "\n")

    def _calc_adx(self, df, period=14):
        """Calculate ADX to measure trend strength."""
        high = df['h']
        low = df['l']
        close = df['c']

        plus_dm = high.diff()
        minus_dm = low.diff().abs()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm < 0] = 0

        tr1 = high - low
        tr2 = (high - close.shift()).abs()
        tr3 = (low - close.shift()).abs()
        true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

        atr = true_range.ewm(span=period, adjust=False).mean()
        plus_di = 100 * (plus_dm.ewm(span=period, adjust=False).mean() / atr)
        minus_di = 100 * (minus_dm.ewm(span=period, adjust=False).mean() / atr)
        dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        adx = dx.ewm(span=period, adjust=False).mean()
        return adx.iloc[-1]

    def detect_regime(self, df_btc, df_eth):
        """
        Dual-asset regime detection.
        BULL:    BTC EMA21 > EMA55, price > EMA21, ADX > 20, ETH trending same direction
        BEAR:    BTC EMA21 < EMA55 OR ETH EMA21 < EMA55
        NEUTRAL: Everything else
        """
        df_btc['ema_fast'] = df_btc['c'].ewm(span=self.ema_fast, adjust=False).mean()
        df_btc['ema_slow'] = df_btc['c'].ewm(span=self.ema_slow, adjust=False).mean()
        df_eth['ema_fast'] = df_eth['c'].ewm(span=self.ema_fast, adjust=False).mean()
        df_eth['ema_slow'] = df_eth['c'].ewm(span=self.ema_slow, adjust=False).mean()

        btc_ema_fast = df_btc['ema_fast'].iloc[-1]
        btc_ema_slow = df_btc['ema_slow'].iloc[-1]
        btc_price    = df_btc['c'].iloc[-1]
        eth_ema_fast = df_eth['ema_fast'].iloc[-1]
        eth_ema_slow = df_eth['ema_slow'].iloc[-1]

        btc_adx = self._calc_adx(df_btc, self.adx_period)

        btc_bull = (btc_ema_fast > btc_ema_slow) and (btc_price > btc_ema_fast)
        eth_bull = (eth_ema_fast > eth_ema_slow)
        btc_bear = (btc_ema_fast < btc_ema_slow)
        eth_bear = (eth_ema_fast < eth_ema_slow)
        strong_trend = btc_adx > 20

        if btc_bull and eth_bull and strong_trend:
            return 'BULL'
        elif btc_bear or eth_bear:
            return 'BEAR'
        else:
            return 'NEUTRAL'

    def get_signals(self):
        """
        Regime-aware BB Logic on 1h candles.
        - BULL:    Tight BB bands, aggressive entry, hold longer, trail tighter
        - NEUTRAL: Standard BB mean reversion
        - BEAR:    No new buys; only manage existing position exits
        """
        try:
            # Fetch BTC and ETH candles for regime detection
            ohlcv_btc = self.exchange.fetch_ohlcv(self.symbol, timeframe=self.timeframe, limit=100)
            ohlcv_eth = self.exchange.fetch_ohlcv(self.eth_symbol, timeframe=self.timeframe, limit=100)

            df = pd.DataFrame(ohlcv_btc, columns=['ts', 'o', 'h', 'l', 'c', 'v'])
            df_eth = pd.DataFrame(ohlcv_eth, columns=['ts', 'o', 'h', 'l', 'c', 'v'])

            # Detect market regime using both assets
            self.current_regime = self.detect_regime(df.copy(), df_eth.copy())

            # Select BB multiplier and trail % based on regime
            if self.current_regime == 'BULL':
                bb_mult = self.bull_bb_multiplier
                trail_pct = self.bull_trail_pct
            else:
                bb_mult = self.bear_bb_multiplier
                trail_pct = self.bear_trail_pct

            # Bollinger Band Calculation with regime-aware multiplier
            df['sma'] = df['c'].rolling(20).mean()
            df['std'] = df['c'].rolling(20).std()
            df['upper'] = df['sma'] + (bb_mult * df['std'])
            df['lower'] = df['sma'] - (bb_mult * df['std'])

            last_close  = df['c'].iloc[-1]
            lower_band  = df['lower'].iloc[-1]
            upper_band  = df['upper'].iloc[-1]
            ema21       = df['c'].ewm(span=21, adjust=False).mean().iloc[-1]

            # Clock-Sync Delay Check
            last_candle_ts = df['ts'].iloc[-1]
            current_time_ms = int(time.time() * 1000)
            delay = current_time_ms - (last_candle_ts + 3600000)

            # --- ENTRY LOGIC ---
            if self.virtual_btc == 0:
                if self.current_regime == 'BEAR':
                    # Bearish: no new positions, wait for regime to flip
                    return "HOLD", last_close, delay

                elif self.current_regime == 'BULL':
                    # Bull mode: buy on ANY pullback to lower band OR if price just
                    # crossed above EMA21 from below (momentum entry)
                    ema21_prev = df['c'].ewm(span=21, adjust=False).mean().iloc[-2]
                    prev_close = df['c'].iloc[-2]
                    ema_crossover = (prev_close < ema21_prev) and (last_close > ema21)
                    if last_close < lower_band or ema_crossover:
                        return "BUY", last_close, delay

                else:  # NEUTRAL
                    if last_close < lower_band:
                        return "BUY", last_close, delay

            # --- EXIT LOGIC (active for all regimes when in position) ---
            elif self.virtual_btc > 0:
                # 1. Standard Exit: price hits upper band
                #    In BULL mode, also require price > EMA21 to avoid selling into
                #    a mid-trend dip. Hold unless price is truly extended.
                if last_close > upper_band:
                    if self.current_regime == 'BULL' and last_close < ema21 * 1.01:
                        pass  # Stay in — bull momentum still intact
                    else:
                        return "SELL_TARGET", last_close, delay

                # 2. Hard Stop Loss (3.5% from peak — unchanged for all regimes)
                if last_close <= (self.peak_price * (1 - self.stop_loss_pct)):
                    return "SELL_STOP", last_close, delay

                # 3. Trailing Stop (regime-aware tightness)
                trail_threshold = self.peak_price * (1 - trail_pct)
                if last_close <= trail_threshold:
                    return "SELL_TRAIL", last_close, delay

                # 4. BEAR REGIME FORCE EXIT: if regime flips bearish mid-trade, get out
                if self.current_regime == 'BEAR':
                    return "SELL_REGIME_FLIP", last_close, delay

            return "HOLD", last_close, delay

        except Exception as e:
            print(f"Signal Error: {e}")
            return "HOLD", 0.0, 0.0

    def execute_logic(self, action, sig_price):
        exec_price = 0.0
        if action == "BUY" and self.virtual_usd > 0:
            exec_price = sig_price * (1 + self.slippage_rate)
            self.virtual_btc = self.virtual_usd / exec_price
            self.virtual_usd = 0.0
            self.peak_price = sig_price
            self.trade_count += 1
            print(f"!!! BUY EXECUTED @ ${exec_price:,.2f} | Regime: {self.current_regime} | Slippage: 10bps")

        elif action.startswith("SELL") and self.virtual_btc > 0:
            exec_price = sig_price * (1 - self.slippage_rate)
            self.virtual_usd = self.virtual_btc * exec_price
            self.virtual_btc = 0.0
            self.peak_price = 0.0
            print(f"!!! SELL EXECUTED ({action}) @ ${exec_price:,.2f} | Regime: {self.current_regime}")

        return exec_price

    def update_audit_trail(self, current_price, signal_price, exec_price, delay):
        current_equity = self.virtual_usd + (self.virtual_btc * current_price)

        # Update peak price for trailing stop logic
        if self.virtual_btc > 0:
            self.peak_price = max(self.peak_price, current_price)

        self.max_equity = max(self.max_equity, current_equity)
        drawdown = (self.max_equity - current_equity) / self.max_equity

        self.history.append({
            'timestamp':   datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f'),
            'regime':      self.current_regime,
            'equity':      round(current_equity, 2),
            'drawdown':    round(drawdown, 4),
            'signal_price': signal_price,
            'exec_price':  exec_price,
            'delay_ms':    delay
        })
        pd.DataFrame(self.history).to_csv(self.log_file, index=False)
        return current_equity, drawdown

    def main(self):
        print("--- KRAKEN V3.6: REGIME-AWARE BB + AGGRESSIVE BULL MODE ACTIVE ---")
        while True:
            try:
                action, sig_price, delay = self.get_signals()
                exec_price = self.execute_logic(action, sig_price)

                ticker = self.exchange.fetch_ticker(self.symbol)
                equity, dd = self.update_audit_trail(ticker['last'], sig_price, exec_price, delay)

                self.print_health_check(equity, dd)

                if dd >= self.max_dd_limit:
                    print("CRITICAL RISK EVENT: 15% DRAWDOWN HIT. TERMINATING BOT.")
                    break

                # Sleep to next hour boundary
                now = time.time()
                next_hour = (now // 3600 + 1) * 3600
                time.sleep(max(0, next_hour - now + 1))

            except Exception as e:
                print(f"Main Loop Error: {e}")
                time.sleep(60)

if __name__ == "__main__":
    MyKrakenIrelandAuditableBotV3_6().main()
