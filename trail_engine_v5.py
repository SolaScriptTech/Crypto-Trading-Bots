"""
trail_engine_v5.py — TrailEngine
Historical analog matching system for adaptive trailing stops.

Four modules:
  1. Historical DB Builder  — rolling 1-min OHLCV file, appends every cycle
  2. Situation Fingerprinter — normalized 7-indicator feature vector
  3. Analog Matcher          — cosine similarity, top 20 matches, 6hr contamination filter
  4. Adaptive Trail Calc     — 25th pct reversal → trail, confidence gating, shadow mode

Shadow mode: for first 48 hours, logs suggested trail vs active trail without
affecting live trades. After 48 hours (or manual override), goes live.
"""

import os
import time
import json
import numpy as np
import pandas as pd
from datetime import datetime, timezone


SHADOW_MODE_HOURS  = 48          # observe-only before going live
MIN_MATCHES        = 8           # minimum analog matches before trusting result
MIN_SIMILARITY     = 0.85        # cosine similarity threshold
CONTAMINATION_HRS  = 6           # ignore matches within last N hours
FORWARD_CANDLES    = 30          # how far ahead to measure forward returns (minutes)
DB_FILE            = 'trail_engine_1m_db.csv'
SHADOW_LOG         = 'trail_engine_shadow.csv'


class TrailEngine:
    def __init__(self, exchange, rate_limiter, symbol='BTC/USD', shadow_mode=True):
        self.exchange    = exchange
        self.rl          = rate_limiter
        self.symbol      = symbol
        self.shadow_mode = shadow_mode
        self.boot_ts     = time.time()
        self.db          = None          # numpy array once built
        self.db_df       = None          # pandas for forward-return calc
        self.last_suggestion = None
        self.shadow_rows     = []

        print(f"[TrailEngine] Init | shadow_mode={shadow_mode} | DB file: {DB_FILE}")

    # ─────────────────────────────────────────────────────────
    # MODULE 1: HISTORICAL DATABASE BUILDER
    # ─────────────────────────────────────────────────────────
    def build_db(self):
        """
        Fetch 1-min candles in chunks, compute indicators, store as CSV.
        On subsequent calls, appends only new candles to existing file.
        """
        print("[TrailEngine] Building/updating historical 1-min DB...")
        try:
            # Load existing DB if present
            if os.path.exists(DB_FILE):
                existing = pd.read_csv(DB_FILE)
                last_ts  = int(existing['ts'].max()) if len(existing) > 0 else 0
                print(f"[TrailEngine] Existing DB: {len(existing)} rows | "
                      f"last: {datetime.fromtimestamp(last_ts/1000).strftime('%Y-%m-%d %H:%M')}")
            else:
                existing = pd.DataFrame()
                last_ts  = 0

            # Fetch in chunks of 720 candles (Kraken 1-min limit per call)
            all_new = []
            since   = last_ts + 60000 if last_ts > 0 else None
            chunks  = 0
            max_chunks = 20   # 20 × 720 = ~10 days of 1-min data on first run

            while chunks < max_chunks:
                raw = self.rl.call(
                    self.exchange.fetch_ohlcv,
                    self.symbol, '1m', since, 720
                )
                if not raw or len(raw) < 2:
                    break
                all_new.extend(raw)
                since   = raw[-1][0] + 60000
                chunks += 1
                if len(raw) < 720:
                    break   # reached current time
                time.sleep(2)   # 2-second gap between chunks — Kraken safe

            if not all_new:
                print("[TrailEngine] DB up to date — no new candles.")
                if existing is not None and len(existing) > 0:
                    self._load_db(existing)
                return

            new_df = pd.DataFrame(all_new, columns=['ts','o','h','l','c','v'])
            new_df.drop_duplicates(subset='ts', inplace=True)

            # Compute indicators on new data
            new_df = self._compute_indicators(new_df)

            # Merge with existing
            if len(existing) > 0:
                combined = pd.concat([existing, new_df], ignore_index=True)
                combined.drop_duplicates(subset='ts', inplace=True)
                combined.sort_values('ts', inplace=True)
            else:
                combined = new_df

            # Keep last 20,160 rows (14 days × 1440 min/day)
            combined = combined.tail(20160).reset_index(drop=True)
            combined.to_csv(DB_FILE, index=False)

            self._load_db(combined)
            print(f"[TrailEngine] DB ready: {len(combined)} rows | "
                  f"span: {len(combined)/1440:.1f} days")

        except Exception as e:
            print(f"[TrailEngine] DB build error: {e}")

    def _compute_indicators(self, df):
        """Compute all 7 fingerprint indicators on a dataframe."""
        df = df.copy()
        c  = df['c']
        v  = df['v']

        # EMA9 for micro-trend
        df['ema9']      = c.ewm(span=9,  adjust=False).mean()

        # MACD histogram (12/26/9)
        ema12           = c.ewm(span=12, adjust=False).mean()
        ema26           = c.ewm(span=26, adjust=False).mean()
        macd_line       = ema12 - ema26
        signal_line     = macd_line.ewm(span=9, adjust=False).mean()
        df['macd_hist'] = macd_line - signal_line

        # BB %B (20-period)
        sma20           = c.rolling(20).mean()
        std20           = c.rolling(20).std()
        df['bb_pct_b']  = (c - (sma20 - 2*std20)) / (4*std20 + 1e-9)

        # RSI (14-period)
        delta           = c.diff()
        gain            = delta.clip(lower=0).rolling(14).mean()
        loss            = (-delta.clip(upper=0)).rolling(14).mean()
        df['rsi']       = 100 - (100 / (1 + gain / (loss + 1e-9)))

        # ATR (14-period normalized by price)
        high, low, close = df['h'], df['l'], df['c']
        tr               = pd.concat([
            high - low,
            (high - close.shift()).abs(),
            (low  - close.shift()).abs()
        ], axis=1).max(axis=1)
        df['atr_pct']   = tr.rolling(14).mean() / c

        # Volume ratio vs 20-period average
        vol_avg         = v.rolling(20).mean()
        df['vol_ratio'] = v / (vol_avg + 1e-9)

        # Price vs EMA9 (normalized)
        df['price_vs_ema9'] = (c - df['ema9']) / (df['ema9'] + 1e-9)

        df.dropna(inplace=True)
        return df

    def _load_db(self, df):
        """Load computed DB into numpy arrays for fast similarity search."""
        self.db_df = df.reset_index(drop=True)
        feature_cols = ['price_vs_ema9','vol_ratio','macd_hist',
                        'bb_pct_b','rsi','atr_pct','vol_ratio']
        # Normalize macd_hist and rsi to 0-1 range for fair cosine comparison
        feat = self.db_df[feature_cols].copy()
        feat['macd_hist'] = feat['macd_hist'] / (feat['macd_hist'].abs().max() + 1e-9)
        feat['rsi']       = feat['rsi'] / 100.0
        self.db = feat.values.astype(np.float32)
        print(f"[TrailEngine] Feature matrix loaded: {self.db.shape}")

    # ─────────────────────────────────────────────────────────
    # MODULE 2: SITUATION FINGERPRINTER
    # ─────────────────────────────────────────────────────────
    def fingerprint(self, df_1m, df_5m, entry_price, peak_price):
        """
        Build normalized feature vector for current market state.
        Combines 1-min micro-structure with 5-min context.
        Returns numpy array of shape (7,) or None on failure.
        """
        try:
            df1 = self._compute_indicators(df_1m.copy()).iloc[-1]
            df5 = self._compute_indicators(df_5m.copy()).iloc[-1]

            # Gain state from entry (the most important dimension)
            gain_pct = ((peak_price - entry_price) / entry_price
                        if entry_price > 0 else 0.0)

            # 1-min indicators
            f1 = float(df1['price_vs_ema9'])
            f2 = float(df1['vol_ratio'])
            f3 = float(df1['macd_hist']) / (abs(float(df1['macd_hist'])) + 1e-9)
            f4 = float(df1['bb_pct_b'])
            f5 = float(df1['rsi']) / 100.0
            f6 = float(df1['atr_pct'])

            # 5-min MACD for context weighting
            f7 = float(df5['macd_hist']) / (abs(float(df5['macd_hist'])) + 1e-9)

            vec = np.array([f1, f2, f3, f4, f5, f6, f7], dtype=np.float32)

            # Flag if 1-min and 5-min MACD disagree (weak context)
            self._context_agreement = (np.sign(f3) == np.sign(f7))

            return vec, gain_pct

        except Exception as e:
            print(f"[TrailEngine] Fingerprint error: {e}")
            return None, 0.0

    # ─────────────────────────────────────────────────────────
    # MODULE 3: ANALOG MATCHER
    # ─────────────────────────────────────────────────────────
    def find_analogs(self, fingerprint_vec):
        """
        Find top-20 historical fingerprint matches by cosine similarity.
        Filters out last 6 hours to prevent contamination.
        Returns list of (similarity, row_index) sorted descending.
        """
        if self.db is None or len(self.db) < FORWARD_CANDLES + 10:
            return []

        now_ts       = time.time() * 1000
        cutoff_ts    = now_ts - (CONTAMINATION_HRS * 3600 * 1000)
        valid_mask   = self.db_df['ts'].values < cutoff_ts

        if valid_mask.sum() < MIN_MATCHES:
            return []

        # Cosine similarity: dot(a,b) / (|a| * |b|)
        db_valid   = self.db[valid_mask]
        idx_map    = np.where(valid_mask)[0]
        norms      = np.linalg.norm(db_valid, axis=1) + 1e-9
        query_norm = np.linalg.norm(fingerprint_vec) + 1e-9
        sims       = db_valid.dot(fingerprint_vec) / (norms * query_norm)

        # Top 20 above similarity threshold
        top_idx    = np.argsort(sims)[::-1][:20]
        results    = []
        for i in top_idx:
            sim = float(sims[i])
            if sim < MIN_SIMILARITY:
                break
            orig_idx = idx_map[i]
            # Only include if we have FORWARD_CANDLES of data after this point
            if orig_idx + FORWARD_CANDLES < len(self.db_df):
                results.append((sim, int(orig_idx)))

        return results

    # ─────────────────────────────────────────────────────────
    # MODULE 4: ADAPTIVE TRAIL CALCULATOR
    # ─────────────────────────────────────────────────────────
    def calc_adaptive_trail(self, analogs, current_price, entry_price, peak_price,
                            fallback_trail_pct):
        """
        Derives trail from forward return distribution of analog matches.

        Returns:
            trail_pct    — effective trail percentage
            stop_price   — absolute stop price
            method       — 'analog' | 'analog_shadow' | 'tiered' | 'profit_floor'
            confidence   — float 0-1
            detail       — dict for health check display
        """
        # Always compute fallback first
        fallback_trail, fallback_stop, fallback_reason = self._tiered_profit_floor(
            entry_price, peak_price, fallback_trail_pct
        )

        if not analogs or len(analogs) < MIN_MATCHES:
            return (fallback_trail, fallback_stop, fallback_reason, 0.0,
                    {'matches': len(analogs), 'confidence': 'LOW — using fallback'})

        # Compute forward returns for each analog
        forward_returns = []
        for sim, idx in analogs:
            future_prices = self.db_df['c'].iloc[idx+1 : idx+FORWARD_CANDLES+1].values
            if len(future_prices) == 0:
                continue
            base_price    = self.db_df['c'].iloc[idx]
            max_gain      = (future_prices.max() - base_price) / base_price
            # Find where it reversed (max drawdown from peak within window)
            peak_idx      = np.argmax(future_prices)
            post_peak     = future_prices[peak_idx:]
            if len(post_peak) > 1:
                reversal  = (post_peak.min() - post_peak[0]) / (post_peak[0] + 1e-9)
            else:
                reversal  = 0.0
            forward_returns.append({
                'sim':      sim,
                'max_gain': max_gain,
                'reversal': reversal,   # negative = price dropped after peak
            })

        if not forward_returns:
            return (fallback_trail, fallback_stop, fallback_reason, 0.0,
                    {'matches': 0, 'confidence': 'LOW — using fallback'})

        reversals  = np.array([r['reversal'] for r in forward_returns])
        max_gains  = np.array([r['max_gain']  for r in forward_returns])
        avg_sim    = np.mean([r['sim'] for r in forward_returns])

        # 25th percentile of reversal depth = trail distance
        # (75% of analog situations still had room at this distance)
        pct25_rev  = abs(np.percentile(reversals, 25))
        median_fwd = float(np.median(max_gains))

        # Tighten if 1-min and 5-min context disagree
        if not getattr(self, '_context_agreement', True):
            pct25_rev *= 0.8
            context_note = ' (tightened: 1m/5m disagreement)'
        else:
            context_note = ''

        # Confidence score
        confidence = min(1.0, (len(analogs) / 20) * avg_sim)
        conf_label = 'HIGH' if confidence > 0.75 else 'MEDIUM' if confidence > 0.5 else 'LOW'

        analog_stop  = peak_price * (1 - pct25_rev)
        analog_trail = pct25_rev

        # Profit floor always applies on top of analog
        _, floor_stop, floor_reason = self._tiered_profit_floor(
            entry_price, peak_price, analog_trail
        )

        # Tightest stop wins
        if floor_stop > analog_stop:
            final_stop   = floor_stop
            final_trail  = 1 - (floor_stop / peak_price) if peak_price > 0 else fallback_trail
            method       = 'profit_floor'
        else:
            final_stop   = analog_stop
            final_trail  = analog_trail
            method       = 'analog_shadow' if self.shadow_mode else 'analog'

        detail = {
            'matches':        len(analogs),
            'avg_similarity': round(avg_sim * 100, 1),
            'median_fwd_gain': round(median_fwd * 100, 3),
            'pct25_reversal': round(pct25_rev * 100, 3),
            'confidence':     f"{conf_label} ({confidence:.2f}){context_note}",
            'method':         method,
        }

        # Shadow mode: log suggestion but return fallback for live use
        if self.shadow_mode:
            hours_running = (time.time() - self.boot_ts) / 3600
            self._log_shadow(final_trail, final_stop, fallback_trail,
                             fallback_stop, detail, hours_running)
            if hours_running < SHADOW_MODE_HOURS:
                print(f"[TrailEngine] SHADOW: analog suggests "
                      f"{final_trail*100:.3f}% trail @ ${final_stop:,.2f} | "
                      f"active: fallback {fallback_trail*100:.3f}%")
                return (fallback_trail, fallback_stop, fallback_reason,
                        confidence, detail)
            else:
                print(f"[TrailEngine] Shadow period complete — analog trail NOW LIVE")
                self.shadow_mode = False

        return final_trail, final_stop, method, confidence, detail

    def _tiered_profit_floor(self, entry_price, peak_price, base_trail):
        """
        V4.1 tiered trail + profit floor.
        Tiers:  <0.3% → 1.3%  |  0.3-0.7% → 0.8%  |  0.7-1.2% → 0.5%  |  >1.2% → 0.3%
        Floor:  once gain > 0.3%, stop >= entry * 1.001
        """
        if entry_price <= 0 or peak_price <= 0:
            return base_trail, 0.0, 'default'

        gain_pct = (peak_price - entry_price) / entry_price

        # Tiered trail
        if gain_pct >= 0.012:
            tier_trail = 0.003
        elif gain_pct >= 0.007:
            tier_trail = 0.005
        elif gain_pct >= 0.003:
            tier_trail = 0.008
        else:
            tier_trail = base_trail

        tier_stop = peak_price * (1 - tier_trail)

        # Profit floor
        if gain_pct >= 0.003:
            floor_stop  = entry_price * 1.001
            if floor_stop > tier_stop:
                floor_trail = 1 - (floor_stop / peak_price)
                return floor_trail, floor_stop, 'profit_floor'

        return tier_trail, tier_stop, 'tiered'

    def _log_shadow(self, analog_trail, analog_stop, active_trail,
                    active_stop, detail, hours_running):
        """Append shadow comparison row to CSV."""
        row = {
            'timestamp':      datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S'),
            'hours_running':  round(hours_running, 2),
            'analog_trail':   round(analog_trail * 100, 4),
            'analog_stop':    round(analog_stop, 2),
            'active_trail':   round(active_trail * 100, 4),
            'active_stop':    round(active_stop, 2),
            'matches':        detail.get('matches', 0),
            'avg_similarity': detail.get('avg_similarity', 0),
            'confidence':     detail.get('confidence', ''),
            'method':         detail.get('method', ''),
        }
        self.shadow_rows.append(row)
        pd.DataFrame(self.shadow_rows).to_csv(SHADOW_LOG, index=False)

    # ─────────────────────────────────────────────────────────
    # MAIN ENTRY POINT
    # ─────────────────────────────────────────────────────────
    def evaluate(self, df_1m, df_5m, entry_price, peak_price, base_trail):
        """
        Full pipeline: fingerprint → analog match → adaptive trail.
        Falls back gracefully at every step.

        Returns (trail_pct, stop_price, method, confidence, detail)
        """
        try:
            if self.db is None:
                self.build_db()

            if entry_price <= 0 or peak_price <= 0:
                trail, stop, reason = self._tiered_profit_floor(
                    entry_price, peak_price, base_trail)
                return trail, stop, reason, 0.0, {}

            vec, gain_pct = self.fingerprint(df_1m, df_5m, entry_price, peak_price)

            if vec is None:
                trail, stop, reason = self._tiered_profit_floor(
                    entry_price, peak_price, base_trail)
                return trail, stop, reason, 0.0, {}

            analogs = self.find_analogs(vec)
            return self.calc_adaptive_trail(
                analogs, df_1m['c'].iloc[-1],
                entry_price, peak_price, base_trail
            )

        except Exception as e:
            print(f"[TrailEngine] evaluate error: {e}")
            trail, stop, reason = self._tiered_profit_floor(
                entry_price, peak_price, base_trail)
            return trail, stop, reason, 0.0, {}

    def print_detail(self, detail):
        """Print trail engine detail block for health check."""
        if not detail:
            return
        print(f" Analog Matches:    {detail.get('matches', 0)} found "
              f"(avg similarity: {detail.get('avg_similarity', 0)}%)")
        print(f" Hist Fwd Gain:     {detail.get('median_fwd_gain', 0)}% median")
        print(f" Hist Reversal:     -{detail.get('pct25_reversal', 0)}% (25th pct)")
        print(f" Trail Confidence:  {detail.get('confidence', 'N/A')}")
