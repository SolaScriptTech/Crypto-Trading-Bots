# Breakout Advisor — Installation & Usage

Interactive advisor for passing the Breakout Prop 1-step evaluation. Uses your
existing Lv1/Lv2/Lv3/RME intelligence stack for signal analysis and exit
recommendations; you execute manually on Breakout Terminal.

## Files in this bundle

| File | Purpose |
|------|---------|
| `advisor.py` | Main interactive REPL (~900 lines) |
| `advisor_state.py` | Persistent state at `~/.breakout_advisor/state.json` |
| `advisor_notify.py` | ntfy client lifted from `kraken_bull_bot_v8_1` |
| `wedge_detector.py` | Rising/falling wedge pattern recognition |
| `position_sizer.py` | Breakout-native sizer (daily + max DD constraints) |
| `Lv3_quant_trader.py` | **Your Lv3 with collision bugs fixed** — REVIEW DIFF before replacing your prod copy |

## Prerequisites

- Python 3.9+
- `pip install ccxt requests` (pandas/pandas_ta not needed — advisor uses pure-Python indicators)
- Your existing `Lv1_quant_trader.py`, `Lv2_quant_trader.py`, `RiskEngine_quant_trader.py` (unchanged) in the same directory as `advisor.py`
- **Replace** your `Lv3_quant_trader.py` with the patched version in this bundle (see "Lv3 changes" below)

## Deployment

Drop all files in one directory on your EC2 or local machine:

```
quant_trader/
├── advisor.py                    # NEW
├── advisor_state.py              # NEW
├── advisor_notify.py             # NEW
├── wedge_detector.py             # NEW
├── position_sizer.py             # NEW
├── Lv1_quant_trader.py           # existing — unchanged
├── Lv2_quant_trader.py           # existing — unchanged
├── Lv3_quant_trader.py           # REPLACE with patched version
├── RiskEngine_quant_trader.py    # existing — unchanged
└── (your existing bots continue to live here)
```

Run:

```bash
python3 advisor.py
```

Add `--debug` for tracebacks on errors during development.

## Lv3 changes — review before replacing

Your original `Lv3_quant_trader.py` had a `**base` collision bug: seven
`_check_*` functions took `adx`, `rsi`, `vol_ratio`, `mfi`, `bb_pct_b` as
both positional arguments AND via `**base` kwargs, causing
`"got multiple values for argument 'adx'"` TypeErrors on every scan.

The patched version removes those indicators from the positional parameter
lists and pulls them from `**kwargs` inside each function body:

```python
# Before
def _check_reversal_long(self, closes, highs, lows, volumes,
                         rsi, adx, vol_ratio,
                         rsi_floor, vol_min, **kwargs):

# After
def _check_reversal_long(self, closes, highs, lows, volumes,
                         rsi_floor, vol_min, **kwargs):
    rsi = kwargs.get("rsi")
    adx = kwargs.get("adx")
    vol_ratio = kwargs.get("vol_ratio", 1.0)
```

Same pattern applied to: `_check_macd_cross`, `_check_zero_line_cross`,
`_check_bb_mean_rev`, `_check_breakout`, `_check_macd_bear_cross`,
`_check_bb_upper_reject`, `_check_reversal_short`.

The base dict in `SignalRouter.route()` is unchanged. The call sites are
simplified to remove redundant positional passes.

**If you're currently running a bot against this Lv3, test in paper first.**
The fix is mechanical — the logic of the checks is untouched — but anything
that touches live orders deserves a paper session before you trust it.

## Commands

Type `?` at the prompt for the full command list. Key ones:

- **`scan`** — asks for symbol + equity + current price, runs the full Lv1→Lv2→Lv3 pipeline against Kraken data, returns entry verdict with size, stop, TP, and wedge analysis
- **`track`** — register a position you opened manually on Breakout Terminal. Asks for side, entry, size, stop, TP. Persists to disk.
- **`exit`** — asks current price, runs the RME wave model against your tracked position, returns CLOSE/TIGHTEN/HOLD with adaptive detail
- **`update`** — modify stop/TP/size on an existing tracked position
- **`close`** — mark position as closed, update equity from Breakout Terminal
- **`status`** — full dashboard: equity, daily budget, DD floor, open positions, cooldowns
- **`size`** — standalone sizer: "how much can I risk given current state?"
- **`wedge`** — dedicated wedge pattern scan (select 1h/4h/1d timeframe)
- **`equity`** — update your current equity when it drifts from Breakout Terminal

## Form-input conventions

- Brackets `[default]` show the remembered value — press Enter to keep it
- Type `q` at any prompt to abort the current command
- `$5,000` / `5000` / `5_000` all parse as `5000`
- `BTC` auto-completes to `BTC/USD`
- Type `quit` at the main prompt to save state and exit

## Eval settings (defaults)

Defaults are set to Breakout 1-step:
- Daily limit: 4% (resets 00:30 UTC)
- Max drawdown: 6% static (from $5,000 starting balance = $4,700 floor)

If you bought the 2-step instead, edit `~/.breakout_advisor/state.json`
after first run:
```json
"eval": {
  "daily_limit_pct": 5.0,
  "max_drawdown_pct": 8.0,
  "drawdown_type": "trailing"
}
```

## First-run checklist

1. Copy all files to your working directory
2. Replace Lv3 with the patched version (review diff first)
3. Run `python3 advisor.py` — it creates `~/.breakout_advisor/state.json` with defaults
4. Type `equity` to set your starting equity
5. Type `scan BTC/USD` to verify the pipeline runs end-to-end
6. Once you open a position on Breakout Terminal, type `track` to register it
7. Every time you're considering an exit, type `exit` and feed it the current price
8. Type `status` anytime to see daily budget and DD room

## Known limitations

- **Watch mode is a stub.** `--watch` doesn't start a background monitor yet.
  Add that in a second session once the interactive flow feels right.
- **Peak equity drift.** If you close a winning position on Breakout Terminal
  but forget to `close` in the advisor, the peak won't update. Run `equity`
  periodically to resync.
- **Kraken data ≠ Breakout data.** The advisor scans Kraken spot as the
  proxy market. Prices are close but not identical. For critical entry/exit
  decisions, verify the level on Breakout Terminal itself.
- **No API trading.** By design. Breakout doesn't allow it; this tool is
  decision support, not execution.

## ntfy setup

The default topic is `quant-crystal-ball` (same as v8.1). Subscribe once
in your phone's ntfy app. Priority-max alerts bypass Do Not Disturb, which
is intentional for exit triggers.

Test from terminal:
```bash
python3 advisor_notify.py "Test message"
```

## Emergency

There's no `EMERGENCY_STOP` file handling in the advisor because the
advisor doesn't execute trades. The only thing to "stop" is the REPL:
`Ctrl-C` or type `quit`. Your state is saved.
