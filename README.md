# Crypto Trading Bots Research Archive

A curated collection of experimental cryptocurrency trading bots, backtests, signal loggers, risk engines, and market-research tools created by Matthew Johns.

This repository is an **engineering and strategy research archive**. It contains both paper-trading experiments and code paths capable of submitting live orders. Nothing here is financial advice or a promise of performance.

> [!CAUTION]
> Read each file before running it. Several scripts can place real trades when credentials are present. Use dedicated API keys, keep withdrawals disabled, and begin with paper trading.

## Standalone projects

Two cohesive projects now have their own repositories:

- [Kraken Live Quant Trader](https://github.com/SolaScriptTech/Kraken-Live-Quant-Trader) — real-order multi-asset trader with a pattern engine, audit trail, and layered risk controls.
- [SolaScript Crypto Scalper](https://github.com/SolaScriptTech/SolaScript-Crypto-Scalper) — asynchronous scalper research with the current engine and archived iterations.

## Major families retained here

### Kraken paper and shadow traders

- `btc_trader.py`
- `kraken_v3_6.py`
- `kraken_v4.py`, `kraken_v4_1.py`, `kraken_v4_2.py`
- `kraken_v5.py`, `kraken_v5_1.py`, `kraken_v5_fixed.py`
- `kraken_v6.py`, `kraken_v6_1_fixed.py`
- `kraken_v7.py`
- `kraken_v8.py`, `kraken_v8_1.py`

These files document an evolving strategy stack. Version numbers do not by themselves establish that a file is safer, profitable, or production-ready.

### Signal and market intelligence

- `kraken_signal_logger*.py`
- `kraken_macro_aggregator*.py`
- `kraken_macro_hunter*.py`
- `kraken_macro_tape_sniffer_v6.py`
- `order_book*.py`
- `signal_engine.py`
- `grade_signals.py`

### Backtesting and research

- `backtest*.py`
- `btc_trader_backtest*.py`
- `kraken_signal_backtester*.py`
- `export_ohlcv_for_v3_backtest*.py`
- `research.py`, `replay.py`, `rebound_research.py`

### Prop-firm and DXtrade experiments

- `prop_bot_system.py`, `prop_bot_v7.py`
- `bybit_prop_bot*.py`
- `goat_funded*.py`
- `dxtrade_client.py`
- `risk_monitor.py`

### Investigator and execution chain

- `trader.py`
- `investigator*.py`
- `super_sleuth*.py`
- `buyer.py`, `seller.py`, `executor.py`
- `database.py`

### Whale and breakout research

- `hunterkiller*.py`
- `breakout*.py`
- `market_open_scanner*.py`
- `volume_scan.py`

## Setup

Create an isolated environment:

```sh
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

Copy the credential template only when a specific script requires authenticated access:

```sh
cp .env.example .env
```

The broad requirements file supports the common modules in this archive; individual scripts may need fewer packages.

## Repository policy

- No API credentials, account data, live state, logs, databases, bytecode, or private infrastructure details.
- Generated files stay out of Git.
- Paper and live execution must be clearly distinguished.
- Backtest results must describe their assumptions and must not be presented as guaranteed returns.
- Closely related versions remain together; cohesive independent projects may be extracted.

See [ABOUT.md](ABOUT.md), [RISK_DISCLOSURE.md](RISK_DISCLOSURE.md), and [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
