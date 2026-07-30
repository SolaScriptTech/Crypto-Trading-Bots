# Setup

This repository contains many separate experiments rather than one application.

## Create an environment

```sh
python -m venv .venv
# Windows: .venv\Scripts\activate
# macOS/Linux: source .venv/bin/activate
pip install -r requirements.txt
```

## Credentials

Only configure credentials for a script that actually requires authenticated access:

```sh
cp .env.example .env
```

Use dedicated API keys with the minimum permissions required. Keep withdrawals disabled. Never place credentials directly in source code or commit `.env`.

## Choose a script deliberately

Read the module documentation and order-execution functions before running a bot. Confirm whether it is:

- A backtest
- A signal logger
- A paper/shadow trader
- A live-order trader

Start with paper trading. Runtime state, logs, databases, CSV output, and audit files are intentionally ignored by Git.
