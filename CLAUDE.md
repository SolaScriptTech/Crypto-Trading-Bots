# CLAUDE.md

## Repository role

This repository is a public research archive containing multiple independent trading experiments. It is not one deployable application.

## Safety rules

- Never add credentials, account identifiers, runtime state, logs, databases, infrastructure addresses, or private-key paths.
- Never enable withdrawals in exchange API guidance.
- Distinguish paper/shadow trading from live execution in both code and documentation.
- Do not run or test live-order paths without explicit user authorization.
- Preserve closed-candle evaluation where a strategy uses it to prevent look-ahead bias.
- Model fees and slippage explicitly in backtests.
- Use atomic state writes for persistent bot state.
- Treat performance claims skeptically and document assumptions.

## Organization

Closely related historical versions stay together in this archive. Cohesive projects with independent dependencies and documentation may be extracted into their own repositories.

Start with README.md, ABOUT.md, RISK_DISCLOSURE.md, and the relevant script's module-level documentation.
