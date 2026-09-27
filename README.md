# MARSOF AI — Phase 1 Final Release

This package is the Phase 1 baseline for the transition to Phase 2.

## Included
- FastAPI + Telegram webhook
- PostgreSQL persistence and schema migration
- Automatic network detection from real indexed/on-chain data
- BNB Smart Chain, Ethereum, Base, Solana, Hyperliquid/HyperEVM, Sui, Arc, and Robinhood Chain configuration
- GoPlus security data where supported
- DEX Screener market data
- Real-data-only reporting: missing values remain `Not available`
- Risk-first verdict logic
- Top-10 concentration >= 40% classified as `RISK`
- Low-liquidity rule retained as an explicit market risk signal
- Price movement is reported as market movement, not automatically as smart-contract risk
- Scan history by network and token
- Re-scan and previous-scan comparison
- Security, Market, Holders, Comparison, Risk, and Full Report views
- Pagination for long reports
- Single active UI flow with contract-message deletion
- MARSOF AI logo on the main menu
- Website / X / Telegram buttons around Analyze Token
- No registration required

## Required Render environment variables
- `TELEGRAM_TOKEN`
- `DATABASE_URL`

## Start command
`uvicorn main:app --host 0.0.0.0 --port $PORT`

## Important
This is the Phase 1 baseline. Phase 2 can build on the saved PostgreSQL scan history and market/holder snapshots without replacing them with fabricated historical values.
