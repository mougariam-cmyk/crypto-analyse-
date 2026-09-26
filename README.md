# MARSOF AI V6 — Multi-Network Token Analysis

Supported networks:
- BNB Smart Chain
- Ethereum
- Base
- Hyperliquid / HyperEVM
- Solana
- Sui
- Arc
- Robinhood Chain

The engine never converts unavailable source data into zero. EVM networks use on-chain ERC-20 metadata as a fallback when the security provider does not return a verified security record. Market data is only shown when the selected network/token pair is actually returned by the market indexer.

## Render
Start Command:
`uvicorn main:app --host 0.0.0.0 --port 10000`

Required environment variables:
- `TELEGRAM_TOKEN`
- `DATABASE_URL`
