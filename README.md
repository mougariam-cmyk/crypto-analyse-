# MARSOF AI — Final Integrated Build v7.0

This package integrates the MARSOF AI architecture agreed during the project work.

## Included
- Automatic network detection for BNB Smart Chain, Ethereum, Base, Solana, Hyperliquid/HyperEVM, Sui, Arc, and Robinhood Chain.
- Real-source aggregation with field-level fallback.
- GoPlus security as the primary EVM/Solana/Sui security source when available.
- DEX Screener as the primary market source.
- GeckoTerminal market fallback and external daily OHLCV history when a Gecko pool is available.
- Blockchain RPC enrichment for EVM, Solana, and Sui.
- No fabricated values: missing values remain unavailable and are never changed to zero.
- Holder classification: verified investor / non-investor / unclassified.
- Generic LP, pool, burn, locker, router, factory, staking, vesting, multisig, and system detection.
- Raw Top 10 Concentration kept separately from Adjusted Investor Concentration.
- Excluded Non-Investor Share and Unclassified Share are shown separately.
- Internal weighted scoring across technical security, liquidity lock, investor concentration, liquidity size, holder count, activity, price trend, and contract age. Internal points are not displayed to Telegram users.
- Critical verified risk indicators can produce `HIGH RISK`; otherwise the main verdict is `NO MAJOR RISK DETECTED`.
- Market movement and contract/security risk remain separate.
- PostgreSQL snapshots for scan-to-scan comparison.
- Telegram single-active-message UI and the agreed report sections.
- Original MARSOF logo copied without redesign or recoloring.

## Important data rule
A missing field is `UNAVAILABLE`, not `0`. A real zero returned by a source remains zero.

## Network detection
For EVM addresses, MARSOF checks deployed bytecode through the configured RPC endpoints and can use indexed DEX data as a secondary confirmation. If more than one supported network is genuinely detected, the bot asks only which detected network to scan; it does not ask for a redundant confirmation.

## Deployment
Set:
- `TELEGRAM_TOKEN`
- `DATABASE_URL`

Render should run:
`uvicorn main:app --host 0.0.0.0 --port $PORT`

The webhook endpoint is `/webhook`.

## Notes
Live provider responses depend on provider availability, rate limits, token/network support, and the fields actually returned for a given asset. The local build check validates Python syntax; live API values are not claimed to have been verified inside this offline build environment.
