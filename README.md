# MARSOF AI — Corrected Phase 1 Final

This package is the corrected multi-network Phase 1 release of MARSOF AI.

## Included
- Automatic network detection from real indexed/on-chain data
- BNB Smart Chain, Ethereum, Base, Solana, Hyperliquid, Sui, Arc, Robinhood Chain
- Multi-source market data: DEX Screener first, GeckoTerminal fills only missing fields
- Security data: GoPlus for supported EVM networks, GoPlus Solana, GoPlus Sui
- Solana RPC fallback/supplement for verifiable on-chain fields
- Sui RPC metadata fallback
- Exact-contract matching; never silently use another token's result
- Missing values remain `Not available` / `None`, never converted to zero
- Scan history and market snapshots
- Original MARSOF logo only (`marsof_logo.jpg`)
- Current English Telegram UI with Security / Market / Holders / Comparison / Risk / Full Report / Scan History
- Holder concentration correction:
  - Raw Top 10 remains visible for transparency
  - Burn/dead addresses are excluded from investor concentration
  - Liquidity pools, LP/lock contracts, known DEX/system addresses and locked holders are excluded when verified
  - Adjusted Investor Concentration drives concentration risk, not raw LP/burn concentration
  - Missing holder percentages are never treated as zero

## Important
This package does not add the Phase 2 trend scanner, whale monitoring, or X auto-publishing engine. Those remain separate from the stable Phase 1 core.
