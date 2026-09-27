MARSOF AI — FINAL HOLDER CONCENTRATION FIX

This version fixes the holder concentration risk bug.

Key behavior:
- Raw Top 10 Concentration is kept for transparency.
- Burn/dead addresses are excluded from investor concentration.
- Liquidity pools and known liquidity/lock contracts are excluded when identified by GoPlus tags, LP-holder data, DEX pair data, or lock status.
- System/contract addresses with recognized tags are classified separately.
- Risk is based on Adjusted Investor Concentration, not Raw Top 10 Concentration.
- Missing holder percentages are never converted to zero; concentration becomes UNAVAILABLE when required percentages are missing.
- GoPlus holder percentages are interpreted according to its API documentation: 1 = 100%.

The tested example with 88.68% burn, 4.75% PinkLock, and 1.59% PancakeV2 produces:
Raw Top 10 Concentration: 95.40%
Adjusted Investor Concentration: 0.38% (PASS)
Excluded Non-Investor Share: 95.02%

Note: This package preserves the existing Phase 1 BNB analyzer architecture. It does not add the Phase 2 trend scanner or X publisher.
