# MARSOF AI — Phase 2 Internal Scoring

This build keeps MARSOF's numeric risk scoring internal. Users do not see the point calculation.

## User-facing behavior
- Shows a qualitative assessment and the actual risk indicators.
- Shows verified market/security data.
- Missing fields remain `Not available` / `UNAVAILABLE`; they are never converted to zero.
- Critical risk indicators still override the qualitative assessment.

## Internal scoring model
The internal weighted model uses the agreed factors:
- Technical security
- Liquidity lock
- Eligible investor Top 10 concentration
- Liquidity size
- Holder count
- 24h Volume / Market Cap
- 24h price trend
- Contract age

LP/liquidity-pool and known burn/system addresses are excluded from investor concentration when the source identifies them. Their raw holdings remain visible.

The numeric score is stored inside the classification data used by the scan record, but is not rendered in the Telegram UI.
