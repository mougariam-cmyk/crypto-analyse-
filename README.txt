MARSOF AI — Step 1 fixed market/snapshot build

Changes:
- Exact BSC token-to-pair matching in DexScreener.
- Highest-liquidity exact pair is selected.
- Missing DATA values become UNAVAILABLE/UNKNOWN, never 0.
- DexScreener pairCreatedAt supports milliseconds.
- SINCE LAST SCAN is calculated before the current snapshot is saved.
- Database additive migration covers legacy Step-1 columns.
- Runtime logs explain whether an exact market pair was found.
- No ATH/ATL is fabricated from MARSOF snapshots.

Render start command:
uvicorn main:app --host 0.0.0.0 --port 10000
