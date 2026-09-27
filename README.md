# MARSOF AI — Database History + Rescan Update

This build adds the persistent scan-history workflow and keeps one active Telegram UI view.

## New behavior
- `📚 Scan History` lists previously scanned tokens with network and latest verdict.
- Selecting a token opens its latest stored scan result from PostgreSQL.
- `🔄 Scan Again` runs a fresh real-data scan for the same contract/network.
- The new scan is stored immediately.
- The result compares against the previous snapshot and surfaces newly appearing negative changes/risk indicators.
- `📊 Comparison` shows price/liquidity/market-cap/volume changes and newly appearing risk indicators.
- `📋 Full Report` is paginated.
- `🧠 AI Checks` was removed.
- Security and Market sections use their actual classified check lists; they no longer depend on missing `security_checks` / `market_checks` keys.
- A single active Telegram UI is maintained; old UI messages are removed before replacement when Telegram cannot edit the current message type.
- Missing source values remain `Not available`; they are never converted to zero.
- Top-10 holder concentration of 40% or more is classified as `RISK`.
- Critically low liquidity below $5,000 is classified as `RISK`; $5,000–$9,999.99 is `CAUTION`.

## Deploy on Render
Start command:
`uvicorn main:app --host 0.0.0.0 --port 10000`

Required environment variables:
- `TELEGRAM_TOKEN`
- `DATABASE_URL`
