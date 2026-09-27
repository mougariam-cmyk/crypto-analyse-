# MARSOF AI — Final V8

Telegram crypto-analysis bot using real data only.

## Included
- Original MARSOF logo and designed Telegram home menu.
- Automatic network verification for supported networks.
- Multi-source security and market analysis.
- Missing values remain unavailable; real zero values remain zero.
- Holder concentration separates verified LP/pool/burn/locked/system addresses from investor concentration where evidence exists.
- Internal scoring is hidden from users.
- Scan history and rescans.
- Security / Market / Holders / Liquidity / Comparison / Risk / Full Report pages.

## Render
Set `TELEGRAM_TOKEN` and `DATABASE_URL`, then run:
`uvicorn main:app --host 0.0.0.0 --port $PORT`
