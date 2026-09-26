MARSOF AI — STEP 1

This package contains the current BNB token analyzer with:
- Real GoPlus security data
- Real DEX Screener market data
- PostgreSQL schema migration for existing Render databases
- Telegram webhook handling
- Telegram PASS / CAUTION / RISK / DATA / Missing / ALL sections
- Safe splitting of long DATA and ALL Telegram responses

Required Render environment variables:
- TELEGRAM_TOKEN
- DATABASE_URL

Start command:
uvicorn main:app --host 0.0.0.0 --port $PORT

No secrets are included in this package.
