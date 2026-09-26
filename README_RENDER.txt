MARSOF AI - STEP 1 FINAL FIX

Render Start Command:
uvicorn main:app --host 0.0.0.0 --port 10000

Required environment variables:
TELEGRAM_TOKEN
DATABASE_URL

This version fixes:
- DEX Screener URL placeholder
- exact BSC pair matching
- metric() central constructor
- None stays unavailable; never fabricated as 0
- psycopg[binary] dependency
