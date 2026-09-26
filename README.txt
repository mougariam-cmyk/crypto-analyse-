MARSOF AI - Step 1 fixed build

Fixed:
1. DEX Screener URL now uses the real contract address:
   https://api.dexscreener.com/latest/dex/tokens/{address}
2. Only exact BSC pairs containing the requested token are accepted.
3. Highest-liquidity exact BSC pair is selected.
4. If no exact pair exists, market values remain unavailable; no zeros or fabricated values.
5. psycopg[binary] is included for SQLAlchemy PostgreSQL support.

Render Start Command:
uvicorn main:app --host 0.0.0.0 --port 10000

Do not change TELEGRAM_TOKEN or DATABASE_URL.
