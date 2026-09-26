# MARSOF AI — Token Analyzer V4

BNB Smart Chain token analysis service for the MARSOF AI product.

## Render

Start Command:

```text
uvicorn main:app --host 0.0.0.0 --port 10000
```

Required environment variables:

- `TELEGRAM_TOKEN`
- `DATABASE_URL`

## V4 UI

- English product UI
- MARSOF AI branded onboarding with logo
- Single active Telegram analysis window
- Inline section navigation
- Pagination for long reports
- Security / Market / Holders / Comparison / AI Checks / Project Links / Full Report
- Scan-to-scan comparison with previous/current values and elapsed time
- Significant market movement alerts are kept separate from security findings
- Missing values are never converted to zero
- DEX pair links are generated only from the verified pair returned by the scan

## Data principle

The database stores MARSOF snapshots for comparison with the previous scan. Snapshot history is not presented as full launch-to-present price history. A complete historical price view requires a real historical market-data source.
