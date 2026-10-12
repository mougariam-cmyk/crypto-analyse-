MARSOF AI — Smart Token Radar TRIAL PACKAGE

This is a trial build, not a production-certified release.
Files to replace in the existing repository: main.py and main_monitor.py.
Keep a backup of the current files first. Do not reset or delete the database.

Preserved behavior
- Existing crypto and marketing publishing functions were compared with the supplied original source and are unchanged in the staged main.py.
- Existing menu buttons are retained; the requested “🚀 Smart Token Radar” button is added.
- Group Manager remains visible as Coming Soon.

New trial features
- Per-user token watch, 30-day subscription, renewal/stop commands.
- Secure single-use link flow for private Whale Alerts.
- Private volume-change alerts and deduplication records.
- Marketing opt-in/out for the separate Whale Alerts bot.

Before deploy
1. Back up the current main.py and main_monitor.py and export/backup the database.
2. Keep the existing DATABASE_URL, Telegram main-bot token, X settings, and marketing settings unchanged.
3. Confirm WHALE_ALERTS_BOT_TOKEN is configured for the separate Whale Alerts bot.
4. Set WHALE_ALERTS_BOT_USERNAME to that bot's username without @.
5. Set WHALE_ALERTS_WEBHOOK_SECRET to a random 32–256 character string using only A–Z, a–z, 0–9, underscore, or hyphen. Do not share the value in chat or logs.
6. Ensure the separate bot uses the same public service base URL and that Telegram can reach /whale-webhook.
7. Deploy, then test commands in a private Telegram chat before using any production token subscriptions.

Trial checks completed locally
- Python py_compile and AST parsing passed for main.py and main_monitor.py.
- Static review checked menu/button wiring, core publishing function preservation, per-user subscription model, command registration, webhook secret checks, single-use linking, dedupe constraints, and cleanup scope/order.
- SQLite in-memory constraint checks were previously run for per-user subscription uniqueness and dedupe constraints.

Not yet tested end-to-end
- No live Render deploy or Telegram webhook send was performed from this environment.
- No live PostgreSQL migration or data-preservation test was performed.
- No real X post was sent during this review.

First Telegram trial sequence
1. Open the main MARSOF AI bot and tap Smart Token Radar.
2. Send /watch <supported_network> <contract_address>.
3. Send /mywatch; then try a second /watch. It should reject a second active watch.
4. Test /renewwatch and /stopwatch on the test account.
5. Send /linkwhale, open the one-time link, and press Start in the separate Whale Alerts bot.
6. Check Render logs for webhook registration/errors without sharing secret values.

Rollback
If startup fails or an existing function regresses, restore the backed-up main.py and main_monitor.py. Do not reset the database; the added tables/columns are designed to be additive, but compatibility must be verified against your actual schema.
