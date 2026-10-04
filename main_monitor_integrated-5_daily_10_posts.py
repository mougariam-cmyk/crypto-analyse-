"""
MARSOF AI — Phase 2 Monitor

Standalone background worker for monitoring a configured watchlist.

Design goals:
- Keep the stable MARSOF analysis engine untouched.
- Reuse the already-loaded main.py core for real market/security/holder data.
- Persist monitoring events in the same PostgreSQL database.
- Detect unusual market activity before involving an AI model.
- AI/X publishing are optional and disabled unless explicitly configured.

Can run standalone or embedded as a background thread inside the Telegram web service.

Embedded mode is intended for free/single-service deployments where a separate
Render Background Worker is not available or desired.
"""

import os
import json
import time
import math
import importlib
import importlib.util
import sys
import threading
import base64
import hashlib
import hmac
import secrets
import random
import requests
from urllib.parse import quote, urlencode
from pathlib import Path
from datetime import datetime, timezone, timedelta

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text, or_, text, UniqueConstraint
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy import create_engine

# ============================================================
# CONFIG
# ============================================================

BASE_DIR = Path(__file__).resolve().parent
CORE_FILE = Path(os.getenv("MARSOF_CORE_FILE", str(BASE_DIR / "main.py")))
CORE_MODULE_NAME = os.getenv("MARSOF_CORE_MODULE", "main")
DATABASE_URL = os.getenv("DATABASE_URL")
MONITOR_INTERVAL_SECONDS = max(30, int(os.getenv("MONITOR_INTERVAL_SECONDS", "300")))
MONITOR_MIN_ACTIVITY_SCORE = float(os.getenv("MONITOR_MIN_ACTIVITY_SCORE", "50"))
MONITOR_COOLDOWN_MINUTES = max(1, int(os.getenv("MONITOR_COOLDOWN_MINUTES", "60")))
MONITOR_RUN_ONCE = os.getenv("MONITOR_RUN_ONCE", "0").lower() in {"1", "true", "yes"}
TRANSFER_MONITOR_ENABLED = os.getenv("MONITOR_TRANSFER_ENABLED", "1").lower() in {"1", "true", "yes"}
TRANSFER_MAX_BLOCKS_PER_CYCLE = max(50, int(os.getenv("MONITOR_TRANSFER_MAX_BLOCKS", "1000")))
TRANSFER_MAX_EVENTS_PER_TOKEN = max(1, int(os.getenv("MONITOR_TRANSFER_MAX_EVENTS", "50")))
TRANSFER_SCAN_LOOKBACK_BLOCKS = max(10, int(os.getenv("MONITOR_TRANSFER_LOOKBACK_BLOCKS", "500")))
WHALE_THRESHOLD_USD = max(0.0, float(os.getenv("MONITOR_WHALE_THRESHOLD_USD", "500")))
# Exceptional verified whale movements can be sent directly to Telegram without Gemini.
WHALE_ALERT_THRESHOLD_USD = max(WHALE_THRESHOLD_USD, float(os.getenv("MONITOR_WHALE_ALERT_THRESHOLD_USD", "10000")))
MARKETING_IMAGE_DIR = Path(os.getenv("MARSOF_MARKETING_IMAGE_DIR", str(BASE_DIR / "marketing_images" / "default"))).resolve()
ERC20_TRANSFER_TOPIC = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
_DECIMALS_CACHE = {}

# Phase-2 watchlist is intentionally stored in this file.
# Runtime secrets/configuration (DB, API keys, interval, etc.) remain in Environment.
#
# NOTE: The watchlist is kept explicit and chain-aware. The `name` field is
# only an internal alias for configuration/debugging. Real token name/symbol
# are resolved at runtime from contract/on-chain/core metadata and are used
# for reports and alerts. Each supplied address is stored exactly as provided;
# the same EVM address may legitimately appear on different networks.
WATCHLIST_CONFIG = [
    {
        "name": "LOFI",
        "network": "sui",
        "address": "0xf22da9a24ad027cccb5f2d496cbe91de953d363513db08a3a734d361c7c17503::LOFI::LOFI",
        "enabled": True,
    },
    {
        "name": "MANIFEST",
        "network": "sui",
        "address": "0xc466c28d87b3d5cd34f3d5c088751532d71a38d93a8aae4551dd56272cfb4355::manifest::MANIFEST",
        "enabled": True,
    },
    {
        "name": "BLUB",
        "network": "sui",
        "address": "0xfa7ac3951fdca92c5200d468d31a365eb03b2be9936fde615e69f0c1274ad3a0::BLUB::BLUB",
        "enabled": True,
    },
    {
        "name": "BASE-1",
        "network": "base",
        "address": "0x532f27101965dd16442E59d40670FaF5eBB142E4",
        "enabled": True,
    },
    {
        "name": "TOSHI",
        "network": "bsc",
        "address": "0x6a2608Dabe09bc1128EEC7275B92DFB939D5Db3f",
        "enabled": True,
    },
    {
        "name": "BASE-3",
        "network": "base",
        "address": "0xB2000000000000000000004c27f6523082f41D01",
        "enabled": True,
    },
    {
        "name": "TROLL",
        "network": "solana",
        "address": "5UUH9RTDiSpq6HKS6bp4NdU9PNJpXRXuiw6ShBTBhgH2",
        "enabled": True,
    },
    {
        "name": "STONK",
        "network": "solana",
        "address": "6GmAFSYs4gk3FDao5FzzySQpPZaWsa4rUJHacpMpUNgx",
        "enabled": True,
    },
    {
        "name": "ROBINHOOD-1",
        "network": "robinhood",
        "address": "0x2E8c31162b855A2ffa90F6F8634643Ad6F111e18",
        "enabled": True,
    },
    {
        "name": "ROBINHOOD-2",
        "network": "robinhood",
        "address": "0x020bfC650A365f8BB26819deAAbF3E21291018b4",
        "enabled": True,
    },
    {
        "name": "ARC-1",
        "network": "arc",
        "address": "0xeCe5cA8bf9220718E5727754026757512212cb3c",
        "enabled": True,
    },
    {
        "name": "BNB-1",
        "network": "bsc",
        "address": "0xFe189E97832DA1573e4e4Ff034F4fFC3a15c7777",
        "enabled": True,
    },
    {
        "name": "BNB-2",
        "network": "bsc",
        "address": "0x82Ec31D69b3c289E541b50E30681FD1ACAd24444",
        "enabled": True,
    },
    {
        "name": "ETH-1",
        "network": "ethereum",
        "address": "0xE0f63A424a4439cBE457D80E4f4b51aD25b2c56C",
        "enabled": True,
    },
    {
        "name": "SOLANA-3",
        "network": "solana",
        "address": "2qEHjDLDLbuBgRYvsxhc5D6uDWAivNFZGan56P1tpump",
        "enabled": True,
    },
    {
        "name": "BNB-3",
        "network": "bsc",
        "address": "0x1D0A4821FDEf156b0d051D08A166DE5DF2788Cf7",
        "enabled": True,
    },
    {
        "name": "SOLANA-4",
        "network": "solana",
        "address": "7GCihgDB8fe6KNjn2MYtkzZcRjQy3t9GHdC8uHYmW2hr",
        "enabled": True,
    },
    {
        "name": "CATECOIN",
        "network": "bsc",
        "address": "0xe4fae3faa8300810c835970b9187c268f55d998f",
        "enabled": True,
    },
    {
        "name": "ROBINHOOD-3",
        "network": "robinhood",
        "address": "0x39dBED3a2bd333467115dE45665cC57F813C4571",
        "enabled": True,
    },
    {
        "name": "HYPERLIQUID-1",
        "network": "hyperliquid",
        "address": "0x9b498C3c8A0b8CD8BA1D9851d40D186F1872b44E",
        "enabled": True,
    },
    {
        "name": "SOLANA-5",
        "network": "solana",
        "address": "pumpCmXqMfrsAkQ5r49WcJnRayYRqmXz6ae8H7H9Dfn",
        "enabled": True,
    },
    {
        "name": "SOLANA-6",
        "network": "solana",
        "address": "DezXAZ8z7PnrnRJjz3wXBoRgixCa6xjnB7YaB1pPB263",
        "enabled": True,
    },
    {
        "name": "SUI-4",
        "network": "sui",
        "address": "0x2::sui::SUI",
        "enabled": True,
    },
]

# Optional Telegram alerting. This worker does not require it.
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
MONITOR_TELEGRAM_CHAT_ID = os.getenv("MONITOR_TELEGRAM_CHAT_ID")
# Optional public Telegram channel destination for Gemini-generated alerts.
MONITOR_TELEGRAM_CHANNEL_ID = os.getenv("MONITOR_TELEGRAM_CHANNEL_ID")
# Optional clickable MARSOF footer destination. Leave unset until the official URL is configured.
MARSOF_AI_URL = os.getenv("MARSOF_AI_URL")

# Daily content publishing is intentionally separate from event alerts.
# Seven crypto posts are scheduled every day and selected from the full WATCHLIST.
# Posts do not require a major event: real market data is enough for an editorial update.
# Three additional posts are reserved for MARSOF AI product/marketing content.
DAILY_CONTENT_ENABLED = os.getenv("MONITOR_DAILY_CONTENT_ENABLED", "1").lower() in {"1", "true", "yes"}
DAILY_CRYPTO_POST_TARGET = min(7, max(1, int(os.getenv("MONITOR_DAILY_CRYPTO_POSTS", "7"))))
DAILY_MARKETING_POST_TARGET = min(3, max(1, int(os.getenv("MONITOR_DAILY_MARKETING_POSTS", "3"))))
DAILY_CRYPTO_SLOTS = tuple(x.strip() for x in os.getenv(
    "MONITOR_DAILY_CRYPTO_SLOTS", "01:00,07:00,10:00,13:00,16:00,19:00,22:00"
).split(",") if x.strip())
DAILY_MARKETING_SLOTS = tuple(x.strip() for x in os.getenv(
    "MONITOR_DAILY_MARKETING_SLOTS", "08:30,17:30,23:30"
).split(",") if x.strip())
# Standalone public whale alerts are OFF by default. Whale data is still collected
# and remains available to Daily Crypto/Gemini analysis.
WHALE_ALERTS_ENABLED = os.getenv("MONITOR_WHALE_ALERTS_ENABLED", "0").lower() in {"1", "true", "yes"}
DAILY_PRIORITY_TOKENS = tuple(x.strip().upper() for x in os.getenv(
    "MONITOR_DAILY_PRIORITY_TOKENS", ""
).split(",") if x.strip())
DAILY_LOOKBACK_HOURS = max(6, int(os.getenv("MONITOR_DAILY_LOOKBACK_HOURS", "24")))
DAILY_MAX_SNAPSHOTS = max(100, int(os.getenv("MONITOR_DAILY_MAX_SNAPSHOTS", "500")))

# Optional AI stage. Detection works without AI.
AI_ENABLED = os.getenv("MONITOR_AI_ENABLED", "0").lower() in {"1", "true", "yes"}
AI_API_KEY = os.getenv("GEMINI_API_KEY")
AI_MODEL = os.getenv("MONITOR_AI_MODEL", "gemini-3.5-flash-lite")
AI_FALLBACK_MODEL = os.getenv("MONITOR_AI_FALLBACK_MODEL", "gemini-3.5-flash-lite")
AI_MAX_OUTPUT_TOKENS = int(os.getenv("MONITOR_AI_MAX_OUTPUT_TOKENS", "1200"))
AI_TIMEOUT_SECONDS = int(os.getenv("MONITOR_AI_TIMEOUT_SECONDS", "45"))
AI_EVENT_THRESHOLD = float(os.getenv("MONITOR_AI_EVENT_THRESHOLD", "50"))
AI_EVENT_BATCH_LIMIT = max(1, int(os.getenv("MONITOR_AI_EVENT_BATCH_LIMIT", "5")))

# Optional X/Twitter stage. Publishing is intentionally disabled until enabled.
X_ENABLED = os.getenv("MONITOR_X_ENABLED", "0").lower() in {"1", "true", "yes"}
# Read-only X API credential used for the monitoring connectivity test and,
# later, for public social-activity snapshots. Never log this value.
X_BEARER_TOKEN = os.getenv("X_BEARER_TOKEN")
# OAuth 1.0a user-context credentials used only for publishing to the
# configured MARSOF AI X account. Keep these secrets in Render Environment.
X_PUBLISH_ENABLED = os.getenv("MONITOR_X_PUBLISH_ENABLED", "0").lower() in {"1", "true", "yes"}
X_CONSUMER_KEY = os.getenv("X_API_KEY") or os.getenv("X_CONSUMER_KEY")
X_CONSUMER_SECRET = os.getenv("X_API_SECRET") or os.getenv("X_CONSUMER_SECRET")
X_ACCESS_TOKEN = os.getenv("X_ACCESS_TOKEN")
X_ACCESS_TOKEN_SECRET = os.getenv("X_ACCESS_TOKEN_SECRET")
X_MAX_POST_CHARS = max(100, int(os.getenv("MONITOR_X_MAX_POST_CHARS", "275")))
X_AGGREGATE_LOOKBACK_HOURS = max(1, int(os.getenv("MONITOR_X_AGGREGATE_LOOKBACK_HOURS", "24")))
X_AGGREGATE_MAX_EVENTS = max(1, int(os.getenv("MONITOR_X_AGGREGATE_MAX_EVENTS", "20")))
# X should only publish fresh events. Old confirmed events are not resurrected.
X_MAX_EVENT_AGE_HOURS = max(1, int(os.getenv("MONITOR_X_MAX_EVENT_AGE_HOURS", "6")))

# Interactive Telegram test command. If MONITOR_TEST_CHAT_ID (or the existing
# MONITOR_TELEGRAM_CHAT_ID) is configured, only that chat may run the test.
MONITOR_TEST_ENABLED = os.getenv("MONITOR_TEST_ENABLED", "1").lower() in {"1", "true", "yes"}
MONITOR_TEST_CHAT_ID = os.getenv("MONITOR_TEST_CHAT_ID") or MONITOR_TELEGRAM_CHAT_ID

# ============================================================
# CORE ENGINE LOADER
# ============================================================


def load_core():
    """Return the already-loaded MARSOF core when embedded in main.py.

    This is critical for Render Web Service mode: main.py imports this module
    from its FastAPI startup hook, so loading main.py again from disk would
    create a second FastAPI app, second SQLAlchemy registry, and duplicate
    module-level initialization.
    """
    # Embedded mode: main.py is already present in sys.modules. Reuse that
    # exact module object so the monitor shares its DB/session/models.
    existing = sys.modules.get(CORE_MODULE_NAME)
    if existing is not None:
        return existing

    # Standalone/local mode: import the configured core module normally.
    if CORE_MODULE_NAME:
        try:
            return importlib.import_module(CORE_MODULE_NAME)
        except ModuleNotFoundError:
            pass

    # Legacy fallback for an explicitly supplied file path. This path is not
    # used by the normal main.py + main_monitor.py Render deployment.
    if not CORE_FILE.exists():
        raise FileNotFoundError(f"MARSOF core file not found: {CORE_FILE}")

    spec = importlib.util.spec_from_file_location("marsof_core", str(CORE_FILE))
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load MARSOF core: {CORE_FILE}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


core = load_core()

# Reuse the exact SQLAlchemy engine/session from the stable core when available.
SessionLocal = getattr(core, "SessionLocal", None)
MarketSnapshot = getattr(core, "MarketSnapshot", None)
HolderSnapshot = getattr(core, "HolderSnapshot", None)
Token = getattr(core, "Token", None)
normalize_address = getattr(core, "normalize_address")
chain_id_for = getattr(core, "chain_id_for")
run_analysis = getattr(core, "run_analysis")
network_label = getattr(core, "network_label")
fmt_money = getattr(core, "fmt_money")
fmt_pct = getattr(core, "fmt_pct")

if SessionLocal is None or MarketSnapshot is None or HolderSnapshot is None:
    raise RuntimeError("MARSOF core database models/session are unavailable")

# ============================================================
# MONITOR-ONLY TABLES
# ============================================================

MonitorBase = declarative_base()


class MonitorEvent(MonitorBase):
    __tablename__ = "monitor_events"
    id = Column(Integer, primary_key=True)
    chain_id = Column(String(20), nullable=False)
    network = Column(String(40), nullable=False)
    token_address = Column(String(100), nullable=False)
    token_symbol = Column(String(80))
    activity_score = Column(Float, nullable=False)
    severity = Column(String(30), nullable=False)
    triggered_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    payload = Column(Text, nullable=False)
    ai_report = Column(Text)
    ai_processed = Column(Boolean, default=False)
    published_x = Column(Boolean, default=False)
    published_telegram = Column(Boolean, default=False)


class EventConfirmation(MonitorBase):
    __tablename__ = "monitor_event_confirmations"
    id = Column(Integer, primary_key=True)
    monitor_event_id = Column(Integer, nullable=False, unique=True)
    chain_id = Column(String(20), nullable=False)
    network = Column(String(40), nullable=False)
    token_address = Column(String(100), nullable=False)
    status = Column(String(30), nullable=False, default="DETECTED")
    evidence_count = Column(Integer, nullable=False, default=0)
    whale_count = Column(Integer, nullable=False, default=0)
    integrity_verified = Column(Boolean, nullable=False, default=False)
    reason = Column(Text)
    detected_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    confirmed_at = Column(DateTime)
    updated_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class MonitorState(MonitorBase):
    __tablename__ = "monitor_states"
    id = Column(Integer, primary_key=True)
    chain_id = Column(String(20), nullable=False)
    network = Column(String(40), nullable=False)
    token_address = Column(String(100), nullable=False)
    last_event_at = Column(DateTime)
    last_checked_at = Column(DateTime)
    last_score = Column(Float)
    last_status = Column(String(30))


class TransferCursor(MonitorBase):
    __tablename__ = "monitor_transfer_cursors"
    id = Column(Integer, primary_key=True)
    chain_id = Column(String(20), nullable=False)
    network = Column(String(40), nullable=False)
    token_address = Column(String(100), nullable=False)
    last_block = Column(Integer)


class TransferEvent(MonitorBase):
    __tablename__ = "monitor_transfer_events"
    id = Column(Integer, primary_key=True)
    chain_id = Column(String(20), nullable=False)
    network = Column(String(40), nullable=False)
    token_address = Column(String(100), nullable=False)
    tx_hash = Column(String(100), nullable=False)
    log_index = Column(Integer)
    block_number = Column(Integer)
    block_time = Column(DateTime)
    sender = Column(String(100))
    receiver = Column(String(100))
    raw_amount = Column(Text)
    amount_ui = Column(Float)
    price_usd = Column(Float)
    value_usd = Column(Float)
    transfer_type = Column(String(30), nullable=False)
    source = Column(String(40), nullable=False)


class WhaleEvent(MonitorBase):
    __tablename__ = "monitor_whale_events"
    id = Column(Integer, primary_key=True)
    transfer_event_id = Column(Integer, nullable=False)
    chain_id = Column(String(20), nullable=False)
    network = Column(String(40), nullable=False)
    token_address = Column(String(100), nullable=False)
    tx_hash = Column(String(100), nullable=False)
    whale_wallet = Column(String(100), nullable=False)
    whale_role = Column(String(20), nullable=False)
    amount_ui = Column(Float)
    value_usd = Column(Float, nullable=False)
    balance_delta_ui = Column(Float)
    transfer_type = Column(String(30), nullable=False)
    detected_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    source = Column(String(40), nullable=False)
    telegram_alerted = Column(Boolean, nullable=False, default=False)




class DailyPost(MonitorBase):
    __tablename__ = "monitor_daily_posts"
    id = Column(Integer, primary_key=True)
    post_date = Column(String(10), nullable=False)
    slot = Column(String(20), nullable=False)
    kind = Column(String(20), nullable=False)
    network = Column(String(40))
    token_address = Column(String(100))
    token_symbol = Column(String(80))
    ai_report = Column(Text)
    published_telegram = Column(Boolean, default=False)
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    __table_args__ = (UniqueConstraint("post_date", "slot", "kind", name="uq_monitor_daily_post_slot"),)

def init_monitor_tables():
    """Create only the Phase-2 monitoring tables in the shared database."""
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required for the monitor")

    # The stable core already creates its tables when configured. Reuse that
    # engine when possible so both services point at exactly the same DB.
    engine = getattr(core, "engine", None)
    if engine is None:
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        if url.startswith("postgresql://"):
            url = url.replace("postgresql://", "postgresql+psycopg2://", 1)
        engine = create_engine(url, pool_pre_ping=True)

    MonitorBase.metadata.create_all(engine)

    # SQLAlchemy create_all() does not alter an existing table. Whale Detection
    # was introduced after some deployments had already created
    # monitor_whale_events, so explicitly add any missing columns here. This is
    # intentionally small and idempotent: existing data is preserved.
    whale_columns = {
        "transfer_event_id": "INTEGER",
        "chain_id": "VARCHAR(20)",
        "network": "VARCHAR(40)",
        "token_address": "VARCHAR(100)",
        "tx_hash": "VARCHAR(100)",
        "whale_wallet": "VARCHAR(100)",
        "whale_role": "VARCHAR(20)",
        "amount_ui": "DOUBLE PRECISION",
        "value_usd": "DOUBLE PRECISION",
        "balance_delta_ui": "DOUBLE PRECISION",
        "transfer_type": "VARCHAR(30)",
        "detected_at": "TIMESTAMP WITH TIME ZONE",
        "source": "VARCHAR(40)",
        "telegram_alerted": "BOOLEAN DEFAULT FALSE",
    }
    try:
        with engine.begin() as conn:
            existing = {
                row[0]
                for row in conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND table_name = 'monitor_whale_events'"
                ))
            }
            for column_name, column_type in whale_columns.items():
                if column_name not in existing:
                    conn.execute(text(
                        f'ALTER TABLE monitor_whale_events ADD COLUMN "{column_name}" {column_type}'
                    ))
                    print(f"🛠️ DB migration: added monitor_whale_events.{column_name}")

            # Telegram channel publication flag. Idempotent so existing monitor
            # databases are upgraded without touching existing event data.
            event_columns = {
                row[0]
                for row in conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema = current_schema() AND table_name = 'monitor_events'"
                ))
            }
            if "published_telegram" not in event_columns:
                conn.execute(text(
                    'ALTER TABLE monitor_events ADD COLUMN "published_telegram" BOOLEAN DEFAULT FALSE'
                ))
                print("🛠️ DB migration: added monitor_events.published_telegram")

            # Older Whale Detection versions may have extra NOT NULL columns
            # (for example whale_side) that are no longer part of the current
            # model. Keep those historical columns for compatibility, but make
            # them nullable so the current model can insert new evidence rows.
            model_columns = set(whale_columns) | {"id"}
            extra_required = conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema = current_schema() "
                "AND table_name = 'monitor_whale_events' "
                "AND is_nullable = 'NO'"
            )).fetchall()
            for row in extra_required:
                column_name = row[0]
                if column_name not in model_columns and column_name != "id":
                    conn.execute(text(
                        f'ALTER TABLE monitor_whale_events ALTER COLUMN "{column_name}" DROP NOT NULL'
                    ))
                    print(f"🛠️ DB migration: relaxed legacy monitor_whale_events.{column_name}")
    except Exception as exc:
        print(f"❌ Monitor DB migration failed: {exc}")
        raise

    return engine


MONITOR_ENGINE = init_monitor_tables()
MonitorSession = sessionmaker(autocommit=False, autoflush=False, bind=MONITOR_ENGINE)

# ============================================================
# WATCHLIST
# ============================================================


def build_watchlist():
    """Validate the in-code watchlist against the networks exposed by the core."""
    items = []
    supported = getattr(core, "NETWORKS", {})

    for entry in WATCHLIST_CONFIG:
        if not entry.get("enabled", True):
            continue

        name = str(entry.get("name") or "UNKNOWN").strip()
        network = str(entry.get("network") or "").strip().lower()
        address = str(entry.get("address") or "").strip()

        if network not in supported:
            print(f"⚠️ Unsupported monitor network for {name}: {network}")
            continue
        if not address:
            print(f"⚠️ Missing monitor address for {name}")
            continue

        item = {"name": name, "network": network, "address": address}
        items.append(item)

    return items


WATCHLIST = build_watchlist()

if TRANSFER_MONITOR_ENABLED:
    print("🔗 Transfer Monitor: ENABLED | ERC-20 topic verified")

# ============================================================
# GENERIC HELPERS
# ============================================================


def now_utc():
    return datetime.now(timezone.utc)


MONITOR_PROCESS_START_TIME = now_utc()


def safe_float(value):
    try:
        if value is None:
            return None
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def pct_change(current, previous):
    current = safe_float(current)
    previous = safe_float(previous)
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / abs(previous) * 100.0


def delta(current, previous):
    current = safe_float(current)
    previous = safe_float(previous)
    if current is None or previous is None:
        return None
    return current - previous


def signed_pct(value):
    return "Not available" if value is None else f"{value:+.2f}%"


def snapshot_integrity(current, previous):
    """Validate continuity before comparing market snapshots.

    Market deltas are only trustworthy when both snapshots refer to the same
    DEX pair and are reasonably close in time. A pair switch or a very stale
    previous snapshot can otherwise create fake -90%/-99% moves.
    """
    reasons = []
    if current is None or previous is None:
        return False, ["missing_snapshot"]

    current_pair = getattr(current, "pair_address", None)
    previous_pair = getattr(previous, "pair_address", None)
    current_pair = str(current_pair).strip().lower() if current_pair else None
    previous_pair = str(previous_pair).strip().lower() if previous_pair else None

    if not current_pair or not previous_pair:
        reasons.append("pair_unverified")
    elif current_pair != previous_pair:
        reasons.append("pair_changed")

    current_ts = getattr(current, "snapshot_time", None)
    previous_ts = getattr(previous, "snapshot_time", None)
    if current_ts and previous_ts:
        try:
            gap_seconds = abs((current_ts - previous_ts).total_seconds())
            # The monitor normally samples every 5 minutes. Do not compare a
            # fresh snapshot with a much older snapshot after an outage/restart.
            if gap_seconds > max(1800, MONITOR_INTERVAL_SECONDS * 3):
                reasons.append("stale_snapshot_gap")
        except Exception:
            reasons.append("timestamp_unverified")
    else:
        reasons.append("timestamp_unverified")

    return not reasons, reasons


def latest_two_snapshots(session, address, network):
    addr = normalize_address(address)
    cid = chain_id_for(network)
    rows = (
        session.query(MarketSnapshot)
        .filter(
            MarketSnapshot.chain_id == cid,
            MarketSnapshot.token_address == addr,
        )
        .order_by(MarketSnapshot.snapshot_time.desc())
        .limit(2)
        .all()
    )
    return rows[0] if rows else None, rows[1] if len(rows) > 1 else None


def holder_snapshot_map(session, address, network, snapshot_time=None):
    addr = normalize_address(address)
    cid = chain_id_for(network)
    q = session.query(HolderSnapshot).filter(
        HolderSnapshot.chain_id == cid,
        HolderSnapshot.token_address == addr,
    )
    if snapshot_time is not None:
        q = q.filter(HolderSnapshot.snapshot_time == snapshot_time)
    rows = q.order_by(HolderSnapshot.snapshot_time.desc()).limit(10).all()
    return {
        normalize_address(row.wallet): row
        for row in rows
        if row.wallet
    }


def latest_holder_snapshots(session, address, network):
    """Return current and previous holder snapshots as two time groups.

    HolderSnapshot has one row per wallet, so we identify the two most recent
    distinct snapshot timestamps rather than assuming one row represents a
    complete snapshot.
    """
    addr = normalize_address(address)
    cid = chain_id_for(network)
    rows = (
        session.query(HolderSnapshot)
        .filter(
            HolderSnapshot.chain_id == cid,
            HolderSnapshot.token_address == addr,
        )
        .order_by(HolderSnapshot.snapshot_time.desc())
        .limit(40)
        .all()
    )
    groups = []
    seen = set()
    for row in rows:
        ts = row.snapshot_time
        key = ts.isoformat() if ts else None
        if key in seen:
            continue
        seen.add(key)
        groups.append(ts)
        if len(groups) >= 2:
            break
    if not groups:
        return {}, {}
    current = holder_snapshot_map(session, address, network, groups[0])
    previous = holder_snapshot_map(session, address, network, groups[1]) if len(groups) > 1 else {}
    return current, previous

# ============================================================
# ACTIVITY DETECTION
# ============================================================


def detect_activity(current, previous, holder_current, holder_previous):
    """Detect unusual activity using only verified stored snapshots.

    This is a trigger engine, not an investment score and not a prediction.
    Missing values remain unavailable and contribute no points.
    """
    if current is None:
        return None

    signals = []
    score = 0.0

    market_integrity_ok, integrity_reasons = snapshot_integrity(current, previous)

    def add_signal(name, value, points, detail):
        nonlocal score
        if value:
            score += points
            signals.append({"name": name, "points": points, "detail": detail})

    if market_integrity_ok:
        # 1) Short-term volume acceleration.
        vol5 = pct_change(current.volume_5m_usd, previous.volume_5m_usd if previous else None)
        vol1 = pct_change(current.volume_1h_usd, previous.volume_1h_usd if previous else None)
        add_signal(
            "volume_spike_5m",
            vol5 is not None and vol5 >= 100,
            20,
            f"5m volume {signed_pct(vol5)}",
        )
        add_signal(
            "volume_spike_1h",
            vol1 is not None and vol1 >= 75,
            10,
            f"1h volume {signed_pct(vol1)}",
        )

        # 2) Trade-count acceleration and buy/sell imbalance.
        buys5 = delta(current.buys_5m, previous.buys_5m if previous else None)
        sells5 = delta(current.sells_5m, previous.sells_5m if previous else None)
        buys5_current = safe_float(current.buys_5m)
        sells5_current = safe_float(current.sells_5m)
        total_current = (buys5_current or 0) + (sells5_current or 0)
        buy_ratio = ((buys5_current / total_current) if buys5_current is not None and total_current else None)
        add_signal(
            "trade_activity",
            buys5 is not None and sells5 is not None and abs(buys5) + abs(sells5) >= 25,
            10,
            f"5m buys Δ {buys5 if buys5 is not None else 'N/A'}, sells Δ {sells5 if sells5 is not None else 'N/A'}",
        )
        add_signal(
            "buy_pressure",
            buy_ratio is not None and buy_ratio >= 0.65,
            10,
            f"5m buy share {(buy_ratio * 100):.1f}%" if buy_ratio is not None else "5m buy share N/A",
        )

        # 3) Liquidity movement.
        liq = pct_change(current.liquidity_usd, previous.liquidity_usd if previous else None)
        add_signal(
            "liquidity_movement",
            liq is not None and abs(liq) >= 15,
            10,
            f"Liquidity {signed_pct(liq)}",
        )

        # 4) Price acceleration. This is deliberately symmetric: a sharp move
        # can be an upside or downside event and should be analyzed by AI.
        price = pct_change(current.price_usd, previous.price_usd if previous else None)
        add_signal(
            "price_move",
            price is not None and abs(price) >= 5,
            10,
            f"Price {signed_pct(price)}",
        )

        # 5) Market-cap movement.
        mc = pct_change(current.market_cap_usd, previous.market_cap_usd if previous else None)
        add_signal(
            "market_cap_move",
            mc is not None and abs(mc) >= 5,
            5,
            f"Market cap {signed_pct(mc)}",
        )
    else:
        # Do not turn pair/source discontinuity into a market event.
        vol5 = vol1 = liq = price = mc = None
        buys5 = sells5 = buy_ratio = None
        signals.append({
            "name": "data_integrity_guard",
            "points": 0,
            "detail": "Market delta suppressed: " + ", ".join(integrity_reasons),
        })

    # 6) Top-holder balance/percentage movement.
    # Important: this is NOT a transfer-indexer claim. It is a change in the
    # holder data returned by the stable MARSOF engine.
    holder_changes = []
    for wallet, cur in holder_current.items():
        prev = holder_previous.get(wallet)
        if not prev:
            continue
        balance_change = pct_change(cur.balance, prev.balance)
        pct_point_change = delta(cur.percentage, prev.percentage)
        if balance_change is not None and abs(balance_change) >= 10:
            holder_changes.append({
                "wallet": wallet,
                "balance_change_pct": balance_change,
                "percentage_point_change": pct_point_change,
                "tag": cur.tag,
            })

    add_signal(
        "top_holder_movement",
        bool(holder_changes),
        20,
        f"{len(holder_changes)} top-holder balance changes ≥10% detected",
    )

    # 7) Holder concentration shift.
    current_top_pct = sum(
        safe_float(x.percentage) or 0 for x in holder_current.values()
        if (safe_float(x.percentage) is not None)
    ) if holder_current else None
    previous_top_pct = sum(
        safe_float(x.percentage) or 0 for x in holder_previous.values()
        if (safe_float(x.percentage) is not None)
    ) if holder_previous else None
    concentration_change = delta(current_top_pct, previous_top_pct)
    add_signal(
        "holder_concentration_shift",
        concentration_change is not None and abs(concentration_change) >= 3,
        5,
        f"Top-holder concentration change {concentration_change:+.2f} percentage points" if concentration_change is not None else "Not available",
    )

    # Build a neutral, evidence-only pattern label for downstream reporting.
    # This does not change the activity score or trigger threshold.
    pattern = "MIXED_ACTIVITY"
    if price is not None and buy_ratio is not None:
        if price >= 5 and buy_ratio >= 0.65:
            pattern = "UPSIDE_MOMENTUM"
        elif price <= -5 and buy_ratio <= 0.35:
            pattern = "DOWNSIDE_PRESSURE"
    if liq is not None and liq <= -15:
        pattern = "LIQUIDITY_OUTFLOW"
    elif liq is not None and liq >= 15 and buy_ratio is not None and buy_ratio >= 0.65:
        pattern = "LIQUIDITY_INFLOW"

    evidence_count = len(signals)

    # Keep score bounded for easy trigger thresholds.
    score = min(100.0, score)

    if score >= 70:
        severity = "MAJOR"
    elif score >= 50:
        severity = "HIGH"
    elif score >= 30:
        severity = "ELEVATED"
    else:
        severity = "NORMAL"

    return {
        "activity_score": round(score, 1),
        "severity": severity,
        "pattern": pattern,
        "evidence_count": evidence_count,
        "signals": signals,
        "metrics": {
            "data_integrity_ok": market_integrity_ok,
            "data_integrity_reasons": integrity_reasons,
            "volume_5m_change_pct": vol5,
            "volume_1h_change_pct": vol1,
            "liquidity_change_pct": liq,
            "price_change_pct": price,
            "market_cap_change_pct": mc,
            "buy_share_5m_pct": buy_ratio * 100 if buy_ratio is not None else None,
            "buys_5m_delta": buys5,
            "sells_5m_delta": sells5,
            "top_holder_changes": holder_changes,
            "top_holder_concentration_change_pp": concentration_change,
        },
    }

# ============================================================
# TOKEN IDENTITY RESOLUTION
# ============================================================

def _clean_identity(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in {"unknown", "n/a", "na", "none", "null", "not available"}:
        return None
    return text


# Successful identities are cached for the lifetime of the monitor process so
# we do not hit chain metadata endpoints on every monitoring cycle.
_IDENTITY_CACHE = {}


def _identity_from_dict(data):
    """Extract token name/symbol only from token metadata structures."""
    if not isinstance(data, dict):
        return None, None, None

    for container in (
        data,
        data.get("metadata"),
        data.get("_onchain"),
        data.get("token"),
        data.get("token_info"),
        data.get("content"),
    ):
        if not isinstance(container, dict):
            continue
        name = (
            _clean_identity(container.get("token_name"))
            or _clean_identity(container.get("name"))
        )
        symbol = (
            _clean_identity(container.get("token_symbol"))
            or _clean_identity(container.get("symbol"))
        )
        if name or symbol:
            source = _clean_identity(container.get("_source")) or "On-chain token metadata"
            return name, symbol, source
    return None, None, None


def _call_core_identity_fetcher(name, *args):
    """Call a metadata fetcher exposed by the stable MARSOF core."""
    fn = getattr(core, name, None)
    if not callable(fn):
        return None
    try:
        return fn(*args)
    except Exception as exc:
        print(f"ℹ️ Token identity source {name} unavailable: {exc}")
        return None


def resolve_token_identity(result, item, session):
    """Resolve the real token identity from the contract/on-chain metadata.

    Priority:
      1. Chain-specific on-chain metadata from the stable MARSOF core.
      2. Persisted MARSOF token metadata.
      3. DEX symbol only as a last-resort partial identity.

    WATCHLIST aliases such as BASE-1/SOLANA-1 are never used as the token
    name or symbol.
    """
    address = normalize_address(item["address"])
    network = item["network"]
    cache_key = f"{network}:{address}"

    cached = _IDENTITY_CACHE.get(cache_key)
    if cached:
        return dict(cached)

    name = symbol = source = None

    # 1) Chain-specific metadata directly from the stable core.
    # EVM: ERC-20 name()/symbol() via eth_call.
    if network not in {"solana", "sui"}:
        metadata = _call_core_identity_fetcher("fetch_evm_metadata", address, network)
        name, symbol, source = _identity_from_dict(metadata)
        if name or symbol:
            source = "EVM RPC (contract metadata)"

    # Solana: Helius DAS reads the mint's token metadata. Birdeye is an
    # additional metadata source when its key is configured.
    if network == "solana" and not name:
        metadata = _call_core_identity_fetcher("fetch_helius_token", address)
        name, symbol, source = _identity_from_dict(metadata)
        if name or symbol:
            source = "Helius on-chain metadata"

        if not name:
            metadata = _call_core_identity_fetcher("fetch_birdeye_token", address)
            b_name, b_symbol, _ = _identity_from_dict(metadata)
            name = name or b_name
            symbol = symbol or b_symbol
            if b_name or b_symbol:
                source = "Birdeye token metadata"

    # Sui: the standard coin metadata RPC is required for coin types such as
    # 0x2::sui::SUI; sui_getObject alone cannot resolve a coin type reliably.
    if network == "sui" and not name:
        sui_rpc = getattr(core, "sui_rpc", None)
        metadata = None
        if callable(sui_rpc):
            try:
                raw = sui_rpc("suix_getCoinMetadata", [address])
                metadata = (raw or {}).get("result") if isinstance(raw, dict) else None
            except Exception as exc:
                print(f"ℹ️ Sui coin metadata unavailable for {address}: {exc}")
        if isinstance(metadata, dict):
            name = _clean_identity(metadata.get("name"))
            symbol = _clean_identity(metadata.get("symbol"))
            if name or symbol:
                source = "Sui on-chain coin metadata"

        if not name:
            metadata = _call_core_identity_fetcher("fetch_sui_metadata", address)
            s_name, s_symbol, _ = _identity_from_dict(metadata)
            name = name or s_name
            symbol = symbol or s_symbol
            if s_name or s_symbol:
                source = source or "Sui RPC"

    # 2) Existing persisted MARSOF token metadata.
    if Token is not None and (not name or not symbol):
        try:
            token = (
                session.query(Token)
                .filter(
                    Token.contract_address == address,
                    Token.chain_id == chain_id_for(network),
                )
                .first()
            )
            if token:
                db_name = _clean_identity(getattr(token, "name", None))
                db_symbol = _clean_identity(getattr(token, "symbol", None))
                name = name or db_name
                symbol = symbol or db_symbol
                if db_name or db_symbol:
                    source = source or "MARSOF token metadata"
        except Exception as exc:
            print(f"ℹ️ Token identity DB lookup unavailable for {network}:{address}: {exc}")

    # 3) Core result metadata, if the analysis already exposed it.
    if not name or not symbol:
        security = (result or {}).get("security") if isinstance(result, dict) else None
        r_name, r_symbol, r_source = _identity_from_dict(security)
        name = name or r_name
        symbol = symbol or r_symbol
        source = source or r_source

    # 4) Last resort: DEX Screener symbol. This is explicitly PARTIAL and is
    # never treated as a contract-derived name.
    if not symbol and isinstance(result, dict):
        market = result.get("market") or {}
        for check in market.get("checks") or []:
            if not isinstance(check, dict):
                continue
            label = str(check.get("label") or "").strip().lower()
            if label == "base token":
                symbol = _clean_identity(check.get("detail")) or _clean_identity(check.get("value"))
                if symbol:
                    source = source or "DEX Screener symbol (partial)"
                    break

    identity = {
        "name": name,
        "symbol": symbol,
        "source": source or "Unavailable",
        "resolved": bool(name or symbol),
    }

    # Cache only a real identity; do not cache failures so a later cycle can
    # retry metadata resolution after a provider becomes available.
    if name or symbol:
        _IDENTITY_CACHE[cache_key] = dict(identity)

    return identity

# ============================================================
# ON-CHAIN TRANSFER MONITOR
# ============================================================


def _core_network_cfg(network):
    networks = getattr(core, "NETWORKS", {}) or {}
    return networks.get(network) or {}


def _normalize_evm_address(value):
    if not isinstance(value, str) or not value.startswith("0x") or len(value) != 42:
        return None
    return value.lower()


def _evm_transfer_cursor(session, address, network):
    addr = normalize_address(address)
    cid = chain_id_for(network)
    row = (session.query(TransferCursor)
           .filter(TransferCursor.chain_id == cid,
                   TransferCursor.network == network,
                   TransferCursor.token_address == addr)
           .first())
    if row is None:
        row = TransferCursor(chain_id=cid, network=network, token_address=addr)
        session.add(row)
        session.flush()
    return row


def _evm_rpc(network, method, params):
    cfg = _core_network_cfg(network)
    rpc = cfg.get("rpc")
    if not rpc:
        return None
    response = requests.post(
        rpc,
        json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
        timeout=15,
    )
    response.raise_for_status()
    obj = response.json()
    if isinstance(obj, dict) and obj.get("error"):
        raise RuntimeError(str(obj["error"])[:500])
    return obj.get("result") if isinstance(obj, dict) else None


def _evm_token_decimals(address, network):
    key = (network, normalize_address(address))
    if key in _DECIMALS_CACHE:
        return _DECIMALS_CACHE[key]
    try:
        raw = _evm_rpc(network, "eth_call", [{"to": address, "data": "0x313ce567"}, "latest"])
        if isinstance(raw, str) and raw.startswith("0x"):
            value = int(raw, 16)
            if 0 <= value <= 36:
                _DECIMALS_CACHE[key] = value
                return value
    except Exception:
        pass
    _DECIMALS_CACHE[key] = None
    return None


def _hex_int(value):
    try:
        return int(value, 16) if isinstance(value, str) else int(value)
    except (TypeError, ValueError):
        return None


def _topic_address(topic):
    if not isinstance(topic, str) or len(topic) < 42:
        return None
    return _normalize_evm_address("0x" + topic[-40:])


def _classify_transfer(sender, receiver, dex_addresses=None):
    dex_addresses = {_normalize_evm_address(x) for x in (dex_addresses or [])}
    dex_addresses.discard(None)
    sender = _normalize_evm_address(sender)
    receiver = _normalize_evm_address(receiver)
    if sender in dex_addresses and receiver not in dex_addresses:
        return "BUY"
    if receiver in dex_addresses and sender not in dex_addresses:
        return "SELL"
    return "TRANSFER"


def _extract_dex_addresses(result):
    addresses = set()
    market = (result or {}).get("market") or {}
    pair = market.get("pair")
    if isinstance(pair, dict):
        for key in ("pairAddress", "pair_address", "address"):
            value = pair.get(key)
            if value:
                addresses.add(value)
    data = market.get("data") or {}
    if isinstance(data, dict):
        for key in ("pair_address", "pairAddress"):
            value = data.get(key)
            if value:
                addresses.add(value)
    return addresses


def _eth_get_logs_resilient(network, token_address, start, end):
    """Fetch ERC-20 logs with progressively smaller ranges.

    Some public RPC providers reject larger eth_getLogs windows even when the
    request itself is valid. Shrinking the window preserves correctness and
    avoids turning a provider limit into a false transfer-monitor failure.
    """
    if start > end:
        return [], start, end

    cursor = start
    all_logs = []
    window = max(1, end - start + 1)
    while cursor <= end:
        chunk_end = min(end, cursor + window - 1)
        try:
            logs = _evm_rpc(network, "eth_getLogs", [{
                "address": token_address,
                "fromBlock": hex(cursor),
                "toBlock": hex(chunk_end),
                "topics": [ERC20_TRANSFER_TOPIC],
            }]) or []
            all_logs.extend(logs)
            cursor = chunk_end + 1
            # Gradually restore a larger window after successful calls.
            window = min(max(window * 2, 1), TRANSFER_MAX_BLOCKS_PER_CYCLE)
        except Exception as exc:
            if window <= 1:
                raise
            new_window = max(1, window // 2)
            print(
                f"ℹ️ Transfer log range reduced for {network}: "
                f"{window}→{new_window} blocks ({str(exc)[:180]})"
            )
            window = new_window
    return all_logs, start, end


def scan_evm_transfers(session, item, current=None, analysis_result=None):
    """Read ERC-20 Transfer logs directly from the configured EVM RPC.

    This layer records only verifiable on-chain events. It never guesses BUY/SELL
    unless a verified DEX pair address is available from the current MARSOF market data.
    Networks without a compatible EVM log endpoint are explicitly marked
    unavailable rather than sending malformed requests.
    """
    network = item.get("network")
    evm_networks = {n for n, c in _core_network_cfgs() if isinstance(c, dict) and c.get("kind") == "evm"}
    # Hyperliquid's EVM-like address space does not imply support for the
    # standard eth_getLogs method on its configured RPC. Keep it out of this
    # generic ERC-20 scanner until a network-specific endpoint is verified.
    if network == "hyperliquid":
        return {"status": "UNAVAILABLE", "events": 0, "reason": "network_specific_transfer_indexer_required"}
    if not TRANSFER_MONITOR_ENABLED or network not in evm_networks:
        return {"status": "UNAVAILABLE", "events": 0, "reason": "network_not_supported"}
    address = normalize_address(item["address"])
    token_address = _normalize_evm_address(address)
    if not token_address:
        return {"status": "UNAVAILABLE", "events": 0, "reason": "not_evm_token"}
    cursor = _evm_transfer_cursor(session, address, network)
    latest_hex = _evm_rpc(network, "eth_blockNumber", [])
    latest = _hex_int(latest_hex)
    if latest is None:
        return {"status": "UNAVAILABLE", "events": 0, "reason": "block_number_unavailable"}
    start = (cursor.last_block + 1) if cursor.last_block is not None else max(0, latest - TRANSFER_SCAN_LOOKBACK_BLOCKS + 1)
    end = min(latest, start + TRANSFER_MAX_BLOCKS_PER_CYCLE - 1)
    if start > end:
        return {"status": "OK", "events": 0, "from_block": start, "to_block": end}
    logs, _, _ = _eth_get_logs_resilient(network, token_address, start, end)
    decimals = _evm_token_decimals(token_address, network)
    price = safe_float(getattr(current, "price_usd", None)) if current else None
    dex_addresses = _extract_dex_addresses(analysis_result)
    added = 0
    for log in logs[-TRANSFER_MAX_EVENTS_PER_TOKEN:]:
        tx_hash = log.get("transactionHash")
        log_index = _hex_int(log.get("logIndex"))
        block_number = _hex_int(log.get("blockNumber"))
        topics_raw = log.get("topics") or []
        if not tx_hash or len(topics_raw) < 3:
            continue
        sender = _topic_address(topics_raw[1])
        receiver = _topic_address(topics_raw[2])
        data = log.get("data") or "0x"
        raw_amount = str(_hex_int(data) or 0)
        amount_ui = None
        value_usd = None
        if decimals is not None:
            try:
                amount_ui = int(raw_amount) / (10 ** decimals)
            except (TypeError, ValueError, OverflowError):
                amount_ui = None
        if amount_ui is not None and price is not None:
            value_usd = amount_ui * price
        exists = (session.query(TransferEvent)
                  .filter(TransferEvent.network == network,
                          TransferEvent.token_address == address,
                          TransferEvent.tx_hash == tx_hash,
                          TransferEvent.log_index == log_index)
                  .first())
        if exists:
            continue
        session.add(TransferEvent(
            chain_id=chain_id_for(network), network=network, token_address=address,
            tx_hash=tx_hash, log_index=log_index, block_number=block_number,
            sender=sender, receiver=receiver, raw_amount=raw_amount,
            amount_ui=amount_ui, price_usd=price, value_usd=value_usd,
            transfer_type=_classify_transfer(sender, receiver, dex_addresses),
            source="EVM RPC",
        ))
        added += 1
    cursor.last_block = end
    session.flush()
    return {"status": "OK", "events": added, "from_block": start, "to_block": end, "raw_logs": len(logs)}



def _sui_graphql_urls():
    """Return ordered Sui GraphQL endpoints.

    The current Sui Foundation endpoint is first. Operators can provide one
    or more production provider endpoints through MARSOF_SUI_GRAPHQL_URLS
    (comma-separated), or a single MARSOF_SUI_GRAPHQL_URL.
    """
    configured = os.getenv("MARSOF_SUI_GRAPHQL_URLS", "").strip()
    single = os.getenv("MARSOF_SUI_GRAPHQL_URL", "").strip()
    urls = [u.strip() for u in configured.split(",") if u.strip()]
    if single:
        urls.append(single)
    urls.append("https://graphql.mainnet.sui.io/graphql")
    seen = set()
    return [u for u in urls if not (u in seen or seen.add(u))]


def _sui_graphql(query, variables=None):
    import requests
    timeout = max(10, int(os.getenv("MARSOF_SUI_GRAPHQL_TIMEOUT", "20")))
    retries = max(1, min(3, int(os.getenv("MARSOF_SUI_GRAPHQL_RETRIES", "2"))))
    errors = []
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "MARSOF-AI/1.0 SuiTransferMonitor",
        "x-sui-rpc-show-usage": "true",
    }
    for url in _sui_graphql_urls():
        for attempt in range(retries):
            try:
                response = requests.post(
                    url,
                    json={"query": query, "variables": variables or {}},
                    headers=headers,
                    timeout=timeout,
                )
                if response.status_code in (429, 500, 502, 503, 504):
                    detail = response.text[:300].replace("\n", " ")
                    errors.append(f"{url} HTTP {response.status_code}: {detail}")
                    if attempt + 1 < retries:
                        time.sleep(min(2 * (attempt + 1), 5))
                        continue
                    break
                if response.status_code >= 400:
                    errors.append(f"{url} HTTP {response.status_code}: {response.text[:300]}")
                    break
                payload = response.json()
                if payload.get("errors"):
                    messages = "; ".join(str(e.get("message") or e) for e in payload["errors"][:3])
                    errors.append(f"{url} GraphQL: {messages}")
                    break
                return payload.get("data") or {}
            except Exception as exc:
                errors.append(f"{url}: {type(exc).__name__}: {str(exc)[:220]}")
                if attempt + 1 < retries:
                    time.sleep(min(2 * (attempt + 1), 5))
    raise RuntimeError(" | ".join(errors[-6:]))


def _sui_transfer_cursor(session, address, network="sui"):
    return _evm_transfer_cursor(session, address, network)


def scan_sui_transfers(session, item, current=None, analysis_result=None):
    """Scan recent Sui transaction balance changes through GraphQL.

    Sui no longer relies on the legacy public fullnode JSON-RPC surface for
    current mainnet data. GraphQL exposes transaction effects and balance
    changes, which lets MARSOF record verifiable token movements without
    inventing BUY/SELL classifications.
    """
    if not TRANSFER_MONITOR_ENABLED or item.get("network") != "sui":
        return {"status": "UNAVAILABLE", "events": 0, "reason": "network_not_supported"}

    token_type = str(item.get("address") or "").strip()
    if "::" not in token_type:
        return {"status": "UNAVAILABLE", "events": 0, "reason": "invalid_sui_coin_type"}

    # Query a bounded recent window. Existing DB rows prevent duplicates, so
    # this remains safe even when cycles overlap.
    limit = max(10, min(50, int(os.getenv("MONITOR_SUI_TRANSFER_TX_LIMIT", "50"))))
    query = """
    query ($limit: Int!) {
      transactions(last: $limit) {
        nodes {
          digest
          sender { address }
          effects {
            timestamp
            balanceChanges(first: 50) {
              nodes {
                amount
                coinType { repr }
                owner { address }
              }
            }
          }
        }
      }
    }
    """

    try:
        data = _sui_graphql(query, {"limit": limit})
    except Exception as exc:
        return {"status": "UNAVAILABLE", "events": 0, "reason": "graphql_unavailable", "detail": str(exc)[:600]}

    transactions = ((data.get("transactions") or {}).get("nodes") or [])
    price = safe_float(getattr(current, "price_usd", None)) if current else None
    decimals = None
    try:
        # Reuse MARSOF core metadata where available; no default decimal value.
        metadata_rpc = getattr(core, "sui_rpc", None)
        if callable(metadata_rpc):
            raw = metadata_rpc("suix_getCoinMetadata", [token_type])
            meta = (raw or {}).get("result") if isinstance(raw, dict) else None
            decimals = int(meta.get("decimals")) if isinstance(meta, dict) and meta.get("decimals") is not None else None
    except Exception:
        decimals = None

    added = 0
    for tx in transactions:
        digest = tx.get("digest")
        if not digest:
            continue
        effects = tx.get("effects") or {}
        changes = ((effects.get("balanceChanges") or {}).get("nodes") or [])
        relevant = []
        for change in changes:
            coin_type = ((change.get("coinType") or {}).get("repr") or "").strip()
            if coin_type.lower() != token_type.lower():
                continue
            owner = ((change.get("owner") or {}).get("address") or "").strip()
            amount_raw = change.get("amount")
            if not owner or amount_raw is None:
                continue
            try:
                amount_int = int(amount_raw)
            except (TypeError, ValueError):
                continue
            if amount_int:
                relevant.append((owner, amount_int))

        if not relevant:
            continue

        negatives = [(owner, amount) for owner, amount in relevant if amount < 0]
        positives = [(owner, amount) for owner, amount in relevant if amount > 0]
        if not negatives or not positives:
            continue

        sender, neg_amount = min(negatives, key=lambda x: x[1])
        receiver, pos_amount = max(positives, key=lambda x: x[1])
        raw_amount_int = min(abs(neg_amount), pos_amount)
        amount_ui = None
        value_usd = None
        if decimals is not None:
            amount_ui = raw_amount_int / (10 ** decimals)
            if price is not None:
                value_usd = amount_ui * price

        exists = (session.query(TransferEvent)
                  .filter(TransferEvent.network == "sui",
                          TransferEvent.token_address == token_type,
                          TransferEvent.tx_hash == digest)
                  .first())
        if exists:
            continue

        # Balance changes prove movement, but they do not by themselves prove
        # a market BUY/SELL. Keep the event as TRANSFER unless later evidence
        # identifies a DEX pair.
        transfer_type = "TRANSFER"
        dex_addresses = _extract_dex_addresses(analysis_result)
        if sender.lower() in dex_addresses and receiver.lower() not in dex_addresses:
            transfer_type = "BUY"
        elif receiver.lower() in dex_addresses and sender.lower() not in dex_addresses:
            transfer_type = "SELL"

        timestamp = effects.get("timestamp")
        session.add(TransferEvent(
            chain_id=chain_id_for("sui"), network="sui", token_address=token_type,
            tx_hash=digest, log_index=0, block_number=None,
            sender=sender, receiver=receiver, raw_amount=str(raw_amount_int),
            amount_ui=amount_ui, price_usd=price, value_usd=value_usd,
            transfer_type=transfer_type, source="Sui GraphQL",
        ))
        added += 1

    session.flush()
    return {
        "status": "OK", "events": added,
        "transactions_scanned": len(transactions),
        "coin_type": token_type,
    }


def _core_network_cfgs():
    networks = getattr(core, "NETWORKS", {}) or {}
    return list(networks.items())


def detect_whale_events(session, network, address):
    """Promote only real transfer rows with a verified USD value to whale evidence.

    No USD value means no whale classification. The threshold is configurable
    but defaults to $10,000. Sender/receiver delta is the transfer amount for
    the whale wallet; it is not presented as a full wallet-balance snapshot.
    """
    if WHALE_THRESHOLD_USD <= 0:
        return []
    addr = normalize_address(address)
    rows = (session.query(TransferEvent)
            .filter(TransferEvent.network == network,
                    TransferEvent.token_address == addr,
                    TransferEvent.value_usd.isnot(None),
                    TransferEvent.value_usd >= WHALE_THRESHOLD_USD)
            .order_by(TransferEvent.id.asc())
            .all())
    detected = []
    for transfer in rows:
        exists = (session.query(WhaleEvent)
                  .filter(WhaleEvent.transfer_event_id == transfer.id)
                  .first())
        if exists:
            continue
        sender = normalize_address(transfer.sender) if transfer.sender else ""
        receiver = normalize_address(transfer.receiver) if transfer.receiver else ""
        # A transfer can involve two large wallets, but we record both sides
        # independently when the same verified USD transfer meets the threshold.
        candidates = []
        if sender:
            candidates.append((sender, "SENDER", -(safe_float(transfer.amount_ui) or 0.0)))
        if receiver and receiver != sender:
            candidates.append((receiver, "RECEIVER", safe_float(transfer.amount_ui) or 0.0))
        for wallet, role, balance_delta in candidates:
            row = WhaleEvent(
                transfer_event_id=transfer.id,
                chain_id=chain_id_for(network),
                network=network,
                token_address=addr,
                tx_hash=transfer.tx_hash,
                whale_wallet=wallet,
                whale_role=role,
                amount_ui=transfer.amount_ui,
                value_usd=transfer.value_usd,
                balance_delta_ui=balance_delta if transfer.amount_ui is not None else None,
                transfer_type=transfer.transfer_type,
                detected_at=now_utc(),
                source=transfer.source,
            )
            session.add(row)
            detected.append(row)
    if detected:
        session.flush()
    return detected


# ============================================================
# COOLDOWN / EVENT STORAGE
# ============================================================


def get_monitor_state(session, address, network):
    addr = normalize_address(address)
    cid = chain_id_for(network)
    row = (
        session.query(MonitorState)
        .filter(
            MonitorState.chain_id == cid,
            MonitorState.network == network,
            MonitorState.token_address == addr,
        )
        .first()
    )
    if row is None:
        row = MonitorState(
            chain_id=cid,
            network=network,
            token_address=addr,
        )
        session.add(row)
        session.flush()
    return row


def cooldown_active(state):
    if not state.last_event_at:
        return False
    return now_utc() - state.last_event_at < timedelta(minutes=MONITOR_COOLDOWN_MINUTES)


def count_whale_evidence(session, network, address, since=None):
    """Count only persisted real whale transfers for confirmation evidence."""
    query = (session.query(WhaleEvent)
             .filter(WhaleEvent.network == network,
                     WhaleEvent.token_address == normalize_address(address)))
    if since is not None:
        query = query.filter(WhaleEvent.detected_at >= since)
    return query.count()


def confirm_monitor_event(session, event, activity, current, previous, address, network):
    """Confirm an event only from fresh, integrity-checked evidence.

    Confirmation requires either two independent market signals with a valid
    snapshot pair, or one valid market signal plus a verified whale transfer.
    No future prediction or synthetic evidence is used.
    """
    evidence_count = int(activity.get("evidence_count") or len(activity.get("signals") or []))
    integrity_ok, integrity_reasons = snapshot_integrity(current, previous)
    whale_count = count_whale_evidence(session, network, address, event.triggered_at)

    confirmed = bool(
        integrity_ok and (
            evidence_count >= 2
            or (whale_count >= 1 and evidence_count >= 1)
        )
    )

    confirmation = (session.query(EventConfirmation)
                    .filter(EventConfirmation.monitor_event_id == event.id)
                    .first())
    if confirmation is None:
        confirmation = EventConfirmation(
            monitor_event_id=event.id,
            chain_id=chain_id_for(network),
            network=network,
            token_address=normalize_address(address),
            status="DETECTED",
        )
        session.add(confirmation)
        session.flush()

    confirmation.evidence_count = evidence_count
    confirmation.whale_count = whale_count
    confirmation.integrity_verified = integrity_ok
    confirmation.updated_at = now_utc()

    if confirmed:
        confirmation.status = "CONFIRMED"
        confirmation.confirmed_at = confirmation.confirmed_at or now_utc()
        confirmation.reason = (
            f"Confirmed with {evidence_count} market evidence signal(s)"
            + (f" + {whale_count} verified whale event(s)" if whale_count else "")
        )
    else:
        confirmation.status = "CONFIRMING"
        if not integrity_ok:
            confirmation.reason = "Waiting: snapshot integrity not verified (" + ", ".join(integrity_reasons) + ")"
        else:
            confirmation.reason = (
                f"Waiting: {evidence_count} market evidence signal(s)"
                + (f", {whale_count} verified whale event(s)" if whale_count else "")
                + "; more verified evidence required"
            )
    return confirmed, confirmation


def store_event(session, item, activity, current, identity=None):
    address = normalize_address(item["address"])
    network = item["network"]
    identity = identity or {"name": None, "symbol": None, "source": "Unavailable", "resolved": False}
    token_symbol = identity.get("symbol")

    payload = {
        "token": {
            "address": address,
            "network": network,
            "network_name": network_label(network),
            "name": identity.get("name"),
            "symbol": token_symbol,
            "identity_source": identity.get("source"),
            "identity_verified": bool(identity.get("resolved")),
        },
        "snapshot": {
            "timestamp": current.snapshot_time.isoformat() if current.snapshot_time else None,
            "price_usd": current.price_usd,
            "market_cap_usd": current.market_cap_usd,
            "liquidity_usd": current.liquidity_usd,
            "volume_5m_usd": current.volume_5m_usd,
            "volume_1h_usd": current.volume_1h_usd,
            "volume_24h_usd": current.volume_24h_usd,
            "buys_5m": current.buys_5m,
            "sells_5m": current.sells_5m,
            "price_change_5m": current.price_change_5m,
            "price_change_1h": current.price_change_1h,
        },
        "activity": activity,
    }

    event = MonitorEvent(
        chain_id=chain_id_for(network),
        network=network,
        token_address=address,
        token_symbol=token_symbol,
        activity_score=activity["activity_score"],
        severity=activity["severity"],
        payload=json.dumps(payload, ensure_ascii=False, default=str),
        triggered_at=now_utc(),
    )
    session.add(event)
    session.flush()
    return event, payload

# ============================================================
# OPTIONAL OUTPUTS
# ============================================================


def format_event_alert(payload):
    token = payload["token"]
    snap = payload["snapshot"]
    activity = payload["activity"]
    symbol = token.get("symbol") or "Symbol unavailable"
    name = token.get("name") or "Token name unavailable"
    identity_text = f"{name} ({symbol})" if symbol != "Symbol unavailable" else name
    lines = [
        "🚨 <b>MARSOF AI — ACTIVITY DETECTED</b>",
        "",
        f"<b>{identity_text}</b> · <b>{token['network_name']}</b>",
        f"<code>{token['address']}</code>",
        "",
        f"⚡ Activity Score: <b>{activity['activity_score']:.1f}/100</b>",
        f"📌 Severity: <b>{activity['severity']}</b>",
        f"🧭 Pattern: <b>{activity.get('pattern', 'MIXED_ACTIVITY')}</b>",
        f"🔎 Evidence signals: <b>{activity.get('evidence_count', len(activity.get('signals', [])))}</b>",
        "",
        f"💵 Price: {fmt_money(snap['price_usd'])}",
        f"💰 Market Cap: {fmt_money(snap['market_cap_usd'])}",
        f"💧 Liquidity: {fmt_money(snap['liquidity_usd'])}",
        f"📈 5m Volume: {fmt_money(snap['volume_5m_usd'])}",
        f"📊 1h Volume: {fmt_money(snap['volume_1h_usd'])}",
        "",
    ]
    for signal in activity.get("signals", []):
        lines.append(f"• {signal['detail']}")
    return "\n".join(lines)


def send_telegram(text):
    if not TELEGRAM_TOKEN or not MONITOR_TELEGRAM_CHAT_ID:
        return False
    try:
        r = __import__("requests").post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": MONITOR_TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=20,
        )
        r.raise_for_status()
        return bool(r.json().get("ok"))
    except Exception as exc:
        print(f"⚠️ monitor Telegram alert failed: {exc}")
        return False


def _marsof_footer_html():
    return (
        "\n\nPowered by MARSOF AI\n\n"
        '<a href="https://x.com/MARSOF_AI">𝕏 X / Twitter: @MARSOF_AI</a>\n'
        '<a href="https://marsof.ct.ws/?i=1">🌐 Website: marsof.ct.ws</a>\n'
        '<a href="https://t.me/marsofaic">👥 Community: @marsofaic</a>'
    )


def publish_telegram_channel(report, image_path=None):
    """Publish a Telegram channel post with the fixed MARSOF footer.

    Marketing posts may include a random image from marketing_images/default/.
    Crypto posts remain text-only.
    """
    if not TELEGRAM_TOKEN or not MONITOR_TELEGRAM_CHANNEL_ID:
        return False
    report = str(report or "").strip()
    if not report:
        return False
    try:
        import requests
        from html import escape
        footer = _marsof_footer_html()
        message_text = escape(report) + footer
        if image_path and Path(image_path).is_file():
            with open(image_path, "rb") as fh:
                response = requests.post(
                    f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendPhoto",
                    data={"chat_id": MONITOR_TELEGRAM_CHANNEL_ID, "caption": message_text, "parse_mode": "HTML"},
                    files={"photo": fh}, timeout=30,
                )
        else:
            response = requests.post(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                json={
                    "chat_id": MONITOR_TELEGRAM_CHANNEL_ID,
                    "text": message_text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": True,
                }, timeout=20,
            )
        response.raise_for_status()
        ok = bool(response.json().get("ok"))
        if ok:
            print("📣 Published MARSOF post to Telegram channel")
        return ok
    except Exception as exc:
        print(f"⚠️ Telegram channel publish failed: {exc}")
        return False


def _random_marketing_image():
    """Pick one real image from marketing_images/default/; never fabricate a path."""
    if not MARKETING_IMAGE_DIR.is_dir():
        return None
    files = [p for p in MARKETING_IMAGE_DIR.iterdir() if p.is_file() and p.suffix.lower() in {".jpg", ".jpeg", ".png", ".webp"}]
    return str(random.choice(files)) if files else None


def build_ai_input(payload):
    """Return the exact evidence bundle that will be sent to AI later."""
    return {
        "system": (
            "You are the MARSOF AI activity-report writer. Analyze only the supplied verified data. "
            "Do not invent missing values, do not claim that activity predicts future price, and clearly "
            "distinguish observed facts from interpretation. The report is informational, not financial advice."
        ),
        "event": payload,
    }


def generate_ai_report(payload):
    """Generate one evidence-grounded event report through the Gemini API.

    AI is strictly downstream of the monitor: it receives only the verified
    snapshot/event bundle already produced by MARSOF. Any AI/API failure is
    isolated and never marks the monitoring event as failed.

    Connection note:
    - Try the current v1 REST endpoint first.
    - Fall back to v1beta only when v1 returns HTTP 404.
    - Keep the same GEMINI_API_KEY and MONITOR_AI_MODEL environment variables.
    """
    if not AI_ENABLED:
        return None
    if not AI_API_KEY:
        print("⚠️ AI enabled but GEMINI_API_KEY is not configured; skipping AI report")
        return None

    ai_input = build_ai_input(payload)
    system_prompt = (
        "You are MARSOF AI, the live market-activity analyst and Telegram alert writer. "
        "You receive the COMPLETE verified event payload from MARSOF. Analyze all supplied evidence internally, "
        "then write ONE standalone Telegram alert for THIS ONE token/event only. Never combine multiple events or tokens into one post. "
        "The raw payload is for analysis, not a checklist of numbers to reproduce. Choose only the few metrics that make this specific event interesting. "
        "Every number, token fact, network fact, timestamp, wallet fact, market metric, and change you mention MUST exist in the supplied data. "
        "Never invent, estimate, or convert unavailable/missing data into zero. If a field is unavailable, omit it. "
        "Do not claim order-book depth, whale intent, manipulation, accumulation, liquidity problems, or causation unless the supplied evidence directly supports it. "
        "Do not predict a future price, guarantee continuation, or give a buy/sell instruction. "
        "Do not write a formal research report and do not use fixed sections such as SUMMARY, VERIFIED SIGNALS, INTERPRETATION, RISKS, or WHAT TO WATCH. "
        "Do not use a rigid template: the hook, wording, selected metrics, emoji choice, and emphasis must naturally change with the event. "
        "Make it feel like a live crypto alert feed: short, energetic, visually easy to scan, and understandable to ordinary Telegram readers. "
        "Use the token name and/or $SYMBOL prominently when available, and mention the network when useful. "
        "Use alert emojis only when justified by actual evidence: 🚀 for a strong measured upward move or surge, 🔥 for unusually strong measured activity/volume, 🐋 ONLY when verified whale/large-wallet evidence is actually present, ⚠️ for elevated risk/uncertainty, 🛑 for a clearly negative/high-risk signal when supported. Do not add emojis just for decoration. "
        "The post MUST begin with a clear status line in exactly this style: `🟢 STATUS: BULLISH`, `🔴 STATUS: BEARISH`, or `🟡 STATUS: MIXED`. "
        "Choose BULLISH only when the supplied evidence shows a clearly positive short-term signal such as measured positive price movement and/or stronger buy-side activity. "
        "Choose BEARISH only when the supplied evidence shows a clearly negative short-term signal such as measured negative price movement and/or stronger sell-side activity. "
        "Use MIXED when the evidence is conflicting, insufficient, or not clearly directional. Status is a description of the observed event, not investment advice. "
        "After the status line, write a dynamic headline/hook and a short explanation using only the strongest 2-4 data points. Do not dump the dataset. "
        "Normally keep the complete post under about 120 words and preferably 60-100 words when the event is simple. "
        "It is acceptable to use short lines or bullets, but never make every post look identical. "
        "If whale evidence exists, you may naturally use 🐋 and mention the verified transfer/whale signal. If it does not exist, never imply a whale event. "
        "If the event is mainly a volume spike, 🔥 or 🚀 may be appropriate. If the event is negative, use ⚠️/🛑 as justified. "
        "Finish with one concise evidence-based observation or watch point when useful. "
        "The final output must contain ONLY the Telegram-ready English post. Do not include analysis notes, disclaimers, prompt explanations, markdown code fences, URLs, or buttons. "
        "The delivery layer adds the Powered by MARSOF AI footer separately."
    )
    user_prompt = json.dumps(ai_input["event"], ensure_ascii=False, default=str)
    combined_prompt = system_prompt + "\n\nVERIFIED EVENT DATA:\n" + user_prompt

    try:
        import requests

        payload_json = {
            "contents": [{
                "role": "user",
                "parts": [{"text": combined_prompt}],
            }],
            "generationConfig": {
                "maxOutputTokens": AI_MAX_OUTPUT_TOKENS,
                "temperature": 0.2,
            },
        }
        headers = {
            "x-goog-api-key": AI_API_KEY,
            "Content-Type": "application/json",
        }

        # Google currently documents both the stable v1 REST surface and the
        # v1beta surface. Try v1 first so the monitor is not tied to a beta
        # endpoint; retain v1beta as a narrow compatibility fallback.
        models_to_try = []
        for model_name in (AI_MODEL, AI_FALLBACK_MODEL):
            if model_name and model_name not in models_to_try:
                models_to_try.append(model_name)

        last_error = None
        for model_name in models_to_try:
            endpoints = [
                f"https://generativelanguage.googleapis.com/v1/models/{model_name}:generateContent",
                f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent",
            ]

            for endpoint in endpoints:
                response = requests.post(
                    endpoint,
                    headers=headers,
                    json=payload_json,
                    timeout=AI_TIMEOUT_SECONDS,
                )

                if response.status_code == 404 and endpoint != endpoints[-1]:
                    last_error = f"HTTP 404 from {endpoint}"
                    continue

                if response.status_code >= 400:
                    detail = response.text[:500].replace("\n", " ")
                    last_error = f"Gemini {model_name} HTTP {response.status_code}: {detail}"
                    break

                data = response.json()
                candidates = data.get("candidates") or []
                chunks = []
                for candidate in candidates:
                    content = candidate.get("content") or {}
                    for part in content.get("parts") or []:
                        text_value = part.get("text")
                        if text_value:
                            chunks.append(str(text_value))
                report = "\n".join(chunks).strip()
                if report:
                    return report

                block_reason = (data.get("promptFeedback") or {}).get("blockReason")
                finish_reasons = [
                    c.get("finishReason") for c in candidates if c.get("finishReason")
                ]
                detail_parts = [f"{model_name} HTTP 200 but returned no text"]
                if block_reason:
                    detail_parts.append(f"blockReason={block_reason}")
                if finish_reasons:
                    detail_parts.append(f"finishReason={','.join(finish_reasons)}")
                last_error = "; ".join(detail_parts)
                break

        print(f"⚠️ Gemini connection failed: {last_error or 'unknown error'}")
        return None
    except Exception as exc:
        print(f"⚠️ Gemini AI report generation failed: {exc}")
        return None


def _oauth1_header(method, url, params):
    """Build an OAuth 1.0a Authorization header without adding a dependency."""
    oauth = {
        "oauth_consumer_key": X_CONSUMER_KEY,
        "oauth_nonce": secrets.token_hex(16),
        "oauth_signature_method": "HMAC-SHA1",
        "oauth_timestamp": str(int(time.time())),
        "oauth_token": X_ACCESS_TOKEN,
        "oauth_version": "1.0",
    }
    all_params = {**params, **oauth}
    encoded = sorted((quote(str(k), safe="~"), quote(str(v), safe="~")) for k, v in all_params.items())
    normalized = "&".join(f"{k}={v}" for k, v in encoded)
    base_url = url.split("?", 1)[0]
    base_string = "&".join([method.upper(), quote(base_url, safe="~"), quote(normalized, safe="~")])
    signing_key = f"{quote(X_CONSUMER_SECRET or '', safe='~')}&{quote(X_ACCESS_TOKEN_SECRET or '', safe='~')}"
    digest = hmac.new(signing_key.encode(), base_string.encode(), hashlib.sha1).digest()
    oauth["oauth_signature"] = base64.b64encode(digest).decode()
    header = ", ".join(
        f'{quote(k, safe="~")}="{quote(str(v), safe="~")}"'
        for k, v in sorted(oauth.items())
    )
    return "OAuth " + header


def _clean_x_text(text, max_chars=None):
    """Convert an AI report into a compact single X post."""
    max_chars = max_chars or X_MAX_POST_CHARS
    text = " ".join(str(text or "").split())
    if len(text) <= max_chars:
        return text
    # Keep the beginning because Gemini's SUMMARY comes first.
    cut = max_chars - 1
    return text[:cut].rsplit(" ", 1)[0] + "…"


def build_x_post(report, payload):
    token = payload.get("token") or {}
    activity = payload.get("activity") or {}
    snap = payload.get("snapshot") or {}
    symbol = token.get("symbol") or token.get("name") or "Token"
    network = token.get("network_name") or token.get("network") or "unknown"
    score = activity.get("activity_score")
    pattern = activity.get("pattern") or "MIXED_ACTIVITY"
    price = snap.get("price_change_5m")
    volume = snap.get("volume_5m_usd")

    facts = [f"{symbol} · {network}", f"Activity {score:.0f}/100" if isinstance(score, (int, float)) else None, pattern]
    if isinstance(price, (int, float)):
        facts.append(f"5m price {price:+.2f}%")
    if isinstance(volume, (int, float)):
        facts.append(f"5m volume ${volume:,.0f}")
    lead = "🚨 MARSOF AI — Unusual activity detected"
    detail = " · ".join(x for x in facts if x)
    # Do not publish the full AI report automatically: the post is an
    # evidence summary, while the full report remains available in Telegram/DB.
    return _clean_x_text(f"{lead}\n{detail}", X_MAX_POST_CHARS)


def publish_x(report, payload):
    """Publish one evidence summary to the MARSOF AI X account."""
    if not X_ENABLED or not X_PUBLISH_ENABLED:
        return False
    required = (X_CONSUMER_KEY, X_CONSUMER_SECRET, X_ACCESS_TOKEN, X_ACCESS_TOKEN_SECRET)
    if not all(required):
        print("⚠️ X publishing enabled but OAuth 1.0a credentials are incomplete.")
        return False
    try:
        import requests
        url = "https://api.x.com/2/tweets"
        body = {"text": build_x_post(report, payload)}
        auth_header = _oauth1_header("POST", url, {})
        response = requests.post(
            url,
            headers={"Authorization": auth_header, "Content-Type": "application/json"},
            json=body,
            timeout=20,
        )
        if response.status_code >= 400:
            detail = response.text[:500].replace("\n", " ")
            print(f"⚠️ X publish failed: HTTP {response.status_code}: {detail}")
            return False
        data = response.json()
        post_id = (data.get("data") or {}).get("id")
        print(f"𝕏 Published MARSOF event{(' · id=' + str(post_id)) if post_id else ''}")
        return bool(post_id)
    except Exception as exc:
        print(f"⚠️ X publish error: {exc}")
        return False


def get_recent_events(session, lookback_hours=None, limit=None):
    """Return recent stored events ordered newest first for the aggregator."""
    lookback_hours = lookback_hours or X_AGGREGATE_LOOKBACK_HOURS
    limit = limit or X_AGGREGATE_MAX_EVENTS
    cutoff = now_utc() - timedelta(hours=lookback_hours)
    return (
        session.query(MonitorEvent)
        .filter(MonitorEvent.triggered_at >= cutoff)
        .order_by(MonitorEvent.triggered_at.desc())
        .limit(limit)
        .all()
    )


def _extract_event_evidence(payload):
    """Extract human-readable evidence already present in a stored event."""
    activity = payload.get("activity") or {}
    snapshot = payload.get("snapshot") or {}
    evidence = []

    # Prefer the exact signal details generated by detect_activity.
    for signal in activity.get("signals") or []:
        detail = signal.get("detail") if isinstance(signal, dict) else None
        if detail:
            evidence.append(str(detail))

    # Keep useful observed snapshot context even when a signal was not
    # triggered. Missing values stay unavailable and are not converted to 0.
    for key, label in (("price_change_5m", "5m price"), ("price_change_1h", "1h price")):
        value = snapshot.get(key)
        if isinstance(value, (int, float)) and not any(label in item for item in evidence):
            evidence.append(f"{label} {value:+.2f}%")

    for key, label in (("buys_5m", "5m buys"), ("sells_5m", "5m sells")):
        value = snapshot.get(key)
        if isinstance(value, (int, float)):
            evidence.append(f"{label} {value:,.0f}")

    return evidence[:8]


def aggregate_recent_events(lookback_hours=None, limit=None):
    """Aggregate unusual events by token without changing detection/scoring."""
    session = MonitorSession()
    try:
        lookback_hours = lookback_hours or X_AGGREGATE_LOOKBACK_HOURS
        limit = limit or X_AGGREGATE_MAX_EVENTS
        events = get_recent_events(session, lookback_hours, limit)
        grouped = {}
        for event in events:
            key = (event.chain_id, event.network, event.token_address)
            bucket = grouped.setdefault(key, {
                "network": event.network,
                "token_address": event.token_address,
                "token_symbol": event.token_symbol,
                "events": 0,
                "max_score": 0.0,
                "latest_severity": event.severity,
                "latest_at": event.triggered_at.isoformat() if event.triggered_at else None,
                "patterns": [],
                "evidence_signals": 0,
                "evidence": [],
            })
            bucket["events"] += 1
            bucket["max_score"] = max(bucket["max_score"], float(event.activity_score or 0))
            try:
                payload = json.loads(event.payload or "{}")
                activity = payload.get("activity") or {}
                pattern = activity.get("pattern")
                if pattern and pattern not in bucket["patterns"]:
                    bucket["patterns"].append(pattern)
                bucket["evidence_signals"] = max(
                    bucket["evidence_signals"], int(activity.get("evidence_count") or len(activity.get("signals") or []))
                )
                if not bucket.get("token_symbol"):
                    bucket["token_symbol"] = (payload.get("token") or {}).get("symbol")

                # Preserve the verified event evidence for the aggregator.
                # These values are already produced by detect_activity and
                # are never used to recalculate or alter the activity score.
                bucket["evidence"] = _extract_event_evidence(payload)
            except Exception:
                pass
        rows = list(grouped.values())
        rows.sort(key=lambda r: (-r["max_score"], r["latest_at"] or ""))
        return {"lookback_hours": lookback_hours, "events_scanned": len(events), "tokens": rows}
    finally:
        session.close()


def format_monitor_events(report):
    """Compact Telegram view of the recent event aggregation board."""
    rows = report.get("tokens") or []
    lines = [
        "📡 <b>MARSOF AI — EVENT AGGREGATOR</b>",
        f"Last {report.get('lookback_hours') or X_AGGREGATE_LOOKBACK_HOURS}h · events scanned: <b>{report.get('events_scanned', 0)}</b>",
        "",
    ]
    if not rows:
        lines.append("🟢 No unusual activity events in the selected period.")
        return "\n".join(lines)
    for i, row in enumerate(rows[:10], 1):
        symbol = row.get("token_symbol") or "Symbol unavailable"
        patterns = ", ".join(row.get("patterns") or ["MIXED_ACTIVITY"])
        lines.append(
            f"{i}. <b>{symbol}</b> · {row['network']} · "
            f"{row['events']} event(s) · max <b>{row['max_score']:.1f}</b> · "
            f"{row['latest_severity']} · {patterns}"
        )
        lines.append(f"   🔎 Evidence signals: <b>{row.get('evidence_signals', 0)}</b>")
        for evidence in (row.get("evidence") or [])[:5]:
            lines.append(f"   • {evidence}")
        lines.append("")
    return "\n".join(lines)


def run_x_connectivity_test():
    """Test read access to @MARSOF_AI through the X API.

    This is deliberately separate from publishing and from the token monitor.
    It performs one user lookup and, when successful, one recent-post request
    with public_metrics. No X data is stored and no post is published.
    """
    if not X_BEARER_TOKEN:
        return {
            "status": "MISSING_KEY",
            "detail": "X_BEARER_TOKEN is not configured",
        }

    try:
        import requests

        headers = {
            "Authorization": f"Bearer {X_BEARER_TOKEN}",
            "Accept": "application/json",
        }

        user_url = "https://api.x.com/2/users/by/username/MARSOF_AI"
        user_params = {"user.fields": "id,name,username,public_metrics"}
        response = requests.get(user_url, headers=headers, params=user_params, timeout=20)

        if response.status_code >= 400:
            detail = response.text[:600].replace("\n", " ")
            return {
                "status": "FAILED",
                "http_status": response.status_code,
                "detail": f"User lookup failed: HTTP {response.status_code}: {detail}",
            }

        user_data = response.json().get("data") or {}
        user_id = user_data.get("id")
        if not user_id:
            return {"status": "FAILED", "detail": "X returned no user id for @MARSOF_AI"}

        tweets_url = f"https://api.x.com/2/users/{user_id}/tweets"
        tweet_params = {
            "max_results": "5",
            "tweet.fields": "created_at,public_metrics",
        }
        tweets_response = requests.get(tweets_url, headers=headers, params=tweet_params, timeout=20)

        if tweets_response.status_code >= 400:
            detail = tweets_response.text[:600].replace("\n", " ")
            return {
                "status": "USER_OK_POSTS_FAILED",
                "http_status": tweets_response.status_code,
                "user": {
                    "id": user_id,
                    "username": user_data.get("username"),
                    "name": user_data.get("name"),
                },
                "detail": f"User lookup succeeded, but recent-post read failed: HTTP {tweets_response.status_code}: {detail}",
            }

        tweets = tweets_response.json().get("data") or []
        metrics_available = 0
        sample = []
        for tweet in tweets[:5]:
            metrics = tweet.get("public_metrics") or {}
            if metrics:
                metrics_available += 1
            sample.append({
                "created_at": tweet.get("created_at"),
                "public_metrics": metrics,
            })

        return {
            "status": "OK",
            "http_status": 200,
            "user": {
                "id": user_id,
                "username": user_data.get("username"),
                "name": user_data.get("name"),
                "public_metrics": user_data.get("public_metrics") or {},
            },
            "recent_posts": len(tweets),
            "posts_with_public_metrics": metrics_available,
            "sample": sample,
            "detail": "X read access and post public_metrics are working for @MARSOF_AI.",
        }
    except Exception as exc:
        return {"status": "FAILED", "detail": str(exc)[:600]}

# ============================================================
# ONE MONITOR CYCLE
# ============================================================


def monitor_token(item):
    address = item["address"]
    network = item["network"]
    session = MonitorSession()
    try:
        state = get_monitor_state(session, address, network)
        print(f"🔎 Monitoring {network} {address}")

        # The stable MARSOF engine fetches fresh DEX/security/holder data and
        # persists the snapshot to the shared database.
        result = run_analysis(address, network)
        state.last_checked_at = now_utc()

        if not result:
            state.last_status = "NO_RESULT"
            session.commit()
            return {"status": "NO_RESULT", "identity": {"name": None, "symbol": None, "source": "Unavailable", "resolved": False}}

        # Transfer monitoring is deterministic and independent from the activity score.
        transfer_result = {"status": "UNAVAILABLE", "events": 0}
        try:
            current_for_transfer, _ = latest_two_snapshots(session, address, network)
            if network == "sui":
                transfer_result = scan_sui_transfers(session, item, current_for_transfer, result)
            else:
                transfer_result = scan_evm_transfers(session, item, current_for_transfer, result)
            transfer_status = transfer_result.get('status', 'UNKNOWN')
            transfer_reason = transfer_result.get('reason')
            reason_text = f" | reason={transfer_reason}" if transfer_status == "UNAVAILABLE" and transfer_reason else ""
            detail_text = ""
            if transfer_status == "UNAVAILABLE" and transfer_result.get("detail"):
                detail_text = f" | detail={str(transfer_result.get('detail'))[:350]}"
            print(
                f"🔗 Transfer Monitor: {transfer_status} | "
                f"{network} {address} | events={transfer_result.get('events', 0)}"
                f"{reason_text}{detail_text}"
            )
            # Whale persistence is isolated in a savepoint. A legacy schema
            # mismatch must never abort the whole monitor transaction.
            whale_events = []
            try:
                with session.begin_nested():
                    whale_events = detect_whale_events(session, network, address)
            except Exception as whale_exc:
                print(f"ℹ️ Whale detection unavailable for {network}:{address}: {whale_exc}")
            if whale_events:
                print(
                    f"🐋 Whale Detection: {len(whale_events)} large transfer(s) | "
                    f"{network} {address} | threshold=${WHALE_THRESHOLD_USD:,.0f}"
                )
            if transfer_result.get("events"):
                print(f"🐋 New transfers: {transfer_result.get('events')} | {network} {address}")
        except Exception as transfer_exc:
            print(f"ℹ️ Transfer monitor unavailable for {network}:{address}: {transfer_exc}")

        # Resolve the real token identity from the contract/core metadata.
        # The configured WATCHLIST alias is never used for display identity.
        identity = resolve_token_identity(result, item, session)

        current, previous = latest_two_snapshots(session, address, network)
        holder_current, holder_previous = latest_holder_snapshots(session, address, network)

        if current is None or previous is None:
            state.last_status = "BASELINE"
            state.last_score = None
            session.commit()
            print("ℹ️ Baseline created; waiting for another snapshot before activity detection.")
            return {"status": "BASELINE", "identity": identity, "transfers": transfer_result}

        activity = detect_activity(current, previous, holder_current, holder_previous)
        state.last_score = activity["activity_score"]
        state.last_status = activity["severity"]

        if activity["activity_score"] < MONITOR_MIN_ACTIVITY_SCORE:
            session.commit()
            return {"status": "NORMAL", "activity": activity, "identity": identity, "transfers": transfer_result}

        if cooldown_active(state):
            session.commit()
            print(
                f"ℹ️ Activity detected but cooldown is active for {network}:{address} "
                f"({MONITOR_COOLDOWN_MINUTES}m)."
            )
            return {"status": "COOLDOWN", "activity": activity, "identity": identity, "transfers": transfer_result}

        event, payload = store_event(session, item, activity, current, identity)
        confirmed, confirmation = confirm_monitor_event(
            session, event, activity, current, previous, address, network
        )

        # Keep the event payload tied to the verified confirmation evidence.
        payload["confirmation"] = {
            "status": confirmation.status,
            "evidence_count": confirmation.evidence_count,
            "whale_count": confirmation.whale_count,
            "integrity_verified": confirmation.integrity_verified,
            "reason": confirmation.reason,
        }
        event.payload = json.dumps(payload, ensure_ascii=False, default=str)
        state.last_event_at = now_utc()
        session.commit()

        if confirmed:
            alert = format_event_alert(payload)
            print(alert.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", ""))
            send_telegram(alert)
            print(f"✅ Event Confirmation: CONFIRMED | {network} {address} | event={event.id}")
            result_status = "CONFIRMED"
        else:
            print(
                f"⏳ Event Confirmation: CONFIRMING | {network} {address} | "
                f"event={event.id} | {confirmation.reason}"
            )
            result_status = "CONFIRMING"

        # AI is downstream of confirmed events only.
        return {"status": result_status, "activity": activity, "payload": payload, "identity": identity, "transfers": transfer_result}

    except Exception as exc:
        session.rollback()
        print(f"❌ Monitor error {network}:{address}: {exc}")
        return {"status": "ERROR", "error": str(exc)}
    finally:
        session.close()


def process_pending_ai_events(force_reprocess=False, publish_x_posts=True):
    """Process qualifying stored events after aggregation, never per token.

    This is the boundary between the deterministic Evidence/Event layers and
    Gemini. Only stored events at or above AI_EVENT_THRESHOLD are eligible,
    and each event is processed once after a successful AI response.
    """
    if not AI_ENABLED:
        return {"status": "DISABLED", "processed": 0, "eligible": 0}
    if not AI_API_KEY:
        return {"status": "MISSING_KEY", "processed": 0, "eligible": 0}

    session = MonitorSession()
    try:
        # Production publishing is intentionally fresh-only. The general
        # aggregate lookback can be 24h, but X must never revive an old event
        # simply because it is still inside that reporting window.
        cutoff = now_utc() - timedelta(hours=X_MAX_EVENT_AGE_HOURS)
        events = (
            session.query(MonitorEvent)
            .filter(
                MonitorEvent.triggered_at >= cutoff,
                MonitorEvent.activity_score >= AI_EVENT_THRESHOLD,
                MonitorEvent.id.in_(
                    session.query(EventConfirmation.monitor_event_id)
                    .filter(EventConfirmation.status == "CONFIRMED")
                ),
                # Normal production mode selects only unprocessed events.
                # /monitor_ai_test explicitly sets force_reprocess=True so it
                # can test events that production already processed.
                *(() if force_reprocess else (or_(
                    MonitorEvent.ai_processed.is_(False),
                    MonitorEvent.ai_processed.is_(None),
                ),)),
                # A successfully published event is permanently excluded from
                # the production queue. This is an extra guard in addition to
                # ai_processed so an old event can never be posted twice.
                MonitorEvent.published_x.is_(False),
            )
            .order_by(MonitorEvent.activity_score.desc(), MonitorEvent.triggered_at.desc())
            .limit(AI_EVENT_BATCH_LIMIT)
            .all()
        )

        processed = 0
        failed = 0
        for event in events:
            try:
                payload = json.loads(event.payload or "{}")
                report = generate_ai_report(payload)
                if not report:
                    failed += 1
                    continue

                event.ai_report = report
                event.ai_processed = True
                session.commit()

                # Manual AI tests never publish to the public channel. Only the
                # live production path sends the confirmed Gemini alert.
                if not force_reprocess and not event.published_telegram:
                    event.published_telegram = publish_telegram_channel(report)
                    session.commit()

                processed += 1

                # Manual AI tests never publish to X, preventing duplicate posts.
                if publish_x_posts and not event.published_x:
                    event.published_x = publish_x(report, payload)
                    session.commit()
            except Exception as exc:
                session.rollback()
                failed += 1
                print(f"⚠️ AI event processing failed for event {getattr(event, 'id', '?')}: {exc}")

        return {
            "status": "OK",
            "eligible": len(events),
            "processed": processed,
            "failed": failed,
            "threshold": AI_EVENT_THRESHOLD,
            "max_event_age_hours": X_MAX_EVENT_AGE_HOURS,
        }
    except Exception as exc:
        session.rollback()
        print(f"⚠️ AI batch processing failed: {exc}")
        return {"status": "FAILED", "processed": 0, "eligible": 0, "detail": str(exc)[:300]}
    finally:
        session.close()



def _parse_daily_slot(slot):
    try:
        hour, minute = [int(x) for x in str(slot).strip().split(":", 1)]
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    except Exception:
        pass
    return None


def _daily_slot_due(slot, now):
    """Return True when the slot is due or overdue; caller limits catch-up to avoid bursts."""
    parsed = _parse_daily_slot(slot)
    if not parsed:
        return False
    hour, minute = parsed
    slot_minutes = hour * 60 + minute
    now_minutes = now.hour * 60 + now.minute
    # A slot remains eligible after its scheduled time. This prevents missed posts
    # when Render restarts or a cycle is delayed. process_daily_content publishes
    # at most one catch-up crypto slot and one marketing slot per monitor cycle.
    return slot_minutes <= now_minutes


def _daily_token_priority(item):
    alias = str(item.get("name") or "").upper()
    return 0 if alias in DAILY_PRIORITY_TOKENS else 1


def _daily_direction(payload):
    """Classify only the observed short-term price direction; never invent a signal."""
    changes = []
    for horizon in ("1h", "6h", "24h"):
        value = safe_float((payload.get("observed_price_change") or {}).get(horizon))
        if value is not None:
            changes.append(value)
    positive = sum(1 for value in changes if value > 0)
    negative = sum(1 for value in changes if value < 0)
    if len(changes) >= 2:
        if positive >= 2 and positive > negative:
            return "BULLISH"
        if negative >= 2 and negative > positive:
            return "BEARISH"
        return "MIXED"
    if len(changes) == 1:
        if changes[0] > 0:
            return "BULLISH"
        if changes[0] < 0:
            return "BEARISH"
    return "MIXED"


def _daily_notability_score(payload):
    """Rank real short-term movement/activity for daily editorial selection."""
    changes = payload.get("observed_price_change") or {}
    abs_changes = [abs(v) for v in (safe_float(changes.get(k)) for k in ("1h", "6h", "24h")) if v is not None]
    score = 0.0
    if abs_changes:
        score += max(abs_changes) * 8.0
        score += sum(abs_changes) * 2.0
    current = payload.get("current") or {}
    volume_1h = safe_float(current.get("volume_1h_usd")) or 0.0
    liquidity = safe_float(current.get("liquidity_usd"))
    if liquidity and liquidity > 0:
        score += min((volume_1h / liquidity) * 100.0, 25.0)
    buys = current.get("buys_1h")
    sells = current.get("sells_1h")
    if isinstance(buys, (int, float)) and isinstance(sells, (int, float)):
        total = buys + sells
        if total > 0:
            score += min(abs(buys - sells) / total * 20.0, 20.0)
    return score


def _build_daily_market_payload(session, item):
    """Build a real-data chart bundle from persisted MARSOF market snapshots."""
    address = item["address"]
    network = item["network"]
    cid = chain_id_for(network)
    addr = normalize_address(address)
    cutoff = now_utc() - timedelta(hours=DAILY_LOOKBACK_HOURS)
    rows = (
        session.query(MarketSnapshot)
        .filter(
            MarketSnapshot.chain_id == cid,
            MarketSnapshot.token_address == addr,
            MarketSnapshot.snapshot_time >= cutoff,
        )
        .order_by(MarketSnapshot.snapshot_time.desc())
        .limit(DAILY_MAX_SNAPSHOTS)
        .all()
    )
    if not rows:
        return None

    current = rows[0]
    # Pick the closest stored snapshot at or before each reference horizon.
    def ref_value(hours):
        target = current.snapshot_time - timedelta(hours=hours) if current.snapshot_time else now_utc() - timedelta(hours=hours)
        candidates = [r for r in rows if r.snapshot_time and r.snapshot_time <= target]
        return candidates[0] if candidates else None

    one_h = ref_value(1)
    six_h = ref_value(6)
    twenty_four_h = ref_value(24)

    prices = [safe_float(r.price_usd) for r in rows if safe_float(r.price_usd) is not None]
    high_24h = max(prices) if prices else None
    low_24h = min(prices) if prices else None

    token_row = None
    try:
        token_row = session.query(Token).filter(
            Token.chain_id == cid,
            Token.contract_address == addr,
        ).order_by(Token.last_scanned.desc()).first()
    except Exception:
        token_row = None
    identity = resolve_token_identity({}, item, session)
    name = (getattr(token_row, "name", None) if token_row else None) or identity.get("name")
    symbol = (getattr(token_row, "symbol", None) if token_row else None) or identity.get("symbol")

    def observed_change(reference):
        if reference is None:
            return None
        a = safe_float(current.price_usd)
        b = safe_float(reference.price_usd)
        if a is None or b is None or b == 0:
            return None
        return ((a - b) / b) * 100.0

    # Whale activity is evidence-only and is intentionally separate from the analyzer score.
    whale_rows = (session.query(WhaleEvent)
                  .filter(WhaleEvent.network == network,
                          WhaleEvent.token_address == addr,
                          WhaleEvent.detected_at >= cutoff)
                  .order_by(WhaleEvent.detected_at.desc())
                  .limit(50).all())
    whale_activity = {
        "status": "DATA" if whale_rows else "UNAVAILABLE",
        "event_count": len(whale_rows) if whale_rows else None,
        "total_verified_value_usd": (sum(safe_float(r.value_usd) for r in whale_rows if safe_float(r.value_usd) is not None) if whale_rows else None),
        "events": [
            {
                "timestamp": r.detected_at.isoformat() if r.detected_at else None,
                "tx_hash": r.tx_hash,
                "wallet": r.whale_wallet,
                "role": r.whale_role,
                "amount_ui": safe_float(r.amount_ui),
                "value_usd": safe_float(r.value_usd),
                "balance_delta_ui": safe_float(r.balance_delta_ui),
                "transfer_type": r.transfer_type,
                "source": r.source,
            } for r in whale_rows
        ],
    }

    return {
        "kind": "DAILY_CRYPTO",
        "token": {
            "name": name,
            "symbol": symbol,
            "network": network,
            "network_name": network_label(network),
            "address": addr,
        },
        "current": {
            "timestamp": current.snapshot_time.isoformat() if current.snapshot_time else None,
            "price_usd": safe_float(current.price_usd),
            "market_cap_usd": safe_float(current.market_cap_usd),
            "fdv_usd": safe_float(current.fdv_usd),
            "liquidity_usd": safe_float(current.liquidity_usd),
            "volume_5m_usd": safe_float(current.volume_5m_usd),
            "volume_1h_usd": safe_float(current.volume_1h_usd),
            "volume_6h_usd": safe_float(current.volume_6h_usd),
            "volume_24h_usd": safe_float(current.volume_24h_usd),
            "buys_5m": current.buys_5m,
            "sells_5m": current.sells_5m,
            "buys_1h": current.buys_1h,
            "sells_1h": current.sells_1h,
        },
        "observed_price_change": {
            "1h": observed_change(one_h),
            "6h": observed_change(six_h),
            "24h": observed_change(twenty_four_h),
        },
        "observed_24h_range": {
            "high_usd": high_24h,
            "low_usd": low_24h,
        },
        "snapshot_count": len(rows),
        "chart_window_hours": DAILY_LOOKBACK_HOURS,
        "whale_activity": whale_activity,
    }


def _generate_daily_ai_post(payload, kind):
    if not AI_ENABLED or not AI_API_KEY:
        return None

    if kind == "DAILY_CRYPTO":
        system_prompt = (
            "You are MARSOF AI's high-energy short-term crypto market writer for Telegram/X. "
            "Write a concise, emotionally engaging market post about ONLY the supplied token. "
            "The editorial focus is SHORT-TERM observed momentum, not long-term investing. "
            "Use only verified persisted market snapshots. Never invent, estimate, or turn unavailable data into zero. "
            "The supplied DIRECTION is authoritative: use exactly BULLISH, BEARISH, or MIXED and never override it. "
            "Open with a strong crypto-style hook using the supplied direction and $SYMBOL, with fitting emojis such as 🚀🔥👀⚠️🔴🟢. "
            "Describe only what the supplied data directly supports. Price changes may be described as observed momentum or direction, but do not call it buyer/seller pressure unless verified buy/sell counts are supplied. ""Never mention an order book, order-book depth, resistance, support, accumulation, distribution, or whale intent unless that exact evidence exists in the supplied data. "
            "Numbers are evidence, not the story: use only the most useful real figures and do not dump a list of metrics. "
            "Mention observed 1h/6h/24h movement only when available; if a horizon is unavailable, omit it. "
            "Observed high/low are reference levels only. Never call them targets, guarantees, predictions, or promises. "
            "If whale_activity is DATA, MUST include one concise whale-activity sentence in the post using only verified whale transfers, verified value, role, and transfer type. If whale_activity is UNAVAILABLE, omit whale activity entirely. "
            "Never convert missing whale data to zero and never infer BUY/SELL from SENDER/RECEIVER alone; only use BUY/SELL when transfer_type explicitly says so. "
            "End with a concise evidence-based closing sentence. Do not ask the reader to predict or speculate about the next price move. "
            "Do not give buy/sell instructions, entry calls, guaranteed upside/downside, or future-price predictions. "
            "Use a different natural hook and wording each time. Avoid repeatedly saying 'short-term'. "
            "Keep it around 70-130 words. Final output ONLY the post, with no analysis notes, URLs, disclaimer, or code fences."
        )
    else:
        system_prompt = (
            "You are MARSOF AI's high-energy product marketing writer. Write a punchy English Telegram post that feels exciting, modern and varied. "
            "Focus 60-70% of marketing on the FREE AI Community Manager: intelligent member replies, engagement posts, community activity, joins/leaves, community mood, community-token monitoring, real price/volume alerts when available, purchase activity only when real data supports it, community stats, and advanced AI features. "
            "Other themes include Crypto Intelligence, verified Whale Intelligence, Market Monitoring, and MARSOF development. "
            "Never claim unreleased features are live; label them In Development or Coming Next when appropriate. Never invent users, partnerships, results, dates or numbers. "
            "Use a strong hook, short paragraphs, emojis naturally, and varied wording. Make the reader want to follow MARSOF AI. Do not include URLs or the signature; the publisher adds the fixed footer. "
            "Final output ONLY the Telegram-ready post, no analysis notes, code fences, or prompt explanation."
        )

    topic = payload.get("marketing_topic") if kind == "MARKETING" else payload
    if kind == "DAILY_CRYPTO":
        topic = dict(topic)
        topic["editorial_direction"] = _daily_direction(topic)
        topic["editorial_notability_score"] = round(_daily_notability_score(topic), 2)
    combined = system_prompt + "\
\
VERIFIED CONTENT DATA:\
" + json.dumps(topic, ensure_ascii=False, default=str)
    try:
        models_to_try = []
        for model_name in (AI_MODEL, AI_FALLBACK_MODEL):
            if model_name and model_name not in models_to_try:
                models_to_try.append(model_name)
        last_error = None
        for model_name in models_to_try:
            for endpoint in (
                f"https://generativelanguage.googleapis.com/v1/models/{model_name}:generateContent",
                f"https://generativelanguage.googleapis.com/v1beta/models/{model_name}:generateContent",
            ):
                # Daily publishing must never be allowed to block for an
                # unexpectedly large Render environment timeout. Keep this
                # path bounded even if MONITOR_AI_TIMEOUT_SECONDS was set very high.
                daily_timeout = min(max(int(AI_TIMEOUT_SECONDS), 1), 30)
                response = requests.post(
                    endpoint,
                    headers={"x-goog-api-key": AI_API_KEY, "Content-Type": "application/json"},
                    json={
                        "contents": [{"role": "user", "parts": [{"text": combined}]}],
                        "generationConfig": {"maxOutputTokens": AI_MAX_OUTPUT_TOKENS, "temperature": 0.25},
                    },
                    timeout=daily_timeout,
                )
                if response.status_code == 404 and endpoint.endswith("/v1/models/" + model_name + ":generateContent"):
                    last_error = response.text[:300]
                    continue
                if response.status_code >= 400:
                    last_error = response.text[:500].replace("\
", " ")
                    break
                data = response.json()
                chunks = []
                for candidate in data.get("candidates") or []:
                    for part in (candidate.get("content") or {}).get("parts") or []:
                        if part.get("text"):
                            chunks.append(str(part["text"]))
                report = "\
".join(chunks).strip()
                if report:
                    return report
                last_error = "Gemini returned no text"
                break
        print(f"⚠️ Daily Gemini post failed: {last_error or 'unknown error'}")
    except Exception as exc:
        print(f"⚠️ Daily Gemini generation failed: {exc}")
    return None


def _daily_marketing_topic(slot_index):
    topics = (
        {
            "theme": "FREE AI Community Manager",
            "headline_direction": "Make the free Community Manager the hero: an AI that understands and helps run Telegram communities.",
            "service": "AI Community Manager",
            "status": "FREE; current product direction",
            "points": [
                "Answers member questions intelligently.",
                "Creates engaging community posts.",
                "Monitors community activity and tracks members entering or leaving.",
                "Understands community mood and activity patterns.",
                "Can monitor the community token and surface real price/volume alerts when supported by data.",
                "Can surface purchase activity and community statistics when real data is available.",
                "Advanced AI community features are part of the evolving MARSOF ecosystem.",
            ],
        },
        {
            "theme": "Crypto Intelligence",
            "headline_direction": "Show MARSOF turning raw token data into useful intelligence: analysis, security, liquidity, volume, holders and risk.",
            "service": "Crypto Intelligence",
            "status": "Current MARSOF capability direction",
            "points": [
                "Token analysis and security evidence.",
                "Market data, liquidity and volume.",
                "Holder activity and market movement.",
                "Risk detection grounded in available real data.",
            ],
        },
        {
            "theme": "Whale Intelligence",
            "headline_direction": "Make verified blockchain movement feel important without inventing buy/sell conclusions.",
            "service": "Whale Intelligence",
            "status": "Evidence-first monitoring",
            "points": [
                "Verified transfer activity from monitored networks.",
                "Whale value and wallet roles when real data is available.",
                "BUY/SELL is shown only when independently verified by transfer context.",
                "Missing evidence stays unavailable rather than becoming zero.",
            ],
        },
        {
            "theme": "MARSOF Market Monitor",
            "headline_direction": "Show the market moving in real time: volume spikes, price movement, liquidity changes, activity and whale movement.",
            "service": "Market Monitoring",
            "status": "Current monitoring system",
            "points": [
                "Monitors configured assets with persisted market snapshots.",
                "Tracks real volume, price and liquidity changes when available.",
                "Separates normal activity from detected events before AI interpretation.",
                "Uses evidence first and avoids fabricated numbers.",
            ],
        },
        {
            "theme": "Behind MARSOF AI",
            "headline_direction": "Show the evolution toward one MARSOF AI ecosystem combining Crypto Intelligence, Whale Intelligence and AI Community Management.",
            "service": "MARSOF AI ecosystem",
            "status": "Development / ongoing",
            "points": [
                "One ecosystem instead of disconnected tools.",
                "Real-data monitoring is the foundation.",
                "AI is used downstream to explain verified evidence.",
                "Community management is being integrated into the same MARSOF AI product.",
            ],
        },
    )
    # Community Manager appears most often, while the other pillars rotate.
    order = [0, 1, 0, 2, 0, 4, 0, 0]
    return topics[order[slot_index % len(order)]]


def process_daily_content():
    """Publish seven varied crypto analysis posts plus three MARSOF product posts per day.

    A crypto post does not require a major event. If a real persisted market snapshot
    exists, the token can receive an editorial update; movement/activity only affects
    which tokens are prioritized first.
    """
    if not DAILY_CONTENT_ENABLED or not MONITOR_TELEGRAM_CHANNEL_ID:
        return {"status": "DISABLED", "crypto": 0, "marketing": 0}
    if not AI_ENABLED or not AI_API_KEY:
        return {"status": "AI_UNAVAILABLE", "crypto": 0, "marketing": 0}

    now = now_utc()
    today = now.strftime("%Y-%m-%d")
    session = MonitorSession()
    crypto_published = 0
    marketing_published = 0
    try:
        # The monitor analyzes the full WATCHLIST every cycle. Daily editorial posts
        # are a separate layer: choose the most notable REAL movement among all assets,
        # and do not permanently favor LOFI or any other token.
        crypto_slots = list(DAILY_CRYPTO_SLOTS[:DAILY_CRYPTO_POST_TARGET])
        used_tokens = set()
        existing_today = (session.query(DailyPost)
                          .filter_by(post_date=today, kind="CRYPTO")
                          .all())
        for post_row in existing_today:
            if post_row.published_telegram and post_row.token_symbol:
                used_tokens.add(str(post_row.token_symbol).upper())

        # Catch up missed crypto slots one at a time. This keeps the system reliable
        # after restarts without dumping six posts into the channel at once.
        due_crypto = []
        for slot in crypto_slots:
            if not _daily_slot_due(slot, now):
                continue
            existing = session.query(DailyPost).filter_by(post_date=today, slot=slot, kind="CRYPTO").first()
            if existing and existing.published_telegram:
                continue
            due_crypto.append((slot, existing))
        if due_crypto:
            slot, existing = due_crypto[0]
            candidates = []
            for item in WATCHLIST:
                payload = _build_daily_market_payload(session, item)
                if not payload or not payload.get("current"):
                    continue
                token = payload.get("token") or {}
                symbol = str(token.get("symbol") or item.get("name") or "").upper()
                if not symbol:
                    continue
                notability = _daily_notability_score(payload)
                repeat_penalty = 1 if symbol in used_tokens else 0
                # Prefer a fresh token for today's editorial rotation, but allow a
                # repeat only when every configured asset has already been used.
                candidates.append((repeat_penalty, -notability, symbol, item, payload))

            if candidates:
                fresh = [c for c in candidates if c[0] == 0]
                pool = fresh or candidates
                _, _, symbol, item, payload = sorted(pool, key=lambda x: (x[0], x[1], x[2]))[0]
                report = _generate_daily_ai_post(payload, "DAILY_CRYPTO")
                if report:
                    post = existing or DailyPost(post_date=today, slot=slot, kind="CRYPTO")
                    post.network = item["network"]
                    post.token_address = item["address"]
                    post.token_symbol = (payload.get("token") or {}).get("symbol") or symbol
                    post.ai_report = report
                    post.published_telegram = publish_telegram_channel(report)
                    if post.published_telegram:
                        if existing is None:
                            session.add(post)
                        session.commit()
                        crypto_published = 1
                    else:
                        session.rollback()
            else:
                print(f"ℹ️ Daily crypto slot {slot}: no real market snapshot available")

        # Marketing has the same reliable catch-up behavior: one missed slot is
        # published per cycle, and the next cycle can publish the next missed slot.
        marketing_slots = list(DAILY_MARKETING_SLOTS[:DAILY_MARKETING_POST_TARGET])
        due_marketing = []
        for slot_index, slot in enumerate(marketing_slots):
            if not _daily_slot_due(slot, now):
                continue
            existing = session.query(DailyPost).filter_by(post_date=today, slot=slot, kind="MARKETING").first()
            if existing and existing.published_telegram:
                continue
            due_marketing.append((slot_index, slot, existing))
        if due_marketing:
            slot_index, slot, existing = due_marketing[0]
            payload = {"marketing_topic": _daily_marketing_topic(slot_index)}
            report = _generate_daily_ai_post(payload, "MARKETING")
            if report:
                post = existing or DailyPost(post_date=today, slot=slot, kind="MARKETING")
                post.ai_report = report
                post.published_telegram = publish_telegram_channel(report, _random_marketing_image())
                if post.published_telegram:
                    if existing is None:
                        session.add(post)
                    session.commit()
                    marketing_published = 1
                else:
                    session.rollback()

        return {
            "status": "OK",
            "crypto": crypto_published,
            "marketing": marketing_published,
            "target_crypto": DAILY_CRYPTO_POST_TARGET,
            "target_marketing": DAILY_MARKETING_POST_TARGET,
        }
    except Exception as exc:
        session.rollback()
        print(f"⚠️ Daily content processing failed: {exc}")
        return {"status": "FAILED", "crypto": crypto_published, "marketing": marketing_published, "detail": str(exc)[:300]}
    finally:
        session.close()


def process_exceptional_whale_alerts():
    """Publish only fresh, verified whale alerts for the current watchlist.

    Display identity always comes from the real token metadata (for example
    BRETT), while the network is displayed separately (for example Base).
    Historical DB rows are not replayed after a process restart, and one TX
    cannot generate separate SENDER/RECEIVER public alerts.
    """
    if not WHALE_ALERTS_ENABLED:
        return 0
    if not TELEGRAM_TOKEN or not MONITOR_TELEGRAM_CHANNEL_ID or WHALE_ALERT_THRESHOLD_USD <= 0:
        return 0

    allowed = {
        (str(item.get("network") or "").strip().lower(), normalize_address(item.get("address"))): item
        for item in WATCHLIST
        if item.get("network") and item.get("address")
    }
    if not allowed:
        return 0

    session = MonitorSession()
    sent = 0
    try:
        rows = (session.query(WhaleEvent)
                .filter(WhaleEvent.value_usd.isnot(None),
                        WhaleEvent.value_usd >= WHALE_ALERT_THRESHOLD_USD,
                        WhaleEvent.telegram_alerted.is_(False),
                        WhaleEvent.detected_at.isnot(None),
                        WhaleEvent.detected_at >= MONITOR_PROCESS_START_TIME)
                .order_by(WhaleEvent.detected_at.asc())
                .limit(200).all())

        # Consolidate rows by one real blockchain transaction. A transfer can
        # legitimately create both SENDER and RECEIVER WhaleEvent rows, but it
        # must produce one public alert only.
        grouped = {}
        for row in rows:
            key = (
                str(row.network or "").strip().lower(),
                normalize_address(row.token_address),
                str(row.tx_hash or "").strip().lower(),
            )
            if (key[0], key[1]) not in allowed or not key[2]:
                continue
            grouped.setdefault(key, []).append(row)

        for key, group in grouped.items():
            row = group[0]
            item = allowed[(key[0], key[1])]

            # Resolve the real token identity. The WATCHLIST internal alias
            # (e.g. BASE-1) is never used in user-facing output.
            identity = resolve_token_identity({}, item, session)
            symbol = identity.get("symbol")
            name = identity.get("name")
            token_label = symbol or name or "Token name unavailable"

            roles = sorted({str(x.whale_role or "").strip().upper() for x in group if x.whale_role})
            role_label = " + ".join(roles) if roles else "UNAVAILABLE"

            lines = [
                "🐋 WHALE ALERT",
                f"{token_label} • {network_label(row.network)} • verified large movement",
                f"💰 Value: ${row.value_usd:,.2f}",
                f"👛 Wallet role(s): {role_label}",
                f"🔄 Verified type: {row.transfer_type or 'UNAVAILABLE'}",
            ]
            if row.amount_ui is not None:
                lines.append(f"🪙 Amount: {row.amount_ui:,.6f}")
            lines.append(f"🔗 TX: {row.tx_hash}")
            lines.append(f"Contract: {row.token_address}")

            if publish_telegram_channel("\n".join(lines)):
                # Mark every duplicate representation of this TX as alerted so
                # SENDER/RECEIVER rows cannot create repeated posts next cycle.
                for duplicate in group:
                    duplicate.telegram_alerted = True
                sent += 1

        session.commit()
        return sent
    except Exception as exc:
        session.rollback()
        print(f"ℹ️ Exceptional whale alerts unavailable: {exc}")
        return sent
    finally:
        session.close()


def run_cycle():
    if not WATCHLIST:
        print("⚠️ No enabled tokens are configured in WATCHLIST_CONFIG.")
        return []

    print(
        f"\n=== MARSOF Monitor cycle {now_utc().isoformat()} | "
        f"tokens={len(WATCHLIST)} | interval={MONITOR_INTERVAL_SECONDS}s ==="
    )
    results = []
    for item in WATCHLIST:
        results.append(monitor_token(item))

    # Exceptional verified whale movements are alerted directly; normal whale activity
    # remains evidence inside the daily Gemini analysis. Whale data never changes the analyzer score.
    whale_alerts = process_exceptional_whale_alerts()
    if whale_alerts:
        print(f"🐋 Exceptional whale alerts sent: {whale_alerts}")

    # Evidence and Event Aggregator complete first; Gemini runs only now.
    ai_result = process_pending_ai_events()
    print(
        f"🤖 AI batch: {ai_result.get('status')} | "
        f"eligible={ai_result.get('eligible', 0)} | "
        f"processed={ai_result.get('processed', 0)}"
    )

    # Daily content is independent from confirmed-event alerts. It uses only
    # persisted real market snapshots and never changes event detection/scoring.
    daily_result = process_daily_content()
    print(
        f"🗓️ Daily content: {daily_result.get('status')} | "
        f"crypto={daily_result.get('crypto', 0)}/{daily_result.get('target_crypto', DAILY_CRYPTO_POST_TARGET)} | "
        f"marketing={daily_result.get('marketing', 0)}/{daily_result.get('target_marketing', DAILY_MARKETING_POST_TARGET)}"
    )
    return results


def run_interactive_test():
    """Run one immediate monitoring pass and return a compact per-token report."""
    started = now_utc()
    results = []
    for item in WATCHLIST:
        result = monitor_token(item)
        results.append({
            "name": (result.get("identity") or {}).get("name"),
            "symbol": (result.get("identity") or {}).get("symbol"),
            "network": item["network"],
            "address": item["address"],
            "status": result.get("status", "UNKNOWN") if isinstance(result, dict) else "UNKNOWN",
            "activity_score": (result.get("activity") or {}).get("activity_score") if isinstance(result, dict) else None,
            "error": result.get("error") if isinstance(result, dict) else None,
        })
    # Keep test behavior aligned with production: AI is downstream of the
    # complete monitoring pass, not called from each token.
    ai_result = process_pending_ai_events()
    return {
        "started_at": started,
        "finished_at": now_utc(),
        "results": results,
        "ai": ai_result,
    }


def run_gemini_connectivity_test():
    """Make one minimal real Gemini request for /monitor_test only.

    This does not create a monitoring event, change snapshots, alter scoring,
    or publish anything. It only verifies that the configured Gemini API key
    and model can accept a request and return text.
    """
    if not AI_ENABLED:
        return {"status": "DISABLED", "model": AI_MODEL, "detail": "MONITOR_AI_ENABLED is not enabled"}
    if not AI_API_KEY:
        return {"status": "MISSING_KEY", "model": AI_MODEL, "detail": "GEMINI_API_KEY is not configured"}

    try:
        report = generate_ai_report({
            "test": True,
            "purpose": "Gemini connectivity check",
            "instruction": "Reply with exactly: GEMINI_OK",
            "verified_data": {"source": "MARSOF monitor test", "status": "connectivity_only"},
        })
        if report:
            return {"status": "OK", "model": AI_MODEL, "detail": report[:160].replace("\n", " ")}
        return {"status": "NO_RESPONSE", "model": AI_MODEL, "detail": "Gemini returned no text"}
    except Exception as exc:
        return {"status": "FAILED", "model": AI_MODEL, "detail": str(exc)[:300]}


def format_interactive_test(report):
    """Format a short Telegram report without exposing full addresses."""
    rows = report.get("results", [])
    counts = {"OK": 0, "BASELINE": 0, "NORMAL": 0, "EVENT": 0, "COOLDOWN": 0, "ERROR": 0, "NO_RESULT": 0}
    for row in rows:
        status = row.get("status", "UNKNOWN")
        if status in counts:
            counts[status] += 1

    lines = [
        "🧪 <b>MARSOF AI — MONITOR TEST</b>",
        "",
        f"Assets configured: <b>{len(rows)}/{len(WATCHLIST)}</b>",
        f"Errors: <b>{counts['ERROR']}</b>",
        f"Baseline: {counts['BASELINE']} · Normal: {counts['NORMAL']} · Events: {counts['EVENT']} · Cooldown: {counts['COOLDOWN']}",
        "",
    ]

    icons = {
        "BASELINE": "🟡",
        "NORMAL": "🟢",
        "EVENT": "🚨",
        "COOLDOWN": "🟠",
        "ERROR": "🔴",
        "NO_RESULT": "⚪",
    }
    for row in rows:
        status = row.get("status", "UNKNOWN")
        icon = icons.get(status, "⚪")
        score = row.get("activity_score")
        score_text = f" · {score:.1f}" if isinstance(score, (int, float)) else ""
        identity_name = row.get("name") or "Token name unavailable"
        identity_symbol = row.get("symbol")
        identity_text = f"{identity_name} ({identity_symbol})" if identity_symbol and identity_symbol != identity_name else identity_name
        extra = f" — {row['error']}" if status == "ERROR" and row.get("error") else ""
        lines.append(f"{icon} <b>{identity_text}</b> · {row['network']} · {status}{score_text}{extra}")

    gemini = report.get("gemini_test") or {}
    gemini_status = gemini.get("status", "NOT_RUN")
    gemini_icon = {"OK": "🟢", "DISABLED": "🟡", "MISSING_KEY": "🔴", "NO_RESPONSE": "🔴", "FAILED": "🔴"}.get(gemini_status, "⚪")
    lines.extend([
        "",
        f"🤖 <b>Gemini:</b> {gemini_icon} <b>{gemini_status}</b> · {gemini.get('model', AI_MODEL)}",
    ])
    if gemini.get("detail"):
        lines.append(f"<i>{gemini['detail']}</i>")

    x_test = report.get("x_test") or {}
    x_status = x_test.get("status", "NOT_RUN")
    x_icon = {"OK": "🟢", "MISSING_KEY": "🔴", "FAILED": "🔴", "USER_OK_POSTS_FAILED": "🟠"}.get(x_status, "⚪")
    x_detail = x_test.get("detail") or ""
    if x_status == "OK":
        x_detail = (
            f"@{(x_test.get('user') or {}).get('username', 'MARSOF_AI')} · "
            f"recent posts: {x_test.get('recent_posts', 0)} · "
            f"with public metrics: {x_test.get('posts_with_public_metrics', 0)}"
        )
    lines.extend([
        "",
        f"𝕏 <b>X API:</b> {x_icon} <b>{x_status}</b>",
    ])
    if x_detail:
        lines.append(f"<i>{x_detail[:900]}</i>")

    lines.extend([
        "",
        "<i>Monitor test uses the same monitor_token() path as the live monitor. Gemini and X checks are connectivity/read-access tests only; no event, scoring, snapshot, or publishing logic is changed.</i>",
    ])
    return "\n".join(lines)




def run_isolated_event_flow_test():
    """Exercise DETECTED -> CONFIRMING -> CONFIRMED -> Gemini in isolation.

    This test deliberately does NOT call monitor_token(), the live watchlist,
    market APIs, holder APIs, transfer APIs, snapshots, scoring, Telegram
    alerts, or X publishing. A synthetic MonitorEvent/confirmation is created
    only inside an uncommitted DB transaction and rolled back at the end.
    Gemini receives the same evidence-shaped payload used by real events, but
    no synthetic event is persisted.
    """
    started = now_utc()
    session = MonitorSession()
    synthetic_address = "0x" + "0" * 39 + "1"
    result = {
        "status": "STARTED",
        "started_at": started,
        "finished_at": None,
        "stages": [],
        "db_persisted": False,
        "gemini": None,
    }

    try:
        # Keep every DB mutation inside this session. We intentionally never
        # commit; the final rollback guarantees zero persistent test data.
        payload = {
            "token": {
                "address": synthetic_address,
                "network": "sui",
                "network_name": network_label("sui"),
                "name": "MARSOF Synthetic Test Token",
                "symbol": "MTEST",
                "identity_source": "INTERNAL_TEST",
                "identity_verified": True,
            },
            "snapshot": {
                "timestamp": now_utc().isoformat(),
                "price_usd": 1.25,
                "market_cap_usd": 125000.0,
                "liquidity_usd": 50000.0,
                "volume_5m_usd": 12000.0,
                "volume_1h_usd": 35000.0,
                "volume_24h_usd": 150000.0,
                "buys_5m": 42,
                "sells_5m": 8,
                "price_change_5m": 3.4,
                "price_change_1h": 7.8,
            },
            "activity": {
                "activity_score": 95.0,
                "severity": "HIGH",
                "pattern": "SYNTHETIC_TEST_ACTIVITY",
                "evidence_count": 2,
                "signals": [
                    {"name": "synthetic_volume_spike", "detail": "Synthetic test evidence only."},
                    {"name": "synthetic_buy_imbalance", "detail": "Synthetic test evidence only."},
                ],
                "test_only": True,
            },
            "test_only": True,
        }

        event = MonitorEvent(
            chain_id=chain_id_for("sui"),
            network="sui",
            token_address=synthetic_address,
            token_symbol="MTEST",
            activity_score=95.0,
            severity="HIGH",
            payload=json.dumps(payload, ensure_ascii=False, default=str),
            triggered_at=now_utc(),
        )
        session.add(event)
        session.flush()

        confirmation = EventConfirmation(
            monitor_event_id=event.id,
            chain_id=chain_id_for("sui"),
            network="sui",
            token_address=synthetic_address,
            status="DETECTED",
            evidence_count=0,
            whale_count=0,
            integrity_verified=False,
            reason="Synthetic isolated test event created in an uncommitted transaction.",
        )
        session.add(confirmation)
        session.flush()
        result["stages"].append({"stage": "DETECTED", "event_id": event.id})

        confirmation.status = "CONFIRMING"
        confirmation.evidence_count = 1
        confirmation.integrity_verified = True
        confirmation.reason = "Synthetic confirmation stage: waiting for additional verified evidence."
        confirmation.updated_at = now_utc()
        session.flush()
        result["stages"].append({"stage": "CONFIRMING", "event_id": event.id})

        confirmation.status = "CONFIRMED"
        confirmation.evidence_count = 2
        confirmation.integrity_verified = True
        confirmation.confirmed_at = now_utc()
        confirmation.updated_at = now_utc()
        confirmation.reason = "Synthetic isolated test: two verified evidence signals."
        session.flush()
        result["stages"].append({"stage": "CONFIRMED", "event_id": event.id})

        # Gemini is deliberately called only after the synthetic event reaches
        # CONFIRMED, matching the production downstream boundary.
        if AI_ENABLED and AI_API_KEY:
            ai_report = generate_ai_report(payload)
            if ai_report:
                event.ai_report = ai_report
                event.ai_processed = True
                session.flush()
                result["gemini"] = {
                    "status": "OK",
                    "model": AI_MODEL,
                    "detail": ai_report[:500],
                }
                result["stages"].append({"stage": "GEMINI", "status": "OK"})
            else:
                result["gemini"] = {
                    "status": "NO_RESPONSE",
                    "model": AI_MODEL,
                    "detail": "Gemini returned no report.",
                }
                result["stages"].append({"stage": "GEMINI", "status": "NO_RESPONSE"})
        elif not AI_ENABLED:
            result["gemini"] = {
                "status": "DISABLED",
                "model": AI_MODEL,
                "detail": "MONITOR_AI_ENABLED is not enabled.",
            }
            result["stages"].append({"stage": "GEMINI", "status": "DISABLED"})
        else:
            result["gemini"] = {
                "status": "MISSING_KEY",
                "model": AI_MODEL,
                "detail": "GEMINI_API_KEY is not configured.",
            }
            result["stages"].append({"stage": "GEMINI", "status": "MISSING_KEY"})

        result["status"] = "OK" if result["gemini"]["status"] == "OK" else result["gemini"]["status"]
        return result
    except Exception as exc:
        session.rollback()
        result["status"] = "FAILED"
        result["detail"] = str(exc)[:700]
        return result
    finally:
        # Critical isolation guarantee: synthetic rows are never persisted.
        try:
            session.rollback()
        except Exception:
            pass
        session.close()
        result["db_persisted"] = False
        result["finished_at"] = now_utc()


def format_isolated_event_flow_test(result):
    """Format the isolated event-flow test for Telegram."""
    status = result.get("status", "UNKNOWN")
    icon = {"OK": "🟢", "DISABLED": "🟡", "MISSING_KEY": "🟠", "NO_RESPONSE": "🔴", "FAILED": "🔴"}.get(status, "⚪")
    lines = [
        "🧪 <b>MARSOF AI — ISOLATED EVENT FLOW TEST</b>",
        "",
        f"Result: {icon} <b>{status}</b>",
        "",
        "Flow:",
    ]
    for stage in result.get("stages", []):
        stage_name = stage.get("stage", "UNKNOWN")
        stage_status = stage.get("status")
        suffix = f" · {stage_status}" if stage_status else ""
        lines.append(f"• {stage_name}<b>{suffix}</b>")

    gemini = result.get("gemini") or {}
    lines.extend([
        "",
        f"🤖 Gemini: <b>{gemini.get('status', 'NOT_RUN')}</b> · {gemini.get('model', AI_MODEL)}",
        "🗄️ Synthetic DB data persisted: <b>NO</b>",
        "🔒 Live watchlist / snapshots / scoring / Telegram alerts / X publishing: <b>NOT TOUCHED</b>",
    ])
    if gemini.get("detail"):
        lines.extend(["", f"<i>{str(gemini['detail'])[:900]}</i>"])
    if result.get("detail"):
        lines.extend(["", f"<i>{str(result['detail'])[:700]}</i>"])
    return "\n".join(lines)


def register_telegram_isolated_event_test_command():
    """Register /monitor_event_flow_test for the isolated state-machine test."""
    if not MONITOR_TEST_ENABLED:
        return False
    telegram_bot = getattr(core, "bot", None)
    if telegram_bot is None:
        return False
    if getattr(telegram_bot, "_marsof_monitor_event_flow_test_registered", False):
        return True

    @telegram_bot.message_handler(commands=["monitor_event_flow_test"])
    def monitor_event_flow_test_command(message):
        chat_id = str(getattr(message.chat, "id", ""))
        if MONITOR_TEST_CHAT_ID and chat_id != str(MONITOR_TEST_CHAT_ID):
            try:
                telegram_bot.reply_to(message, "⛔ Monitor event-flow test is restricted.")
            except Exception:
                pass
            return

        try:
            telegram_bot.send_message(
                chat_id,
                "🧪 <b>Isolated event-flow test started.</b>\n\nNo live token or watchlist will be scanned.",
                parse_mode="HTML",
            )
        except Exception as exc:
            print(f"⚠️ isolated event-flow acknowledgement failed: {exc}")

        def worker():
            try:
                report = run_isolated_event_flow_test()
                text = format_isolated_event_flow_test(report)
            except Exception as exc:
                text = f"❌ <b>Isolated event-flow test failed</b>\n\n<code>{str(exc)[:1000]}</code>"
            try:
                telegram_bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception as exc:
                print(f"⚠️ isolated event-flow Telegram response failed: {exc}")

        threading.Thread(target=worker, name="marsof-monitor-event-flow-test", daemon=True).start()
        return True

    try:
        if getattr(telegram_bot, "message_handlers", None):
            for idx in range(len(telegram_bot.message_handlers) - 1, -1, -1):
                handler = telegram_bot.message_handlers[idx]
                func = handler.get("function") if isinstance(handler, dict) else None
                if func is monitor_event_flow_test_command:
                    telegram_bot.message_handlers.insert(0, telegram_bot.message_handlers.pop(idx))
                    break
    except Exception as exc:
        print(f"⚠️ Could not prioritize /monitor_event_flow_test handler: {exc}")

    telegram_bot._marsof_monitor_event_flow_test_registered = True
    print("✅ /monitor_event_flow_test command registered")
    return True

def register_telegram_ai_test_command():
    """Register /monitor_ai_test to process existing qualifying events only."""
    if not MONITOR_TEST_ENABLED:
        return False
    telegram_bot = getattr(core, "bot", None)
    if telegram_bot is None:
        return False
    if getattr(telegram_bot, "_marsof_monitor_ai_test_registered", False):
        return True

    @telegram_bot.message_handler(commands=["monitor_ai_test"])
    def monitor_ai_test_command(message):
        chat_id = str(getattr(message.chat, "id", ""))
        if MONITOR_TEST_CHAT_ID and chat_id != str(MONITOR_TEST_CHAT_ID):
            try:
                telegram_bot.reply_to(message, "⛔ Monitor AI test is restricted.")
            except Exception:
                pass
            return
        try:
            result = process_pending_ai_events(force_reprocess=True, publish_x_posts=False)
            lines = [
                "🧪 <b>MARSOF AI — AI EVENT TEST</b>",
                "",
                f"Status: <b>{result.get('status', 'UNKNOWN')}</b>",
                f"Eligible events: <b>{result.get('eligible', 0)}</b>",
                f"Processed by Gemini: <b>{result.get('processed', 0)}</b>",
                f"Failed: <b>{result.get('failed', 0)}</b>",
                f"Threshold: <b>{result.get('threshold', AI_EVENT_THRESHOLD)}</b>",
            ]
            if result.get("detail"):
                lines.extend(["", f"<i>{str(result['detail'])[:700]}</i>"])
            telegram_bot.send_message(chat_id, "\n".join(lines), parse_mode="HTML")
        except Exception as exc:
            telegram_bot.send_message(chat_id, f"❌ monitor_ai_test failed: {str(exc)[:700]}")

    # Keep this command ahead of any generic Telegram text handlers.
    try:
        if getattr(telegram_bot, "message_handlers", None):
            for idx in range(len(telegram_bot.message_handlers) - 1, -1, -1):
                handler = telegram_bot.message_handlers[idx]
                func = handler.get("function") if isinstance(handler, dict) else None
                if func is monitor_ai_test_command:
                    telegram_bot.message_handlers.insert(0, telegram_bot.message_handlers.pop(idx))
                    break
    except Exception as exc:
        print(f"⚠️ Could not prioritize /monitor_ai_test handler: {exc}")

    telegram_bot._marsof_monitor_ai_test_registered = True
    print("✅ /monitor_ai_test command registered")
    return True

def register_telegram_events_command():
    """Register /monitor_events to inspect the recent event aggregation board."""
    if not MONITOR_TEST_ENABLED:
        return False
    telegram_bot = getattr(core, "bot", None)
    if telegram_bot is None:
        return False

    def handler(message):
        if MONITOR_TEST_CHAT_ID and str(getattr(message.chat, "id", "")) != str(MONITOR_TEST_CHAT_ID):
            return
        try:
            report = aggregate_recent_events()
            telegram_bot.send_message(
                message.chat.id,
                format_monitor_events(report),
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
        except Exception as exc:
            telegram_bot.send_message(message.chat.id, f"❌ monitor_events failed: {str(exc)[:500]}")

    try:
        telegram_bot.message_handler(commands=["monitor_events"])(handler)

        # pyTelegramBotAPI checks handlers in registration order. The stable
        # main.py may already have a generic text handler registered before
        # the monitor is loaded, so make /monitor_events the first matching
        # handler just like /monitor_test.
        if getattr(telegram_bot, "message_handlers", None):
            for idx in range(len(telegram_bot.message_handlers) - 1, -1, -1):
                item = telegram_bot.message_handlers[idx]
                func = item.get("function") if isinstance(item, dict) else None
                if func is handler:
                    telegram_bot.message_handlers.insert(0, telegram_bot.message_handlers.pop(idx))
                    break

        print("✅ /monitor_events command registered")
        return True
    except Exception as exc:
        print(f"⚠️ /monitor_events registration failed: {exc}")
        return False


def register_telegram_test_command():
    """Register /monitor_test without modifying main.py."""
    if not MONITOR_TEST_ENABLED:
        return False

    telegram_bot = getattr(core, "bot", None)
    if telegram_bot is None:
        print("ℹ️ Monitor test command unavailable: Telegram bot is not configured.")
        return False
    if getattr(telegram_bot, "_marsof_monitor_test_registered", False):
        return True

    @telegram_bot.message_handler(commands=["monitor_test"])
    def monitor_test_command(message):
        chat_id = str(getattr(message.chat, "id", ""))
        if MONITOR_TEST_CHAT_ID and chat_id != str(MONITOR_TEST_CHAT_ID):
            try:
                telegram_bot.reply_to(message, "⛔ Monitor test is restricted.")
            except Exception:
                pass
            return

        try:
            telegram_bot.send_message(
                chat_id,
                "🧪 <b>Monitor test started.</b>\n\nChecking all configured assets now…",
                parse_mode="HTML",
            )
        except Exception as exc:
            print(f"⚠️ monitor test acknowledgement failed: {exc}")

        def worker():
            try:
                report = run_interactive_test()
                report["gemini_test"] = run_gemini_connectivity_test()
                report["x_test"] = run_x_connectivity_test()
                text = format_interactive_test(report)
            except Exception as exc:
                text = f"❌ <b>Monitor test failed</b>\n\n<code>{str(exc)[:1000]}</code>"
            try:
                telegram_bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception as exc:
                print(f"⚠️ monitor test Telegram response failed: {exc}")

        threading.Thread(target=worker, name="marsof-monitor-test", daemon=True).start()
        return True

    # pyTelegramBotAPI checks handlers in registration order. main.py may
    # already have a generic text handler registered before this monitor is
    # loaded, so make /monitor_test the first matching handler explicitly.
    try:
        if getattr(telegram_bot, "message_handlers", None):
            for idx in range(len(telegram_bot.message_handlers) - 1, -1, -1):
                handler = telegram_bot.message_handlers[idx]
                func = handler.get("function") if isinstance(handler, dict) else None
                if func is monitor_test_command:
                    telegram_bot.message_handlers.insert(0, telegram_bot.message_handlers.pop(idx))
                    break
    except Exception as exc:
        print(f"⚠️ Could not prioritize /monitor_test handler: {exc}")

    telegram_bot._marsof_monitor_test_registered = True
    print("✅ Telegram /monitor_test command registered.")
    return True


def run_monitor_forever():
    """Run the monitoring loop without owning the web service process."""
    print("🚀 MARSOF AI Phase-2 Monitor starting")
    print(f"Core: {CORE_FILE}")
    print(f"Watchlist: {len(WATCHLIST)} token(s)")
    print(f"Activity threshold: {MONITOR_MIN_ACTIVITY_SCORE}")
    print(f"Cooldown: {MONITOR_COOLDOWN_MINUTES} minutes")
    print(f"🔗 Transfer Monitor: {'ENABLED' if TRANSFER_MONITOR_ENABLED else 'DISABLED'}")
    print(f"🗄️ Transfer DB table: monitor_transfer_events")
    print(f"🐋 Whale Detection: ENABLED | threshold=${WHALE_THRESHOLD_USD:,.0f} | alert>=${WHALE_ALERT_THRESHOLD_USD:,.0f}")
    print("🧭 Event Confirmation: ENABLED | verified evidence gate")

    if not WATCHLIST:
        print("⚠️ WATCHLIST_CONFIG is empty or contains no valid enabled entries.")
        return

    while True:
        try:
            run_cycle()
        except Exception as exc:
            # Never allow a monitor-cycle failure to terminate the Telegram web service.
            print(f"❌ Monitor cycle failed: {exc}")
        if MONITOR_RUN_ONCE:
            break
        time.sleep(MONITOR_INTERVAL_SECONDS)


def register_telegram_channel_test_command():
    """Publish an existing unpublished Gemini report to the MARSOF channel."""
    if not MONITOR_TEST_ENABLED:
        return False
    telegram_bot = getattr(core, "bot", None)
    if telegram_bot is None:
        return False
    if getattr(telegram_bot, "_marsof_monitor_channel_test_registered", False):
        return True

    @telegram_bot.message_handler(commands=["monitor_channel_test"])
    def monitor_channel_test_command(message):
        chat_id = str(getattr(message.chat, "id", ""))
        if MONITOR_TEST_CHAT_ID and chat_id != str(MONITOR_TEST_CHAT_ID):
            try:
                telegram_bot.reply_to(message, "⛔ Monitor channel test is restricted.")
            except Exception:
                pass
            return
        session = MonitorSession()
        try:
            event = (session.query(MonitorEvent)
                     .filter(MonitorEvent.ai_report.isnot(None),
                             MonitorEvent.ai_report != "",
                             or_(MonitorEvent.published_telegram.is_(False), MonitorEvent.published_telegram.is_(None)))
                     .order_by(MonitorEvent.triggered_at.desc())
                     .first())
            if not event:
                telegram_bot.send_message(chat_id, "ℹ️ No unpublished Gemini report is available for the channel test. Run /monitor_ai_test first when a qualifying confirmed event exists.")
                return
            ok = publish_telegram_channel(event.ai_report)
            if ok:
                event.published_telegram = True
                session.commit()
                telegram_bot.send_message(chat_id, f"✅ Telegram channel test succeeded.\nEvent: {event.id}\nThe Gemini alert was published to the configured channel.")
            else:
                session.rollback()
                telegram_bot.send_message(chat_id, "❌ Channel publication failed. Check MONITOR_TELEGRAM_CHANNEL_ID, TELEGRAM_TOKEN, channel admin/posting permissions, and MARSOF_AI_URL if configured.")
        except Exception as exc:
            session.rollback()
            telegram_bot.send_message(chat_id, f"❌ monitor_channel_test failed: {str(exc)[:700]}")
        finally:
            session.close()

    try:
        handlers = getattr(telegram_bot, "message_handlers", None)
        if isinstance(handlers, list) and handlers:
            handler = handlers.pop()
            handlers.insert(0, handler)
    except Exception:
        pass
    telegram_bot._marsof_monitor_channel_test_registered = True
    return True




def register_telegram_whale_ai_test_command():
    """Run an isolated real-data whale -> Gemini verification without public publishing."""
    if not MONITOR_TEST_ENABLED:
        return False
    telegram_bot = getattr(core, "bot", None)
    if telegram_bot is None:
        return False
    if getattr(telegram_bot, "_marsof_whale_ai_test_registered", False):
        return True

    @telegram_bot.message_handler(commands=["whale_ai_test"])
    def whale_ai_test_command(message):
        chat_id = str(getattr(message.chat, "id", ""))
        if MONITOR_TEST_CHAT_ID and chat_id != str(MONITOR_TEST_CHAT_ID):
            try:
                telegram_bot.reply_to(message, "⛔ Whale AI test is restricted.")
            except Exception:
                pass
            return

        def send_stage(text):
            try:
                telegram_bot.send_message(chat_id, text, parse_mode="HTML", disable_web_page_preview=True)
            except Exception as exc:
                print(f"⚠️ whale_ai_test stage message failed: {exc}")

        def worker():
            session = MonitorSession()
            try:
                send_stage("🧪 <b>MARSOF AI — WHALE → GEMINI TEST</b>\n\nStep 1/3 — finding a real token with persisted verified whale activity…")

                # Pick the newest real WhaleEvent that also belongs to the configured watchlist.
                allowed = {
                    (str(item.get("network") or ""), normalize_address(item.get("address")))
                    for item in WATCHLIST
                    if item.get("network") and item.get("address")
                }
                whale_row = None
                for row in (session.query(WhaleEvent)
                            .filter(WhaleEvent.value_usd.isnot(None))
                            .order_by(WhaleEvent.detected_at.desc())
                            .limit(200).all()):
                    key = (str(row.network or ""), normalize_address(row.token_address))
                    if key in allowed:
                        whale_row = row
                        break

                if not whale_row:
                    send_stage("⚠️ <b>Whale AI test stopped.</b>\n\nNo persisted verified whale event was found for a configured watchlist token.")
                    return

                item = next((x for x in WATCHLIST if
                             str(x.get("network") or "") == str(whale_row.network or "") and
                             normalize_address(x.get("address")) == normalize_address(whale_row.token_address)), None)
                if not item:
                    send_stage("⚠️ <b>Whale AI test stopped.</b>\n\nThe real whale event could not be mapped back to WATCHLIST_CONFIG.")
                    return

                payload = _build_daily_market_payload(session, item)
                if not payload:
                    send_stage("⚠️ <b>Whale AI test stopped at Step 1.</b>\n\nThe token has whale data, but no persisted market snapshot is available for the daily Gemini payload.")
                    return

                whale_data = payload.get("whale_activity") or {}
                status = whale_data.get("status")
                count = whale_data.get("event_count")
                total = whale_data.get("total_verified_value_usd")
                if status != "DATA" or not count:
                    send_stage("⚠️ <b>Whale AI test stopped at Step 1.</b>\n\nWhale activity was not exposed as DATA in the same daily payload sent to Gemini.")
                    return

                token = payload.get("token") or {}
                send_stage(
                    "🧪 <b>WHALE → GEMINI TEST</b>\n\n"
                    "Step 2/3 — real whale data found and included in the daily payload. Asking Gemini…\n\n"
                    f"• Token: <b>${html.escape(str(token.get('symbol') or token.get('name') or 'UNKNOWN'))}</b>\n"
                    f"• Network: <b>{html.escape(str(token.get('network_name') or token.get('network') or 'UNKNOWN'))}</b>\n"
                    f"• Verified whale transfers: <b>{count}</b>\n"
                    f"• Verified whale value: <b>{html.escape(str(total)) if total is not None else 'DATA unavailable'}</b>"
                )

                report = _generate_daily_ai_post(payload, "DAILY_CRYPTO")
                if not report:
                    send_stage("❌ <b>Whale AI test stopped at Step 2.</b>\n\nGemini returned no usable report.")
                    return

                lower = report.lower()
                whale_keywords = ("whale", "large transfer", "large movement", "transfer")
                mentioned = any(k in lower for k in whale_keywords)
                value_text = f"${total:,.2f}" if isinstance(total, (int, float)) else None
                value_mentioned = bool(value_text and value_text.replace(",", "") in report.replace(",", ""))

                send_stage(
                    "🧪 <b>WHALE → GEMINI TEST</b>\n\n"
                    "Step 3/3 — Gemini report received. No public channel post was sent by this test.\n\n"
                    f"• Whale data sent to Gemini: <b>YES</b>\n"
                    f"• Gemini mentioned whale activity: <b>{'YES' if mentioned else 'NO'}</b>\n"
                    f"• Verified value mentioned: <b>{'YES' if value_mentioned else 'NO'}</b>\n\n"
                    "<b>Gemini report:</b>\n" + html.escape(report[:3500])
                )
            except Exception as exc:
                print(f"⚠️ whale_ai_test worker failed: {exc}")
                try:
                    telegram_bot.send_message(chat_id, f"❌ <b>whale_ai_test failed</b>\n\n<code>{html.escape(str(exc)[:1200])}</code>", parse_mode="HTML")
                except Exception:
                    pass
            finally:
                session.close()

        threading.Thread(target=worker, name="marsof-whale-ai-test", daemon=True).start()
        return True

    try:
        if getattr(telegram_bot, "message_handlers", None):
            for idx in range(len(telegram_bot.message_handlers) - 1, -1, -1):
                handler = telegram_bot.message_handlers[idx]
                func = handler.get("function") if isinstance(handler, dict) else None
                if func is whale_ai_test_command:
                    telegram_bot.message_handlers.insert(0, telegram_bot.message_handlers.pop(idx))
                    break
    except Exception as exc:
        print(f"⚠️ Could not prioritize /whale_ai_test handler: {exc}")

    telegram_bot._marsof_whale_ai_test_registered = True
    print("✅ /whale_ai_test command registered")
    return True

def register_telegram_daily_test_command():
    """Run a real LOFI daily-post test and publish the result to the configured channel."""
    if not MONITOR_TEST_ENABLED:
        return False
    telegram_bot = getattr(core, "bot", None)
    if telegram_bot is None:
        return False
    if getattr(telegram_bot, "_marsof_daily_test_registered", False):
        return True

    @telegram_bot.message_handler(commands=["daily_test"])
    def daily_test_command(message):
        chat_id = str(getattr(message.chat, "id", ""))
        if MONITOR_TEST_CHAT_ID and chat_id != str(MONITOR_TEST_CHAT_ID):
            try:
                telegram_bot.reply_to(message, "⛔ Daily publishing test is restricted.")
            except Exception:
                pass
            return

        try:
            telegram_bot.send_message(
                chat_id,
                "🧪 <b>MARSOF AI — DAILY LOFI TEST</b>\n\n"
                "Step 1/3 — reading the latest real LOFI market snapshots…",
                parse_mode="HTML",
            )
        except Exception as exc:
            print(f"⚠️ daily test acknowledgement failed: {exc}")

        def send_stage(text):
            try:
                telegram_bot.send_message(chat_id, text, parse_mode="HTML", disable_web_page_preview=True)
            except Exception as exc:
                print(f"⚠️ daily test stage message failed: {exc}")

        def worker():
            session = MonitorSession()
            try:
                lofi_item = next(
                    (item for item in WATCHLIST if str(item.get("name") or "").upper() == "LOFI"),
                    None,
                )
                if not lofi_item:
                    send_stage("❌ <b>LOFI test failed:</b> LOFI is not present in WATCHLIST_CONFIG.")
                    return

                payload = _build_daily_market_payload(session, lofi_item)
                if not payload:
                    send_stage(
                        "⚠️ <b>LOFI test stopped at Step 1.</b>\n\n"
                        "No persisted real market snapshots are currently available for LOFI."
                    )
                    return

                current = payload.get("current") or {}
                send_stage(
                    "🧪 <b>DAILY LOFI TEST</b>\n\n"
                    "Step 2/3 — real snapshots found: "
                    f"<b>{payload.get('snapshot_count', 0)}</b>. Asking Gemini for the analysis…"
                )

                report = _generate_daily_ai_post(payload, "DAILY_CRYPTO")
                if not report:
                    send_stage(
                        "❌ <b>LOFI test stopped at Step 2.</b>\n\n"
                        "Gemini returned no usable report. Check Render logs and GEMINI_API_KEY / MONITOR_AI_MODEL."
                    )
                    return

                send_stage("🧪 <b>DAILY LOFI TEST</b>\n\nStep 3/3 — Gemini report ready. Publishing it to the configured MARSOF Telegram channel…")

                published = publish_telegram_channel(report)
                if not published:
                    send_stage(
                        "❌ <b>LOFI test stopped at Step 3.</b>\n\n"
                        "Gemini succeeded, but Telegram channel publication failed. Check "
                        "MONITOR_TELEGRAM_CHANNEL_ID, TELEGRAM_TOKEN, and channel admin/posting permissions."
                    )
                    return

                changes = payload.get("observed_price_change") or {}
                send_stage(
                    "✅ <b>MARSOF AI — DAILY LOFI TEST PASSED</b>\n\n"
                    "The real LOFI analysis was generated and <b>published to the configured channel</b>.\n\n"
                    f"• Snapshots: <b>{payload.get('snapshot_count', 0)}</b>\n"
                    f"• Price: <b>{current.get('price_usd') if current.get('price_usd') is not None else 'DATA unavailable'}</b>\n"
                    f"• 1h: <b>{changes.get('1h') if changes.get('1h') is not None else 'DATA unavailable'}</b>\n"
                    f"• 6h: <b>{changes.get('6h') if changes.get('6h') is not None else 'DATA unavailable'}</b>\n"
                    f"• 24h: <b>{changes.get('24h') if changes.get('24h') is not None else 'DATA unavailable'}</b>\n"
                    f"• Volume 24h: <b>{current.get('volume_24h_usd') if current.get('volume_24h_usd') is not None else 'DATA unavailable'}</b>\n"
                    f"• Liquidity: <b>{current.get('liquidity_usd') if current.get('liquidity_usd') is not None else 'DATA unavailable'}</b>"
                )
            except Exception as exc:
                print(f"⚠️ daily test worker failed: {exc}")
                try:
                    telegram_bot.send_message(
                        chat_id,
                        f"❌ <b>daily_test failed</b>\n\n<code>{html.escape(str(exc)[:1200])}</code>",
                        parse_mode="HTML",
                    )
                except Exception:
                    pass
            finally:
                session.close()

        threading.Thread(target=worker, name="marsof-daily-test", daemon=True).start()
        return True

    try:
        if getattr(telegram_bot, "message_handlers", None):
            for idx in range(len(telegram_bot.message_handlers) - 1, -1, -1):
                handler = telegram_bot.message_handlers[idx]
                func = handler.get("function") if isinstance(handler, dict) else None
                if func is daily_test_command:
                    telegram_bot.message_handlers.insert(0, telegram_bot.message_handlers.pop(idx))
                    break
    except Exception as exc:
        print(f"⚠️ Could not prioritize /daily_test handler: {exc}")

    telegram_bot._marsof_daily_test_registered = True
    print("✅ /daily_test command registered")
    return True

def start_monitor_background():
    """Start one daemon monitor thread for embedded Render Web Service mode."""
    register_telegram_test_command()
    register_telegram_events_command()
    register_telegram_ai_test_command()
    register_telegram_channel_test_command()
    register_telegram_whale_ai_test_command()
    register_telegram_daily_test_command()
    register_telegram_isolated_event_test_command()
    if getattr(start_monitor_background, "_started", False):
        print("ℹ️ MARSOF monitor background thread is already running.")
        return getattr(start_monitor_background, "_thread", None)

    thread = threading.Thread(
        target=run_monitor_forever,
        name="marsof-monitor",
        daemon=True,
    )
    thread.start()
    start_monitor_background._started = True
    start_monitor_background._thread = thread
    print("✅ MARSOF monitor background thread started.")
    return thread


def main():
    # Standalone mode remains available for local testing or a future paid worker.
    run_monitor_forever()


if __name__ == "__main__":
    main()
