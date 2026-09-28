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
from pathlib import Path
from datetime import datetime, timezone, timedelta

from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, Text
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

# Phase-2 watchlist is intentionally stored in this file.
# Runtime secrets/configuration (DB, API keys, interval, etc.) remain in Environment.
#
# NOTE: 13 assets are configured now. We will add the remaining assets when
# their exact network + contract addresses are supplied; no address is invented.
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
        "name": "BASE-2",
        "network": "base",
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
        "name": "SOLANA-1",
        "network": "solana",
        "address": "5UUH9RTDiSpq6HKS6bp4NdU9PNJpXRXuiw6ShBTBhgH2",
        "enabled": True,
    },
    {
        "name": "SOLANA-2",
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
]

# Optional Telegram alerting. This worker does not require it.
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
MONITOR_TELEGRAM_CHAT_ID = os.getenv("MONITOR_TELEGRAM_CHAT_ID")

# Optional AI stage. Detection works without AI.
AI_ENABLED = os.getenv("MONITOR_AI_ENABLED", "0").lower() in {"1", "true", "yes"}
AI_API_KEY = os.getenv("OPENAI_API_KEY")
AI_MODEL = os.getenv("MONITOR_AI_MODEL", "gpt-5.6")

# Optional X/Twitter stage. Publishing is intentionally disabled until enabled.
X_ENABLED = os.getenv("MONITOR_X_ENABLED", "0").lower() in {"1", "true", "yes"}

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

# ============================================================
# GENERIC HELPERS
# ============================================================


def now_utc():
    return datetime.now(timezone.utc)


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

    def add_signal(name, value, points, detail):
        nonlocal score
        if value:
            score += points
            signals.append({"name": name, "points": points, "detail": detail})

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
        f"5m buy share {buy_ratio * 100:.1f}%",
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
        "signals": signals,
        "metrics": {
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


def store_event(session, item, activity, current):
    address = normalize_address(item["address"])
    network = item["network"]
    token_symbol = None

    # Token metadata is optional; do not fabricate a symbol.
    if Token is not None:
        token = (
            session.query(Token)
            .filter(
                Token.contract_address == address,
                Token.chain_id == chain_id_for(network),
            )
            .first()
        )
        if token:
            token_symbol = token.symbol or token.name

    payload = {
        "token": {
            "address": address,
            "network": network,
            "network_name": network_label(network),
            "symbol": token_symbol,
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
    return event, payload

# ============================================================
# OPTIONAL OUTPUTS
# ============================================================


def format_event_alert(payload):
    token = payload["token"]
    snap = payload["snapshot"]
    activity = payload["activity"]
    symbol = token.get("symbol") or "Token"
    lines = [
        "🚨 <b>MARSOF AI — ACTIVITY DETECTED</b>",
        "",
        f"<b>{symbol}</b> · <b>{token['network_name']}</b>",
        f"<code>{token['address']}</code>",
        "",
        f"⚡ Activity Score: <b>{activity['activity_score']:.1f}/100</b>",
        f"📌 Severity: <b>{activity['severity']}</b>",
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
    """AI adapter placeholder.

    Detection and evidence collection are production-ready in this phase. The
    AI publisher remains opt-in so a missing/incorrect AI credential can never
    block monitoring or create an accidental publishing loop.
    """
    if not AI_ENABLED:
        return None
    if not AI_API_KEY:
        print("⚠️ AI enabled but OPENAI_API_KEY is not configured; skipping AI report")
        return None

    # Deliberately leave the provider call isolated here. The monitor stores
    # the complete evidence bundle, so the AI transport can be added without
    # touching detection, snapshots, or the stable MARSOF core.
    ai_input = build_ai_input(payload)
    print("ℹ️ AI stage is enabled but no provider transport is configured yet.")
    print(json.dumps(ai_input, ensure_ascii=False, default=str)[:6000])
    return None


def publish_x(report, payload):
    """X publisher boundary; intentionally no-op until credentials are configured."""
    if not X_ENABLED:
        return False
    print("ℹ️ X publishing is enabled in config, but the publisher is not connected yet.")
    return False

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
            return {"status": "NO_RESULT"}

        current, previous = latest_two_snapshots(session, address, network)
        holder_current, holder_previous = latest_holder_snapshots(session, address, network)

        if current is None or previous is None:
            state.last_status = "BASELINE"
            state.last_score = None
            session.commit()
            print("ℹ️ Baseline created; waiting for another snapshot before activity detection.")
            return {"status": "BASELINE"}

        activity = detect_activity(current, previous, holder_current, holder_previous)
        state.last_score = activity["activity_score"]
        state.last_status = activity["severity"]

        if activity["activity_score"] < MONITOR_MIN_ACTIVITY_SCORE:
            session.commit()
            return {"status": "NORMAL", "activity": activity}

        if cooldown_active(state):
            session.commit()
            print(
                f"ℹ️ Activity detected but cooldown is active for {network}:{address} "
                f"({MONITOR_COOLDOWN_MINUTES}m)."
            )
            return {"status": "COOLDOWN", "activity": activity}

        event, payload = store_event(session, item, activity, current)
        state.last_event_at = now_utc()
        session.commit()

        alert = format_event_alert(payload)
        print(alert.replace("<b>", "").replace("</b>", "").replace("<code>", "").replace("</code>", ""))
        send_telegram(alert)

        report = generate_ai_report(payload)
        if report:
            event.ai_report = report
            event.ai_processed = True
            session.commit()
            event.published_x = publish_x(report, payload)
            session.commit()

        return {"status": "EVENT", "activity": activity, "payload": payload}

    except Exception as exc:
        session.rollback()
        print(f"❌ Monitor error {network}:{address}: {exc}")
        return {"status": "ERROR", "error": str(exc)}
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
    return results


def run_interactive_test():
    """Run one immediate monitoring pass and return a compact per-token report."""
    started = now_utc()
    results = []
    for item in WATCHLIST:
        result = monitor_token(item)
        results.append({
            "name": item["name"],
            "network": item["network"],
            "address": item["address"],
            "status": result.get("status", "UNKNOWN") if isinstance(result, dict) else "UNKNOWN",
            "activity_score": (result.get("activity") or {}).get("activity_score") if isinstance(result, dict) else None,
            "error": result.get("error") if isinstance(result, dict) else None,
        })
    return {"started_at": started, "finished_at": now_utc(), "results": results}


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
        extra = f" — {row['error']}" if status == "ERROR" and row.get("error") else ""
        lines.append(f"{icon} <b>{row['name']}</b> · {row['network']} · {status}{score_text}{extra}")

    lines.extend([
        "",
        "<i>Test uses the same monitor_token() path as the live monitor; no scoring/verdict logic was changed.</i>",
    ])
    return "\n".join(lines)


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
            telegram_bot.reply_to(
                message,
                "🧪 <b>Monitor test started.</b>\n\nChecking all configured assets now…",
                parse_mode="HTML",
            )
        except Exception:
            pass

        def worker():
            try:
                report = run_interactive_test()
                text = format_interactive_test(report)
            except Exception as exc:
                text = f"❌ <b>Monitor test failed</b>\n\n<code>{str(exc)[:1000]}</code>"
            try:
                telegram_bot.send_message(chat_id, text, parse_mode="HTML")
            except Exception as exc:
                print(f"⚠️ monitor test Telegram response failed: {exc}")

        threading.Thread(target=worker, name="marsof-monitor-test", daemon=True).start()
        return True

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


def start_monitor_background():
    """Start one daemon monitor thread for embedded Render Web Service mode."""
    register_telegram_test_command()
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
