import os
import json
import math
import requests
from datetime import datetime, timezone, timedelta

from fastapi import FastAPI, Request
import telebot
from telebot import types

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, Float, DateTime,
    Text, UniqueConstraint, Index, inspect, text,
)
from sqlalchemy.orm import declarative_base, sessionmaker

# ============================================================
# MARSOF AI — TOKEN ANALYZER V4
# BNB Smart Chain analysis service.
# Missing source values ALWAYS remain None / unavailable.
# MARSOF snapshots are used for scan-to-scan comparison, not full price history.
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = "https://crypto-analyse-bot-z7o0.onrender.com/webhook"
CHAIN_ID = "56"
GO_PLUS_URL = f"https://api.gopluslabs.io/api/v1/token_security/{CHAIN_ID}"
DEXSCREENER_URL = "https://api.dexscreener.com/latest/dex/tokens/{address}"

bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

# ============================================================
# DATABASE
# ============================================================

Base = declarative_base()
engine = None
SessionLocal = None

if DATABASE_URL:
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Token(Base):
    __tablename__ = "tokens"
    id = Column(Integer, primary_key=True)
    contract_address = Column(String(100), nullable=False)
    chain_id = Column(String(20), nullable=False)
    symbol = Column(String(80))
    name = Column(String(200))
    first_seen = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    last_scanned = Column(DateTime)
    scan_count = Column(Integer, default=0)
    __table_args__ = (UniqueConstraint("contract_address", "chain_id", name="uq_token_chain"),)


class SecurityScan(Base):
    __tablename__ = "security_scans"
    id = Column(Integer, primary_key=True)
    token_id = Column(Integer, nullable=False)
    verdict = Column(String(30))
    coverage = Column(Float)
    pass_count = Column(Integer)
    caution_count = Column(Integer)
    risk_count = Column(Integer)
    raw_data = Column(Text)
    scanned_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class MarketSnapshot(Base):
    __tablename__ = "market_snapshots"
    id = Column(Integer, primary_key=True)
    chain_id = Column(String(20), nullable=False)
    token_address = Column(String(100), nullable=False)
    pair_address = Column(String(100))
    dex_id = Column(String(80))
    price_usd = Column(Float)
    market_cap_usd = Column(Float)
    fdv_usd = Column(Float)
    liquidity_usd = Column(Float)
    volume_5m_usd = Column(Float)
    volume_1h_usd = Column(Float)
    volume_6h_usd = Column(Float)
    volume_24h_usd = Column(Float)
    buys_5m = Column(Integer)
    sells_5m = Column(Integer)
    buys_1h = Column(Integer)
    sells_1h = Column(Integer)
    buys_6h = Column(Integer)
    sells_6h = Column(Integer)
    buys_24h = Column(Integer)
    sells_24h = Column(Integer)
    price_change_5m = Column(Float)
    price_change_1h = Column(Float)
    price_change_6h = Column(Float)
    price_change_24h = Column(Float)
    pair_created_at = Column(DateTime)
    snapshot_time = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    __table_args__ = (Index("ix_market_token_time", "token_address", "snapshot_time"),)


class HolderSnapshot(Base):
    __tablename__ = "holder_snapshots"
    id = Column(Integer, primary_key=True)
    chain_id = Column(String(20), nullable=False)
    token_address = Column(String(100), nullable=False)
    wallet = Column(String(100), nullable=False)
    balance = Column(Float)
    percentage = Column(Float)
    tag = Column(String(200))
    is_contract = Column(Boolean)
    is_locked = Column(Boolean)
    snapshot_time = Column(DateTime, default=lambda: datetime.now(timezone.utc))


# ============================================================
# HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def normalize_address(value):
    return str(value).strip().lower() if value is not None else ""


def number(value):
    if value is None or isinstance(value, bool):
        return None
    try:
        if isinstance(value, str) and not value.strip():
            return None
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def integer(value):
    x = number(value)
    return int(x) if x is not None else None


def boolean(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    s = str(value).strip().lower()
    if s in {"1", "true", "yes", "y"}:
        return True
    if s in {"0", "false", "no", "n"}:
        return False
    if s in {"-1", "unknown", "null"}:
        return None
    return None


def fmt_num(value):
    if value is None:
        return "غير متوفر"
    v = float(value)
    av = abs(v)
    if av >= 1_000_000_000:
        return f"{v / 1_000_000_000:.2f}B"
    if av >= 1_000_000:
        return f"{v / 1_000_000:.2f}M"
    if av >= 1_000:
        return f"{v / 1_000:.2f}K"
    if av >= 1:
        return f"{v:.4f}".rstrip("0").rstrip(".")
    if av >= 0.01:
        return f"{v:.6f}".rstrip("0").rstrip(".")
    if av == 0:
        return "0"
    return f"{v:.10f}".rstrip("0").rstrip(".")


def fmt_money(value):
    return "غير متوفر" if value is None else f"${fmt_num(value)}"


def fmt_pct(value):
    return "غير متوفر" if value is None else f"{value:.2f}%"


def fmt_ratio(value):
    return "غير متوفر" if value is None else f"{value:.2f}x"


def fmt_bool(value):
    return "نعم" if value is True else "لا" if value is False else "غير متوفر"


def parse_timestamp(value):
    x = number(value)
    if x is None:
        return None
    # DEX Screener returns pairCreatedAt in milliseconds; other providers may use seconds.
    if abs(x) >= 100_000_000_000:
        x /= 1000.0
    try:
        return datetime.fromtimestamp(x, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def age_days(timestamp):
    if timestamp is None:
        return None
    return max(0.0, (utc_now() - timestamp).total_seconds() / 86400)


def is_valid_bsc_address(address):
    if not address:
        return False
    a = address.strip()
    if len(a) != 42 or not a.startswith("0x"):
        return False
    try:
        int(a[2:], 16)
        return True
    except ValueError:
        return False


# ============================================================
# SOURCE FETCHERS
# ============================================================

def fetch_goplus(address):
    response = requests.get(
        GO_PLUS_URL,
        params={"contract_addresses": address},
        timeout=25,
    )
    response.raise_for_status()
    payload = response.json()
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, dict):
        return None

    target = normalize_address(address)
    # IMPORTANT: exact contract match only. Never use the first result.
    for returned_address, data in result.items():
        if normalize_address(returned_address) == target and isinstance(data, dict):
            return dict(data)
    return None


def fetch_dexscreener(address):
    target = normalize_address(address)
    response = requests.get(
        DEXSCREENER_URL.format(address=target),
        timeout=25,
    )
    response.raise_for_status()
    payload = response.json()
    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if not isinstance(pairs, list):
        print(f"⚠️ DEX Screener: invalid pairs response for {target}")
        return []

    exact_bsc_pairs = []
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        if str(pair.get("chainId") or "").lower() != "bsc":
            continue
        base = normalize_address((pair.get("baseToken") or {}).get("address"))
        quote = normalize_address((pair.get("quoteToken") or {}).get("address"))
        if target == base or target == quote:
            exact_bsc_pairs.append(pair)

    exact_bsc_pairs.sort(
        key=lambda p: number((p.get("liquidity") or {}).get("usd")) or -1,
        reverse=True,
    )
    if exact_bsc_pairs:
        selected = exact_bsc_pairs[0]
        print(
            "✅ DEX Screener: "
            f"{len(exact_bsc_pairs)} exact BSC pair(s) for {target}; "
            f"selected {selected.get('pairAddress')} "
            f"liquidity={number((selected.get('liquidity') or {}).get('usd'))}"
        )
    else:
        print(f"⚠️ DEX Screener: 0 exact BSC pairs for {target}; no market values will be fabricated")
    return exact_bsc_pairs


# ============================================================
# METRIC MODEL
# status: PASS / CAUTION / RISK / DATA / UNKNOWN
# DATA = real available information, not a risk judgment.
# ============================================================

def metric(label, value, status="DATA", detail=None, source="GoPlus"):
    return {
        "label": label,
        "value": value,
        "status": status,
        "detail": detail if detail is not None else str(value) if value is not None else "غير متوفر",
        "source": source,
        "available": value is not None,
    }


def risk_bool(value, risky_when_true=True):
    if value is None:
        return "UNKNOWN"
    if risky_when_true:
        return "RISK" if value else "PASS"
    return "PASS" if value else "RISK"


def tax_status(pct):
    if pct is None:
        return "UNKNOWN"
    if pct <= 5:
        return "PASS"
    if pct <= 10:
        return "CAUTION"
    return "RISK"


def build_security(data):
    checks = []
    add = lambda label, value, status="DATA", detail=None: checks.append(
        metric(label, value, status, detail, "GoPlus")
    )

    # Identity / basic information
    token_name = data.get("token_name")
    token_symbol = data.get("token_symbol")
    contract_name = data.get("contract_name")
    holder_count = integer(data.get("holder_count"))
    total_supply = number(data.get("total_supply"))
    decimals = integer(data.get("decimals"))
    add("Token Name", token_name, "DATA" if token_name else "UNKNOWN", token_name or "غير متوفر")
    add("Token Symbol", token_symbol, "DATA" if token_symbol else "UNKNOWN", token_symbol or "غير متوفر")
    add("Contract Name", contract_name, "DATA" if contract_name else "UNKNOWN", contract_name or "غير متوفر")
    add("Holder Count", holder_count, "DATA" if holder_count is not None else "UNKNOWN", str(holder_count) if holder_count is not None else "غير متوفر")
    add("Total Supply", total_supply, "DATA" if total_supply is not None else "UNKNOWN", fmt_num(total_supply))
    add("Decimals", decimals, "DATA" if decimals is not None else "UNKNOWN", str(decimals) if decimals is not None else "غير متوفر")

    # Contract security
    is_honeypot = boolean(data.get("is_honeypot"))
    add("Honeypot", is_honeypot, risk_bool(is_honeypot), fmt_bool(is_honeypot))

    open_source = boolean(data.get("is_open_source"))
    add("Source Code Open", open_source, "PASS" if open_source is True else "RISK" if open_source is False else "UNKNOWN", fmt_bool(open_source))

    for key, label in [
        ("buy_tax", "Buy Tax"),
        ("sell_tax", "Sell Tax"),
        ("transfer_tax", "Transfer Tax"),
    ]:
        raw = number(data.get(key))
        pct = raw * 100 if raw is not None else None
        add(label, pct, tax_status(pct), fmt_pct(pct))

    for key, label in [
        ("is_mintable", "Mintable"),
        ("is_proxy", "Proxy"),
        ("hidden_owner", "Hidden Owner"),
        ("selfdestruct", "Self Destruct"),
        ("privilege_withdraw", "Privilege Withdraw"),
        ("approval_abuse", "Approval Abuse"),
        ("withdraw_missing", "Withdraw Method Missing"),
        ("contract_upgradeable", "Contract Upgradeable"),
        ("blacklist", "Blacklist Function"),
        ("whitelist", "Whitelist Function"),
        ("trading_cooldown", "Trading Cooldown"),
        ("cannot_buy", "Cannot Buy"),
        ("cannot_sell_all", "Cannot Sell All"),
        ("slippage_modifiable", "Tax Modifiable"),
        ("personal_slippage_modifiable", "Personal Slippage Modifiable"),
        ("metadata_modifiable", "Metadata Modifiable"),
        ("external_call", "External Call"),
        ("gas_abuse", "Gas Abuse"),
        ("owner_change_balance", "Owner Can Change Balance"),
        ("transfer_pausable", "Transfer Pausable"),
    ]:
        v = boolean(data.get(key))
        add(label, v, risk_bool(v), fmt_bool(v))

    # Anti-whale itself is not automatically a risk. A modifiable anti-whale rule is
    # the item that needs attention.
    anti_whale = boolean(data.get("is_anti_whale"))
    anti_whale_mod = boolean(data.get("anti_whale_modifiable"))
    add("Anti-whale Function", anti_whale, "DATA" if anti_whale is not None else "UNKNOWN", fmt_bool(anti_whale))
    add("Modifiable Anti-whale", anti_whale_mod, risk_bool(anti_whale_mod), fmt_bool(anti_whale_mod))

    # Trading presence
    in_dex = boolean(data.get("is_in_dex"))
    add("Listed / In DEX", in_dex, "DATA" if in_dex is not None else "UNKNOWN", fmt_bool(in_dex))

    # Owner / creator
    owner = data.get("owner") if isinstance(data.get("owner"), dict) else {}
    owner_address = owner.get("owner_address")
    owner_type = owner.get("owner_type")
    add("Owner Address", owner_address, "DATA" if owner_address else "UNKNOWN", owner_address or "غير متوفر")
    add("Owner Type", owner_type, "DATA" if owner_type else "UNKNOWN", owner_type or "غير متوفر")
    creator = data.get("creator_address")
    add("Creator / Deployer", creator, "DATA" if creator else "UNKNOWN", creator or "غير متوفر")
    creator_balance = number(data.get("creator_balance"))
    creator_percent = number(data.get("creator_percent"))
    add("Creator Balance", creator_balance, "DATA" if creator_balance is not None else "UNKNOWN", fmt_num(creator_balance))
    creator_pct_value = creator_percent * 100 if creator_percent is not None else None
    add("Creator Token %", creator_pct_value, "DATA" if creator_pct_value is not None else "UNKNOWN", fmt_pct(creator_pct_value))

    deployed = parse_timestamp(data.get("deployed_time"))
    add("Contract Deployed", deployed.isoformat() if deployed else None, "DATA" if deployed else "UNKNOWN", deployed.strftime("%Y-%m-%d %H:%M UTC") if deployed else "غير متوفر")
    deployed_age = age_days(deployed)
    add("Contract Age (days)", deployed_age, "DATA", f"{deployed_age:.2f} days" if deployed_age is not None else "غير متوفر")

    # Top holders and concentration. Keep the exact source list; don't fabricate top-20/50
    # from only top-10 data.
    holders = data.get("holders")
    if isinstance(holders, list) and holders:
        top10 = sum(number(h.get("percent")) or 0 for h in holders[:10]) * (100 if sum(number(h.get("percent")) or 0 for h in holders[:10]) <= 1.0 else 1)
        # GoPlus documents percent as a percentage. If a provider ever returns a fraction,
        # the normalization above handles it without changing an already-percent value.
        add("Top 10 Concentration", top10, "DATA", fmt_pct(top10))
        for i, h in enumerate(holders[:10], 1):
            pct = number(h.get("percent"))
            if pct is not None and pct <= 1:
                pct *= 100
            label = f"Top Holder #{i}"
            detail = f"{h.get('address') or 'غير متوفر'} — {fmt_pct(pct)} — {h.get('tag') or 'No tag'}"
            add(label, pct, "DATA", detail)
    else:
        add("Top 10 Concentration", None, "UNKNOWN", "غير متوفر")

    # LP information
    lp_count = integer(data.get("lp_holder_count"))
    lp_supply = number(data.get("lp_total_supply"))
    add("LP Holder Count", lp_count, "DATA", str(lp_count) if lp_count is not None else "غير متوفر")
    add("LP Token Total Supply", lp_supply, "DATA", fmt_num(lp_supply))

    lp_holders = data.get("lp_holders")
    if isinstance(lp_holders, list) and lp_holders:
        for i, h in enumerate(lp_holders[:10], 1):
            pct = number(h.get("percent"))
            if pct is not None and pct <= 1:
                pct *= 100
            detail = f"{h.get('address') or 'غير متوفر'} — {fmt_pct(pct)} — {h.get('tag') or 'No tag'}"
            add(f"LP Holder #{i}", pct, "DATA", detail)

    # DEX data supplied by GoPlus (may be absent even when DexScreener has a pair).
    dexes = data.get("dex")
    if isinstance(dexes, list):
        add("GoPlus DEX Pools", len(dexes), "DATA", str(len(dexes)))
        for i, d in enumerate(dexes[:10], 1):
            if isinstance(d, dict):
                pool_liq = number(d.get("liquidity"))
                detail = f"{d.get('name') or 'Unknown'} | {d.get('pair') or 'غير متوفر'} | ${fmt_num(pool_liq)}"
                add(f"GoPlus Pool #{i}", pool_liq, "DATA", detail)

    return checks


# ============================================================
# MARKET / DEXSCREENER
# ============================================================

def choose_market(pairs):
    return pairs[0] if pairs else None


def txns_for(pair, period):
    obj = (pair.get("txns") or {}).get(period) or {}
    return integer(obj.get("buys")), integer(obj.get("sells"))


def change_for(pair, period):
    return number((pair.get("priceChange") or {}).get(period))


def volume_for(pair, period):
    return number((pair.get("volume") or {}).get(period))


def build_market(pair):
    if not pair:
        return {"checks": [], "data": {}, "missing": ["DEX market data"]}

    checks = []
    add = lambda label, value, status="DATA", detail=None: checks.append(
        metric(label, value, status, detail, "DEX Screener")
    )

    price = number(pair.get("priceUsd"))
    mc = number(pair.get("marketCap"))
    fdv = number(pair.get("fdv"))
    liq = number((pair.get("liquidity") or {}).get("usd"))
    base_liq = number((pair.get("liquidity") or {}).get("base"))
    quote_liq = number((pair.get("liquidity") or {}).get("quote"))
    price_native = number(pair.get("priceNative"))

    add("Price USD", price, "DATA", f"${fmt_num(price)}")
    add("Price Native", price_native, "DATA", fmt_num(price_native))
    add("Market Cap", mc, "DATA", fmt_money(mc))
    add("FDV", fdv, "DATA", fmt_money(fdv))
    add("Liquidity USD", liq, "DATA", fmt_money(liq))
    add("Liquidity Base", base_liq, "DATA", fmt_num(base_liq))
    add("Liquidity Quote", quote_liq, "DATA", fmt_num(quote_liq))
    add("DEX", pair.get("dexId"), "DATA", pair.get("dexId") or "غير متوفر")
    add("Pair Address", pair.get("pairAddress"), "DATA", pair.get("pairAddress") or "غير متوفر")
    add("Base Token", (pair.get("baseToken") or {}).get("address"), "DATA", (pair.get("baseToken") or {}).get("symbol") or "غير متوفر")
    add("Quote Token", (pair.get("quoteToken") or {}).get("address"), "DATA", (pair.get("quoteToken") or {}).get("symbol") or "غير متوفر")

    pair_created = parse_timestamp(number(pair.get("pairCreatedAt")))
    add("Pair Created", pair_created.isoformat() if pair_created else None, "DATA" if pair_created else "UNKNOWN", pair_created.strftime("%Y-%m-%d %H:%M UTC") if pair_created else "غير متوفر")
    pair_age = age_days(pair_created)
    add("Pair Age (days)", pair_age, "DATA", f"{pair_age:.2f} days" if pair_age is not None else "غير متوفر")

    period_data = {}
    for period in ["m5", "h1", "h6", "h24"]:
        buys, sells = txns_for(pair, period)
        volume = volume_for(pair, period)
        change = change_for(pair, period)
        total_tx = (buys + sells) if buys is not None and sells is not None else None
        ratio = (buys / sells) if buys is not None and sells not in (None, 0) else None
        period_data[period] = {"buys": buys, "sells": sells, "volume": volume, "change": change, "tx": total_tx, "ratio": ratio}
        title = {"m5": "5m", "h1": "1h", "h6": "6h", "h24": "24h"}[period]
        add(f"{title} Volume", volume, "DATA", fmt_money(volume))
        add(f"{title} Buys", buys, "DATA", str(buys) if buys is not None else "غير متوفر")
        add(f"{title} Sells", sells, "DATA", str(sells) if sells is not None else "غير متوفر")
        add(f"{title} Buy/Sell Ratio", ratio, "DATA", fmt_ratio(ratio))
        add(f"{title} Transactions", total_tx, "DATA", str(total_tx) if total_tx is not None else "غير متوفر")
        add(f"{title} Price Change", change, "DATA", fmt_pct(change))

    liq_mc = (liq / mc * 100) if liq is not None and mc not in (None, 0) else None
    liq_fdv = (liq / fdv * 100) if liq is not None and fdv not in (None, 0) else None
    vol_liq = (period_data["h24"]["volume"] / liq) if period_data["h24"]["volume"] is not None and liq not in (None, 0) else None
    vol_mc = (period_data["h24"]["volume"] / mc * 100) if period_data["h24"]["volume"] is not None and mc not in (None, 0) else None
    fdv_mc = (fdv / mc) if fdv is not None and mc not in (None, 0) else None

    add("Liquidity / Market Cap", liq_mc, "CAUTION" if liq_mc is not None and liq_mc < 3 else "DATA" if liq_mc is not None else "UNKNOWN", fmt_pct(liq_mc))
    add("Liquidity / FDV", liq_fdv, "DATA", fmt_pct(liq_fdv))
    add("Volume / Liquidity", vol_liq, "CAUTION" if vol_liq is not None and vol_liq > 20 else "DATA" if vol_liq is not None else "UNKNOWN", fmt_ratio(vol_liq))
    add("24h Volume / Market Cap", vol_mc, "DATA", fmt_pct(vol_mc))
    add("FDV / Market Cap", fdv_mc, "DATA", fmt_ratio(fdv_mc))

    # No artificial 7d value. It is filled only from historical snapshots.
    add("7d Volume", None, "UNKNOWN", "لا توجد قيمة فورية من هذا المصدر")
    add("7d Price Change", None, "UNKNOWN", "يحتاج snapshots تاريخية")
    add("24h Liquidity Change", None, "UNKNOWN", "يحتاج snapshots تاريخية")
    add("7d Liquidity Change", None, "UNKNOWN", "يحتاج snapshots تاريخية")

    return {
        "checks": checks,
        "data": {
            "pair": pair,
            "price": price,
            "market_cap": mc,
            "fdv": fdv,
            "liquidity": liq,
            "price_native": price_native,
            "volume_24h": period_data["h24"]["volume"],
            "buys_24h": period_data["h24"]["buys"],
            "sells_24h": period_data["h24"]["sells"],
            "price_change_24h": period_data["h24"]["change"],
            "liq_mc": liq_mc,
            "liq_fdv": liq_fdv,
            "vol_liq": vol_liq,
            "vol_mc": vol_mc,
            "fdv_mc": fdv_mc,
            "periods": period_data,
        },
        "missing": [],
    }


# ============================================================
# HISTORICAL RATIOS FROM OUR OWN SNAPSHOTS
# ============================================================

def _historical_rows(session, address):
    return (
        session.query(MarketSnapshot)
        .filter(
            MarketSnapshot.chain_id == CHAIN_ID,
            MarketSnapshot.token_address == normalize_address(address),
        )
        .order_by(MarketSnapshot.snapshot_time.asc())
        .all()
    )


def _nearest_before(rows, target):
    candidates = [r for r in rows if r.snapshot_time and r.snapshot_time <= target]
    return candidates[-1] if candidates else None


def _pct_change(current, previous):
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / previous * 100.0


def _trend_label(change_values):
    vals = [v for v in change_values if v is not None]
    if not vals:
        return None
    # Keep the interpretation descriptive and evidence-based.
    if all(v < -2 for v in vals):
        return "persistent downtrend"
    if all(v > 2 for v in vals):
        return "persistent uptrend"
    if any(v < -2 for v in vals) and any(v > 2 for v in vals):
        return "mixed / volatile"
    return "mostly sideways"


# ============================================================
# CLASSIFICATION
# ============================================================

def classify_all(security_checks, market_checks):
    all_checks = security_checks + market_checks
    risk_tests = [x for x in all_checks if x["status"] in {"PASS", "CAUTION", "RISK"}]
    data_only = [x for x in all_checks if x["status"] == "DATA"]
    missing = [x["label"] for x in all_checks if x["status"] == "UNKNOWN"]
    available = [x for x in all_checks if x["status"] != "UNKNOWN"]

    p = sum(x["status"] == "PASS" for x in risk_tests)
    c = sum(x["status"] == "CAUTION" for x in risk_tests)
    r = sum(x["status"] == "RISK" for x in risk_tests)
    coverage = (len(available) / len(all_checks) * 100) if all_checks else 0.0

    if r > 0:
        verdict = "RISK"
    elif c > 0:
        verdict = "CAUTION"
    elif risk_tests:
        verdict = "PASS"
    else:
        verdict = "INCOMPLETE"

    confidence = coverage
    security_total = len(security_checks)
    security_available = sum(x["status"] != "UNKNOWN" for x in security_checks)
    security_coverage = (security_available / security_total * 100) if security_total else 0.0

    return {
        "all": all_checks,
        "security_checks": security_checks,
        "market_checks": market_checks,
        "pass": p,
        "caution": c,
        "risk": r,
        "data": len(data_only),
        "missing": missing,
        "coverage": coverage,
        "security_coverage": security_coverage,
        "analysis_confidence": confidence,
        "verdict": verdict,
    }


# ============================================================
# DATABASE SAVE
# ============================================================

def save_analysis(address, data, classification, market):
    if not SessionLocal:
        return
    session = SessionLocal()
    try:
        addr = normalize_address(address)
        token = session.query(Token).filter(
            Token.contract_address == addr,
            Token.chain_id == CHAIN_ID,
        ).first()

        if not token:
            token = Token(
                contract_address=addr,
                chain_id=CHAIN_ID,
                symbol=data.get("token_symbol"),
                name=data.get("token_name"),
                first_seen=utc_now(),
            )
            session.add(token)
            session.flush()

        token.symbol = data.get("token_symbol") or token.symbol
        token.name = data.get("token_name") or token.name
        token.last_scanned = utc_now()
        token.scan_count = (token.scan_count or 0) + 1

        session.add(SecurityScan(
            token_id=token.id,
            verdict=classification["verdict"],
            coverage=classification["coverage"],
            pass_count=classification["pass"],
            caution_count=classification["caution"],
            risk_count=classification["risk"],
            raw_data=json.dumps({"security": data, "market": market}, ensure_ascii=False, default=str),
            scanned_at=utc_now(),
        ))

        scan_time = utc_now()

        if market.get("pair"):
            p = market["pair"]
            periods = market.get("periods", {})
            pair_created = parse_timestamp(p.get("pairCreatedAt"))

            session.add(MarketSnapshot(
                chain_id=CHAIN_ID,
                token_address=addr,
                pair_address=p.get("pairAddress"),
                dex_id=p.get("dexId"),
                price_usd=market.get("price"),
                market_cap_usd=market.get("market_cap"),
                fdv_usd=market.get("fdv"),
                liquidity_usd=market.get("liquidity"),
                volume_5m_usd=(periods.get("m5") or {}).get("volume"),
                volume_1h_usd=(periods.get("h1") or {}).get("volume"),
                volume_6h_usd=(periods.get("h6") or {}).get("volume"),
                volume_24h_usd=(periods.get("h24") or {}).get("volume"),
                buys_5m=(periods.get("m5") or {}).get("buys"),
                sells_5m=(periods.get("m5") or {}).get("sells"),
                buys_1h=(periods.get("h1") or {}).get("buys"),
                sells_1h=(periods.get("h1") or {}).get("sells"),
                buys_6h=(periods.get("h6") or {}).get("buys"),
                sells_6h=(periods.get("h6") or {}).get("sells"),
                buys_24h=(periods.get("h24") or {}).get("buys"),
                sells_24h=(periods.get("h24") or {}).get("sells"),
                price_change_5m=(periods.get("m5") or {}).get("change"),
                price_change_1h=(periods.get("h1") or {}).get("change"),
                price_change_6h=(periods.get("h6") or {}).get("change"),
                price_change_24h=(periods.get("h24") or {}).get("change"),
                pair_created_at=pair_created,
                snapshot_time=scan_time,
            ))

        holders = data.get("holders")
        if isinstance(holders, list):
            for h in holders[:10]:
                wallet = str(h.get("address") or "").strip()
                if not wallet:
                    continue
                pct = number(h.get("percent"))
                if pct is not None and pct <= 1:
                    pct *= 100
                session.add(HolderSnapshot(
                    chain_id=CHAIN_ID,
                    token_address=addr,
                    wallet=wallet,
                    balance=number(h.get("balance")),
                    percentage=pct,
                    tag=str(h.get("tag")) if h.get("tag") is not None else None,
                    is_contract=boolean(h.get("is_contract")),
                    is_locked=boolean(h.get("is_locked")),
                    snapshot_time=scan_time,
                ))

        session.commit()
    except Exception as exc:
        session.rollback()
        print(f"⚠️ DB save error: {exc}")
    finally:
        session.close()


# ============================================================
# UI — MARSOF AI V4
# ============================================================

PROJECT_WEBSITE = "https://marsof.ct.ws/?i=1"
PROJECT_X = "https://x.com/MARSOF_AI"
PROJECT_TELEGRAM = "https://t.me/marsofai"
LOGO_PATH = os.path.join(os.path.dirname(__file__), "marsof_logo.jpg")


def split_text(text_value, max_chars=3500):
    if not text_value:
        return [""]
    lines = text_value.split("\n")
    chunks, current = [], ""
    for line in lines:
        candidate = line if not current else current + "\n" + line
        if len(candidate) <= max_chars:
            current = candidate
        else:
            if current:
                chunks.append(current)
            while len(line) > max_chars:
                chunks.append(line[:max_chars])
                line = line[max_chars:]
            current = line
    if current or not chunks:
        chunks.append(current)
    return chunks


def verdict_icon(status):
    return {
        "PASS": "🟢", "CAUTION": "🟠", "RISK": "🔴",
        "DATA": "🔵", "UNKNOWN": "⚪", "INCOMPLETE": "⚪",
    }.get(status, "🔵")


def short_address(address):
    if not address:
        return "Not available"
    return f"{address[:6]}…{address[-4:]}"


def fmt_signed_pct(value):
    if value is None:
        return "Not available"
    return f"{value:+.2f}%"


def elapsed_text(timestamp):
    if not timestamp:
        return "Not available"
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    seconds = max(0, int((utc_now() - timestamp).total_seconds()))
    if seconds < 60:
        return "just now"
    if seconds < 3600:
        n = seconds // 60
        return f"{n} minute{'s' if n != 1 else ''} ago"
    if seconds < 86400:
        n = seconds // 3600
        return f"{n} hour{'s' if n != 1 else ''} ago"
    n = seconds // 86400
    return f"{n} day{'s' if n != 1 else ''} ago"


def _esc(value):
    import html
    return html.escape(str(value) if value is not None else "Not available")


def _movement_alerts(comparison):
    alerts = []
    if not comparison.get("available"):
        return alerts
    price = comparison.get("price_change_pct")
    liq = comparison.get("liquidity_change_pct")
    if price is not None and price <= -50:
        alerts.append(("RISK", f"SIGNIFICANT PRICE MOVEMENT — price dropped {abs(price):.2f}% since the previous scan."))
    elif price is not None and price >= 50:
        alerts.append(("DATA", f"SIGNIFICANT PRICE MOVEMENT — price increased {price:.2f}% since the previous scan."))
    if liq is not None and liq <= -50:
        alerts.append(("RISK", f"LIQUIDITY MOVEMENT — liquidity fell {abs(liq):.2f}% since the previous scan."))
    elif liq is not None and liq <= -30:
        alerts.append(("CAUTION", f"LIQUIDITY MOVEMENT — liquidity fell {abs(liq):.2f}% since the previous scan."))
    return alerts


def _comparison_lines(result):
    c = result.get("comparison", {})
    if not c.get("available"):
        return [
            "📊 <b>SINCE LAST SCAN</b>",
            "",
            "No previous MARSOF scan is available for comparison.",
            "MARSOF stores snapshots for scan-to-scan comparison only.",
        ]
    lines = [
        "📊 <b>SINCE LAST SCAN</b>",
        f"Last scanned: <b>{_esc(elapsed_text(c.get('previous_time')))}</b>",
        "",
        f"💵 Price: <b>{fmt_money(c.get('previous_price'))} → {fmt_money(c.get('current_price'))}</b> {fmt_signed_pct(c.get('price_change_pct'))}",
        f"💧 Liquidity: <b>{fmt_money(c.get('previous_liquidity'))} → {fmt_money(c.get('current_liquidity'))}</b> {fmt_signed_pct(c.get('liquidity_change_pct'))}",
        f"💰 Market Cap: <b>{fmt_money(c.get('previous_market_cap'))} → {fmt_money(c.get('current_market_cap'))}</b> {fmt_signed_pct(c.get('market_cap_change_pct'))}",
        f"📈 24h Volume: <b>{fmt_money(c.get('previous_volume_24h'))} → {fmt_money(c.get('current_volume_24h'))}</b> {fmt_signed_pct(c.get('volume_24h_change_pct'))}",
    ]
    alerts = _movement_alerts(c)
    if alerts:
        lines += ["", "🚨 <b>MARKET MOVEMENT ALERTS</b>"]
        for status, message in alerts:
            lines.append(f"{verdict_icon(status)} {_esc(message)}")
    return lines


def build_home(result):
    d = result["security"]
    c = result["classification"]
    m = result["market"]["data"]
    symbol = d.get("token_symbol") or "Unknown token"
    name = d.get("token_name") or ""
    address = result["address"]
    verdict = c["verdict"]
    verdict_text = {
        "PASS": "No risk indicators detected in completed checks.",
        "CAUTION": "Attention indicators were detected in completed checks.",
        "RISK": "Risk indicators were detected in completed checks.",
        "INCOMPLETE": "The analysis is incomplete because important data is unavailable.",
    }.get(verdict, "The analysis is incomplete.")
    security_findings = [x for x in c.get("security_checks", []) if x["status"] in {"RISK", "CAUTION"}]
    market_findings = [x for x in c.get("market_checks", []) if x["status"] in {"RISK", "CAUTION"}]
    lines = [
        "╭────────────────────────────╮",
        "│          MARSOF AI         │",
        "│   ADVANCED TOKEN ANALYSIS  │",
        "╰────────────────────────────╯",
        "",
        f"🪙 <b>{_esc(symbol)}</b>{' — ' + _esc(name) if name else ''}",
        f"Chain: <b>BSC</b>",
        f"Contract: <code>{_esc(short_address(address))}</code>",
        "",
        f"{verdict_icon(verdict)} <b>{verdict}</b>",
        _esc(verdict_text),
        "",
        f"💵 Price: <b>{_esc(fmt_money(m.get('price')))}</b>",
        f"💧 Liquidity: <b>{_esc(fmt_money(m.get('liquidity')))}</b>",
        f"💰 Market Cap: <b>{_esc(fmt_money(m.get('market_cap')))}</b>",
        f"📈 24h Volume: <b>{_esc(fmt_money(m.get('volume_24h')))}</b>",
        f"👥 Holders: <b>{_esc(str(integer(d.get('holder_count')) if integer(d.get('holder_count')) is not None else 'Not available'))}</b>",
        "",
        f"🛡 Security checks verified: <b>{c.get('security_coverage', 0):.1f}%</b>",
        f"📡 Overall data coverage: <b>{c.get('coverage', 0):.1f}%</b>",
        "",
        f"🟢 PASS {c['pass']}   🟠 CAUTION {c['caution']}   🔴 RISK {c['risk']}",
    ]
    if security_findings:
        lines += ["", "🔎 <b>SECURITY FINDINGS</b>"]
        for x in security_findings:
            lines.append(f"{verdict_icon(x['status'])} <b>{_esc(x['label'])}</b>: {_esc(x['detail'])}")
    elif market_findings:
        lines += ["", "🔎 <b>MARKET FINDINGS</b>"]
        for x in market_findings[:5]:
            lines.append(f"{verdict_icon(x['status'])} <b>{_esc(x['label'])}</b>: {_esc(x['detail'])}")
    else:
        lines += ["", "🟢 No security risk indicators detected in completed checks."]
    lines += ["", *_comparison_lines(result), "", "Missing data is never converted to zero."]
    return "\n".join(lines)


def _market_text(result):
    m = result["market"]["data"]
    lines = ["💰 <b>MARKET</b>", "", f"Price: <b>{_esc(fmt_money(m.get('price')))}</b>",
             f"Market Cap: <b>{_esc(fmt_money(m.get('market_cap')))}</b>",
             f"FDV: <b>{_esc(fmt_money(m.get('fdv')))}</b>",
             f"Liquidity: <b>{_esc(fmt_money(m.get('liquidity')))}</b>",
             f"24h Volume: <b>{_esc(fmt_money(m.get('volume_24h')))}</b>",
             f"24h Buys / Sells: <b>{_esc(str(m.get('buys_24h') if m.get('buys_24h') is not None else 'Not available'))} / {_esc(str(m.get('sells_24h') if m.get('sells_24h') is not None else 'Not available'))}</b>",
             f"24h Buy/Sell: <b>{_esc(fmt_ratio((m.get('periods', {}).get('h24') or {}).get('ratio')))}</b>",
             f"24h Price Change: <b>{_esc(fmt_pct(m.get('price_change_24h')))}</b>",
             f"Liquidity / Market Cap: <b>{_esc(fmt_pct(m.get('liq_mc')))}</b>",
             f"Liquidity / FDV: <b>{_esc(fmt_pct(m.get('liq_fdv')))}</b>",
             "", *_comparison_lines(result)]
    return "\n".join(lines)


def _holders_items(result):
    d = result["security"]
    holders = d.get("holders") if isinstance(d.get("holders"), list) else []
    items = []
    for i, h in enumerate(holders[:10], 1):
        pct = number(h.get("percent"))
        if pct is not None and pct <= 1:
            pct *= 100
        tag = h.get("tag") or "No tag"
        items.append(f"👛 <b>#{i} { _esc(short_address(h.get('address'))) }</b> — {_esc(fmt_pct(pct))} — {_esc(tag)}")
    return items


def _holders_page(result, page):
    d = result["security"]
    holders = _holders_items(result)
    per_page = 5
    total = max(1, math.ceil(len(holders) / per_page))
    page = max(1, min(page, total))
    items = holders[(page-1)*per_page:page*per_page]
    lines = ["👛 <b>HOLDERS</b>", "", f"Holder Count: <b>{_esc(str(integer(d.get('holder_count')) if integer(d.get('holder_count')) is not None else 'Not available'))}</b>", f"Total Supply: <b>{_esc(fmt_num(number(d.get('total_supply'))))}</b>", ""]
    lines += items or ["⚪ Top holder data is not available."]
    changes = result.get("comparison", {}).get("top_holder_changes") or []
    if changes and page == total:
        lines += ["", "📊 <b>HOLDER CHANGES SINCE LAST SCAN</b>"]
        for x in changes[:5]:
            lines.append(f"🔵 {_esc(short_address(x['wallet']))}: {_esc(fmt_signed_pct(x['change_pct_points']))} points")
    lines += ["", f"Page {page}/{total}"]
    return "\n".join(lines), total


def _comparison_text(result):
    c = result.get("comparison", {})
    lines = _comparison_lines(result)
    if c.get("available"):
        lines += ["", "Security findings and market movement are reported separately.", "MARSOF snapshots are used only for scan-to-scan comparison; they are not treated as full launch-to-present price history."]
    else:
        lines += ["", "No previous snapshot is available yet.", "A later scan will create the comparison baseline."]
    return "\n".join(lines)


def _security_items(result):
    return result["classification"].get("security_checks", [])


def _ai_items(result):
    # Automated intelligence checks: risk-oriented checks only; measurements stay in DATA.
    return [x for x in result["classification"].get("security_checks", []) if x["status"] in {"PASS", "CAUTION", "RISK"}]


def _full_items(result):
    return result["classification"].get("all", [])


def _checks_page(result, items, title, page, per_page=7):
    total = max(1, math.ceil(len(items) / per_page))
    page = max(1, min(page, total))
    subset = items[(page-1)*per_page:page*per_page]
    lines = [title, ""]
    if not subset:
        lines.append("No items are available in this section.")
    for x in subset:
        lines.append(f"{verdict_icon(x['status'])} <b>{_esc(x['label'])}</b>")
        lines.append(f"   {_esc(x['detail'])}")
    lines += ["", f"Page {page}/{total}"]
    return "\n".join(lines), total


def _links_text(result):
    pair = result.get("market", {}).get("data", {}).get("pair") or {}
    pair_address = pair.get("pairAddress")
    dex_url = f"https://dexscreener.com/bsc/{pair_address}" if pair_address else None
    lines = [
        "🔗 <b>MARSOF AI LINKS</b>", "",
        "Official project links:",
        "• Website",
        "• X / Twitter",
        "• Telegram",
    ]
    if dex_url:
        lines += ["• Verified DEX pair for this scan"]
    else:
        lines += ["• DEX pair: Not available from the completed scan"]
    return "\n".join(lines), dex_url


def render_section(result, section, page=1):
    if section == "home":
        return build_home(result), 1
    if section == "market":
        return _market_text(result), 1
    if section == "comparison":
        return _comparison_text(result), 1
    if section == "holders":
        return _holders_page(result, page)
    if section == "security":
        return _checks_page(result, _security_items(result), "🔐 <b>SECURITY</b>", page)
    if section == "ai":
        return _checks_page(result, _ai_items(result), "🧠 <b>AI CHECKS</b>", page)
    if section == "full":
        return _checks_page(result, _full_items(result), "📋 <b>FULL REPORT</b>", page, per_page=6)
    if section == "links":
        return _links_text(result)
    return build_home(result), 1


def _nav_button(label, data):
    return types.InlineKeyboardButton(label, callback_data=data)


def keyboard(section="home", page=1, total_pages=1, result=None):
    kb = types.InlineKeyboardMarkup(row_width=2)
    if section == "home":
        kb.row(_nav_button("🔐 SECURITY", "v:security:1"), _nav_button("💰 MARKET", "v:market:1"))
        kb.row(_nav_button("👛 HOLDERS", "v:holders:1"), _nav_button("📊 COMPARISON", "v:comparison:1"))
        kb.row(_nav_button("🧠 AI CHECKS", "v:ai:1"), _nav_button("🔗 PROJECT LINKS", "v:links:1"))
        kb.row(_nav_button("📋 FULL REPORT", "v:full:1"))
    else:
        nav = []
        if page > 1:
            nav.append(_nav_button("◀ Previous", f"v:{section}:{page-1}"))
        if page < total_pages:
            nav.append(_nav_button("Next ▶", f"v:{section}:{page+1}"))
        if nav:
            kb.row(*nav)
        kb.row(_nav_button("🏠 Overview", "v:home:1"))
        if section != "links":
            kb.row(_nav_button("🔗 Project Links", "v:links:1"))
    return kb


def links_keyboard(result):
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("🌐 Website", url=PROJECT_WEBSITE))
    kb.add(types.InlineKeyboardButton("𝕏 X / Twitter", url=PROJECT_X))
    kb.add(types.InlineKeyboardButton("✈️ Telegram", url=PROJECT_TELEGRAM))
    pair = result.get("market", {}).get("data", {}).get("pair") or {}
    pair_address = pair.get("pairAddress")
    if pair_address:
        kb.add(types.InlineKeyboardButton("📈 Open Verified DEX Pair", url=f"https://dexscreener.com/bsc/{pair_address}"))
    kb.add(types.InlineKeyboardButton("🏠 Overview", callback_data="v:home:1"))
    return kb


def _cleanup_ui(chat_id, keep_message_id=None):
    state = getattr(bot, "_marsof_ui", {}).get(chat_id) if bot else None
    if not state:
        return
    mids = [state.get("message_id")] + list(state.get("extra_message_ids", []))
    for mid in mids:
        if not mid or mid == keep_message_id:
            continue
        try:
            bot.delete_message(chat_id, mid)
        except Exception:
            pass


def _store_ui(chat_id, message_id, result, section="home", page=1):
    bot._marsof_ui = getattr(bot, "_marsof_ui", {})
    bot._marsof_ui[chat_id] = {
        "message_id": message_id,
        "extra_message_ids": [],
        "result": result,
        "section": section,
        "page": page,
    }


def send_result_ui(chat_id, result):
    _cleanup_ui(chat_id)
    sent = bot.send_message(chat_id, build_home(result), parse_mode="HTML", reply_markup=keyboard())
    _store_ui(chat_id, sent.message_id, result)
    return sent

# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(title="MARSOF AI", version="4.0.0")


@app.get("/")
def root():
    return {
        "status": "running",
        "service": "MARSOF AI",
        "version": "4.0.0",
        "database": bool(engine),
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "telegram_token": bool(TELEGRAM_TOKEN),
        "database_configured": bool(engine),
        "webhook_url": WEBHOOK_URL,
    }


def setup_telegram_webhook():
    if not TELEGRAM_TOKEN:
        return False
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook",
            json={
                "url": WEBHOOK_URL,
                "allowed_updates": ["message", "callback_query"],
            },
            timeout=20,
        )
        r.raise_for_status()
        return bool(r.json().get("ok"))
    except Exception as e:
        print(f"❌ webhook setup: {e}")
        return False


def get_telegram_webhook_info():
    if not TELEGRAM_TOKEN:
        return {"ok": False, "error": "TELEGRAM_TOKEN غير موجود"}
    try:
        r = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getWebhookInfo",
            timeout=20,
        )
        r.raise_for_status()
        return r.json()
    except Exception as e:
        return {"ok": False, "error": str(e)}


def migrate_database():
    if not engine:
        return
    try:
        Base.metadata.create_all(bind=engine)
        inspector = inspect(engine)
        tables = set(inspector.get_table_names())
        additions = {
            "tokens": {
                "name": "VARCHAR(200)",
                "last_scanned": "TIMESTAMP",
                "scan_count": "INTEGER DEFAULT 0",
            },
            "security_scans": {
                "verdict": "VARCHAR(30)",
                "coverage": "DOUBLE PRECISION",
                "pass_count": "INTEGER",
                "caution_count": "INTEGER",
                "risk_count": "INTEGER",
                "raw_data": "TEXT",
                "scanned_at": "TIMESTAMP",
            },
            "market_snapshots": {
                "pair_address": "VARCHAR(100)",
                "dex_id": "VARCHAR(80)",
                "price_usd": "DOUBLE PRECISION",
                "market_cap_usd": "DOUBLE PRECISION",
                "fdv_usd": "DOUBLE PRECISION",
                "liquidity_usd": "DOUBLE PRECISION",
                "volume_5m_usd": "DOUBLE PRECISION",
                "volume_1h_usd": "DOUBLE PRECISION",
                "volume_6h_usd": "DOUBLE PRECISION",
                "volume_24h_usd": "DOUBLE PRECISION",
                "buys_5m": "INTEGER", "sells_5m": "INTEGER",
                "buys_1h": "INTEGER", "sells_1h": "INTEGER",
                "buys_6h": "INTEGER", "sells_6h": "INTEGER",
                "buys_24h": "INTEGER", "sells_24h": "INTEGER",
                "price_change_5m": "DOUBLE PRECISION",
                "price_change_1h": "DOUBLE PRECISION",
                "price_change_6h": "DOUBLE PRECISION",
                "price_change_24h": "DOUBLE PRECISION",
                "pair_created_at": "TIMESTAMP",
                "snapshot_time": "TIMESTAMP",
            },
            "holder_snapshots": {
                "wallet": "VARCHAR(100)",
                "balance": "DOUBLE PRECISION",
                "percentage": "DOUBLE PRECISION",
                "tag": "VARCHAR(200)",
                "is_contract": "BOOLEAN",
                "is_locked": "BOOLEAN",
                "snapshot_time": "TIMESTAMP",
            },
        }
        with engine.begin() as conn:
            for table, cols in additions.items():
                if table not in tables:
                    continue
                existing = {c["name"] for c in inspect(conn).get_columns(table)}
                for col, typ in cols.items():
                    if col not in existing:
                        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {col} {typ}"))
        print("✅ Database tables/migrations ready")
    except Exception as e:
        print(f"❌ Database migration: {e}")


@app.on_event("startup")
def startup():
    migrate_database()
    setup_telegram_webhook()
    print("📡", json.dumps(get_telegram_webhook_info(), ensure_ascii=False))


@app.post("/webhook")
async def webhook(request: Request):
    if not bot:
        return {"ok": False, "error": "TELEGRAM_TOKEN غير موجود"}
    try:
        raw = await request.json()
        update = telebot.types.Update.de_json(json.dumps(raw))
        if update is None:
            raise ValueError("Invalid Telegram update")
        bot.process_new_updates([update])
        return {"ok": True}
    except Exception as e:
        print(f"❌ webhook processing: {e}")
        return {"ok": False, "error": str(e)}


# ============================================================
# TELEGRAM
# ============================================================

if bot:
    @bot.message_handler(commands=["start"])
    def start(message):
        _cleanup_ui(message.chat.id)
        caption = (
            "<b>MARSOF AI</b>\n"
            "<b>Advanced AI-Assisted Cryptocurrency Token Analysis</b>\n\n"
            "Analyze a BSC token using live security, market and holder data.\n\n"
            "<b>What MARSOF checks</b>\n"
            "• Contract security indicators\n"
            "• Market and liquidity data\n"
            "• Holder concentration\n"
            "• Scan-to-scan changes\n"
            "• Automated risk indicators\n\n"
            "<b>Reporting principle</b>\n"
            "Real returned values are shown as DATA. Missing values are never converted to zero. If an important field cannot be verified, the relevant test is marked incomplete.\n\n"
            "🟢 PASS  •  🟠 CAUTION  •  🔴 RISK  •  🔵 DATA  •  ⚪ UNAVAILABLE\n\n"
            "Send a BSC contract address to start a free analysis."
        )
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.row(types.InlineKeyboardButton("🔍 ANALYZE TOKEN", callback_data="start:help"))
        markup.row(types.InlineKeyboardButton("🔗 PROJECT LINKS", callback_data="start:links"))
        try:
            if os.path.exists(LOGO_PATH):
                sent = bot.send_photo(message.chat.id, open(LOGO_PATH, "rb"), caption=caption, parse_mode="HTML", reply_markup=markup)
                _store_ui(message.chat.id, sent.message_id, None, "start", 1)
            else:
                sent = bot.send_message(message.chat.id, caption, parse_mode="HTML", reply_markup=markup)
                _store_ui(message.chat.id, sent.message_id, None, "start", 1)
        except Exception as exc:
            print(f"⚠️ start logo UI fallback: {exc}")
            sent = bot.send_message(message.chat.id, caption, parse_mode="HTML", reply_markup=markup)
            _store_ui(message.chat.id, sent.message_id, None, "start", 1)

    @bot.message_handler(content_types=["text"])
    def text_handler(message):
        if not message.text or message.text.startswith("/"):
            return
        address = message.text.strip()
        if not is_valid_bsc_address(address):
            bot.reply_to(message, "⚠️ Please send a valid BSC contract address (0x + 40 hexadecimal characters).")
            return
        try:
            bot.send_chat_action(message.chat.id, "typing")
            result = run_analysis(address)
            if not result:
                bot.reply_to(message, "⚠️ No matching security data was returned for this contract. No other contract was substituted and no values were fabricated.")
                return
            send_result_ui(message.chat.id, result)
        except requests.RequestException as exc:
            print(f"❌ data source error: {exc}")
            bot.reply_to(message, "⚠️ A live data source is temporarily unavailable. No result was fabricated.")
        except Exception as exc:
            print(f"❌ analysis error: {exc}")
            bot.reply_to(message, "⚠️ A technical error occurred during analysis. No result was fabricated.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("start:"))
    def start_callbacks(call):
        try:
            action = call.data.split(":", 1)[1]
            if action == "links":
                bot.edit_message_caption("🔗 <b>MARSOF AI PROJECT</b>\n\nUse the official links below.", call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🌐 Website", url=PROJECT_WEBSITE)))
                # Replace the one-button markup with the complete link set.
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=types.InlineKeyboardMarkup(row_width=1))
                kb = types.InlineKeyboardMarkup(row_width=1)
                kb.add(types.InlineKeyboardButton("🌐 Website", url=PROJECT_WEBSITE))
                kb.add(types.InlineKeyboardButton("𝕏 X / Twitter", url=PROJECT_X))
                kb.add(types.InlineKeyboardButton("✈️ Telegram", url=PROJECT_TELEGRAM))
                kb.add(types.InlineKeyboardButton("← Back", callback_data="start:home"))
                bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=kb)
            elif action == "home":
                caption = (
                    "<b>MARSOF AI</b>\n\n"
                    "Advanced AI-assisted cryptocurrency token analysis.\n\n"
                    "Send a BSC contract address to begin a free analysis."
                )
                bot.edit_message_caption(caption, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🔍 ANALYZE TOKEN", callback_data="start:help"), types.InlineKeyboardButton("🔗 PROJECT LINKS", callback_data="start:links")))
            else:
                bot.answer_callback_query(call.id, "Send a BSC contract address to start.")
                return
            bot.answer_callback_query(call.id)
        except Exception as exc:
            print(f"❌ start callback error: {exc}")
            bot.answer_callback_query(call.id, "Unable to update the interface.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("v:"))
    def callbacks(call):
        try:
            state = getattr(bot, "_marsof_ui", {}).get(call.message.chat.id)
            if not state or not state.get("result"):
                bot.answer_callback_query(call.id, "Run a token analysis first.")
                return
            _, section, page_text = call.data.split(":", 2)
            page = int(page_text)
            result = state["result"]
            rendered, total = render_section(result, section, page)
            if section == "links":
                bot.edit_message_text(rendered, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=links_keyboard(result))
            else:
                bot.edit_message_text(rendered, call.message.chat.id, call.message.message_id, parse_mode="HTML", reply_markup=keyboard(section, page, total, result))
            state["section"] = section
            state["page"] = page
            state["extra_message_ids"] = []
            bot._marsof_ui[call.message.chat.id] = state
            bot.answer_callback_query(call.id)
        except Exception as exc:
            print(f"❌ callback error: {exc}")
            bot.answer_callback_query(call.id, "Unable to update the interface.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
