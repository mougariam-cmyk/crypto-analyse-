import os
import json
import math
import re
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
# MARSOF AI — STEP 1: REAL-TIME TOKEN ANALYZER
# BNB Smart Chain only for this phase.
# No whale/wallet movement engine and no trend bot yet.
# Missing source values ALWAYS remain None / "Not available".
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = "https://crypto-analyse-bot-z7o0.onrender.com/webhook"
DEFAULT_CHAIN = "bsc"
NETWORKS = {
    "bsc": {"name": "BNB Smart Chain", "chain_id": "56", "dex_chain": "bsc", "kind": "evm", "rpc": "https://bsc-dataseed.bnbchain.org", "explorer": "https://bscscan.com"},
    "solana": {"name": "Solana", "chain_id": "solana", "dex_chain": "solana", "kind": "solana", "rpc": "https://api.mainnet-beta.solana.com", "explorer": "https://solscan.io"},
    "sui": {"name": "Sui", "chain_id": "sui", "dex_chain": "sui", "kind": "sui", "rpc": "https://fullnode.mainnet.sui.io:443", "explorer": "https://suiscan.xyz/mainnet"},
    "arc": {"name": "Arc", "chain_id": "5042", "dex_chain": "arc", "kind": "evm", "rpc": "https://rpc.mainnet.arc.io", "explorer": "https://explorer.arc.io"},
    "robinhood": {"name": "Robinhood Chain", "chain_id": "4663", "dex_chain": "robinhood", "kind": "evm", "rpc": "https://rpc.mainnet.chain.robinhood.com", "explorer": "https://robinhoodchain.blockscout.com"},
    "base": {"name": "Base", "chain_id": "8453", "dex_chain": "base", "kind": "evm", "rpc": "https://mainnet.base.org", "explorer": "https://basescan.org"},
    "ethereum": {"name": "Ethereum", "chain_id": "1", "dex_chain": "ethereum", "kind": "evm", "rpc": "https://cloudflare-eth.com", "explorer": "https://etherscan.io"},
    "hyperliquid": {"name": "Hyperliquid", "chain_id": "999", "dex_chain": "hyperliquid", "kind": "evm", "rpc": "https://rpc.hyperliquid.xyz/evm", "explorer": "https://hyperevmscan.io"},
}
GO_PLUS_BASE = "https://api.gopluslabs.io/api/v1/token_security"
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
    # Render provides DATABASE_URL; requirements install psycopg2-binary.
    # Force SQLAlchemy to use psycopg2 instead of auto-selecting psycopg (v3).
    if DATABASE_URL.startswith("postgresql://"):
        DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+psycopg2://", 1)
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
        return "Not available"
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
    return "Not available" if value is None else f"${fmt_num(value)}"


def fmt_pct(value):
    return "Not available" if value is None else f"{value:.2f}%"


def fmt_ratio(value):
    return "Not available" if value is None else f"{value:.2f}x"


def fmt_bool(value):
    return "نعم" if value is True else "لا" if value is False else "Not available"


def parse_timestamp(value):
    x = number(value)
    if x is None:
        return None
    try:
        return datetime.fromtimestamp(x, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def age_days(timestamp):
    if timestamp is None:
        return None
    return max(0.0, (utc_now() - timestamp).total_seconds() / 86400)


def get_network(key):
    return NETWORKS.get(key)


def is_valid_evm_address(address):
    if not address or len(address.strip()) != 42 or not address.strip().lower().startswith("0x"):
        return False
    try:
        int(address.strip()[2:], 16)
        return True
    except ValueError:
        return False


def is_valid_solana_address(address):
    alphabet = set("123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")
    if not address or not (32 <= len(address) <= 44) or any(c not in alphabet for c in address):
        return False
    # Conservative base58 decode length check without an external dependency.
    n = 0
    for c in address:
        n = n * 58 + "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".index(c)
    raw_len = max(1, (n.bit_length() + 7) // 8)
    raw_len += len(address) - len(address.lstrip("1"))
    return raw_len == 32


def is_valid_sui_address(address):
    a = (address or "").strip().lower()
    return bool(re.fullmatch(r"0x[0-9a-f]{1,64}(?:::[A-Za-z_][A-Za-z0-9_]*:[A-Za-z_][A-Za-z0-9_]*){0,2}", a))


def is_valid_for_network(address, network):
    kind = NETWORKS.get(network, {}).get("kind")
    if kind == "evm":
        return is_valid_evm_address(address)
    if kind == "solana":
        return is_valid_solana_address(address)
    if kind == "sui":
        return is_valid_sui_address(address)
    return False


def chain_id_for(network):
    return NETWORKS[network]["chain_id"]


def network_label(network):
    return NETWORKS.get(network, {}).get("name", network)


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

def fetch_goplus(address, network):
    cfg = NETWORKS.get(network)
    if not cfg or cfg.get("kind") != "evm":
        return None
    try:
        response = requests.get(
            f"{GO_PLUS_BASE}/{cfg['chain_id']}",
            params={"contract_addresses": address},
            timeout=25,
        )
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict):
            return None
        target = normalize_address(address)
        for returned_address, data in result.items():
            if normalize_address(returned_address) == target and isinstance(data, dict):
                return dict(data)
    except Exception as exc:
        print(f"ℹ️ GoPlus unavailable for {network}: {exc}")
    return None


def fetch_dexscreener(address, network):
    cfg = NETWORKS[network]
    response = requests.get(DEXSCREENER_URL.format(address=address.strip()), timeout=25)
    response.raise_for_status()
    payload = response.json()
    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if not isinstance(pairs, list):
        return []
    target = address.strip().lower()
    matches = []
    for pair in pairs:
        if not isinstance(pair, dict):
            continue
        if str(pair.get("chainId") or "").lower() != cfg["dex_chain"]:
            continue
        base = str((pair.get("baseToken") or {}).get("address") or "").lower()
        quote = str((pair.get("quoteToken") or {}).get("address") or "").lower()
        if target == base or target == quote:
            matches.append(pair)
    matches.sort(key=lambda p: number((p.get("liquidity") or {}).get("usd")) or -1, reverse=True)
    if matches:
        print(f"✅ DEX Screener: {len(matches)} exact {cfg['name']} pair(s); selected {matches[0].get('pairAddress')}")
    else:
        print(f"ℹ️ DEX Screener: no indexed {cfg['name']} pair for {address}; no market value fabricated")
    return matches


def evm_rpc_call(rpc, method, params):
    r = requests.post(rpc, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=20)
    r.raise_for_status()
    obj = r.json()
    return obj.get("result") if isinstance(obj, dict) else None


def _decode_abi_string(hexdata):
    if not isinstance(hexdata, str) or not hexdata.startswith("0x") or len(hexdata) < 2:
        return None
    raw = bytes.fromhex(hexdata[2:])
    try:
        if len(raw) >= 64:
            offset = int.from_bytes(raw[:32], "big")
            if offset + 32 <= len(raw):
                ln = int.from_bytes(raw[offset:offset+32], "big")
                body = raw[offset+32:offset+32+ln]
                return body.decode("utf-8", errors="replace")
        return raw.rstrip(b"\x00").decode("utf-8", errors="replace") or None
    except Exception:
        return None


def _decode_abi_uint(hexdata):
    try:
        return int(hexdata, 16) if hexdata and hexdata != "0x" else None
    except Exception:
        return None


def fetch_evm_metadata(address, network):
    cfg = NETWORKS[network]
    data = {}
    try:
        code = evm_rpc_call(cfg["rpc"], "eth_getCode", [address, "latest"])
        data["contract_exists"] = bool(code and code != "0x")
    except Exception as exc:
        print(f"⚠️ EVM code lookup {network}: {exc}")
        data["contract_exists"] = None
    calls = {"token_name":"0x06fdde03", "token_symbol":"0x95d89b41", "decimals":"0x313ce567", "total_supply":"0x18160ddd"}
    for key, selector in calls.items():
        try:
            raw = evm_rpc_call(cfg["rpc"], "eth_call", [{"to": address, "data": selector}, "latest"])
            if key in {"decimals", "total_supply"}:
                data[key] = _decode_abi_uint(raw)
            else:
                data[key] = _decode_abi_string(raw)
        except Exception:
            data[key] = None
    return data


def solana_rpc(method, params):
    return evm_rpc_call(NETWORKS["solana"]["rpc"], method, params)


def fetch_solana_metadata(address):
    data = {}
    try:
        acct = solana_rpc("getAccountInfo", [address, {"encoding":"jsonParsed"}])
        value = (acct or {}).get("value") if isinstance(acct, dict) else None
        parsed = ((value or {}).get("data") or {}).get("parsed") or {}
        info = parsed.get("info") or {}
        data["mint_authority"] = info.get("mintAuthority")
        data["freeze_authority"] = info.get("freezeAuthority")
        data["decimals"] = ((info.get("decimals")) if info else None)
        data["supply"] = number(info.get("supply"))
        data["program"] = (value or {}).get("owner")
    except Exception as exc:
        print(f"⚠️ Solana mint lookup: {exc}")
    try:
        supply = solana_rpc("getTokenSupply", [address])
        sinfo = ((supply or {}).get("value")) if isinstance(supply, dict) else None
        if isinstance(sinfo, dict):
            data["decimals"] = sinfo.get("decimals", data.get("decimals"))
            data["supply"] = number(sinfo.get("uiAmount")) if sinfo.get("uiAmount") is not None else data.get("supply")
    except Exception:
        pass
    try:
        largest = solana_rpc("getTokenLargestAccounts", [address])
        data["largest_accounts"] = ((largest or {}).get("value")) if isinstance(largest, dict) else []
    except Exception:
        data["largest_accounts"] = []
    return data


def build_non_bsc_security(meta, network):
    checks = []
    add = lambda label, value, status="DATA", detail=None: checks.append(metric(label, value, status, detail, "On-chain"))
    add("Network", network_label(network), "DATA", network_label(network))
    if network == "solana":
        add("Mint Authority", meta.get("mint_authority"), "CAUTION" if meta.get("mint_authority") else "DATA" if meta.get("mint_authority") is not None else "UNKNOWN", meta.get("mint_authority") or "Revoked / not available")
        add("Freeze Authority", meta.get("freeze_authority"), "CAUTION" if meta.get("freeze_authority") else "DATA" if meta.get("freeze_authority") is not None else "UNKNOWN", meta.get("freeze_authority") or "Revoked / not available")
        add("Decimals", integer(meta.get("decimals")), "DATA" if meta.get("decimals") is not None else "UNKNOWN", str(integer(meta.get("decimals"))) if meta.get("decimals") is not None else "Not available")
        add("Total Supply", number(meta.get("supply")), "DATA" if meta.get("supply") is not None else "UNKNOWN", fmt_num(meta.get("supply")))
        la = meta.get("largest_accounts") or []
        for i, item in enumerate(la[:10], 1):
            amount = number(item.get("uiAmount")) if isinstance(item, dict) else None
            add(f"Largest Token Account #{i}", amount, "DATA" if amount is not None else "UNKNOWN", fmt_num(amount))
    else:
        add("Contract Code", meta.get("contract_exists"), "DATA" if meta.get("contract_exists") is not None else "UNKNOWN", "Contract bytecode detected" if meta.get("contract_exists") else "No contract bytecode detected" if meta.get("contract_exists") is False else "Not available")
        add("Token Name", meta.get("token_name"), "DATA" if meta.get("token_name") else "UNKNOWN", meta.get("token_name") or "Not available")
        add("Token Symbol", meta.get("token_symbol"), "DATA" if meta.get("token_symbol") else "UNKNOWN", meta.get("token_symbol") or "Not available")
        add("Decimals", integer(meta.get("decimals")), "DATA" if meta.get("decimals") is not None else "UNKNOWN", str(integer(meta.get("decimals"))) if meta.get("decimals") is not None else "Not available")
        add("Total Supply", number(meta.get("total_supply")), "DATA" if meta.get("total_supply") is not None else "UNKNOWN", fmt_num(meta.get("total_supply")))
        add("Security Engine", None, "UNKNOWN", "Network-specific contract security checks are not yet available")
    return checks


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
        "detail": detail if detail is not None else str(value) if value is not None else "Not available",
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
    add("Token Name", data.get("token_name"), "DATA", data.get("token_name"))
    add("Token Symbol", data.get("token_symbol"), "DATA", data.get("token_symbol"))
    add("Contract Name", data.get("contract_name"), "DATA", data.get("contract_name"))
    add("Holder Count", integer(data.get("holder_count")), "DATA", str(integer(data.get("holder_count"))) if integer(data.get("holder_count")) is not None else "Not available")
    add("Total Supply", number(data.get("total_supply")), "DATA", fmt_num(number(data.get("total_supply"))))
    add("Decimals", integer(data.get("decimals")), "DATA", str(integer(data.get("decimals"))) if integer(data.get("decimals")) is not None else "Not available")

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
    add("Owner Address", owner_address, "DATA" if owner_address else "UNKNOWN", owner_address or "Not available")
    add("Owner Type", owner_type, "DATA" if owner_type else "UNKNOWN", owner_type or "Not available")
    creator = data.get("creator_address")
    add("Creator / Deployer", creator, "DATA" if creator else "UNKNOWN", creator or "Not available")
    creator_balance = number(data.get("creator_balance"))
    creator_percent = number(data.get("creator_percent"))
    add("Creator Balance", creator_balance, "DATA", fmt_num(creator_balance))
    add("Creator Token %", creator_percent * 100 if creator_percent is not None else None, "DATA", fmt_pct(creator_percent * 100 if creator_percent is not None else None))

    deployed = parse_timestamp(data.get("deployed_time"))
    add("Contract Deployed", deployed.isoformat() if deployed else None, "DATA" if deployed else "UNKNOWN", deployed.strftime("%Y-%m-%d %H:%M UTC") if deployed else "Not available")
    deployed_age = age_days(deployed)
    add("Contract Age (days)", deployed_age, "DATA", f"{deployed_age:.2f} days" if deployed_age is not None else "Not available")

    # Top holders and concentration. Keep the exact source list; don't fabricate top-20/50
    # from only top-10 data.
    holders = data.get("holders")
    if isinstance(holders, list) and holders:
        top10 = sum(number(h.get("percent")) or 0 for h in holders[:10]) * (100 if sum(number(h.get("percent")) or 0 for h in holders[:10]) <= 1.0 else 1)
        # GoPlus documents percent as a percentage. If a provider ever returns a fraction,
        # the normalization above handles it without changing an already-percent value.
        add("Top 10 Concentration", top10, "CAUTION" if top10 >= 40 else "PASS", fmt_pct(top10))
        for i, h in enumerate(holders[:10], 1):
            pct = number(h.get("percent"))
            if pct is not None and pct <= 1:
                pct *= 100
            label = f"Top Holder #{i}"
            detail = f"{h.get('address') or 'Not available'} — {fmt_pct(pct)} — {h.get('tag') or 'No tag'}"
            add(label, pct, "DATA", detail)
    else:
        add("Top 10 Concentration", None, "UNKNOWN", "Not available")

    # LP information
    lp_count = integer(data.get("lp_holder_count"))
    lp_supply = number(data.get("lp_total_supply"))
    add("LP Holder Count", lp_count, "DATA", str(lp_count) if lp_count is not None else "Not available")
    add("LP Token Total Supply", lp_supply, "DATA", fmt_num(lp_supply))

    lp_holders = data.get("lp_holders")
    if isinstance(lp_holders, list) and lp_holders:
        for i, h in enumerate(lp_holders[:10], 1):
            pct = number(h.get("percent"))
            if pct is not None and pct <= 1:
                pct *= 100
            detail = f"{h.get('address') or 'Not available'} — {fmt_pct(pct)} — {h.get('tag') or 'No tag'}"
            add(f"LP Holder #{i}", pct, "DATA", detail)

    # DEX data supplied by GoPlus (may be absent even when DexScreener has a pair).
    dexes = data.get("dex")
    if isinstance(dexes, list):
        add("GoPlus DEX Pools", len(dexes), "DATA", str(len(dexes)))
        for i, d in enumerate(dexes[:10], 1):
            if isinstance(d, dict):
                pool_liq = number(d.get("liquidity"))
                detail = f"{d.get('name') or 'Unknown'} | {d.get('pair') or 'Not available'} | ${fmt_num(pool_liq)}"
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
    add("DEX", pair.get("dexId"), "DATA", pair.get("dexId") or "Not available")
    add("Pair Address", pair.get("pairAddress"), "DATA", pair.get("pairAddress") or "Not available")
    add("Base Token", (pair.get("baseToken") or {}).get("address"), "DATA", (pair.get("baseToken") or {}).get("symbol") or "Not available")
    add("Quote Token", (pair.get("quoteToken") or {}).get("address"), "DATA", (pair.get("quoteToken") or {}).get("symbol") or "Not available")

    pair_created = parse_timestamp(number(pair.get("pairCreatedAt")))
    add("Pair Created", pair_created.isoformat() if pair_created else None, "DATA" if pair_created else "UNKNOWN", pair_created.strftime("%Y-%m-%d %H:%M UTC") if pair_created else "Not available")
    pair_age = age_days(pair_created)
    add("Pair Age (days)", pair_age, "DATA", f"{pair_age:.2f} days" if pair_age is not None else "Not available")

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
        add(f"{title} Buys", buys, "DATA", str(buys) if buys is not None else "Not available")
        add(f"{title} Sells", sells, "DATA", str(sells) if sells is not None else "Not available")
        add(f"{title} Buy/Sell Ratio", ratio, "DATA", fmt_ratio(ratio))
        add(f"{title} Transactions", total_tx, "DATA", str(total_tx) if total_tx is not None else "Not available")
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
    add("7d Volume", None, "UNKNOWN", "No immediate value is available")
    add("7d Price Change", None, "UNKNOWN", "Requires historical snapshots")
    add("24h Liquidity Change", None, "UNKNOWN", "Requires historical snapshots")
    add("7d Liquidity Change", None, "UNKNOWN", "Requires historical snapshots")

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

def enrich_historical_market(market, address, network):
    if not SessionLocal or not market.get("data", {}).get("pair"):
        return market

    session = SessionLocal()
    try:
        now = utc_now()
        addr = normalize_address(address)
        current = market["data"]
        current_liq = current.get("liquidity")
        current_vol = current.get("volume_24h")
        current_price = current.get("price")

        checks_by_label = {x["label"]: x for x in market["checks"]}

        def add_or_update(label, value, detail):
            if label in checks_by_label:
                checks_by_label[label].update({"value": value, "available": value is not None, "status": "DATA" if value is not None else "UNKNOWN", "detail": detail})
            else:
                market["checks"].append(metric(label, value, "DATA" if value is not None else "UNKNOWN", detail, "MARSOF historical snapshots"))

        def nearest_before(delta):
            target = now - delta
            return (
                session.query(MarketSnapshot)
                .filter(
                    MarketSnapshot.chain_id == chain_id_for(network),
                    MarketSnapshot.token_address == addr,
                    MarketSnapshot.snapshot_time <= target,
                )
                .order_by(MarketSnapshot.snapshot_time.desc())
                .first()
            )

        for days, prefix in [(1, "24h"), (7, "7d")]:
            snap = nearest_before(timedelta(days=days))
            if not snap:
                add_or_update(f"{prefix} Price Change (historical)", None, "Requires historical snapshots")
                add_or_update(f"{prefix} Liquidity Change (historical)", None, "Requires historical snapshots")
                continue
            price_change = ((current_price - snap.price_usd) / snap.price_usd * 100) if current_price is not None and snap.price_usd not in (None, 0) else None
            liq_change = ((current_liq - snap.liquidity_usd) / snap.liquidity_usd * 100) if current_liq is not None and snap.liquidity_usd not in (None, 0) else None
            add_or_update(f"{prefix} Price Change (historical)", price_change, fmt_pct(price_change))
            add_or_update(f"{prefix} Liquidity Change (historical)", liq_change, fmt_pct(liq_change))

        # Replace immediate placeholders only when historical data really exists.
        market["checks"] = list(checks_by_label.values())
        return market
    finally:
        session.close()


# ============================================================
# CLASSIFICATION
# ============================================================

def classify_all(security_checks, market_checks):
    all_checks = security_checks + market_checks
    available = [x for x in all_checks if x["status"] != "UNKNOWN"]
    risk_tests = [x for x in all_checks if x["status"] in {"PASS", "CAUTION", "RISK"}]
    missing = [x["label"] for x in all_checks if x["status"] == "UNKNOWN"]
    data_only = [x for x in all_checks if x["status"] == "DATA"]

    p = sum(x["status"] == "PASS" for x in risk_tests)
    c = sum(x["status"] == "CAUTION" for x in risk_tests)
    r = sum(x["status"] == "RISK" for x in risk_tests)
    coverage = (len(available) / len(all_checks) * 100) if all_checks else 0

    if r > 0:
        verdict = "RISK"
    elif c > 0:
        verdict = "CAUTION"
    elif risk_tests:
        verdict = "PASS"
    else:
        verdict = "INCOMPLETE"

    return {
        "all": all_checks,
        "pass": p,
        "caution": c,
        "risk": r,
        "data": len(data_only),
        "missing": missing,
        "coverage": coverage,
        "verdict": verdict,
    }


# ============================================================
# DATABASE SAVE
# ============================================================

def save_analysis(address, data, classification, market, network):
    if not SessionLocal:
        return
    session = SessionLocal()
    try:
        addr = normalize_address(address)
        token = session.query(Token).filter(
            Token.contract_address == addr,
            Token.chain_id == chain_id_for(network),
        ).first()

        if not token:
            token = Token(
                contract_address=addr,
                chain_id=chain_id_for(network),
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

        if market.get("pair"):
            p = market["pair"]
            periods = market.get("periods", {})
            pair_created = parse_timestamp(p.get("pairCreatedAt"))

            session.add(MarketSnapshot(
                chain_id=chain_id_for(network),
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
                snapshot_time=utc_now(),
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
                    chain_id=chain_id_for(network),
                    token_address=addr,
                    wallet=wallet,
                    balance=number(h.get("balance")),
                    percentage=pct,
                    tag=str(h.get("tag")) if h.get("tag") is not None else None,
                    is_contract=boolean(h.get("is_contract")),
                    is_locked=boolean(h.get("is_locked")),
                    snapshot_time=utc_now(),
                ))

        session.commit()
    except Exception as exc:
        session.rollback()
        print(f"⚠️ DB save error: {exc}")
    finally:
        session.close()


def run_analysis(address, network):
    cfg = NETWORKS.get(network)
    if not cfg:
        return None

    security_raw = None
    if cfg.get("kind") == "evm":
        security_raw = fetch_goplus(address, network)
        if security_raw:
            security_checks = build_security(security_raw)
            security_data = security_raw
        else:
            metadata = fetch_evm_metadata(address, network)
            security_data = metadata
            security_checks = build_non_bsc_security(metadata, network)
    elif network == "solana":
        security_data = fetch_solana_metadata(address)
        security_checks = build_non_bsc_security(security_data, network)
    else:
        # Sui currently has no equivalent contract-security adapter in this build.
        # Keep the network and market layer usable without fabricating security data.
        security_data = {}
        security_checks = build_non_bsc_security(security_data, network)

    pairs = fetch_dexscreener(address, network)
    market = build_market(choose_market(pairs))
    classification = classify_all(security_checks, market["checks"])
    result = {
        "address": normalize_address(address),
        "network": network,
        "network_name": network_label(network),
        "security": security_data,
        "market": market,
        "classification": classification,
    }

    save_analysis(address, security_data, classification, market["data"], network)
    result["market"] = enrich_historical_market(market, address, network)
    result["classification"] = classify_all(security_checks, result["market"]["checks"])
    return result


# ============================================================
# UI
# ============================================================

def verdict_icon(status):
    return {"PASS":"🟢","CAUTION":"🟠","RISK":"🔴","DATA":"🔵","UNKNOWN":"⚪","INCOMPLETE":"⚪"}.get(status,"🔵")


def comparison_text(result):
    if not SessionLocal:
        return "<b>📊 COMPARISON</b>\n\nNo database connection is configured."
    session=SessionLocal()
    try:
        addr=normalize_address(result["address"]); cid=chain_id_for(result["network"])
        rows=(session.query(MarketSnapshot).filter(MarketSnapshot.chain_id==cid, MarketSnapshot.token_address==addr).order_by(MarketSnapshot.snapshot_time.desc()).limit(2).all())
        if len(rows)<2:
            return "<b>📊 COMPARISON</b>\n\nNo previous scan is available yet.\nThe next scan will create the comparison baseline."
        cur, prev=rows[0], rows[1]
        def ch(a,b): return ((a-b)/b*100) if a is not None and b not in (None,0) else None
        def signed(x): return "Not available" if x is None else f"{x:+.2f}%"
        age=elapsed_text(prev.snapshot_time) if 'elapsed_text' in globals() else "previous scan"
        alerts=[]
        pc=ch(cur.price_usd,prev.price_usd)
        lc=ch(cur.liquidity_usd,prev.liquidity_usd)
        if pc is not None and pc<=-50: alerts.append(f"🚨 <b>SIGNIFICANT PRICE MOVEMENT</b>\nPrice dropped {abs(pc):.2f}% since the previous scan.")
        if lc is not None and lc<=-50: alerts.append(f"🚨 <b>LIQUIDITY MOVEMENT</b>\nLiquidity fell {abs(lc):.2f}% since the previous scan.")
        lines=["<b>📊 COMPARISON</b>","",f"Last scanned: <b>{age}</b>","",f"💵 Price: <b>{fmt_money(prev.price_usd)} → {fmt_money(cur.price_usd)}</b> {signed(pc)}",f"💧 Liquidity: <b>{fmt_money(prev.liquidity_usd)} → {fmt_money(cur.liquidity_usd)}</b> {signed(lc)}",f"💰 Market Cap: <b>{fmt_money(prev.market_cap_usd)} → {fmt_money(cur.market_cap_usd)}</b> {signed(ch(cur.market_cap_usd,prev.market_cap_usd))}",f"📈 24h Volume: <b>{fmt_money(prev.volume_24h_usd)} → {fmt_money(cur.volume_24h_usd)}</b> {signed(ch(cur.volume_24h_usd,prev.volume_24h_usd))}"]
        if alerts: lines += ["", *alerts]
        lines += ["", "Security risk and market movement are reported separately."]
        return "\n".join(lines)
    finally:
        session.close()


def elapsed_text(timestamp):
    if not timestamp: return "Not available"
    if timestamp.tzinfo is None: timestamp=timestamp.replace(tzinfo=timezone.utc)
    sec=max(0,int((utc_now()-timestamp).total_seconds()))
    if sec<60: return "just now"
    if sec<3600: return f"{sec//60} minute{'s' if sec//60!=1 else ''} ago"
    if sec<86400: return f"{sec//3600} hour{'s' if sec//3600!=1 else ''} ago"
    return f"{sec//86400} day{'s' if sec//86400!=1 else ''} ago"


def build_home(result):
    d=result.get("security",{}) or {}; c=result["classification"]; m=result["market"]["data"]; net=result.get("network_name", "Unknown")
    name=d.get("token_symbol") or d.get("token_name") or "Token"
    address=result["address"]
    verdict_msg={"PASS":"No detected risk indicators among completed checks.","CAUTION":"Attention indicators were detected among completed checks.","RISK":"Risk indicators were detected among completed checks.","INCOMPLETE":"Analysis is incomplete because important data is unavailable."}.get(c["verdict"],"Analysis incomplete.")
    return (f"<b>MARSOF AI</b> — Advanced Token Analysis\n\n<b>{name}</b> · <b>{net}</b>\n<code>{address[:8]}…{address[-6:]}</code>\n\n{verdict_icon(c['verdict'])} <b>{c['verdict']}</b>\n{verdict_msg}\n\n🟢 {c['pass']} PASS   🟠 {c['caution']} CAUTION   🔴 {c['risk']} RISK\n🔵 {c['data']} DATA   ⚪ {len(c['missing'])} UNAVAILABLE\n\n💵 Price: <b>{fmt_money(m.get('price'))}</b>\n💧 Liquidity: <b>{fmt_money(m.get('liquidity'))}</b>\n💰 Market Cap: <b>{fmt_money(m.get('market_cap'))}</b>\n📈 24h Volume: <b>{fmt_money(m.get('volume_24h'))}</b>\n\n🔐 Security coverage: <b>{c.get('security_coverage', c.get('coverage',0)):.1f}%</b>\n\nReal values are shown as DATA. Missing values are never converted to zero.")


def section_text(result, category):
    checks=result["classification"]["all"]
    if category in {"security","ai"}:
        checks=result["classification"].get("security_checks",[])
        if category=="ai": checks=[x for x in checks if x["status"] in {"PASS","CAUTION","RISK"}]
    elif category=="market": checks=result["classification"].get("market_checks",[])
    elif category=="holders":
        checks=[x for x in checks if any(k in x.get("label", "").lower() for k in ("holder", "concentration", "lp", "top"))]
    elif category=="risk": checks=[x for x in checks if x["status"]=="RISK"]
    elif category=="missing": checks=[x for x in checks if x["status"]=="UNKNOWN"]
    title={"security":"🔐 SECURITY","market":"💰 MARKET","holders":"👛 HOLDERS","risk":"🔴 RISK INDICATORS","missing":"⚪ UNAVAILABLE","ai":"🧠 AI CHECKS"}.get(category,"📋 FULL REPORT")
    lines=[f"<b>{title}</b>",""]
    if not checks: lines.append("No items are available in this section.")
    for x in checks:
        lines.append(f"{verdict_icon(x['status'])} <b>{x['label']}</b>")
        lines.append(f"   {x.get('detail') or 'Not available'}")
    return "\n".join(lines)


def network_keyboard():
    kb=types.InlineKeyboardMarkup(row_width=2)
    kb.row(types.InlineKeyboardButton("🟡 BNB",callback_data="net:bsc"),types.InlineKeyboardButton("♦️ Ethereum",callback_data="net:ethereum"))
    kb.row(types.InlineKeyboardButton("🔵 Base",callback_data="net:base"),types.InlineKeyboardButton("🟣 Solana",callback_data="net:solana"))
    kb.row(types.InlineKeyboardButton("⚫ Hyperliquid",callback_data="net:hyperliquid"),types.InlineKeyboardButton("🔷 Sui",callback_data="net:sui"))
    kb.row(types.InlineKeyboardButton("🟢 Arc",callback_data="net:arc"),types.InlineKeyboardButton("🟥 Robinhood",callback_data="net:robinhood"))
    return kb


def keyboard(result=None):
    kb=types.InlineKeyboardMarkup(row_width=2)
    kb.row(types.InlineKeyboardButton("🔐 Security",callback_data="v:security"),types.InlineKeyboardButton("💰 Market",callback_data="v:market"))
    kb.row(types.InlineKeyboardButton("👛 Holders",callback_data="v:holders"),types.InlineKeyboardButton("📊 Comparison",callback_data="v:comparison"))
    kb.row(types.InlineKeyboardButton("🧠 AI Checks",callback_data="v:ai"),types.InlineKeyboardButton("🔴 Risk",callback_data="v:risk"))
    kb.row(types.InlineKeyboardButton("📋 Full Report",callback_data="v:full"),types.InlineKeyboardButton("🔍 Analyze Another",callback_data="menu:analyse"))
    kb.row(types.InlineKeyboardButton("🌐 Network",callback_data="net:choose"))
    return kb


def links_keyboard(result):
    kb=types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("🌐 MARSOF Website",url="https://marsof.ct.ws/?i=1"))
    kb.add(types.InlineKeyboardButton("𝕏 X / Twitter",url="https://x.com/MARSOF_AI"))
    kb.add(types.InlineKeyboardButton("✈️ Telegram",url="https://t.me/marsofai"))
    pair=(result.get("market",{}).get("data",{}).get("pair") or {})
    if pair.get("url"): kb.add(types.InlineKeyboardButton("📈 Verified DEX Pair",url=pair["url"]))
    return kb


def main_keyboard():
    kb=types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("🌐 Website",url="https://marsof.ct.ws/?i=1"),
        types.InlineKeyboardButton("𝕏 X / Twitter",url="https://x.com/MARSOF_AI"),
    )
    kb.row(types.InlineKeyboardButton("🔍 Analyze Token",callback_data="menu:analyse"))
    kb.row(types.InlineKeyboardButton("✈️ Telegram",url="https://t.me/marsofai"))
    return kb


def build_start():
    return "<b>MARSOF AI</b>\n<b>Advanced AI-Assisted Cryptocurrency Token Analysis</b>\n\nUse <b>Analyze Token</b> to start a scan.\n\n🟢 PASS = no detected risk indicator in the completed test\n🟠 CAUTION = attention required\n🔴 RISK = risk indicator detected\n🔵 DATA = verified measurement\n⚪ UNAVAILABLE = could not be verified\n\nMissing data is never converted to zero."


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(title="MARSOF AI", version="6.0.0")


@app.get("/")
def root():
    return {
        "status": "running",
        "service": "MARSOF AI",
        "version": "6.0.0",
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
        with engine.begin() as conn:
            # Additive migration for older Step-1/V2 databases.
            token_columns = {c["name"] for c in inspector.get_columns("tokens")} if "tokens" in tables else set()
            token_additions = {
                "name": "VARCHAR(200)",
                "last_scanned": "TIMESTAMP",
                "scan_count": "INTEGER DEFAULT 0",
            }
            for col, typ in token_additions.items():
                if col not in token_columns:
                    conn.execute(text(f"ALTER TABLE tokens ADD COLUMN {col} {typ}"))
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
        bot._marsof_ui=getattr(bot,"_marsof_ui",{})
        try:
            prev=bot._marsof_ui.get(message.chat.id)
            if prev: bot.delete_message(message.chat.id,prev.get("message_id"))
        except Exception: pass
        sent=bot.send_message(message.chat.id,build_start(),parse_mode="HTML",reply_markup=main_keyboard())
        bot._marsof_ui[message.chat.id]={"message_id":sent.message_id,"network":None,"result":None}

    @bot.message_handler(content_types=["text"])
    def text_handler(message):
        if not message.text or message.text.startswith("/"): return
        state=getattr(bot,"_marsof_ui",{}).get(message.chat.id,{})
        network=state.get("network")
        if not network:
            bot.reply_to(message,"Please select a network first.",reply_markup=network_keyboard()); return
        address=message.text.strip()
        if not is_valid_for_network(address,network):
            bot.reply_to(message,f"⚠️ Invalid {network_label(network)} token address format.")
            return
        try:
            bot.send_chat_action(message.chat.id,"typing")
            result=run_analysis(address,network)
            if not result:
                bot.reply_to(message,"⚠️ The token could not be analyzed with verified data. No values were fabricated."); return
            try: bot.delete_message(message.chat.id,state.get("message_id"))
            except Exception: pass
            sent=bot.send_message(message.chat.id,build_home(result),parse_mode="HTML",reply_markup=keyboard(result))
            bot._marsof_ui[message.chat.id]={"message_id":sent.message_id,"network":network,"result":result}
        except requests.RequestException as exc:
            print(f"❌ data source error: {exc}")
            bot.reply_to(message,"⚠️ A live data source is temporarily unavailable. No result was fabricated.")
        except Exception as exc:
            print(f"❌ analysis error: {exc}")
            bot.reply_to(message,"⚠️ Technical analysis error. No result was fabricated.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("menu:"))
    def menu_callbacks(call):
        try:
            action=call.data.split(":",1)[1]
            if action=="analyse":
                bot._marsof_ui=getattr(bot,"_marsof_ui",{})
                state=bot._marsof_ui.get(call.message.chat.id,{})
                state["network"]=None
                state["result"]=None
                state["message_id"]=call.message.message_id
                bot._marsof_ui[call.message.chat.id]=state
                bot.edit_message_text(
                    "<b>Analyze Token</b>\n\nSelect the blockchain network first, then send the token address.",
                    call.message.chat.id,
                    call.message.message_id,
                    parse_mode="HTML",
                    reply_markup=network_keyboard(),
                )
            bot.answer_callback_query(call.id)
        except Exception as exc:
            print(f"❌ menu callback: {exc}")
            bot.answer_callback_query(call.id,"Unable to open token analysis.")


    @bot.callback_query_handler(func=lambda call: call.data.startswith("net:"))
    def network_callbacks(call):
        try:
            action=call.data.split(":",1)[1]
            if action=="choose":
                bot.edit_message_text("<b>Select network</b>\n\nThen send the token address.",call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=network_keyboard())
            else:
                bot._marsof_ui=getattr(bot,"_marsof_ui",{})
                state=bot._marsof_ui.get(call.message.chat.id,{"message_id":call.message.message_id})
                state["network"]=action; state["result"]=None
                bot._marsof_ui[call.message.chat.id]=state
                bot.edit_message_text(f"<b>{network_label(action)}</b> selected.\n\nSend the token address to analyze.",call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🌐 Change Network",callback_data="net:choose")))
            bot.answer_callback_query(call.id)
        except Exception as exc:
            print(f"❌ network callback: {exc}"); bot.answer_callback_query(call.id,"Unable to update network.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("v:"))
    def callbacks(call):
        try:
            state=getattr(bot,"_marsof_ui",{}).get(call.message.chat.id)
            if not state or not state.get("result"):
                bot.answer_callback_query(call.id,"Run a token analysis first."); return
            action=call.data.split(":",1)[1]
            result=state["result"]
            if action=="home": text=build_home(result); markup=keyboard(result)
            elif action=="links": text="<b>🔗 PROJECT LINKS</b>\n\nOfficial MARSOF links and the verified DEX pair from this scan."; markup=links_keyboard(result)
            elif action=="comparison": text=comparison_text(result); markup=keyboard(result)
            else: text=section_text(result,action); markup=keyboard(result)
            bot.edit_message_text(text,call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=markup)
            bot.answer_callback_query(call.id)
        except Exception as exc:
            print(f"❌ callback error: {exc}"); bot.answer_callback_query(call.id,"Unable to update the interface.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))