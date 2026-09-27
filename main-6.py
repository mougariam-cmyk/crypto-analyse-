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
# MARSOF AI — FINAL INTEGRATED BUILD v7.0
# Multi-network, multi-source, holder classification, internal scoring, history.
# Missing source values ALWAYS remain None / "Not available".
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
WEBHOOK_URL = "https://crypto-analyse-bot-z7o0.onrender.com/webhook"
DEFAULT_CHAIN = "bsc"
NETWORKS = {
    "bsc": {"name": "BNB Smart Chain", "chain_id": "56", "dex_chain": "bsc", "kind": "evm", "rpc": "https://bsc-dataseed.bnbchain.org", "rpc_fallbacks": ["https://bsc-dataseed1.bnbchain.org", "https://bsc-dataseed2.bnbchain.org", "https://bsc-dataseed3.bnbchain.org"], "explorer": "https://bscscan.com"},
    "solana": {"name": "Solana", "chain_id": "solana", "dex_chain": "solana", "kind": "solana", "rpc": "https://api.mainnet-beta.solana.com", "explorer": "https://solscan.io"},
    "sui": {"name": "Sui", "chain_id": "sui", "dex_chain": "sui", "kind": "sui", "rpc": "https://fullnode.mainnet.sui.io:443", "explorer": "https://suiscan.xyz/mainnet"},
    "arc": {"name": "Arc", "chain_id": "5042", "dex_chain": "arc", "kind": "evm", "rpc": "https://rpc.mainnet.arc.io", "explorer": "https://explorer.arc.io"},
    "robinhood": {"name": "Robinhood Chain", "chain_id": "4663", "dex_chain": "robinhood", "kind": "evm", "rpc": "https://rpc.mainnet.chain.robinhood.com", "explorer": "https://robinhoodchain.blockscout.com"},
    "base": {"name": "Base", "chain_id": "8453", "dex_chain": "base", "kind": "evm", "rpc": "https://mainnet.base.org", "explorer": "https://basescan.org"},
    "ethereum": {"name": "Ethereum", "chain_id": "1", "dex_chain": "ethereum", "kind": "evm", "rpc": "https://cloudflare-eth.com", "explorer": "https://etherscan.io"},
    "hyperliquid": {"name": "Hyperliquid", "chain_id": "999", "dex_chain": "hyperliquid", "kind": "evm", "rpc": "https://rpc.hyperliquid.xyz/evm", "explorer": "https://hyperevmscan.io"},
}
GO_PLUS_BASE = "https://api.gopluslabs.io/api/v1/token_security"
GO_PLUS_SOLANA = "https://api.gopluslabs.io/api/v1/solana/token_security"
GO_PLUS_SUI = "https://api.gopluslabs.io/api/v1/sui/token_security"
GECKO_BASE = "https://api.geckoterminal.com/api/v2"
GECKO_NETWORKS = {
    "bsc":"bsc", "ethereum":"eth", "base":"base", "solana":"solana",
    "hyperliquid":"hyperevm", "sui":"sui", "arc":"arc", "robinhood":"robinhood"
}

# Internal model only: never displayed to end users.
SCORING_WEIGHTS = {
    "technical_security": 0.22, "liquidity_lock": 0.18,
    "holder_concentration": 0.18, "liquidity_size": 0.12,
    "holder_count": 0.08, "activity": 0.08, "price_trend": 0.07, "contract_age": 0.07,
}
BURN_ADDRESSES = {
    "0x0000000000000000000000000000000000000000",
    "0x000000000000000000000000000000000000dead",
}
NON_INVESTOR_MARKERS = (
    "burn", "dead", "pancake", "uniswap", "sushiswap", "raydium", "orca",
    "liquidity", "pair", "pool", "pinklock", "team.finance", "unicrypt",
    "lock", "locker", "vesting", "staking", "router", "factory", "multisig", "system",
)
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
# SOURCE FETCHERS / FIELD-LEVEL FALLBACKS
# ============================================================

def _safe_get(url, **kwargs):
    try:
        r = requests.get(url, timeout=kwargs.pop("timeout", 20), **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        print(f"ℹ️ source unavailable: {url} :: {exc}")
        return None


def _record(data, field, value, source):
    if value is not None and value != "":
        data[field] = value
        data.setdefault("sources", {})[field] = source


def _merge_dict(base, incoming, source):
    if not isinstance(incoming, dict):
        return base
    for k, v in incoming.items():
        if v is not None and v != "" and base.get(k) in (None, ""):
            base[k] = v
            base.setdefault("sources", {})[k] = source
    return base


def fetch_goplus(address, network):
    cfg = NETWORKS.get(network)
    if not cfg or cfg.get("kind") != "evm":
        return None
    payload = _safe_get(f"{GO_PLUS_BASE}/{cfg['chain_id']}", params={"contract_addresses": address}, timeout=25)
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, dict): return None
    target = normalize_address(address)
    for returned_address, data in result.items():
        if normalize_address(returned_address) == target and isinstance(data, dict):
            out = dict(data); out["sources"] = {k:"GoPlus" for k,v in out.items() if v not in (None,"")}
            return out
    return None


def fetch_goplus_solana(address):
    payload = _safe_get(GO_PLUS_SOLANA, params={"contract_addresses": address}, timeout=25)
    result = payload.get("result") if isinstance(payload, dict) else None
    if isinstance(result, dict):
        for k,v in result.items():
            if normalize_address(k) == normalize_address(address) and isinstance(v, dict):
                v=dict(v); v["sources"]={x:"GoPlus Solana" for x in v if v[x] not in (None,"")}; return v
    return None


def fetch_goplus_sui(address):
    payload = _safe_get(GO_PLUS_SUI, params={"contract_addresses": address}, timeout=25)
    result = payload.get("result") if isinstance(payload, dict) else None
    if isinstance(result, dict):
        for k,v in result.items():
            if normalize_address(k) == normalize_address(address) and isinstance(v, dict):
                v=dict(v); v["sources"]={x:"GoPlus Sui" for x in v if v[x] not in (None,"")}; return v
    return None


def fetch_dexscreener(address, network):
    cfg = NETWORKS[network]
    payload = _safe_get(DEXSCREENER_URL.format(address=address.strip()), timeout=25)
    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if not isinstance(pairs, list): return []
    target = address.strip().lower(); matches=[]
    for pair in pairs:
        if not isinstance(pair, dict) or str(pair.get("chainId") or "").lower()!=cfg["dex_chain"]: continue
        base=str((pair.get("baseToken") or {}).get("address") or "").lower(); quote=str((pair.get("quoteToken") or {}).get("address") or "").lower()
        if target in {base,quote}: matches.append(pair)
    return sorted(matches,key=lambda p:number((p.get("liquidity") or {}).get("usd")) or -1,reverse=True)


def fetch_gecko_market(address, network):
    slug=GECKO_NETWORKS.get(network)
    if not slug: return []
    payload=_safe_get(f"{GECKO_BASE}/networks/{slug}/tokens/{address}/pools", params={"include":"base_token,quote_token","page":"1"}, timeout=25)
    rows=payload.get("data") if isinstance(payload,dict) else None
    out=[]
    if not isinstance(rows,list): return out
    for row in rows:
        a=(row.get("attributes") or {}) if isinstance(row,dict) else {}
        rel=(row.get("relationships") or {}) if isinstance(row,dict) else {}
        out.append({"source":"GeckoTerminal","pool_id":row.get("id"),"name":a.get("name"),"priceUsd":number(a.get("base_token_price_usd")),"fdv":number(a.get("fdv_usd")),"marketCap":number(a.get("market_cap_usd")),"liquidity":{"usd":number(a.get("reserve_in_usd"))},"volume":{"h24":number((a.get("volume_usd") or {}).get("h24"))},"priceChange":{"h24":number((a.get("price_change_percentage") or {}).get("h24"))}})
    return sorted(out,key=lambda x:number((x.get("liquidity") or {}).get("usd")) or -1,reverse=True)


def fetch_gecko_history(pool_id, network):
    if not pool_id: return []
    slug=GECKO_NETWORKS.get(network)
    if not slug: return []
    # GeckoTerminal supplies external OHLCV history; MARSOF never treats its own snapshots as price history.
    try:
        endpoint=f"{GECKO_BASE}/networks/{slug}/pools/{pool_id}/ohlcv/day"
        payload=_safe_get(endpoint,params={"aggregate":"1","limit":"200"},timeout=25)
        rows=((payload or {}).get("data") or {}).get("attributes",{}).get("ohlcv_list") if isinstance(payload,dict) else None
        if not isinstance(rows,list): return []
        return [{"timestamp":r[0],"open":number(r[1]),"high":number(r[2]),"low":number(r[3]),"close":number(r[4]),"volume":number(r[5])} for r in rows if isinstance(r,list) and len(r)>=6]
    except Exception as exc:
        print(f"ℹ️ GeckoTerminal history unavailable: {exc}")
        return []


def evm_rpc_call(rpc, method, params):
    try:
        r = requests.post(
            rpc,
            json={"jsonrpc": "2.0", "id": 1, "method": method, "params": params},
            timeout=12,
        )
        r.raise_for_status()
        obj = r.json()
        if isinstance(obj, dict) and obj.get("error"):
            return None
        return obj.get("result") if isinstance(obj, dict) else None
    except Exception as exc:
        print(f"ℹ️ RPC {method}: {exc}")
        return None


def rpc_call_network(network, method, params):
    """Try the configured RPC and its fallbacks. A single RPC outage must not
    make a real token look like an unsupported network."""
    cfg = NETWORKS.get(network) or {}
    urls = [cfg.get("rpc")] + list(cfg.get("rpc_fallbacks") or [])
    for url in [u for u in urls if u]:
        result = evm_rpc_call(url, method, params)
        if result is not None:
            return result
    return None


def _decode_abi_string(hexdata):
    if not isinstance(hexdata,str) or not hexdata.startswith("0x") or len(hexdata)<2:return None
    try:
        raw=bytes.fromhex(hexdata[2:])
        if len(raw)>=64:
            off=int.from_bytes(raw[:32],"big")
            if off+32<=len(raw):
                ln=int.from_bytes(raw[off:off+32],"big"); body=raw[off+32:off+32+ln]; return body.decode("utf-8",errors="replace") or None
        return raw.rstrip(b"\x00").decode("utf-8",errors="replace") or None
    except Exception:return None


def _decode_uint(hexdata):
    try:return int(hexdata,16) if hexdata and hexdata!="0x" else None
    except Exception:return None


def fetch_evm_metadata(address, network):
    cfg=NETWORKS[network]; out={}
    code=rpc_call_network(network,"eth_getCode",[address,"latest"])
    out["contract_exists"]=bool(code and code!="0x") if code is not None else None
    for field,selector,decoder in [("token_name","0x06fdde03",_decode_abi_string),("token_symbol","0x95d89b41",_decode_abi_string),("decimals","0x313ce567",_decode_uint),("total_supply","0x18160ddd",_decode_uint)]:
        raw=rpc_call_network(network,"eth_call",[{"to":address,"data":selector},"latest"])
        _record(out,field,decoder(raw) if raw is not None else None,"Blockchain RPC")
    return out


def solana_rpc(method,params):
    return evm_rpc_call(NETWORKS["solana"]["rpc"],method,params)


def fetch_solana_metadata(address):
    out={}
    try:
        acct=solana_rpc("getAccountInfo",[address,{"encoding":"jsonParsed"}]); val=(acct or {}).get("value") if isinstance(acct,dict) else None; info=(((val or {}).get("data") or {}).get("parsed") or {}).get("info") or {}
        _record(out,"mint_authority",info.get("mintAuthority"),"Solana RPC"); _record(out,"freeze_authority",info.get("freezeAuthority"),"Solana RPC"); _record(out,"decimals",info.get("decimals"),"Solana RPC"); _record(out,"supply",number(info.get("supply")),"Solana RPC"); _record(out,"program",(val or {}).get("owner"),"Solana RPC")
    except Exception as exc: print(f"ℹ️ Solana mint RPC: {exc}")
    try:
        largest=solana_rpc("getTokenLargestAccounts",[address]); _record(out,"largest_accounts",((largest or {}).get("value") if isinstance(largest,dict) else []),"Solana RPC")
    except Exception: pass
    return out


def fetch_sui_metadata(address):
    out={}
    try:
        r=requests.post(NETWORKS["sui"]["rpc"],json={"jsonrpc":"2.0","id":1,"method":"suix_getCoinMetadata","params":[address]},timeout=20); r.raise_for_status(); obj=r.json(); val=obj.get("result") or {}; _record(out,"token_name",val.get("name"),"Sui RPC"); _record(out,"token_symbol",val.get("symbol"),"Sui RPC"); _record(out,"decimals",val.get("decimals"),"Sui RPC"); _record(out,"total_supply",number((val.get("supply") or {}).get("value")),"Sui RPC")
    except Exception as exc: print(f"ℹ️ Sui RPC: {exc}")
    return out


def _expected_chain_hex(network):
    """Return the canonical EVM chain id as a JSON-RPC hex quantity."""
    try:
        return hex(int(NETWORKS[network]["chain_id"]))
    except (KeyError, TypeError, ValueError):
        return None


def verify_evm_network(address, network, dexscreener_matches=None):
    """Verify an EVM address against the actual target chain.

    Priority is RPC proof: the endpoint must report the expected chain id and
    eth_getCode must return deployed bytecode. DEX Screener/GoPlus are only
    secondary evidence when RPC is temporarily unavailable. A 0x-shaped address
    is never accepted by itself.
    """
    cfg = NETWORKS.get(network) or {}
    if cfg.get("kind") != "evm" or not is_valid_evm_address(address):
        return False

    expected = _expected_chain_hex(network)

    # Strong proof: RPC chain identity + contract bytecode on that same RPC.
    try:
        chain_id = rpc_call_network(network, "eth_chainId", [])
        if chain_id is not None:
            if expected is not None and str(chain_id).lower() != expected.lower():
                print(f"ℹ️ chain mismatch for {network}: RPC={chain_id}, expected={expected}")
                return False
            code = rpc_call_network(network, "eth_getCode", [address, "latest"])
            if isinstance(code, str) and code.lower() not in {"0x", "0x0", "0x00"}:
                return True
            # A responding RPC with the right chain but no code is strong negative
            # evidence for this address on this network; do not guess from other data.
            if code is not None:
                return False
    except Exception as exc:
        print(f"ℹ️ RPC verification skipped for {network}: {exc}")

    # Secondary proof only when the RPC endpoint is unavailable/non-responsive.
    matches = dexscreener_matches if dexscreener_matches is not None else fetch_dexscreener(address, network)
    if matches:
        return True
    gp = fetch_goplus(address, network)
    return bool(gp)


def detect_networks(address):
    a = address.strip()
    detected = []

    # Solana: RPC account existence is required; syntax alone is never enough.
    if is_valid_solana_address(a):
        try:
            x = solana_rpc("getAccountInfo", [a, {"encoding": "base64"}])
            if isinstance(x, dict) and x.get("value") is not None:
                detected.append("solana")
        except Exception as exc:
            print(f"ℹ️ Solana network verification skipped: {exc}")

    # Sui: verify that the address resolves as a coin object.
    if is_valid_sui_address(a):
        try:
            r = requests.post(
                NETWORKS["sui"]["rpc"],
                json={"jsonrpc":"2.0","id":1,"method":"suix_getCoinMetadata","params":[a]},
                timeout=15,
            )
            if r.ok and isinstance(r.json().get("result"), dict):
                detected.append("sui")
        except Exception as exc:
            print(f"ℹ️ Sui network verification skipped: {exc}")

    # EVM: verify each chain independently. DEX discovery is cached only as
    # secondary evidence so a DEX outage cannot erase a valid RPC verification.
    if is_valid_evm_address(a):
        dex_cache = {}
        for n, cfg in NETWORKS.items():
            if cfg.get("kind") != "evm":
                continue
            try:
                dex_cache[n] = fetch_dexscreener(a, n)
            except Exception as exc:
                print(f"ℹ️ DEX verification skipped for {n}: {exc}")
                dex_cache[n] = []

        for n, cfg in NETWORKS.items():
            if cfg.get("kind") != "evm":
                continue
            try:
                if verify_evm_network(a, n, dex_cache.get(n)):
                    detected.append(n)
            except Exception as exc:
                print(f"ℹ️ network verification skipped for {n}: {exc}")

    return list(dict.fromkeys(detected))

def enrich_security(address, network, primary):
    data=dict(primary or {}); data.setdefault("sources",{})
    if network=="solana":
        rpc=fetch_solana_metadata(address); _merge_dict(data,rpc,"Solana RPC")
        gp=fetch_goplus_solana(address); _merge_dict(data,gp,"GoPlus Solana")
    elif network=="sui":
        rpc=fetch_sui_metadata(address); _merge_dict(data,rpc,"Sui RPC")
        gp=fetch_goplus_sui(address); _merge_dict(data,gp,"GoPlus Sui")
    else:
        rpc=fetch_evm_metadata(address,network); _merge_dict(data,rpc,"Blockchain RPC")
    return data


def enrich_market(address,network,pairs):
    if pairs:return pairs
    return fetch_gecko_market(address,network)


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

def normalize_holder_pct(value):
    x=number(value)
    if x is not None and x<=1:x*=100
    return x


def classify_holder(holder, network, pair_addresses=None, lp_addresses=None):
    h=dict(holder or {}); addr=normalize_address(h.get("address")); tag=str(h.get("tag") or "").lower();
    pair_addresses={normalize_address(x) for x in (pair_addresses or []) if x}; lp_addresses={normalize_address(x) for x in (lp_addresses or []) if x}
    if addr in BURN_ADDRESSES or "burn" in tag or "dead" in tag:return "NON-INVESTOR", "burn/dead"
    if boolean(h.get("is_locked")) is True:return "NON-INVESTOR", "locked"
    if addr in pair_addresses or addr in lp_addresses:return "NON-INVESTOR", "LP/pool"
    if any(m in tag for m in NON_INVESTOR_MARKERS):return "NON-INVESTOR", tag
    is_contract=boolean(h.get("is_contract"))
    if is_contract is False:return "INVESTOR", "verified EOA"
    # A contract without an identifying tag is not silently treated as an investor.
    if is_contract is True:return "UNCLASSIFIED", "contract without verified role"
    return "UNCLASSIFIED", "address role not verified"


def holder_analysis(data, market):
    holders=data.get("holders") if isinstance(data.get("holders"),list) else []
    pair=market.get("pair") or {}; pair_addresses=[]
    for p in [pair]:
        if p.get("pairAddress"):pair_addresses.append(p.get("pairAddress"))
    for d in data.get("dex") or []:
        if isinstance(d,dict) and d.get("pair"):pair_addresses.append(d.get("pair"))
    lp_addresses=[h.get("address") for h in (data.get("lp_holders") or []) if isinstance(h,dict)]
    rows=[]; raw=0.0; investor=0.0; excluded=0.0; unclassified=0.0; excluded_count=0; investor_count=0; unclassified_count=0
    for h in holders[:10]:
        pct=normalize_holder_pct(h.get("percent")); raw += pct or 0
        cls,reason=classify_holder(h,"",pair_addresses,lp_addresses)
        if cls=="INVESTOR": investor += pct or 0; investor_count+=1
        elif cls=="NON-INVESTOR": excluded += pct or 0; excluded_count+=1
        else: unclassified += pct or 0; unclassified_count+=1
        rows.append({**h,"percent_normalized":pct,"classification":cls,"classification_reason":reason})
    adjusted=investor if holders and all(r.get("percent_normalized") is not None for r in rows) else None
    return {"rows":rows,"raw_top10":raw if holders else None,"adjusted_investor_concentration":adjusted,"excluded_non_investor_share":excluded if holders else None,"unclassified_share":unclassified if holders else None,"excluded_holder_count":excluded_count,"investor_holder_count":investor_count,"unclassified_holder_count":unclassified_count}


def _factor_technical(data):
    critical=[("is_honeypot",True),("cannot_sell_all",True),("privilege_withdraw",True),("owner_change_balance",True),("hidden_owner",True),("blacklist",True),("transfer_pausable",True)]
    for k,bad in critical:
        if boolean(data.get(k)) is True:return 0.0
    vals=[]
    for k in ["is_honeypot","is_mintable","is_proxy","hidden_owner","privilege_withdraw","blacklist","cannot_buy","cannot_sell_all","owner_change_balance","transfer_pausable"]:
        v=boolean(data.get(k));
        if v is not None: vals.append(0 if v else 10)
    for k in ["buy_tax","sell_tax","transfer_tax"]:
        v=number(data.get(k));
        if v is not None:
            p=v*100
            vals.append(10 if p<=5 else 8 if p<=10 else 0)
    return sum(vals)/len(vals) if vals else None


def _factor_lock(data):
    holders=data.get("holders") or []; locked=0.0; total=0.0
    seen=False
    for h in holders:
        pct=normalize_holder_pct(h.get("percent"));
        if pct is not None: total+=pct; seen=True; locked += pct if boolean(h.get("is_locked")) is True else 0
    if seen and total>0:return min(10.0,max(0.0,locked/total*10))
    # Explicit LP/lock fields can provide evidence without holders.
    for k in ["liquidity_locked_percent","lp_locked_percent","locked_percent"]:
        v=number(data.get(k));
        if v is not None:
            if v<=1:v*=100
            return min(10.0,max(0.0,v/10))
    return None


def _factor_concentration(ha):
    v=ha.get("adjusted_investor_concentration")
    if v is None:return None
    return 10 if v<20 else 8 if v<=30 else 5 if v<=45 else 0


def _factor_liquidity(m):
    v=m.get("liquidity")
    if v is None:return None
    return 10 if v>100000 else 8 if v>=50000 else 5 if v>=20000 else 0


def _factor_holders(data):
    v=number(data.get("holder_count"));
    if v is None:return None
    return 10 if v>20000 else 8 if v>=10000 else 5 if v>=3000 else 0


def _factor_activity(m):
    v=m.get("vol_mc")
    if v is None:return None
    return 10 if v>30 else 8 if v>=15 else 5 if v>=5 else 0


def _factor_trend(m):
    v=m.get("price_change_24h")
    if v is None:return None
    return 10 if v>5 else 8 if v>=-2 else 5 if v>=-15 else 0


def _factor_age(data,m):
    v=number(data.get("contract_age_days")) or number(m.get("pair_age_days"))
    if v is None:return None
    return 10 if v>90 else 8 if v>=30 else 5 if v>=7 else 0


def calculate_internal_score(data,market,ha):
    factors={"technical_security":_factor_technical(data),"liquidity_lock":_factor_lock(data),"holder_concentration":_factor_concentration(ha),"liquidity_size":_factor_liquidity(market),"holder_count":_factor_holders(data),"activity":_factor_activity(market),"price_trend":_factor_trend(market),"contract_age":_factor_age(data,market)}
    weighted=0.0; weight=0.0
    for k,v in factors.items():
        if v is not None: weighted+=v*SCORING_WEIGHTS[k]; weight+=SCORING_WEIGHTS[k]
    return {"factors":factors,"score":(weighted/weight*10 if weight else None),"coverage":weight}


def build_security(data, market=None):
    checks=[]; add=lambda label,value,status="DATA",detail=None,source=None: checks.append(metric(label,value,status,detail,source or data.get("sources",{}).get(label,"GoPlus")))
    fields=[("Token Name","token_name"),("Token Symbol","token_symbol"),("Contract Name","contract_name"),("Holder Count","holder_count"),("Total Supply","total_supply"),("Decimals","decimals")]
    for label,key in fields:
        v=number(data.get(key)) if key in {"holder_count","total_supply","decimals"} else data.get(key); add(label,v,"DATA" if v is not None else "UNKNOWN",fmt_num(v) if isinstance(v,(int,float)) else (str(v) if v is not None else "Not available"))
    for key,label in [("is_honeypot","Honeypot"),("is_open_source","Source Code Open"),("is_mintable","Mintable"),("is_proxy","Proxy"),("hidden_owner","Hidden Owner"),("privilege_withdraw","Privilege Withdraw"),("blacklist","Blacklist Function"),("cannot_buy","Cannot Buy"),("cannot_sell_all","Cannot Sell All"),("owner_change_balance","Owner Can Change Balance"),("transfer_pausable","Transfer Pausable"),("approval_abuse","Approval Abuse"),("selfdestruct","Self Destruct"),("external_call","External Call")]:
        v=boolean(data.get(key)); status="UNKNOWN" if v is None else ("RISK" if v else "PASS"); add(label,v,status,fmt_bool(v))
    for key,label in [("buy_tax","Buy Tax"),("sell_tax","Sell Tax"),("transfer_tax","Transfer Tax")]:
        raw=number(data.get(key)); pct=raw*100 if raw is not None else None; add(label,pct,tax_status(pct),fmt_pct(pct))
    for key,label in [("owner_address","Owner Address"),("creator_address","Creator / Deployer"),("owner_balance","Owner Balance"),("owner_percent","Owner Token %"),("creator_balance","Creator Balance"),("creator_percent","Creator Token %")]:
        v=data.get(key); v=number(v) if key not in {"owner_address","creator_address"} else v; add(label,v,"DATA" if v is not None else "UNKNOWN",fmt_num(v) if isinstance(v,(int,float)) else (str(v) if v is not None else "Not available"))
    deployed=parse_timestamp(data.get("deployed_time")); age=age_days(deployed); data["contract_age_days"]=age; add("Contract Deployed",deployed.isoformat() if deployed else None,"DATA" if deployed else "UNKNOWN",deployed.strftime("%Y-%m-%d %H:%M UTC") if deployed else "Not available"); add("Contract Age (days)",age,"DATA" if age is not None else "UNKNOWN",f"{age:.2f} days" if age is not None else "Not available")
    ha=holder_analysis(data,market or {"pair":None}); data["holder_analysis"]=ha
    add("Raw Top 10 Concentration",ha["raw_top10"],"DATA" if ha["raw_top10"] is not None else "UNKNOWN",fmt_pct(ha["raw_top10"])); add("Adjusted Investor Concentration",ha["adjusted_investor_concentration"],"DATA" if ha["adjusted_investor_concentration"] is not None else "UNKNOWN",fmt_pct(ha["adjusted_investor_concentration"])); add("Excluded Non-Investor Share",ha["excluded_non_investor_share"],"DATA" if ha["excluded_non_investor_share"] is not None else "UNKNOWN",fmt_pct(ha["excluded_non_investor_share"])); add("Unclassified Share",ha["unclassified_share"],"DATA" if ha["unclassified_share"] is not None else "UNKNOWN",fmt_pct(ha["unclassified_share"])); add("Excluded Holder Count",ha["excluded_holder_count"],"DATA",str(ha["excluded_holder_count"])); add("Unclassified Holder Count",ha["unclassified_holder_count"],"DATA",str(ha["unclassified_holder_count"]))
    return checks


def classify_all(security_checks,market_checks,data=None,market=None):
    all_checks=security_checks+market_checks; tested=[x for x in all_checks if x["status"] in {"PASS","CAUTION","RISK"}]; missing=[x["label"] for x in all_checks if x["status"]=="UNKNOWN"]; available=[x for x in all_checks if x["status"]!="UNKNOWN"]
    p=sum(x["status"]=="PASS" for x in tested); c=sum(x["status"]=="CAUTION" for x in tested); r=sum(x["status"]=="RISK" for x in tested); coverage=(len(available)/len(all_checks)*100) if all_checks else 0
    ha=(data or {}).get("holder_analysis") or holder_analysis(data or {},market or {})
    internal=calculate_internal_score(data or {},market or {},ha)
    critical=any(x["status"]=="RISK" for x in security_checks)
    if ha.get("adjusted_investor_concentration") is not None and ha["adjusted_investor_concentration"]>45: critical=True
    verdict="HIGH RISK" if critical else "NO MAJOR RISK DETECTED"
    return {"all":all_checks,"security_checks":security_checks,"market_checks":market_checks,"pass":p,"caution":c,"risk":r,"data":sum(x["status"]=="DATA" for x in all_checks),"missing":missing,"coverage":coverage,"analysis_confidence":coverage,"verdict":verdict,"internal_score":internal}


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


def build_market(pair):
    if not pair:return {"checks":[],"data":{},"missing":["Market data"]}
    source=pair.get("source") or "DEX Screener"; checks=[]; add=lambda l,v,st="DATA",d=None:checks.append(metric(l,v,st,d,source))
    price=number(pair.get("priceUsd")); mc=number(pair.get("marketCap")); fdv=number(pair.get("fdv")); liq=number((pair.get("liquidity") or {}).get("usd")); price_native=number(pair.get("priceNative"))
    add("Price USD",price,"DATA" if price is not None else "UNKNOWN",fmt_money(price)); add("Price Native",price_native,"DATA" if price_native is not None else "UNKNOWN",fmt_num(price_native)); add("Market Cap",mc,"DATA" if mc is not None else "UNKNOWN",fmt_money(mc)); add("FDV",fdv,"DATA" if fdv is not None else "UNKNOWN",fmt_money(fdv)); add("Liquidity USD",liq,"DATA" if liq is not None else "UNKNOWN",fmt_money(liq)); add("DEX",pair.get("dexId") or pair.get("name"),"DATA" if pair.get("dexId") or pair.get("name") else "UNKNOWN",pair.get("dexId") or pair.get("name") or "Not available"); add("Pair Address",pair.get("pairAddress") or pair.get("pool_id"),"DATA" if pair.get("pairAddress") or pair.get("pool_id") else "UNKNOWN",pair.get("pairAddress") or pair.get("pool_id") or "Not available"); add("Base Token",(pair.get("baseToken") or {}).get("symbol") if isinstance(pair.get("baseToken"),dict) else None,"DATA" if pair.get("baseToken") else "UNKNOWN",str((pair.get("baseToken") or {}).get("symbol") or "Not available")); add("Quote Token",(pair.get("quoteToken") or {}).get("symbol") if isinstance(pair.get("quoteToken"),dict) else None,"DATA" if pair.get("quoteToken") else "UNKNOWN",str((pair.get("quoteToken") or {}).get("symbol") or "Not available"))
    created=parse_timestamp(pair.get("pairCreatedAt")); page=age_days(created); add("Pair Created",created.isoformat() if created else None,"DATA" if created else "UNKNOWN",created.strftime("%Y-%m-%d %H:%M UTC") if created else "Not available"); add("Pair Age (days)",page,"DATA" if page is not None else "UNKNOWN",f"{page:.2f} days" if page is not None else "Not available")
    periods={}
    for period in ["m5","h1","h6","h24"]:
        tx=(pair.get("txns") or {}).get(period) or {}; buys=integer(tx.get("buys")); sells=integer(tx.get("sells")); vol=number((pair.get("volume") or {}).get(period)); ch=number((pair.get("priceChange") or {}).get(period)); ratio=(buys/sells) if buys is not None and sells not in (None,0) else None; periods[period]={"buys":buys,"sells":sells,"volume":vol,"change":ch,"ratio":ratio,"tx":(buys+sells if buys is not None and sells is not None else None)}
        title={"m5":"5m","h1":"1h","h6":"6h","h24":"24h"}[period]
        add(f"{title} Volume",vol,"DATA" if vol is not None else "UNKNOWN",fmt_money(vol)); add(f"{title} Buys",buys,"DATA" if buys is not None else "UNKNOWN",str(buys) if buys is not None else "Not available"); add(f"{title} Sells",sells,"DATA" if sells is not None else "UNKNOWN",str(sells) if sells is not None else "Not available"); add(f"{title} Buy/Sell Ratio",ratio,"DATA" if ratio is not None else "UNKNOWN",fmt_ratio(ratio)); add(f"{title} Transactions",periods[period]["tx"],"DATA" if periods[period]["tx"] is not None else "UNKNOWN",str(periods[period]["tx"]) if periods[period]["tx"] is not None else "Not available"); add(f"{title} Price Change",ch,"DATA" if ch is not None else "UNKNOWN",fmt_pct(ch))
    liq_mc=liq/mc*100 if liq is not None and mc not in (None,0) else None; liq_fdv=liq/fdv*100 if liq is not None and fdv not in (None,0) else None; vol_liq=periods["h24"]["volume"]/liq if periods["h24"]["volume"] is not None and liq not in (None,0) else None; vol_mc=periods["h24"]["volume"]/mc*100 if periods["h24"]["volume"] is not None and mc not in (None,0) else None; fdv_mc=fdv/mc if fdv is not None and mc not in (None,0) else None
    add("Liquidity / Market Cap",liq_mc,"DATA" if liq_mc is not None else "UNKNOWN",fmt_pct(liq_mc)); add("Liquidity / FDV",liq_fdv,"DATA" if liq_fdv is not None else "UNKNOWN",fmt_pct(liq_fdv)); add("Volume / Liquidity",vol_liq,"DATA" if vol_liq is not None else "UNKNOWN",fmt_ratio(vol_liq)); add("24h Volume / Market Cap",vol_mc,"DATA" if vol_mc is not None else "UNKNOWN",fmt_pct(vol_mc)); add("FDV / Market Cap",fdv_mc,"DATA" if fdv_mc is not None else "UNKNOWN",fmt_ratio(fdv_mc))
    return {"checks":checks,"data":{"pair":pair,"price":price,"market_cap":mc,"fdv":fdv,"liquidity":liq,"price_native":price_native,"volume_24h":periods["h24"]["volume"],"buys_24h":periods["h24"]["buys"],"sells_24h":periods["h24"]["sells"],"price_change_24h":periods["h24"]["change"],"liq_mc":liq_mc,"liq_fdv":liq_fdv,"vol_liq":vol_liq,"vol_mc":vol_mc,"fdv_mc":fdv_mc,"pair_age_days":page,"periods":periods},"missing":[]}


def run_analysis(address, network):
    cfg=NETWORKS.get(network)
    if not cfg:return None
    primary=fetch_goplus(address,network) if cfg.get("kind")=="evm" else fetch_goplus_solana(address) if network=="solana" else fetch_goplus_sui(address)
    security_data=enrich_security(address,network,primary)
    pairs=fetch_dexscreener(address,network); market_pair=(pairs[0] if pairs else None)
    if market_pair is None:
        gecko=fetch_gecko_market(address,network); market_pair=gecko[0] if gecko else None
    market=build_market(market_pair)
    if market_pair:
        pool_id=market_pair.get("pool_id") or market_pair.get("pairAddress")
        market["data"]["external_history"]=fetch_gecko_history(pool_id,network) if pool_id else []
    security_checks=build_security(security_data,market["data"])
    ha=security_data.get("holder_analysis") or holder_analysis(security_data,market["data"]); security_data["holder_analysis"]=ha
    classification=classify_all(security_checks,market["checks"],security_data,market["data"])
    result={"address":normalize_address(address),"network":network,"network_name":network_label(network),"security":security_data,"market":market,"classification":classification,"sources":security_data.get("sources",{})}
    save_analysis(address,security_data,classification,market["data"],network)
    return result


# ============================================================
# UI
# ============================================================

def split_text(text_value, max_chars=3500):
    if not text_value:
        return [""]
    chunks=[]; current=""
    for line in str(text_value).split("\n"):
        candidate=line if not current else current+"\n"+line
        if len(candidate)<=max_chars:
            current=candidate
        else:
            if current: chunks.append(current)
            while len(line)>max_chars:
                chunks.append(line[:max_chars]); line=line[max_chars:]
            current=line
    if current or not chunks: chunks.append(current)
    return chunks


def verdict_icon(status): return {"PASS":"🟢","CAUTION":"🟠","RISK":"🔴","HIGH RISK":"🔴","NO MAJOR RISK DETECTED":"🟢","DATA":"🔵","UNKNOWN":"⚪"}.get(status,"🔵")

def html(s):
    return str(s).replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def build_home(result):
    c=result["classification"]; d=result["security"]; m=result["market"]["data"]; ha=d.get("holder_analysis") or {}
    return (f"<b>MARSOF AI</b>\n\n<b>{html(d.get('token_symbol') or 'Token')}</b> — {html(result['network_name'])}\n<code>{html(result['address'])}</code>\n\n{verdict_icon(c['verdict'])} <b>{html(c['verdict'])}</b>\n\n" f"📡 Data Coverage: <b>{c['coverage']:.1f}%</b>\n🔵 Data: <b>{c['data']}</b>   ⚪ Unavailable: <b>{len(c['missing'])}</b>\n\n💵 Price: <b>{fmt_money(m.get('price'))}</b>\n💧 Liquidity: <b>{fmt_money(m.get('liquidity'))}</b>\n💰 Market Cap: <b>{fmt_money(m.get('market_cap'))}</b>\n📈 24h Volume: <b>{fmt_money(m.get('volume_24h'))}</b>\n👛 Adjusted Investor Concentration: <b>{fmt_pct(ha.get('adjusted_investor_concentration'))}</b>\n\n<i>Missing data is never converted to zero. Internal scoring is not displayed.</i>")

def section_text(result, category):
    d=result["security"]; m=result["market"]["data"]; c=result["classification"]; ha=d.get("holder_analysis") or {}
    if category=="security": items=c["security_checks"]
    elif category=="market": items=c["market_checks"]
    elif category=="risk": items=[x for x in c["all"] if x["status"]=="RISK"]
    elif category=="data": items=[x for x in c["all"] if x["status"]=="DATA"]
    elif category=="missing": items=[x for x in c["all"] if x["status"]=="UNKNOWN"]
    elif category=="holders":
        lines=["<b>👛 HOLDERS</b>","",f"Holder Count: <b>{html(d.get('holder_count') or 'Not available')}</b>",f"Raw Top 10 Concentration: <b>{fmt_pct(ha.get('raw_top10'))}</b>",f"Adjusted Investor Concentration: <b>{fmt_pct(ha.get('adjusted_investor_concentration'))}</b>",f"Excluded Non-Investor Share: <b>{fmt_pct(ha.get('excluded_non_investor_share'))}</b>",f"Unclassified Share: <b>{fmt_pct(ha.get('unclassified_share'))}</b>",""]
        for i,h in enumerate(ha.get("rows",[])[:10],1):lines.append(f"{i}. <code>{html(h.get('address') or 'Not available')}</code> — {fmt_pct(h.get('percent_normalized'))} — <b>{h.get('classification')}</b>")
        return "\n".join(lines)
    elif category=="liquidity": return f"<b>💧 LIQUIDITY</b>\n\nCurrent: <b>{fmt_money(m.get('liquidity'))}</b>\nLiquidity / Market Cap: <b>{fmt_pct(m.get('liq_mc'))}</b>\nLiquidity / FDV: <b>{fmt_pct(m.get('liq_fdv'))}</b>\nVolume / Liquidity: <b>{fmt_ratio(m.get('vol_liq'))}</b>\n\nLP share is not treated as investor concentration by itself."
    elif category=="comparison": return comparison_text(result)
    elif category=="full":
        parts=[build_home(result),section_text(result,"security"),section_text(result,"market"),section_text(result,"holders"),section_text(result,"liquidity"),section_text(result,"risk")]; return "\n\n".join(parts)
    else: return build_home(result)
    lines=["<b>"+{"security":"🔐 SECURITY","market":"💰 MARKET","risk":"🔴 RISK","data":"🔵 DATA","missing":"⚪ UNAVAILABLE"}.get(category,"REPORT")+"</b>",""]
    if not items: lines.append("No items in this category.")
    for x in items: lines.append(f"{verdict_icon(x['status'])} <b>{html(x['label'])}</b>: {html(x['detail'])}\n   └ Source: {html(x.get('source') or 'Unknown')}")
    return "\n".join(lines)

def comparison_text(result):
    if not SessionLocal:return "<b>📊 COMPARISON</b>\n\nDatabase is not configured."
    s=SessionLocal()
    try:
        rows=(s.query(MarketSnapshot).filter(MarketSnapshot.chain_id==chain_id_for(result["network"]),MarketSnapshot.token_address==normalize_address(result["address"])).order_by(MarketSnapshot.snapshot_time.desc()).limit(2).all())
        if len(rows)<2:return "<b>📊 COMPARISON</b>\n\nNo previous scan is available yet."
        cur,prev=rows[0],rows[1]
        def ch(a,b):return (a-b)/b*100 if a is not None and b not in (None,0) else None
        def f(x):return "Not available" if x is None else f"{x:+.2f}%"
        hist=result.get("market",{}).get("data",{}).get("external_history") or []
        history_line=f"📚 External price history: <b>{len(hist)} daily points</b>" if hist else "📚 External price history: <b>Not available</b>"
        return "\n".join(["<b>📊 COMPARISON</b>","",f"💵 Price: {fmt_money(prev.price_usd)} → {fmt_money(cur.price_usd)} ({f(ch(cur.price_usd,prev.price_usd))})",f"💧 Liquidity: {fmt_money(prev.liquidity_usd)} → {fmt_money(cur.liquidity_usd)} ({f(ch(cur.liquidity_usd,prev.liquidity_usd))})",f"💰 Market Cap: {fmt_money(prev.market_cap_usd)} → {fmt_money(cur.market_cap_usd)} ({f(ch(cur.market_cap_usd,prev.market_cap_usd))})",f"📈 24h Volume: {fmt_money(prev.volume_24h_usd)} → {fmt_money(cur.volume_24h_usd)} ({f(ch(cur.volume_24h_usd,prev.volume_24h_usd))})",history_line,"","Security risk and market movement are evaluated separately."])
    finally:s.close()

def network_keyboard(networks=None):
    kb=types.InlineKeyboardMarkup(row_width=2)
    for n in networks or NETWORKS:
        kb.add(types.InlineKeyboardButton(network_label(n),callback_data=f"net:{n}"))
    return kb

def keyboard(result=None):
    kb=types.InlineKeyboardMarkup(row_width=2)
    for a,b in [("🔐 Security","security"),("💰 Market","market"),("👛 Holders","holders"),("📊 Comparison","comparison"),("💧 Liquidity","liquidity"),("🔴 Risk","risk"),("📋 Full Report","full")]:kb.add(types.InlineKeyboardButton(a,callback_data=f"v:{b}"))
    kb.add(types.InlineKeyboardButton("🔎 Analyze Another",callback_data="net:choose"))
    return kb

def build_start(): return "<b>MARSOF AI</b>\n\nSend a token contract/address.\nThe network is verified automatically from real sources.\n\n🟢 NO MAJOR RISK DETECTED\n🔴 HIGH RISK\n🔵 DATA\n⚪ UNAVAILABLE\n\nMissing data is never converted to zero."


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(title="MARSOF AI", version="7.0.0")


@app.get("/")
def root():
    return {
        "status": "running",
        "service": "MARSOF AI",
        "version": "7.0.0",
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
        prev=bot._marsof_ui.get(message.chat.id)
        if prev:
            try: bot.delete_message(message.chat.id,prev.get("message_id"))
            except Exception: pass
        sent=bot.send_message(message.chat.id,build_start(),parse_mode="HTML")
        bot._marsof_ui[message.chat.id]={"message_id":sent.message_id,"network":None,"result":None,"candidates":[]}

    @bot.message_handler(content_types=["text"])
    def text_handler(message):
        if not message.text or message.text.startswith("/"):return
        address=message.text.strip(); state=getattr(bot,"_marsof_ui",{}).get(message.chat.id,{})
        try:
            bot.send_chat_action(message.chat.id,"typing")
            candidates=detect_networks(address)
            if not candidates:
                bot.reply_to(message,"⚠️ No supported network could be verified for this address from the live sources.\n\nNo value was fabricated."); return
            if len(candidates)>1:
                state["pending_address"]=address; state["candidates"]=candidates; bot._marsof_ui[message.chat.id]=state
                bot.send_message(message.chat.id,"<b>Multiple real networks detected.</b> Select the network to scan:",parse_mode="HTML",reply_markup=network_keyboard(candidates)); return
            network=candidates[0]
            result=run_analysis(address,network)
            if not result: bot.reply_to(message,"⚠️ Verified analysis data was not available. No result was fabricated."); return
            prev=state.get("message_id")
            if prev:
                try: bot.delete_message(message.chat.id,prev)
                except Exception: pass
            sent=bot.send_message(message.chat.id,build_home(result),parse_mode="HTML",reply_markup=keyboard(result)); bot._marsof_ui[message.chat.id]={"message_id":sent.message_id,"network":network,"result":result}
        except requests.RequestException as exc:
            print(f"❌ source error: {exc}"); bot.reply_to(message,"⚠️ A live data source is temporarily unavailable. No value was fabricated.")
        except Exception as exc:
            print(f"❌ analysis error: {exc}"); bot.reply_to(message,"⚠️ Technical analysis error. No result was fabricated.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("net:"))
    def network_callbacks(call):
        try:
            action=call.data.split(":",1)[1]; state=getattr(bot,"_marsof_ui",{}).get(call.message.chat.id,{})
            if action=="choose":
                bot.edit_message_text("<b>Send the token address.</b>\n\nThe network will be detected automatically.",call.message.chat.id,call.message.message_id,parse_mode="HTML"); bot.answer_callback_query(call.id); return
            address=state.get("pending_address")
            if not address:
                bot.answer_callback_query(call.id,"Send the token address first."); return
            if action not in detect_networks(address):
                bot.answer_callback_query(call.id,"That network was not verified for this address."); return
            result=run_analysis(address,action)
            if not result: bot.answer_callback_query(call.id,"Verified data unavailable."); return
            bot.edit_message_text(build_home(result),call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=keyboard(result)); bot._marsof_ui[call.message.chat.id]={"message_id":call.message.message_id,"network":action,"result":result}; bot.answer_callback_query(call.id)
        except Exception as exc: print(f"❌ network callback: {exc}"); bot.answer_callback_query(call.id,"Unable to update network.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("v:"))
    def callbacks(call):
        try:
            state=getattr(bot,"_marsof_ui",{}).get(call.message.chat.id); result=state.get("result") if state else None
            if not result: bot.answer_callback_query(call.id,"Run an analysis first."); return
            action=call.data.split(":",1)[1]; text_value=build_home(result) if action=="home" else section_text(result,action)
            chunks=split_text(text_value,3500)
            bot.edit_message_text(chunks[0],call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=keyboard(result)); extras=[]
            for chunk in chunks[1:]:extras.append(bot.send_message(call.message.chat.id,chunk,parse_mode="HTML").message_id)
            state["extra_message_ids"]=extras; bot.answer_callback_query(call.id)
        except Exception as exc: print(f"❌ callback error: {exc}"); bot.answer_callback_query(call.id,"Unable to update the interface.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app,host="0.0.0.0",port=int(os.getenv("PORT","8000")))
