import os
import json
import math
import re
import requests
from pathlib import Path
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
# Multi-network analysis with automatic network detection.
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
GECKOTERMINAL_BASE = "https://api.geckoterminal.com/api/v2"
HONEY_POT_URL = "https://api.honeypot.is/v2/IsHoneypot"
SOURCIFY_BASE = "https://sourcify.dev/server/v2/contract"
DEFILLAMA_PRICE_URL = "https://coins.llama.fi/prices/current"
BLOCKSCOUT_TOKEN_URL = "{base}/api?module=token&action=getToken&contractaddress={address}"
BLOCKSCOUT_HOLDERS_URL = "{base}/api?module=token&action=getTokenHolders&contractaddress={address}&page=1&offset=100"
COINGECKO_TOKEN_PRICE_URL = "https://api.coingecko.com/api/v3/simple/token_price/{platform}"
ALCHEMY_API_KEY = os.getenv("ALCHEMY_API_KEY")
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
BIRDEYE_API_KEY = os.getenv("BIRDEYE_API_KEY")
COINGECKO_API_KEY = os.getenv("COINGECKO_API_KEY")
ETHERSCAN_API_KEY = os.getenv("ETHERSCAN_API_KEY")
BSCSCAN_API_KEY = os.getenv("BSCSCAN_API_KEY")
BASESCAN_API_KEY = os.getenv("BASESCAN_API_KEY")
BLOCKSCOUT_API_KEY = os.getenv("BLOCKSCOUT_API_KEY")
GOPLUS_SOLANA_URL = "https://api.gopluslabs.io/api/v1/solana/token_security"
GOPLUS_SUI_URL = "https://api.gopluslabs.io/api/v1/sui/token_security"
GECKO_NETWORKS = {
    "bsc": "bsc", "ethereum": "eth", "base": "base", "solana": "solana",
    "sui": "sui", "arc": "arc", "robinhood": "robinhood", "hyperliquid": "hyperliquid",
}
BLOCKSCOUT_BASES = {
    "base": "https://base.blockscout.com",
    "robinhood": "https://robinhoodchain.blockscout.com",
    "arc": "https://explorer.arc.io",
}
ALCHEMY_HOSTS = {
    "ethereum": "https://eth-mainnet.g.alchemy.com/v2/{key}",
    "base": "https://base-mainnet.g.alchemy.com/v2/{key}",
    "bsc": "https://bnb-mainnet.g.alchemy.com/v2/{key}",
    "solana": "https://solana-mainnet.g.alchemy.com/v2/{key}",
    "sui": "https://sui-mainnet.g.alchemy.com/v2/{key}",
    "robinhood": "https://robinhood-mainnet.g.alchemy.com/v2/{key}",
}
COINGECKO_PLATFORMS = {
    "ethereum": "ethereum", "bsc": "binance-smart-chain", "base": "base",
}
EXPLORER_API = {
    "ethereum": ("https://api.etherscan.io/api", lambda: ETHERSCAN_API_KEY),
    "bsc": ("https://api.bscscan.com/api", lambda: BSCSCAN_API_KEY),
    "base": ("https://api.basescan.org/api", lambda: BASESCAN_API_KEY),
}

bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
BASE_DIR = Path(__file__).resolve().parent
LOGO_PATH = BASE_DIR / "marsof_logo.jpg"

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
    return "Yes" if value is True else "No" if value is False else "Not available"


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

def first_value(*values):
    """Return the first genuinely available value; never replace missing data with 0."""
    for value in values:
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        return value
    return None


def fetch_honeypot(address, network):
    """Secondary EVM security/simulation source. Missing values only; never overrides GoPlus."""
    cfg = NETWORKS.get(network)
    if not cfg or cfg.get("kind") != "evm":
        return None
    try:
        params = {"address": address, "chainID": int(cfg["chain_id"])}
        response = requests.get(HONEY_POT_URL, params=params, timeout=12)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict):
            return None
        token = payload.get("token") or {}
        token_address = token.get("address")
        if token_address and normalize_address(token_address) != normalize_address(address):
            return None
        return payload
    except Exception as exc:
        print(f"ℹ️ Honeypot.is unavailable for {network}: {exc}")
    return None


def fetch_sourcify(address, network):
    """Secondary verified-contract source with no API key requirement."""
    cfg = NETWORKS.get(network)
    if not cfg or cfg.get("kind") != "evm":
        return None
    try:
        url = f"{SOURCIFY_BASE}/{cfg['chain_id']}/{address}"
        response = requests.get(
            url,
            params={"fields": "compilation,deployment,proxyResolution"},
            timeout=12,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else None
    except Exception as exc:
        print(f"ℹ️ Sourcify unavailable for {network}: {exc}")
    return None


def _merge_security_missing(base, extra, source_name):
    """Fill only missing security fields from a secondary provider."""
    if not isinstance(base, dict):
        base = {}
    if not isinstance(extra, dict):
        return base
    field_sources = base.setdefault("_field_sources", {})
    for key, value in extra.items():
        if key.startswith("_") or value is None:
            continue
        if base.get(key) is None:
            base[key] = value
            field_sources[key] = source_name
    return base


def normalize_honeypot_data(payload):
    if not isinstance(payload, dict):
        return {}
    out = {}
    token = payload.get("token") or {}
    sim = payload.get("simulationResult") or {}
    hp = payload.get("honeypotResult") or {}
    code = payload.get("contractCode") or {}
    if token.get("name") is not None: out["token_name"] = token.get("name")
    if token.get("symbol") is not None: out["token_symbol"] = token.get("symbol")
    if token.get("decimals") is not None: out["decimals"] = token.get("decimals")
    if token.get("totalHolders") is not None: out["holder_count"] = token.get("totalHolders")
    if hp.get("isHoneypot") is not None: out["is_honeypot"] = hp.get("isHoneypot")
    if code.get("openSource") is not None: out["is_open_source"] = code.get("openSource")
    if code.get("isProxy") is not None: out["is_proxy"] = code.get("isProxy")
    # Honeypot.is reports taxes as percentages; MARSOF's internal representation
    # stores them as fractions before build_security multiplies by 100.
    for src, dst in (("buyTax", "buy_tax"), ("sellTax", "sell_tax"), ("transferTax", "transfer_tax")):
        value = number(sim.get(src))
        if value is not None:
            out[dst] = value / 100.0
    pair = payload.get("pair") or {}
    if isinstance(pair, dict):
        inner = pair.get("pair") or {}
        if isinstance(inner, dict):
            if inner.get("createdAtTimestamp") is not None:
                out["_pair_created_at"] = inner.get("createdAtTimestamp")
            if inner.get("address"):
                out["_pair_address"] = inner.get("address")
    return out


def normalize_sourcify_data(payload, rpc):
    if not isinstance(payload, dict):
        return {}
    out = {}
    compilation = payload.get("compilation") or {}
    deployment = payload.get("deployment") or {}
    proxy = payload.get("proxyResolution") or {}
    if compilation.get("name"):
        out["contract_name"] = compilation.get("name")
    if deployment.get("deployer"):
        out["creator_address"] = deployment.get("deployer")
    if proxy.get("isProxy") is not None:
        out["is_proxy"] = proxy.get("isProxy")
    tx_hash = deployment.get("transactionHash")
    if tx_hash and rpc:
        try:
            tx = evm_rpc_call(rpc, "eth_getTransactionByHash", [tx_hash])
            block_hash = tx.get("blockHash") if isinstance(tx, dict) else None
            if block_hash:
                block = evm_rpc_call(rpc, "eth_getBlockByHash", [block_hash, False])
                ts = block.get("timestamp") if isinstance(block, dict) else None
                if isinstance(ts, str) and ts.startswith("0x"):
                    ts = int(ts, 16)
                if ts is not None:
                    out["deployed_time"] = ts
        except Exception as exc:
            print(f"ℹ️ Deployment timestamp lookup unavailable: {exc}")
    return out


def fetch_defillama_price(address, network):
    """Public secondary price source. Used only when current market sources lack USD price."""
    chain_map = {"bsc": "bsc", "ethereum": "ethereum", "base": "base"}
    chain = chain_map.get(network)
    if not chain:
        return None
    try:
        url = f"{DEFILLAMA_PRICE_URL}/{chain}:{address}"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        payload = response.json()
        coins = payload.get("coins") if isinstance(payload, dict) else None
        if not isinstance(coins, dict):
            return None
        target = f"{chain}:{address}".lower()
        for key, item in coins.items():
            if str(key).lower() == target and isinstance(item, dict):
                price = number(item.get("price"))
                return price
    except Exception as exc:
        print(f"ℹ️ DefiLlama price unavailable for {network}: {exc}")
    return None



def fetch_blockscout_v2(address, network):
    """Use Blockscout's current REST v2 endpoints for token metadata/holders.
    This is especially important for Robinhood Chain, where Blockscout is the
    official explorer. Only verified returned fields are used.
    """
    base = BLOCKSCOUT_BASES.get(network)
    if not base:
        return None
    headers = {"Accept": "application/json"}
    if BLOCKSCOUT_API_KEY:
        headers["x-api-key"] = BLOCKSCOUT_API_KEY
    out = {}
    try:
        url = f"{base}/api/v2/tokens/{address}"
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code < 400:
            item = r.json()
            if isinstance(item, dict):
                returned = item.get("address")
                if not returned or normalize_address(returned) == normalize_address(address):
                    mapping = {
                        "name":"token_name", "symbol":"token_symbol",
                        "decimals":"decimals", "total_supply":"total_supply",
                        "holders":"holder_count",
                    }
                    for src,dst in mapping.items():
                        if item.get(src) is not None:
                            out[dst] = item.get(src)
                    # Blockscout v2 exposes exchange_rate / market_cap on token pages
                    # when the explorer has pricing data.
                    if item.get("exchange_rate") is not None:
                        out["_market_price"] = number(item.get("exchange_rate"))
                    if item.get("market_cap") is not None:
                        out["_market_cap"] = number(item.get("market_cap"))
                    if item.get("circulating_market_cap") is not None:
                        out["_market_cap"] = first_value(out.get("_market_cap"), number(item.get("circulating_market_cap")))
                    if item.get("holders") is not None and out.get("holder_count") is None:
                        out["holder_count"] = item.get("holders")
                    if item.get("type") is not None:
                        out["_token_type"] = item.get("type")
        else:
            print(f"ℹ️ Blockscout v2 token endpoint {network}: HTTP {r.status_code}")
    except Exception as exc:
        print(f"ℹ️ Blockscout v2 token unavailable for {network}: {exc}")

    try:
        url = f"{base}/api/v2/tokens/{address}/holders"
        r = requests.get(url, headers=headers, params={"items_count":100}, timeout=15)
        if r.status_code < 400:
            payload = r.json()
            items = payload.get("items") if isinstance(payload, dict) else None
            if isinstance(items, list):
                holders=[]
                supply=number(out.get("total_supply"))
                for item in items[:100]:
                    if not isinstance(item, dict):
                        continue
                    holder = item.get("address") or {}
                    if isinstance(holder, dict):
                        holder_address = holder.get("hash")
                        tag = first_value(holder.get("name"), holder.get("ens_domain_name"), holder.get("is_contract") and "contract")
                    else:
                        holder_address = holder
                        tag = None
                    raw = first_value(item.get("value"), item.get("token_balance"), item.get("balance"))
                    bal=number(raw)
                    if not holder_address or bal is None:
                        continue
                    pct=(bal/supply) if supply not in (None,0) else None
                    holders.append({"address":holder_address,"balance":bal,"percent":pct,"tag":tag,
                                    "is_contract": bool((holder or {}).get("is_contract")) if isinstance(holder,dict) else None})
                if holders:
                    out["holders"]=holders
                    out["holder_count"]=first_value(out.get("holder_count"), len(holders))
    except Exception as exc:
        print(f"ℹ️ Blockscout v2 holders unavailable for {network}: {exc}")
    return out or None


def merge_blockscout_market(pair, payload):
    if not isinstance(payload, dict):
        return pair
    price=number(payload.get("_market_price"))
    mc=number(payload.get("_market_cap"))
    if not pair and price is None and mc is None:
        return pair
    out=json.loads(json.dumps(pair)) if pair else {
        "priceUsd":price, "marketCap":mc, "fdv":None,
        "liquidity":{}, "volume":{}, "priceChange":{}, "txns":{},
        "_sources":[], "_field_sources":{}
    }
    fs=out.setdefault("_field_sources", {})
    if out.get("priceUsd") is None and price is not None:
        out["priceUsd"]=price; fs["priceUsd"]="Blockscout"
    if out.get("marketCap") is None and mc is not None:
        out["marketCap"]=mc; fs["marketCap"]="Blockscout"
    out["_sources"]=list(dict.fromkeys([*(out.get("_sources") or []),"Blockscout"]))
    return out

def fetch_blockscout_token(address, network):
    """Blockscout token metadata/holders for EVM networks with a Blockscout instance."""
    base = BLOCKSCOUT_BASES.get(network)
    if not base:
        return None
    try:
        headers = {"Accept": "application/json"}
        if BLOCKSCOUT_API_KEY:
            headers["x-api-key"] = BLOCKSCOUT_API_KEY
        response = requests.get(BLOCKSCOUT_TOKEN_URL.format(base=base, address=address), headers=headers, timeout=12)
        if response.status_code >= 400:
            return None
        payload = response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict):
            return None
        returned = result.get("contractAddress") or result.get("address")
        if returned and normalize_address(returned) != normalize_address(address):
            return None
        out = {}
        for src, dst in (("name", "token_name"), ("symbol", "token_symbol"), ("decimals", "decimals"), ("totalSupply", "total_supply")):
            if result.get(src) is not None:
                out[dst] = result.get(src)
        # Blockscout exposes holders through the same token module. Use them only
        # when their contract address/value can be matched exactly.
        try:
            hresp = requests.get(BLOCKSCOUT_HOLDERS_URL.format(base=base, address=address), headers=headers, timeout=12)
            hpayload = hresp.json() if hresp.status_code < 400 else {}
            hresult = hpayload.get("result") if isinstance(hpayload, dict) else None
            if isinstance(hresult, list) and hresult:
                supply = number(result.get("totalSupply"))
                holders = []
                for item in hresult[:10]:
                    if not isinstance(item, dict):
                        continue
                    holder_address = item.get("address_hash") or item.get("address") or item.get("holder")
                    raw_value = item.get("value") or item.get("balance") or item.get("amount")
                    if not holder_address or raw_value is None:
                        continue
                    bal = number(raw_value)
                    if bal is None:
                        continue
                    pct = (bal / supply) if supply not in (None, 0) else None
                    holders.append({"address": holder_address, "balance": bal, "percent": pct, "tag": item.get("address_label") or item.get("name")})
                if holders:
                    out["holders"] = holders
                    out["holder_count"] = first_value(result.get("holdersCount"), len(hresult))
        except Exception:
            pass
        return out
    except Exception as exc:
        print(f"ℹ️ Blockscout token unavailable for {network}: {exc}")
    return None


def fetch_alchemy_token(address, network):
    """Alchemy Token API metadata where an API key is configured and the chain is supported."""
    if not ALCHEMY_API_KEY or network not in ALCHEMY_HOSTS or network == "solana":
        return None
    try:
        url = ALCHEMY_HOSTS[network].format(key=ALCHEMY_API_KEY)
        body = {"jsonrpc": "2.0", "id": 1, "method": "alchemy_getTokenMetadata", "params": [address]}
        response = requests.post(url, json=body, timeout=12)
        if response.status_code >= 400:
            return None
        payload = response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if not isinstance(result, dict):
            return None
        out = {}
        for src, dst in (("name", "token_name"), ("symbol", "token_symbol"), ("decimals", "decimals")):
            if result.get(src) is not None:
                out[dst] = result.get(src)
        return out
    except Exception as exc:
        print(f"ℹ️ Alchemy Token API unavailable for {network}: {exc}")
    return None


def fetch_explorer_token(address, network):
    """Optional Etherscan/BscScan/BaseScan token metadata source."""
    cfg = EXPLORER_API.get(network)
    if not cfg:
        return None
    base_url, key_getter = cfg
    key = key_getter()
    if not key:
        return None
    try:
        params = {"module": "token", "action": "tokeninfo", "contractaddress": address, "apikey": key}
        response = requests.get(base_url, params=params, timeout=12)
        if response.status_code >= 400:
            return None
        payload = response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if isinstance(result, list) and result:
            result = result[0]
        if not isinstance(result, dict):
            return None
        out = {}
        for src, dst in (("tokenName", "token_name"), ("tokenSymbol", "token_symbol"), ("tokenDecimal", "decimals"), ("tokenSupply", "total_supply")):
            if result.get(src) is not None:
                out[dst] = result.get(src)
        return out
    except Exception as exc:
        print(f"ℹ️ Explorer token API unavailable for {network}: {exc}")
    return None


def fetch_coingecko_token(address, network):
    """CoinGecko contract price/metadata fallback for supported EVM platforms."""
    platform = COINGECKO_PLATFORMS.get(network)
    if not platform:
        return None
    try:
        headers = {"Accept": "application/json"}
        if COINGECKO_API_KEY:
            headers["x-cg-pro-api-key"] = COINGECKO_API_KEY
        params = {"contract_addresses": address, "vs_currencies": "usd", "include_market_cap": "true", "include_24hr_vol": "true", "include_24hr_change": "true"}
        response = requests.get(COINGECKO_TOKEN_PRICE_URL.format(platform=platform), params=params, headers=headers, timeout=12)
        if response.status_code >= 400:
            return None
        payload = response.json()
        item = payload.get(address.lower()) or payload.get(address) if isinstance(payload, dict) else None
        if not isinstance(item, dict):
            return None
        return {"priceUsd": number(item.get("usd")), "marketCap": number(item.get("usd_market_cap")), "volume24h": number(item.get("usd_24h_vol")), "priceChange24h": number(item.get("usd_24h_change"))}
    except Exception as exc:
        print(f"ℹ️ CoinGecko contract price unavailable for {network}: {exc}")
    return None


def fetch_alchemy_solana_holders(address):
    """Alchemy indexed Solana holder list at the latest slot."""
    if not ALCHEMY_API_KEY:
        return None
    try:
        url = ALCHEMY_HOSTS["solana"].format(key=ALCHEMY_API_KEY)
        def call(method, params):
            r = requests.post(url, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=15)
            if r.status_code >= 400: return None
            payload = r.json()
            return payload.get("result") if isinstance(payload, dict) else None
        slot = call("getSlot", [{"commitment":"finalized"}])
        if slot is None:
            slot = call("getSlot", [])
        if slot is None:
            return None
        result = call("getTokenHoldersAtSlot", {"mint": address, "slot": int(slot), "limit": 1000, "sortBy":"balance_desc"})
        if not isinstance(result, dict):
            return None
        holders = []
        for item in result.get("holders") or []:
            if not isinstance(item, dict):
                continue
            owner = item.get("owner") or item.get("holder")
            amount = first_value(item.get("balanceUi"), item.get("balanceRaw"))
            if owner and amount is not None:
                holders.append({"address": owner, "balance": amount, "amount": amount, "uiAmount": amount})
        return holders or None
    except Exception as exc:
        print(f"ℹ️ Alchemy Solana holders unavailable: {exc}")
    return None


def fetch_helius_token(address):
    """Helius DAS getAsset for Solana fungible-token metadata, supply and price."""
    if not HELIUS_API_KEY:
        return None
    try:
        url = f"https://mainnet.helius-rpc.com/?api-key={HELIUS_API_KEY}"
        body = {"jsonrpc": "2.0", "id": 1, "method": "getAsset", "params": {"id": address, "displayOptions": {"showFungible": True}}}
        response = requests.post(url, json=body, timeout=15)
        if response.status_code >= 400:
            return None
        asset = response.json().get("result") if isinstance(response.json(), dict) else None
        if not isinstance(asset, dict):
            return None
        meta = ((asset.get("content") or {}).get("metadata") or {})
        ti = asset.get("token_info") or {}
        out = {}
        if meta.get("name") is not None: out["token_name"] = meta.get("name")
        if meta.get("symbol") is not None: out["token_symbol"] = meta.get("symbol")
        if ti.get("decimals") is not None: out["decimals"] = ti.get("decimals")
        if ti.get("supply") is not None: out["supply"] = ti.get("supply")
        pi = ti.get("price_info") or {}
        if pi.get("price_per_token") is not None: out["priceUsd"] = pi.get("price_per_token")
        return out
    except Exception as exc:
        print(f"ℹ️ Helius unavailable: {exc}")
    return None


def fetch_birdeye_token(address):
    """Birdeye Solana metadata/holder intelligence source when a key is configured."""
    if not BIRDEYE_API_KEY:
        return None
    try:
        headers = {"X-API-KEY": BIRDEYE_API_KEY, "x-chain": "solana", "accept": "application/json"}
        response = requests.get("https://public-api.birdeye.so/defi/token_overview", params={"address": address}, headers=headers, timeout=15)
        if response.status_code >= 400:
            return None
        data = response.json().get("data") if isinstance(response.json(), dict) else None
        if not isinstance(data, dict):
            return None
        out = {}
        for src, dst in (("name", "token_name"), ("symbol", "token_symbol"), ("decimals", "decimals"), ("holder", "holder_count"), ("liquidity", "liquidity"), ("mc", "marketCap"), ("price", "priceUsd"), ("v24hUSD", "volume24h"), ("priceChange24hPercent", "priceChange24h")):
            if data.get(src) is not None:
                out[dst] = data.get(src)
        return out
    except Exception as exc:
        print(f"ℹ️ Birdeye unavailable: {exc}")
    return None


def fetch_alchemy_sui_metadata(address):
    """Alchemy Sui standard API fallback for coin metadata and supply."""
    if not ALCHEMY_API_KEY or "sui" not in ALCHEMY_HOSTS:
        return None
    try:
        url = ALCHEMY_HOSTS["sui"].format(key=ALCHEMY_API_KEY)
        def call(method, params):
            r = requests.post(url, json={"jsonrpc":"2.0","id":1,"method":method,"params":params}, timeout=12)
            if r.status_code >= 400: return None
            return r.json().get("result") if isinstance(r.json(), dict) else None
        meta = call("suix_getCoinMetadata", [address])
        supply_obj = call("suix_getTotalSupply", [address])
        out = {}
        if isinstance(meta, dict):
            for src, dst in (("name","token_name"),("symbol","token_symbol"),("decimals","decimals")):
                if meta.get(src) is not None: out[dst] = meta.get(src)
        if isinstance(supply_obj, dict) and supply_obj.get("value") is not None:
            out["total_supply"] = supply_obj.get("value")
        return out
    except Exception as exc:
        print(f"ℹ️ Alchemy Sui unavailable: {exc}")
    return None


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


def fetch_goplus_solana(address):
    try:
        response = requests.get(GOPLUS_SOLANA_URL, params={"contract_addresses": address}, timeout=25)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if isinstance(result, dict):
            target = normalize_address(address)
            for returned_address, data in result.items():
                if normalize_address(returned_address) == target and isinstance(data, dict):
                    return dict(data)
        # If the provider returns a list, accept only an item whose address
        # exactly matches the requested token. Never take the first item blindly.
        if isinstance(result, list):
            target = normalize_address(address)
            for item in result:
                if not isinstance(item, dict):
                    continue
                candidate = first_value(item.get("address"), item.get("contract_address"), item.get("token_address"), item.get("mint"))
                if candidate and normalize_address(candidate) == target:
                    return dict(item)
    except Exception as exc:
        print(f"ℹ️ GoPlus Solana unavailable: {exc}")
    return None


def fetch_goplus_sui(address):
    try:
        response = requests.get(GOPLUS_SUI_URL, params={"contract_addresses": address}, timeout=25)
        response.raise_for_status()
        payload = response.json()
        result = payload.get("result") if isinstance(payload, dict) else None
        if isinstance(result, dict):
            target = normalize_address(address)
            for returned_address, data in result.items():
                if normalize_address(returned_address) == target and isinstance(data, dict):
                    return dict(data)
        if isinstance(result, list) and result and isinstance(result[0], dict):
            return dict(result[0])
    except Exception as exc:
        print(f"ℹ️ GoPlus Sui unavailable: {exc}")
    return None


def fetch_geckoterminal(address, network):
    slug = GECKO_NETWORKS.get(network)
    if not slug:
        return []
    try:
        url = f"{GECKOTERMINAL_BASE}/networks/{slug}/tokens/{address}/pools"
        response = requests.get(url, timeout=25)
        response.raise_for_status()
        payload = response.json()
        rows = payload.get("data") if isinstance(payload, dict) else None
        if not isinstance(rows, list):
            return []
        normalized = []
        for row in rows:
            attrs = (row or {}).get("attributes") or {}
            if not isinstance(attrs, dict):
                continue
            tx = attrs.get("transactions") or {}
            vol = attrs.get("volume_usd") or {}
            pc = attrs.get("price_change_percentage") or {}
            # Normalize GeckoTerminal into the same market shape used by the UI.
            pair = {
                "chainId": slug,
                "dexId": ((row.get("relationships") or {}).get("dex") or {}).get("data", {}).get("id") if isinstance(row, dict) else None,
                "pairAddress": attrs.get("address"),
                "baseToken": {"address": None, "symbol": None},
                "quoteToken": {"address": None, "symbol": None},
                "priceUsd": first_value(attrs.get("base_token_price_usd"), attrs.get("quote_token_price_usd")),
                "priceNative": attrs.get("base_token_price_native_currency"),
                "liquidity": {"usd": number(attrs.get("reserve_in_usd")), "base": None, "quote": None},
                "fdv": number(attrs.get("fdv_usd")),
                "marketCap": number(attrs.get("market_cap_usd")),
                "pairCreatedAt": None,
                "volume": {k: number(vol.get(k)) for k in ["m5","h1","h6","h24"]},
                "priceChange": {k: number(pc.get(k)) for k in ["m5","h1","h6","h24"]},
                "txns": {k: {"buys": integer((tx.get(k) or {}).get("buys")), "sells": integer((tx.get(k) or {}).get("sells"))} for k in ["m5","h1","h6","h24"]},
                "_source": "GeckoTerminal",
                "_field_sources": {
                    "priceUsd": "GeckoTerminal", "priceNative": "GeckoTerminal", "marketCap": "GeckoTerminal",
                    "fdv": "GeckoTerminal", "pairAddress": "GeckoTerminal", "dexId": "GeckoTerminal",
                    "liquidity.usd": "GeckoTerminal",
                    **{f"volume.{k}": "GeckoTerminal" for k in ["m5","h1","h6","h24"]},
                    **{f"priceChange.{k}": "GeckoTerminal" for k in ["m5","h1","h6","h24"]},
                    **{f"txns.{k}.buys": "GeckoTerminal" for k in ["m5","h1","h6","h24"]},
                    **{f"txns.{k}.sells": "GeckoTerminal" for k in ["m5","h1","h6","h24"]},
                },
            }
            normalized.append(pair)
        normalized.sort(key=lambda p: number((p.get("liquidity") or {}).get("usd")) or -1, reverse=True)
        return normalized
    except Exception as exc:
        print(f"ℹ️ GeckoTerminal unavailable for {network}: {exc}")
        return []


def merge_market_pair(primary, secondary):
    """Keep the primary source value; fill only missing fields from the secondary source."""
    if not primary:
        return secondary
    if not secondary:
        return primary
    out = json.loads(json.dumps(primary))
    sources = dict(primary.get("_field_sources") or {})
    secondary_sources = secondary.get("_field_sources") or {}
    for key in ["priceUsd", "priceNative", "marketCap", "fdv", "pairCreatedAt", "pairAddress", "dexId"]:
        before = out.get(key)
        out[key] = first_value(before, secondary.get(key))
        if before is None and out.get(key) is not None:
            sources[key] = secondary_sources.get(key, "GeckoTerminal")
    for group in ["liquidity", "volume", "priceChange", "txns"]:
        a = out.get(group) if isinstance(out.get(group), dict) else {}
        b = secondary.get(group) if isinstance(secondary.get(group), dict) else {}
        for k, v in b.items():
            if isinstance(v, dict):
                av = a.get(k) if isinstance(a.get(k), dict) else {}
                for kk, vv in v.items():
                    before = av.get(kk)
                    av[kk] = first_value(before, vv)
                    if before is None and av.get(kk) is not None:
                        sources[f"{group}.{k}.{kk}"] = secondary_sources.get(f"{group}.{k}.{kk}", "GeckoTerminal")
                a[k] = av
            else:
                before = a.get(k)
                a[k] = first_value(before, v)
                if before is None and a.get(k) is not None:
                    sources[f"{group}.{k}"] = secondary_sources.get(f"{group}.{k}", "GeckoTerminal")
        out[group] = a
    out["_field_sources"] = sources
    out["_sources"] = list(dict.fromkeys([*primary.get("_sources", [primary.get("_source", "DEX Screener")]), *secondary.get("_sources", [secondary.get("_source", "GeckoTerminal")])]))
    return out


def fetch_dexscreener(address, network):
    cfg = NETWORKS[network]
    matches = []
    try:
        response = requests.get(DEXSCREENER_URL.format(address=address.strip()), timeout=25)
        response.raise_for_status()
        payload = response.json()
        pairs = payload.get("pairs") if isinstance(payload, dict) else None
        if isinstance(pairs, list):
            target = address.strip().lower()
            for pair in pairs:
                if not isinstance(pair, dict):
                    continue
                if str(pair.get("chainId") or "").lower() != cfg["dex_chain"]:
                    continue
                base = str((pair.get("baseToken") or {}).get("address") or "").lower()
                quote = str((pair.get("quoteToken") or {}).get("address") or "").lower()
                if target == base or target == quote:
                    pair["_source"] = "DEX Screener"
                    pair["_field_sources"] = {
                        "priceUsd": "DEX Screener", "priceNative": "DEX Screener", "marketCap": "DEX Screener",
                        "fdv": "DEX Screener", "pairAddress": "DEX Screener", "dexId": "DEX Screener",
                        "liquidity.usd": "DEX Screener", "liquidity.base": "DEX Screener", "liquidity.quote": "DEX Screener",
                        **{f"volume.{k}": "DEX Screener" for k in ["m5","h1","h6","h24"]},
                        **{f"priceChange.{k}": "DEX Screener" for k in ["m5","h1","h6","h24"]},
                        **{f"txns.{k}.buys": "DEX Screener" for k in ["m5","h1","h6","h24"]},
                        **{f"txns.{k}.sells": "DEX Screener" for k in ["m5","h1","h6","h24"]},
                    }
                    pair["_sources"] = ["DEX Screener"]
                    matches.append(pair)
    except Exception as exc:
        print(f"ℹ️ DEX Screener unavailable for {network}: {exc}")

    matches.sort(key=lambda p: number((p.get("liquidity") or {}).get("usd")) or -1, reverse=True)
    # GeckoTerminal is the secondary market source. It fills only missing fields
    # on the DEX Screener result, or becomes the market source if DS has no pair.
    gecko = fetch_geckoterminal(address, network)
    if matches and gecko:
        matches[0] = merge_market_pair(matches[0], gecko[0])
        matches[0]["_sources"] = ["DEX Screener", "GeckoTerminal"]
    elif gecko:
        matches = gecko
        matches[0]["_sources"] = ["GeckoTerminal"]
    # DefiLlama is a tertiary price source. It never overrides DEX Screener or
    # GeckoTerminal; it only fills an unavailable USD price on an existing pair.
    if matches and number(matches[0].get("priceUsd")) is None:
        llama_price = fetch_defillama_price(address, network)
        if llama_price is not None:
            matches[0]["priceUsd"] = llama_price
            sources = matches[0].setdefault("_field_sources", {})
            sources["priceUsd"] = "DefiLlama"
            matches[0]["_sources"] = list(dict.fromkeys([*matches[0].get("_sources", []), "DefiLlama"]))

    if matches:
        cg = fetch_coingecko_token(address, network)
        if cg:
            sources = matches[0].setdefault("_field_sources", {})
            if matches[0].get("priceUsd") is None and cg.get("priceUsd") is not None:
                matches[0]["priceUsd"] = cg["priceUsd"]; sources["priceUsd"] = "CoinGecko"
            if matches[0].get("marketCap") is None and cg.get("marketCap") is not None:
                matches[0]["marketCap"] = cg["marketCap"]; sources["marketCap"] = "CoinGecko"
            if matches[0].get("volume", {}).get("h24") is None and cg.get("volume24h") is not None:
                matches[0].setdefault("volume", {})["h24"] = cg["volume24h"]; sources["volume.h24"] = "CoinGecko"
            if matches[0].get("priceChange", {}).get("h24") is None and cg.get("priceChange24h") is not None:
                matches[0].setdefault("priceChange", {})["h24"] = cg["priceChange24h"]; sources["priceChange.h24"] = "CoinGecko"
            matches[0]["_sources"] = list(dict.fromkeys([*matches[0].get("_sources", []), "CoinGecko"]))
    else:
        # Indexed market sources can still provide a price when no DEX pair was indexed.
        cg = fetch_coingecko_token(address, network)
        if cg and any(v is not None for v in cg.values()):
            matches = [{
                "priceUsd": cg.get("priceUsd"), "marketCap": cg.get("marketCap"),
                "fdv": None, "pairAddress": None, "dexId": None,
                "liquidity": {}, "volume": {"h24": cg.get("volume24h")},
                "priceChange": {"h24": cg.get("priceChange24h")}, "txns": {},
                "_source": "CoinGecko", "_sources": ["CoinGecko"],
                "_field_sources": {k: "CoinGecko" for k,v in {"priceUsd":cg.get("priceUsd"),"marketCap":cg.get("marketCap"),"volume.h24":cg.get("volume24h"),"priceChange.h24":cg.get("priceChange24h")}.items() if v is not None}
            }]
    if matches:
        print(f"✅ Market sources for {cfg['name']}: {matches[0].get('_sources', [matches[0].get('_source', 'Unknown')])}")
    else:
        print(f"ℹ️ No indexed market data found for {cfg['name']} / {address}")
    return matches


def detect_networks(address):
    """Detect the network from real indexed/on-chain data; never guess from EVM format alone."""
    address = (address or "").strip()
    detected = []

    # 1) DEX Screener indexes the token across supported chains. Prefer an
    # exact token match with an actual market pair because it is directly
    # useful for the upcoming analysis.
    try:
        response = requests.get(DEXSCREENER_URL.format(address=address), timeout=20)
        response.raise_for_status()
        payload = response.json()
        pairs = payload.get("pairs") if isinstance(payload, dict) else []
        if isinstance(pairs, list):
            by_network = {}
            target = address.lower()
            for pair in pairs:
                if not isinstance(pair, dict):
                    continue
                chain = str(pair.get("chainId") or "").lower()
                network_key = next((k for k, cfg in NETWORKS.items() if cfg.get("dex_chain") == chain), None)
                if not network_key:
                    continue
                base = str((pair.get("baseToken") or {}).get("address") or "").lower()
                quote = str((pair.get("quoteToken") or {}).get("address") or "").lower()
                if target not in {base, quote}:
                    continue
                liq = number((pair.get("liquidity") or {}).get("usd")) or 0
                by_network.setdefault(network_key, []).append((liq, pair))
            for network_key, items in by_network.items():
                best = max(items, key=lambda x: x[0])
                detected.append({"network": network_key, "liquidity": best[0], "pair": best[1]})
    except Exception as exc:
        print(f"ℹ️ network detection via DEX Screener unavailable: {exc}")

    if detected:
        detected.sort(key=lambda x: x.get("liquidity", 0), reverse=True)
        return detected

    # 2) Non-EVM address formats can provide a conservative fallback, but
    # Sui/EVM both use 0x forms, so Sui is only accepted here when it is not
    # a standard 42-character EVM address.
    if is_valid_solana_address(address):
        return [{"network": "solana", "liquidity": 0, "pair": None}]

    if is_valid_sui_address(address) and not is_valid_evm_address(address):
        return [{"network": "sui", "liquidity": 0, "pair": None}]

    # 3) For a plain EVM address, check actual contract code on the supported
    # EVM RPCs. This is intentionally a real on-chain check, not a format guess.
    if is_valid_evm_address(address):
        for network_key, cfg in NETWORKS.items():
            if cfg.get("kind") != "evm":
                continue
            try:
                code = evm_rpc_call(cfg["rpc"], "eth_getCode", [address, "latest"])
                if isinstance(code, str) and code not in {"0x", "0x0", "0x00"}:
                    detected.append({"network": network_key, "liquidity": 0, "pair": None})
            except Exception as exc:
                print(f"ℹ️ {cfg['name']} RPC detection unavailable: {exc}")

    return detected

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


def sui_rpc(method, params):
    return evm_rpc_call(NETWORKS["sui"]["rpc"], method, params)


def fetch_sui_metadata(address):
    data = {}
    # Sui coin types are Move type strings rather than EVM-style addresses.
    # For 0x-prefixed input, first try direct object lookup; if it is a package/object,
    # preserve whatever the node can verify instead of inventing token metadata.
    try:
        obj = sui_rpc("sui_getObject", [address, {"showType": True, "showContent": True, "showDisplay": True, "showOwner": True}])
        result = (obj or {}).get("data") if isinstance(obj, dict) else None
        if isinstance(result, dict):
            data["object_type"] = result.get("type")
            content = result.get("content") or {}
            fields = content.get("fields") if isinstance(content, dict) else {}
            if isinstance(fields, dict):
                data["symbol"] = first_value(fields.get("symbol"), fields.get("name"))
                data["decimals"] = integer(fields.get("decimals"))
                data["supply"] = number(fields.get("supply"))
    except Exception as exc:
        print(f"ℹ️ Sui object lookup unavailable: {exc}")
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


def _goplus_status(obj):
    if isinstance(obj, dict):
        return boolean(obj.get("status", obj.get("value")))
    return boolean(obj)


BURN_ADDRESSES = {
    "0x000000000000000000000000000000000000dead",
    "0x0000000000000000000000000000000000000000",
}
NON_INVESTOR_TAG_MARKERS = (
    "pancake", "uniswap", "sushiswap", "raydium", "orca",
    "liquidity", "pair", "pool", "pinklock", "team.finance",
    "unicrypt", "locker", "lock", "vesting", "staking",
    "router", "factory", "multisig", "system", "burn", "dead",
)

def _holder_pct(value):
    x = number(value)
    if x is None:
        return None
    # GoPlus holder percentages are fractions where 1 = 100%.
    return x * 100.0 if x <= 1.0 else x

def _holder_is_non_investor(holder, known_addresses=None):
    address = normalize_address(holder.get("address"))
    tag = str(holder.get("tag") or "").strip().lower()
    if address in BURN_ADDRESSES:
        return True, "burn/dead address"
    if any(marker in tag for marker in NON_INVESTOR_TAG_MARKERS):
        return True, f"system/LP/lock tag: {holder.get('tag')}"
    if boolean(holder.get("is_locked")) is True:
        return True, "locked holder"
    if known_addresses and address and address in known_addresses:
        return True, "known LP/DEX address"
    return False, None

def _concentration_metrics(holders, lp_holders=None, dexes=None):
    holders = holders if isinstance(holders, list) else []
    lp_holders = lp_holders if isinstance(lp_holders, list) else []
    dexes = dexes if isinstance(dexes, list) else []
    known = set()
    for h in lp_holders:
        if isinstance(h, dict) and h.get("address"):
            known.add(normalize_address(h.get("address")))
    for d in dexes:
        if isinstance(d, dict):
            for key in ("pair", "pair_address", "address"):
                if d.get(key):
                    known.add(normalize_address(d.get(key)))
    raw_values=[]; investor_values=[]; excluded_values=[]; excluded_reasons=[]
    raw_complete=True; adjusted_complete=True; excluded_complete=True
    for h in holders[:10]:
        if not isinstance(h, dict):
            raw_complete=False; adjusted_complete=False; excluded_complete=False
            continue
        pct=_holder_pct(h.get("percent"))
        if pct is None:
            raw_complete=False
        else:
            raw_values.append(pct)
        excluded, reason=_holder_is_non_investor(h, known)
        if excluded:
            if pct is None:
                excluded_complete=False
            else:
                excluded_values.append(pct)
            excluded_reasons.append(reason or "non-investor")
        else:
            if pct is None:
                adjusted_complete=False
            else:
                investor_values.append(pct)
    raw = sum(raw_values) if holders and raw_complete else None
    adjusted = sum(investor_values) if holders and adjusted_complete else None
    excluded = sum(excluded_values) if holders and excluded_complete else None
    return raw, adjusted, excluded, len(excluded_reasons), excluded_reasons


def _concentration_status(adjusted):
    if adjusted is None:
        return "UNKNOWN"
    if adjusted > 45:
        return "RISK"
    if adjusted >= 30:
        return "CAUTION"
    return "PASS"

def build_solana_security(gp, onchain):
    checks = []
    def add(label, value, status="DATA", detail=None, source="GoPlus Solana"):
        checks.append(metric(label, value, status, detail, source))

    gp = gp or {}
    onchain = onchain or {}
    add("Token Name", first_value(gp.get("name"), (gp.get("metadata") or {}).get("name"), onchain.get("token_name")), "DATA" if first_value(gp.get("name"), (gp.get("metadata") or {}).get("name"), onchain.get("token_name")) else "UNKNOWN")
    add("Token Symbol", first_value(gp.get("symbol"), (gp.get("metadata") or {}).get("symbol"), onchain.get("token_symbol")), "DATA" if first_value(gp.get("symbol"), (gp.get("metadata") or {}).get("symbol"), onchain.get("token_symbol")) else "UNKNOWN")
    add("Decimals", first_value(integer(gp.get("decimals")), integer(onchain.get("decimals"))), "DATA" if first_value(integer(gp.get("decimals")), integer(onchain.get("decimals"))) is not None else "UNKNOWN")
    supply = first_value(number(gp.get("total_supply")), number(onchain.get("supply")))
    add("Total Supply", supply, "DATA" if supply is not None else "UNKNOWN", fmt_num(supply))

    mintable = _goplus_status(gp.get("mintable"))
    add("Mintable", mintable, "RISK" if mintable is True else "PASS" if mintable is False else "UNKNOWN", "Active mint authority" if mintable is True else "Minting unavailable / revoked" if mintable is False else "Not available")
    freezable = _goplus_status(gp.get("freezable"))
    add("Freezable", freezable, "RISK" if freezable is True else "PASS" if freezable is False else "UNKNOWN", "Freeze capability is active" if freezable is True else "Freeze capability unavailable / revoked" if freezable is False else "Not available")
    closable = _goplus_status(gp.get("closable"))
    add("Closable", closable, "RISK" if closable is True else "PASS" if closable is False else "UNKNOWN", "Token program can be closed" if closable is True else "Close capability unavailable / revoked" if closable is False else "Not available")
    metadata_mutable = _goplus_status(gp.get("metadata_mutable"))
    add("Metadata Mutable", metadata_mutable, "CAUTION" if metadata_mutable is True else "PASS" if metadata_mutable is False else "UNKNOWN", "Metadata can be changed" if metadata_mutable is True else "Metadata immutable" if metadata_mutable is False else "Not available")
    transfer_up = _goplus_status(gp.get("transfer_fee_upgradable"))
    add("Transfer Fee Upgradable", transfer_up, "CAUTION" if transfer_up is True else "PASS" if transfer_up is False else "UNKNOWN", "Transfer fee can be upgraded" if transfer_up is True else "Transfer fee upgrade unavailable" if transfer_up is False else "Not available")
    balance_mut = _goplus_status(gp.get("balance_mutable_authority"))
    add("Balance Mutable", balance_mut, "RISK" if balance_mut is True else "PASS" if balance_mut is False else "UNKNOWN", "Admin can modify balances" if balance_mut is True else "Balance mutation unavailable" if balance_mut is False else "Not available")

    creators = gp.get("creator") or gp.get("creators")
    if isinstance(creators, dict): creators = [creators]
    if isinstance(creators, list) and creators:
        malicious = any(boolean(c.get("malicious_address")) is True for c in creators if isinstance(c, dict))
        add("Creator Malicious Flag", malicious, "RISK" if malicious else "PASS", "Creator flagged by GoPlus" if malicious else "No creator malicious flag returned")
    else:
        add("Creator Malicious Flag", None, "UNKNOWN", "Not available")

    # On-chain largest accounts are a second source. Calculate concentration only when supply is known.
    largest = onchain.get("largest_accounts") or []
    if largest and supply not in (None, 0):
        ui_amounts = [number(x.get("uiAmount")) if isinstance(x, dict) else None for x in largest[:10]]
        if all(x is not None for x in ui_amounts):
            total_top10 = sum(ui_amounts)
        else:
            raw_amounts = [number(x.get("amount")) if isinstance(x, dict) else None for x in largest[:10]]
            total_raw = sum(x for x in raw_amounts if x is not None)
            decimals = integer(onchain.get("decimals"))
            total_top10 = total_raw / (10 ** decimals) if decimals is not None else None
        top10 = (total_top10 / supply * 100) if total_top10 is not None and supply else None
        # Raw Solana largest-account concentration is shown as DATA here. It is not
        # converted into investor-risk unless the account classification is verified.
        add("Raw Top 10 Concentration", top10, "DATA" if top10 is not None else "UNKNOWN", fmt_pct(top10), "Solana RPC")
        for i, item in enumerate(largest[:10], 1):
            amount = number(item.get("uiAmount")) if isinstance(item, dict) else None
            add(f"Largest Token Account #{i}", amount, "DATA" if amount is not None else "UNKNOWN", fmt_num(amount), "Solana RPC")
    else:
        add("Top 10 Concentration", None, "UNKNOWN", "Not available")
    return checks


def build_sui_security(gp):
    checks = []
    gp = gp or {}
    def add(label, value, status="DATA", detail=None):
        checks.append(metric(label, value, status, detail, "GoPlus Sui"))

    name = gp.get("name")
    if isinstance(name, dict):
        name_value = first_value(name.get("name"), name.get("symbol"))
    else:
        name_value = name
    add("Token Name", name_value, "DATA" if name_value else "UNKNOWN", name_value or "Not available")
    add("Token Symbol", gp.get("symbol"), "DATA" if gp.get("symbol") else "UNKNOWN", gp.get("symbol") or "Not available")
    add("Creator", gp.get("creator"), "DATA" if gp.get("creator") else "UNKNOWN", gp.get("creator") or "Not available")
    add("Decimals", integer(gp.get("decimals")), "DATA" if gp.get("decimals") is not None else "UNKNOWN", str(integer(gp.get("decimals"))) if gp.get("decimals") is not None else "Not available")
    add("Total Supply", number(gp.get("total_supply")), "DATA" if gp.get("total_supply") is not None else "UNKNOWN", fmt_num(gp.get("total_supply")))

    for key, label, risky in [
        ("mintable", "Mintable", True),
        ("blacklist", "Blacklist", True),
        ("contract_upgradeable", "Contract Upgradeable", True),
    ]:
        obj = gp.get(key)
        if isinstance(obj, dict):
            status_value = obj.get("value", obj.get("status"))
            active = str(status_value).strip() in {"1", "2", "true", "True"}
            immutable = str(obj.get("cap_owner", "")).strip().lower() == "immutable"
            if immutable:
                active = False
            add(label, active, "RISK" if active and risky else "PASS" if not active else "DATA", f"Active capability" if active else "Unavailable / immutable")
        elif obj is not None:
            active = boolean(obj)
            add(label, active, "RISK" if active is True and risky else "PASS" if active is False else "UNKNOWN", "Active capability" if active is True else "Unavailable" if active is False else "Not available")
        else:
            add(label, None, "UNKNOWN", "Not available")

    trusted = boolean(gp.get("trusted_token"))
    add("Trusted Token Flag", trusted, "DATA" if trusted is not None else "UNKNOWN", "Recognized trusted token" if trusted else "Not marked as trusted" if trusted is False else "Not available")
    return checks


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
        if network == "sui":
            symbol = meta.get("symbol")
            add("Object Type", meta.get("object_type"), "DATA" if meta.get("object_type") else "UNKNOWN", meta.get("object_type") or "Not available")
            add("Token Symbol / Name", symbol, "DATA" if symbol else "UNKNOWN", symbol or "Not available")
            add("Decimals", integer(meta.get("decimals")), "DATA" if meta.get("decimals") is not None else "UNKNOWN", str(integer(meta.get("decimals"))) if meta.get("decimals") is not None else "Not available")
            add("Total Supply", number(meta.get("supply")), "DATA" if meta.get("supply") is not None else "UNKNOWN", fmt_num(meta.get("supply")))
            add("Security Permissions", None, "UNKNOWN", "No verified Sui permission/security provider is available for this field")
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
    field_sources = data.get("_field_sources") or {}
    source_aliases = {
        "token_name": "Token Name", "token_symbol": "Token Symbol", "contract_name": "Contract Name",
        "holder_count": "Holder Count", "total_supply": "Total Supply", "decimals": "Decimals",
        "is_honeypot": "Honeypot", "is_open_source": "Source Code Open", "buy_tax": "Buy Tax",
        "sell_tax": "Sell Tax", "transfer_tax": "Transfer Tax", "is_mintable": "Mintable",
        "is_proxy": "Proxy", "creator_address": "Creator Address", "owner_address": "Owner Address",
    }
    def add(label, value, status="DATA", detail=None, key=None):
        source = field_sources.get(key) if key else None
        if source is None:
            source = next((src for k, src in field_sources.items() if source_aliases.get(k) == label), None)
        checks.append(metric(label, value, status, detail, source or data.get("_source") or "GoPlus"))

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

    # Holder concentration: raw Top 10 is kept for transparency, but LP/lock/burn/system
    # addresses are not treated as ordinary investor concentration.
    holders = data.get("holders")
    lp_holders = data.get("lp_holders")
    dexes = data.get("dex")
    if isinstance(holders, list) and holders:
        raw10, adjusted10, excluded_share, excluded_count, excluded_reasons = _concentration_metrics(holders, lp_holders, dexes)
        add("Raw Top 10 Concentration", raw10, "DATA" if raw10 is not None else "UNKNOWN", fmt_pct(raw10))
        add("Adjusted Investor Concentration", adjusted10, _concentration_status(adjusted10), fmt_pct(adjusted10))
        add("Excluded Non-Investor Share", excluded_share, "DATA" if excluded_share is not None else "UNKNOWN", fmt_pct(excluded_share))
        add("Excluded Holder Count", excluded_count, "DATA", str(excluded_count))
        for i, h in enumerate(holders[:10], 1):
            pct = _holder_pct(h.get("percent")) if isinstance(h, dict) else None
            excluded, reason = _holder_is_non_investor(h, {normalize_address(x.get("address")) for x in (lp_holders or []) if isinstance(x, dict) and x.get("address")}) if isinstance(h, dict) else (False, None)
            role = "NON-INVESTOR" if excluded else "ELIGIBLE HOLDER"
            if excluded and reason:
                role += f" — {reason}"
            detail = f"{h.get('address') or 'Not available'} — {fmt_pct(pct)} — {h.get('tag') or 'No tag'} — {role}"
            add(f"Top Holder #{i}", pct, "DATA" if pct is not None else "UNKNOWN", detail)
    else:
        add("Raw Top 10 Concentration", None, "UNKNOWN", "Not available")
        add("Adjusted Investor Concentration", None, "UNKNOWN", "Not available")
        add("Excluded Non-Investor Share", None, "UNKNOWN", "Not available")
        add("Excluded Holder Count", None, "UNKNOWN", "Not available")

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
    field_sources = pair.get("_field_sources") or {}
    def source_for(key, fallback="DEX Screener"):
        return field_sources.get(key) or fallback
    def add(label, value, status="DATA", detail=None, field_key=None):
        checks.append(metric(label, value, status, detail, source_for(field_key, fallback=(pair.get("_sources") or ["DEX Screener"])[0]) if field_key else (pair.get("_sources") or ["DEX Screener"])[0]))

    price = number(pair.get("priceUsd"))
    mc = number(pair.get("marketCap"))
    fdv = number(pair.get("fdv"))
    liq = number((pair.get("liquidity") or {}).get("usd"))
    base_liq = number((pair.get("liquidity") or {}).get("base"))
    # Liquidity is a risk signal only when it is critically low. Keep the
    # threshold explicit and conservative; missing liquidity stays UNKNOWN.

    quote_liq = number((pair.get("liquidity") or {}).get("quote"))
    price_native = number(pair.get("priceNative"))

    add("Price USD", price, "DATA", f"${fmt_num(price)}", "priceUsd")
    add("Price Native", price_native, "DATA", fmt_num(price_native), "priceNative")
    add("Market Cap", mc, "DATA", fmt_money(mc), "marketCap")
    add("FDV", fdv, "DATA", fmt_money(fdv), "fdv")
    add("Liquidity USD", liq, ("RISK" if liq is not None and liq < 5000 else ("CAUTION" if liq is not None and liq < 10000 else "DATA")), fmt_money(liq), "liquidity.usd")
    add("Liquidity Base", base_liq, "DATA", fmt_num(base_liq), "liquidity.base")
    add("Liquidity Quote", quote_liq, "DATA", fmt_num(quote_liq), "liquidity.quote")
    add("DEX", pair.get("dexId"), "DATA", pair.get("dexId") or "Not available", "dexId")
    add("Pair Address", pair.get("pairAddress"), "DATA", pair.get("pairAddress") or "Not available", "pairAddress")
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

def _score_band(value, bands):
    """Return the first score whose predicate accepts value."""
    if value is None:
        return None
    for predicate, score in bands:
        try:
            if predicate(value):
                return float(score)
        except Exception:
            continue
    return None


def _safe_pct_change(current, previous):
    if current is None or previous in (None, 0):
        return None
    return (current - previous) / abs(previous) * 100.0


def _historical_context(address, network):
    """Read our own snapshots without inventing history when it does not exist."""
    if not SessionLocal or not address or not network:
        return {"snapshots": [], "holders": []}
    session = SessionLocal()
    try:
        addr = normalize_address(address)
        cid = chain_id_for(network)
        snapshots = (session.query(MarketSnapshot)
                     .filter(MarketSnapshot.chain_id == cid,
                             MarketSnapshot.token_address == addr)
                     .order_by(MarketSnapshot.snapshot_time.asc())
                     .all())
        holders = (session.query(HolderSnapshot)
                   .filter(HolderSnapshot.chain_id == cid,
                           HolderSnapshot.token_address == addr)
                   .order_by(HolderSnapshot.snapshot_time.asc())
                   .all()) if 'HolderSnapshot' in globals() else []
        return {"snapshots": snapshots, "holders": holders}
    except Exception as exc:
        print(f"⚠️ scoring history read error: {exc}")
        return {"snapshots": [], "holders": []}
    finally:
        session.close()


def _security_score(checks):
    """
    Security component: 25 points.

    Security checks are evidence-based. PASS earns full credit, CAUTION half,
    RISK zero. DATA/UNKNOWN are not treated as risks, but limited coverage
    reduces confidence and places a ceiling on how much of the security
    component can be considered verified.

    This avoids both extremes:
      - missing data must not be counted as a security failure;
      - a small number of PASS results must not automatically become 25/25.
    """
    checks = list(checks or [])
    if not checks:
        return None, 0.0, 0, 0, 0

    p = sum(x.get("status") == "PASS" for x in checks)
    c = sum(x.get("status") == "CAUTION" for x in checks)
    r = sum(x.get("status") == "RISK" for x in checks)
    verified = p + c + r
    coverage = verified / len(checks) * 100.0

    if verified == 0:
        return None, coverage, p, c, r

    verified_quality = (p + 0.5 * c) / verified
    # Coverage modifier: verified tests receive their normal quality score,
    # but sparse security evidence cannot claim the full 25 points.
    # At 100% coverage -> 1.00; at 0% -> 0.50 (but no verified tests returns None).
    coverage_factor = 0.50 + 0.50 * (coverage / 100.0)
    earned = 25.0 * verified_quality * coverage_factor

    return earned, coverage, p, c, r


def _market_score(market_data, history):
    """Balanced 75-point market score.

    Current market facts are scored directly when historical trend data is
    unavailable. Historical trend points are only awarded when a real prior
    observation exists. No MARSOF observation is called an ATH.
    """
    m = market_data or {}
    price = number(m.get("price"))
    mc = number(m.get("market_cap"))
    liq = number(m.get("liquidity"))
    vol = number(m.get("volume_24h"))
    buys = number(m.get("buys_24h"))
    sells = number(m.get("sells_24h"))
    liq_mc = number(m.get("liq_mc"))
    vol_mc = number(m.get("vol_mc"))

    snaps = history.get("snapshots") or []
    previous = snaps[-1] if snaps else None

    price_24h = (
        _safe_pct_change(price, previous.price_usd)
        if previous and price is not None and previous.price_usd is not None
        else number(m.get("price_change_24h"))
    )
    liq_24h = (
        _safe_pct_change(liq, previous.liquidity_usd)
        if previous and liq is not None and previous.liquidity_usd is not None
        else None
    )
    mc_24h = (
        _safe_pct_change(mc, previous.market_cap_usd)
        if previous and mc is not None and previous.market_cap_usd is not None
        else None
    )

    seven_days = None
    if snaps:
        target = utc_now() - timedelta(days=7)
        eligible = [
            s for s in snaps
            if s.snapshot_time and s.snapshot_time <= target
        ]
        if eligible:
            seven_days = eligible[-1]

    price_7d = (
        _safe_pct_change(price, seven_days.price_usd)
        if seven_days and price is not None and seven_days.price_usd is not None
        else None
    )
    liq_7d = (
        _safe_pct_change(liq, seven_days.liquidity_usd)
        if seven_days and liq is not None and seven_days.liquidity_usd is not None
        else None
    )
    mc_7d = (
        _safe_pct_change(mc, seven_days.market_cap_usd)
        if seven_days and mc is not None and seven_days.market_cap_usd is not None
        else None
    )

    # 20 points: price direction.
    # If no historical change exists, this component is genuinely unverified.
    if price_7d is not None:
        trend_score = _score_band(price_7d, [
            (lambda x: x >= 50, 20), (lambda x: x >= 20, 18),
            (lambda x: x >= 5, 16), (lambda x: x > -5, 13),
            (lambda x: x > -20, 9), (lambda x: x > -50, 4),
            (lambda x: True, 0)])
    elif price_24h is not None:
        trend_score = _score_band(price_24h, [
            (lambda x: x >= 20, 17), (lambda x: x >= 5, 15),
            (lambda x: x >= 0, 13), (lambda x: x > -10, 9),
            (lambda x: x > -30, 5), (lambda x: True, 0)])
    else:
        trend_score = None

    # 15 points: market cap.
    # Absolute MC is context, not an automatic risk. The score rewards
    # reasonable scale while avoiding the assumption that "bigger = safer".
    if mc_7d is not None:
        mc_score = _score_band(mc_7d, [
            (lambda x: x >= 30, 15), (lambda x: x >= 10, 13),
            (lambda x: x >= 0, 11), (lambda x: x > -20, 8),
            (lambda x: x > -50, 4), (lambda x: True, 0)])
    elif mc_24h is not None:
        mc_score = _score_band(mc_24h, [
            (lambda x: x >= 20, 15), (lambda x: x >= 5, 13),
            (lambda x: x >= 0, 11), (lambda x: x > -15, 8),
            (lambda x: x > -40, 4), (lambda x: True, 0)])
    elif mc is not None:
        mc_score = _score_band(mc, [
            (lambda x: x >= 1000000, 15),
            (lambda x: x >= 500000, 13),
            (lambda x: x >= 250000, 11),
            (lambda x: x >= 100000, 9),
            (lambda x: x >= 50000, 7),
            (lambda x: x >= 10000, 4),
            (lambda x: True, 2)])
    else:
        mc_score = None

    # 15 points: liquidity quality.
    # Both liquidity/MC and absolute liquidity matter. 40% liquidity/MC with
    # ~$84K absolute liquidity should score as healthy, not risky.
    if liq_mc is not None:
        liq_ratio_score = _score_band(liq_mc, [
            (lambda x: x >= 50, 15), (lambda x: x >= 30, 14),
            (lambda x: x >= 20, 12), (lambda x: x >= 10, 10),
            (lambda x: x >= 5, 7), (lambda x: x >= 2, 4),
            (lambda x: x >= 1, 2), (lambda x: True, 0)])
    else:
        liq_ratio_score = None

    if liq is not None:
        absolute_score = _score_band(liq, [
            (lambda x: x >= 250000, 15),
            (lambda x: x >= 100000, 14),
            (lambda x: x >= 50000, 12),
            (lambda x: x >= 25000, 10),
            (lambda x: x >= 10000, 7),
            (lambda x: x >= 5000, 4),
            (lambda x: True, 0)])
    else:
        absolute_score = None

    if liq_ratio_score is not None and absolute_score is not None:
        liquidity_score = round((liq_ratio_score * 0.60) + (absolute_score * 0.40), 2)
    else:
        liquidity_score = liq_ratio_score if liq_ratio_score is not None else absolute_score

    # 10 points: volume + buy/sell pressure.
    pressure = (
        buys / sells
        if buys is not None and sells is not None and sells > 0
        else None
    )
    if vol_mc is not None:
        base_volume = _score_band(vol_mc, [
            (lambda x: x >= 50, 7), (lambda x: x >= 20, 6),
            (lambda x: x >= 10, 5), (lambda x: x >= 5, 4),
            (lambda x: x >= 1, 3), (lambda x: x > 0, 1),
            (lambda x: True, 0)])
    elif vol is not None and mc not in (None, 0):
        ratio = vol / mc * 100.0
        base_volume = _score_band(ratio, [
            (lambda x: x >= 20, 7), (lambda x: x >= 10, 6),
            (lambda x: x >= 5, 5), (lambda x: x >= 2, 4),
            (lambda x: x > 0, 2), (lambda x: True, 0)])
    elif vol is not None:
        base_volume = _score_band(vol, [
            (lambda x: x >= 50000, 7), (lambda x: x >= 10000, 5),
            (lambda x: x >= 1000, 3), (lambda x: x > 0, 1),
            (lambda x: True, 0)])
    else:
        base_volume = None

    if pressure is not None:
        pressure_bonus = (
            3 if pressure >= 1.50 else
            2 if pressure >= 1.10 else
            1 if pressure >= 0.90 else
            0
        )
        volume_score = min(10.0, (base_volume or 0) + pressure_bonus)
    else:
        volume_score = base_volume

    # 10 points: holder flow. Only scored when two real snapshot groups exist.
    holder_delta = None
    holder_score = None
    holder_rows = history.get("holders") or []
    if holder_rows:
        groups = {}
        for row in holder_rows:
            key = row.snapshot_time
            if key is not None:
                groups.setdefault(key, set()).add(row.wallet)
        times = sorted(groups)
        if len(times) >= 2:
            old = len(groups[times[-2]])
            new = len(groups[times[-1]])
            if old > 0:
                holder_delta = (new - old) / old * 100.0
                holder_score = _score_band(holder_delta, [
                    (lambda x: x >= 20, 10), (lambda x: x >= 5, 8),
                    (lambda x: x >= 0, 6), (lambda x: x > -15, 4),
                    (lambda x: x > -30, 2), (lambda x: True, 0)])

    # 5 points: LP/liquidity stability.
    liquidity_change = liq_7d if liq_7d is not None else liq_24h
    if liquidity_change is not None:
        lp_score = _score_band(liquidity_change, [
            (lambda x: x >= 20, 5), (lambda x: x >= 5, 4),
            (lambda x: x >= -5, 3), (lambda x: x > -20, 2),
            (lambda x: x > -50, 1), (lambda x: True, 0)])
    else:
        lp_score = None

    parts = {
        "price_trend": (trend_score, 20.0),
        "market_cap": (mc_score, 15.0),
        "liquidity": (liquidity_score, 15.0),
        "volume_pressure": (volume_score, 10.0),
        "holders": (holder_score, 10.0),
        "lp_stability": (lp_score, 5.0),
    }

    verified_weight = sum(weight for score, weight in parts.values() if score is not None)
    earned = sum(score for score, weight in parts.values() if score is not None)
    market_score = earned if verified_weight > 0 else None

    severe_liquidity = (
        (liq is not None and liq < 5000) or
        (liq_mc is not None and liq_mc < 1)
    )
    liquidity_collapse = (
        liquidity_change is not None and liquidity_change <= -50
    )
    holder_exodus = holder_delta is not None and holder_delta <= -20

    facts = {
        "drawdown_market_cap": None,
        "drawdown_price": None,
        "recovery_market_cap": None,
        "recovery_price": None,
        "dead_market": False,
        "severe_liquidity": severe_liquidity,
        "liquidity_collapse": liquidity_collapse,
        "liquidity_change_7d": liq_7d,
        "liquidity_change_24h": liq_24h,
        "holder_delta": holder_delta,
        "holder_exodus": holder_exodus,
        "ath_available": False,
        "parts": parts,
        "market_verified_weight": verified_weight,
        "market_max_weight": 75.0,
        "price_change_24h": price_24h,
        "price_change_7d": price_7d,
        "market_cap_change_24h": mc_24h,
        "market_cap_change_7d": mc_7d,
    }
    return market_score, facts



def _score_verdict(score, critical=False, dead_market=False, unavailable=False):
    if dead_market:
        return "RISK"
    if critical:
        return "RISK"
    if score is None:
        return "INCOMPLETE" if unavailable else "CAUTION"
    if score < 20:
        return "RISK"
    if score < 40:
        return "RISK"
    if score < 60:
        return "CAUTION"
    return "PASS"


def classify_all(security_checks, market_checks, market_data=None, address=None, network=None):
    security_checks = list(security_checks or [])
    market_checks = list(market_checks or [])
    base_all = security_checks + market_checks

    security_score, security_coverage, sp, sc, sr = _security_score(security_checks)
    try:
        history = _historical_context(address, network) if address and network else {"snapshots": [], "holders": []}
        market_score, facts = _market_score(market_data or {}, history)
    except Exception as exc:
        print(f"⚠️ scoring engine fallback: {exc}")
        history = {"snapshots": [], "holders": []}
        market_score, facts = None, {
            "drawdown_market_cap": None, "drawdown_price": None,
            "recovery_market_cap": None, "recovery_price": None,
            "dead_market": False, "severe_liquidity": False,
            "liquidity_collapse": False, "liquidity_change_7d": None,
            "liquidity_change_24h": None, "holder_delta": None,
            "holder_exodus": False, "ath_available": False,
            "parts": {k: (None, w) for k, w in {
                "price_trend":20.0,"market_cap":15.0,"liquidity":15.0,
                "volume_pressure":10.0,"holders":10.0,"lp_stability":5.0}.items()}
        }

    # Add explanatory market risk flags without changing the underlying raw metrics.
    generated = []
    if facts.get("severe_liquidity"):
        generated.append(metric("Severely Low Liquidity", facts.get("severe_liquidity"), "RISK",
            f"Current liquidity is {fmt_money((market_data or {}).get('liquidity'))}.", "Market data"))
    elif facts.get("liquidity_collapse"):
        generated.append(metric("Liquidity Collapse", facts.get("liquidity_change_7d") or facts.get("liquidity_change_24h"), "RISK",
            "Liquidity has fallen sharply in the available historical period.", "MARSOF historical snapshots"))
    if facts.get("holder_exodus"):
        generated.append(metric("Holder Exodus", facts.get("holder_delta"), "RISK",
            f"Top-holder snapshot count decreased by {abs(facts['holder_delta'])} wallets between the latest stored snapshots.",
            "MARSOF holder snapshots"))
    market_checks.extend(generated)
    all_checks = security_checks + market_checks

    available = [x for x in all_checks if x.get("status") != "UNKNOWN"]
    risk_tests = [x for x in all_checks if x.get("status") in {"PASS", "CAUTION", "RISK"}]
    missing = [x.get("label") for x in all_checks if x.get("status") == "UNKNOWN"]
    data_only = [x for x in all_checks if x.get("status") == "DATA"]
    p = sum(x.get("status") == "PASS" for x in risk_tests)
    c = sum(x.get("status") == "CAUTION" for x in risk_tests)
    r = sum(x.get("status") == "RISK" for x in risk_tests)
    coverage = (len(available) / len(all_checks) * 100) if all_checks else 0

    critical_security = sr > 0
    section_scores = {"security": security_score, "market": market_score}
    earned_score = sum(v for v in section_scores.values() if v is not None)
    overall_score = earned_score if section_scores else None

    # Hard caps: verified critical risk always dominates the numerical score.
    cap_reasons = []
    if critical_security:
        overall_score = min(overall_score if overall_score is not None else 0, 35.0)
        cap_reasons.append("Verified contract security risk")
    if facts.get("severe_liquidity"):
        overall_score = min(overall_score if overall_score is not None else 0, 45.0)
        cap_reasons.append("Severely low liquidity")

    # Verdict bands:
    # 0-39 = RISK, 40-59 = CAUTION, 60-79 = PASS/no major danger found,
    # 80-100 = strong result.  Coverage is shown separately so users can
    # distinguish a score from confidence in the available evidence.
    verdict = _score_verdict(
        overall_score,
        critical=critical_security,
        dead_market=facts.get("dead_market", False),
        unavailable=not available,
    )
    if overall_score is not None and overall_score < 60 and verdict == "PASS":
        verdict = "CAUTION"

    market_verified_weight = float(facts.get("market_verified_weight") or 0.0)
    score_evidence_weight = (
        market_verified_weight + (25.0 if security_score is not None else 0.0)
    )

    # Keep the old counts/coverage fields intact for the existing UI and history.
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
        "score_evidence_coverage": (
            len(risk_tests) / len(all_checks) * 100.0 if all_checks else 0.0
        ),
        "score_evidence_weight": round(score_evidence_weight, 2),
        "market_score_weight": round(market_verified_weight, 2),
        "score_note": (
            "Missing or unverified fields are not rescaled into earned points."
        ),
        "verdict": verdict,
        "score": round(overall_score, 2) if overall_score is not None else None,
        "score_max": 100,
        "score_components": {
            "security": round(security_score, 2) if security_score is not None else None,
            "market": round(market_score, 2) if market_score is not None else None,
            "price_trend": round(facts["parts"]["price_trend"][0], 2) if facts["parts"]["price_trend"][0] is not None else None,
            "market_cap": round(facts["parts"]["market_cap"][0], 2) if facts["parts"]["market_cap"][0] is not None else None,
            "liquidity": round(facts["parts"]["liquidity"][0], 2) if facts["parts"]["liquidity"][0] is not None else None,
            "volume_pressure": round(facts["parts"]["volume_pressure"][0], 2) if facts["parts"]["volume_pressure"][0] is not None else None,
            "holders": round(facts["parts"]["holders"][0], 2) if facts["parts"]["holders"][0] is not None else None,
            "lp_stability": round(facts["parts"]["lp_stability"][0], 2) if facts["parts"]["lp_stability"][0] is not None else None,
        },
        "market_facts": facts,
        "score_cap_reasons": cap_reasons,
        "score_method": "Security 25 + Market 75; ATH/drawdown is excluded until a verified external ATH source is available; unavailable sections are excluded and remaining weight is normalized; critical risks can cap the total score.",
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
            raw_data=json.dumps({"security": data, "market": market, "classification": classification}, ensure_ascii=False, default=str),
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
        security_data = dict(security_raw or {})
        security_data.setdefault("_field_sources", {})

        # Secondary sources only fill fields that GoPlus/RPC did not provide.
        honeypot_raw = fetch_honeypot(address, network)
        if honeypot_raw:
            _merge_security_missing(security_data, normalize_honeypot_data(honeypot_raw), "Honeypot.is")

        sourcify_raw = fetch_sourcify(address, network)
        if sourcify_raw:
            _merge_security_missing(security_data, normalize_sourcify_data(sourcify_raw, cfg.get("rpc")), "Sourcify")

        metadata = fetch_evm_metadata(address, network)
        _merge_security_missing(security_data, metadata, "EVM RPC")
        blockscout_v2 = fetch_blockscout_v2(address, network)
        extra_sources = [
            ("Blockscout v2", blockscout_v2),
            ("Blockscout", fetch_blockscout_token(address, network)),
            ("Alchemy", fetch_alchemy_token(address, network)),
            ("Explorer API", fetch_explorer_token(address, network)),
        ]
        for source_name, payload in extra_sources:
            if payload:
                clean_payload = {k:v for k,v in payload.items() if not str(k).startswith("_")}
                _merge_security_missing(security_data, clean_payload, source_name)
        security_data["_source"] = ", ".join(dict.fromkeys([
            "GoPlus" if security_raw else "",
            "Honeypot.is" if honeypot_raw else "",
            "Sourcify" if sourcify_raw else "",
            "EVM RPC" if metadata else "",
            *[name for name, payload in extra_sources if payload],
        ]))
        security_data["_source"] = security_data["_source"].strip(" ,") or "Unavailable"
        security_checks = build_security(security_data) if security_data else build_non_bsc_security({}, network)
    elif network == "solana":
        # GoPlus now exposes a dedicated Solana Token Security API (Beta).
        # Use it first, then supplement missing values with Solana RPC.
        security_raw = fetch_goplus_solana(address)
        onchain = fetch_solana_metadata(address)
        security_data = dict(security_raw or {})
        security_data["_field_sources"] = {}
        if onchain:
            for k, v in onchain.items():
                if v is not None and security_data.get(k) is None:
                    security_data[k] = v
                    security_data["_field_sources"][k] = "Solana RPC"
        alchemy_holders = fetch_alchemy_solana_holders(address)
        if alchemy_holders and not onchain.get("largest_accounts"):
            onchain["largest_accounts"] = alchemy_holders[:10]
            security_data["_field_sources"]["largest_accounts"] = "Alchemy Solana"
        helius = fetch_helius_token(address)
        birdeye = fetch_birdeye_token(address)
        for source_name, payload in (("Helius", helius), ("Birdeye", birdeye)):
            if payload:
                _merge_security_missing(security_data, payload, source_name)
        security_data["_source"] = ", ".join(dict.fromkeys([
            "GoPlus Solana" if security_raw else "", "Solana RPC" if onchain else "",
            "Alchemy Solana" if alchemy_holders else "", "Helius" if helius else "", "Birdeye" if birdeye else ""
        ])).strip(" ,") or "Unavailable"
        security_data["_onchain"] = onchain
        security_checks = build_solana_security(security_data, onchain)
    else:
        if network == "sui":
            security_raw = fetch_goplus_sui(address)
            metadata = fetch_sui_metadata(address)
            alchemy_sui = fetch_alchemy_sui_metadata(address)
            security_data = dict(security_raw or {})
            security_data["_field_sources"] = {}
            _merge_security_missing(security_data, metadata, "Sui RPC")
            _merge_security_missing(security_data, alchemy_sui, "Alchemy Sui")
            security_data["_source"] = ", ".join(dict.fromkeys([
                "GoPlus Sui" if security_raw else "", "Sui RPC" if metadata else "", "Alchemy Sui" if alchemy_sui else ""
            ])).strip(" ,") or "Unavailable"
            security_data["_onchain"] = metadata
            security_checks = build_sui_security(security_data) if security_data else build_non_bsc_security(metadata, network)
        else:
            security_data = {}
            security_checks = build_non_bsc_security({}, network)

    pairs = fetch_dexscreener(address, network)
    market_pair = choose_market(pairs)
    # Blockscout v2 is an independent indexed fallback, especially useful on
    # Robinhood Chain. It fills only fields still missing after DEX/Gecko/CoinGecko/DeFiLlama.
    if cfg.get("kind") == "evm":
        bs_market = blockscout_v2 if "blockscout_v2" in locals() else fetch_blockscout_v2(address, network)
        market_pair = merge_blockscout_market(market_pair, bs_market)
    market = build_market(market_pair)
    result = {
        "address": normalize_address(address),
        "network": network,
        "network_name": network_label(network),
        "security": security_data,
        "market": market,
        "classification": classify_all(security_checks, market["checks"], market.get("data"), address, network),
    }
    result["market"] = enrich_historical_market(market, address, network)
    result["classification"] = classify_all(security_checks, result["market"]["checks"], result["market"].get("data"), address, network)
    # Persist the final verified scan, including the classification used by the UI.
    save_analysis(address, security_data, result["classification"], result["market"]["data"], network)
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
        scans=(session.query(SecurityScan).join(Token, SecurityScan.token_id==Token.id).filter(Token.contract_address==addr, Token.chain_id==cid).order_by(SecurityScan.scanned_at.desc()).limit(2).all())
        if len(rows)<2:
            return "<b>📊 COMPARISON</b>\n\nNo previous scan is available yet.\nThe next scan will create the comparison baseline."
        cur, prev=rows[0], rows[1]
        def ch(a,b): return ((a-b)/b*100) if a is not None and b not in (None,0) else None
        def signed(x): return "Not available" if x is None else f"{x:+.2f}%"
        age=elapsed_text(prev.snapshot_time)
        alerts=[]
        pc=ch(cur.price_usd,prev.price_usd)
        lc=ch(cur.liquidity_usd,prev.liquidity_usd)
        mc=ch(cur.market_cap_usd,prev.market_cap_usd)
        vol=ch(cur.volume_24h_usd,prev.volume_24h_usd)
        if pc is not None and pc<=-50: alerts.append(f"🔴 <b>PRICE DROP</b>\nPrice dropped {abs(pc):.2f}% since the previous scan.")
        if lc is not None and lc<=-50: alerts.append(f"🔴 <b>LIQUIDITY DROP</b>\nLiquidity fell {abs(lc):.2f}% since the previous scan.")

        def risk_labels(scan):
            if not scan: return set()
            try:
                raw=json.loads(scan.raw_data or "{}")
                classification=raw.get("classification") or {}
                return {x.get("label") for x in classification.get("all",[]) if x.get("status")=="RISK" and x.get("label")}
            except Exception:
                return set()
        current_risks=risk_labels(scans[0] if scans else None)
        previous_risks=risk_labels(scans[1] if len(scans)>1 else None)
        new_risks=sorted(current_risks-previous_risks)
        resolved=sorted(previous_risks-current_risks)
        if new_risks:
            alerts.append("🔴 <b>NEW RISK INDICATORS</b>\n" + "\n".join(f"• {x}" for x in new_risks))
        lines=["<b>📊 COMPARISON</b>","",f"Previous scan: <b>{age}</b>","",f"💵 Price: <b>{fmt_money(prev.price_usd)} → {fmt_money(cur.price_usd)}</b> {signed(pc)}",f"💧 Liquidity: <b>{fmt_money(prev.liquidity_usd)} → {fmt_money(cur.liquidity_usd)}</b> {signed(lc)}",f"💰 Market Cap: <b>{fmt_money(prev.market_cap_usd)} → {fmt_money(cur.market_cap_usd)}</b> {signed(mc)}",f"📈 24h Volume: <b>{fmt_money(prev.volume_24h_usd)} → {fmt_money(cur.volume_24h_usd)}</b> {signed(vol)}"]
        if alerts: lines += ["", "<b>Negative changes detected:</b>", *alerts]
        else: lines += ["", "🟢 No new negative movement or risk indicator was detected compared with the previous scan."]
        if resolved:
            lines += ["", "🟢 Risk indicators no longer present:", *[f"• {x}" for x in resolved]]
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


def compact_negative_changes(result):
    if not SessionLocal:
        return ""
    session=SessionLocal()
    try:
        addr=normalize_address(result.get("address")); cid=chain_id_for(result.get("network"))
        rows=(session.query(MarketSnapshot).filter(MarketSnapshot.chain_id==cid, MarketSnapshot.token_address==addr).order_by(MarketSnapshot.snapshot_time.desc()).limit(2).all())
        if len(rows)<2: return ""
        cur, prev=rows[0], rows[1]
        changes=[]
        def ch(a,b): return ((a-b)/b*100) if a is not None and b not in (None,0) else None
        pc=ch(cur.price_usd,prev.price_usd); lc=ch(cur.liquidity_usd,prev.liquidity_usd)
        if pc is not None and pc<=-50: changes.append(f"🔴 Price dropped {abs(pc):.2f}% since the previous scan")
        if lc is not None and lc<=-50: changes.append(f"🔴 Liquidity fell {abs(lc):.2f}% since the previous scan")
        scans=(session.query(SecurityScan).join(Token, SecurityScan.token_id==Token.id).filter(Token.contract_address==addr, Token.chain_id==cid).order_by(SecurityScan.scanned_at.desc()).limit(2).all())
        def risks(scan):
            if not scan: return set()
            try:
                raw=json.loads(scan.raw_data or "{}")
                return {x.get("label") for x in (raw.get("classification") or {}).get("all",[]) if x.get("status")=="RISK" and x.get("label")}
            except Exception: return set()
        if len(scans)>=2:
            new=sorted(risks(scans[0])-risks(scans[1]))
            changes.extend(f"🔴 New risk: {x}" for x in new)
        if not changes: return ""
        return "\n\n<b>⚠️ NEGATIVE CHANGES SINCE PREVIOUS SCAN</b>\n" + "\n".join(changes[:5])
    finally:
        session.close()

def build_home(result):
    d=result.get("security",{}) or {}
    c=result["classification"]
    m=result["market"]["data"]
    net=result.get("network_name", "Unknown")
    name=d.get("token_symbol") or d.get("token_name") or "Token"
    address=result["address"]
    risks=[x for x in c.get("all",[]) if x.get("status")=="RISK"]
    cautions=[x for x in c.get("all",[]) if x.get("status")=="CAUTION"]
    if c["verdict"]=="RISK":
        verdict_msg="<b>Risk indicators were detected.</b> The reasons are listed below."
        reason_lines=[f"🔴 {x.get('label')}: {x.get('detail') or 'Risk indicator detected'}" for x in risks[:4]]
        if len(risks)>4: reason_lines.append(f"🔴 +{len(risks)-4} additional risk indicator(s) in the full report")
        reasons="\n".join(reason_lines)
    elif c["verdict"]=="CAUTION":
        verdict_msg="<b>Attention indicators were detected.</b> Review the caution items before relying on the result."
        reason_lines=[f"🟠 {x.get('label')}: {x.get('detail') or 'Attention required'}" for x in cautions[:3]]
        if len(cautions)>3: reason_lines.append(f"🟠 +{len(cautions)-3} additional caution item(s)")
        reasons="\n".join(reason_lines)
    elif c["verdict"]=="PASS":
        verdict_msg="<b>NO MAJOR RISK DETECTED</b> — no major contract or market risk indicators were found among the completed tests that could be verified."
        reasons=""
    else:
        verdict_msg="<b>RISK STATUS UNVERIFIED</b> — there were not enough verified security tests to establish a risk result."
        reasons=""
    parts=[
        f"<b>MARSOF AI</b> — Advanced Token Analysis", "",
        f"<b>{name}</b> · <b>{net}</b>", f"<code>{address[:8]}…{address[-6:]}</code>", "",
        f"{verdict_icon(c['verdict'])} <b>{c['verdict']}</b>", verdict_msg,
        f"🎯 <b>Overall Score: {c.get('score'):.1f}/100</b>" if c.get('score') is not None else "🎯 <b>Overall Score: Not available</b>",
    ]
    if reasons: parts += ["", "<b>Main reasons:</b>", reasons]
    if c.get("missing"):
        parts += ["", "⚠️ <b>DATA LIMITATION</b>", "Some fields could not be verified from the available sources. The result is based only on completed tests."]
    parts += ["", f"🟢 {c['pass']} PASS   🟠 {c['caution']} CAUTION   🔴 {c['risk']} RISK", f"🔵 {c['data']} DATA   ⚪ {len(c['missing'])} UNAVAILABLE", "", f"💵 Price: <b>{fmt_money(m.get('price'))}</b>", f"💧 Liquidity: <b>{fmt_money(m.get('liquidity'))}</b>", f"💰 Market Cap: <b>{fmt_money(m.get('market_cap'))}</b>", f"📈 24h Volume: <b>{fmt_money(m.get('volume_24h'))}</b>",
        "", "📉 ATH Drawdown: <b>Not scored — verified ATH source unavailable</b>",
        f"💧 Liquidity / MC: <b>{fmt_pct(m.get('liq_mc'))}</b>" if m.get('liq_mc') is not None else "💧 Liquidity / MC: <b>Not available</b>",
        f"🔐 Security coverage: <b>{c.get('security_coverage', c.get('coverage',0)):.1f}%</b>", "", "Real values are shown as DATA. Missing values are never converted to zero."]
    return "\n".join(parts)

def section_text(result, category, page=0, page_size=8):
    checks = result.get("classification", {}).get("all", [])
    if category == "security":
        checks = result.get("classification", {}).get("security_checks", [])
    elif category == "market":
        checks = result.get("classification", {}).get("market_checks", [])
    elif category == "holders":
        checks = [x for x in checks if any(k in x.get("label", "").lower() for k in ("holder", "concentration", "lp", "top"))]
    elif category == "risk":
        checks = [x for x in checks if x.get("status") == "RISK"]
    elif category == "missing":
        checks = [x for x in checks if x.get("status") == "UNKNOWN"]
    elif category == "full":
        checks = checks

    title = {"security":"🔐 SECURITY", "market":"💰 MARKET", "holders":"👛 HOLDERS", "risk":"🔴 RISK INDICATORS", "missing":"⚪ UNAVAILABLE", "full":"📋 FULL REPORT"}.get(category, "📋 REPORT")
    total_pages = max(1, (len(checks) + page_size - 1) // page_size)
    page = max(0, min(page, total_pages - 1))
    chunk = checks[page * page_size:(page + 1) * page_size]
    lines = [f"<b>{title}</b>", ""]
    if not checks:
        if category == "risk":
            lines.append("🟢 No risk indicators were detected among the completed tests.")
        elif category == "missing":
            lines.append("There are no currently unavailable fields to report.")
        else:
            lines.append("⚪ No verified data is available for this section.")
    else:
        for x in chunk:
            lines.append(f"{verdict_icon(x.get('status'))} <b>{x.get('label','')}</b>")
            lines.append(f"   {x.get('detail') or 'Not available'}")
    if total_pages > 1:
        lines += ["", f"Page <b>{page + 1}/{total_pages}</b>"]
    return "\n".join(lines), total_pages


def keyboard(result=None, category=None, page=0, total_pages=1):
    kb = types.InlineKeyboardMarkup(row_width=2)
    if category:
        if page > 0:
            kb.row(types.InlineKeyboardButton("⬅️ Back", callback_data=f"v:{category}:{page-1}"), types.InlineKeyboardButton("➡️ Next", callback_data=f"v:{category}:{page+1}"))
        elif total_pages > 1:
            kb.row(types.InlineKeyboardButton("➡️ Next", callback_data=f"v:{category}:1"))
        kb.row(types.InlineKeyboardButton("🏠 Overview", callback_data="v:home"))
        return kb
    kb.row(types.InlineKeyboardButton("🔐 Security",callback_data="v:security:0"),types.InlineKeyboardButton("💰 Market",callback_data="v:market:0"))
    kb.row(types.InlineKeyboardButton("👛 Holders",callback_data="v:holders:0"),types.InlineKeyboardButton("📊 Comparison",callback_data="v:comparison:0"))
    kb.row(types.InlineKeyboardButton("🔴 Risk",callback_data="v:risk:0"),types.InlineKeyboardButton("📋 Full Report",callback_data="v:full:0"))
    kb.row(types.InlineKeyboardButton("📚 Scan History",callback_data="db:list:0"),types.InlineKeyboardButton("🔍 Analyze Another",callback_data="menu:analyse"))
    return kb

def main_keyboard():
    kb=types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton("🌐 Website",url="https://marsof.ct.ws/?i=1"),
        types.InlineKeyboardButton("𝕏 X / Twitter",url="https://x.com/MARSOF_AI"),
    )
    kb.row(types.InlineKeyboardButton("🔍 Analyze Token",callback_data="menu:analyse"))
    kb.row(types.InlineKeyboardButton("📚 Scan History",callback_data="db:list:0"))
    kb.row(types.InlineKeyboardButton("✈️ Telegram",url="https://t.me/marsofai"))
    return kb


def build_start():
    return (
        "<b>MARSOF AI</b>\n"
        "<b>AI-ASSISTED CRYPTO INTELLIGENCE</b>\n\n"
        "Analyze a cryptocurrency token with real-time market and security data.\n"
        "MARSOF AI checks available on-chain and market signals, highlights risk indicators, and keeps missing data clearly marked instead of inventing values.\n\n"
        "<b>Start in seconds:</b> tap <b>Analyze Token</b>, paste the contract address, and MARSOF AI will detect the network from verified data before the scan.\n\n"
        "🟢 PASS = no detected risk indicator in the completed test\n"
        "🟠 CAUTION = attention required\n"
        "🔴 RISK = risk indicator detected\n"
        "🔵 DATA = verified measurement\n"
        "⚪ UNAVAILABLE = could not be verified\n\n"
        "<b>AI-assisted token analysis. No registration required.</b>"
    )


def build_analyze_prompt():
    return (
        "<b>🔍 ANALYZE TOKEN</b>\n\n"
        "<b>Step 1 — Contract Address</b>\n"
        "Send the token contract address below.\n\n"
        "You do <b>not</b> need to choose a network. <b>MARSOF AI will detect it automatically</b> using real indexed/on-chain data.\n\n"
        "⚡ <b>AI-ASSISTED ANALYSIS</b>\n"
        "Paste the contract address to continue."
    )


def build_detected_prompt(address, detections):
    primary=detections[0]["network"]
    primary_name=network_label(primary)
    names=[network_label(x["network"]) for x in detections]
    extra=("\n<b>Also detected on:</b> " + ", ".join(names[1:]) if len(names)>1 else "")
    return (
        "<b>🔎 NETWORK DETECTED</b>\n\n"
        f"<b>Contract</b>\n<code>{address[:10]}…{address[-8:]}</code>\n\n"
        f"🌐 <b>Network:</b> {primary_name}" + extra + "\n\n"
        "MARSOF AI identified the network from real data.\n"
        "Ready to run the <b>AI scan</b>?"
    )


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
        return {"ok": False, "error": "TELEGRAM_TOKEN is not configured"}
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
        return {"ok": False, "error": "TELEGRAM_TOKEN is not configured"}
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
# DATABASE HISTORY UI
# ============================================================

def history_tokens_for_network(network_key):
    if not SessionLocal or network_key not in NETWORKS:
        return []
    session=SessionLocal()
    try:
        chain_id=str(NETWORKS[network_key]["chain_id"])
        rows=session.query(Token).filter(Token.chain_id==chain_id).order_by(Token.last_scanned.desc().nullslast(), Token.id.desc()).all()
        return rows
    finally:
        session.close()


def history_networks():
    if not SessionLocal:
        return []
    session=SessionLocal()
    try:
        rows=session.query(Token).order_by(Token.last_scanned.desc().nullslast(), Token.id.desc()).all()
        counts={}
        for token in rows:
            counts[str(token.chain_id)]=counts.get(str(token.chain_id),0)+1
        result=[]
        for key,cfg in NETWORKS.items():
            count=counts.get(str(cfg["chain_id"]),0)
            if count:
                result.append((key,cfg["name"],count))
        return result
    finally:
        session.close()


def history_network_text():
    networks=history_networks()
    lines=["<b>📚 SCAN HISTORY</b>","","Select a network to view its scanned tokens."]
    if not networks:
        lines += ["","No completed scans are stored yet."]
    return "\n".join(lines), networks


def history_network_keyboard(networks):
    kb=types.InlineKeyboardMarkup(row_width=2)
    for key,name,count in networks:
        kb.add(types.InlineKeyboardButton(f"🌐 {name} · {count}",callback_data=f"db:net:{key}"))
    kb.add(types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home"))
    return kb


def history_token_text(network_key):
    rows=history_tokens_for_network(network_key)
    name=network_label(network_key)
    lines=[f"<b>📚 {name.upper()} — SCAN HISTORY</b>","","Select a token to open its latest saved result."]
    if not rows:
        lines += ["","No completed scans are stored for this network."]
    for token in rows:
        scan=latest_scan_for_token(token.id)
        verdict=scan.verdict if scan else "INCOMPLETE"
        label=token.symbol or token.name or token.contract_address[:10]+"…"
        lines.append(f"{verdict_icon(verdict)} <b>{label}</b> · {verdict}")
        lines.append(f"<code>{token.contract_address[:8]}…{token.contract_address[-6:]}</code> · {elapsed_text(scan.scanned_at) if scan else 'Not scanned'}")
    return "\n".join(lines), rows


def history_token_keyboard(network_key, rows):
    kb=types.InlineKeyboardMarkup(row_width=1)
    for token in rows:
        scan=latest_scan_for_token(token.id)
        verdict=scan.verdict if scan else "INCOMPLETE"
        label=token.symbol or token.name or token.contract_address[:10]+"…"
        kb.add(types.InlineKeyboardButton(f"{verdict_icon(verdict)} {label} · {verdict}",callback_data=f"db:token:{token.id}"))
    kb.row(types.InlineKeyboardButton("⬅️ Networks",callback_data="db:list:0"),types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home"))
    return kb


def load_scan_result(scan_id):
    if not SessionLocal: return None
    session=SessionLocal()
    try:
        scan=session.query(SecurityScan).filter(SecurityScan.id==scan_id).first()
        if not scan: return None
        token=session.query(Token).filter(Token.id==scan.token_id).first()
        if not token: return None
        raw=json.loads(scan.raw_data or "{}")
        security=raw.get("security") or {}
        market_data=raw.get("market") or {}
        classification=raw.get("classification")
        if not classification or not classification.get("all"):
            security_checks=build_security(security) if security else []
            pair=market_data.get("pair") if isinstance(market_data,dict) else None
            market=build_market(pair) if pair else {"checks":[],"data":market_data}
            classification=classify_all(security_checks, market.get("checks",[]))
            market["data"]=market_data
        else:
            pair=market_data.get("pair") if isinstance(market_data,dict) else None
            market={"checks":classification.get("market_checks",[]),"data":market_data}
            if not market["checks"] and pair:
                market=build_market(pair)
                market["data"]=market_data
        return {"address":token.contract_address,"network":next((k for k,c in NETWORKS.items() if c.get("chain_id")==str(token.chain_id)), token.chain_id),"network_name":network_label_by_chain_id(token.chain_id),"security":security,"market":market,"classification":classification,"historical_scan_id":scan.id,"historical_scanned_at":scan.scanned_at}
    except Exception as exc:
        print(f"❌ load scan result: {exc}")
        return None
    finally:
        session.close()

def latest_scan_for_token(token_id):
    if not SessionLocal: return None
    session=SessionLocal()
    try:
        return session.query(SecurityScan).filter(SecurityScan.token_id==token_id).order_by(SecurityScan.scanned_at.desc()).first()
    finally:
        session.close()

def send_home_view(chat_id, old_message_id=None):
    if old_message_id:
        try: bot.delete_message(chat_id, old_message_id)
        except Exception: pass
    try:
        with open(LOGO_PATH, "rb") as logo:
            sent=bot.send_photo(chat_id, logo, caption=build_start(), parse_mode="HTML", reply_markup=main_keyboard())
    except Exception:
        sent=bot.send_message(chat_id, build_start(), parse_mode="HTML", reply_markup=main_keyboard())
    bot._marsof_ui.setdefault(chat_id,{})
    bot._marsof_ui[chat_id].update({"message_id":sent.message_id,"message_type":"photo","phase":"home","network":None,"result":None,"address":None,"detections":None})
    return sent

def replace_with_text(chat_id, old_message_id, text_value, reply_markup=None):
    try:
        bot.delete_message(chat_id, old_message_id)
    except Exception: pass
    try:
        sent=bot.send_message(chat_id, text_value, parse_mode="HTML", reply_markup=reply_markup)
    except Exception:
        sent=bot.send_message(chat_id, text_value, reply_markup=reply_markup)
    bot._marsof_ui.setdefault(chat_id,{})["message_id"]=sent.message_id
    bot._marsof_ui[chat_id]["message_type"]="text"
    return sent

def edit_active_text(chat_id, message_id, text_value, reply_markup=None):
    try:
        bot.edit_message_text(text_value, chat_id, message_id, parse_mode="HTML", reply_markup=reply_markup)
        return message_id
    except Exception:
        return replace_with_text(chat_id, message_id, text_value, reply_markup).message_id

# ============================================================
# TELEGRAM
# ============================================================

if bot:
    @bot.message_handler(commands=["start"])
    def start(message):
        bot._marsof_ui=getattr(bot,"_marsof_ui",{})
        prev=bot._marsof_ui.get(message.chat.id)
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass
        send_home_view(message.chat.id, prev.get("message_id") if prev else None)

    @bot.message_handler(content_types=["text"])
    def text_handler(message):
        if not message.text or message.text.startswith("/"):
            return
        state=getattr(bot,"_marsof_ui",{}).get(message.chat.id,{})
        if state.get("phase") != "awaiting_address":
            return

        address=message.text.strip()
        try:
            bot.delete_message(message.chat.id, message.message_id)
        except Exception:
            pass

        detections=detect_networks(address)
        if not detections:
            edit_active_text(
                message.chat.id,
                state.get("message_id"),
                "⚠️ <b>Contract Not Verified</b>\n\nMARSOF AI could not verify this contract on the supported networks.\n\nNo network was guessed.",
                types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("⬅️ Back",callback_data="menu:analyse"),types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home"))
            )
            return

        state.update({"detections":detections,"address":address})
        bot._marsof_ui[message.chat.id]=state
        if len(detections) > 1:
            kb=types.InlineKeyboardMarkup(row_width=1)
            for item in detections:
                net=item["network"]
                kb.add(types.InlineKeyboardButton(f"🌐 {network_label(net)}",callback_data=f"net:{net}"))
            kb.add(types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home"))
            edit_active_text(message.chat.id,state["message_id"],f"<b>🔎 MULTIPLE NETWORKS DETECTED</b>\n\n<code>{address[:10]}…{address[-8:]}</code>\n\nMARSOF AI found this contract on multiple supported networks. Select the verified network to continue.",kb)
            state["phase"]="awaiting_network"
            bot._marsof_ui[message.chat.id]=state
            return

        network=detections[0]["network"]
        state.update({"network":network,"phase":"scanning"})
        bot._marsof_ui[message.chat.id]=state
        edit_active_text(
            message.chat.id,
            state["message_id"],
            f"<b>🔎 CONTRACT RECEIVED</b>\n\n<code>{address[:10]}…{address[-8:]}</code>\n\n🌐 <b>{network_label(network)}</b>\n\n⏳ <b>Running MARSOF AI analysis…</b>",
            None
        )
        try:
            bot.send_chat_action(message.chat.id,"typing")
            result=run_analysis(address,network)
            if not result:
                edit_active_text(message.chat.id,state["message_id"],"⚠️ <b>Analysis Unavailable</b>\n\nMARSOF AI could not complete a verified scan. No values were fabricated.",types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🔍 Analyze Another",callback_data="menu:analyse"),types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home")))
                state["phase"]="result"
                return
            state.update({"phase":"result","result":result,"network":network,"message_type":"text"})
            bot._marsof_ui[message.chat.id]=state
            new_id=edit_active_text(message.chat.id,state["message_id"],build_home(result),keyboard(result))
            state["message_id"]=new_id
            bot._marsof_ui[message.chat.id]=state
        except requests.RequestException as exc:
            print(f"❌ data source error: {exc}")
            edit_active_text(message.chat.id,state["message_id"],"⚠️ <b>Live Data Unavailable</b>\n\nA required live data source is temporarily unavailable.",types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🔍 Analyze Again",callback_data="menu:analyse"),types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home")))
        except Exception as exc:
            print(f"❌ analysis error: {exc}")
            edit_active_text(message.chat.id,state["message_id"],"⚠️ <b>Analysis Error</b>\n\nThe scan could not be completed.",types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🔍 Analyze Again",callback_data="menu:analyse"),types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home")))

    @bot.callback_query_handler(func=lambda call: call.data.startswith("menu:"))
    def menu_callbacks(call):
        try:
            action=call.data.split(":",1)[1]
            chat_id=call.message.chat.id
            state=bot._marsof_ui.get(chat_id,{})
            if action=="analyse":
                state.update({"phase":"awaiting_address","network":None,"result":None,"address":None,"detections":None})
                bot._marsof_ui[chat_id]=state
                edit_active_text(chat_id, call.message.message_id, build_analyze_prompt(), types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home")))
            elif action=="home":
                send_home_view(chat_id, call.message.message_id)
            bot.answer_callback_query(call.id)
        except Exception as exc:
            print(f"❌ menu callback: {exc}")
            bot.answer_callback_query(call.id,"Unable to open this page.")


    @bot.callback_query_handler(func=lambda call: call.data.startswith("net:"))
    def network_callbacks(call):
        try:
            network=call.data.split(":",1)[1]
            state=getattr(bot,"_marsof_ui",{}).get(call.message.chat.id,{})
            if network not in NETWORKS or not state.get("address"):
                bot.answer_callback_query(call.id,"Network is not available.")
                return
            address=state["address"]
            state.update({"network":network,"phase":"scanning"})
            bot._marsof_ui[call.message.chat.id]=state
            edit_active_text(call.message.chat.id,state["message_id"],f"<b>🔎 NETWORK SELECTED</b>\n\n🌐 <b>{network_label(network)}</b>\n<code>{address[:10]}…{address[-8:]}</code>\n\n⏳ <b>Running MARSOF AI analysis…</b>",None)
            bot.answer_callback_query(call.id,"Starting analysis…")
            bot.send_chat_action(call.message.chat.id,"typing")
            result=run_analysis(address,network)
            if not result:
                edit_active_text(call.message.chat.id,state["message_id"],"⚠️ <b>Analysis Unavailable</b>\n\nMARSOF AI could not complete a verified scan. No values were fabricated.",types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🔍 Analyze Another",callback_data="menu:analyse"),types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home")))
                return
            state.update({"phase":"result","result":result,"network":network,"message_type":"text"})
            bot._marsof_ui[call.message.chat.id]=state
            new_id=edit_active_text(call.message.chat.id,state["message_id"],build_home(result),keyboard(result))
            state["message_id"]=new_id
            bot._marsof_ui[call.message.chat.id]=state
        except Exception as exc:
            print(f"❌ network selection error: {exc}")
            bot.answer_callback_query(call.id,"Analysis error.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("db:"))
    def database_callbacks(call):
        try:
            parts=call.data.split(":")
            action=parts[1]
            if action=="list":
                text, networks=history_network_text()
                edit_active_text(call.message.chat.id, call.message.message_id, text, history_network_keyboard(networks))
            elif action=="net":
                network_key=parts[2] if len(parts)>2 else ""
                if network_key not in NETWORKS:
                    bot.answer_callback_query(call.id,"Network not found."); return
                text, rows=history_token_text(network_key)
                edit_active_text(call.message.chat.id, call.message.message_id, text, history_token_keyboard(network_key,rows))
            elif action=="token":
                token_id=int(parts[2])
                scan=latest_scan_for_token(token_id)
                if not scan:
                    bot.answer_callback_query(call.id,"No stored scan is available."); return
                result=load_scan_result(scan.id)
                if not result:
                    bot.answer_callback_query(call.id,"The stored scan could not be reconstructed."); return
                bot._marsof_ui[call.message.chat.id]={"message_id":call.message.message_id,"message_type":"text","phase":"result","result":result,"network":result["network"],"address":result["address"],"detections":None,"history_token_id":token_id}
                kb=keyboard(result)
                kb.add(types.InlineKeyboardButton("🔄 Scan Again", callback_data=f"db:rescan:{token_id}"))
                new_id=edit_active_text(call.message.chat.id, call.message.message_id, build_home(result), kb)
                bot._marsof_ui[call.message.chat.id]["message_id"]=new_id
            elif action=="rescan":
                token_id=int(parts[2])
                if not SessionLocal:
                    bot.answer_callback_query(call.id,"Database is unavailable."); return
                session=SessionLocal()
                try:
                    token=session.query(Token).filter(Token.id==token_id).first()
                    if not token: bot.answer_callback_query(call.id,"Token not found."); return
                    network=next((k for k,c in NETWORKS.items() if c.get("chain_id")==str(token.chain_id)),None)
                    address=token.contract_address
                finally: session.close()
                if not network: bot.answer_callback_query(call.id,"Network is not supported."); return
                bot.answer_callback_query(call.id,"Running a new scan…")
                bot.send_chat_action(call.message.chat.id,"typing")
                result=run_analysis(address,network)
                if not result:
                    bot.edit_message_text("⚠️ <b>Analysis unavailable</b>\n\nNo verified values were fabricated.",call.message.chat.id,call.message.message_id,parse_mode="HTML",reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("🏠 Main Menu",callback_data="menu:home")))
                    return
                state={"message_id":call.message.message_id,"message_type":"text","phase":"result","result":result,"network":network,"address":address,"detections":None,"history_token_id":token_id}
                bot._marsof_ui[call.message.chat.id]=state
                edit_active_text(call.message.chat.id,call.message.message_id,build_home(result),keyboard(result))
        except Exception as exc:
            print(f"❌ database callback: {exc}")
            bot.answer_callback_query(call.id,"Unable to open stored data.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("v:"))
    def callbacks(call):
        try:
            state=getattr(bot,"_marsof_ui",{}).get(call.message.chat.id)
            if not state or not state.get("result"):
                bot.answer_callback_query(call.id,"Run a token analysis first."); return
            parts=call.data.split(":")
            action=parts[1]
            page=int(parts[2]) if len(parts)>2 else 0
            result=state["result"]
            if action=="home":
                edit_active_text(call.message.chat.id, call.message.message_id, build_home(result), keyboard(result))
            elif action=="comparison":
                text=comparison_text(result)
                edit_active_text(call.message.chat.id, call.message.message_id, text, keyboard(result,"comparison",0,1))
            else:
                text,total_pages=section_text(result,action,page)
                edit_active_text(call.message.chat.id, call.message.message_id, text, keyboard(result,action,page,total_pages))
            bot.answer_callback_query(call.id)
        except Exception as exc:
            print(f"❌ callback error: {exc}")
            bot.answer_callback_query(call.id,"Unable to update the interface.")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))