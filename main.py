import os
import json
import math
import re
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
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
# Verified project-specific exception: LOFI on Sui.
# This does NOT alter the scoring engine. It only permits a final PASS verdict
# for this explicitly verified project while keeping the underlying technical
# findings visible in the full report.
LOFI_SUI_PACKAGE_ADDRESS = "0xf22da9a24ad027cccb5f2d496cbe91de953d363513db08a3a734d361c7c17503::lofi::lofi"
LOFI_EXCEPTION_NOTE = (
    "LOFI has a MARSOF verified-project exception based on its verified "
    "Binance.US listing. The underlying technical scan remains unchanged."
)

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


LARGE_CAP_THRESHOLD_USD = max(100_000_000.0, number(os.getenv("MARSOF_LARGE_CAP_USD")) or 1_000_000_000.0)


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


def market_class(market_cap_usd):
    """Classify market size independently from the security verdict."""
    mc = number(market_cap_usd)
    if mc is None:
        return "UNKNOWN"
    if mc >= 1_000_000_000:
        return "LARGE_CAP"
    if mc >= 100_000_000:
        return "MID_CAP"
    if mc >= 1_000_000:
        return "SMALL_CAP"
    return "MICRO_CAP"


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

    Verified PASS/CAUTION/RISK tests determine earned points.
    DATA/UNKNOWN are not treated as failures, but they also cannot
    manufacture points. Coverage is reported separately.
    """
    checks = list(checks or [])
    if not checks:
        return None, 0.0, 0, 0, 0

    p = sum(x.get("status") == "PASS" for x in checks)
    c = sum(x.get("status") == "CAUTION" for x in checks)
    r = sum(x.get("status") == "RISK" for x in checks)
    verified = p + c + r

    # Security is based on the verified test population, then confidence
    # is reported separately. This avoids both 100/100 inflation and the
    # previous collapse caused by raw coverage alone.
    if verified:
        quality = (p + 0.5 * c) / verified
        score = quality * 25.0
    else:
        score = None

    coverage = verified / len(checks) * 100.0
    return score, coverage, p, c, r


def _market_score(market_checks, market=None, holder_delta=None,
                 liquidity_delta=None, price_trend=None, mc_trend=None):
    """
    Market component: 75 points split across the six market dimensions.

    Only dimensions with real supporting data earn points. Missing history
    is not interpreted as a negative value. No normalization is performed
    that can turn partial data into a perfect market score.
    """
    market = market or {}
    checks = list(market_checks or [])

    # Nominal weights remain fixed.
    parts = {}

    # Price direction — 20
    price_score = None
    if price_trend is not None:
        try:
            x = float(price_trend)
            if x >= 50:
                price_score = 20.0
            elif x >= 20:
                price_score = 17.0
            elif x >= 5:
                price_score = 14.0
            elif x > -5:
                price_score = 10.0
            elif x > -20:
                price_score = 6.0
            elif x > -50:
                price_score = 3.0
            else:
                price_score = 0.0
        except (TypeError, ValueError):
            pass
    parts["price"] = (price_score, 20.0)

    # Market cap — 15. This is a size/context measure, not a direct
    # "small MC = bad" rule.
    mc_score = None
    mc = market.get("market_cap_usd")
    if mc is not None:
        try:
            x = float(mc)
            if x >= 100_000_000:
                mc_score = 15.0
            elif x >= 10_000_000:
                mc_score = 13.0
            elif x >= 1_000_000:
                mc_score = 11.0
            elif x >= 250_000:
                mc_score = 9.0
            elif x >= 100_000:
                mc_score = 7.0
            elif x > 0:
                mc_score = 5.0
            else:
                mc_score = 0.0
        except (TypeError, ValueError):
            pass
    parts["market_cap"] = (mc_score, 15.0)

    # Liquidity + liquidity/MC — 15
    liq_score = None
    liq = market.get("liquidity_usd")
    liq_mc = market.get("liquidity_mc_ratio")
    if liq is not None or liq_mc is not None:
        sub = []
        if liq is not None:
            try:
                l = float(liq)
                if l >= 1_000_000:
                    sub.append(8.0)
                elif l >= 250_000:
                    sub.append(7.0)
                elif l >= 100_000:
                    sub.append(6.0)
                elif l >= 50_000:
                    sub.append(5.0)
                elif l >= 10_000:
                    sub.append(3.0)
                elif l > 0:
                    sub.append(1.0)
                else:
                    sub.append(0.0)
            except (TypeError, ValueError):
                pass
        if liq_mc is not None:
            try:
                r = float(liq_mc)
                # Accept either fraction or percentage representation.
                if r <= 1:
                    r *= 100.0
                if r >= 50:
                    sub.append(7.0)
                elif r >= 25:
                    sub.append(6.0)
                elif r >= 10:
                    sub.append(4.0)
                elif r >= 5:
                    sub.append(2.0)
                elif r > 0:
                    sub.append(1.0)
                else:
                    sub.append(0.0)
            except (TypeError, ValueError):
                pass
        if sub:
            liq_score = min(15.0, sum(sub))
    parts["liquidity"] = (liq_score, 15.0)

    # Volume / pressure — 10
    volume_score = None
    vol = market.get("volume_24h_usd")
    buys = market.get("buys_24h")
    sells = market.get("sells_24h")
    if vol is not None or buys is not None or sells is not None:
        sub = []
        if vol is not None and mc is not None:
            try:
                vm = float(vol) / float(mc) * 100.0
                if vm >= 50:
                    sub.append(5.0)
                elif vm >= 20:
                    sub.append(4.0)
                elif vm >= 10:
                    sub.append(3.0)
                elif vm >= 2:
                    sub.append(2.0)
                elif vm > 0:
                    sub.append(1.0)
            except (TypeError, ValueError, ZeroDivisionError):
                pass
        if buys is not None and sells is not None:
            try:
                b, s = float(buys), float(sells)
                total = b + s
                if total > 0:
                    ratio = b / total
                    if ratio >= 0.65:
                        sub.append(5.0)
                    elif ratio >= 0.55:
                        sub.append(4.0)
                    elif ratio >= 0.45:
                        sub.append(3.0)
                    elif ratio >= 0.35:
                        sub.append(1.5)
                    else:
                        sub.append(0.0)
            except (TypeError, ValueError):
                pass
        if sub:
            volume_score = min(10.0, sum(sub))
    parts["volume"] = (volume_score, 10.0)

    # Holder flow — 10
    holder_score = None
    if holder_delta is not None:
        try:
            d = float(holder_delta)
            if d >= 20:
                holder_score = 10.0
            elif d >= 5:
                holder_score = 8.0
            elif d > -5:
                holder_score = 6.0
            elif d > -20:
                holder_score = 3.0
            else:
                holder_score = 0.0
        except (TypeError, ValueError):
            pass
    parts["holders"] = (holder_score, 10.0)

    # LP stability — 5
    lp_score = None
    if liquidity_delta is not None:
        try:
            d = float(liquidity_delta)
            if d >= 20:
                lp_score = 5.0
            elif d >= 5:
                lp_score = 4.0
            elif d > -5:
                lp_score = 3.0
            elif d > -20:
                lp_score = 1.0
            else:
                lp_score = 0.0
        except (TypeError, ValueError):
            pass
    parts["lp_stability"] = (lp_score, 5.0)

    earned = sum(score for score, _ in parts.values() if score is not None)
    verified_weight = sum(weight for score, weight in parts.values() if score is not None)
    market_score = earned if verified_weight else None

    return market_score, parts, verified_weight


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
    """Official MARSOF verdict engine.

    Philosophy:
      - RISK is triggered only by verified risk evidence/critical thresholds.
      - PASS is a strict gate, never a score threshold.
      - Everything that is not RISK and does not satisfy every PASS gate is CAUTION.
      - Missing data never becomes zero and never becomes RISK by itself.

    PASS requires all major market dimensions to be positively evidenced and
    minimum market-strength conditions to be met. Social/community analysis is
    intentionally NOT part of this version and can be added later as a separate
    evidence dimension.
    """
    security_checks = list(security_checks or [])
    market_checks = list(market_checks or [])
    all_checks = security_checks + market_checks

    available = [x for x in all_checks if x.get("status") != "UNKNOWN"]
    risk_tests = [x for x in all_checks if x.get("status") in {"PASS", "CAUTION", "RISK"}]
    missing = [x.get("label") for x in all_checks if x.get("status") == "UNKNOWN"]
    data_only = [x for x in all_checks if x.get("status") == "DATA"]

    p = sum(x.get("status") == "PASS" for x in risk_tests)
    c = sum(x.get("status") == "CAUTION" for x in risk_tests)
    r = sum(x.get("status") == "RISK" for x in risk_tests)
    coverage = (len(available) / len(all_checks) * 100.0) if all_checks else 0.0

    security_score, security_coverage, sp, sc, sr = _security_score(security_checks)

    # Extract market values from the verified market payload. Keep compatibility
    # with existing check structures as a fallback.
    market = {}
    md = market_data if isinstance(market_data, dict) else {}
    aliases = {
        "price_usd": "price",
        "market_cap_usd": "market_cap",
        "liquidity_usd": "liquidity",
        "volume_24h_usd": "volume_24h",
        "liquidity_mc_ratio": "liq_mc",
        "price_change_24h": "price_change_24h",
        "market_cap_change_24h": "market_cap_change_24h",
        "holder_change_24h": "holder_change_24h",
        "liquidity_change_24h": "liquidity_change_24h",
        "buys_24h": "buys_24h",
        "sells_24h": "sells_24h",
    }
    for public_key, internal_key in aliases.items():
        if md.get(internal_key) is not None:
            market[public_key] = md.get(internal_key)
    for item in market_checks:
        if not isinstance(item, dict):
            continue
        for key in aliases:
            if market.get(key) is None and item.get(key) is not None:
                market[key] = item.get(key)
        nested = item.get("market")
        if isinstance(nested, dict):
            for key in aliases:
                if market.get(key) is None and nested.get(key) is not None:
                    market[key] = nested.get(key)

    price_trend = market.get("price_change_24h")
    holder_delta = market.get("holder_change_24h")
    liquidity_delta = market.get("liquidity_change_24h")

    market_score = None
    market_parts = {}
    market_verified_weight = 0.0
    try:
        result = _market_score(
            market_checks,
            market=market,
            holder_delta=holder_delta,
            liquidity_delta=liquidity_delta,
            price_trend=price_trend,
            mc_trend=market.get("market_cap_change_24h"),
        )
        if isinstance(result, tuple) and len(result) == 3:
            market_score, market_parts, market_verified_weight = result
    except Exception:
        market_score = None
        market_parts = {}
        market_verified_weight = 0.0

    earned_score = 0.0
    if security_score is not None:
        earned_score += float(security_score)
    if market_score is not None:
        earned_score += float(market_score)
    overall_score = earned_score

    # ------------------------------------------------------------
    # VERIFIED RISK GATES
    # ------------------------------------------------------------
    critical_security = sr > 0
    severe_liquidity = False
    severe_collapse = False
    holder_exodus = False
    mc_collapse = False

    try:
        liq = float(market.get("liquidity_usd")) if market.get("liquidity_usd") is not None else None
        liq_mc = float(market.get("liquidity_mc_ratio")) if market.get("liquidity_mc_ratio") is not None else None
        if liq_mc is not None and liq_mc <= 1:
            liq_mc *= 100.0
        severe_liquidity = (
            (liq is not None and liq < 5000.0) or
            (liq_mc is not None and liq_mc < 1.0)
        )
    except (TypeError, ValueError):
        pass

    try:
        severe_collapse = price_trend is not None and float(price_trend) <= -90.0
        mc_collapse = market.get("market_cap_change_24h") is not None and float(market.get("market_cap_change_24h")) <= -70.0
        holder_exodus = holder_delta is not None and float(holder_delta) <= -20.0
    except (TypeError, ValueError):
        pass

    # Large-cap context: market size is kept separate from security risk.
    # For established large-cap assets, non-critical security flags such as
    # mintability/concentration are not allowed to turn the whole project into
    # RISK by themselves. Critical exploit/exit-risk findings still do.
    mc_value = number(market.get("market_cap_usd"))
    is_large_cap = mc_value is not None and mc_value >= LARGE_CAP_THRESHOLD_USD
    large_cap_noncritical_labels = {
        "mintable",
        "adjusted investor concentration",
        "investor concentration",
    }
    critical_security_labels = {
        "honeypot", "cannot sell", "transfer blocked", "blacklist",
        "malicious owner", "owner can drain", "hidden owner",
    }
    large_cap_waived = []
    if is_large_cap:
        for check in all_checks:
            if check.get("status") != "RISK":
                continue
            label = str(check.get("label", "")).strip().lower()
            if label in large_cap_noncritical_labels:
                check["status"] = "CAUTION"
                check["detail"] = (
                    f"{check.get('detail') or 'Risk indicator detected'}; "
                    "classified as non-critical in large-cap context."
                )
                check["large_cap_context"] = True
                large_cap_waived.append(check.get("label"))
        # Recompute counters after the large-cap context adjustment.
        p = sum(x.get("status") == "PASS" for x in risk_tests)
        c = sum(x.get("status") == "CAUTION" for x in risk_tests)
        r = sum(x.get("status") == "RISK" for x in risk_tests)
        critical_security = any(
            x.get("status") == "RISK" and
            (str(x.get("label", "")).strip().lower() in critical_security_labels
             or any(k in str(x.get("label", "")).strip().lower() for k in ("honeypot", "drain", "cannot sell", "transfer blocked")))
            for x in all_checks
        )

    verified_risk = critical_security or severe_liquidity or severe_collapse or mc_collapse or holder_exodus or (r > 0 and not is_large_cap)

    # ------------------------------------------------------------
    # STRICT PASS GATE
    # ------------------------------------------------------------
    pass_reasons = []

    # 1) No verified caution/risk test.
    if r > 0:
        pass_reasons.append("A security/market test is marked RISK.")
    if c > 0:
        pass_reasons.append("One or more completed tests are marked CAUTION.")

    # 2) Security evidence must be complete enough to support PASS.
    #    DATA/UNKNOWN are not treated as failures, but they cannot support PASS.
    if not security_checks:
        pass_reasons.append("No verified security test set is available.")
    elif security_coverage < 100.0:
        pass_reasons.append(f"Security evidence is incomplete ({security_coverage:.1f}% coverage).")

    # 3) All seven market dimensions must have actual evidence.
    required_parts = {
        "price": "Price trend",
        "market_cap": "Market cap",
        "liquidity": "Liquidity",
        "volume": "Volume / buy-sell pressure",
        "holders": "Holder flow",
        "lp_stability": "LP stability",
    }
    for key, label in required_parts.items():
        value = market_parts.get(key, (None, 0.0))[0]
        if value is None and not is_large_cap:
            pass_reasons.append(f"{label} evidence is unavailable.")

    # 4) Strong market-size/liquidity gate.
    try:
        mc = float(market.get("market_cap_usd")) if market.get("market_cap_usd") is not None else None
        liq = float(market.get("liquidity_usd")) if market.get("liquidity_usd") is not None else None
        liq_mc = float(market.get("liquidity_mc_ratio")) if market.get("liquidity_mc_ratio") is not None else None
        if liq_mc is not None and liq_mc <= 1:
            liq_mc *= 100.0

        # Official baseline: PASS requires MC in the millions and strong absolute
        # liquidity plus a healthy liquidity/MC relationship.
        if not is_large_cap:
            if mc is None:
                pass_reasons.append("Market cap is unavailable for the PASS gate.")
            elif mc < 1_000_000:
                pass_reasons.append("Market cap is below the PASS minimum of $1M.")

            if liq is None:
                pass_reasons.append("Liquidity is unavailable for the PASS gate.")
            elif liq < 100_000:
                pass_reasons.append("Liquidity is below the PASS minimum of $100K.")

            if liq_mc is None:
                pass_reasons.append("Liquidity/MC is unavailable for the PASS gate.")
            elif liq_mc < 10.0:
                pass_reasons.append("Liquidity/MC is below the PASS minimum of 10%.")
    except (TypeError, ValueError):
        pass_reasons.append("Market-cap/liquidity values could not be verified for PASS.")

    # 5) Directional market conditions must not be negative.
    try:
        if price_trend is None and not is_large_cap:
            pass_reasons.append("24h price trend is unavailable.")
        elif price_trend is not None and float(price_trend) < 0 and not is_large_cap:
            pass_reasons.append("24h price trend is negative.")
    except (TypeError, ValueError):
        pass_reasons.append("24h price trend could not be verified.")

    try:
        if market.get("market_cap_change_24h") is not None and float(market.get("market_cap_change_24h")) < 0 and not is_large_cap:
            pass_reasons.append("24h market-cap trend is negative.")
    except (TypeError, ValueError):
        pass

    try:
        if holder_delta is None and not is_large_cap:
            pass_reasons.append("Holder-flow trend is unavailable.")
        elif holder_delta is not None and float(holder_delta) < 0 and not is_large_cap:
            pass_reasons.append("Holder-flow trend is negative.")
    except (TypeError, ValueError):
        pass

    try:
        if liquidity_delta is None and not is_large_cap:
            pass_reasons.append("Liquidity trend is unavailable.")
        elif liquidity_delta is not None and float(liquidity_delta) < 0 and not is_large_cap:
            pass_reasons.append("Liquidity trend is negative.")
    except (TypeError, ValueError):
        pass

    # 6) Volume and buy/sell evidence must be present and positive.
    try:
        vol = float(market.get("volume_24h_usd")) if market.get("volume_24h_usd") is not None else None
        mc = float(market.get("market_cap_usd")) if market.get("market_cap_usd") is not None else None
        buys = float(market.get("buys_24h")) if market.get("buys_24h") is not None else None
        sells = float(market.get("sells_24h")) if market.get("sells_24h") is not None else None
        if vol is None or mc in (None, 0):
            if not is_large_cap:
                pass_reasons.append("24h volume/MC evidence is unavailable.")
        elif vol / mc < 0.02 and not is_large_cap:
            pass_reasons.append("24h volume is below 2% of market cap.")
        if buys is None or sells is None or buys + sells <= 0:
            if not is_large_cap:
                pass_reasons.append("Buy/sell pressure is unavailable.")
        elif buys <= sells and not is_large_cap:
            pass_reasons.append("Buy pressure is not greater than sell pressure.")
    except (TypeError, ValueError, ZeroDivisionError):
        pass_reasons.append("Volume/buy-sell values could not be verified.")

    # ------------------------------------------------------------
    # FINAL VERDICT
    # ------------------------------------------------------------
    if verified_risk:
        verdict = "RISK"
    elif not pass_reasons:
        verdict = "PASS"
    else:
        verdict = "CAUTION"

    # Large-cap quality override: once an asset is established as large-cap,
    # non-critical large-cap-context findings do not block PASS. Critical
    # security findings and severe market-collapse/liquidity conditions still do.
    if is_large_cap and not verified_risk:
        blocking_cautions = [
            x for x in all_checks
            if x.get("status") == "CAUTION" and not x.get("large_cap_context")
        ]
        security_complete = bool(security_checks) and security_coverage >= 100.0
        if security_complete and not blocking_cautions:
            verdict = "PASS"

    # Keep verdict and counters synchronized. A derived CAUTION is transparent:
    # it means PASS was not earned, not that missing data was secretly treated as risk.
    if verdict == "CAUTION" and c == 0 and r == 0:
        reason_text = "PASS gate not satisfied: " + " ".join(pass_reasons[:4])
        if len(pass_reasons) > 4:
            reason_text += f" (+{len(pass_reasons)-4} additional conditions)."
        all_checks.append({
            "label": "PASS Gate / Market Quality",
            "status": "CAUTION",
            "detail": reason_text,
            "source": "MARSOF official verdict gate",
            "derived": True,
        })
        c += 1
    # ------------------------------------------------------------
    # VERIFIED PROJECT EXCEPTION — LOFI / SUI
    # ------------------------------------------------------------
    # Keep the score and all market/security measurements unchanged.
    # The exception is deliberately narrow: it applies only to the known
    # LOFI Sui package prefix and only when the sole risk indicator is the
    # Upgrade Capability finding. Other verified risks still force RISK.
    lofi_risks = [
        x for x in all_checks
        if x.get("status") == "RISK"
    ]
    lofi_only_upgrade_risk = (
        bool(lofi_risks)
        and all(
            str(x.get("label", "")).strip().lower() == "contract upgradeable"
            for x in lofi_risks
        )
    )
    lofi_exception = (
        str(network or "").lower() == "sui"
        and normalize_address(address) == LOFI_SUI_PACKAGE_ADDRESS
        and (r == 0 or lofi_only_upgrade_risk)
    )
    if lofi_exception:
        # This is a FINAL-VERDICT exception only. The score, market values,
        # caution findings and technical evidence are deliberately untouched.
        for check in all_checks:
            if str(check.get("label", "")).strip().lower() == "contract upgradeable" and check.get("status") == "RISK":
                check["status"] = "PASS"
                check["detail"] = (
                    "Active upgrade capability detected; LOFI/Sui is covered by "
                    "the MARSOF verified-project exception. The technical finding "
                    "remains documented in the report."
                )
                check["exception"] = "LOFI verified-project exception"
        if lofi_only_upgrade_risk:
            p += r
            r = 0
        verdict = "PASS"

    if verdict == "RISK" and r == 0:
        if critical_security:
            reason = "A verified security test is marked RISK."
        elif severe_liquidity:
            reason = "Verified liquidity threshold indicates elevated market risk."
        elif severe_collapse:
            reason = "Verified price-collapse threshold indicates elevated market risk."
        elif mc_collapse:
            reason = "Verified market-cap collapse threshold indicates elevated market risk."
        elif holder_exodus:
            reason = "Verified holder-flow threshold indicates elevated market risk."
        else:
            reason = "A verified risk threshold was triggered."
        all_checks.append({
            "label": "Verified Risk Gate",
            "status": "RISK",
            "detail": reason,
            "source": "MARSOF official verdict gate",
            "derived": True,
        })
        r += 1

    score_evidence_coverage = (
        (25.0 if security_score is not None else 0.0) +
        float(market_verified_weight or 0.0)
    )

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
        "verdict": verdict,
        "verified_project_exception": "LOFI / Sui" if lofi_exception else None,
        "verified_project_exception_note": LOFI_EXCEPTION_NOTE if lofi_exception else None,
        "market_class": market_class(market.get("market_cap_usd")),
        "large_cap": is_large_cap,
        "large_cap_context": large_cap_waived,
        "score": overall_score,
        "score_components": {
            "security": security_score,
            "market": market_score,
            "market_parts": market_parts,
            "verified_market_weight": market_verified_weight,
            "score_evidence_coverage": score_evidence_coverage,
        },
        "score_evidence_coverage": score_evidence_coverage,
        "pass_gate": {
            "passed": verdict == "PASS",
            "reasons": pass_reasons,
            "social_analysis": "not included in this version",
        },
        "score_note": "Score reflects earned points; PASS requires the independent strict PASS gate. Missing data is tracked separately and never converted to zero.",
    }

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

        # Independent secondary sources run concurrently to reduce analysis latency.
        # GoPlus remains the primary security source; secondary sources only fill missing fields.
        secondary_jobs = {
            "Honeypot.is": lambda: fetch_honeypot(address, network),
            "Sourcify": lambda: fetch_sourcify(address, network),
            "EVM RPC": lambda: fetch_evm_metadata(address, network),
            "Blockscout v2": lambda: fetch_blockscout_v2(address, network),
            "Blockscout": lambda: fetch_blockscout_token(address, network),
            "Alchemy": lambda: fetch_alchemy_token(address, network),
            "Explorer API": lambda: fetch_explorer_token(address, network),
        }
        secondary_results = {}
        with ThreadPoolExecutor(max_workers=min(7, len(secondary_jobs))) as executor:
            futures = {executor.submit(fn): name for name, fn in secondary_jobs.items()}
            for future in as_completed(futures):
                name = futures[future]
                try:
                    secondary_results[name] = future.result()
                except Exception as exc:
                    secondary_results[name] = None
                    print(f"⚠️ {name} secondary source failed: {exc}")

        honeypot_raw = secondary_results.get("Honeypot.is")
        if honeypot_raw:
            _merge_security_missing(security_data, normalize_honeypot_data(honeypot_raw), "Honeypot.is")

        sourcify_raw = secondary_results.get("Sourcify")
        if sourcify_raw:
            _merge_security_missing(security_data, normalize_sourcify_data(sourcify_raw, cfg.get("rpc")), "Sourcify")

        metadata = secondary_results.get("EVM RPC")
        _merge_security_missing(security_data, metadata, "EVM RPC")
        blockscout_v2 = secondary_results.get("Blockscout v2")
        extra_sources = [
            ("Blockscout v2", blockscout_v2),
            ("Blockscout", secondary_results.get("Blockscout")),
            ("Alchemy", secondary_results.get("Alchemy")),
            ("Explorer API", secondary_results.get("Explorer API")),
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

    # Identity fallback: DEX Screener provides the traded token symbol even
    # when the security/indexed metadata providers do not return token_name
    # or token_symbol. Use it for display/identity only; do not fabricate a
    # separate token name and do not change scoring or security checks.
    base_token = (market_pair or {}).get("baseToken") or {}
    quote_token = (market_pair or {}).get("quoteToken") or {}
    requested = normalize_address(address)
    base_address = normalize_address(base_token.get("address"))
    quote_address = normalize_address(quote_token.get("address"))
    display_token = base_token if base_address == requested else quote_token if quote_address == requested else base_token
    display_symbol = display_token.get("symbol") if isinstance(display_token, dict) else None
    if not security_data.get("token_symbol") and display_symbol:
        security_data["token_symbol"] = str(display_symbol).strip()
        security_data.setdefault("_field_sources", {})["token_symbol"] = "DEX Screener"

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
    market_pair = (result.get("market") or {}).get("pair") or {}
    base_token = market_pair.get("baseToken") or {}
    quote_token = market_pair.get("quoteToken") or {}
    requested = normalize_address(result.get("address"))
    base_address = normalize_address(base_token.get("address"))
    quote_address = normalize_address(quote_token.get("address"))
    display_token = base_token if base_address == requested else quote_token if quote_address == requested else base_token
    name=d.get("token_symbol") or d.get("token_name") or display_token.get("symbol") or display_token.get("name") or "Token"
    address=result["address"]
    market_class_value = c.get("market_class", market_class(m.get("market_cap")))
    market_class_label = {"LARGE_CAP":"🏦 LARGE CAP","MID_CAP":"🟢 MID CAP","SMALL_CAP":"🟡 SMALL CAP","MICRO_CAP":"🔴 MICRO CAP","UNKNOWN":"⚪ MARKET CLASS UNAVAILABLE"}.get(market_class_value, "⚪ MARKET CLASS UNAVAILABLE")
    risks=[x for x in c.get("all",[]) if x.get("status")=="RISK"]
    cautions=[x for x in c.get("all",[]) if x.get("status")=="CAUTION"]
    if c.get("verified_project_exception"):
        verdict_msg="<b>Verified project exception applied.</b> The technical scan and score remain unchanged; the final verdict is PASS under the configured LOFI exception."
        reasons=(
            "🟢 <b>Verified Project Status</b>\n"
            "🟢 Binance.US listing: Verified\n"
            "ℹ️ The underlying technical findings remain available in the full report.\n"
            f"ℹ️ {c.get('verified_project_exception_note') or LOFI_EXCEPTION_NOTE}"
        )
    elif c["verdict"]=="RISK":
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
        f"<b>{name}</b> · <b>{net}</b>", f"<code>{address[:8]}…{address[-6:]}</code>", f"{market_class_label}", "",
        f"{verdict_icon(c['verdict'])} <b>{c['verdict']}</b>", verdict_msg,
        f"🎯 <b>Overall Score: {c.get('score'):.1f}/100</b>\n"
        f"📊 <b>Score Evidence Coverage: {c.get('score_evidence_coverage', 0):.1f}%</b>" if c.get('score') is not None else "🎯 <b>Overall Score: Not available</b>",
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

    # ========================================================
    # PHASE 2 MONITOR — embedded background thread
    # Keeps the existing Telegram/FastAPI service unchanged
    # while running the watchlist monitor in the same Render
    # Web Service (no separate Background Worker required).
    # ========================================================
    try:
        from main_monitor import start_monitor_background
        start_monitor_background()
    except Exception as exc:
        # Monitor failure must never prevent Telegram/FastAPI from starting.
        print(f"❌ Phase-2 monitor startup failed: {exc}")


@app.post("/webhook")
async def webhook(request: Request):
    if not bot:
        return {"ok": False, "error": "TELEGRAM_TOKEN is not configured"}
    try:
        raw = await request.json()
        update = telebot.types.Update.de_json(json.dumps(raw))
        if update is None:
            raise ValueError("Invalid Telegram update")

        # Telegram webhook delivery must stay fast. The Phase-2 monitor can
        # perform slow external API work in parallel, so never make the HTTP
        # webhook wait for handler execution. This also prevents a slow
        # monitor/test handler from making ordinary /start messages appear
        # unresponsive.
        def dispatch_update():
            try:
                bot.process_new_updates([update])
            except Exception as exc:
                print(f"❌ Telegram update processing: {exc}")

        threading.Thread(
            target=dispatch_update,
            name="telegram-update-dispatch",
            daemon=True,
        ).start()
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
