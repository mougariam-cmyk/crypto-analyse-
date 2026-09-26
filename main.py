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
    Text, UniqueConstraint, Index,
)
from sqlalchemy.orm import declarative_base, sessionmaker

# ============================================================
# CONFIG
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
    volume_24h_usd = Column(Float)
    txns_24h_buys = Column(Integer)
    txns_24h_sells = Column(Integer)
    price_change_24h = Column(Float)
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
    is_locked = Column(Boolean)
    snapshot_time = Column(DateTime, default=lambda: datetime.now(timezone.utc))

class Transaction(Base):
    __tablename__ = "transactions"
    id = Column(Integer, primary_key=True)
    chain_id = Column(String(20), nullable=False)
    token_address = Column(String(100), nullable=False)
    tx_hash = Column(String(150), nullable=False)
    wallet = Column(String(100), nullable=False)
    tx_type = Column(String(20))
    amount = Column(Float)
    value_usd = Column(Float)
    block_time = Column(DateTime)
    raw_data = Column(Text)
    __table_args__ = (UniqueConstraint("chain_id", "tx_hash", name="uq_transaction_chain_hash"),)

# ============================================================
# HELPERS
# ============================================================
def utc_now():
    return datetime.now(timezone.utc)

def normalize_address(value):
    return str(value).strip().lower() if value is not None else ""

def number(value):
    if value is None or isinstance(value, bool): return None
    try:
        if isinstance(value, str) and not value.strip(): return None
        x = float(value)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None

def integer(value):
    x = number(value)
    return int(x) if x is not None else None

def boolean(value):
    if value is None: return None
    if isinstance(value, bool): return value
    if str(value).strip().lower() in {"1", "true", "yes", "y"}: return True
    if str(value).strip().lower() in {"0", "false", "no", "n"}: return False
    return None

def fmt_num(value, suffix=""):
    if value is None: return "غير متوفر"
    if abs(value) >= 1_000_000_000: return f"{value/1_000_000_000:.2f}B{suffix}"
    if abs(value) >= 1_000_000: return f"{value/1_000_000:.2f}M{suffix}"
    if abs(value) >= 1_000: return f"{value/1_000:.2f}K{suffix}"
    return f"{value:.4g}{suffix}"

def fmt_pct(value):
    return "غير متوفر" if value is None else f"{value:.2f}%"

def is_valid_bsc_address(address):
    if not address: return False
    a = address.strip()
    if len(a) != 42 or not a.startswith("0x"): return False
    try:
        int(a[2:], 16); return True
    except ValueError:
        return False

# ============================================================
# SOURCE FETCHERS
# ============================================================
def fetch_goplus(address):
    response = requests.get(GO_PLUS_URL, params={"contract_addresses": address}, timeout=20)
    response.raise_for_status()
    payload = response.json()
    result = payload.get("result") if isinstance(payload, dict) else None
    if not isinstance(result, dict): return None
    target = normalize_address(address)
    for returned_address, data in result.items():
        if normalize_address(returned_address) == target and isinstance(data, dict):
            return dict(data)
    return None

def fetch_dexscreener(address):
    response = requests.get(DEXSCREENER_URL.format(address=address), timeout=20)
    response.raise_for_status()
    payload = response.json()
    pairs = payload.get("pairs") if isinstance(payload, dict) else None
    if not isinstance(pairs, list): return []
    # BSC only; prefer highest USD liquidity, but never invent a pair.
    bsc_pairs = [p for p in pairs if p.get("chainId") == "bsc"]
    return sorted(bsc_pairs, key=lambda p: number((p.get("liquidity") or {}).get("usd")) or -1, reverse=True)

# ============================================================
# ANALYTICS
# ============================================================
def field(data, key):
    return data.get(key)

def status_for(value, mode="positive"):
    # Returns PASS/CAUTION/RISK/UNKNOWN without converting missing to a risk.
    if value is None: return "UNKNOWN"
    if mode == "negative_bool": return "RISK" if value else "PASS"
    if mode == "positive_bool": return "PASS" if value else "CAUTION"
    return "PASS"

def build_security(data):
    checks = []
    def add(label, value, status, detail):
        checks.append({"label": label, "value": value, "status": status, "detail": detail})

    honeypot = boolean(field(data, "is_honeypot"))
    add("Honeypot", honeypot, status_for(honeypot, "negative_bool"), "لا" if honeypot is False else "نعم" if honeypot else "غير متوفر")

    for key, label in [("buy_tax", "Buy Tax"), ("sell_tax", "Sell Tax"), ("transfer_tax", "Transfer Tax")]:
        raw = number(field(data, key))
        pct = raw * 100 if raw is not None else None
        if pct is None: st = "UNKNOWN"
        elif pct <= 5: st = "PASS"
        elif pct <= 10: st = "CAUTION"
        else: st = "RISK"
        add(label, pct, st, fmt_pct(pct))

    open_source = boolean(field(data, "is_open_source"))
    add("Source Code", open_source, "PASS" if open_source is True else "RISK" if open_source is False else "UNKNOWN", "نعم" if open_source is True else "لا" if open_source is False else "غير متوفر")

    bool_risks = [
        ("Mintable", "is_mintable"), ("Proxy", "is_proxy"), ("Blacklist", "is_blacklisted"),
        ("Whitelist", "is_whitelisted"), ("Trading suspension", "is_trading_cooldown"),
        ("Anti-whale", "is_anti_whale"), ("Modifiable anti-whale", "anti_whale_modifiable"),
        ("Contract upgradeable", "contract_upgradeable"), ("Self destruct", "selfdestruct"),
        ("Privilege withdraw", "privilege_withdraw"), ("Approval abuse", "approval_abuse"),
    ]
    for label, key in bool_risks:
        v = boolean(field(data, key))
        # Anti-whale being present is not inherently a risk; its modifiability is.
        if key == "is_anti_whale": st = "PASS" if v is True else "UNKNOWN" if v is None else "CAUTION"
        else: st = "RISK" if v is True else "PASS" if v is False else "UNKNOWN"
        add(label, v, st, "نعم" if v is True else "لا" if v is False else "غير متوفر")

    owner = field(data, "owner")
    if isinstance(owner, dict):
        owner_type = owner.get("owner_type") or owner.get("owner_name")
        owner_addr = owner.get("owner_address")
        add("Owner", owner_addr, "CAUTION" if owner_addr and owner_type not in {"blackhole"} else "PASS" if owner_type == "blackhole" else "UNKNOWN", owner_type or owner_addr or "غير متوفر")

    holders = field(data, "holders")
    if isinstance(holders, list) and holders:
        top10 = sum(number(h.get("percent")) or 0 for h in holders[:10])
        add("Top 10 concentration", top10, "RISK" if top10 >= 70 else "CAUTION" if top10 >= 40 else "PASS", fmt_pct(top10))
    else:
        add("Top 10 concentration", None, "UNKNOWN", "غير متوفر")

    return checks

def choose_market(pairs):
    return pairs[0] if pairs else None

def build_market(pair):
    if not pair: return {"checks": [], "data": {}, "missing": ["DEX market data"]}
    liq = number((pair.get("liquidity") or {}).get("usd"))
    mc = number(pair.get("marketCap"))
    fdv = number(pair.get("fdv"))
    vol = number((pair.get("volume") or {}).get("h24"))
    buys = integer((pair.get("txns") or {}).get("h24", {}).get("buys"))
    sells = integer((pair.get("txns") or {}).get("h24", {}).get("sells"))
    price = number(pair.get("priceUsd"))
    change = number((pair.get("priceChange") or {}).get("h24"))
    checks = []
    def add(label, value, status="INFO", detail=None): checks.append({"label": label, "value": value, "status": status, "detail": detail if detail is not None else fmt_num(value)})
    liq_mc = (liq / mc * 100) if liq is not None and mc and mc > 0 else None
    vol_liq = (vol / liq) if vol is not None and liq and liq > 0 else None
    buy_sell = (buys / sells) if buys is not None and sells not in (None, 0) else None
    add("Price", price, "INFO", f"${fmt_num(price)}")
    add("Market Cap", mc, "INFO", f"${fmt_num(mc)}")
    add("FDV", fdv, "INFO", f"${fmt_num(fdv)}")
    add("Liquidity", liq, "INFO", f"${fmt_num(liq)}")
    add("24h Volume", vol, "INFO", f"${fmt_num(vol)}")
    add("Liquidity / Market Cap", liq_mc, "RISK" if liq_mc is not None and liq_mc < 1 else "CAUTION" if liq_mc is not None and liq_mc < 3 else "PASS" if liq_mc is not None else "UNKNOWN", fmt_pct(liq_mc))
    add("Volume / Liquidity", vol_liq, "CAUTION" if vol_liq is not None and vol_liq > 20 else "PASS" if vol_liq is not None else "UNKNOWN", f"{vol_liq:.2f}x" if vol_liq is not None else "غير متوفر")
    add("Buys 24h", buys, "INFO", str(buys) if buys is not None else "غير متوفر")
    add("Sells 24h", sells, "INFO", str(sells) if sells is not None else "غير متوفر")
    add("Buy/Sell transaction ratio", buy_sell, "INFO", f"{buy_sell:.2f}x" if buy_sell is not None else "غير متوفر")
    add("Price change 24h", change, "INFO", fmt_pct(change))
    return {"checks": checks, "data": {"pair": pair, "liquidity": liq, "market_cap": mc, "fdv": fdv, "volume_24h": vol, "buys": buys, "sells": sells, "price": price, "price_change_24h": change, "liq_mc": liq_mc, "vol_liq": vol_liq}, "missing": []}

def classify_all(security_checks, market_checks):
    all_checks = security_checks + market_checks
    known = [x for x in all_checks if x["status"] != "UNKNOWN"]
    missing = [x["label"] for x in all_checks if x["status"] == "UNKNOWN"]
    p = sum(x["status"] == "PASS" for x in known)
    c = sum(x["status"] == "CAUTION" for x in known)
    r = sum(x["status"] == "RISK" for x in known)
    coverage = (len(known) / len(all_checks) * 100) if all_checks else 0
    if r > 0: verdict = "RISK"
    elif c > 0: verdict = "CAUTION"
    elif known: verdict = "PASS"
    else: verdict = "INCOMPLETE"
    return {"all": all_checks, "pass": p, "caution": c, "risk": r, "missing": missing, "coverage": coverage, "verdict": verdict}

# ============================================================
# DATABASE SAVE
# ============================================================
def save_analysis(address, data, classification, market):
    if not SessionLocal: return
    session = SessionLocal()
    try:
        addr = normalize_address(address)
        token = session.query(Token).filter(Token.contract_address == addr, Token.chain_id == CHAIN_ID).first()
        if not token:
            token = Token(contract_address=addr, chain_id=CHAIN_ID, symbol=data.get("token_symbol"), name=data.get("token_name"), first_seen=utc_now())
            session.add(token); session.flush()
        token.symbol = data.get("token_symbol") or token.symbol
        token.name = data.get("token_name") or token.name
        token.last_scanned = utc_now(); token.scan_count = (token.scan_count or 0) + 1
        session.add(SecurityScan(token_id=token.id, verdict=classification["verdict"], coverage=classification["coverage"], pass_count=classification["pass"], caution_count=classification["caution"], risk_count=classification["risk"], raw_data=json.dumps({"security": data, "market": market}, ensure_ascii=False), scanned_at=utc_now()))
        if market.get("pair"):
            m = market
            p = m.get("pair")
            session.add(MarketSnapshot(chain_id=CHAIN_ID, token_address=addr, pair_address=p.get("pairAddress"), dex_id=p.get("dexId"), price_usd=m.get("price"), market_cap_usd=m.get("market_cap"), fdv_usd=m.get("fdv"), liquidity_usd=m.get("liquidity"), volume_24h_usd=m.get("volume_24h"), txns_24h_buys=m.get("buys"), txns_24h_sells=m.get("sells"), price_change_24h=m.get("price_change_24h"), snapshot_time=utc_now()))
        holders = data.get("holders")
        if isinstance(holders, list):
            for h in holders[:10]:
                session.add(HolderSnapshot(chain_id=CHAIN_ID, token_address=addr, wallet=str(h.get("address") or ""), balance=number(h.get("balance")), percentage=number(h.get("percent")), tag=str(h.get("tag")) if h.get("tag") is not None else None, is_locked=boolean(h.get("is_locked")), snapshot_time=utc_now()))
        session.commit()
    except Exception as exc:
        session.rollback(); print(f"⚠️ DB save error: {exc}")
    finally:
        session.close()

# ============================================================
# UI
# ============================================================
def verdict_icon(v): return {"PASS":"🟢", "CAUTION":"🟠", "RISK":"🔴", "INCOMPLETE":"⚪"}.get(v, "⚪")

def build_home(result):
    d = result["security"]
    symbol = d.get("token_symbol") or "غير متوفر"
    name = d.get("token_name") or ""
    c = result["classification"]
    m = result["market"]["data"]
    return (
        "╭────────────────────────────╮\n"
        "│       MARSOF AI             │\n"
        "│    TOKEN INTELLIGENCE       │\n"
        "╰────────────────────────────╯\n\n"
        f"🪙 **{symbol}** {('— ' + name) if name else ''}\n"
        f"`{result['address']}`\n\n"
        f"{verdict_icon(c['verdict'])} **{c['verdict']}**\n"
        f"🟢 Pass: {c['pass']}   🟠 Caution: {c['caution']}   🔴 Risk: {c['risk']}\n\n"
        f"📡 Data Coverage: **{c['coverage']:.0f}%**\n"
        f"💧 Liquidity: **${fmt_num(m.get('liquidity'))}**\n"
        f"💰 Market Cap: **${fmt_num(m.get('market_cap'))}**\n"
        f"📈 24h Volume: **${fmt_num(m.get('volume_24h'))}**\n\n"
        "اختر القسم لعرض التفاصيل. البيانات المفقودة لا تتحول إلى 0."
    )

def section_text(result, category):
    checks = result["classification"]["all"]
    if category == "pass": items = [x for x in checks if x["status"] == "PASS"]
    elif category == "caution": items = [x for x in checks if x["status"] == "CAUTION"]
    elif category == "risk": items = [x for x in checks if x["status"] == "RISK"]
    elif category == "missing": items = [x for x in checks if x["status"] == "UNKNOWN"]
    else: items = checks
    title = {"pass":"🟢 PASS — الاختبارات المجتازة", "caution":"🟠 CAUTION — مؤشرات تستحق الانتباه", "risk":"🔴 RISK — مؤشرات الخطر", "missing":"⚪ DATA GAPS — بيانات غير متوفرة", "all":"📊 جميع النتائج"}.get(category, "📊 النتائج")
    lines = [title, ""]
    if not items:
        lines.append("لا توجد عناصر في هذه الفئة ضمن البيانات المتاحة.")
    for x in items:
        lines.append(f"{verdict_icon(x['status'])} **{x['label']}**: {x['detail']}")
    return "\n".join(lines)

def keyboard():
    kb = types.InlineKeyboardMarkup(row_width=3)
    kb.add(types.InlineKeyboardButton("🟢 PASS", callback_data="m:pass"), types.InlineKeyboardButton("🟠 CAUTION", callback_data="m:caution"), types.InlineKeyboardButton("🔴 RISK", callback_data="m:risk"))
    kb.add(types.InlineKeyboardButton("📊 ALL", callback_data="m:all"), types.InlineKeyboardButton("⚪ Missing", callback_data="m:missing"), types.InlineKeyboardButton("🏠 Overview", callback_data="m:home"))
    return kb

def run_analysis(address):
    security = fetch_goplus(address)
    if not security: return None
    pairs = fetch_dexscreener(address)
    market = build_market(choose_market(pairs))
    classification = classify_all(build_security(security), market["checks"])
    result = {"address": normalize_address(address), "security": security, "market": market, "classification": classification}
    save_analysis(address, security, classification, market["data"])
    return result

# ============================================================
# FASTAPI
# ============================================================
app = FastAPI(title="MARSOF AI", version="2.0.0")

@app.get("/")
def root(): return {"status":"running","service":"MARSOF AI","version":"2.0.0","database":bool(engine)}

@app.get("/health")
def health(): return {"status":"ok","telegram_token":bool(TELEGRAM_TOKEN),"database_configured":bool(engine),"webhook_url":WEBHOOK_URL}

@app.get("/telegram-webhook")
def telegram_webhook_status(): return get_telegram_webhook_info()

def setup_telegram_webhook():
    if not TELEGRAM_TOKEN: return False
    try:
        r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/setWebhook", json={"url":WEBHOOK_URL,"allowed_updates":["message","callback_query"]}, timeout=20)
        r.raise_for_status(); return bool(r.json().get("ok"))
    except Exception as e:
        print(f"❌ webhook setup: {e}"); return False

def get_telegram_webhook_info():
    if not TELEGRAM_TOKEN: return {"ok":False,"error":"TELEGRAM_TOKEN غير موجود"}
    try:
        r=requests.get(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getWebhookInfo",timeout=20); r.raise_for_status(); return r.json()
    except Exception as e: return {"ok":False,"error":str(e)}

def migrate_database():
    """Small additive migration for the existing reference database."""
    if not engine:
        return
    try:
        from sqlalchemy import inspect, text
        Base.metadata.create_all(bind=engine)
        inspector = inspect(engine)
        columns = {c["name"] for c in inspector.get_columns("tokens")} if "tokens" in inspector.get_table_names() else set()
        additions = {
            "name": "VARCHAR(200)",
            "last_scanned": "TIMESTAMP",
            "scan_count": "INTEGER DEFAULT 0",
        }
        with engine.begin() as conn:
            for col, typ in additions.items():
                if col not in columns:
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
    if not bot: return {"ok":False,"error":"TELEGRAM_TOKEN غير موجود"}
    try:
        raw = await request.json()
        update = telebot.types.Update.de_json(json.dumps(raw))
        if update is None: raise ValueError("Invalid Telegram update")
        bot.process_new_updates([update])
        return {"ok":True}
    except Exception as e:
        print(f"❌ webhook processing: {e}"); return {"ok":False,"error":str(e)}

# ============================================================
# TELEGRAM
# ============================================================
if bot:
    @bot.message_handler(commands=["start"])
    def start(message):
        bot.send_message(message.chat.id, "🤖 **MARSOF AI**\n\nأرسل عقد BSC وسأبني لك تقريرًا مبنيًا على البيانات المتاحة فعليًا.\n\n🟢 PASS\n🟠 CAUTION\n🔴 RISK\n\n⚠️ نقص البيانات يظهر بوضوح ولا يتحول إلى 0.", parse_mode="Markdown")

    @bot.message_handler(content_types=["text"])
    def text_handler(message):
        if not message.text or message.text.startswith("/"): return
        address = message.text.strip()
        if not is_valid_bsc_address(address):
            bot.reply_to(message, "⚠️ أرسل عنوان عقد BSC صحيحًا (0x + 40 hex characters).")
            return
        try:
            bot.send_chat_action(message.chat.id, "typing")
            result = run_analysis(address)
            if not result:
                bot.reply_to(message, "⚠️ لم أحصل على بيانات GoPlus مطابقة لهذا العقد. لم أستخدم عقدًا آخر ولم أخترع أرقامًا.")
                return
            # Keep one active MARSOF interface per chat.
            bot._marsof_ui = getattr(bot, "_marsof_ui", {})
            previous = bot._marsof_ui.get(message.chat.id)
            if previous:
                try:
                    bot.delete_message(message.chat.id, previous["message_id"])
                except Exception as delete_error:
                    print(f"⚠️ previous UI delete skipped: {delete_error}")
            sent = bot.send_message(message.chat.id, build_home(result), parse_mode="Markdown", reply_markup=keyboard())
            bot._marsof_ui[message.chat.id] = {"message_id": sent.message_id, "result": result}
        except requests.RequestException as e:
            print(f"❌ data source error: {e}"); bot.reply_to(message, "⚠️ تعذر الوصول إلى مصدر البيانات حاليًا. لم يتم اختراع أي نتيجة.")
        except Exception as e:
            print(f"❌ analysis error: {e}"); bot.reply_to(message, "⚠️ حدث خطأ تقني أثناء التحليل. لم يتم اختراع أي نتيجة.")

    @bot.callback_query_handler(func=lambda call: call.data.startswith("m:"))
    def callbacks(call):
        try:
            state = getattr(bot, "_marsof_ui", {}).get(call.message.chat.id)
            if not state:
                bot.answer_callback_query(call.id, "أعد إرسال العقد لإعادة بناء التحليل.")
                return
            category = call.data.split(":",1)[1]
            text = build_home(state["result"]) if category == "home" else section_text(state["result"], category)
            bot.edit_message_text(text, call.message.chat.id, call.message.message_id, parse_mode="Markdown", reply_markup=keyboard())
            bot.answer_callback_query(call.id)
        except Exception as e:
            print(f"❌ callback error: {e}")
            bot.answer_callback_query(call.id, "تعذر تحديث النافذة.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
