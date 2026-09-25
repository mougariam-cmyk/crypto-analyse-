import os
import json
import requests
from datetime import datetime, timezone

from fastapi import FastAPI, Request
import telebot

from sqlalchemy import (
    create_engine, Column, Integer, String, Boolean, Float, DateTime,
    Text, ForeignKey, UniqueConstraint
)
from sqlalchemy.orm import declarative_base, sessionmaker

# =========================
# Configuration
# =========================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
app = FastAPI(title="MARSOF AI")

Base = declarative_base()
engine = None
SessionLocal = None

if DATABASE_URL:
    # Render may provide postgres://; SQLAlchemy expects postgresql://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=300,
    )
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


# =========================
# Database models
# =========================

class Token(Base):
    __tablename__ = "tokens"

    id = Column(Integer, primary_key=True)
    contract_address = Column(String(128), nullable=False)
    chain_id = Column(String(32), nullable=False)
    symbol = Column(String(64), nullable=True)
    first_seen = Column(DateTime(timezone=True), nullable=False)

    __table_args__ = (
        UniqueConstraint("contract_address", "chain_id", name="uq_token_chain"),
    )


class SecurityScan(Base):
    __tablename__ = "security_scans"

    id = Column(Integer, primary_key=True)
    token_id = Column(Integer, ForeignKey("tokens.id"), nullable=False)

    is_honeypot = Column(Boolean, nullable=True)
    buy_tax = Column(Float, nullable=True)
    sell_tax = Column(Float, nullable=True)
    is_open_source = Column(Boolean, nullable=True)
    is_mintable = Column(Boolean, nullable=True)
    holder_count = Column(String(64), nullable=True)

    score = Column(Integer, nullable=True)
    verdict = Column(String(255), nullable=True)

    # Full source response is preserved for future re-analysis.
    raw_data = Column(Text, nullable=False)

    scanned_at = Column(DateTime(timezone=True), nullable=False)


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True)
    chain_id = Column(String(32), nullable=False)
    token_address = Column(String(128), nullable=False)
    tx_hash = Column(String(128), nullable=False)
    wallet = Column(String(128), nullable=False)

    tx_type = Column(String(16), nullable=True)  # buy / sell / unknown
    amount = Column(Float, nullable=True)
    value_usd = Column(Float, nullable=True)
    block_time = Column(DateTime(timezone=True), nullable=True)

    raw_data = Column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint("chain_id", "tx_hash", name="uq_chain_tx"),
    )


class LiquiditySnapshot(Base):
    __tablename__ = "liquidity_snapshots"

    id = Column(Integer, primary_key=True)
    chain_id = Column(String(32), nullable=False)
    token_address = Column(String(128), nullable=False)
    liquidity_usd = Column(Float, nullable=True)
    market_cap_usd = Column(Float, nullable=True)
    snapshot_time = Column(DateTime(timezone=True), nullable=False)


class HolderSnapshot(Base):
    __tablename__ = "holder_snapshots"

    id = Column(Integer, primary_key=True)
    chain_id = Column(String(32), nullable=False)
    token_address = Column(String(128), nullable=False)
    wallet = Column(String(128), nullable=False)
    balance = Column(Float, nullable=True)
    percentage = Column(Float, nullable=True)
    snapshot_time = Column(DateTime(timezone=True), nullable=False)


def init_db():
    if engine is None:
        print("WARNING: DATABASE_URL is not configured. Database storage is disabled.")
        return
    Base.metadata.create_all(bind=engine)
    print("MARSOF AI database initialized successfully.")


@app.on_event("startup")
def startup():
    init_db()


# =========================
# Helpers
# =========================

CHAIN_ID = "56"  # BSC for the current GoPlus integration


def parse_real_number(value):
    """Return a number only when the source actually supplied one."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_real_bool(value):
    """Return True/False only when the source supplied a recognizable value."""
    if value is None or value == "":
        return None

    value = str(value).strip().lower()

    if value in {"1", "true", "yes"}:
        return True
    if value in {"0", "false", "no"}:
        return False

    return None


def get_token(session, contract):
    token = (
        session.query(Token)
        .filter_by(contract_address=contract.lower(), chain_id=CHAIN_ID)
        .first()
    )

    if token is None:
        token = Token(
            contract_address=contract.lower(),
            chain_id=CHAIN_ID,
            first_seen=datetime.now(timezone.utc),
        )
        session.add(token)
        session.commit()
        session.refresh(token)

    return token


def save_security_scan(contract, data, score, verdict):
    if SessionLocal is None:
        return

    session = SessionLocal()
    try:
        token = get_token(session, contract)

        scan = SecurityScan(
            token_id=token.id,
            is_honeypot=parse_real_bool(data.get("is_honeypot")),
            buy_tax=parse_real_number(data.get("buy_tax")),
            sell_tax=parse_real_number(data.get("sell_tax")),
            is_open_source=parse_real_bool(data.get("is_open_source")),
            is_mintable=parse_real_bool(data.get("is_mintable")),
            holder_count=(
                str(data["holder_count"])
                if data.get("holder_count") not in (None, "")
                else None
            ),
            score=score,
            verdict=verdict,
            raw_data=json.dumps(data, ensure_ascii=False),
            scanned_at=datetime.now(timezone.utc),
        )

        session.add(scan)
        session.commit()
    except Exception as e:
        session.rollback()
        print(f"Database save error: {e}")
    finally:
        session.close()


def fetch_goplus_token_security(contract):
    api_url = (
        "https://api.gopluslabs.io/api/v1/token_security/56"
        f"?contract_addresses={contract}"
    )

    response = requests.get(api_url, timeout=8)
    response.raise_for_status()

    payload = response.json()
    result = payload.get("result")

    if not isinstance(result, dict):
        return None

    # IMPORTANT:
    # Never accept an arbitrary first result. The returned address
    # must match the requested contract.
    target = contract.lower()

    for returned_address, item in result.items():
        if str(returned_address).lower() == target and isinstance(item, dict):
            return item

    return None


def build_report(contract, data):
    """
    Analyze only fields actually returned by the source.
    Missing fields remain missing; they are never converted to fake zeroes.
    """

    score = 100
    deductions = []
    missing = []

    is_honeypot = parse_real_bool(data.get("is_honeypot"))
    buy_tax_raw = parse_real_number(data.get("buy_tax"))
    sell_tax_raw = parse_real_number(data.get("sell_tax"))
    is_open_source = parse_real_bool(data.get("is_open_source"))
    is_mintable = parse_real_bool(data.get("is_mintable"))

    # GoPlus tax values are normally fractions, e.g. 0.05 = 5%.
    buy_tax = buy_tax_raw * 100 if buy_tax_raw is not None else None
    sell_tax = sell_tax_raw * 100 if sell_tax_raw is not None else None

    if is_honeypot is None:
        missing.append("Honeypot status")
    elif is_honeypot:
        score -= 50
        deductions.append("❌ Honeypot detected [-50]")
    else:
        deductions.append("✅ Honeypot not detected")

    if buy_tax is None:
        missing.append("Buy tax")
    elif buy_tax > 5:
        score -= 20
        deductions.append(
            f"❌ High buy tax: {buy_tax:g}% [-20]"
        )
    else:
        deductions.append(f"✅ Buy tax: {buy_tax:g}%")

    if sell_tax is None:
        missing.append("Sell tax")
    elif sell_tax > 5:
        score -= 20
        deductions.append(
            f"❌ High sell tax: {sell_tax:g}% [-20]"
        )
    else:
        deductions.append(f"✅ Sell tax: {sell_tax:g}%")

    if is_open_source is None:
        missing.append("Open-source status")
    elif not is_open_source:
        score -= 15
        deductions.append("❌ Source code not verified/open [-15]")
    else:
        deductions.append("✅ Source code is open/verified")

    if is_mintable is None:
        missing.append("Mint status")
    elif is_mintable:
        score -= 15
        deductions.append("❌ Mint capability detected [-15]")
    else:
        deductions.append("✅ Mint capability not detected")

    score = max(0, score)

    # Do not claim a final/complete result if important inputs are missing.
    if missing:
        verdict = "⚠️ تحليل حالي مبني على البيانات المتاحة — الاختبار غير مكتمل"
    elif score >= 80:
        verdict = "🟢 مؤشرات الخطر المكتشفة منخفضة وفق الاختبارات المكتملة"
    elif score >= 50:
        verdict = "🟡 مؤشرات خطر متوسطة وفق الاختبارات المكتملة"
    else:
        verdict = "🔴 مؤشرات خطر مرتفعة وفق الاختبارات المكتملة"

    return score, verdict, deductions, missing, buy_tax, sell_tax


# =========================
# HTTP endpoints
# =========================

@app.get("/")
def root():
    return {
        "status": "running",
        "service": "MARSOF AI",
        "database": bool(DATABASE_URL),
    }


@app.get("/health")
def health():
    return {
        "status": "ok",
        "telegram_token": bool(TELEGRAM_TOKEN),
        "database_configured": bool(DATABASE_URL),
    }


@app.post("/webhook")
async def webhook(request: Request):
    print(">>> Webhook request received")

    try:
        if not bot:
            return {"ok": False, "error": "TELEGRAM_TOKEN missing"}

        json_data = await request.json()
        update = telebot.types.Update.de_json(json_data)

        if not update or not update.message:
            return {"ok": True}

        chat_id = update.message.chat.id
        text = update.message.text or ""

        if text.startswith("/start"):
            bot.send_message(
                chat_id,
                "أهلاً بك في MARSOF AI.\n\n"
                "أرسل عنوان العقد لفحصه بالبيانات الحقيقية فقط.\n"
                "لن يتم اختراع أي رقم عند عدم توفر معلومة."
            )
            return {"ok": True}

        raw_text = text.strip()
        if not raw_text:
            return {"ok": True}

        wait_msg = bot.send_message(
            chat_id,
            "🔍 جاري جلب البيانات الحقيقية وتحليل العقد..."
        )

        contract = raw_text.split("::")[0].strip()

        try:
            data = fetch_goplus_token_security(contract)
        except Exception as e:
            print(f"GoPlus request error: {e}")
            bot.edit_message_text(
                "⚠️ حدث خطأ تقني أثناء الاتصال بمصدر البيانات الأمنية. "
                "لم يتم اختراع أي أرقام أو نتائج.",
                chat_id=chat_id,
                message_id=wait_msg.message_id,
            )
            return {"ok": True}

        if not data:
            bot.edit_message_text(
                "⚠️ لم تصل بيانات أمنية مطابقة لهذا العقد من المصدر الحالي. "
                "لذلك لم يتم إنشاء أرقام أو نتيجة وهمية.",
                chat_id=chat_id,
                message_id=wait_msg.message_id,
            )
            return {"ok": True}

        score, verdict, checks, missing, buy_tax, sell_tax = build_report(
            contract, data
        )

        holder_count = data.get("holder_count")
        if holder_count in (None, ""):
            holder_display = "غير متوفر من المصدر الحالي"
        else:
            holder_display = str(holder_count)

        missing_text = (
            "\n\n⚠️ **الاختبار غير مكتمل حالياً بسبب نقص المعلومات التالية:**\n"
            + "\n".join(f"- {item}" for item in missing)
            if missing
            else ""
        )

        checks_text = "\n".join(f"- {item}" for item in checks)

        buy_tax_display = f"{buy_tax:g}%" if buy_tax is not None else "غير متوفر"
        sell_tax_display = f"{sell_tax:g}%" if sell_tax is not None else "غير متوفر"

        report = (
            "📊 **MARSOF AI — التقرير الأمني**\n\n"
            f"📌 العقد: `{contract[:10]}...{contract[-6:]}`\n\n"
            f"🎯 **النتيجة الحالية:** {verdict}\n"
            f"📈 **المؤشر الحالي:** `{score}/100`\n\n"
            "🔢 **البيانات التي تم الحصول عليها فعلياً:**\n"
            f"- Buy Tax: `{buy_tax_display}`\n"
            f"- Sell Tax: `{sell_tax_display}`\n"
            f"- Holders: `{holder_display}`\n\n"
            "🔍 **نتائج الاختبارات المتاحة:**\n"
            f"{checks_text}"
            f"{missing_text}\n\n"
            "🔒 جميع الأرقام المعروضة مصدرها البيانات التي تم جلبها فعلياً؛ "
            "ولا يتم تحويل البيانات المفقودة إلى أرقام افتراضية."
        )

        save_security_scan(contract, data, score, verdict)

        bot.edit_message_text(
            report,
            chat_id=chat_id,
            message_id=wait_msg.message_id,
            parse_mode="Markdown",
        )

        return {"ok": True}

    except Exception as e:
        print(f"Webhook internal error: {e}")
        return {"ok": False, "error": str(e)}
