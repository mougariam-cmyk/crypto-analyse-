import os
import json
import requests
from datetime import datetime, timezone

from fastapi import FastAPI, Request
import telebot

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Boolean,
    Float,
    DateTime,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, sessionmaker


# ============================================================
# CONFIG
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

WEBHOOK_URL = "https://crypto-analyse-bot-z7o0.onrender.com/webhook"

GO_PLUS_URL = (
    "https://api.gopluslabs.io/api/v1/token_security/56"
)

CHAIN_ID = "56"  # BNB Smart Chain


# ============================================================
# TELEGRAM BOT
# ============================================================

bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None


# ============================================================
# DATABASE
# ============================================================

Base = declarative_base()

engine = None
SessionLocal = None

if DATABASE_URL:
    # Render may provide postgres:// while SQLAlchemy expects
    # postgresql://
    if DATABASE_URL.startswith("postgres://"):
        DATABASE_URL = DATABASE_URL.replace(
            "postgres://",
            "postgresql://",
            1,
        )

    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
    )

    SessionLocal = sessionmaker(
        autocommit=False,
        autoflush=False,
        bind=engine,
    )


class Token(Base):
    __tablename__ = "tokens"

    id = Column(Integer, primary_key=True)
    contract_address = Column(String(100), nullable=False)
    chain_id = Column(String(20), nullable=False)
    symbol = Column(String(50))
    first_seen = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
    )

    __table_args__ = (
        UniqueConstraint(
            "contract_address",
            "chain_id",
            name="uq_token_chain",
        ),
    )


class SecurityScan(Base):
    __tablename__ = "security_scans"

    id = Column(Integer, primary_key=True)
    token_id = Column(Integer, nullable=False)

    is_honeypot = Column(Boolean)
    buy_tax = Column(Float)
    sell_tax = Column(Float)
    is_open_source = Column(Boolean)
    is_mintable = Column(Boolean)
    holder_count = Column(Integer)

    score = Column(Float)
    verdict = Column(String(255))

    raw_data = Column(Text)

    scanned_at = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
    )


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

    __table_args__ = (
        UniqueConstraint(
            "chain_id",
            "tx_hash",
            name="uq_transaction_chain_hash",
        ),
    )


class LiquiditySnapshot(Base):
    __tablename__ = "liquidity_snapshots"

    id = Column(Integer, primary_key=True)

    chain_id = Column(String(20), nullable=False)
    token_address = Column(String(100), nullable=False)

    liquidity_usd = Column(Float)
    market_cap_usd = Column(Float)

    snapshot_time = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
    )


class HolderSnapshot(Base):
    __tablename__ = "holder_snapshots"

    id = Column(Integer, primary_key=True)

    chain_id = Column(String(20), nullable=False)
    token_address = Column(String(100), nullable=False)

    wallet = Column(String(100), nullable=False)
    balance = Column(Float)
    percentage = Column(Float)

    snapshot_time = Column(
        DateTime,
        default=lambda: datetime.now(timezone.utc),
    )


# ============================================================
# FASTAPI
# ============================================================

app = FastAPI(
    title="MARSOF AI",
    version="1.0.0",
)


# ============================================================
# HELPERS
# ============================================================

def utc_now():
    return datetime.now(timezone.utc)


def parse_real_number(value):
    """
    Return a real numeric value when the source actually
    provides one.

    Missing / empty / invalid values return None.
    IMPORTANT: None is NOT converted to 0.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        return float(value)

    if isinstance(value, str):
        value = value.strip()

        if not value:
            return None

        try:
            return float(value)
        except ValueError:
            return None

    return None


def parse_real_bool(value):
    """
    Parse recognizable boolean values only.
    Unknown / missing values return None.
    """
    if value is None:
        return None

    if isinstance(value, bool):
        return value

    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
        return None

    if isinstance(value, str):
        value = value.strip().lower()

        if value in {
            "1",
            "true",
            "yes",
            "y",
        }:
            return True

        if value in {
            "0",
            "false",
            "no",
            "n",
        }:
            return False

    return None


def normalize_address(address):
    return str(address).strip().lower()


def is_valid_bsc_address(address):
    """
    Basic BSC/EVM address validation.
    """
    if not address:
        return False

    address = address.strip()

    return (
        address.startswith("0x")
        and len(address) == 42
    )


# ============================================================
# DATABASE HELPERS
# ============================================================

def get_token(contract_address, symbol=None):
    if not SessionLocal:
        return None

    session = SessionLocal()

    try:
        address = normalize_address(contract_address)

        token = (
            session.query(Token)
            .filter(
                Token.contract_address == address,
                Token.chain_id == CHAIN_ID,
            )
            .first()
        )

        if token:
            if symbol and not token.symbol:
                token.symbol = symbol
                session.commit()

            return token

        token = Token(
            contract_address=address,
            chain_id=CHAIN_ID,
            symbol=symbol,
            first_seen=utc_now(),
        )

        session.add(token)
        session.commit()
        session.refresh(token)

        return token

    finally:
        session.close()


def save_security_scan(
    contract_address,
    data,
    score,
    verdict,
):
    if not SessionLocal:
        return

    symbol = data.get("token_symbol")

    token = get_token(
        contract_address,
        symbol=symbol,
    )

    if not token:
        return

    session = SessionLocal()

    try:
        scan = SecurityScan(
            token_id=token.id,
            is_honeypot=parse_real_bool(
                data.get("is_honeypot")
            ),
            buy_tax=parse_real_number(
                data.get("buy_tax")
            ),
            sell_tax=parse_real_number(
                data.get("sell_tax")
            ),
            is_open_source=parse_real_bool(
                data.get("is_open_source")
            ),
            is_mintable=parse_real_bool(
                data.get("is_mintable")
            ),
            holder_count=(
                int(data["holder_count"])
                if parse_real_number(data.get("holder_count"))
                is not None
                else None
            ),
            score=score,
            verdict=verdict,
            raw_data=json.dumps(
                data,
                ensure_ascii=False,
            ),
            scanned_at=utc_now(),
        )

        session.add(scan)
        session.commit()

    finally:
        session.close()


# ============================================================
# GOPLUS
# ============================================================

def fetch_goplus_token_security(contract_address):
    """
    Fetch token security data from GoPlus.

    IMPORTANT:
    We only accept the exact requested contract address.
    We never fall back to the first returned token.
    """
    address = normalize_address(contract_address)

    response = requests.get(
        GO_PLUS_URL,
        params={
            "contract_addresses": contract_address,
        },
        timeout=20,
    )

    response.raise_for_status()

    payload = response.json()

    if not isinstance(payload, dict):
        return None

    result = payload.get("result")

    if not isinstance(result, dict):
        return None

    # GoPlus normally returns a mapping:
    # address -> security data
    exact_data = None

    for returned_address, returned_data in result.items():
        if (
            normalize_address(returned_address)
            == address
        ):
            exact_data = returned_data
            break

    if not isinstance(exact_data, dict):
        return None

    # Keep only data that actually came from GoPlus.
    data = dict(exact_data)

    # Some useful aliases for our internal reporting.
    data["token_symbol"] = (
        data.get("token_symbol")
        or data.get("symbol")
    )

    return data


# ============================================================
# SECURITY REPORT
# ============================================================

def build_report(data):
    """
    Build a report from available real data.

    Missing values are never converted to zero.
    Missing fields are explicitly reported.
    """

    # --------------------------------------------------------
    # Raw values
    # --------------------------------------------------------

    is_honeypot = parse_real_bool(
        data.get("is_honeypot")
    )

    is_open_source = parse_real_bool(
        data.get("is_open_source")
    )

    is_mintable = parse_real_bool(
        data.get("is_mintable")
    )

    holder_count = parse_real_number(
        data.get("holder_count")
    )

    buy_tax_raw = parse_real_number(
        data.get("buy_tax")
    )

    sell_tax_raw = parse_real_number(
        data.get("sell_tax")
    )

    # GoPlus tax values are generally fractions:
    # 0.01 = 1%
    # Convert ONLY when a real value exists.
    buy_tax = (
        buy_tax_raw * 100
        if buy_tax_raw is not None
        else None
    )

    sell_tax = (
        sell_tax_raw * 100
        if sell_tax_raw is not None
        else None
    )

    # --------------------------------------------------------
    # Missing fields
    # --------------------------------------------------------

    missing = []

    if is_honeypot is None:
        missing.append("Honeypot status")

    if buy_tax is None:
        missing.append("Buy Tax")

    if sell_tax is None:
        missing.append("Sell Tax")

    if is_open_source is None:
        missing.append("Source-code verification")

    if is_mintable is None:
        missing.append("Mintable status")

    if holder_count is None:
        missing.append("Holder count")

    # --------------------------------------------------------
    # Score
    # --------------------------------------------------------

    # This score is ONLY based on tests whose data exists.
    # It is not a claim of absolute safety.
    score = 100.0

    completed_tests = 0

    if is_honeypot is not None:
        completed_tests += 1

        if is_honeypot:
            score -= 50

    if buy_tax is not None:
        completed_tests += 1

        if buy_tax > 5:
            score -= 20

    if sell_tax is not None:
        completed_tests += 1

        if sell_tax > 5:
            score -= 20

    if is_open_source is not None:
        completed_tests += 1

        if not is_open_source:
            score -= 15

    if is_mintable is not None:
        completed_tests += 1

        if is_mintable:
            score -= 15

    score = max(0, score)

    # --------------------------------------------------------
    # Verdict
    # --------------------------------------------------------

    if missing:
        verdict = (
            "⚠️ تحليل حالي مبني على البيانات المتاحة — "
            "الاختبار غير مكتمل"
        )
    elif score >= 80:
        verdict = (
            "🟢 مؤشرات الخطر المكتشفة منخفضة "
            "وفق الاختبارات المكتملة"
        )
    elif score >= 50:
        verdict = (
            "🟡 مؤشرات خطر متوسطة "
            "وفق الاختبارات المكتملة"
        )
    else:
        verdict = (
            "🔴 مؤشرات خطر مرتفعة "
            "وفق الاختبارات المكتملة"
        )

    # --------------------------------------------------------
    # Display values
    # --------------------------------------------------------

    if buy_tax is None:
        buy_tax_display = "غير متوفر"
    else:
        buy_tax_display = f"{buy_tax:g}%"

    if sell_tax is None:
        sell_tax_display = "غير متوفر"
    else:
        sell_tax_display = f"{sell_tax:g}%"

    if holder_count is None:
        holder_display = "غير متوفر"
    else:
        holder_display = f"{int(holder_count):,}"

    if is_honeypot is None:
        honeypot_display = "غير متوفر"
    elif is_honeypot:
        honeypot_display = "نعم"
    else:
        honeypot_display = "لا"

    if is_open_source is None:
        source_display = "غير متوفر"
    elif is_open_source:
        source_display = "نعم"
    else:
        source_display = "لا"

    if is_mintable is None:
        mintable_display = "غير متوفر"
    elif is_mintable:
        mintable_display = "نعم"
    else:
        mintable_display = "لا"

    symbol = data.get("token_symbol") or "غير متوفر"

    # --------------------------------------------------------
    # Report
    # --------------------------------------------------------

    report = (
        "📊 **MARSOF AI — التقرير الأمني**\n\n"
        f"🪙 الرمز: `{symbol}`\n\n"
        f"- Honeypot: `{honeypot_display}`\n"
        f"- Buy Tax: `{buy_tax_display}`\n"
        f"- Sell Tax: `{sell_tax_display}`\n"
        f"- Source Code Open: `{source_display}`\n"
        f"- Mintable: `{mintable_display}`\n"
        f"- Holders: `{holder_display}`\n\n"
        f"📈 **النتيجة الحالية:** `{score:g}/100`\n"
        f"{verdict}\n"
    )

    if missing:
        report += (
            "\n⚠️ **الاختبار غير مكتمل حالياً بسبب نقص المعلومات التالية:**\n"
        )

        for item in missing:
            report += f"- {item}\n"

    report += (
        "\nℹ️ جميع الأرقام الظاهرة في هذا التقرير مبنية "
        "على البيانات التي تم الحصول عليها فعلياً من المصدر. "
        "لم يتم تحويل المعلومات المفقودة إلى 0.\n"
    )

    return {
        "report": report,
        "score": score,
        "verdict": verdict,
        "missing": missing,
        "data": data,
    }


# ============================================================
# TELEGRAM WEBHOOK
# ============================================================

def setup_telegram_webhook():
    """
    Register the Telegram webhook automatically when
    Render starts the service.
    """
    if not TELEGRAM_TOKEN:
        print("❌ TELEGRAM_TOKEN غير موجود")
        return False

    try:
        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_TOKEN}/setWebhook"
        )

        response = requests.post(
            url,
            json={
                "url": WEBHOOK_URL,
                "allowed_updates": ["message"],
            },
            timeout=20,
        )

        response.raise_for_status()

        result = response.json()

        if result.get("ok"):
            print(
                "✅ Telegram webhook تم تسجيله بنجاح"
            )
            print(
                f"🔗 Webhook URL: {WEBHOOK_URL}"
            )
            return True

        print(
            "❌ Telegram رفض تسجيل webhook:"
        )
        print(result)

        return False

    except Exception as e:
        print(
            f"❌ Webhook setup error: {e}"
        )
        return False


def get_telegram_webhook_info():
    """
    Get current Telegram webhook status.
    """
    if not TELEGRAM_TOKEN:
        return {
            "ok": False,
            "error": "TELEGRAM_TOKEN غير موجود",
        }

    try:
        url = (
            f"https://api.telegram.org/"
            f"bot{TELEGRAM_TOKEN}/getWebhookInfo"
        )

        response = requests.get(
            url,
            timeout=20,
        )

        response.raise_for_status()

        return response.json()

    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
        }


# ============================================================
# FASTAPI STARTUP
# ============================================================

@app.on_event("startup")
def startup_event():
    print("🚀 MARSOF AI starting...")

    # Create database tables when database is configured.
    if engine:
        try:
            Base.metadata.create_all(bind=engine)
            print("✅ Database tables ready")
        except Exception as e:
            print(
                f"❌ Database initialization error: {e}"
            )
    else:
        print(
            "⚠️ DATABASE_URL غير موجود"
        )

    # Register Telegram webhook automatically.
    setup_telegram_webhook()

    # Print webhook status to Render logs.
    webhook_info = get_telegram_webhook_info()

    print("📡 Telegram Webhook Info:")
    print(
        json.dumps(
            webhook_info,
            ensure_ascii=False,
        )
    )


# ============================================================
# ROUTES
# ============================================================

@app.get("/")
def root():
    return {
        "status": "running",
        "service": "MARSOF AI",
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


@app.get("/telegram-webhook")
def telegram_webhook_status():
    """
    Public diagnostic endpoint.

    It does not expose the Telegram token.
    """
    return get_telegram_webhook_info()


# ============================================================
# TELEGRAM WEBHOOK ENDPOINT
# ============================================================

@app.post("/webhook")
async def telegram_webhook(request: Request):
    """
    Receive Telegram updates and pass them to pyTelegramBotAPI.
    """
    if not bot:
        return {
            "ok": False,
            "error": "TELEGRAM_TOKEN غير موجود",
        }

    try:
        update_data = await request.json()

        # pyTelegramBotAPI expects a list of update dictionaries.
        bot.process_new_updates([update_data])

        return {
            "ok": True,
        }

    except Exception as e:
        print(
            f"❌ Telegram webhook processing error: {e}"
        )

        return {
            "ok": False,
            "error": str(e),
        }


# ============================================================
# TELEGRAM MESSAGE HANDLER
# ============================================================

if bot:

    @bot.message_handler(commands=["start"])
    def handle_start(message):
        try:
            bot.reply_to(
                message,
                (
                    "أهلاً بك في MARSOF AI.\n\n"
                    "أرسل عنوان العقد لفحصه "
                    "بالبيانات الحقيقية فقط.\n\n"
                    "لن يتم اختراع أي رقم عند عدم توفر معلومة."
                ),
            )
        except Exception as e:
            print(
                f"❌ /start handler error: {e}"
            )


    @bot.message_handler(
        content_types=["text"]
    )
    def handle_text(message):
        text = (
            message.text.strip()
            if message.text
            else ""
        )

        # Ignore commands already handled above.
        if text.startswith("/"):
            return

        contract = text

        if not is_valid_bsc_address(contract):
            bot.reply_to(
                message,
                (
                    "⚠️ أرسل عنوان عقد BSC صحيحاً.\n\n"
                    "مثال:\n"
                    "`0x...`"
                ),
                parse_mode="Markdown",
            )
            return

        try:
            bot.send_chat_action(
                message.chat.id,
                "typing",
            )

            data = fetch_goplus_token_security(
                contract
            )

            if not data:
                bot.reply_to(
                    message,
                    (
                        "⚠️ لم يتم الحصول على بيانات حقيقية "
                        "مطابقة لهذا العقد من GoPlus.\n\n"
                        "لم يتم استخدام بيانات عقد آخر "
                        "ولم يتم اختراع أي قيمة."
                    ),
                )
                return

            result = build_report(data)

            # Save the scan to database when available.
            try:
                save_security_scan(
                    contract_address=contract,
                    data=result["data"],
                    score=result["score"],
                    verdict=result["verdict"],
                )
            except Exception as db_error:
                print(
                    f"⚠️ Security scan DB save error: "
                    f"{db_error}"
                )

            bot.reply_to(
                message,
                result["report"],
                parse_mode="Markdown",
            )

        except requests.HTTPError as e:
            print(
                f"❌ GoPlus HTTP error: {e}"
            )

            bot.reply_to(
                message,
                (
                    "⚠️ تعذر الحصول على بيانات GoPlus "
                    "حالياً.\n\n"
                    "لم يتم اختراع أي رقم."
                ),
            )

        except requests.RequestException as e:
            print(
                f"❌ GoPlus request error: {e}"
            )

            bot.reply_to(
                message,
                (
                    "⚠️ تعذر الاتصال بمصدر البيانات "
                    "حالياً.\n\n"
                    "لم يتم اختراع أي رقم."
                ),
            )

        except Exception as e:
            print(
                f"❌ Token analysis error: {e}"
            )

            bot.reply_to(
                message,
                (
                    "⚠️ حدث خطأ تقني أثناء تحليل العقد.\n"
                    "لم يتم اختراع أي نتيجة."
                ),
            )


# ============================================================
# LOCAL RUN
# ============================================================

if __name__ == "__main__":
    import uvicorn

    port = int(
        os.getenv(
            "PORT",
            "8000",
        )
    )

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
    )
