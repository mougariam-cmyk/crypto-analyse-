import os
import requests
from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel
import telebot

TELEGRAM_TOKEN = "8914016582:AAEPtfe_8IlT_cOf3SLtxdyseV_k3j94raY"
RENDER_URL = "https://crypto-analyse-bot-z7o0.onrender.com"

bot = telebot.TeleBot(TELEGRAM_TOKEN)
app = FastAPI(title="MARSOF AI Production Engine", version="3.0.0")

class TokenCheckRequest(BaseModel):
    contract_address: str
    chain: str = "sui"

@app.on_event("startup")
def setup_webhook():
    """ربط البوت تلقائياً برابط الويب هوك الخاص بسيرفرك على Render عند الإقلاع"""
    webhook_url = f"{RENDER_URL}/webhook"
    try:
        bot.remove_webhook()
        bot.set_webhook(url=webhook_url)
    except Exception as e:
        print(f"Webhook setup error: {e}")

@app.get("/")
def home():
    return {"status": "online", "system": "MARSOF AI Webhook Engine is active."}

@app.post("/webhook")
async def receive_telegram_update(request: Request):
    """استقبال تحديثات تيليغرام وإيقاظ السيرفر تلقائياً عند وصول أي رسالة"""
    try:
        json_data = await request.json()
        update = telebot.types.Update.de_json(json_data)
        bot.process_new_updates([update])
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}

@app.post("/analyze-token")
def analyze_token(data: TokenCheckRequest):
    contract = data.contract_address.strip()
    chain = data.chain.strip().lower()
    api_url = f"https://api.gopluslabs.io/api/v1/token_security/{chain}?contract_addresses={contract}"
    
    try:
        response = requests.get(api_url, timeout=10)
        res_json = response.json()
        result_dict = res_json.get("result", {})
        
        if not result_dict or contract.lower() not in result_dict:
            raise HTTPException(status_code=404, detail="العقد غير موجود أو غير مدعوم.")
            
        info = result_dict[contract.lower()]
        is_honeypot = info.get("is_honeypot", "0") == "1"
        is_mintable = info.get("mintable", "0") == "1"
        
        risk_score = 50 if is_honeypot else (25 if is_mintable else 0)
        status_text = "عالية المخاطر" if risk_score >= 50 else ("متوسطة المخاطر" if risk_score > 0 else "سليمة تقنياً")
        
        report = f"""
تقرير فحص الأمان والنمط التداولي - MARSOF AI

التصنيف: {status_text}
مؤشر المخاطر: {risk_score} / 100
        """.strip()
        
        return {
            "status": "success",
            "contract_address": contract,
            "risk_score": risk_score,
            "report_text": report
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# معالجة رسائل تيليغرام مباشرة عبر البوت
@bot.message_handler(commands=['start', 'help'])
def send_welcome(message):
    bot.reply_to(message, "مرحباً بك في نظام MARSOF AI الأمني السحابي.\nأرسل عنوان العقد (Contract Address) لفحصه الآن.")

@bot.message_handler(func=lambda message: True)
def handle_contract_query(message):
    contract = message.text.strip()
    wait_msg = bot.reply_to(message, "جاري إيقاظ المحرك وفحص العقد...")
    
    try:
        # استدعاء محلي داخلي أو عبر الـ API لفحص العقد
        api_url = f"https://api.gopluslabs.io/api/v1/token_security/sui?contract_addresses={contract}"
        response = requests.get(api_url, timeout=10)
        res_json = response.json()
        result_dict = res_json.get("result", {})
        
        if not result_dict or contract.lower() not in result_dict:
            bot.edit_message_text("تعذر العثور على بيانات لهذا العقد. تأكد من صحة العنوان.", chat_id=message.chat.id, message_id=wait_msg.message_id)
            return
            
        info = result_dict[contract.lower()]
        risk_score = 50 if info.get("is_honeypot", "0") == "1" else 10
        status_text = "عالية المخاطر" if risk_score >= 50 else "سليمة تقنياً"
        
        report = f"نتائج فحص MARSOF AI:\n- الحالة: {status_text}\n- مؤشر المخاطر: {risk_score}/100"
        bot.edit_message_text(report, chat_id=message.chat.id, message_id=wait_msg.message_id)
    except Exception as e:
        bot.edit_message_text(f"حدث خطأ أثناء المعالجة: {str(e)}", chat_id=message.chat.id, message_id=wait_msg.message_id)
