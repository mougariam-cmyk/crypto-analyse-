import os
import requests
from fastapi import FastAPI, Request
from pydantic import BaseModel
import telebot

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

app = FastAPI(title="MARSOF AI Engine", version="3.4.0")

class TokenCheckRequest(BaseModel):
    contract_address: str
    chain: str = "sui"

@app.get("/")
def home():
    return {"status": "online"}

@app.post("/webhook")
async def receive_telegram_update(request: Request):
    if not bot:
        return {"status": "error", "message": "Token not configured"}
    try:
        json_string = await request.body()
        update = telebot.types.Update.de_json(json_string.decode("utf-8"))
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
            return {"status": "error", "report_text": "العقد غير موجود أو غير مدعوم."}
            
        info = result_dict[contract.lower()]
        is_honeypot = info.get("is_honeypot", "0") == "1"
        risk_score = 50 if is_honeypot else 10
        status_text = "عالية المخاطر" if risk_score >= 50 else "سليمة تقنياً"
        
        report = f"تقرير فحص MARSOF AI:\n- الحالة: {status_text}\n- مؤشر المخاطر: {risk_score}/100"
        return {"status": "success", "report_text": report}
    except Exception as e:
        return {"status": "error", "report_text": str(e)}

if bot:
    @bot.message_handler(commands=['start', 'help'])
    def send_welcome(message):
        bot.reply_to(message, "مرحباً بك في نظام MARSOF AI الأمني.\nأرسل عنوان العقد (Contract Address) مباشرة لفحصه الآن.")

    @bot.message_handler(func=lambda message: True)
    def handle_message(message):
        contract = message.text.strip()
        if contract.startswith('/'):
            return
            
        wait_msg = bot.reply_to(message, "جاري فحص العقد عبر محرك MARSOF AI...")
        
        try:
            api_url = f"https://api.gopluslabs.io/api/v1/token_security/sui?contract_addresses={contract}"
            response = requests.get(api_url, timeout=10)
            res_json = response.json()
            result_dict = res_json.get("result", {})
            
            if not result_dict or contract.lower() not in result_dict:
                bot.edit_message_text("لم يتم العثور على بيانات لهذا العقد. تأكد من صحة العنوان.", chat_id=message.chat.id, message_id=wait_msg.message_id)
                return
                
            info = result_dict[contract.lower()]
            is_honeypot = info.get("is_honeypot", "0") == "1"
            risk_score = 50 if is_honeypot else 10
            status_text = "عالية المخاطر" if risk_score >= 50 else "سليمة تقنياً"
            
            report = f"تقرير فحص MARSOF AI:\n- الحالة: {status_text}\n- مؤشر المخاطر: {risk_score}/100"
            bot.edit_message_text(report, chat_id=message.chat.id, message_id=wait_msg.message_id)
        except Exception as e:
            bot.edit_message_text(f"حدث خطأ أثناء الاتصال: {str(e)}", chat_id=message.chat.id, message_id=wait_msg.message_id)
