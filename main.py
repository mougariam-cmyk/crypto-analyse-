import os
import requests
from fastapi import FastAPI, Request
from pydantic import BaseModel
import telebot

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
bot = telebot.TeleBot(TELEGRAM_TOKEN, threaded=False) if TELEGRAM_TOKEN else None

app = FastAPI(title="MARSOF AI Engine", version="3.7.0")

class TokenCheckRequest(BaseModel):
    contract_address: str
    chain: str = "sui"

@app.get("/")
def home():
    return {"status": "online"}

@app.post("/webhook")
async def receive_telegram_update(request: Request):
    if not bot:
        return {"status": "error"}
    try:
        json_data = await request.json()
        update = telebot.types.Update.de_json(json_data)
        
        if update and update.message:
            chat_id = update.message.chat.id
            text = update.message.text
            
            if text:
                if text.strip().startswith('/start'):
                    bot.send_message(chat_id, "مرحباً بك في نظام MARSOF AI الأمني.\nأرسل عنوان العقد (Contract Address) مباشرة لفحصه الآن.")
                else:
                    contract = text.strip()
                    wait_msg = bot.send_message(chat_id, "جاري فحص العقد عبر محرك MARSOF AI...")
                    
                    try:
                        api_url = f"https://api.gopluslabs.io/api/v1/token_security/sui?contract_addresses={contract}"
                        response = requests.get(api_url, timeout=10)
                        res_json = response.json()
                        result_dict = res_json.get("result", {})
                        
                        if not result_dict or contract.lower() not in result_dict:
                            bot.edit_message_text("لم يتم العثور على بيانات لهذا العقد. تأكد من صحة العنوان.", chat_id=chat_id, message_id=wait_msg.message_id)
                            return
                            
                        info = result_dict[contract.lower()]
                        is_honeypot = info.get("is_honeypot", "0") == "1"
                        risk_score = 50 if is_honeypot else 10
                        status_text = "عالية المخاطر" if risk_score >= 50 else "سليمة تقنياً"
                        
                        report = f"تقرير فحص MARSOF AI:\n- الحالة: {status_text}\n- مؤشر المخاطر: {risk_score}/100"
                        bot.edit_message_text(report, chat_id=chat_id, message_id=wait_msg.message_id)
                    except Exception as err:
                        bot.edit_message_text(f"خطأ في الفحص: {str(err)}", chat_id=chat_id, message_id=wait_msg.message_id)
                        
        return {"status": "ok"}
    except Exception as e:
        return {"status": "error", "detail": str(e)}
