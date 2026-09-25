import os
import requests
from fastapi import FastAPI, Request
import telebot

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")

print(f"--- فحص قيمة التوكن: {'موجود ومحمل بنجاح' if TELEGRAM_TOKEN else 'مفقود تماماً!'} ---")

bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None
app = FastAPI()

@app.get("/")
def root():
    return {"status": "running"}

@app.post("/webhook")
async def webhook(request: Request):
    print(">>> تم استلام طلب في الـ Webhook بنجاح!")
    try:
        if not bot:
            print("خطأ: كائن البوت غير مُهندس لأن التوكن مفقود في متغيرات البيئة!")
            return {"ok": False, "error": "Bot token missing"}
            
        json_data = await request.json()
        update = telebot.types.Update.de_json(json_data)
        
        if update and update.message:
            chat_id = update.message.chat.id
            text = update.message.text
            print(f"رسالة من المستخدم {chat_id}: {text}")
            
            if text and text.startswith('/start'):
                bot.send_message(chat_id, "أهلاً بك في نظام فحص العمل رقمياً. أرسل عنوان العقد (Contract Address) الآن.")
            else:
                contract = text.strip()
                wait_msg = bot.send_message(chat_id, "جاري فحص العقد...")
                try:
                    api_url = f"https://api.gopluslabs.io/api/v1/token_security/sui?contract_addresses={contract}"
                    response = requests.get(api_url, timeout=10)
                    res_json = response.json()
                    result_dict = res_json.get("result", {})
                    
                    if not result_dict or contract.lower() not in result_dict:
                        bot.edit_message_text("لم يتم العثور على بيانات لهذا العقد.", chat_id=chat_id, message_id=wait_msg.message_id)
                        return
                        
                    info = result_dict[contract.lower()]
                    is_honeypot = info.get("is_honeypot", "0") == "1"
                    risk_score = 50 if is_honeypot else 10
                    status_text = "عالية المخاطر" if risk_score >= 50 else "سليمة تقنياً"
                    
                    report = f"تقرير فحص MARSOF AI:\n- الحالة: {status_text}\n- مؤشر المخاطر: {risk_score}/100"
                    bot.edit_message_text(report, chat_id=chat_id, message_id=wait_msg.message_id)
                except Exception as inner_e:
                    bot.edit_message_text(f"خطأ في الفحص: {str(inner_e)}", chat_id=chat_id, message_id=wait_msg.message_id)
                
        return {"ok": True}
    except Exception as e:
        print(f"حدث خطأ داخلي في المعالجة: {str(e)}")
        return {"ok": False, "error": str(e)}
