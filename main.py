import os
import fastapi
from fastapi import FastAPI, Request
import telebot

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
bot = telebot.TeleBot(TELEGRAM_TOKEN) if TELEGRAM_TOKEN else None

app = FastAPI()

@app.get("/")
def root():
    return {"status": "running"}

@app.post("/webhook")
async def webhook(request: Request):
    print(">>> تم استلام طلب في الـ Webhook بنجاح!")
    try:
        json_data = await request.json()
        print(f"بيانات الطلب: {json_data}")
        
        update = telebot.types.Update.de_json(json_data)
        if update and update.message:
            chat_id = update.message.chat.id
            text = update.message.text
            print(f"رسالة من المستخدم {chat_id}: {text}")
            
            if text and text.startswith('/start'):
                bot.send_message(chat_id, "أهلاً بك! تم استلام أمر البدء بنجاح.")
                
        return {"ok": True}
    except Exception as e:
        print(f"حدث خطأ داخلي: {str(e)}")
        return {"ok": False, "error": str(e)}
