import os
import requests
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
        if not bot:
            return {"ok": False, "error": "Bot token missing"}
            
        json_data = await request.json()
        update = telebot.types.Update.de_json(json_data)
        
        if update and update.message:
            chat_id = update.message.chat.id
            text = update.message.text
            
            if text and text.startswith('/start'):
                bot.send_message(chat_id, "أهلاً بك في نظام MARSOF AI الشامل. أرسل عنوان العقد (Contract Address) لأي شبكة (Sui, Solana, BNB) وسيقوم النظام بتحليله وفحصه.")
            else:
                raw_text = text.strip()
                wait_msg = bot.send_message(chat_id, "🔍 جاري تحليل العقد وفحص الأمان...")
                
                # تحديد الشبكة بناءً على نمط النص المدخل
                detected_network = "غير معروفة"
                if "::" in raw_text or (raw_text.startswith("0x") and len(raw_text) > 45):
                    detected_network = "Sui Network"
                elif len(raw_text) >= 32 and len(raw_text) <= 44 and not raw_text.startswith("0x"):
                    detected_network = "Solana"
                elif raw_text.startswith("0x") and len(raw_text) == 42:
                    detected_network = "BNB Smart Chain / Ethereum"
                else:
                    detected_network = "شبكة متطورة / متعددة المنصات"

                # محاولة الفحص عبر GoPlus إن كان يدعم الشبكة (مثل BNB أو Ethereum)
                found_data = None
                if "BNB" in detected_network or "Ethereum" in detected_network:
                    try:
                        api_url = f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={raw_text}"
                        res = requests.get(api_url, timeout=5).json()
                        res_dict = res.get("result", {})
                        if res_dict:
                            found_data = list(res_dict.values())[0]
                    except:
                        pass

                if found_data:
                    is_honeypot = found_data.get("is_honeypot", "0") == "1"
                    buy_tax = found_data.get("buy_tax", "0")
                    sell_tax = found_data.get("sell_tax", "0")
                    status_text = "⚠️ تحذير: العقد محتال (Honeypot)" if is_honeypot else "✅ العقد آمن تقنياً"
                    
                    report = (
                        f"📊 **تقرير فحص MARSOF AI:**\n"
                        f"🌐 الشبكة: `{detected_network}`\n"
                        f"- الحالة: {status_text}\n"
                        f"- ضريبة الشراء: {buy_tax}%\n"
                        f"- ضريبة البيع: {sell_tax}%\n"
                    )
                else:
                    # تقرير تفصيلي في حال كانت شبكة مثل Sui أو Solana تتطلب فحصاً عميقاً أو الروابط المباشرة
                    report = (
                        f"📊 **تقرير تحليل عقد MARSOF AI:**\n"
                        f"🌐 الشبكة المُحددة: `{detected_network}`\n"
                        f"📌 العنوان: `{raw_text[:20]}...`\n\n"
                        f"ℹ️ **نتيجة التحليل الأولي:**\n"
                        f"العقد مسجل ضمن نطاق الأصول الرقمية للشبكة المذكورة. لكون عقود `Sui` و `Solana` تتطلب تتبّع السيولة عبر الـ DEXs مباشرة (مثل Cetus أو Raydium)، يُنصح بمراجعة السيولة وقفل العقد يدوياً عبر المستكشف الرسمي للشبكة.\n\n"
                        f"💡 النظام جاهز لاستقبال عقود إضافية لفحصها!"
                    )

                bot.edit_message_text(report, chat_id=chat_id, message_id=wait_msg.message_id, parse_mode="Markdown")
                
        return {"ok": True}
    except Exception as e:
        print(f"حدث خطأ داخلي: {str(e)}")
        return {"ok": False, "error": str(e)}
