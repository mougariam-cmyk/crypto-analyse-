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
                bot.send_message(chat_id, "أهلاً بك في نظام MARSOF AI الشامل. أرسل عنوان العقد لأي شبكة (Sui, Solana, BNB, Ethereum) وسيقوم البوت بفحصه بدقة.")
            else:
                raw_text = text.strip()
                wait_msg = bot.send_message(chat_id, "جاري تحليل وتحديد الشبكة وفحص العقد...")
                
                # معالجة عناوين Sui التي تحتوي على رموز :: لاستخراج عنوان العقد الحقيقي
                contract = raw_text.split("::")[0] if "::" in raw_text else raw_text
                
                apis_to_check = []
                
                # التوجيه الذكي للشبكات بناءً على شكل العنوان
                if "::" in raw_text or len(contract) > 42:
                    # غالباً شبكة Sui أو Solana
                    apis_to_check = [
                        ("Sui Network", f"https://api.gopluslabs.io/api/v1/token_security/sui?contract_addresses={contract}"),
                        ("Solana", f"https://api.gopluslabs.io/api/v1/token_security/solana?contract_addresses={contract}")
                    ]
                else:
                    # شبكات EVM (مثل BNB Smart Chain و Ethereum)
                    apis_to_check = [
                        ("BNB Smart Chain", f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={contract}"),
                        ("Ethereum", f"https://api.gopluslabs.io/api/v1/token_security/1?contract_addresses={contract}")
                    ]
                
                found_data = None
                network_name = ""
                
                for net_title, api_url in apis_to_check:
                    try:
                        response = requests.get(api_url, timeout=7)
                        res_json = response.json()
                        result_dict = res_json.get("result", {})
                        
                        found_key = next((k for k in result_dict.keys() if k.lower() == contract.lower()), None)
                        if found_key:
                            found_data = result_dict[found_key]
                            network_name = net_title
                            break
                    except Exception:
                        continue
                
                if not found_data:
                    bot.edit_message_text("لم يتم العثور على بيانات لهذا العقد في الشبكات المدعومة. تأكد من صحة العنوان.", chat_id=chat_id, message_id=wait_msg.message_id)
                    return
                
                is_honeypot = found_data.get("is_honeypot", "0") == "1"
                buy_tax = found_data.get("buy_tax", "0")
                sell_tax = found_data.get("sell_tax", "0")
                open_source = found_data.get("is_open_source", "0") == "1"
                
                status_text = "⚠️ تحذير: العقد محتال (Honeypot)" if is_honeypot else "✅ العقد آمن تقنياً"
                source_text = "مكشوف المصدر" if open_source else "غير مكشوف المصدر"
                
                report = (
                    f"📊 **تقرير فحص MARSOF AI:**\n"
                    f"🌐 الشبكة المكتشفة: `{network_name}`\n"
                    f"- الحالة: {status_text}\n"
                    f"- حالة العقد: {source_text}\n"
                    f"- ضريبة الشراء: {buy_tax}%\n"
                    f"- ضريبة البيع: {sell_tax}%\n"
                )
                bot.edit_message_text(report, chat_id=chat_id, message_id=wait_msg.message_id, parse_mode="Markdown")
                
        return {"ok": True}
    except Exception as e:
        print(f"حدث خطأ داخلي: {str(e)}")
        return {"ok": False, "error": str(e)}
