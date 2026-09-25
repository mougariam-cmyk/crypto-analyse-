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
                bot.send_message(chat_id, "أهلاً بك في نظام MARSOF AI. أرسل عنوان العقد لأي شبكة لفحصه الآن.")
            else:
                raw_text = text.strip()
                wait_msg = bot.send_message(chat_id, "جاري فحص العقد في كافة الشبكات...")
                
                # استخراج العنوان الأساسي بدقة (في حال وجود رموز إضافية مثل ::)
                contract = raw_text.split("::")[0] if "::" in raw_text else raw_text
                
                # قائمة بكل معرفات الشبكات المتاحة في GoPlus (Sui, Solana, BNB, Ethereum)
                chains_to_check = [
                    ("Sui Network", f"https://api.gopluslabs.io/api/v1/token_security/sui?contract_addresses={contract}"),
                    ("Solana", f"https://api.gopluslabs.io/api/v1/token_security/solana?contract_addresses={contract}"),
                    ("BNB Smart Chain", f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={contract}"),
                    ("Ethereum", f"https://api.gopluslabs.io/api/v1/token_security/1?contract_addresses={contract}")
                ]
                
                found_data = None
                network_name = ""
                
                for net_title, api_url in chains_to_check:
                    try:
                        print(f"جاري الفحص على {net_title} باستخدام الرابط: {api_url}")
                        response = requests.get(api_url, timeout=6)
                        res_json = response.json()
                        print(f"نتيجة {net_title}: {res_json}")
                        
                        result_dict = res_json.get("result", {})
                        if result_dict:
                            # البحث عن العنوان بغض النظر عن حالة الأحرف
                            for k, v in result_dict.items():
                                if contract.lower() in k.lower() or k.lower() in contract.lower():
                                    found_data = v
                                    network_name = net_title
                                    break
                        if found_data:
                            break
                    except Exception as e:
                        print(f"خطأ في الاتصال بـ {net_title}: {str(e)}")
                        continue
                
                if not found_data:
                    bot.edit_message_text("لم يتم العثور على بيانات لهذا العقد في أي من الشبكات المدعومة. تأكد من صحة العنوان المرسل.", chat_id=chat_id, message_id=wait_msg.message_id)
                    return
                
                is_honeypot = found_data.get("is_honeypot", "0") == "1"
                buy_tax = found_data.get("buy_tax", "0")
                sell_tax = found_data.get("sell_tax", "0")
                
                status_text = "⚠️ تحذير: العقد قد يكون محتالاً (Honeypot)" if is_honeypot else "✅ العقد آمن تقنياً"
                
                report = (
                    f"📊 **تقرير فحص MARSOF AI الشامل:**\n"
                    f"🌐 الشبكة المكتشفة: `{network_name}`\n"
                    f"- الحالة: {status_text}\n"
                    f"- ضريبة الشراء: {buy_tax}%\n"
                    f"- ضريبة البيع: {sell_tax}%\n"
                )
                bot.edit_message_text(report, chat_id=chat_id, message_id=wait_msg.message_id, parse_mode="Markdown")
                
        return {"ok": True}
    except Exception as e:
        print(f"حدث خطأ داخلي: {str(e)}")
        return {"ok": False, "error": str(e)}
