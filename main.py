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
                bot.send_message(chat_id, "أهلاً بك في نظام MARSOF AI. أرسل عنوان العقد لجلب البيانات والأرقام الحقيقية بدقة.")
            else:
                raw_text = text.strip()
                wait_msg = bot.send_message(chat_id, "🔍 جاري الاتصال المباشر وفحص البيانات الحقيقية للعقد...")
                
                contract = raw_text.split("::")[0] if "::" in raw_text else raw_text
                
                # جلب البيانات الحقيقية من API موثوق (مثال لشبكة BNB Chain / EVM التي تدعمها المنصة بأسلوب دقيق)
                api_url = f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={contract}"
                
                data = None
                try:
                    response = requests.get(api_url, timeout=8)
                    res_json = response.json()
                    result_dict = res_json.get("result", {})
                    if result_dict:
                        # البحث عن المفتاح بغض النظر عن حالة الأحرف
                        for k, v in result_dict.items():
                            if contract.lower() in k.lower() or k.lower() in contract.lower():
                                data = v
                                break
                        if not data and len(result_dict) > 0:
                            data = list(result_dict.values())[0]
                except Exception as e:
                    print(f"خطأ في الاتصال: {str(e)}")

                if not data:
                    bot.edit_message_text(f"⚠️ تعذر جلب بيانات حقيقية لهذا العنوان من السيرفر. تأكد أن العقد يتبع الشبكة المدعومة أو أرسل عنواناً صحيحاً.", chat_id=chat_id, message_id=wait_msg.message_id)
                    return

                # استخراج الأرقام الحقيقية بدقة من الـ JSON العائد
                is_honeypot = str(data.get("is_honeypot", "0"))
                buy_tax_raw = float(data.get("buy_tax", 0)) * 100
                sell_tax_raw = float(data.get("sell_tax", 0)) * 100
                is_open_source = str(data.get("is_open_source", "0"))
                is_mintable = str(data.get("is_mintable", "0"))
                can_take_back_ownership = str(data.get("can_take_back_ownership", "0"))
                holder_count = data.get("holder_count", "غير متوفر")
                total_supply = data.get("total_supply", "غير متوفر")

                # خوارزمية التنقيط الحقيقية بناءً على الأرقام المستلمة
                score = 100
                deductions = []

                if is_honeypot == "1":
                    score -= 50
                    deductions.append("❌ العقد Honeypot (ممنوع البيع) [-50 نقطة]")
                else:
                    deductions.append("✅ العقد ليس Honeypot [0 خصم]")

                if buy_tax_raw > 5 or sell_tax_raw > 5:
                    score -= 20
                    deductions.append(f"❌ ضرائب مرتفعة (شراء: {buy_tax_raw}% / بيع: {sell_tax_raw}%) [-20 نقطة]")
                else:
                    deductions.append(f"✅ الضرائب ضمن الطبيعي (شراء: {buy_tax_raw}% / بيع: {sell_tax_raw}%) [0 خصم]")

                if is_open_source != "1":
                    score -= 15
                    deductions.append("❌ الكود غير مكشوف/موثق [-15 نقطة]")
                else:
                    deductions.append("✅ الكود مكشوف وموثق [0 خصم]")

                if is_mintable == "1":
                    score -= 15
                    deductions.append("❌ صلاحية طباعة عملات جديدة (Mint) مفعلة [-15 نقطة]")
                else:
                    deductions.append("✅ صلاحية طباعة العملات مغلقة [0 خصم]")

                score = max(0, score)

                # تحديد النتيجة بالاعتماد على النقاط الحقيقية
                if score >= 80:
                    verdict = "🟢 عملة نظيفة وآمنة"
                elif score >= 50:
                    verdict = "🟡 عملة متوسطة الخطورة"
                else:
                    verdict = "🔴 عملة خطيرة جداً (محتالة)"

                deductions_text = "\n".join([f"- {item}" for item in deductions])

                report = (
                    f"📊 **التقرير التحليلي الرقمي المباشر (MARSOF AI):**\n"
                    f"📌 العقد: `{contract[:10]}...{contract[-6:]}`\n\n"
                    f"🎯 **النتيجة:** {verdict}\n"
                    f"📈 **مؤشر الأمان الحقيقي:** `{score}/100`\n\n"
                    f"🔢 **الأرقام والبيانات الفعلية:**\n"
                    f"- ضريبة الشراء الحقيقية: `{buy_tax_raw}%`\n"
                    f"- ضريبة البيع الحقيقية: `{sell_tax_raw}%`\n"
                    f"- عدد الحاملين (Holders): `{holder_count}`\n"
                    f"- إجمالي المعروض (Supply): `{total_supply}`\n\n"
                    f"🔍 **تفاصيل التنقيط وخطوط الخطر:**\n"
                    f"{deductions_text}"
                )

                bot.edit_message_text(report, chat_id=chat_id, message_id=wait_msg.message_id, parse_mode="Markdown")
                
        return {"ok": True}
    except Exception as e:
        print(f"خطأ داخلي: {str(e)}")
        return {"ok": False, "error": str(e)}
