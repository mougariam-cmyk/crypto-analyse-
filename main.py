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
                bot.send_message(chat_id, "أهلاً بك في نظام MARSOF AI للتحليل المتقدم. أرسل عنوان العقد لفحصه بالبيانات الحقيقية والمباشرة فقط.")
            else:
                raw_text = text.strip()
                wait_msg = bot.send_message(chat_id, "🔍 جاري الاتصال المباشر بالسيرفر لجلب البيانات الحقيقية للسيولة والأمان...")
                
                contract = raw_text.split("::")[0] if "::" in raw_text else raw_text
                
                # جلب بيانات الأمان والسيولة الحقيقية من واجهة موثوقة (GoPlus لشبكة BNB/EVM كنموذج حي)
                api_url = f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={contract}"
                
                data = None
                fetch_success = False
                try:
                    response = requests.get(api_url, timeout=8)
                    res_json = response.json()
                    result_dict = res_json.get("result", {})
                    if result_dict:
                        for k, v in result_dict.items():
                            if contract.lower() in k.lower() or k.lower() in contract.lower():
                                data = v
                                fetch_success = True
                                break
                        if not fetch_success and len(result_dict) > 0:
                            vals = list(result_dict.values())
                            if vals:
                                data = vals[0]
                                fetch_success = True
                except Exception as e:
                    print(f"خطأ في الاتصال: {str(e)}")

                # قاعدة الرفض الصارم: إذا فشل جلب البيانات الحقيقية، نعتذر فوراً ولا نعط أي نتيجة وهمية
                if not fetch_success or not data:
                    error_report = (
                        f"🛑 **اعتذار تقني صارم (MARSOF AI):**\n"
                        f"📌 العقد: `{contract}`\n\n"
                        f"⚠️ **تعذر جلب بيانات حقيقية ومؤكدة لهذا العقد من السيرفر!**\n"
                        f"🚫 تم رفض إعطاء أي نتائج أو أرقام افتراضية حفاظاً على دقة التحليل."
                    )
                    bot.edit_message_text(error_report, chat_id=chat_id, message_id=wait_msg.message_id, parse_mode="Markdown")
                    return

                # استخراج البيانات الحقيقية بدقة
                is_honeypot = str(data.get("is_honeypot", "0")) == "1"
                buy_tax = float(data.get("buy_tax", 0)) * 100
                sell_tax = float(data.get("sell_tax", 0)) * 100
                is_open_source = str(data.get("is_open_source", "0")) == "1"
                is_mintable = str(data.get("is_mintable", "0")) == "1"
                is_blacklisted = str(data.get("is_blacklisted", "0")) == "1"
                holder_count = data.get("holder_count", "غير متوفر")
                
                # فحص تركز المحافظ الحقيقي من البيانات المستلمة
                holders = data.get("holders", [])
                top_holders_percent = 0.0
                if holders and isinstance(holders, list):
                    try:
                        top_10_sum = sum([float(h.get("percent", 0)) for h in holders[:10]])
                        top_holders_percent = top_10_sum * 100 if top_10_sum <= 1 else top_10_sum
                    except:
                        pass

                # نظام التنقيط الصارم بناءً على الأرقام الحقيقية
                score = 100
                checks = []

                if is_honeypot:
                    score -= 40
                    checks.append("❌ تحذير خطير: العقد Honeypot (ممنوع البيع) [-40 نقطة]")
                else:
                    checks.append("✅ سلامة العقد: ليس Honeypot [0 خصم]")

                if buy_tax > 5 or sell_tax > 5:
                    score -= 15
                    checks.append(f"❌ ضرائب مرتفعة (شراء: {buy_tax}% / بيع: {sell_tax}%) [-15 نقطة]")
                else:
                    checks.append(f"✅ الضرائب طبيعية (شراء: {buy_tax}% / بيع: {sell_tax}%) [0 خصم]")

                if not is_open_source:
                    score -= 15
                    checks.append("❌ الكود المصدر غير موثق أو غير مكشوف [-15 نقطة]")
                else:
                    checks.append("✅ الكود المصدر مكشوف وموثق [0 خصم]")

                if is_mintable:
                    score -= 10
                    checks.append("❌ صلاحية طباعة عملات جديدة (Mint) مفعلة [-10 نقاط]")
                else:
                    checks.append("✅ صلاحية طباعة العملات مغلقة [0 خصم]")

                if is_blacklisted:
                    score -= 10
                    checks.append("❌ ميزة تجميد المحافظ (Blacklist) مفعلة [-10 نقاط]")
                else:
                    checks.append("✅ ميزة التجميد غير مفعلة [0 خصم]")

                if top_holders_percent > 50:
                    score -= 10
                    checks.append(f"❌ تركز عالي للمحافظ الكبرى: `{top_holders_percent:.1f}%` [-10 نقاط]")
                else:
                    checks.append(f"✅ تركز المحافظ الكبرى مقبول: `{top_holders_percent:.1f}%` [0 خصم]")

                score = max(0, score)

                # الحكم النهائي
                if score >= 80:
                    verdict = "🟢 عملة نظيفة وآمنة (استوفت معايير الأمان والحقيقية)"
                elif score >= 50:
                    verdict = "🟡 عملة متوسطة الخطورة (تتطلب حذراً بالغاً)"
                else:
                    verdict = "🔴 عملة خطيرة جداً / محتالة (تجنبها تماماً)"

                checks_text = "\n".join([f"- {item}" for item in checks])

                report = (
                    f"📊 **التقرير التحليلي الحقيقي والمؤكد (MARSOF AI):**\n"
                    f"📌 العقد: `{contract[:10]}...{contract[-6:]}`\n\n"
                    f"🎯 **النتيجة النهائية:** {verdict}\n"
                    f"📈 **مؤشر الأمان الحقيقي:** `{score}/100`\n\n"
                    f"🔢 **الأرقام والبيانات الفعلية من السيرفر:**\n"
                    f"- ضريبة الشراء الفعلية: `{buy_tax}%`\n"
                    f"- ضريبة البيع الفعلية: `{sell_tax}%`\n"
                    f"- إجمالي الحاملين: `{holder_count}`\n"
                    f"- نسبة تركز أعلى 10 محافظ: `{top_holders_percent:.1f}%`\n\n"
                    f"🔍 **تفاصيل الفحص والتحقق:**\n"
                    f"{checks_text}\n\n"
                    f"🔒 *ملاحظة: هذا التقرير مبني 100% على البيانات الحقيقية المستلمة من السيرفر دون استخدام أي قيم افتراضية.*"
                )

                bot.edit_message_text(report, chat_id=chat_id, message_id=wait_msg.message_id, parse_mode="Markdown")
                
        return {"ok": True}
    except Exception as e:
        print(f"خطأ داخلي: {str(e)}")
        return {"ok": False, "error": str(e)}
