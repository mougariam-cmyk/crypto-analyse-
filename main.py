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
                bot.send_message(chat_id, "أهلاً بك في نظام MARSOF AI المتقدم. أرسل عنوان العقد لفحصه بالكامل (السلامة، السيولة، والتركز).")
            else:
                raw_text = text.strip()
                wait_msg = bot.send_message(chat_id, "🔍 جاري فحص معايير السلامة، السيولة، وتركز المحافظ من السيرفر...")
                
                contract = raw_text.split("::")[0] if "::" in raw_text else raw_text
                
                # جلب البيانات الأمنية من GoPlus
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

                if not fetch_success or not data:
                    error_report = (
                        f"🛑 **اعتذار تقني (MARSOF AI):**\n"
                        f"📌 العقد: `{contract}`\n\n"
                        f"⚠️ تعذر جلب بيانات حقيقية لهذا العقد من السيرفر. تأكد من صحة العنوان أو دعم الشبكة."
                    )
                    bot.edit_message_text(error_report, chat_id=chat_id, message_id=wait_msg.message_id, parse_mode="Markdown")
                    return

                # استخراج المعايير الحقيقية من السيرفر
                is_honeypot = str(data.get("is_honeypot", "0")) == "1"
                buy_tax = float(data.get("buy_tax", 0)) * 100
                sell_tax = float(data.get("sell_tax", 0)) * 100
                is_open_source = str(data.get("is_open_source", "0")) == "1"
                is_mintable = str(data.get("is_mintable", "0")) == "1"
                is_blacklisted = str(data.get("is_blacklisted", "0")) == "1"
                can_take_back_ownership = str(data.get("can_take_back_ownership", "0")) == "1"
                holder_count = data.get("holder_count", "غير متوفر")
                
                # جلب بيانات تركز المحافظ والسيولة إن وجدت في الاستجابة
                lp_holder_count = data.get("lp_holder_count", "غير متوفر")
                lp_total_supply = data.get("lp_total_supply", "غير متوفر")
                
                holders = data.get("holders", [])
                top_holders_percent = 0.0
                if holders and isinstance(holders, list):
                    try:
                        # حساب نسبة أول 10 محافظ كبرى
                        top_10_sum = sum([float(h.get("percent", 0)) for h in holders[:10]])
                        top_holders_percent = top_10_sum * 100 if top_10_sum <= 1 else top_10_sum
                    except:
                        pass

                # نظام التنقيط الدقيق بناءً على المعايير الكاملة (100 نقطة)
                score = 100
                checks = []

                # 1. الهانيبوت
                if is_honeypot:
                    score -= 40
                    checks.append("❌ Honeypot مفعل (ممنوع البيع) [-40 نقطة]")
                else:
                    checks.append("✅ ليس Honeypot [0 خصم]")

                # 2. الضرائب
                if buy_tax > 5 or sell_tax > 5:
                    score -= 15
                    checks.append(f"❌ ضرائب مرتفعة (شراء: {buy_tax}% / بيع: {sell_tax}%) [-15 نقطة]")
                else:
                    checks.append(f"✅ الضرائب طبيعية (شراء: {buy_tax}% / بيع: {sell_tax}%) [0 خصم]")

                # 3. الكود المصدر
                if not is_open_source:
                    score -= 15
                    checks.append("❌ الكود غير موثق/مكشوف [-15 نقطة]")
                else:
                    checks.append("✅ الكود مكشوف وموثق [0 خصم]")

                # 4. Mint (طباعة العملات)
                if is_mintable:
                    score -= 10
                    checks.append("❌ صلاحية Mint (طباعة عملات جديدة) مفعلة [-10 نقاط]")
                else:
                    checks.append("✅ صلاحية طباعة العملات مغلقة [0 خصم]")

                # 5. Blacklist (تجميد المحافظ)
                if is_blacklisted:
                    score -= 10
                    checks.append("❌ ميزة تجميد المحافظ (Blacklist) مفعلة [-10 نقاط]")
                else:
                    checks.append("✅ ميزة التجميد غير مفعلة [0 خصم]")

                # 6. صلاحية استعادة الملكية
                if can_take_back_ownership:
                    score -= 10
                    checks.append("❌ يمكن للمالك استعادة الصلاحيات [-10 نقاط]")
                else:
                    checks.append("✅ لا يمكن للمالك استعادة الصلاحيات [0 خصم]")

                # 7. تركز المحافظ الكبرى
                if top_holders_percent > 50:
                    score -= 10
                    checks.append(f"❌ تركز عالي للمحافظ الكبرى: `{top_holders_percent:.1f}%` [-10 نقاط]")
                else:
                    checks.append(f"✅ تركز المحافظ الكبرى مقبول: `{top_holders_percent:.1f}%` [0 خصم]")

                score = max(0, score)

                # النتيجة النهائية
                if score >= 80:
                    verdict = "🟢 عملة نظيفة وآمنة للغاية"
                elif score >= 50:
                    verdict = "🟡 عملة متوسطة الخطورة (تتطلب حذراً)"
                else:
                    verdict = "🔴 عملة خطيرة جداً / محتالة (تجنبها)"

                checks_text = "\n".join([f"- {item}" for item in checks])

                report = (
                    f"📊 **التقرير الأمني والسيولة الشامل (MARSOF AI):**\n"
                    f"📌 العقد: `{contract[:10]}...{contract[-6:]}`\n\n"
                    f"🎯 **النتيجة النهائية:** {verdict}\n"
                    f"📈 **مؤشر الأمان الحقيقي:** `{score}/100`\n\n"
                    f"🔢 **الأرقام والنسب الفعلية:**\n"
                    f"- ضريبة الشراء: `{buy_tax}%` | ضريبة البيع: `{sell_tax}%`\n"
                    f"- إجمالي الحاملين: `{holder_count}`\n"
                    f"- نسبة تركز أعلى 10 محافظ: `{top_holders_percent:.1f}%`\n\n"
                    f"🔍 **تفاصيل فحص معايير السلامة:**\n"
                    f"{checks_text}\n\n"
                    f"🔒 *تم جلب وتحليل هذه البيانات مباشرة وحصرياً من السيرفر.*"
                )

                bot.edit_message_text(report, chat_id=chat_id, message_id=wait_msg.message_id, parse_mode="Markdown")
                
        return {"ok": True}
    except Exception as e:
        print(f"خطأ داخلي: {str(e)}")
        return {"ok": False, "error": str(e)}
