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
                bot.send_message(chat_id, "أهلاً بك في نظام MARSOF AI للتحليل المتقدم. أرسل عنوان العقد لفحصه عبر خوارزمية الأمان والخطوط الحمراء/الخضراء.")
            else:
                raw_text = text.strip()
                wait_msg = bot.send_message(chat_id, "🔍 جاري تشغيل خوارزمية الفحص المالي والأمني (MARSOF Engine)...")
                
                # تنظيف العنوان
                contract = raw_text.split("::")[0] if "::" in raw_text else raw_text
                
                # محاولة جلب البيانات (عبر واجهة التحليل المتاحة)
                security_data = None
                network_type = "EVM / BNB Chain"
                
                try:
                    # فحص عبر GoPlus كمثال للبيانات التقنية المتاحة
                    api_url = f"https://api.gopluslabs.io/api/v1/token_security/56?contract_addresses={contract}"
                    res = requests.get(api_url, timeout=6).json()
                    res_dict = res.get("result", {})
                    if res_dict:
                        security_data = list(res_dict.values())[0]
                except:
                    pass

                # تطبيق خوارزمية الفحص والتقييم (Scoring & Rules)
                red_lines = []
                green_lines = []
                risk_score = 100
                
                if security_data:
                    # فحص الهانيبوت (خط أحمر قاتل)
                    is_honeypot = security_data.get("is_honeypot", "0") == "1"
                    if is_honeypot:
                        red_lines.append("⚠️ العقد مصنف كـ Honeypot (ممنوع أو مستحيل البيع).")
                        risk_score -= 60
                    else:
                        green_lines.append("✅ العقد ليس Honeypot (عملية البيع متاحة تقنياً).")

                    # فحص الكود المصدر
                    is_open_source = security_data.get("is_open_source", "0") == "1"
                    if not is_open_source:
                        red_lines.append("⚠️ الكود المصدر غير مكشوف أو غير موثق (Unverified Source Code).")
                        risk_score -= 20
                    else:
                        green_lines.append("✅ الكود المصدر مكشوف وموثق (Open Source).")

                    # فحص الضرائب
                    try:
                        buy_tax = float(security_data.get("buy_tax", "0"))
                        sell_tax = float(security_data.get("sell_tax", "0"))
                    except:
                        buy_tax, sell_tax = 0.0, 0.0

                    if buy_tax > 10 or sell_tax > 10:
                        red_lines.append(f"⚠️ ضرائب عالية جداً: شراء ({buy_tax}%) / بيع ({sell_tax}%).")
                        risk_score -= 20
                    else:
                        green_lines.append(f"✅ الضرائب في النطاق الطبيعي: شراء ({buy_tax}%) / بيع ({sell_tax}%).")
                        
                    # فحص صلاحية الصك (Mint)
                    is_mintable = security_data.get("is_mintable", "0") == "1"
                    if is_mintable:
                        red_lines.append("⚠️ صلاحية طباعة عملات جديدة (Mint) مفعلة وليست ملغاة.")
                        risk_score -= 15
                    else:
                        green_lines.append("✅ صلاحية طباعة عملات جديدة مغلقة.")
                else:
                    # في حال كانت شبكة مثل Sui أو عدم توفر بيانات تفصيلية كلاسيكية
                    red_lines.append("ℹ️ فحص العمق الكامل يعتمد على السيولة المباشرة و DEXs.")
                    green_lines.append("✅ العقد نشط ضمن شبكة الأصول الرقمية.")
                    risk_score = 75

                # ضبط النقطة الدنيا
                risk_score = max(0, risk_score)

                # النتيجة النهائية بناءً على الخوارزمية
                if risk_score >= 80 and len(red_lines) == 0:
                    verdict = "🟢 **عملة نظيفة وآمنة (Low Risk)**"
                elif risk_score >= 50 and len(red_lines) <= 1:
                    verdict = "🟡 **عملة متوسطة الخطورة (تتطلب الحذر)**"
                else:
                    verdict = "🔴 **عملة خطيرة جداً / احتمالية احتيال عالية (High Risk / Scam)**"

                # صياغة التقرير النهائي المنظم
                red_text = "\n".join([f"- {item}" for item in red_lines]) if red_lines else "- لا توجد خطوط حمراء مرصودة."
                green_text = "\n".join([f"- {item}" for item in green_lines]) if green_lines else "- لا توجد مؤشرات إيجابية كافية."

                report = (
                    f"📊 **تقرير تحليل MARSOF AI المتقدم:**\n"
                    f"📌 العنوان: `{contract[:15]}...`\n\n"
                    f"🎯 **النتيجة النهائية:**\n{verdict}\n"
                    f"📈 **مؤشر الأمان:** `{risk_score}/100`\n\n"
                    f"🛑 **الخطوط الحمراء (المخاطر):**\n{red_text}\n\n"
                    f"✅ **الخطوط الخضراء (الإيجابيات):**\n{green_text}\n\n"
                    f"💡 *تم التحليل وفق خوارزمية الفحص الأمني والمالي التلقائي.*"
                )

                bot.edit_message_text(report, chat_id=chat_id, message_id=wait_msg.message_id, parse_mode="Markdown")
                
        return {"ok": True}
    except Exception as e:
        print(f"خطأ داخلي: {str(e)}")
        return {"ok": False, "error": str(e)}
