# invoices_service.py - الحل النهائي بدون أخطاء

import os
import requests
import time
import uuid
import logging
from datetime import datetime
import hashlib
import traceback

# تسجيل مبسط
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# المتغيرات
DAFTRA_URL = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number):
    """تحويل رقم إلى UUID"""
    hash_input = f"invoice-{number}".encode('utf-8')
    hash_digest = hashlib.md5(hash_input).hexdigest()
    return f"{hash_digest[:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}-{hash_digest[16:20]}-{hash_digest[20:32]}"

def test_supabase_connection():
    """اختبار اتصال Supabase بشكل مفصل"""
    logger.info("🧪 اختبار اتصال Supabase...")
    
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        # اختبار قراءة
        logger.info("📖 اختبار القراءة...")
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/invoices?select=count",
            headers=headers,
            timeout=30
        )
        logger.info(f"📖 نتيجة القراءة: {response.status_code}")
        
        if response.status_code not in [200, 406]:
            logger.error(f"❌ فشل القراءة: {response.text}")
            return False
        
        # اختبار كتابة
        logger.info("✍️ اختبار الكتابة...")
        test_uuid = str(uuid.uuid4())
        test_data = {
            "id": test_uuid,
            "invoice_no": "TEST-CONNECTION",
            "total": 999.99
        }
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=test_data,
            timeout=30
        )
        
        logger.info(f"✍️ نتيجة الكتابة: {response.status_code}")
        logger.info(f"✍️ رد الخادم: {response.text}")
        
        if response.status_code in [200, 201]:
            logger.info("✅ نجح اختبار الكتابة!")
            
            # حذف البيانات التجريبية
            delete_response = requests.delete(
                f"{SUPABASE_URL}/rest/v1/invoices?id=eq.{test_uuid}",
                headers=headers,
                timeout=10
            )
            logger.info(f"🗑️ حذف البيانات التجريبية: {delete_response.status_code}")
            
            return True
        else:
            logger.error(f"❌ فشل اختبار الكتابة: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في اختبار Supabase: {e}")
        logger.error(f"📋 التفاصيل: {traceback.format_exc()}")
        return False

def safe_save_invoice(invoice_data):
    """حفظ آمن للفاتورة مع معالجة شاملة للأخطاء"""
    try:
        invoice_id = str(invoice_data["id"])
        invoice_uuid = generate_uuid_from_number(invoice_id)
        
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        # بيانات بسيطة ومضمونة
        payload = {
            "id": invoice_uuid,
            "invoice_no": str(invoice_data.get("no", "")).strip(),
            "total": float(invoice_data.get("total", 0))
        }
        
        # إضافة البيانات الاختيارية بحذر
        try:
            if invoice_data.get("date"):
                payload["invoice_date"] = str(invoice_data["date"]).strip()
        except:
            pass
        
        try:
            if invoice_data.get("client_business_name"):
                name = str(invoice_data["client_business_name"]).strip()
                if name and name != "None":
                    payload["client_business_name"] = name[:255]
        except:
            pass
        
        try:
            if invoice_data.get("customer_id"):
                customer_id = str(invoice_data["customer_id"]).strip()
                if customer_id and customer_id != "None":
                    payload["customer_id"] = customer_id
        except:
            pass
        
        # تنظيف نهائي
        clean_payload = {}
        for key, value in payload.items():
            if value is not None and str(value).strip() not in ["", "None", "null"]:
                clean_payload[key] = value
        
        logger.debug(f"💾 حفظ الفاتورة {invoice_id}: {clean_payload}")
        
        # محاولة الحفظ
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=clean_payload,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            logger.info(f"✅ نجح حفظ الفاتورة {invoice_id}")
            return invoice_uuid
        elif response.status_code == 409:
            logger.debug(f"ℹ️ الفاتورة {invoice_id} موجودة مسبقاً")
            return invoice_uuid
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}")
            logger.error(f"📤 الحالة: {response.status_code}")
            logger.error(f"📝 الرد: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفاتورة {invoice_data.get('id', 'unknown')}")
        logger.error(f"📋 الخطأ: {e}")
        logger.error(f"📋 التفاصيل: {traceback.format_exc()}")
        return None

def sync_invoices():
    """المزامنة المحسنة مع معالجة أخطاء شاملة"""
    logger.info("🚀 بدء المزامنة المحسنة...")
    
    result = {"invoices": 0, "items": 0, "errors": []}
    
    try:
        # اختبار الاتصال أولاً
        if not test_supabase_connection():
            logger.error("❌ فشل اختبار اتصال Supabase!")
            result["errors"].append("فشل اتصال Supabase")
            return result
        
        logger.info("✅ اتصال Supabase يعمل بشكل صحيح")
        
        # جلب الفواتير
        logger.info("📥 جلب الفواتير من دفترة...")
        
        headers_daftra = {"apikey": DAFTRA_APIKEY}
        all_invoices = []
        
        # جلب أول 3 صفحات للاختبار
        for page in range(1, 4):
            try:
                url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit=20"
                logger.info(f"📄 جلب الصفحة {page}")
                
                response = requests.get(url, headers=headers_daftra, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    invoices = data.get("data", [])
                    
                    if not invoices:
                        logger.info(f"✅ انتهت الفواتير في الصفحة {page}")
                        break
                    
                    logger.info(f"📊 الصفحة {page}: {len(invoices)} فاتورة")
                    all_invoices.extend(invoices)
                else:
                    logger.error(f"❌ خطأ في جلب الصفحة {page}: {response.text}")
                    break
                
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ خطأ في معالجة الصفحة {page}: {e}")
                break
        
        if not all_invoices:
            logger.warning("⚠️ لم يتم جلب أي فواتير!")
            result["errors"].append("لا توجد فواتير")
            return result
        
        logger.info(f"📋 سيتم معالجة {len(all_invoices)} فاتورة...")
        
        # معالجة الفواتير
        success_count = 0
        
        for i, invoice in enumerate(all_invoices, 1):
            try:
                invoice_id = str(invoice["id"])
                
                if i % 10 == 0:
                    logger.info(f"🔄 معالجة {i}/{len(all_invoices)}: الفاتورة {invoice_id}")
                
                # محاولة حفظ الفاتورة
                invoice_uuid = safe_save_invoice(invoice)
                
                if invoice_uuid:
                    success_count += 1
                    result["invoices"] += 1
                else:
                    result["errors"].append(f"فشل حفظ الفاتورة {invoice_id}")
                
                # استراحة بين الفواتير
                time.sleep(0.5)
                
            except Exception as e:
                error_msg = f"خطأ عام في الفاتورة {invoice.get('id', 'unknown')}: {e}"
                result["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        # النتائج النهائية
        logger.info("=" * 70)
        logger.info("🎯 النتائج النهائية:")
        logger.info(f"📊 إجمالي الفواتير: {len(all_invoices)}")
        logger.info(f"✅ فواتير محفوظة بنجاح: {result['invoices']}")
        logger.info(f"📦 بنود محفوظة: {result['items']}")
        logger.info(f"❌ عدد الأخطاء: {len(result['errors'])}")
        
        if len(all_invoices) > 0:
            success_rate = (result['invoices'] / len(all_invoices)) * 100
            logger.info(f"🏆 معدل النجاح: {success_rate:.1f}%")
        
        if result['errors']:
            logger.error("🚨 عينة من الأخطاء:")
            for error in result['errors'][:5]:
                logger.error(f"  - {error}")
        
        if result['invoices'] > 0:
            logger.info("🎉 تمت المزامنة بنجاح!")
        else:
            logger.warning("⚠️ لم يتم حفظ أي فواتير!")
        
        logger.info("=" * 70)
        
        return result
        
    except Exception as e:
        error_msg = f"خطأ عام في المزامنة: {e}"
        result["errors"].append(error_msg)
        logger.error(f"💥 {error_msg}")
        logger.error(f"📋 التفاصيل: {traceback.format_exc()}")
        return result

if __name__ == "__main__":
    sync_invoices()
