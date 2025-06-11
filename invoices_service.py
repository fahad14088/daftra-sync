# invoices_service.py - الحل النهائي بدون أعمدة إضافية

import os
import requests
import time
import uuid
import logging
from datetime import datetime
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# المتغيرات
DAFTRA_URL = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number):
    """تحويل رقم إلى UUID صحيح"""
    hash_input = f"invoice-{number}".encode('utf-8')
    hash_digest = hashlib.md5(hash_input).hexdigest()
    uuid_str = f"{hash_digest[:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}-{hash_digest[16:20]}-{hash_digest[20:32]}"
    return uuid_str

def save_invoice_basic(invoice_summary):
    """حفظ الفاتورة - الأعمدة الأساسية فقط"""
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        invoice_id = str(invoice_summary["id"])
        proper_uuid = generate_uuid_from_number(invoice_id)
        
        # البيانات الأساسية فقط - الأعمدة الموجودة في Supabase
        data = {
            "id": proper_uuid,
            "invoice_no": str(invoice_summary.get("no", "")),
            "total": float(invoice_summary.get("total", 0))
        }
        
        # أضف التاريخ إذا كان موجود والعمود موجود
        if invoice_summary.get("date"):
            data["invoice_date"] = invoice_summary["date"]
        
        # أضف اسم العميل إذا كان موجود والعمود موجود
        if invoice_summary.get("client_business_name"):
            data["client_business_name"] = str(invoice_summary["client_business_name"])[:255]
        
        # أضف معرف العميل إذا كان موجود والعمود موجود
        if invoice_summary.get("customer_id"):
            data["customer_id"] = str(invoice_summary["customer_id"])
        
        # إزالة القيم الفارغة
        data = {k: v for k, v in data.items() if v not in [None, "", "None", 0]}
        
        logger.info(f"💾 حفظ الفاتورة {invoice_id} (UUID: {proper_uuid[:8]}...)")
        logger.info(f"📊 البيانات: {data}")
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=data,
            timeout=30
        )
        
        logger.info(f"📤 رد الحفظ: {response.status_code}")
        
        if response.status_code in [200, 201]:
            logger.info(f"✅ تم حفظ الفاتورة {invoice_id}")
            return True
        elif response.status_code == 409:
            logger.info(f"ℹ️ الفاتورة {invoice_id} موجودة مسبقاً")
            return True
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفاتورة {invoice_summary.get('id')}: {e}")
        return False

def sync_invoices():
    """الحل البسيط والمضمون"""
    logger.info("🚀 بدء المزامنة النهائية...")
    
    result = {"invoices": 0, "items": 0, "errors": []}
    
    try:
        # اختبار بسيط لـ Supabase
        logger.info("🧪 اختبار Supabase...")
        test_uuid = str(uuid.uuid4())
        test_data = {
            "id": test_uuid,
            "invoice_no": "TEST-001",
            "total": 1.0
        }
        
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        test_response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=test_data,
            timeout=30
        )
        
        if test_response.status_code not in [200, 201]:
            logger.error(f"❌ فشل اختبار Supabase: {test_response.text}")
            result["errors"].append("فشل اختبار Supabase")
            return result
        
        # احذف البيانات التجريبية
        requests.delete(f"{SUPABASE_URL}/rest/v1/invoices?id=eq.{test_uuid}", headers=headers)
        logger.info("✅ اختبار Supabase نجح")
        
        # جلب الفواتير
        logger.info("📥 جلب الفواتير...")
        daftra_headers = {"apikey": DAFTRA_APIKEY}
        
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page=1&limit=5"
        response = requests.get(url, headers=daftra_headers, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"❌ فشل جلب الفواتير: {response.text}")
            result["errors"].append("فشل جلب الفواتير")
            return result
        
        data = response.json()
        invoices = data.get("data", [])
        
        logger.info(f"📋 وجدت {len(invoices)} فاتورة")
        
        if not invoices:
            result["errors"].append("لا توجد فواتير")
            return result
        
        # حفظ كل فاتورة
        success_count = 0
        for i, invoice in enumerate(invoices, 1):
            try:
                invoice_id = str(invoice["id"])
                logger.info(f"🔄 {i}/{len(invoices)}: معالجة الفاتورة {invoice_id}")
                
                if save_invoice_basic(invoice):
                    success_count += 1
                    result["invoices"] += 1
                    logger.info(f"✅ نجح حفظ الفاتورة {invoice_id}")
                else:
                    result["errors"].append(f"فشل حفظ الفاتورة {invoice_id}")
                    logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}")
                
                time.sleep(1)  # استراحة
                
            except Exception as e:
                error_msg = f"خطأ في الفاتورة {invoice.get('id')}: {e}"
                result["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        # النتائج النهائية
        logger.info("=" * 60)
        logger.info("🎯 النتائج النهائية:")
        logger.info(f"✅ فواتير محفوظة بنجاح: {result['invoices']}")
        logger.info(f"📦 بنود: {result['items']} (لم يتم جلب البنود)")
        logger.info(f"❌ عدد الأخطاء: {len(result['errors'])}")
        logger.info(f"📊 معدل النجاح: {success_count}/{len(invoices)}")
        
        if result['errors']:
            logger.error("🚨 عينة من الأخطاء:")
            for error in result['errors'][:3]:
                logger.error(f"  - {error}")
        
        if result['invoices'] > 0:
            logger.info("🎉 تمت المزامنة بنجاح!")
        else:
            logger.warning("⚠️ لم يتم حفظ أي فواتير!")
        
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        error_msg = f"خطأ عام: {e}"
        result["errors"].append(error_msg)
        logger.error(f"💥 {error_msg}")
        return result

if __name__ == "__main__":
    sync_invoices()
