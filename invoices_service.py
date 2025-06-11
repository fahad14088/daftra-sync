# invoices_service.py - الحل المُصحح لمشكلة UUID

import os
import requests
import time
import uuid
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# المتغيرات
DAFTRA_URL = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def test_supabase_write():
    """اختبار الكتابة مع UUID صحيح"""
    logger.info("🧪 اختبار الكتابة في Supabase...")
    
    # إنشاء UUID صحيح
    test_uuid = str(uuid.uuid4())
    
    test_data = {
        "id": test_uuid,  # UUID صحيح
        "invoice_no": "TEST-001",
        "total": 100.0,
        "created_at": datetime.now().isoformat()
    }
    
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        logger.info(f"🧪 اختبار UUID: {test_uuid}")
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=test_data,
            timeout=30
        )
        
        logger.info(f"🧪 نتيجة الاختبار: {response.status_code}")
        
        if response.status_code in [200, 201]:
            logger.info("✅ اختبار الكتابة نجح!")
            
            # احذف البيانات التجريبية
            requests.delete(
                f"{SUPABASE_URL}/rest/v1/invoices?id=eq.{test_uuid}",
                headers=headers,
                timeout=10
            )
            return True
        else:
            logger.error(f"❌ فشل اختبار الكتابة: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في اختبار الكتابة: {e}")
        return False

def generate_uuid_from_number(number):
    """تحويل رقم إلى UUID صحيح"""
    # استخدم الرقم كـ seed لإنتاج UUID ثابت
    import hashlib
    hash_input = f"invoice-{number}".encode('utf-8')
    hash_digest = hashlib.md5(hash_input).hexdigest()
    
    # تحويل إلى UUID format
    uuid_str = f"{hash_digest[:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}-{hash_digest[16:20]}-{hash_digest[20:32]}"
    return uuid_str

def get_invoices_simple():
    """جلب الفواتير"""
    logger.info("📥 جلب الفواتير...")
    
    headers = {"apikey": DAFTRA_APIKEY}
    invoices = []
    
    # جلب صفحتين فقط للاختبار
    for page in range(1, 3):
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit=10"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                page_invoices = data.get("data", [])
                logger.info(f"📊 الصفحة {page}: {len(page_invoices)} فاتورة")
                
                if not page_invoices:
                    break
                    
                invoices.extend(page_invoices)
                time.sleep(1)
            else:
                logger.error(f"❌ خطأ في الصفحة {page}: {response.text}")
                break
                
        except Exception as e:
            logger.error(f"❌ خطأ في جلب الصفحة {page}: {e}")
            break
    
    logger.info(f"📋 إجمالي الفواتير: {len(invoices)}")
    return invoices

def get_invoice_detail_simple(invoice_id):
    """جلب تفاصيل الفاتورة"""
    headers = {"apikey": DAFTRA_APIKEY}
    
    for branch in [1, 2, 3]:
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{invoice_id}"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                invoice = data.get("data", {}).get("Invoice", {})
                if invoice:
                    return invoice
        except:
            continue
    
    return None

def save_invoice_simple(invoice):
    """حفظ الفاتورة مع UUID صحيح"""
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        # إنشاء UUID صحيح من رقم الفاتورة
        invoice_id = str(invoice["id"])
        proper_uuid = generate_uuid_from_number(invoice_id)
        
        # بيانات مع UUID صحيح
        data = {
            "id": proper_uuid,  # UUID صحيح
            "invoice_no": str(invoice.get("no", "")),
            "total": float(invoice.get("total", 0)),
            "daftra_invoice_id": invoice_id  # احتفظ بالرقم الأصلي
        }
        
        # أضف التاريخ إذا كان موجود
        if invoice.get("date"):
            data["invoice_date"] = invoice["date"]
        
        logger.info(f"💾 حفظ الفاتورة: {invoice_id} -> UUID: {proper_uuid}")
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=data,
            timeout=30
        )
        
        logger.info(f"📤 رد الحفظ: {response.status_code}")
        
        if response.status_code in [200, 201, 409]:
            logger.info(f"✅ تم حفظ الفاتورة {invoice_id}")
            return proper_uuid  # أرجع UUID للاستخدام مع البنود
        else:
            logger.error(f"❌ فشل الحفظ: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ خطأ في الحفظ: {e}")
        return None

def save_items_simple(invoice_uuid, invoice_id, items):
    """حفظ البنود مع UUID صحيح"""
    if not items:
        return 0
    
    if not isinstance(items, list):
        items = [items]
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    
    saved = 0
    
    for item in items:
        try:
            qty = float(item.get("quantity", 0))
            if qty <= 0:
                continue
            
            # إنشاء UUID صحيح للبند
            item_id = str(item.get("id", ""))
            if item_id:
                item_uuid = generate_uuid_from_number(f"item-{item_id}")
            else:
                item_uuid = str(uuid.uuid4())
            
            data = {
                "id": item_uuid,  # UUID صحيح
                "invoice_id": invoice_uuid,  # UUID الفاتورة
                "quantity": qty,
                "unit_price": float(item.get("unit_price", 0)),
                "daftra_item_id": item_id,  # الرقم الأصلي
                "daftra_invoice_id": invoice_id  # رقم الفاتورة الأصلي
            }
            
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items",
                headers=headers,
                json=data,
                timeout=30
            )
            
            if response.status_code in [200, 201, 409]:
                saved += 1
            else:
                logger.error(f"❌ فشل حفظ البند: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ خطأ في البند: {e}")
    
    return saved

def sync_invoices():
    """الدالة الرئيسية - مع UUID صحيح"""
    logger.info("🚀 بدء المزامنة مع UUID صحيح...")
    
    result = {"invoices": 0, "items": 0, "errors": []}
    
    try:
        # اختبار الكتابة أولاً
        if not test_supabase_write():
            result["errors"].append("فشل اختبار الكتابة في Supabase")
            return result
        
        # جلب الفواتير
        invoices = get_invoices_simple()
        
        if not invoices:
            result["errors"].append("لا توجد فواتير")
            return result
        
        logger.info(f"📋 معالجة {len(invoices)} فاتورة...")
        
        # معالجة كل فاتورة
        for i, inv_summary in enumerate(invoices[:5], 1):  # أول 5 فواتير
            try:
                invoice_id = str(inv_summary["id"])
                logger.info(f"🔄 {i}/5: معالجة الفاتورة {invoice_id}")
                
                # جلب التفاصيل
                details = get_invoice_detail_simple(invoice_id)
                
                if not details:
                    logger.warning(f"⚠️ لم أجد تفاصيل الفاتورة {invoice_id}")
                    continue
                
                # حفظ الفاتورة
                invoice_uuid = save_invoice_simple(details)
                
                if invoice_uuid:
                    result["invoices"] += 1
                    
                    # حفظ البنود
                    items = details.get("invoice_item", [])
                    if items:
                        saved_items = save_items_simple(invoice_uuid, invoice_id, items)
                        result["items"] += saved_items
                        logger.info(f"✅ تم حفظ {saved_items} بند")
                
                time.sleep(1)
                
            except Exception as e:
                error_msg = f"خطأ في الفاتورة {inv_summary.get('id')}: {e}"
                result["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        # النتائج
        logger.info("=" * 50)
        logger.info(f"🎯 النتائج:")
        logger.info(f"✅ فواتير: {result['invoices']}")
        logger.info(f"📦 بنود: {result['items']}")
        logger.info(f"❌ أخطاء: {len(result['errors'])}")
        logger.info("=" * 50)
        
        return result
        
    except Exception as e:
        error_msg = f"خطأ عام: {e}"
        result["errors"].append(error_msg)
        logger.error(f"💥 {error_msg}")
        return result

if __name__ == "__main__":
    sync_invoices()
