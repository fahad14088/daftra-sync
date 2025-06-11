# invoices_service.py - الحل النهائي المضمون

import os
import requests
import time
import uuid
import logging
import json
from datetime import datetime

# تسجيل بسيط وواضح
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# المتغيرات
DAFTRA_URL = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def test_supabase_write():
    """اختبار الكتابة في Supabase"""
    logger.info("🧪 اختبار الكتابة في Supabase...")
    
    test_data = {
        "id": "TEST-" + str(int(time.time())),
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
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=test_data,
            timeout=30
        )
        
        logger.info(f"🧪 نتيجة الاختبار: {response.status_code}")
        logger.info(f"🧪 الرد: {response.text}")
        
        if response.status_code in [200, 201]:
            logger.info("✅ اختبار الكتابة نجح!")
            
            # احذف البيانات التجريبية
            requests.delete(
                f"{SUPABASE_URL}/rest/v1/invoices?id=eq.{test_data['id']}",
                headers=headers
            )
            return True
        else:
            logger.error(f"❌ فشل اختبار الكتابة: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في اختبار الكتابة: {e}")
        return False

def get_invoices_simple():
    """جلب الفواتير بطريقة بسيطة"""
    logger.info("📥 جلب الفواتير...")
    
    headers = {"apikey": DAFTRA_APIKEY}
    invoices = []
    
    # جلب أول 5 صفحات فقط للاختبار
    for page in range(1, 6):
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit=20"
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
    """جلب تفاصيل الفاتورة بطريقة بسيطة"""
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
    """حفظ الفاتورة بطريقة بسيطة ومضمونة"""
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        # بيانات بسيطة ومضمونة
        data = {
            "id": str(invoice["id"]),
            "invoice_no": str(invoice.get("no", "")),
            "total": float(invoice.get("total", 0))
        }
        
        # أضف التاريخ إذا كان موجود
        if invoice.get("date"):
            data["invoice_date"] = invoice["date"]
        
        logger.info(f"💾 حفظ الفاتورة: {data['id']}")
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=data,
            timeout=30
        )
        
        logger.info(f"📤 رد الحفظ: {response.status_code} - {response.text}")
        
        if response.status_code in [200, 201, 409]:  # 409 = موجود
            return True
        else:
            logger.error(f"❌ فشل الحفظ: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في الحفظ: {e}")
        return False

def save_items_simple(invoice_id, items):
    """حفظ البنود بطريقة بسيطة"""
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
            
            data = {
                "id": str(item.get("id", str(uuid.uuid4()))),
                "invoice_id": str(invoice_id),
                "quantity": qty,
                "unit_price": float(item.get("unit_price", 0))
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
    """الدالة الرئيسية - مبسطة ومضمونة"""
    logger.info("🚀 بدء المزامنة المبسطة...")
    
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
        for i, inv_summary in enumerate(invoices[:10], 1):  # أول 10 فواتير للاختبار
            try:
                invoice_id = str(inv_summary["id"])
                logger.info(f"🔄 {i}/10: معالجة الفاتورة {invoice_id}")
                
                # جلب التفاصيل
                details = get_invoice_detail_simple(invoice_id)
                
                if not details:
                    logger.warning(f"⚠️ لم أجد تفاصيل الفاتورة {invoice_id}")
                    continue
                
                # حفظ الفاتورة
                if save_invoice_simple(details):
                    result["invoices"] += 1
                    logger.info(f"✅ تم حفظ الفاتورة {invoice_id}")
                    
                    # حفظ البنود
                    items = details.get("invoice_item", [])
                    if items:
                        saved_items = save_items_simple(invoice_id, items)
                        result["items"] += saved_items
                        logger.info(f"✅ تم حفظ {saved_items} بند")
                
                time.sleep(1)  # استراحة
                
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
