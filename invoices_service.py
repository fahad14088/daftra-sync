# invoices_service.py - الحل النهائي لجلب التفاصيل

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

def test_supabase_write():
    """اختبار الكتابة"""
    logger.info("🧪 اختبار الكتابة في Supabase...")
    
    test_uuid = str(uuid.uuid4())
    test_data = {
        "id": test_uuid,
        "invoice_no": "TEST-001",
        "total": 100.0
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

def get_invoices_simple():
    """جلب الفواتير"""
    logger.info("📥 جلب الفواتير...")
    
    headers = {"apikey": DAFTRA_APIKEY}
    invoices = []
    
    # جلب صفحة واحدة فقط
    try:
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page=1&limit=5"
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            page_invoices = data.get("data", [])
            logger.info(f"📊 وجدت {len(page_invoices)} فاتورة")
            invoices.extend(page_invoices)
        else:
            logger.error(f"❌ خطأ في جلب الفواتير: {response.text}")
            
    except Exception as e:
        logger.error(f"❌ خطأ في جلب الفواتير: {e}")
    
    return invoices

def get_invoice_detail_enhanced(invoice_id):
    """جلب تفاصيل الفاتورة بطريقة محسنة"""
    logger.info(f"🔍 جلب تفاصيل الفاتورة: {invoice_id}")
    
    headers = {"apikey": DAFTRA_APIKEY}
    
    # جرب عدة فروع وطرق مختلفة
    branches = [1, 2, 3, 4, 5]
    
    for branch in branches:
        try:
            # الطريقة الأساسية
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{invoice_id}"
            logger.info(f"🔍 جربة الفرع {branch}: {url}")
            
            response = requests.get(url, headers=headers, timeout=30)
            logger.info(f"📤 رد الفرع {branch}: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                logger.info(f"📊 بيانات الفرع {branch}: {bool(data.get('data'))}")
                
                # طباعة عينة من البيانات للتشخيص
                if data.get('data'):
                    logger.info(f"🔍 مفاتيح البيانات: {list(data['data'].keys())}")
                
                invoice = data.get("data", {}).get("Invoice", {})
                
                if invoice and invoice.get("id"):
                    logger.info(f"✅ وجدت تفاصيل الفاتورة {invoice_id} في الفرع {branch}")
                    logger.info(f"📋 بيانات الفاتورة: رقم={invoice.get('no')}, المجموع={invoice.get('total')}")
                    return invoice
                
                # جرب مفاتيح أخرى إذا لم تجد "Invoice"
                if data.get('data'):
                    # ربما البيانات مباشرة بدون مفتاح "Invoice"
                    direct_data = data.get('data')
                    if isinstance(direct_data, dict) and direct_data.get('id'):
                        logger.info(f"✅ وجدت البيانات مباشرة للفاتورة {invoice_id}")
                        return direct_data
            
            elif response.status_code == 404:
                logger.info(f"ℹ️ الفاتورة {invoice_id} غير موجودة في الفرع {branch}")
            else:
                logger.warning(f"⚠️ خطأ في الفرع {branch}: {response.status_code} - {response.text[:100]}")
                
        except Exception as e:
            logger.warning(f"⚠️ خطأ في الفرع {branch}: {e}")
            continue
    
    # جرب طرق أخرى
    logger.info(f"🔄 جربة طرق بديلة للفاتورة {invoice_id}")
    
    # الطريقة البديلة - بدون فرع محدد
    try:
        alt_url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{invoice_id}"
        response = requests.get(alt_url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            data = response.json()
            invoice = data.get("data", {})
            if invoice and invoice.get("id"):
                logger.info(f"✅ وجدت الفاتورة {invoice_id} بالطريقة البديلة")
                return invoice
    except:
        pass
    
    logger.warning(f"❌ لم أجد تفاصيل الفاتورة {invoice_id} في أي فرع")
    return None

def save_invoice_simple(invoice):
    """حفظ الفاتورة"""
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        invoice_id = str(invoice["id"])
        proper_uuid = generate_uuid_from_number(invoice_id)
        
        data = {
            "id": proper_uuid,
            "invoice_no": str(invoice.get("no", "")),
            "total": float(invoice.get("total", 0)),
            "daftra_invoice_id": invoice_id
        }
        
        if invoice.get("date"):
            data["invoice_date"] = invoice["date"]
        
        logger.info(f"💾 حفظ الفاتورة: {invoice_id}")
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=data,
            timeout=30
        )
        
        if response.status_code in [200, 201, 409]:
            logger.info(f"✅ تم حفظ الفاتورة {invoice_id}")
            return proper_uuid
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفاتورة: {e}")
        return None

def save_items_simple(invoice_uuid, invoice_id, items):
    """حفظ البنود"""
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
            
            item_id = str(item.get("id", ""))
            if item_id:
                item_uuid = generate_uuid_from_number(f"item-{item_id}")
            else:
                item_uuid = str(uuid.uuid4())
            
            data = {
                "id": item_uuid,
                "invoice_id": invoice_uuid,
                "quantity": qty,
                "unit_price": float(item.get("unit_price", 0)),
                "daftra_item_id": item_id,
                "daftra_invoice_id": invoice_id
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
    """الدالة الرئيسية"""
    logger.info("🚀 بدء المزامنة المحسنة...")
    
    result = {"invoices": 0, "items": 0, "errors": []}
    
    try:
        # اختبار الكتابة
        if not test_supabase_write():
            result["errors"].append("فشل اختبار الكتابة")
            return result
        
        # جلب الفواتير
        invoices = get_invoices_simple()
        
        if not invoices:
            result["errors"].append("لا توجد فواتير")
            return result
        
        logger.info(f"📋 معالجة {len(invoices)} فاتورة...")
        
        # معالجة كل فاتورة
        for i, inv_summary in enumerate(invoices, 1):
            try:
                invoice_id = str(inv_summary["id"])
                logger.info(f"🔄 {i}/{len(invoices)}: معالجة الفاتورة {invoice_id}")
                
                # جلب التفاصيل بالطريقة المحسنة
                details = get_invoice_detail_enhanced(invoice_id)
                
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
                    else:
                        logger.info(f"ℹ️ لا توجد بنود للفاتورة {invoice_id}")
                
                time.sleep(2)  # استراحة أطول
                
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
