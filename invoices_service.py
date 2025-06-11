# invoices_service.py - النسخة المُحسنة والمضمونة

import os
import requests
import time
import uuid
import logging
import json
from datetime import datetime
from typing import Dict, List, Optional

# إعداد التسجيل المحسن
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('invoices_sync.log')
    ]
)
logger = logging.getLogger(__name__)

# متغيرات البيئة
DAFTRA_URL = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Headers محسنة
HEADERS_DAFTRA = {
    "apikey": DAFTRA_APIKEY,
    "Accept": "application/json",
    "Content-Type": "application/json",
    "User-Agent": "InvoiceSync/1.0"
}

HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"  # تحسين الأداء
}

def safe_request(method, url, headers, data=None, timeout=30, retries=3):
    """طلب HTTP آمن مع إعادة محاولة"""
    for attempt in range(retries):
        try:
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=timeout)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=timeout)
            else:
                raise ValueError(f"طريقة HTTP غير مدعومة: {method}")
            
            # تسجيل تفصيلي للأخطاء
            if response.status_code >= 400:
                logger.error(f"❌ خطأ HTTP {response.status_code}")
                logger.error(f"URL: {url}")
                logger.error(f"Response: {response.text[:500]}")
                
                if response.status_code == 409:  # تضارب - البيانات موجودة
                    return {"success": True, "data": {}, "duplicate": True}
                elif response.status_code >= 500:  # خطأ خادم - أعد المحاولة
                    if attempt < retries - 1:
                        logger.warning(f"⏳ إعادة المحاولة {attempt + 1}/{retries}")
                        time.sleep((attempt + 1) * 2)
                        continue
                
                return {"success": False, "error": response.text, "status": response.status_code}
            
            return {"success": True, "data": response.json() if response.text else {}}
            
        except requests.exceptions.Timeout:
            logger.warning(f"⏰ انتهت المهلة الزمنية - المحاولة {attempt + 1}/{retries}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"🔌 خطأ اتصال - المحاولة {attempt + 1}/{retries}")
        except json.JSONDecodeError:
            logger.error(f"❌ خطأ في تحليل JSON: {response.text[:200]}")
            return {"success": False, "error": "Invalid JSON response"}
        except Exception as e:
            logger.error(f"❌ خطأ غير متوقع: {str(e)}")
        
        if attempt < retries - 1:
            time.sleep((attempt + 1) * 2)
    
    return {"success": False, "error": "Max retries exceeded"}

def test_connections():
    """اختبار شامل للاتصالات"""
    logger.info("🔍 اختبار الاتصالات...")
    
    # اختبار دفترة
    result = safe_request('GET', f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page=1&limit=1", HEADERS_DAFTRA)
    if not result["success"]:
        logger.error(f"❌ فشل الاتصال بدفترة: {result.get('error', 'خطأ غير معروف')}")
        return False
    logger.info("✅ اتصال دفترة يعمل")
    
    # اختبار Supabase
    result = safe_request('GET', f"{SUPABASE_URL}/rest/v1/", HEADERS_SUPABASE)
    if not result["success"] and result.get("status") not in [404, 406]:
        logger.error(f"❌ فشل الاتصال بـ Supabase: {result.get('error', 'خطأ غير معروف')}")
        return False
    logger.info("✅ اتصال Supabase يعمل")
    
    return True

def fetch_all_invoices():
    """جلب جميع الفواتير بطريقة محسنة"""
    logger.info("📥 بدء جلب الفواتير...")
    all_invoices = []
    page = 1
    consecutive_errors = 0
    
    while True:
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit=50"
        logger.info(f"🔍 جلب الصفحة {page}")
        
        result = safe_request('GET', url, HEADERS_DAFTRA)
        
        if not result["success"]:
            consecutive_errors += 1
            logger.error(f"❌ فشل جلب الصفحة {page}: {result.get('error', 'خطأ غير معروف')}")
            
            if consecutive_errors >= 3:
                logger.error("❌ فشل في 3 محاولات متتالية - توقف الجلب")
                break
            
            time.sleep(5)
            continue
        
        consecutive_errors = 0
        data = result["data"]
        invoices = data.get("data", [])
        
        logger.info(f"📊 الصفحة {page}: {len(invoices)} فاتورة")
        
        if not invoices:
            logger.info("✅ انتهت جميع الفواتير")
            break
        
        all_invoices.extend(invoices)
        page += 1
        time.sleep(1)  # استراحة بين الطلبات
    
    logger.info(f"📋 إجمالي الفواتير: {len(all_invoices)}")
    return all_invoices

def fetch_invoice_details(invoice_id):
    """جلب تفاصيل الفاتورة بطريقة محسنة"""
    logger.debug(f"🔍 جلب تفاصيل الفاتورة: {invoice_id}")
    
    for branch_id in [1, 2, 3, 4, 5]:
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch_id}/{invoice_id}"
        result = safe_request('GET', url, HEADERS_DAFTRA)
        
        if result["success"]:
            data = result["data"]
            invoice_data = data.get("data", {}).get("Invoice", {})
            
            if invoice_data and invoice_data.get("id"):
                logger.debug(f"✅ وجدت الفاتورة {invoice_id} في الفرع {branch_id}")
                return invoice_data
    
    logger.warning(f"⚠️ لم أجد تفاصيل الفاتورة: {invoice_id}")
    return None

def clean_data_for_supabase(data, field_types=None):
    """تنظيف البيانات لـ Supabase"""
    if not field_types:
        field_types = {}
    
    cleaned = {}
    for key, value in data.items():
        if value in [None, "", "None", "null", "undefined"]:
            continue
        
        # تنظيف النصوص
        if isinstance(value, str):
            value = value.strip()
            if not value:
                continue
        
        # تحويل الأرقام
        if key in field_types.get('numbers', []):
            try:
                value = float(value) if '.' in str(value) else int(value)
            except (ValueError, TypeError):
                logger.warning(f"⚠️ قيمة رقمية غير صالحة {key}: {value}")
                continue
        
        cleaned[key] = value
    
    return cleaned

def save_invoice_safely(invoice_data):
    """حفظ الفاتورة بطريقة آمنة"""
    try:
        invoice_id = str(invoice_data["id"])
        
        # تحضير البيانات المنظفة
        payload = clean_data_for_supabase({
            "id": invoice_id,
            "invoice_no": str(invoice_data.get("no", "")),
            "invoice_date": invoice_data.get("date"),
            "customer_id": str(invoice_data.get("customer_id", "")) if invoice_data.get("customer_id") else None,
            "client_business_name": str(invoice_data.get("client_business_name", ""))[:255] if invoice_data.get("client_business_name") else None,
            "total": invoice_data.get("total", 0),
            "created_at": datetime.now().isoformat()
        }, {
            'numbers': ['total']
        })
        
        if not payload:
            logger.error(f"❌ بيانات فارغة للفاتورة {invoice_id}")
            return False
        
        logger.debug(f"💾 حفظ الفاتورة: {json.dumps(payload, ensure_ascii=False, indent=2)}")
        
        result = safe_request('POST', f"{SUPABASE_URL}/rest/v1/invoices", HEADERS_SUPABASE, payload)
        
        if result["success"] or result.get("duplicate"):
            logger.info(f"✅ تم حفظ الفاتورة: {invoice_id}")
            return True
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}: {result.get('error', 'خطأ غير معروف')}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفاتورة {invoice_data.get('id', 'unknown')}: {str(e)}")
        return False

def save_invoice_items_safely(invoice_id, items):
    """حفظ بنود الفاتورة بطريقة آمنة"""
    if not items:
        return 0
    
    if not isinstance(items, list):
        items = [items] if items else []
    
    saved_count = 0
    
    for item in items:
        try:
            # تحضير البيانات
            item_id = str(item.get("id", str(uuid.uuid4())))
            quantity = item.get("quantity", 0)
            unit_price = item.get("unit_price", 0)
            
            # التحقق من الكمية
            try:
                quantity = float(quantity)
                unit_price = float(unit_price)
            except (ValueError, TypeError):
                logger.warning(f"⚠️ قيم غير صالحة للبند {item_id}")
                continue
            
            if quantity <= 0:
                continue
            
            payload = clean_data_for_supabase({
                "id": item_id,
                "invoice_id": str(invoice_id),
                "product_id": str(item.get("product_id", "")) if item.get("product_id") else None,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": quantity * unit_price,
                "created_at": datetime.now().isoformat()
            }, {
                'numbers': ['quantity', 'unit_price', 'total_price']
            })
            
            if not payload:
                continue
            
            logger.debug(f"💾 حفظ البند: {json.dumps(payload, ensure_ascii=False)}")
            
            result = safe_request('POST', f"{SUPABASE_URL}/rest/v1/invoice_items", HEADERS_SUPABASE, payload)
            
            if result["success"] or result.get("duplicate"):
                saved_count += 1
                logger.debug(f"✅ تم حفظ البند: {item_id}")
            else:
                logger.error(f"❌ فشل حفظ البند {item_id}: {result.get('error', 'خطأ غير معروف')}")
                
        except Exception as e:
            logger.error(f"❌ خطأ في معالجة البند: {str(e)}")
    
    if saved_count > 0:
        logger.info(f"✅ تم حفظ {saved_count} بند للفاتورة {invoice_id}")
    
    return saved_count

def sync_invoices():
    """الدالة الرئيسية المحسنة"""
    logger.info("🚀 بدء مزامنة الفواتير المحسنة...")
    
    result = {
        "invoices": 0,
        "items": 0,
        "errors": []
    }
    
    try:
        # اختبار الاتصالات
        if not test_connections():
            result["errors"].append("فشل اختبار الاتصالات")
            return result
        
        # جلب الفواتير
        all_invoices = fetch_all_invoices()
        
        if not all_invoices:
            result["errors"].append("لا توجد فواتير")
            return result
        
        logger.info(f"📋 معالجة {len(all_invoices)} فاتورة...")
        
        # معالجة الفواتير
        processed = 0
        for i, invoice_summary in enumerate(all_invoices, 1):
            try:
                invoice_id = str(invoice_summary.get("id"))
                
                if i % 10 == 0:  # تقرير كل 10 فواتير
                    logger.info(f"🔄 معالجة {i}/{len(all_invoices)}: الفاتورة {invoice_id}")
                
                # جلب التفاصيل
                invoice_details = fetch_invoice_details(invoice_id)
                
                if not invoice_details:
                    continue
                
                # حفظ الفاتورة
                if save_invoice_safely(invoice_details):
                    result["invoices"] += 1
                    
                    # حفظ البنود
                    items = invoice_details.get("invoice_item", [])
                    if items:
                        saved_items = save_invoice_items_safely(invoice_id, items)
                        result["items"] += saved_items
                
                processed += 1
                
                # استراحة كل 50 فاتورة
                if processed % 50 == 0:
                    logger.info(f"💤 استراحة قصيرة بعد {processed} فاتورة...")
                    time.sleep(2)
                
            except Exception as e:
                error_msg = f"خطأ في الفاتورة {invoice_summary.get('id', 'unknown')}: {str(e)}"
                result["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        # النتائج النهائية
        logger.info("=" * 80)
        logger.info("🎯 النتائج النهائية:")
        logger.info(f"✅ فواتير محفوظة: {result['invoices']}")
        logger.info(f"📦 بنود محفوظة: {result['items']}")
        logger.info(f"❌ أخطاء: {len(result['errors'])}")
        
        if result['errors']:
            logger.error("🚨 عينة أخطاء:")
            for error in result['errors'][:3]:
                logger.error(f"  - {error}")
        
        logger.info("=" * 80)
        
        return result
        
    except Exception as e:
        error_msg = f"خطأ عام: {str(e)}"
        result["errors"].append(error_msg)
        logger.error(f"💥 {error_msg}")
        return result

if __name__ == "__main__":
    test_result = sync_invoices()
    print(f"🎯 النتيجة: {test_result}")
