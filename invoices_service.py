# invoices_service.py

import os
import requests
import time
import uuid
import logging
from datetime import datetime
from typing import Dict, List, Optional

# إعداد التسجيل
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# متغيرات البيئة
DAFTRA_URL = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Headers
HEADERS_DAFTRA = {
    "apikey": DAFTRA_APIKEY,
    "Accept": "application/json",
    "Content-Type": "application/json"
}

HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def test_daftra_connection():
    """اختبار الاتصال بدفترة"""
    try:
        logger.info("🔍 اختبار الاتصال بدفترة...")
        test_url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page=1&limit=1"
        response = requests.get(test_url, headers=HEADERS_DAFTRA, timeout=15)
        
        logger.info(f"📱 دفترة - الحالة: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"📊 عينة البيانات متاحة: {bool(data.get('data'))}")
            return True
        else:
            logger.error(f"❌ فشل الاتصال بدفترة: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في الاتصال بدفترة: {e}")
        return False

def test_supabase_connection():
    """اختبار الاتصال بـ Supabase"""
    try:
        logger.info("🔍 اختبار الاتصال بـ Supabase...")
        test_url = f"{SUPABASE_URL}/rest/v1/"
        response = requests.get(test_url, headers=HEADERS_SUPABASE, timeout=15)
        
        logger.info(f"🗄️ Supabase - الحالة: {response.status_code}")
        
        if response.status_code in [200, 404, 406]:  # 406 طبيعي لهذا الرابط
            return True
        else:
            logger.error(f"❌ فشل الاتصال بـ Supabase: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في الاتصال بـ Supabase: {e}")
        return False

def fetch_all_invoices():
    """جلب جميع الفواتير من دفترة"""
    logger.info("📥 بدء جلب الفواتير من دفترة...")
    all_invoices = []
    page = 1
    
    while True:
        try:
            # جرب بدون فلتر الفرع
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit=100"
            logger.info(f"🔍 جلب الصفحة {page}...")
            
            response = requests.get(url, headers=HEADERS_DAFTRA, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"❌ خطأ في الصفحة {page}: {response.status_code} - {response.text}")
                break
            
            data = response.json()
            invoices = data.get("data", [])
            
            logger.info(f"📊 الصفحة {page}: وجدت {len(invoices)} فاتورة")
            
            if not invoices or len(invoices) == 0:
                logger.info("✅ انتهت جميع الفواتير")
                break
            
            all_invoices.extend(invoices)
            page += 1
            
            # استراحة بين الطلبات
            time.sleep(1)
            
        except requests.exceptions.Timeout:
            logger.error(f"⏰ انتهت مهلة الطلب للصفحة {page}")
            break
        except Exception as e:
            logger.error(f"❌ خطأ في جلب الصفحة {page}: {e}")
            break
    
    logger.info(f"📋 إجمالي الفواتير المجلبة: {len(all_invoices)}")
    return all_invoices

def fetch_invoice_details(invoice_id):
    """جلب تفاصيل الفاتورة من عدة فروع"""
    logger.debug(f"🔍 جلب تفاصيل الفاتورة: {invoice_id}")
    
    # جرب عدة فروع
    for branch_id in [1, 2, 3, 4, 5]:
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch_id}/{invoice_id}"
            response = requests.get(url, headers=HEADERS_DAFTRA, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                invoice_data = data.get("data", {}).get("Invoice", {})
                
                if invoice_data and invoice_data.get("id"):
                    logger.debug(f"✅ وجدت تفاصيل الفاتورة {invoice_id} في الفرع {branch_id}")
                    return invoice_data
            
        except Exception as e:
            logger.debug(f"❌ فشل جلب الفاتورة {invoice_id} من الفرع {branch_id}: {e}")
            continue
    
    logger.warning(f"⚠️ لم يتم العثور على تفاصيل الفاتورة: {invoice_id}")
    return None

def check_invoice_exists(invoice_id):
    """التحقق من وجود الفاتورة في Supabase"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/invoices?select=id&id=eq.{invoice_id}"
        response = requests.get(url, headers=HEADERS_SUPABASE, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return len(data) > 0
        
        return False
    except:
        return False

def save_invoice_to_supabase(invoice_data):
    """حفظ الفاتورة في Supabase"""
    try:
        invoice_id = str(invoice_data["id"])
        
        # تحقق من وجود الفاتورة
        if check_invoice_exists(invoice_id):
            logger.debug(f"ℹ️ الفاتورة {invoice_id} موجودة مسبقاً")
            return True
        
        # تحضير البيانات
        payload = {
            "id": invoice_id,
            "invoice_no": str(invoice_data.get("no", "")),
            "invoice_date": invoice_data.get("date"),
            "customer_id": str(invoice_data.get("customer_id", "")) if invoice_data.get("customer_id") else None,
            "client_business_name": str(invoice_data.get("client_business_name", "")) if invoice_data.get("client_business_name") else None,
            "total": float(invoice_data.get("total", 0)),
            "created_at": datetime.now().isoformat()
        }
        
        # إزالة القيم الفارغة
        payload = {k: v for k, v in payload.items() 
                  if v not in [None, "", "None", "null"]}
        
        logger.debug(f"💾 حفظ الفاتورة: {payload}")
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=HEADERS_SUPABASE,
            json=payload,
            timeout=30
        )
        
        if response.status_code in [200, 201]:
            logger.info(f"✅ تم حفظ الفاتورة: {invoice_id}")
            return True
        elif response.status_code == 409:
            logger.info(f"ℹ️ الفاتورة {invoice_id} موجودة مسبقاً (409)")
            return True
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفاتورة {invoice_data.get('id', 'unknown')}: {e}")
        return False

def save_invoice_items_to_supabase(invoice_id, items):
    """حفظ بنود الفاتورة في Supabase"""
    if not items:
        logger.debug(f"ℹ️ لا توجد بنود للفاتورة: {invoice_id}")
        return 0
    
    # تأكد من أن items قائمة
    if not isinstance(items, list):
        items = [items] if items else []
    
    saved_count = 0
    
    for item in items:
        try:
            # تحضير بيانات البند
            item_id = str(item.get("id", str(uuid.uuid4())))
            quantity = float(item.get("quantity", 0))
            unit_price = float(item.get("unit_price", 0))
            
            # تجاهل البنود بكمية صفر أو أقل
            if quantity <= 0:
                logger.debug(f"⏭️ تجاهل البند بكمية صفر: {item_id}")
                continue
            
            payload = {
                "id": item_id,
                "invoice_id": str(invoice_id),
                "product_id": str(item.get("product_id", "")) if item.get("product_id") else None,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": quantity * unit_price,
                "created_at": datetime.now().isoformat()
            }
            
            # إزالة القيم الفارغة
            payload = {k: v for k, v in payload.items() 
                      if v not in [None, "", "None", "null"]}
            
            logger.debug(f"💾 حفظ البند: {payload}")
            
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items",
                headers=HEADERS_SUPABASE,
                json=payload,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                saved_count += 1
                logger.debug(f"✅ تم حفظ البند: {item_id}")
            elif response.status_code == 409:
                saved_count += 1
                logger.debug(f"ℹ️ البند {item_id} موجود مسبقاً")
            else:
                logger.error(f"❌ فشل حفظ البند {item_id}: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"❌ خطأ في حفظ البند: {e}")
    
    logger.info(f"✅ تم حفظ {saved_count} بند للفاتورة {invoice_id}")
    return saved_count

def sync_invoices():
    """
    الدالة الرئيسية لمزامنة الفواتير - هذا اللي يستورده main.py
    ترجع dictionary بنفس تنسيق main.py: {'invoices': X, 'items': Y}
    """
    logger.info("🚀 بدء عملية مزامنة الفواتير...")
    
    # إحصائيات النتائج
    result = {
        "invoices": 0,
        "items": 0,
        "errors": []
    }
    
    try:
        # اختبار الاتصالات
        logger.info("🔍 اختبار الاتصالات...")
        if not test_daftra_connection():
            result["errors"].append("فشل الاتصال بدفترة")
            return result
            
        if not test_supabase_connection():
            result["errors"].append("فشل الاتصال بـ Supabase")
            return result
        
        logger.info("✅ جميع الاتصالات تعمل بشكل صحيح!")
        
        # جلب جميع الفواتير
        all_invoices = fetch_all_invoices()
        
        if not all_invoices:
            logger.warning("⚠️ لم يتم العثور على أي فواتير!")
            result["errors"].append("لا توجد فواتير")
            return result
        
        logger.info(f"📋 سيتم معالجة {len(all_invoices)} فاتورة...")
        
        # معالجة كل فاتورة
        for i, invoice_summary in enumerate(all_invoices, 1):
            try:
                invoice_id = str(invoice_summary.get("id"))
                logger.info(f"🔄 معالجة الفاتورة {i}/{len(all_invoices)}: {invoice_id}")
                
                # جلب تفاصيل الفاتورة
                invoice_details = fetch_invoice_details(invoice_id)
                
                if not invoice_details:
                    logger.warning(f"⚠️ لم يتم العثور على تفاصيل الفاتورة: {invoice_id}")
                    result["errors"].append(f"فاتورة {invoice_id}: لا توجد تفاصيل")
                    continue
                
                # حفظ الفاتورة
                if save_invoice_to_supabase(invoice_details):
                    result["invoices"] += 1
                    
                    # حفظ بنود الفاتورة
                    items = invoice_details.get("invoice_item", [])
                    if items:
                        saved_items_count = save_invoice_items_to_supabase(invoice_id, items)
                        result["items"] += saved_items_count
                    
                else:
                    result["errors"].append(f"فشل حفظ الفاتورة {invoice_id}")
                
                # استراحة قصيرة لتجنب الضغط على الخادم
                time.sleep(0.3)
                
            except Exception as e:
                error_msg = f"خطأ في معالجة الفاتورة {invoice_summary.get('id', 'unknown')}: {str(e)}"
                result["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        # طباعة النتائج النهائية
        logger.info("=" * 80)
        logger.info("🎯 ملخص النتائج:")
        logger.info(f"✅ الفواتير المحفوظة: {result['invoices']}")
        logger.info(f"📦 البنود المحفوظة: {result['items']}")
        logger.info(f"❌ عدد الأخطاء: {len(result['errors'])}")
        
        if result['errors']:
            logger.error("🚨 عينة من الأخطاء:")
            for error in result['errors'][:3]:  # أول 3 أخطاء
                logger.error(f"  - {error}")
            if len(result['errors']) > 3:
                logger.error(f"  ... و {len(result['errors']) - 3} أخطاء أخرى")
        
        logger.info("=" * 80)
        
        return result
        
    except Exception as e:
        error_msg = f"خطأ عام في المزامنة: {str(e)}"
        result["errors"].append(error_msg)
        logger.error(f"💥 {error_msg}")
        return result

# للاستخدام المباشر
if __name__ == "__main__":
    logger.info("🧪 تشغيل اختبار مباشر...")
    test_result = sync_invoices()
    print(f"🎯 النتيجة: {test_result}")
