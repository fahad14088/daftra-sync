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

def test_connections():
    """اختبار الاتصالات"""
    logger.info("🔍 اختبار الاتصالات...")
    
    try:
        # اختبار دفترة
        test_url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page=1&limit=1"
        response = requests.get(test_url, headers=HEADERS_DAFTRA, timeout=15)
        logger.info(f"📱 دفترة: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"❌ فشل الاتصال بدفترة: {response.text}")
            return False
        
        # اختبار Supabase
        test_url = f"{SUPABASE_URL}/rest/v1/invoices?select=count"
        response = requests.get(test_url, headers=HEADERS_SUPABASE, timeout=15)
        logger.info(f"🗄️ Supabase: {response.status_code}")
        
        if response.status_code != 200 and response.status_code != 406:
            logger.error(f"❌ فشل الاتصال بـ Supabase: {response.text}")
            return False
        
        logger.info("✅ جميع الاتصالات تعمل!")
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في اختبار الاتصالات: {e}")
        return False

def get_all_invoices():
    """جلب جميع الفواتير من دفترة"""
    logger.info("📥 جلب الفواتير من دفترة...")
    all_invoices = []
    page = 1
    
    while True:
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit=100"
            logger.info(f"🔍 الصفحة {page}")
            
            response = requests.get(url, headers=HEADERS_DAFTRA, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"❌ خطأ الصفحة {page}: {response.text}")
                break
            
            data = response.json()
            invoices = data.get("data", [])
            
            if not invoices:
                logger.info("✅ انتهت الفواتير")
                break
            
            logger.info(f"📊 وجدت {len(invoices)} فاتورة")
            all_invoices.extend(invoices)
            page += 1
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"❌ خطأ الصفحة {page}: {e}")
            break
    
    logger.info(f"📋 إجمالي: {len(all_invoices)} فاتورة")
    return all_invoices

def get_invoice_details(invoice_id):
    """جلب تفاصيل الفاتورة"""
    for branch_id in [1, 2, 3, 4, 5]:
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch_id}/{invoice_id}"
            response = requests.get(url, headers=HEADERS_DAFTRA, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                invoice_data = data.get("data", {}).get("Invoice", {})
                if invoice_data:
                    return invoice_data
                    
        except Exception:
            continue
    
    return None

def save_invoice(invoice_data):
    """حفظ الفاتورة"""
    try:
        payload = {
            "id": str(invoice_data["id"]),
            "invoice_no": str(invoice_data.get("no", "")),
            "invoice_date": invoice_data.get("date"),
            "customer_id": str(invoice_data.get("customer_id", "")),
            "client_business_name": str(invoice_data.get("client_business_name", "")),
            "total": float(invoice_data.get("total", 0))
        }
        
        # إزالة القيم الفارغة
        payload = {k: v for k, v in payload.items() if v not in [None, "", "None"]}
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=HEADERS_SUPABASE,
            json=payload,
            timeout=30
        )
        
        if response.status_code in [200, 201, 409]:
            return True
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_data['id']}: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ حفظ الفاتورة {invoice_data.get('id')}: {e}")
        return False

def save_invoice_items(invoice_id, items):
    """حفظ بنود الفاتورة"""
    if not items:
        return 0
    
    saved_count = 0
    
    for item in items:
        try:
            item_id = str(item.get("id", str(uuid.uuid4())))
            quantity = float(item.get("quantity", 0))
            unit_price = float(item.get("unit_price", 0))
            
            # تجاهل البنود بكمية صفر
            if quantity <= 0:
                continue
            
            payload = {
                "id": item_id,
                "invoice_id": str(invoice_id),
                "product_id": str(item.get("product_id", "")),
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": quantity * unit_price
            }
            
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items",
                headers=HEADERS_SUPABASE,
                json=payload,
                timeout=30
            )
            
            if response.status_code in [200, 201, 409]:
                saved_count += 1
            else:
                logger.error(f"❌ فشل حفظ البند: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ خطأ حفظ البند: {e}")
    
    return saved_count

def sync_invoices():
    """الدالة الرئيسية للمزامنة - هذا اللي يستورده main.py"""
    logger.info("🚀 بدء المزامنة...")
    
    # اختبار الاتصالات
    if not test_connections():
        logger.error("❌ فشل اختبار الاتصالات!")
        return {"invoices": 0, "items": 0, "errors": ["فشل الاتصالات"]}
    
    # إحصائيات
    stats = {
        "invoices": 0,
        "items": 0,
        "errors": []
    }
    
    try:
        # جلب جميع الفواتير
        all_invoices = get_all_invoices()
        
        if not all_invoices:
            logger.warning("⚠️ لا توجد فواتير!")
            return stats
        
        logger.info(f"📋 معالجة {len(all_invoices)} فاتورة...")
        
        # معالجة كل فاتورة
        for i, invoice_summary in enumerate(all_invoices, 1):
            try:
                invoice_id = str(invoice_summary.get("id"))
                logger.info(f"🔄 {i}/{len(all_invoices)}: فاتورة {invoice_id}")
                
                # جلب التفاصيل
                invoice_details = get_invoice_details(invoice_id)
                
                if not invoice_details:
                    logger.warning(f"⚠️ لا توجد تفاصيل للفاتورة {invoice_id}")
                    continue
                
                # حفظ الفاتورة
                if save_invoice(invoice_details):
                    stats["invoices"] += 1
                    logger.info(f"✅ تم حفظ الفاتورة {invoice_id}")
                    
                    # حفظ البنود
                    items = invoice_details.get("invoice_item", [])
                    if not isinstance(items, list):
                        items = [items] if items else []
                    
                    if items:
                        saved_items = save_invoice_items(invoice_id, items)
                        stats["items"] += saved_items
                        logger.info(f"✅ تم حفظ {saved_items} بند")
                
                # استراحة قصيرة
                time.sleep(0.5)
                
            except Exception as e:
                error_msg = f"خطأ معالجة الفاتورة {invoice_summary.get('id')}: {e}"
                stats["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        # النتائج النهائية
        logger.info("=" * 60)
        logger.info("🎯 النتائج النهائية:")
        logger.info(f"✅ الفواتير: {stats['invoices']}")
        logger.info(f"📦 البنود: {stats['items']}")
        logger.info(f"❌ الأخطاء: {len(stats['errors'])}")
        logger.info("=" * 60)
        
        return stats
        
    except Exception as e:
        error_msg = f"خطأ عام في المزامنة: {e}"
        stats["errors"].append(error_msg)
        logger.error(f"💥 {error_msg}")
        return stats

# للاستخدام المباشر
if __name__ == "__main__":
    result = sync_invoices()
    print(f"✅ النتيجة: {result}")
