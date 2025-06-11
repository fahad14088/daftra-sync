# invoices_service.py - الحل النهائي مع البنود

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

def get_invoice_details_for_items(invoice_id):
    """جلب تفاصيل الفاتورة للبنود فقط"""
    headers = {"apikey": DAFTRA_APIKEY}
    
    # جرب الفروع المختلفة
    for branch in [1, 2, 3, 4, 5]:
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{invoice_id}"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                invoice_data = data.get("data", {}).get("Invoice", {})
                
                if invoice_data and invoice_data.get("invoice_item"):
                    logger.info(f"✅ وجدت بنود للفاتورة {invoice_id} في الفرع {branch}")
                    return invoice_data.get("invoice_item", [])
            
        except Exception as e:
            logger.debug(f"خطأ في الفرع {branch}: {e}")
            continue
    
    logger.debug(f"ℹ️ لم أجد بنود للفاتورة {invoice_id}")
    return []

def save_invoice_basic(invoice_summary):
    """حفظ الفاتورة"""
    try:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        invoice_id = str(invoice_summary["id"])
        proper_uuid = generate_uuid_from_number(invoice_id)
        
        data = {
            "id": proper_uuid,
            "invoice_no": str(invoice_summary.get("no", "")),
            "total": float(invoice_summary.get("total", 0))
        }
        
        if invoice_summary.get("date"):
            data["invoice_date"] = invoice_summary["date"]
        
        if invoice_summary.get("client_business_name"):
            data["client_business_name"] = str(invoice_summary["client_business_name"])[:255]
        
        if invoice_summary.get("customer_id"):
            data["customer_id"] = str(invoice_summary["customer_id"])
        
        data = {k: v for k, v in data.items() if v not in [None, "", "None", 0]}
        
        logger.info(f"💾 حفظ الفاتورة {invoice_id}")
        
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

def save_invoice_items(invoice_uuid, invoice_id, items):
    """حفظ بنود الفاتورة"""
    if not items:
        return 0
    
    if not isinstance(items, list):
        items = [items]
    
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    
    saved_count = 0
    logger.info(f"📦 حفظ {len(items)} بند للفاتورة {invoice_id}")
    
    for i, item in enumerate(items, 1):
        try:
            quantity = float(item.get("quantity", 0))
            unit_price = float(item.get("unit_price", 0))
            
            if quantity <= 0:
                logger.debug(f"⏭️ تجاهل البند {i} - كمية صفر")
                continue
            
            # إنشاء UUID للبند
            item_id = str(item.get("id", ""))
            if item_id:
                item_uuid = generate_uuid_from_number(f"item-{item_id}-{invoice_id}")
            else:
                item_uuid = str(uuid.uuid4())
            
            item_data = {
                "id": item_uuid,
                "invoice_id": invoice_uuid,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": quantity * unit_price
            }
            
            # أضف معرف المنتج إذا كان موجود
            if item.get("product_id"):
                item_data["product_id"] = str(item["product_id"])
            
            logger.debug(f"💾 حفظ البند {i}: كمية={quantity}, سعر={unit_price}")
            
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items",
                headers=headers,
                json=item_data,
                timeout=30
            )
            
            if response.status_code in [200, 201, 409]:
                saved_count += 1
                logger.debug(f"✅ تم حفظ البند {i}")
            else:
                logger.error(f"❌ فشل حفظ البند {i}: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ خطأ في البند {i}: {e}")
    
    logger.info(f"✅ تم حفظ {saved_count} بند للفاتورة {invoice_id}")
    return saved_count

def sync_invoices():
    """الدالة الرئيسية مع البنود"""
    logger.info("🚀 بدء المزامنة الكاملة (فواتير + بنود)...")
    
    result = {"invoices": 0, "items": 0, "errors": []}
    
    try:
        # اختبار Supabase
        logger.info("🧪 اختبار Supabase...")
        test_uuid = str(uuid.uuid4())
        test_data = {"id": test_uuid, "invoice_no": "TEST", "total": 1.0}
        
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
            return result
        
        requests.delete(f"{SUPABASE_URL}/rest/v1/invoices?id=eq.{test_uuid}", headers=headers)
        logger.info("✅ اختبار Supabase نجح")
        
        # جلب الفواتير
        logger.info("📥 جلب الفواتير...")
        daftra_headers = {"apikey": DAFTRA_APIKEY}
        
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page=1&limit=10"
        response = requests.get(url, headers=daftra_headers, timeout=30)
        
        if response.status_code != 200:
            logger.error(f"❌ فشل جلب الفواتير: {response.text}")
            return result
        
        data = response.json()
        invoices = data.get("data", [])
        
        logger.info(f"📋 وجدت {len(invoices)} فاتورة")
        
        if not invoices:
            return result
        
        # معالجة كل فاتورة
        for i, invoice in enumerate(invoices, 1):
            try:
                invoice_id = str(invoice["id"])
                logger.info(f"🔄 {i}/{len(invoices)}: معالجة الفاتورة {invoice_id}")
                
                # حفظ الفاتورة
                invoice_uuid = save_invoice_basic(invoice)
                
                if invoice_uuid:
                    result["invoices"] += 1
                    
                    # جلب وحفظ البنود
                    items = get_invoice_details_for_items(invoice_id)
                    if items:
                        saved_items = save_invoice_items(invoice_uuid, invoice_id, items)
                        result["items"] += saved_items
                    else:
                        logger.info(f"ℹ️ لا توجد بنود للفاتورة {invoice_id}")
                
                time.sleep(1)
                
            except Exception as e:
                error_msg = f"خطأ في الفاتورة {invoice.get('id')}: {e}"
                result["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        # النتائج
        logger.info("=" * 60)
        logger.info("🎯 النتائج النهائية:")
        logger.info(f"✅ فواتير محفوظة: {result['invoices']}")
        logger.info(f"📦 بنود محفوظة: {result['items']}")
        logger.info(f"❌ عدد الأخطاء: {len(result['errors'])}")
        
        if result['invoices'] > 0:
            logger.info("🎉 تمت المزامنة بنجاح!")
        
        logger.info("=" * 60)
        
        return result
        
    except Exception as e:
        error_msg = f"خطأ عام: {e}"
        result["errors"].append(error_msg)
        logger.error(f"💥 {error_msg}")
        return result

if __name__ == "__main__":
    sync_invoices()
