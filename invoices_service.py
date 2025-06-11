# invoices_service.py - الحل الشامل والنهائي

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
    """تحويل رقم إلى UUID"""
    hash_input = f"invoice-{number}".encode('utf-8')
    hash_digest = hashlib.md5(hash_input).hexdigest()
    return f"{hash_digest[:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}-{hash_digest[16:20]}-{hash_digest[20:32]}"

def check_invoice_exists(invoice_id):
    """تحقق من وجود الفاتورة"""
    try:
        invoice_uuid = generate_uuid_from_number(invoice_id)
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/invoices?select=id&id=eq.{invoice_uuid}",
            headers=headers,
            timeout=10
        )
        
        if response.status_code == 200:
            return len(response.json()) > 0
        return False
    except:
        return False

def get_all_invoices():
    """جلب جميع الفواتير من جميع الصفحات"""
    logger.info("📥 جلب جميع الفواتير...")
    
    headers = {"apikey": DAFTRA_APIKEY}
    all_invoices = []
    page = 1
    
    while True:
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit=100"
            logger.info(f"🔍 جلب الصفحة {page}")
            
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"❌ خطأ في الصفحة {page}: {response.text}")
                break
            
            data = response.json()
            invoices = data.get("data", [])
            
            if not invoices:
                logger.info(f"✅ انتهت الفواتير في الصفحة {page}")
                break
            
            logger.info(f"📊 الصفحة {page}: {len(invoices)} فاتورة")
            all_invoices.extend(invoices)
            
            page += 1
            time.sleep(1)  # استراحة بين الصفحات
            
        except Exception as e:
            logger.error(f"❌ خطأ في جلب الصفحة {page}: {e}")
            break
    
    logger.info(f"📋 إجمالي الفواتير: {len(all_invoices)}")
    return all_invoices

def get_invoice_details_all_branches(invoice_id):
    """جلب تفاصيل الفاتورة من جميع الفروع"""
    headers = {"apikey": DAFTRA_APIKEY}
    
    # جرب جميع الفروع الممكنة
    for branch in range(1, 11):  # فروع من 1 إلى 10
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{invoice_id}"
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                invoice_data = data.get("data", {})
                
                # جرب مفاتيح مختلفة
                if invoice_data.get("Invoice"):
                    invoice_info = invoice_data["Invoice"]
                elif invoice_data.get("id"):
                    invoice_info = invoice_data
                else:
                    continue
                
                if invoice_info and invoice_info.get("id"):
                    logger.debug(f"✅ وجدت تفاصيل الفاتورة {invoice_id} في الفرع {branch}")
                    return invoice_info
                    
        except Exception as e:
            logger.debug(f"خطأ في الفرع {branch}: {e}")
            continue
    
    return None

def save_invoice_complete(invoice_summary):
    """حفظ الفاتورة بشكل كامل"""
    try:
        invoice_id = str(invoice_summary["id"])
        
        # تحقق من وجود الفاتورة
        if check_invoice_exists(invoice_id):
            logger.debug(f"ℹ️ الفاتورة {invoice_id} موجودة مسبقاً")
            return generate_uuid_from_number(invoice_id)
        
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        invoice_uuid = generate_uuid_from_number(invoice_id)
        
        data = {
            "id": invoice_uuid,
            "invoice_no": str(invoice_summary.get("no", "")),
            "total": float(invoice_summary.get("total", 0))
        }
        
        # إضافة البيانات الإضافية
        if invoice_summary.get("date"):
            data["invoice_date"] = invoice_summary["date"]
        
        if invoice_summary.get("client_business_name"):
            data["client_business_name"] = str(invoice_summary["client_business_name"])[:255]
        
        if invoice_summary.get("customer_id"):
            data["customer_id"] = str(invoice_summary["customer_id"])
        
        # تنظيف البيانات
        data = {k: v for k, v in data.items() if v not in [None, "", "None", 0]}
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=data,
            timeout=30
        )
        
        if response.status_code in [200, 201, 409]:
            logger.debug(f"✅ تم حفظ الفاتورة {invoice_id}")
            return invoice_uuid
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}: {response.text}")
            return None
            
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفاتورة {invoice_summary.get('id')}: {e}")
        return None

def save_items_complete(invoice_uuid, invoice_id, items):
    """حفظ جميع البنود"""
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
    
    for i, item in enumerate(items, 1):
        try:
            quantity = float(item.get("quantity", 0))
            unit_price = float(item.get("unit_price", 0))
            
            if quantity <= 0:
                continue
            
            # UUID للبند
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
            
            # إضافة معرف المنتج إذا كان موجود
            if item.get("product_id"):
                item_data["product_id"] = str(item["product_id"])
            
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items",
                headers=headers,
                json=item_data,
                timeout=30
            )
            
            if response.status_code in [200, 201, 409]:
                saved_count += 1
            else:
                logger.error(f"❌ فشل حفظ البند {i}: {response.text}")
                
        except Exception as e:
            logger.error(f"❌ خطأ في البند {i}: {e}")
    
    return saved_count

def sync_invoices():
    """المزامنة الشاملة - جميع الفواتير والبنود"""
    logger.info("🚀 بدء المزامنة الشاملة...")
    
    result = {"invoices": 0, "items": 0, "errors": []}
    
    try:
        # جلب جميع الفواتير
        all_invoices = get_all_invoices()
        
        if not all_invoices:
            logger.warning("⚠️ لا توجد فواتير!")
            return result
        
        logger.info(f"📋 ستتم معالجة {len(all_invoices)} فاتورة...")
        
        # معالجة كل فاتورة
        for i, invoice in enumerate(all_invoices, 1):
            try:
                invoice_id = str(invoice["id"])
                
                # تقرير كل 50 فاتورة
                if i % 50 == 0:
                    logger.info(f"🔄 معالجة {i}/{len(all_invoices)}: الفاتورة {invoice_id}")
                
                # حفظ الفاتورة
                invoice_uuid = save_invoice_complete(invoice)
                
                if invoice_uuid:
                    result["invoices"] += 1
                    
                    # جلب وحفظ البنود
                    details = get_invoice_details_all_branches(invoice_id)
                    if details:
                        items = details.get("invoice_item", [])
                        if items:
                            saved_items = save_items_complete(invoice_uuid, invoice_id, items)
                            result["items"] += saved_items
                            
                            if saved_items > 0:
                                logger.debug(f"✅ حفظ {saved_items} بند للفاتورة {invoice_id}")
                
                # استراحة كل 10 فواتير
                if i % 10 == 0:
                    time.sleep(1)
                
            except Exception as e:
                error_msg = f"خطأ في الفاتورة {invoice.get('id')}: {e}"
                result["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        # النتائج النهائية
        logger.info("=" * 80)
        logger.info("🎯 النتائج النهائية:")
        logger.info(f"📊 إجمالي الفواتير المعالجة: {len(all_invoices)}")
        logger.info(f"✅ فواتير محفوظة: {result['invoices']}")
        logger.info(f"📦 بنود محفوظة: {result['items']}")
        logger.info(f"❌ عدد الأخطاء: {len(result['errors'])}")
        logger.info(f"🏆 معدل النجاح: {(result['invoices']/len(all_invoices)*100):.1f}%")
        
        if result['errors']:
            logger.error("🚨 عينة من الأخطاء:")
            for error in result['errors'][:5]:
                logger.error(f"  - {error}")
        
        if result['invoices'] > 0:
            logger.info("🎉 تمت المزامنة بنجاح!")
        
        logger.info("=" * 80)
        
        return result
        
    except Exception as e:
        error_msg = f"خطأ عام: {e}"
        result["errors"].append(error_msg)
        logger.error(f"💥 {error_msg}")
        return result

if __name__ == "__main__":
    sync_invoices()
