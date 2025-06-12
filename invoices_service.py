import os
import requests
import time
import uuid
import logging
from datetime import datetime
import hashlib
import json
import traceback

# تم تصحيح هذا السطر باستخدام raw string
logging.basicConfig(level=logging.INFO, format=r'%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# المتغيرات
DAFTRA_URL = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number):
    hash_input = f"invoice-{number}".encode("utf-8")
    hash_digest = hashlib.md5(hash_input).hexdigest()
    return f"{hash_digest[:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}-{hash_digest[16:20]}-{hash_digest[20:32]}"

def safe_float(value, default=0.0):
    """تحويل آمن للرقم"""
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ""))
    except Exception as e:
        logger.error(f"❌ خطأ في تحويل القيمة '{value}' إلى رقم: {e}", exc_info=True)
        return default

def safe_string(value, max_length=None):
    """تحويل آمن للنص"""
    try:
        if value is None:
            return ""
        result = str(value).strip()
        if max_length and len(result) > max_length:
            result = result[:max_length]
        return result
    except Exception as e:
        logger.error(f"❌ خطأ في تحويل القيمة '{value}' إلى نص: {e}", exc_info=True)
        return ""

def get_all_invoices_complete():
    """جلب جميع الفواتير من جميع الصفحات ولجميع المخازن المعروفة"""
    logger.info("📥 جلب جميع الفواتير...")
    
    headers = {"apikey": DAFTRA_APIKEY}
    all_invoices = []
    
    # قائمة بمعرفات المخازن (store_id) التي تريد جلب الفواتير منها
    store_ids = [1, 2, 3]
    
    for store_id in store_ids:
        logger.info(f"🔄 جلب الفواتير من المخزن (store_id): {store_id}...")
        page = 1
        while True:
            try:
                url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/{store_id}?page={page}&limit=100"
                logger.info(f"📄 المخزن {store_id}, الصفحة {page}")
                
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"❌ خطأ في المخزن {store_id}, الصفحة {page}: {response.text}", exc_info=True)
                    break
                
                data = response.json()
                invoices = data.get("data", [])
                
                if not invoices:
                    logger.info(f"✅ انتهت الفواتير للمخزن {store_id}")
                    break
                
                logger.info(f"📊 وجدت {len(invoices)} فاتورة في المخزن {store_id}")
                all_invoices.extend(invoices)
                
                page += 1
                time.sleep(1) # تأخير لتجنب تجاوز حدود معدل الطلبات
                
            except Exception as e:
                logger.error(f"❌ خطأ في جلب الفواتير من المخزن {store_id}, الصفحة {page}: {e}", exc_info=True)
                break
    
    logger.info(f"📋 إجمالي الفواتير التي تم جلبها من جميع المخازن: {len(all_invoices)}")
    return all_invoices

def get_invoice_full_details(invoice_id):
    """جلب تفاصيل الفاتورة الكاملة من جميع المخازن المحتملة"""
    headers = {"apikey": DAFTRA_APIKEY}
    
    # جرب جميع المخازن المعروفة لجلب التفاصيل
    store_ids_for_details = [1, 2, 3]
    
    for branch in store_ids_for_details:
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{invoice_id}"
            logger.info(f"🔍 محاولة جلب تفاصيل الفاتورة {invoice_id} من المخزن {branch}")
            response = requests.get(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                
                # تسجيل الاستجابة الكاملة للتحليل
                logger.info(f"✅ استجابة API لتفاصيل الفاتورة {invoice_id} من المخزن {branch}")
                
                # البحث عن البيانات في مسارات مختلفة
                invoice_data = None
                if "data" in data:
                    if isinstance(data["data"], dict):
                        if "Invoice" in data["data"]:
                            invoice_data = data["data"]["Invoice"]
                        else:
                            invoice_data = data["data"]
                
                if invoice_data and invoice_data.get("id"):
                    logger.info(f"✅ وجدت تفاصيل الفاتورة {invoice_id} في المخزن {branch}")
                    
                    # تسجيل القيم المالية المهمة
                    logger.info(f"💰 القيم المالية للفاتورة {invoice_id}:")
                    logger.info(f"   - المبلغ الإجمالي (summary_total): {invoice_data.get('summary_total')}")
                    logger.info(f"   - المبلغ المدفوع (summary_paid): {invoice_data.get('summary_paid')}")
                    logger.info(f"   - المبلغ غير المدفوع (summary_unpaid): {invoice_data.get('summary_unpaid')}")
                    
                    # البحث عن بنود الفاتورة في مسارات مختلفة
                    invoice_items = None
                    
                    # المسار المتوقع الأول
                    if "invoice_item" in invoice_data:
                        invoice_items = invoice_data["invoice_item"]
                        logger.info(f"✅ وجدت {len(invoice_items)} بند في المسار invoice_item")
                    
                    # المسار المتوقع الثاني
                    elif "invoice_items" in invoice_data:
                        invoice_items = invoice_data["invoice_items"]
                        logger.info(f"✅ وجدت {len(invoice_items)} بند في المسار invoice_items")
                    
                    # المسار المتوقع الثالث - البحث في الاستجابة الكاملة
                    elif "invoice_item" in data:
                        invoice_items = data["invoice_item"]
                        logger.info(f"✅ وجدت {len(invoice_items)} بند في المسار الرئيسي invoice_item")
                    
                    # تسجيل عدم وجود بنود
                    else:
                        logger.warning(f"⚠️ الفاتورة {invoice_id} من المخزن {branch} لا تحتوي على بنود في استجابة API.")
                        
                        # تسجيل المفاتيح المتاحة للمساعدة في التشخيص
                        logger.info(f"🔑 المفاتيح المتاحة في بيانات الفاتورة: {list(invoice_data.keys())}")
                        
                        # البحث عن أي مفتاح قد يحتوي على كلمة "item"
                        item_keys = [key for key in invoice_data.keys() if "item" in key.lower()]
                        if item_keys:
                            logger.info(f"🔍 وجدت مفاتيح قد تحتوي على بنود: {item_keys}")
                    
                    # إضافة البنود إلى بيانات الفاتورة
                    if invoice_items:
                        invoice_data["invoice_item"] = invoice_items
                    
                    return invoice_data
                    
        except Exception as e:
            logger.error(f"❌ خطأ أثناء محاولة جلب تفاصيل الفاتورة {invoice_id} من المخزن {branch}: {e}", exc_info=True)
            continue
    
    logger.warning(f"⚠️ لم أجد تفاصيل للفاتورة {invoice_id} في أي من المخازن المعروفة.")
    return None

def save_invoice_complete(invoice_summary, invoice_details=None):
    """حفظ الفاتورة بجميع البيانات"""
    try:
        invoice_id = str(invoice_summary["id"])
        invoice_uuid = generate_uuid_from_number(invoice_id)
        
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        # استخدم بيانات التفاصيل إذا كانت متاحة، وإلا استخدم الملخص
        source_data = invoice_details if invoice_details else invoice_summary
        
        # البيانات الأساسية
        payload = {
            "id": invoice_uuid,
            "invoice_no": safe_string(source_data.get("no", "")),
        }
        
        # استخراج القيم المالية مباشرة من الحقول الصحيحة
        # استخدام الحقول المباشرة من استجابة API
        payload["total"] = safe_float(source_data.get("summary_total", 0))
        payload["summary_paid"] = safe_float(source_data.get("summary_paid", 0))
        payload["summary_unpaid"] = safe_float(source_data.get("summary_unpaid", 0))
        
        # تسجيل القيم المالية للتأكد من صحتها
        logger.info(f"💰 القيم المالية التي سيتم حفظها للفاتورة {invoice_id}:")
        logger.info(f"   - المبلغ الإجمالي (total): {payload['total']}")
        logger.info(f"   - المبلغ المدفوع (summary_paid): {payload['summary_paid']}")
        logger.info(f"   - المبلغ غير المدفوع (summary_unpaid): {payload['summary_unpaid']}")
        
        # البيانات الإضافية
        if source_data.get("date"):
            payload["invoice_date"] = safe_string(source_data["date"])
        
        if source_data.get("client_business_name"):
            payload["client_business_name"] = safe_string(source_data["client_business_name"], 255)
        
        if source_data.get("customer_id"):
            payload["customer_id"] = safe_string(source_data["customer_id"])
        
        # معلومات إضافية من التفاصيل
        if invoice_details:
            if invoice_details.get("notes"):
                payload["notes"] = safe_string(invoice_details["notes"], 500)
            
            if invoice_details.get("created_at"):
                payload["created_at"] = safe_string(invoice_details["created_at"])
            elif invoice_details.get("created"):
                payload["created_at"] = safe_string(invoice_details["created"])
        
        # تنظيف البيانات
        clean_payload = {k: v for k, v in payload.items() if v not in [None, "", "None"]}
        
        logger.info(f"💾 حفظ الفاتورة {invoice_id} - المبلغ: {clean_payload.get('total', 0)}")
        
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            json=clean_payload,
            timeout=30
        )
        
        if response.status_code in [200, 201, 409]:
            logger.info(f"✅ تم حفظ الفاتورة {invoice_id}")
            return invoice_uuid
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}: {response.text}", exc_info=True)
            return None
            
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفاتورة {invoice_summary.get('id')}: {e}", exc_info=True)
        return None

def save_invoice_items_complete(invoice_uuid, invoice_id, items):
    """حفظ بنود الفاتورة بشكل كامل"""
    if not items:
        logger.warning(f"⚠️ لا توجد بنود لحفظها للفاتورة {invoice_id}")
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
            # تسجيل بيانات البند للتشخيص
            logger.info(f"🔍 بيانات البند {i}: {json.dumps(item)}")
            
            # البحث عن الكمية في مسارات مختلفة
            quantity = None
            if "quantity" in item:
                quantity = safe_float(item["quantity"], 0)
            elif "qty" in item:
                quantity = safe_float(item["qty"], 0)
            else:
                # البحث عن أي مفتاح يحتوي على "qty" أو "quantity"
                qty_keys = [key for key in item.keys() if "qty" in key.lower() or "quantity" in key.lower()]
                if qty_keys:
                    quantity = safe_float(item[qty_keys[0]], 0)
                else:
                    logger.warning(f"⚠️ لم أجد حقل الكمية في البند {i}")
                    quantity = 1  # قيمة افتراضية
            
            # البحث عن سعر الوحدة في مسارات مختلفة
            unit_price = None
            if "unit_price" in item:
                unit_price = safe_float(item["unit_price"], 0)
            elif "price" in item:
                unit_price = safe_float(item["price"], 0)
            else:
                # البحث عن أي مفتاح يحتوي على "price"
                price_keys = [key for key in item.keys() if "price" in key.lower() and "total" not in key.lower()]
                if price_keys:
                    unit_price = safe_float(item[price_keys[0]], 0)
                else:
                    logger.warning(f"⚠️ لم أجد حقل سعر الوحدة في البند {i}")
                    unit_price = 0
            
            if quantity <= 0:
                logger.warning(f"⚠️ البند {i} للفاتورة {invoice_id} لديه كمية صفر أو أقل. تخطي.")
                continue
            
            # UUID للبند
            item_id = safe_string(item.get("id", ""))
            if item_id:
                item_uuid = generate_uuid_from_number(f"item-{item_id}-{invoice_id}")
            else:
                item_uuid = str(uuid.uuid4())
            
            item_payload = {
                "id": item_uuid,
                "invoice_id": invoice_uuid,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": quantity * unit_price
            }
            
            # معلومات إضافية
            if item.get("product_id"):
                item_payload["product_id"] = safe_string(item["product_id"])
            
            if item.get("product_code"):
                item_payload["product_code"] = safe_string(item["product_code"])
            
            # تسجيل البيانات التي سيتم حفظها
            logger.info(f"💾 حفظ البند {i} للفاتورة {invoice_id}: الكمية={quantity}, السعر={unit_price}, الإجمالي={quantity * unit_price}")
            
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items",
                headers=headers,
                json=item_payload,
                timeout=30
            )
            
            if response.status_code in [200, 201, 409]:
                saved_count += 1
                logger.info(f"✅ تم حفظ البند {i} للفاتورة {invoice_id}")
            else:
                logger.error(f"❌ فشل حفظ البند {i} للفاتورة {invoice_id}: {response.text}", exc_info=True)
                
        except Exception as e:
            logger.error(f"❌ خطأ في البند {i} للفاتورة {invoice_id}: {e}", exc_info=True)
    
    logger.info(f"✅ تم حفظ {saved_count} بند للفاتورة {invoice_id}")
    return saved_count

def sync_invoices():
    """المزامنة الشاملة النهائية"""
    logger.info("🚀 بدء المزامنة الشاملة...")
    
    result = {"invoices": 0, "items": 0, "errors": []}
    
    try:
        # جلب جميع الفواتير
        all_invoices = get_all_invoices_complete()
        
        if not all_invoices:
            logger.error("❌ لا توجد فواتير لجلبها!")
            return result
        
        logger.info(f"📋 معالجة {len(all_invoices)} فاتورة...")
        
        # معالجة كل فاتورة
        for i, invoice in enumerate(all_invoices, 1):
            try:
                invoice_id = str(invoice["id"])
                
                # تقرير التقدم
                if i % 10 == 0:
                    logger.info(f"🔄 معالجة {i}/{len(all_invoices)}: الفاتورة {invoice_id}")
                
                # جلب التفاصيل الكاملة
                details = get_invoice_full_details(invoice_id)
                
                # حفظ الفاتورة (مع أو بدون تفاصيل)
                invoice_uuid = save_invoice_complete(invoice, details)
                
                if invoice_uuid:
                    result["invoices"] += 1
                    
                    # حفظ البنود إذا كانت متاحة
                    if details and details.get("invoice_item"):
                        items = details["invoice_item"]
                        saved_items = save_invoice_items_complete(invoice_uuid, invoice_id, items)
                        result["items"] += saved_items
                    else:
                        logger.warning(f"⚠️ الفاتورة {invoice_id} لا تحتوي على بنود في التفاصيل. تخطي حفظ البنود.")
                else:
                    logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}، تخطي حفظ البنود.")
                
                # استراحة كل 50 فاتورة
                if i % 50 == 0:
                    time.sleep(2)
                
            except Exception as e:
                error_msg = f"خطأ في معالجة الفاتورة {invoice.get('id')}: {e}"
                result["errors"].append(error_msg)
                logger.error(f"❌ {error_msg}", exc_info=True)
        
        # النتائج النهائية
        logger.info("=" * 80)
        logger.info("🎯 النتائج النهائية:")
        logger.info(f"📊 إجمالي الفواتير التي تم جلبها: {len(all_invoices)}")
        logger.info(f"✅ فواتير محفوظة بنجاح: {result['invoices']}")
        logger.info(f"📦 بنود محفوظة بنجاح: {result['items']}")
        logger.info(f"❌ عدد الأخطاء التي حدثت: {len(result['errors'])}")
        
        if len(all_invoices) > 0:
            success_rate = (result["invoices"] / len(all_invoices)) * 100
            logger.info(f"🏆 معدل نجاح حفظ الفواتير: {success_rate:.1f}%")
        
        if result["invoices"] > 0:
            logger.info("🎉 تمت المزامنة بنجاح لبعض الفواتير على الأقل!")
        
        logger.info("=" * 80)
        
        return result
        
    except Exception as e:
        logger.error(f"❌ خطأ عام في المزامنة: {e}", exc_info=True)
        return result

if __name__ == "__main__":
    sync_invoices()
