import os
import requests
import time
import uuid
import logging
from datetime import datetime
import hashlib
import json
import traceback

# إعداد التسجيل
logging.basicConfig(level=logging.INFO, format=r'%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# المتغيرات
DAFTRA_URL = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number):
    """توليد معرف UUID من رقم"""
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

def get_all_branches():
    """الحصول على قائمة الفروع"""
    # استخدام قائمة ثابتة للفروع كما في كودك المحلي
    branches = [1, 2, 3]
    logger.info(f"✅ استخدام الفروع المحددة: {branches}")
    return branches

def fetch_with_retry(url, headers, max_retries=3, timeout=30):
    """محاولة جلب البيانات مع إعادة المحاولة في حالة فشل الاتصال"""
    for retry in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"⚠️ كود استجابة غير متوقع: {response.status_code}")
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 5
                    logger.info(f"⏱️ انتظار {wait_time} ثوانٍ قبل إعادة المحاولة...")
                    time.sleep(wait_time)
                    continue
        
        except requests.exceptions.Timeout:
            logger.warning(f"⚠️ انتهت مهلة الاتصال")
            if retry < max_retries - 1:
                wait_time = (retry + 1) * 5
                logger.info(f"⏱️ انتظار {wait_time} ثوانٍ قبل إعادة المحاولة...")
                time.sleep(wait_time)
                continue
        
        except Exception as e:
            logger.error(f"❌ خطأ غير متوقع: {e}", exc_info=True)
            if retry < max_retries - 1:
                wait_time = (retry + 1) * 5
                logger.info(f"⏱️ انتظار {wait_time} ثوانٍ قبل إعادة المحاولة...")
                time.sleep(wait_time)
                continue
    
    # إذا وصلنا إلى هنا، فقد فشلت جميع المحاولات
    return None

def check_invoice_exists(invoice_id):
    """التحقق مما إذا كانت الفاتورة موجودة بالفعل في قاعدة البيانات"""
    try:
        invoice_uuid = generate_uuid_from_number(invoice_id)
        
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        
        response = requests.get(
            f"{SUPABASE_URL}/rest/v1/invoices?id=eq.{invoice_uuid}&select=id",
            headers=headers,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            return len(data) > 0
        else:
            logger.warning(f"⚠️ فشل التحقق من وجود الفاتورة {invoice_id}: {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ خطأ في التحقق من وجود الفاتورة {invoice_id}: {e}", exc_info=True)
        return False

def get_all_invoices_complete():
    """جلب جميع الفواتير من جميع الصفحات ولجميع الفروع المعروفة"""
    logger.info("📥 جلب جميع الفواتير...")
    
    headers = {"apikey": DAFTRA_APIKEY}
    all_invoices = []
    processed_ids = set()  # مجموعة لتتبع معرفات الفواتير التي تمت معالجتها
    
    # قائمة بمعرفات الفروع التي تريد جلب الفواتير منها
    branches = get_all_branches()
    
    for branch_id in branches:
        logger.info(f"🔄 جلب الفواتير من الفرع: {branch_id}...")
        page = 1
        limit = 100
        new_invoices_found = False
        
        while True:
            try:
                # استخدام نفس طريقة جلب الفواتير كما في كودك المحلي
                url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?filter[branch_id]={branch_id}&page={page}&limit={limit}"
                logger.info(f"📄 الفرع {branch_id}, الصفحة {page}")
                
                data = fetch_with_retry(url, headers)
                
                # إذا فشلت جميع المحاولات
                if data is None:
                    logger.error(f"❌ فشل استرجاع البيانات من الصفحة {page} بعد عدة محاولات")
                    break
                
                invoices = data.get("data", [])
                
                if not invoices:
                    logger.info(f"✅ انتهت الفواتير للفرع {branch_id}")
                    break
                
                logger.info(f"📊 وجدت {len(invoices)} فاتورة في الصفحة {page}")
                
                # التحقق من الفواتير الجديدة وتجنب التكرار
                new_invoices_count = 0
                for invoice in invoices:
                    invoice_id = str(invoice.get("id"))
                    
                    # تخطي الفواتير المكررة
                    if invoice_id in processed_ids:
                        logger.info(f"⏭️ الفاتورة {invoice_id} تمت معالجتها بالفعل. تخطي.")
                        continue
                    
                    # التحقق مما إذا كانت الفاتورة موجودة بالفعل في قاعدة البيانات
                    if check_invoice_exists(invoice_id):
                        logger.info(f"⏭️ الفاتورة {invoice_id} موجودة بالفعل في قاعدة البيانات. تخطي.")
                        processed_ids.add(invoice_id)
                        continue
                    
                    # إضافة الفاتورة الجديدة
                    all_invoices.append(invoice)
                    processed_ids.add(invoice_id)
                    new_invoices_count += 1
                
                logger.info(f"✅ تمت إضافة {new_invoices_count} فاتورة جديدة من الصفحة {page}")
                
                # إذا لم نجد أي فواتير جديدة في هذه الصفحة، نتوقف
                if new_invoices_count == 0:
                    logger.info(f"🏁 لم يتم العثور على فواتير جديدة في الصفحة {page}. التوقف عن جلب المزيد من الصفحات.")
                    break
                
                # إذا كان عدد الفواتير في الصفحة أقل من الحد، فقد وصلنا للنهاية
                if len(invoices) < limit:
                    logger.info(f"🏁 وصلنا للصفحة الأخيرة ({page}). انتهاء الجلب لهذا الفرع.")
                    break
                
                page += 1
                time.sleep(1) # تأخير لتجنب تجاوز حدود معدل الطلبات
                
            except Exception as e:
                logger.error(f"❌ خطأ في جلب الفواتير من الفرع {branch_id}, الصفحة {page}: {e}", exc_info=True)
                break
    
    logger.info(f"📋 إجمالي الفواتير الجديدة التي تم جلبها من جميع الفروع: {len(all_invoices)}")
    return all_invoices

def get_invoice_full_details(invoice_id):
    """جلب تفاصيل الفاتورة الكاملة"""
    headers = {"apikey": DAFTRA_APIKEY}
    
    try:
        # استخدام نفس طريقة جلب تفاصيل الفاتورة كما في كودك المحلي
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/{invoice_id}"
        logger.info(f"🔍 جلب تفاصيل الفاتورة {invoice_id}")
        
        data = fetch_with_retry(url, headers)
        
        if data is None:
            logger.error(f"❌ فشل في جلب تفاصيل الفاتورة {invoice_id} بعد عدة محاولات")
            return None
        
        # تسجيل القيم المالية المهمة
        logger.info(f"💰 القيم المالية للفاتورة {invoice_id}:")
        logger.info(f"   - المبلغ الإجمالي (summary_total): {data.get('summary_total')}")
        logger.info(f"   - المبلغ المدفوع (summary_paid): {data.get('summary_paid')}")
        logger.info(f"   - المبلغ غير المدفوع (summary_unpaid): {data.get('summary_unpaid')}")
        
        # البحث عن بنود الفاتورة
        invoice_items = data.get("invoice_item", [])
        if invoice_items:
            if not isinstance(invoice_items, list):
                invoice_items = [invoice_items]
            logger.info(f"✅ وجدت {len(invoice_items)} بند في الفاتورة {invoice_id}")
        else:
            logger.warning(f"⚠️ الفاتورة {invoice_id} لا تحتوي على بنود في استجابة API.")
        
        return data
            
    except Exception as e:
        logger.error(f"❌ خطأ أثناء محاولة جلب تفاصيل الفاتورة {invoice_id}: {e}", exc_info=True)
        return None

def save_invoice_complete(invoice_data):
    """حفظ الفاتورة في قاعدة البيانات Supabase"""
    try:
        invoice_id = str(invoice_data.get("id"))
        invoice_uuid = generate_uuid_from_number(invoice_id)
        
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        
        # إعداد البيانات وفقاً لهيكل جدول الفواتير في Supabase
        payload = {
            "id": invoice_uuid,
            "invoice_no": safe_string(invoice_data.get("no", "")),
            "invoice_date": safe_string(invoice_data.get("date", "")),
            "customer_id": safe_string(invoice_data.get("client_id", "")),
            "total": safe_float(invoice_data.get("summary_total", 0)),
            "summary_paid": safe_float(invoice_data.get("summary_paid", 0)),
            "summary_unpaid": safe_float(invoice_data.get("summary_unpaid", 0)),
            "branch": invoice_data.get("branch_id"),
            "client_id": safe_string(invoice_data.get("client_id", "")),
            "client_business_name": safe_string(invoice_data.get("client_business_name", ""), 255),
            "client_city": safe_string(invoice_data.get("client_city", ""))
        }
        
        # تسجيل القيم المالية للتأكد من صحتها
        logger.info(f"💰 القيم المالية التي سيتم حفظها للفاتورة {invoice_id}:")
        logger.info(f"   - المبلغ الإجمالي (total): {payload['total']}")
        logger.info(f"   - المبلغ المدفوع (summary_paid): {payload['summary_paid']}")
        logger.info(f"   - المبلغ غير المدفوع (summary_unpaid): {payload['summary_unpaid']}")
        
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
        logger.error(f"❌ خطأ في حفظ الفاتورة {invoice_data.get('id')}: {e}", exc_info=True)
        return None

def save_invoice_items_complete(invoice_uuid, invoice_id, items, client_business_name=""):
    """حفظ بنود الفاتورة في قاعدة البيانات Supabase"""
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
            
            # استخراج البيانات المطلوبة
            quantity = safe_float(item.get("quantity", 0))
            unit_price = safe_float(item.get("unit_price", 0))
            total_price = quantity * unit_price
            product_id = safe_string(item.get("product_id", ""))
            product_code = safe_string(item.get("product_code", ""))
            
            if quantity <= 0:
                logger.warning(f"⚠️ البند {i} للفاتورة {invoice_id} لديه كمية صفر أو أقل. تخطي.")
                continue
            
            # UUID للبند
            item_id = safe_string(item.get("id", ""))
            if item_id:
                item_uuid = generate_uuid_from_number(f"item-{item_id}-{invoice_id}")
            else:
                item_uuid = str(uuid.uuid4())
            
            # إعداد البيانات وفقاً لهيكل جدول بنود الفواتير في Supabase
            item_payload = {
                "id": item_uuid,
                "invoice_id": invoice_uuid,
                "product_id": product_id,
                "quantity": quantity,
                "total_price": total_price,
                "unit_price": unit_price,
                "product_code": product_code,
                "client_business_name": client_business_name
            }
            
            # تسجيل البيانات التي سيتم حفظها
            logger.info(f"💾 حفظ البند {i} للفاتورة {invoice_id}: الكمية={quantity}, السعر={unit_price}, الإجمالي={total_price}")
            
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
            logger.info("✅ لا توجد فواتير جديدة لجلبها! تم الانتهاء من المزامنة.")
            return result
        
        logger.info(f"📋 معالجة {len(all_invoices)} فاتورة جديدة...")
        
        # معالجة كل فاتورة
        for i, invoice in enumerate(all_invoices, 1):
            try:
                invoice_id = str(invoice["id"])
                
                # تقرير التقدم
                if i % 10 == 0:
                    logger.info(f"🔄 معالجة {i}/{len(all_invoices)}: الفاتورة {invoice_id}")
                
                # جلب التفاصيل الكاملة
                details = get_invoice_full_details(invoice_id)
                
                if not details:
                    logger.error(f"❌ لم يتم العثور على تفاصيل للفاتورة {invoice_id}. تخطي.")
                    continue
                
                # حفظ الفاتورة
                invoice_uuid = save_invoice_complete(details)
                
                if invoice_uuid:
                    result["invoices"] += 1
                    
                    # حفظ البنود إذا كانت متاحة
                    items = details.get("invoice_item", [])
                    if items:
                        client_business_name = safe_string(details.get("client_business_name", ""))
                        saved_items = save_invoice_items_complete(invoice_uuid, invoice_id, items, client_business_name)
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
