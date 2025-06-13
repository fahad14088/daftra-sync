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
logging.basicConfig(level=logging.DEBUG, format=r'%(asctime)s - %(levelname)s - %(message)s')
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
    branches = [1, 2, 3]
    logger.info(f"✅ استخدام الفروع المحددة: {branches}")
    return branches

def fetch_with_retry(url, headers, max_retries=3, timeout=30, params=None):
    """محاولة جلب البيانات مع إعادة المحاولة في حالة فشل الاتصال"""
    for retry in range(max_retries):
        try:
            logger.info(f"🔄 محاولة جلب البيانات من: {url} مع params={params}")
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            text = response.text
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"⚠️ استجابة {response.status_code}: {text}")
                if retry < max_retries - 1:
                    time.sleep((retry + 1) * 5)
                    continue

        except requests.exceptions.Timeout:
            logger.warning("⚠️ انتهت مهلة الاتصال")
            if retry < max_retries - 1:
                time.sleep((retry + 1) * 5)
                continue

        except Exception as e:
            logger.error(f"❌ خطأ غير متوقع: {e}", exc_info=True)
            if retry < max_retries - 1:
                time.sleep((retry + 1) * 5)
                continue

    return None

def check_invoice_exists(invoice_id):
    """التحقق مما إذا كانت الفاتورة موجودة بالفعل في قاعدة البيانات"""
    try:
        invoice_uuid = generate_uuid_from_number(invoice_id)
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}"
        }
        resp = requests.get(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=headers,
            params={"id": f"eq.{invoice_uuid}"},
            timeout=30
        )
        if resp.status_code == 200:
            return len(resp.json()) > 0
        else:
            logger.warning(f"⚠️ فشل التحقق من الفاتورة {invoice_id}: {resp.text}")
            return False
    except Exception as e:
        logger.error(f"❌ خطأ في التحقق من الفاتورة {invoice_id}: {e}", exc_info=True)
        return False

def get_all_invoices_complete():
    """جلب جميع الفواتير من جميع الصفحات ولجميع الفروع المعروفة"""
    logger.info("📥 جلب جميع الفواتير...")
    
    headers = {"apikey": DAFTRA_APIKEY}
    all_invoices = []
    processed_ids = set()
    branches = get_all_branches()
    
    for branch_id in branches:
        logger.info(f"🔄 جلب الفواتير من الفرع: {branch_id}...")
        page = 1
        limit = 100
        
        while True:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list"
            params = {
                "filter[branch_id]": branch_id,
                "page": page,
                "limit": limit,
                "sort[id]": "desc"
            }
            try:
                data = fetch_with_retry(url, headers, params=params)
                if data is None:
                    logger.error(f"❌ فشل استرجاع الصفحة {page} للفرع {branch_id}")
                    break

                logger.debug(f"🔍 استجابة كاملة: {json.dumps(data, ensure_ascii=False)}")

                if isinstance(data, dict) and "data" in data:
                    invoices = data["data"]
                elif isinstance(data, list):
                    invoices = data
                else:
                    invoices = next((v for v in data.values() if isinstance(v, list)), [])

                if not invoices:
                    logger.info(f"✅ لا توجد فواتير في الصفحة {page} للفرع {branch_id}.")
                    break

                logger.info(f"📊 وجدت {len(invoices)} فاتورة في الصفحة {page}")
                for inv in invoices:
                    inv_id = str(inv.get("id"))
                    if inv_id in processed_ids:
                        continue
                    if check_invoice_exists(inv_id):
                        processed_ids.add(inv_id)
                        continue
                    all_invoices.append(inv)
                    processed_ids.add(inv_id)

                if len(invoices) < limit:
                    break
                page += 1
                time.sleep(1)

            except Exception as e:
                logger.error(f"❌ خطأ في جلب الفواتير فرع {branch_id} صفحة {page}: {e}", exc_info=True)
                break
    
    logger.info(f"📋 إجمالي الفواتير الجديدة: {len(all_invoices)}")
    return all_invoices

def get_invoice_full_details(invoice_id):
    """جلب تفاصيل الفاتورة الكاملة"""
    headers = {"apikey": DAFTRA_APIKEY}
    try:
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/{invoice_id}"
        logger.info(f"🔍 جلب تفاصيل الفاتورة {invoice_id}")
        data = fetch_with_retry(url, headers)
        if data is None:
            logger.error(f"❌ فشل جلب تفاصيل الفاتورة {invoice_id}")
            return None

        logger.info(f"""💰 الفاتورة {invoice_id}:
   - summary_total: {data.get('summary_total')}
   - summary_paid: {data.get('summary_paid')}
   - summary_unpaid: {data.get('summary_unpaid')}""")

        items = data.get("invoice_item") or []
        if not isinstance(items, list):
            items = [items]
        logger.info(f"✅ {len(items)} بند في الفاتورة {invoice_id}")
        return data

    except Exception as e:
        logger.error(f"❌ خطأ جلب تفاصيل الفاتورة {invoice_id}: {e}", exc_info=True)
        return None

def save_invoice_complete(invoice_data):
    """حفظ الفاتورة في قاعدة بيانات Supabase"""
    try:
        invoice_id = str(invoice_data.get("id"))
        invoice_uuid = generate_uuid_from_number(invoice_id)
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json"
        }
        payload = {
            "id": invoice_uuid,
            "invoice_no": safe_string(invoice_data.get("no", "")),
            "invoice_date": safe_string(invoice_data.get("date", "")),
            "total": safe_float(invoice_data.get("summary_total", 0)),
            "summary_paid": safe_float(invoice_data.get("summary_paid", 0)),
            "summary_unpaid": safe_float(invoice_data.get("summary_unpaid", 0)),
            "branch": invoice_data.get("branch_id"),
            "client_business_name": safe_string(invoice_data.get("client_business_name", ""), 255),
            "client_city": safe_string(invoice_data.get("client_city", ""))
        }
        clean_payload = {k: v for k, v in payload.items() if v not in (None, "", "None")}
        response = requests.post(f"{SUPABASE_URL}/rest/v1/invoices", headers=headers, json=clean_payload, timeout=30)
        if response.status_code in (200, 201, 409):
            logger.info(f"✅ حفظ الفاتورة {invoice_id}")
            return invoice_uuid
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}: {response.text}")
            return None
    except Exception as e:
        logger.error(f"❌ خطأ حفظ الفاتورة {invoice_data.get('id')}: {e}", exc_info=True)
        return None

def save_invoice_items_complete(invoice_uuid, invoice_id, items, client_business_name=""):
    """حفظ بنود الفاتورة في Supabase"""
    if not items:
        logger.warning(f"⚠️ لا توجد بنود للفاتورة {invoice_id}")
        return 0
    if not isinstance(items, list):
        items = [items]
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    saved = 0
    for idx, item in enumerate(items, 1):
        try:
            quantity = safe_float(item.get("quantity", 0))
            unit_price = safe_float(item.get("unit_price", 0))
            total_price = quantity * unit_price
            prod_id = safe_string(item.get("product_id", ""))
            code = safe_string(item.get("product_code", ""))
            if quantity <= 0:
                continue
            item_uuid = generate_uuid_from_number(f"item-{item.get('id')}-{invoice_id}") if item.get("id") else str(uuid.uuid4())
            payload = {
                "id": item_uuid,
                "invoice_id": invoice_uuid,
                "product_id": prod_id,
                "product_code": code,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_price": total_price,
                "client_business_name": client_business_name
            }
            resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items", headers=headers, json=payload, timeout=30)
            if resp.status_code in (200, 201, 409):
                saved += 1
                logger.debug(f"✅ حفظ بند {idx} للفاتورة {invoice_id}")
            else:
                logger.error(f"❌ فشل حفظ بند {idx}: {resp.text}")
        except Exception as e:
            logger.error(f"❌ خطأ بند {idx} للفاتورة {invoice_id}: {e}", exc_info=True)
    logger.info(f"✅ {saved} بنود حفظت للفاتورة {invoice_id}")
    return saved

def sync_invoices():
    """المزامنة الشاملة النهائية"""
    logger.info("🚀 بدء المزامنة...")
    result = {"invoices": 0, "items": 0, "errors": []}
    try:
        all_invs = get_all_invoices_complete()
        if not all_invs:
            logger.error("❌ لا توجد فواتير!")
            return result
        for idx, inv in enumerate(all_invs, 1):
            inv_id = str(inv.get("id"))
            if idx % 10 == 0:
                logger.info(f"🔄 معالجة {idx}/{len(all_invs)}: {inv_id}")
            details = get_invoice_full_details(inv_id) or {}
            full = inv.copy()
            full.update(details)
            uuid_inv = save_invoice_complete(full)
            if uuid_inv:
                result["invoices"] += 1
                items = details.get("invoice_item", [])
                saved = save_invoice_items_complete(uuid_inv, inv_id, items, full.get("client_business_name", ""))
                result["items"] += saved
            else:
                result["errors"].append(f"حفظ الفاتورة {inv_id} فشل")
            if idx % 50 == 0:
                time.sleep(2)
        logger.info("🎯 المزامنة انتهت:")
        logger.info(f"🔹 فواتير محفوظة: {result['invoices']}")
        logger.info(f"🔹 بنود محفوظة: {result['items']}")
        logger.info(f"🔹 أخطاء: {len(result['errors'])}")
        return result
    except Exception as e:
        logger.error(f"❌ خطأ عام: {e}", exc_info=True)
        return result

if __name__ == "__main__":
    sync_invoices()
