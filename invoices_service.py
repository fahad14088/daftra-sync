import os
import requests
import time
import uuid
import logging
from datetime import datetime
import hashlib
import json
import traceback

# تفعيل الـ DEBUG لرؤية كامل الـ logs
logging.basicConfig(level=logging.DEBUG, format=r'%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# المتغيرات البيئية
DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number):
    """توليد UUID من رقم معرف الفاتورة"""
    hash_input = f"invoice-{number}".encode("utf-8")
    digest = hashlib.md5(hash_input).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"

def safe_float(value, default=0.0):
    """تحويل آمن إلى float"""
    try:
        if value in (None, ""):
            return default
        return float(str(value).replace(",", ""))
    except:
        return default

def safe_string(value, max_length=None):
    """تحويل آمن إلى str مع تقليم الطول"""
    if value is None:
        return ""
    s = str(value).strip()
    return s[:max_length] if max_length and len(s) > max_length else s

def get_all_branches():
    """قائمة الفروع الثابتة"""
    return [1, 2, 3]

def fetch_with_retry(url, headers, params=None, max_retries=3, timeout=30):
    """GET مع إعادة المحاولة"""
    for attempt in range(max_retries):
        try:
            logger.debug(f"GET {url} params={params}")
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Response {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.warning(f"Attempt {attempt+1} failed: {e}")
        time.sleep((attempt + 1) * 2)
    return None

def check_invoice_exists(invoice_id):
    """التحقق من وجود الفاتورة باستخدام HEAD لتجنب GROUP BY"""
    invoice_uuid = generate_uuid_from_number(invoice_id)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    params = {
        "id": f"eq.{invoice_uuid}"
    }
    # نستخدم HEAD للحصول فقط على Content-Range
    resp = requests.head(f"{SUPABASE_URL}/rest/v1/invoices", headers=headers, params=params, timeout=30)
    if resp.status_code == 200:
        cr = resp.headers.get("Content-Range", "")
        total = int(cr.split("/")[-1]) if "/" in cr else 0
        return total > 0
    logger.warning(f"Supabase HEAD failed ({resp.status_code}): {resp.text}")
    return False

def get_all_invoices_complete():
    """جلب جميع الفواتير الجديدة عبر جميع الصفحات والفروع"""
    headers = {"apikey": DAFTRA_APIKEY}
    all_invoices = []
    seen = set()
    for branch in get_all_branches():
        page = 1
        while True:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list"
            params = {
                "filter[branch_id]": branch,
                "page": page,
                "limit": 100,
                "sort[id]": "desc"
            }
            data = fetch_with_retry(url, headers, params=params)
            if not data:
                break

            # استخراج القائمة بمرونة
            if isinstance(data, dict) and "data" in data:
                invoices = data["data"]
            elif isinstance(data, list):
                invoices = data
            else:
                invoices = next((v for v in data.values() if isinstance(v, list)), [])

            if not invoices:
                break

            for inv in invoices:
                inv_id = str(inv.get("id"))
                if inv_id in seen or check_invoice_exists(inv_id):
                    seen.add(inv_id)
                    continue
                all_invoices.append(inv)
                seen.add(inv_id)

            if len(invoices) < 100:
                break
            page += 1
            time.sleep(1)

    logger.info(f"📋 إجمالي الفواتير الجديدة: {len(all_invoices)}")
    return all_invoices

def get_invoice_full_details(invoice_id):
    """جلب تفاصيل فاتورة واحدة"""
    headers = {"apikey": DAFTRA_APIKEY}
    url = f"{DAFTRA_URL}/v2/api/entity/invoice/{invoice_id}"
    return fetch_with_retry(url, headers)

def save_invoice_complete(inv):
    """حفظ البيانات الأساسية للفاتورة في Supabase"""
    invoice_id = str(inv["id"])
    inv_uuid = generate_uuid_from_number(invoice_id)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "id": inv_uuid,
        "invoice_no": safe_string(inv.get("no", "")),
        "invoice_date": safe_string(inv.get("date", "")),
        "total": safe_float(inv.get("summary_total", 0)),
        "summary_paid": safe_float(inv.get("summary_paid", 0)),
        "summary_unpaid": safe_float(inv.get("summary_unpaid", 0)),
        "branch": inv.get("branch_id"),
        "client_business_name": safe_string(inv.get("client_business_name", ""), 255),
        "client_city": safe_string(inv.get("client_city", ""))
    }
    clean = {k: v for k, v in payload.items() if v not in (None, "", "None")}
    resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoices", headers=headers, json=clean, timeout=30)
    return resp.status_code in (200, 201, 409)

def save_invoice_items(inv_uuid, invoice_id, items, client_name=""):
    """حفظ بنود الفاتورة في Supabase"""
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    count = 0
    for item in (items if isinstance(items, list) else [items]):
        qty = safe_float(item.get("quantity", 0))
        if qty <= 0:
            continue
        unit = safe_float(item.get("unit_price", 0))
        item_uuid = generate_uuid_from_number(f"item-{item.get('id')}-{invoice_id}")
        payload = {
            "id": item_uuid,
            "invoice_id": inv_uuid,
            "product_id": safe_string(item.get("product_id", "")),
            "product_code": safe_string(item.get("product_code", "")),
            "quantity": qty,
            "unit_price": unit,
            "total_price": qty * unit,
            "client_business_name": client_name
        }
        resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items", headers=headers, json=payload, timeout=30)
        if resp.status_code in (200, 201, 409):
            count += 1
    return count

def sync_invoices():
    """تشغيل المزامنة الشاملة"""
    logger.info("🚀 بدء المزامنة...")
    result = {"invoices": 0, "items": 0, "errors": []}

    invoices = get_all_invoices_complete()
    if not invoices:
        logger.error("❌ لا توجد فواتير جديدة!")
        return result

    for idx, inv in enumerate(invoices, 1):
        inv_id = str(inv["id"])
        if idx % 10 == 0:
            logger.info(f"🔄 [{idx}/{len(invoices)}] معالجة {inv_id}")
        details = get_invoice_full_details(inv_id) or {}
        full = {**inv, **details}
        if save_invoice_complete(full):
            result["invoices"] += 1
            items = details.get("invoice_item", [])
            result["items"] += save_invoice_items(generate_uuid_from_number(inv_id), inv_id, items, inv.get("client_business_name", ""))
        else:
            result["errors"].append(f"حفظ الفاتورة {inv_id} فشل")
        if idx % 50 == 0:
            time.sleep(2)

    logger.info(f"✅ فواتير محفوظة: {result['invoices']}, بنود محفوظة: {result['items']}, أخطاء: {len(result['errors'])}")
    return result

if __name__ == "__main__":
    sync_invoices()
