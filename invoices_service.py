import os
import requests
import time
import uuid
import logging
from datetime import datetime
import hashlib

# ----------------------------------------
# إعداد الـ logging
# ----------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------------------
# متغيرات البيئة و HEADERS
# ----------------------------------------
DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

# نستخدم هذا الهيدر لجميع طلبات DaFtra
DAFTRA_HEADERS = {"apikey": DAFTRA_APIKEY}

# ----------------------------------------
# دوال مساعدة
# ----------------------------------------
def generate_uuid_from_number(number: str) -> str:
    """توليد UUID ثابت من رقم الفاتورة."""
    digest = hashlib.md5(f"invoice-{number}".encode("utf-8")).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"

def safe_float(val, default=0.0):
    """تحويل آمن إلى float."""
    try:
        return float(str(val).replace(",", "")) if val not in (None, "") else default
    except:
        return default

def safe_string(val, length=None):
    """تحويل آمن إلى str مع تقليم الطول."""
    s = "" if val is None else str(val).strip()
    return s[:length] if length and len(s) > length else s

def fetch_with_retry(url, headers, params=None, max_retries=3, timeout=30):
    """GET مع إعادة المحاولة."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"🔸 Response {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.warning(f"🔸 Attempt {attempt} failed: {e}")
        time.sleep(attempt * 2)
    return None

def check_invoice_exists(invoice_id: str) -> bool:
    """
    فحص وجود الفاتورة في Supabase بطلب GET يقتصر على عمود id.
    نتجنب الـ JOINات الزائدة بوضع select=id و Prefer: count=exact.
    """
    inv_uuid = generate_uuid_from_number(invoice_id)
    url = f"{SUPABASE_URL}/rest/v1/invoices"
    params = {
        "select": "id",
        "id": f"eq.{inv_uuid}"
    }
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Prefer": "count=exact"
    }
    resp = requests.get(url, headers=headers, params=params, timeout=30)
    if resp.status_code == 200:
        cr = resp.headers.get("Content-Range", "")
        total = int(cr.split("/")[-1]) if "/" in cr else 0
        return total > 0
    logger.warning(f"❌ Supabase count failed ({resp.status_code}): {resp.text}")
    return False

# ----------------------------------------
# جلب الفواتير
# ----------------------------------------
def get_all_invoices_complete():
    all_invoices = []
    seen = set()
    branch_ids = [1, 2, 3]  # نفس قائمة الفروع في كودك المحلي

    for branch in branch_ids:
        page = 1
        while True:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list"
            params = {
                "filter[branch_id]": branch,
                "page": page,
                "limit": 100,
                "sort[id]": "desc"
            }
            data = fetch_with_retry(url, DAFTRA_HEADERS, params=params)
            if not data:
                logger.error(f"❌ Failed to fetch branch {branch} page {page}")
                break

            invoices = data.get("data") or []
            if not invoices:
                break

            for inv in invoices:
                inv_id = str(inv.get("id", ""))
                if inv_id in seen or check_invoice_exists(inv_id):
                    seen.add(inv_id)
                    continue
                all_invoices.append(inv)
                seen.add(inv_id)

            if len(invoices) < 100:
                break
            page += 1
            time.sleep(1)

    logger.info(f"📋 Total invoices to process: {len(all_invoices)}")
    return all_invoices

# ----------------------------------------
# جلب تفاصيل الفاتورة
# ----------------------------------------
def get_invoice_full_details(invoice_id: str):
    url = f"{DAFTRA_URL}/v2/api/entity/invoice/{invoice_id}"
    return fetch_with_retry(url, DAFTRA_HEADERS) or {}

# ----------------------------------------
# حفظ بيانات الفاتورة
# ----------------------------------------
def save_invoice_complete(inv: dict) -> bool:
    inv_id = str(inv.get("id", ""))
    inv_uuid = generate_uuid_from_number(inv_id)
    payload = {
        "id": inv_uuid,
        "invoice_no": safe_string(inv.get("no", "")),
        "invoice_date": safe_string(inv.get("date", "")),
        "total": safe_float(inv.get("summary_total")),
        "summary_paid": safe_float(inv.get("summary_paid")),
        "summary_unpaid": safe_float(inv.get("summary_unpaid")),
        "branch": inv.get("branch_id"),
        "client_business_name": safe_string(inv.get("client_business_name", ""), 255),
        "client_city": safe_string(inv.get("client_city", ""))
    }
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/invoices",
        headers=headers,
        json=payload,
        timeout=30
    )
    return resp.status_code in (200, 201, 409)

# ----------------------------------------
# حفظ بنود الفاتورة
# ----------------------------------------
def save_invoice_items(inv_uuid: str, invoice_id: str, items) -> int:
    count = 0
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    for itm in (items if isinstance(items, list) else [items]):
        qty = safe_float(itm.get("quantity"))
        if qty <= 0:
            continue
        unit = safe_float(itm.get("unit_price"))
        item_uuid = generate_uuid_from_number(f"item-{itm.get('id')}-{invoice_id}")
        payload = {
            "id": item_uuid,
            "invoice_id": inv_uuid,
            "product_id": safe_string(itm.get("product_id")),
            "product_code": safe_string(itm.get("product_code")),
            "quantity": qty,
            "unit_price": unit,
            "total_price": qty * unit
        }
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoice_items",
            headers=headers,
            json=payload,
            timeout=30
        )
        if resp.status_code in (200, 201, 409):
            count += 1
    return count

# ----------------------------------------
# المزامنة الرئيسية
# ----------------------------------------
def sync_invoices():
    logger.info("🚀 Starting sync...")
    invoices = get_all_invoices_complete()
    saved = 0

    for inv in invoices:
        inv_id = str(inv.get("id", ""))
        details = get_invoice_full_details(inv_id)
        full = {**inv, **details}
        if save_invoice_complete(full):
            saved += 1
            inv_uuid = generate_uuid_from_number(inv_id)
            items = details.get("invoice_item", [])
            save_invoice_items(inv_uuid, inv_id, items)
        time.sleep(0.2)

    logger.info(f"✅ Sync complete: {saved}/{len(invoices)} invoices saved.")
    return {"processed": len(invoices), "saved": saved}

if __name__ == "__main__":
    sync_invoices()
