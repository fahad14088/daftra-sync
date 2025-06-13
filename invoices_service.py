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
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------------------
# متغيرات البيئة
# ----------------------------------------
BASE_URL         = os.getenv("DAFTRA_URL", "https://shadowpeace.daftra.com")
DAFTRA_APIKEY    = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL     = os.getenv("SUPABASE_URL")
SUPABASE_KEY     = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA   = {"apikey": DAFTRA_APIKEY}
HEADERS_SUPABASE = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}

# ----------------------------------------
# دوال مساعدة
# ----------------------------------------

def generate_uuid_from_number(number: str) -> str:
    digest = hashlib.md5(f"invoice-{number}".encode()).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"


def safe_float(val, default=0.0):
    try:
        return float(str(val).replace(",", "")) if val not in (None, "") else default
    except Exception:
        return default


def safe_string(val, length=None):
    s = "" if val is None else str(val).strip()
    return s[:length] if length and len(s) > length else s


def fetch_with_retry(url, headers, params=None, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            logger.debug(f"Request URL: {resp.request.url}")
            logger.debug(f"Status: {resp.status_code}")
            logger.debug(f"Headers: {resp.headers}")
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Attempt {attempt} - unexpected status {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"Attempt {attempt} failed: {e}")
        time.sleep(attempt * 2)
    return None

# ----------------------------------------
# فحص وجود الفاتورة لتجنب التكرار
# ----------------------------------------
def check_invoice_exists(invoice_id: str) -> bool:
    inv_uuid = generate_uuid_from_number(invoice_id)
    url = f"{SUPABASE_URL}/rest/v1/invoices"
    params = {"select": "id", "id": f"eq.{inv_uuid}", "limit": 1}
    try:
        resp = requests.get(url, headers={**HEADERS_SUPABASE, "Prefer": "count=exact"}, params=params, timeout=30)
        logger.debug(f"Check URL: {resp.request.url}")
        logger.debug(f"Check Headers: {resp.headers}")
        if resp.status_code == 200:
            cr = resp.headers.get("Content-Range", "")
            total = int(cr.split("/")[-1]) if "/" in cr else 0
            return total > 0
        logger.warning(f"Check failed {resp.status_code}: {resp.text}")
    except Exception as e:
        logger.error(f"Error checking invoice exists: {e}")
    return False

# ----------------------------------------
# جلب جميع الفواتير
# ----------------------------------------
def get_all_invoices_complete():
    all_invoices = []
    seen = set()
    branch_ids = [1, 2, 3]

    for branch in branch_ids:
        page = 1
        while True:
            url = f"{BASE_URL}/v2/api/entity/invoice/list/1"
            params = {"filter[branch_id]": branch, "page": page, "limit": 100}
            data = fetch_with_retry(url, HEADERS_DAFTRA, params=params)
            if data is None:
                logger.error(f"Failed to fetch invoices for branch {branch}, page {page}")
                break
            invoices = data.get("data") or []
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
    logger.info(f"Total to process: {len(all_invoices)}")
    return all_invoices

# ----------------------------------------
# جلب تفاصيل فاتورة
# ----------------------------------------
def get_invoice_full_details(invoice_id: str):
    url = f"{BASE_URL}/v2/api/entity/invoice/{invoice_id}"
    return fetch_with_retry(url, HEADERS_DAFTRA) or {}

# ----------------------------------------
# حفظ فاتورة
# ----------------------------------------
def save_invoice_complete(inv: dict) -> bool:
    inv_id = str(inv.get("id"))
    inv_uuid = generate_uuid_from_number(inv_id)
    payload = {
        "id": inv_uuid,
        "invoice_no": safe_string(inv.get("no")),
        "invoice_date": safe_string(inv.get("date")),
        "total": safe_float(inv.get("summary_total")),
        "summary_paid": safe_float(inv.get("summary_paid")),
        "summary_unpaid": safe_float(inv.get("summary_unpaid")),
        "branch": inv.get("branch_id"),
        "client_business_name": safe_string(inv.get("client_business_name"), 255),
        "client_city": safe_string(inv.get("client_city"))
    }
    resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoices", headers=HEADERS_SUPABASE, json=payload)
    logger.debug(f"Save invoice status: {resp.status_code}, text: {resp.text}")
    return resp.status_code in (200, 201, 409)

# ----------------------------------------
# حفظ بنود الفاتورة
# ----------------------------------------
def save_invoice_items(inv_uuid: str, invoice_id: str, items) -> int:
    count = 0
    for itm in (items if isinstance(items, list) else [items]):
        qty = safe_float(itm.get("quantity"))
        if qty <= 0:
            continue
        unit = safe_float(itm.get("unit_price"))
        item_uuid = generate_uuid_from_number(f"item-{itm.get('id')}-{invoice_id}")
        payload = {"id": item_uuid, "invoice_id": inv_uuid,
                   "product_id": safe_string(itm.get("product_id")),
                   "product_code": safe_string(itm.get("product_code")),
                   "quantity": qty, "unit_price": unit,
                   "total_price": qty * unit}
        resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items", headers=HEADERS_SUPABASE, json=payload)
        logger.debug(f"Save item status: {resp.status_code}, text: {resp.text}")
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
        inv_id = str(inv.get("id"))
        details = get_invoice_full_details(inv_id)
        full = {**inv, **details}
        if save_invoice_complete(full):
            saved += 1
            inv_uuid = generate_uuid_from_number(inv_id)
            save_invoice_items(inv_uuid, inv_id, full.get("invoice_item", []))
        time.sleep(0.2)
    logger.info(f"✅ Sync complete: {saved}/{len(invoices)} saved.")

if __name__ == "__main__":
    sync_invoices()
