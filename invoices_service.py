import os
import requests
import time
import logging
import hashlib
from datetime import datetime

# ----------------------------------------
# إعداد الـ logging
# ----------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ----------------------------------------
# متغيرات البيئة
# ----------------------------------------
BASE_URL      = os.getenv("DAFTRA_URL", "https://shadowpeace.daftra.com")
API_KEY       = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
LAST_SYNC_ENV = os.getenv("LAST_SYNC", "2000-01-01T00:00:00")

HEADERS_DAFTRA   = {"apikey": API_KEY}
HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

EXPECTED_TYPE = 0  # فواتير المبيعات
PAGE_LIMIT    = 100
BRANCH_IDS    = [1, 2, 3]

# ----------------------------------------
# دوال مساعدة
# ----------------------------------------

def safe_float(val, default=0.0):
    try:
        return float(str(val).replace(",", "")) if val not in (None, "") else default
    except:
        return default


def safe_string(val, length=None):
    s = "" if val is None else str(val).strip()
    return s[:length] if length and len(s) > length else s


def fetch_with_retry(url, headers, params=None, max_retries=3, timeout=30):
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            logger.debug(f"Request URL: {resp.request.url}")
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"Attempt {attempt} - status {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"Attempt {attempt} failed: {e}")
        time.sleep(attempt * 2)
    return None

# ----------------------------------------
# جلب قائمة الفواتير (صفحة بكل طلب)
# ----------------------------------------
def get_all_invoices(last_sync: datetime):
    invoices = []

    for branch in BRANCH_IDS:
        page = 1
        while True:
            url = f"{BASE_URL}/v2/api/entity/invoice/list/1"
            params = {"filter[branch_id]": branch, "page": page, "limit": PAGE_LIMIT}
            data = fetch_with_retry(url, HEADERS_DAFTRA, params=params)
            if not data:
                logger.error(f"Failed fetch branch {branch} page {page}")
                break

            items = data.get("data") or []
            if not items:
                break

            new_found = False
            for inv in items:
                try:
                    inv_date = datetime.fromisoformat(inv.get("date"))
                    inv_type = int(inv.get("type", -1))
                except:
                    continue

                if inv_type != EXPECTED_TYPE or inv_date <= last_sync:
                    continue
                new_found = True
                invoices.append(inv)

            if not new_found or len(items) < PAGE_LIMIT:
                break
            page += 1
            time.sleep(1)

    logger.info(f"Invoices to process: {len(invoices)}")
    return invoices

# ----------------------------------------
# جلب تفاصيل فاتورة
# ----------------------------------------
def get_invoice_details(inv_id: str):
    url = f"{BASE_URL}/v2/api/entity/invoice/{inv_id}"
    data = fetch_with_retry(url, HEADERS_DAFTRA)
    return data or {}

# ----------------------------------------
# حفظ فاتورة وبنودها في Supabase
# ----------------------------------------
def save_invoice_and_items(inv: dict):
    inv_id = str(inv.get("id"))
    details = get_invoice_details(inv_id)
    full = {**inv, **details}

    payload = {
        "id": inv_id,
        "invoice_no": safe_string(full.get("no")),
        "invoice_date": safe_string(full.get("date")),
        "total": safe_float(full.get("summary_total")),
        "summary_paid": safe_float(full.get("summary_paid")),
        "summary_unpaid": safe_float(full.get("summary_unpaid")),
        "branch": full.get("branch_id"),
        "client_business_name": safe_string(full.get("client_business_name"), 255),
        "client_city": safe_string(full.get("client_city"))
    }
    resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoices", headers=HEADERS_SUPABASE, json=payload)
    logger.debug(f"Invoice save {inv_id}: {resp.status_code}")

    items = full.get("invoice_item") or []
    for itm in (items if isinstance(items, list) else [items]):
        qty = safe_float(itm.get("quantity"))
        if qty <= 0:
            continue
        item_payload = {
            "id": f"{inv_id}-{itm.get('id')}",
            "invoice_id": inv_id,
            "product_id": safe_string(itm.get("product_id")),
            "product_code": safe_string(itm.get("product_code")),
            "quantity": qty,
            "unit_price": safe_float(itm.get("unit_price")),
            "total_price": qty * safe_float(itm.get("unit_price"))
        }
        item_resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items", headers=HEADERS_SUPABASE, json=item_payload)
        logger.debug(f"Item save {inv_id}-{itm.get('id')}: {item_resp.status_code}")

# ----------------------------------------
# المزامنة الرئيسية
# ----------------------------------------
def sync_invoices():
    try:
        last_sync = datetime.fromisoformat(LAST_SYNC_ENV)
    except:
        last_sync = datetime(2000, 1, 1)

    invoices = get_all_invoices(last_sync)
    for inv in invoices:
        save_invoice_and_items(inv)
        time.sleep(0.2)

    # تحديث LAST_SYNC في البيئة/ملف
    now_iso = datetime.now().isoformat()
    logger.info(f"Sync done. Updated LAST_SYNC to {now_iso}")
    # هنا يمكنك تخزين now_iso في مكان دائم

if __name__ == "__main__":
    sync_invoices()
