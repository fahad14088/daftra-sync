import os
import requests
import time
import logging

# إعدادات التسجيل
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# المتغيرات البيئية
BASE_URL = os.getenv("DAFTRA_URL")
API_KEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": API_KEY}
HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

EXPECTED_TYPE = 0  # نوع فاتورة مبيعات
PAGE_LIMIT = 100
BRANCH_IDS = [1, 2, 3]

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
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"محاولة {attempt} - كود الاستجابة {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.error(f"محاولة {attempt} فشلت: {e}")
        time.sleep(attempt * 2)
    return None

def get_all_invoices():
    invoices = []
    for branch in BRANCH_IDS:
        page = 1
        while True:
            url = f"{BASE_URL}/v2/api/entity/invoice/list/1"
            params = {
                "filter[branch_id]": branch,
                "page": page,
                "limit": PAGE_LIMIT
            }
            data = fetch_with_retry(url, HEADERS_DAFTRA, params=params)
            if data is None:
                logger.warning(f"⚠️ فشل في جلب البيانات للفرع {branch} الصفحة {page}، نكمل الصفحة التالية...")
                page += 1
                continue

            items = data.get("data") or []
            if not isinstance(items, list):
                items = [items]

            logger.info(f"📄 فرع {branch} - صفحة {page} فيها {len(items)} فاتورة")

            valid_items = [inv for inv in items if int(inv.get("type", -1)) == EXPECTED_TYPE]
            invoices.extend(valid_items)

            if len(items) < PAGE_LIMIT:
                break
            page += 1
            time.sleep(1.5)

    logger.info(f"📦 عدد الفواتير اللي بنعالجها: {len(invoices)}")
    return invoices

def get_invoice_details(inv_id):
    url = f"{BASE_URL}/v2/api/entity/invoice/{inv_id}"
    data = fetch_with_retry(url, HEADERS_DAFTRA)
    return data or {}

def save_invoice_and_items(inv):
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
    r1 = requests.post(f"{SUPABASE_URL}/rest/v1/invoices", headers=HEADERS_SUPABASE, json=payload)

    if r1.status_code >= 400:
        logger.error(f"❌ فشل حفظ الفاتورة {inv_id}: {r1.status_code} - {r1.text}")
        return False, 0

    items = full.get("invoice_item") or []
    count = 0
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
        r2 = requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items", headers=HEADERS_SUPABASE, json=item_payload)
        if r2.status_code >= 400:
            logger.warning(f"⚠️ فشل حفظ البند {itm.get('id')} للفاتورة {inv_id}: {r2.status_code} - {r2.text}")
            continue
        count += 1

    return True, count

def fetch_all():
    invoices = get_all_invoices()
    count_saved = 0
    count_items = 0

    for inv in invoices:
        saved, item_count = save_invoice_and_items(inv)
        if saved:
            count_saved += 1
            count_items += item_count
        time.sleep(0.2)

    logger.info(f"✅ تم حفظ {count_saved} فاتورة مبيعات جديدة.")
    return {
        "invoices": count_saved,
        "items": count_items
    }

if __name__ == "__main__":
    fetch_all()
