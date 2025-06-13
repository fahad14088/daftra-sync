import os
import requests
import time
import uuid
import logging
from datetime import datetime
import hashlib
import json

from sync_utils import get_last_sync_time, update_sync_time

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

HEADERS = {"apikey": DAFTRA_APIKEY}

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
    """تحويل آمن إلى string مع تقليم الطول."""
    s = "" if val is None else str(val).strip()
    return s[:length] if length and len(s) > length else s

def fetch_with_retry(url, headers, params=None, max_retries=3, timeout=30):
    """GET مع إعادة المحاولة."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            logger.warning(f"🔸 استجابة غير متوقعة {resp.status_code}: {resp.text}")
        except Exception as e:
            logger.warning(f"🔸 محاولة {attempt} فشلت: {e}")
        time.sleep(attempt * 2)
    return None

def check_invoice_exists(invoice_id: str) -> bool:
    """
    التحقق من وجود الفاتورة في Supabase عبر HEAD وقراءة Content-Range،
    لتجنب GROUP BY كبير في الاستعلامات.
    """
    uuid_ = generate_uuid_from_number(invoice_id)
    resp = requests.head(
        f"{SUPABASE_URL}/rest/v1/invoices",
        headers={**HEADERS, **{"Authorization": f"Bearer {SUPABASE_KEY}"}},
        params={"id": f"eq.{uuid_}"},
        timeout=30
    )
    if resp.status_code == 200:
        cr = resp.headers.get("Content-Range", "")
        total = int(cr.split("/")[-1]) if "/" in cr else 0
        return total > 0
    logger.warning(f"❌ Supabase HEAD failed ({resp.status_code}): {resp.text}")
    return False

# ----------------------------------------
# الوظائف الرئيسية
# ----------------------------------------
def get_all_invoices_complete():
    """جلب جميع الفواتير الجديدة عبر جميع الفروع والصفحات."""
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
            data = fetch_with_retry(url, HEADERS, params=params)
            if not data:
                logger.error(f"❌ فشل جلب الفرع {branch} الصفحة {page}")
                break

            invoices = data.get("data") if isinstance(data, dict) else []
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

    logger.info(f"📋 إجمالي الفواتير التي يجب معالجتها: {len(all_invoices)}")
    return all_invoices

def get_invoice_full_details(invoice_id: str):
    """جلب التفاصيل الكاملة لفاتورة واحدة."""
    url = f"{DAFTRA_URL}/v2/api/entity/invoice/{invoice_id}"
    return fetch_with_retry(url, HEADERS)

def save_invoice_complete(inv: dict) -> bool:
    """حفظ بيانات الفاتورة الأساسية في Supabase."""
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
    resp = requests.post(
        f"{SUPABASE_URL}/rest/v1/invoices",
        headers={**HEADERS, **{"Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}},
        json=payload,
        timeout=30
    )
    return resp.status_code in (200, 201, 409)

def save_invoice_items(inv_uuid: str, invoice_id: str, items, client_name="") -> int:
    """حفظ بنود الفاتورة في Supabase."""
    count = 0
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
            "total_price": qty * unit,
            "client_business_name": client_name
        }
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoice_items",
            headers={**HEADERS, **{"Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}},
            json=payload,
            timeout=30
        )
        if resp.status_code in (200, 201, 409):
            count += 1
    return count

def sync_invoices():
    """المزامنة الشاملة: جلب وحفظ الفواتير والبنود."""
    logger.info("🚀 بدء المزامنة الشاملة...")
    result = {"fetched": 0, "saved": 0}

    # قراءة آخر وقت مزامنة
    last_sync = get_last_sync_time("sales_invoices")
    try:
        last_date = datetime.fromisoformat(last_sync)
    except:
        last_date = datetime(2000, 1, 1)

    invoices = get_all_invoices_complete()
    for idx, inv in enumerate(invoices, 1):
        inv_id = str(inv.get("id", ""))
        inv_date = inv.get("date", "")
        try:
            created = datetime.fromisoformat(inv_date)
        except:
            continue
        if created <= last_date:
            continue

        result["fetched"] += 1

        details = get_invoice_full_details(inv_id) or {}
        full = {**inv, **details}

        if save_invoice_complete(full):
            result["saved"] += 1
            inv_uuid = generate_uuid_from_number(inv_id)
            items = details.get("invoice_item", [])
            save_invoice_items(inv_uuid, inv_id, items, full.get("client_business_name", ""))
        time.sleep(0.2)

    # تحديث وقت المزامنة
    update_sync_time("sales_invoices", datetime.now().isoformat())
    logger.info(f"✅ انتهت المزامنة: جُلب {result['fetched']} فاتورة، حُفظ {result['saved']} فاتورة.")
    return result

if __name__ == "__main__":
    sync_invoices()
