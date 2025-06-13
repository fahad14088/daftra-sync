import os
import requests
import time
import uuid
import logging
from datetime import datetime
import hashlib
import json

from config import DAFTRA_URL, DAFTRA_APIKEY, SUPABASE_URL, SUPABASE_KEY
from sync_utils import get_last_sync_time, update_sync_time

# --------------------------------------------------------------------------------
# إعداد الـ logging
# --------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --------------------------------------------------------------------------------
# دوال مساعدة
# --------------------------------------------------------------------------------
def generate_uuid_from_number(number: str) -> str:
    """توليد UUID ثابت من رقم الفاتورة."""
    digest = hashlib.md5(f"invoice-{number}".encode("utf-8")).hexdigest()
    return f"{digest[:8]}-{digest[8:12]}-{digest[12:16]}-{digest[16:20]}-{digest[20:32]}"

def safe_float(val, default=0.0):
    try:
        return float(str(val).replace(",", "")) if val not in (None, "") else default
    except:
        return default

def safe_string(val, length=None):
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
    """تجنب تكرار حفظ الفاتورة عبر HEAD والـ Content-Range."""
    uuid_ = generate_uuid_from_number(invoice_id)
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}"
    }
    resp = requests.head(
        f"{SUPABASE_URL}/rest/v1/invoices",
        headers=headers,
        params={"id": f"eq.{uuid_}"},
        timeout=30
    )
    if resp.status_code == 200:
        cr = resp.headers.get("Content-Range", "")
        total = int(cr.split("/")[-1]) if "/" in cr else 0
        return total > 0
    logger.warning(f"❌ فشل التحقق من الفاتورة {invoice_id}: {resp.status_code}")
    return False

# --------------------------------------------------------------------------------
# الدالة الرئيسية: جلب الفواتير وحفظها
# --------------------------------------------------------------------------------
def sync_invoices():
    logger.info("🚀 بدء مزامنة الفواتير...")
    result = {"fetched": 0, "saved": 0}

    # 1) نحصل على آخر تاريخ مزامنة
    last_sync = get_last_sync_time("sales_invoices")
    try:
        last_date = datetime.fromisoformat(last_sync)
        logger.info(f"⏱️ آخر مزامنة كانت في: {last_date}")
    except:
        last_date = datetime(2000, 1, 1)
        logger.info("⏱️ لا يوجد تاريخ سابق، سيتم جلب كل الفواتير.")

    headers = {"apikey": DAFTRA_APIKEY}
    page_limit = 100
    branch_ids = [1, 2, 3]  # أو استرجاعها من API إذا لزم الأمر

    for branch in branch_ids:
        page = 1
        saw_new = False

        while True:
            params = {
                "filter[branch_id]": branch,
                "page": page,
                "limit": page_limit,
                "sort[id]": "desc"
            }
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list"
            data = fetch_with_retry(url, headers, params=params)
            if not data:
                logger.error(f"❌ فشل جلب الفرع {branch} الصفحة {page}")
                break

            invoices = data.get("data") if isinstance(data, dict) else []
            if not invoices:
                logger.info(f"🏁 لا مزيد من الفواتير للفرع {branch} صفحة {page}")
                break

            for inv in invoices:
                inv_id   = str(inv.get("id", ""))
                inv_date = inv.get("date", "")
                # تحقق من النوع/التاريخ قبل التفاصيل
                try:
                    created = datetime.fromisoformat(inv_date)
                except:
                    continue
                if created <= last_date:
                    continue

                # تحقق من الوجود
                if check_invoice_exists(inv_id):
                    continue

                saw_new = True
                result["fetched"] += 1

                # جلب التفاصيل
                details = fetch_with_retry(
                    f"{DAFTRA_URL}/v2/api/entity/invoice/{inv_id}",
                    headers
                ) or {}
                # دمج الأساسيات والتفاصيل
                full = {**inv, **details}

                # حفظ الفاتورة
                inv_uuid = generate_uuid_from_number(inv_id)
                payload = {
                    "id": inv_uuid,
                    "invoice_no": safe_string(full.get("no", "")),
                    "invoice_date": full.get("date", ""),
                    "total": safe_float(full.get("summary_total")),
                    "summary_paid": safe_float(full.get("summary_paid")),
                    "summary_unpaid": safe_float(full.get("summary_unpaid")),
                    "branch": branch,
                    "client_business_name": safe_string(full.get("client_business_name", ""), 255),
                    "client_city": safe_string(full.get("client_city", ""))
                }
                # إرسال POST
                resp = requests.post(
                    f"{SUPABASE_URL}/rest/v1/invoices",
                    headers={**headers, **{"Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}},
                    json=payload,
                    timeout=30
                )
                if resp.status_code in (200, 201, 409):
                    result["saved"] += 1

                    # حفظ البنود
                    items = full.get("invoice_item") or []
                    if not isinstance(items, list):
                        items = [items]
                    for itm in items:
                        qty = safe_float(itm.get("quantity"))
                        if qty <= 0:
                            continue
                        unit = safe_float(itm.get("unit_price"))
                        item_uuid = generate_uuid_from_number(f"item-{itm.get('id')}-{inv_id}")
                        item_payload = {
                            "id": item_uuid,
                            "invoice_id": inv_uuid,
                            "product_id": safe_string(itm.get("product_id")),
                            "product_code": safe_string(itm.get("product_code")),
                            "quantity": qty,
                            "unit_price": unit,
                            "total_price": qty * unit
                        }
                        requests.post(
                            f"{SUPABASE_URL}/rest/v1/invoice_items",
                            headers={**headers, **{"Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"}},
                            json=item_payload,
                            timeout=30
                        )
                else:
                    logger.error(f"❌ فشل حفظ الفاتورة {inv_id}: {resp.text}")

                # تقييد سرعة الطلبات
                time.sleep(0.2)

            # إذا لم نجد أي فاتورة جديدة وتاريخ مزامنة قديم، نوقف
            if not saw_new and last_date > datetime(2000, 1, 1):
                break
            # إذا وصلت لنهاية الصفحات
            if len(invoices) < page_limit:
                break
            page += 1

    # تحديث وقت المزامنة
    update_sync_time("sales_invoices", datetime.now().isoformat())
    logger.info(f"✅ انتهت المزامنة: جُلب {result['fetched']} فاتورة، حُفظ {result['saved']} فاتورة.")
    return result

if __name__ == "__main__":
    sync_invoices()
