import os
import requests
import time
import logging
import hashlib
from datetime import datetime
from sync_utils import get_last_sync_time, update_sync_time

# ———————— إعداد اللوج ————————
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ———————— إعداد المتغيرات ————————
DAFTRA_URL    = os.getenv("BASE_URL").rstrip("/")            # مثال: "https://shadowpeace.daftra.com/"
DAFTRA_HEADERS = {"apikey": os.getenv("DAFTRA_APIKEY")}      # أو استعمل HEADERS من config.py
SUPABASE_URL  = os.getenv("SUPABASE_URL").rstrip("/")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

# ———————— دوال مساعدة ————————

def generate_uuid_from_number(number: str) -> str:
    """توليد UUID ثابت من رقم الفاتورة."""
    h = hashlib.md5(number.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def safe_float(v, default=0.0):
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", ""))
    except:
        return default

def fetch_with_retry(url, headers, params=None, max_retries=3, timeout=30):
    """GET مع إعادة محاولة تلقائية."""
    for i in range(max_retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            logger.warning(f"⚠️ GET {r.url} → {r.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ exception on GET {url}: {e}")
        # تأخير قبل إعادة المحاولة
        time.sleep((i+1)*2)
    return None

def upsert(table: str, payload: dict) -> bool:
    """INSERT أو UPDATE في Supabase REST مع on_conflict=id."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=id"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates"
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code in (200,201,409):
        return True
    logger.error(f"❌ upsert {table} failed [{r.status_code}]: {r.text}")
    return False

def get_all_branches():
    """قائمة الفروع الثابتة كما في برنامجك المحلي."""
    branches = [
        {"id": 1, "name": "Main"},
        {"id": 2, "name": "العويضة"}
    ]
    ids = [b["id"] for b in branches]
    logger.info(f"✅ استخدام الفروع: {ids}")
    return ids

# ———————— الدالة الرئيسية ————————

def sync_invoices_to_supabase():
    # تحميل آخر وقت تزامن
    last_sync_str = get_last_sync_time("sales_invoices")
    try:
        last_sync = datetime.fromisoformat(last_sync_str)
    except:
        last_sync = datetime(2000,1,1)
    logger.info(f"🧠 آخر تزامن: {last_sync}")

    inserted_invoices = 0
    inserted_items    = 0
    limit = 20

    for branch_id in get_all_branches():
        page = 1
        while True:
            # 1) جلب قائمة الفواتير مع فلتر branch_id
            url_list = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1"
            params = {
                "filter[branch_id]": branch_id,
                "page": page,
                "limit": limit
            }
            data = fetch_with_retry(url_list, DAFTRA_HEADERS, params=params)
            if not data:
                break

            inv_list = data.get("data", [])
            if not inv_list:
                break

            logger.info(f"📄 فرع {branch_id} صفحة {page}: {len(inv_list)} فواتير")

            for inv_summary in inv_list:
                inv_id   = str(inv_summary["id"])
                inv_type = int(inv_summary.get("type", -1))
                inv_no   = inv_summary.get("no","")
                inv_date_str = inv_summary.get("date","")
                try:
                    inv_date = datetime.fromisoformat(inv_date_str)
                except:
                    continue
                # فلترة حسب النوع والتاريخ
                if inv_type != 0 or inv_date <= last_sync:
                    continue

                # 2) جلب تفاصيل الفاتورة
                url_det = f"{DAFTRA_URL}/v2/api/entity/invoice/{inv_id}"
                det = fetch_with_retry(url_det, DAFTRA_HEADERS)
                if not det:
                    continue

                # 3) استخراج البنود
                items = det.get("invoice_item") or []
                if not isinstance(items, list):
                    items = [items]
                if len(items) == 0:
                    logger.error(f"❌ فاتورة {inv_id} بدون بنود → تخطّي")
                    continue
                logger.info(f"✅ فاتورة {inv_id} تحتوي {len(items)} بنود")

                # 4) حفظ الفاتورة في Supabase
                inv_uuid = generate_uuid_from_number(inv_id)
                total_amount = safe_float(det.get("summary_total"))
                payload_inv = {
                    "id":             inv_uuid,
                    "created_at":     inv_date_str,
                    "invoice_type":   inv_type,
                    "branch":         branch_id,
                    "store":          inv_summary.get("store_id", branch_id),
                    "invoice_no":     inv_no,
                    "total":          total_amount
                }
                if upsert("invoices", payload_inv):
                    inserted_invoices += 1

                    # 5) حفظ البنود في Supabase
                    for it in items:
                        item_uuid = generate_uuid_from_number(f"{it.get('id')}-{inv_id}")
                        payload_it = {
                            "id":          item_uuid,
                            "invoice_id":  inv_uuid,
                            "product_id":  it.get("product_id",""),
                            "quantity":    safe_float(it.get("quantity")),
                            "unit_price":  safe_float(it.get("unit_price", it.get("price"))),
                            "total_price": safe_float(it.get("quantity")) 
                                           * safe_float(it.get("unit_price", it.get("price")))
                        }
                        if upsert("invoice_items", payload_it):
                            inserted_items += 1

            if len(inv_list) < limit:
                break
            page += 1

    # تحديث وقت التزامن
    update_sync_time("sales_invoices", datetime.now().isoformat())
    logger.info(f"🎉 انتهى التزامن: فواتير {inserted_invoices}, بنود {inserted_items}")

if __name__ == "__main__":
    sync_invoices_to_supabase()
