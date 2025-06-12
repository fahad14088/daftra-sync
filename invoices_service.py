import os
import requests
import time
import logging
import hashlib

# ———————— إعداد اللوج ————————
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ———————— متغيرات البيئة ————————
BASE_URL     = os.getenv("BASE_URL").rstrip('/')      # مثال: "https://shadowpeace.daftra.com/"
DAFTRA_HEADERS = {"apikey": os.getenv("DAFTRA_APIKEY")}
SUPABASE_URL = os.getenv("SUPABASE_URL").rstrip('/')
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# ———————— دوال مساعدة ————————

def generate_uuid(s: str) -> str:
    h = hashlib.md5(s.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def safe_float(v, default=0.0):
    try:
        return default if v in (None, "") else float(str(v).replace(",", ""))
    except:
        return default

def fetch_with_retry(url, headers, params=None, retries=3, timeout=30):
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            logger.warning(f"⚠️ GET {r.url} → {r.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Exception on GET {url}: {e}")
        time.sleep((i+1)*2)
    return None

def upsert(table: str, payload: dict) -> bool:
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=id"
    headers = {
        "apikey":       SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer":       "resolution=merge-duplicates"
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code in (200, 201, 409):
        return True
    logger.error(f"❌ upsert {table} failed [{r.status_code}]: {r.text}")
    return False

def get_all_branches():
    """الفروع الثابتة لديك."""
    branches = [{"id":1},{"id":2}]
    ids = [b["id"] for b in branches]
    logger.info(f"✅ الفروع: {ids}")
    return ids

# ———————— دالة المزامنة ————————

def sync_invoices():
    limit = 20
    total_inv = total_items = 0

    for branch_id in get_all_branches():
        page = 1
        while True:
            # 1) جلب قائمة الفواتير لهذا الفرع
            url_list = f"{BASE_URL}/v2/api/entity/invoice/list/1"
            params = {"filter[branch_id]": branch_id, "page": page, "limit": limit}
            data = fetch_with_retry(url_list, DAFTRA_HEADERS, params=params)
            if not data:
                break

            inv_list = data.get("data", [])
            logger.info(f"📄 فرع {branch_id} صفحة {page}: {len(inv_list)} فاتورة")
            if not inv_list:
                break

            for inv_summary in inv_list:
                inv_id = str(inv_summary["id"])

                # 2) جلب تفاصيل الفاتورة
                url_det = f"{BASE_URL}/v2/api/entity/invoice/{inv_id}"
                det = fetch_with_retry(url_det, DAFTRA_HEADERS)
                if not det:
                    continue

                # 3) استخراج البنود
                items = det.get("invoice_item") or []
                if not isinstance(items, list):
                    items = [items] if items else []
                if not items:
                    logger.error(f"❌ فاتورة {inv_id} بدون بنود → تخطّي")
                    continue
                logger.info(f"✅ فاتورة {inv_id} تحتوي {len(items)} بنود")

                # 4) حفظ الفاتورة
                inv_uuid = generate_uuid(inv_id)
                payload_inv = {
                    "id":            inv_uuid,
                    "invoice_no":    det.get("no",""),
                    "total":         safe_float(det.get("summary_total")),
                    "invoice_date":  det.get("date",""),
                    "branch":        branch_id,
                }
                if upsert("invoices", payload_inv):
                    total_inv += 1

                    # 5) حفظ البنود
                    for it in items:
                        item_uuid = generate_uuid(f"{it.get('id')}-{inv_id}")
                        payload_it = {
                            "id":          item_uuid,
                            "invoice_id":  inv_uuid,
                            "product_id":  it.get("product_id",""),
                            "quantity":    safe_float(it.get("quantity")),
                            "unit_price":  safe_float(it.get("unit_price") or it.get("price")),
                            "total_price": safe_float(it.get("quantity")) * safe_float(it.get("unit_price") or it.get("price"))
                        }
                        if upsert("invoice_items", payload_it):
                            total_items += 1

            if len(inv_list) < limit:
                break
            page += 1
            time.sleep(0.2)

    logger.info(f"🎉 انتهى: فواتير={total_inv}, بنود={total_items}")

if __name__ == "__main__":
    sync_invoices()
