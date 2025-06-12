import os
import requests
import time
import logging
import hashlib

# ———————— إعداد اللوج ————————
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ———————— متغيرات البيئة مع fallback ————————
DAFTRA_URL     = os.getenv("BASE_URL") or os.getenv("DAFTRA_URL") or ""
BASE_URL       = DAFTRA_URL.rstrip('/')
DAFTRA_APIKEY  = os.getenv("DAFTRA_APIKEY", "")
DAFTRA_HEADERS = {"apikey": DAFTRA_APIKEY}
SUPABASE_URL   = os.getenv("SUPABASE_URL", "").rstrip('/')
SUPABASE_KEY   = os.getenv("SUPABASE_KEY", "")

# ———————— دوال مساعدة ————————

def generate_uuid(s: str) -> str:
    """توليد UUID ثابت بناءً على نص."""
    h = hashlib.md5(s.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def safe_float(v, default=0.0):
    """تحويل آمن إلى float."""
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", ""))
    except:
        return default

def fetch_with_retry(url, headers, params=None, retries=3, timeout=30):
    """GET مع إعادة محاولة تلقائية."""
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            logger.warning(f"⚠️ GET {r.url} → {r.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Exception on GET {url}: {e}")
        time.sleep((i + 1) * 2)
    return None

def upsert(table: str, payload: dict) -> bool:
    """INSERT أو UPDATE في Supabase REST باستخدام on_conflict=id."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=id"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates"
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code in (200, 201, 409):
        return True
    logger.error(f"❌ upsert {table} failed [{r.status_code}]: {r.text}")
    return False

def get_all_branches():
    """قائمة الفروع الثابتة لديك."""
    return [1, 2]

def fetch_invoice_items(inv_id: str, branch: int) -> list:
    """
    يجلب جميع بنود الفاتورة inv_id من فرع branch.
    يعيد قائمة dict لكل بند.
    """
    items = []
    page = 1
    limit = 100

    while True:
        url = f"{BASE_URL}/v2/api/entity/invoice-item/list/{branch}"
        params = {
            "filter[invoice_id]": inv_id,
            "page": page,
            "limit": limit
        }
        data = fetch_with_retry(url, DAFTRA_HEADERS, params=params)
        if not data:
            break

        batch = data.get("data", [])
        if not batch:
            break

        items.extend(batch)
        if len(batch) < limit:
            break
        page += 1

    return items

def sync_invoices():
    """المزامنة الرئيسية للفواتير والبنود إلى Supabase."""
    limit = 100
    tot_inv = 0
    tot_items = 0

    for branch_id in get_all_branches():
        page = 1
        while True:
            # 1) جلب ملخص الفواتير للفرع
            url_list = f"{BASE_URL}/v2/api/entity/invoice/list/1"
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

            logger.info(f"📄 فرع {branch_id} صفحة {page}: {len(inv_list)} فاتورات")

            for inv in inv_list:
                inv_id = str(inv.get("id"))
                # 2) جلب البنود من endpoint مخصَّص
                items = fetch_invoice_items(inv_id, branch_id)

                if not items:
                    logger.error(f"❌ فاتورة {inv_id} بدون بنود → BUG في جلب البيانات")
                    continue

                # 3) حفظ الفاتورة
                inv_uuid = generate_uuid(inv_id)
                payload_inv = {
                    "id":            inv_uuid,
                    "invoice_no":    inv.get("no", ""),
                    "total":         safe_float(inv.get("summary_total") or inv.get("total")),
                    "invoice_date":  inv.get("date", ""),
                    "branch":        branch_id,
                }
                if upsert("invoices", payload_inv):
                    tot_inv += 1

                    # 4) حفظ البنود
                    for it in items:
                        item_uuid = generate_uuid(f"{it.get('id')}-{inv_id}")
                        payload_it = {
                            "id":           item_uuid,
                            "invoice_id":   inv_uuid,
                            "product_id":   it.get("product_id", ""),
                            "quantity":     safe_float(it.get("quantity")),
                            "unit_price":   safe_float(it.get("unit_price") or it.get("price")),
                            "total_price":  safe_float(it.get("quantity")) *
                                            safe_float(it.get("unit_price") or it.get("price"))
                        }
                        if upsert("invoice_items", payload_it):
                            tot_items += 1

            if len(inv_list) < limit:
                break
            page += 1
            time.sleep(0.1)

    logger.info(f"🎉 انتهى التزامن: فواتير={tot_inv}, بنود={tot_items}")

if __name__ == "__main__":
    sync_invoices()
