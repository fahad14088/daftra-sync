import os
import requests
import time
import logging
import hashlib

# تفعيل اللوج
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DAFTRA_URL    = os.getenv("DAFTRA_URL").rstrip('/')
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL").rstrip('/')
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

def generate_uuid(s: str) -> str:
    h = hashlib.md5(s.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def upsert(table, payload):
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=id"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
        "Prefer":        "resolution=merge-duplicates"
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code not in (200,201,409):
        logger.error(f"upsert {table} failed [{r.status_code}]: {r.text}")
        return False
    return True

def fetch_invoice(inv_id, branch):
    """جلب بيانات الفاتورة الأساسية (بدون بنود)."""
    headers = {"apikey": DAFTRA_APIKEY}
    resp = requests.get(f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{inv_id}",
                        headers=headers, timeout=30)
    if resp.status_code != 200:
        logger.error(f"show invoice {inv_id}@branch {branch} → {resp.status_code}")
        return None
    return resp.json().get("data")

def fetch_items(inv_id, branch):
    """جلب بنود الفاتورة من الـ endpoint المخصَّص."""
    headers = {"apikey": DAFTRA_APIKEY}
    params = {"page":1, "limit":100, "invoice_id": inv_id}
    resp = requests.get(f"{DAFTRA_URL}/v2/api/entity/invoice-item/list/{branch}",
                        headers=headers, params=params, timeout=30)
    if resp.status_code != 200:
        logger.error(f"list items for {inv_id}@branch {branch} → {resp.status_code}")
        return []
    return resp.json().get("data", [])

def sync_invoices():
    headers = {"apikey": DAFTRA_APIKEY}
    page = 1
    tot_inv = tot_items = 0

    # 1) جلب ملخص الفواتير
    while True:
        resp = requests.get(f"{DAFTRA_URL}/v2/api/entity/invoice/list/1",
                            headers=headers,
                            params={"page": page, "limit":100},
                            timeout=30)
        if resp.status_code != 200:
            break
        batch = resp.json().get("data", [])
        if not batch:
            break

        for summary in batch:
            inv_id   = str(summary["id"])
            branch   = summary.get("store_id")
            if branch is None:
                logger.error(f"Invoice {inv_id} has no store_id → skipping")
                continue

            # 2) جلب تفاصيل الفاتورة الأساسية
            inv = fetch_invoice(inv_id, branch)
            if not inv:
                continue

            # 3) جلب البنود
            items = fetch_items(inv_id, branch)
            if not items:
                logger.error(f"❌ Invoice {inv_id} has NO items → bug!")
                continue
            logger.info(f"Invoice {inv_id} has {len(items)} items")

            # 4) حفظ الفاتورة
            inv_uuid = generate_uuid(inv_id)
            inv_pl = {
                "id":              inv_uuid,
                "invoice_no":      inv.get("no",""),
                "total":           float(inv.get("total",0)),
                "invoice_date":    inv.get("date",""),
                "branch":          branch,
                "client_business_name": inv.get("client_business_name",""),
                "customer_id":     summary.get("client_id") or summary.get("customer_id",""),
                "summary_paid":    float(inv.get("paid_amount",0)),
                "summary_unpaid":  max(0, float(inv.get("total",0)) - float(inv.get("paid_amount",0)))
            }
            if upsert("invoices", inv_pl):
                tot_inv += 1

                # 5) حفظ البنود
                for it in items:
                    item_uuid = generate_uuid(f"{it.get('id')}-{inv_id}")
                    it_pl = {
                        "id":          item_uuid,
                        "invoice_id":  inv_uuid,
                        "product_id":  it.get("product_id",""),
                        "product_code": it.get("product_code",""),
                        "quantity":    float(it.get("quantity",0)),
                        "unit_price":  float(it.get("unit_price", it.get("price",0))),
                        "total_price": float(it.get("quantity",0)) * float(it.get("unit_price", it.get("price",0)))
                    }
                    if upsert("invoice_items", it_pl):
                        tot_items += 1

        page += 1
        time.sleep(0.2)

    logger.info(f"Done. Invoices: {tot_inv}, Items: {tot_items}")

if __name__ == "__main__":
    sync_invoices()
