import os
import requests
import time
import logging
import hashlib

# تفعيل اللوج للتتبع
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# إعداد المتغيرات من البيئة
DAFTRA_URL    = os.getenv("DAFTRA_URL").rstrip('/')
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL").rstrip('/')
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number: str) -> str:
    h = hashlib.md5(number.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def safe_float(v, default=0.0):
    try:
        if v is None or v == "": return default
        return float(str(v).replace(",", ""))
    except:
        return default

def safe_string(v, max_length=None):
    s = "" if v is None else str(v).strip()
    return s if not max_length or len(s) <= max_length else s[:max_length]

def upsert(table: str, payload: dict):
    """حفظ أو تحديث سجل في Supabase باستخدام on_conflict=id"""
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=id"
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json"
    }
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code not in (200,201,409):
        logger.error("❌ upsert %s failed: %s %s", table, r.status_code, r.text)
        return False
    return True

def sync_invoices():
    headers = {"apikey": DAFTRA_APIKEY}
    page = 1
    total_invoices = 0
    total_items = 0

    while True:
        params = {
            "page": page,
            "limit": 100,
            "with": "InvoiceItem"  # مهم: تضمين البنود مع كل فاتورة
        }
        resp = requests.get(f"{DAFTRA_URL}/v2/api/entity/invoice/list/1",
                            headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            logger.error("❌ Failed to fetch page %s: %s", page, resp.text[:200])
            break

        invoices = resp.json().get("data", [])
        if not invoices:
            logger.info("✅ No more invoices, exiting.")
            break

        logger.info("📄 page %s: got %s invoices", page, len(invoices))
        for inv in invoices:
            inv_id = str(inv.get("id"))
            inv_uuid = generate_uuid_from_number(inv_id)

            # تحضير وحفظ الفاتورة
            inv_payload = {
                "id":                    inv_uuid,
                "invoice_no":            safe_string(inv.get("no", "")),
                "total":                 safe_float(inv.get("total", 0)),
                "invoice_date":          safe_string(inv.get("date", "")),
                "client_business_name":  safe_string(inv.get("client_business_name", ""), 255),
                "customer_id":           safe_string(inv.get("client_id") or inv.get("customer_id", "")),
                "summary_paid":          safe_float(inv.get("paid_amount", 0)),
                "summary_unpaid":        max(0.0, safe_float(inv.get("total", 0)) - safe_float(inv.get("paid_amount", 0)))
            }
            if upsert("invoices", inv_payload):
                total_invoices += 1

                # جلب البنود المحشوة ضمن المفتاح InvoiceItem
                items = inv.get("InvoiceItem") or []
                for it in items:
                    item_uuid = generate_uuid_from_number(f"{it.get('id')}-{inv_id}")
                    it_payload = {
                        "id":          item_uuid,
                        "invoice_id":  inv_uuid,
                        "product_id":  safe_string(it.get("product_id", "")),
                        "product_code": safe_string(it.get("product_code", "")),
                        "quantity":    safe_float(it.get("quantity", 0)),
                        "unit_price":  safe_float(it.get("unit_price", it.get("price", 0))),
                        "total_price": safe_float(it.get("quantity", 0)) * safe_float(it.get("unit_price", it.get("price", 0)))
                    }
                    if upsert("invoice_items", it_payload):
                        total_items += 1

        page += 1
        time.sleep(0.2)  # لتخفيف الضغط

    logger.info("✅ Done. Invoices saved: %s, Items saved: %s", total_invoices, total_items)

if __name__ == "__main__":
    sync_invoices()
