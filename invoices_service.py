import os
import requests
import time
import uuid
import logging
import hashlib

# تكوين اللوج
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# متغيرات البيئة
DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number: str) -> str:
    h = hashlib.md5(number.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def safe_float(v, default=0.0):
    try:
        return default if v is None or v == "" else float(str(v).replace(",", ""))
    except:
        return default

def safe_string(v, max_length=None):
    s = "" if v is None else str(v).strip()
    return s if not max_length or len(s) <= max_length else s[:max_length]

def get_all_invoices_with_items():
    """
    جلب كل الفواتير مع البنود دفعة واحدة
    باستخدام معامل with=InvoiceItem لإرجاع البنود مع كل فاتورة
    """
    headers = {"apikey": DAFTRA_APIKEY}
    page = 1
    all_invoices = []
    while True:
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1"
        params = {"page": page, "limit": 100, "with": "InvoiceItem"}
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        if resp.status_code != 200:
            logger.error(f"❌ خطأ جلب الفواتير صفحة {page}: {resp.status_code}")
            break
        data = resp.json().get("data", [])
        if not data:
            logger.info("✅ انتهت قائمة الفواتير")
            break
        logger.info(f"📄 page {page}: got {len(data)} invoices")
        all_invoices.extend(data)
        page += 1
        time.sleep(0.5)
    logger.info(f"📋 إجمالي الفواتير المجلوبة: {len(all_invoices)}")
    return all_invoices

def save_invoice(summary: dict):
    """
    حفظ أو تحديث الفاتورة
    """
    inv_id = str(summary["id"])
    inv_uuid = generate_uuid_from_number(inv_id)
    payload = {
        "id":            inv_uuid,
        "invoice_no":    safe_string(summary.get("no", "")),
        "total":         safe_float(summary.get("total", 0)),
        "invoice_date":  safe_string(summary.get("date", "")),
        "client_name":   safe_string(summary.get("client_business_name", "")),
        "customer_id":   safe_string(summary.get("client_id") or summary.get("customer_id", "")),
    }
    # دفع/متبقي
    paid = safe_float(summary.get("paid_amount", 0))
    payload["summary_paid"]   = paid
    payload["summary_unpaid"] = max(0, payload["total"] - paid)

    headers = {
        "apikey":       SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    url = f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=id"
    r = requests.post(url, headers=headers, json=payload, timeout=30)
    if r.status_code in (200,201,409):
        return inv_uuid
    else:
        logger.error(f"❌ فشل حفظ الفاتورة {inv_id}: {r.status_code} {r.text}")
        return None

def save_items(inv_uuid: str, items: list):
    """
    حفظ أو تحديث بنود الفاتورة
    """
    saved = 0
    headers = {
        "apikey":       SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json"
    }
    url = f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=id"
    for it in items:
        qty   = safe_float(it.get("quantity", 0))
        price = safe_float(it.get("unit_price", it.get("price", 0)))
        if qty <= 0:
            continue
        raw = str(it.get("id") or uuid.uuid4())
        item_uuid = generate_uuid_from_number(raw + "-" + inv_uuid)
        payload = {
            "id":          item_uuid,
            "invoice_id":  inv_uuid,
            "product_id":  safe_string(it.get("product_id") or it.get("item_id","")),
            "product_code": safe_string(it.get("product_code","")),
            "quantity":    qty,
            "unit_price":  price,
            "total_price": qty * price
        }
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        if r.status_code in (200,201,409):
            saved += 1
        else:
            logger.error(f"❌ بند {item_uuid} لم يُحفظ: {r.status_code} {r.text}")
    return saved

def sync_invoices():
    invoices = get_all_invoices_with_items()
    total_inv = total_items = 0
    for inv in invoices:
        inv_uuid = save_invoice(inv)
        if not inv_uuid:
            continue
        total_inv += 1
        items = inv.get("InvoiceItem") or inv.get("invoice_items") or []
        count = save_items(inv_uuid, items)
        total_items += count
    logger.info(f"✅ انتهى التزامن: فواتير {total_inv}, بنود {total_items}")

if __name__ == "__main__":
    sync_invoices()
