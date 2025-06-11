import os
import requests
import time
import logging
import hashlib

# تفعيل اللوج
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# متغيرات البيئة
DAFTRA_URL    = os.getenv("DAFTRA_URL").rstrip('/')
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL").rstrip('/')
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number: str) -> str:
    h = hashlib.md5(number.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def safe(v, to_type, default=None):
    try:
        if v is None or v == "": return default
        return to_type(v)
    except:
        return default

def get_all_invoices():
    """يرجع قائمة ملخصات الفواتير من دفرة."""
    headers = {"apikey": DAFTRA_APIKEY}
    invoices = []
    page = 1
    while True:
        resp = requests.get(
            f"{DAFTRA_URL}/v2/api/entity/invoice/list/1",
            headers=headers,
            params={"page": page, "limit": 100},
            timeout=30
        )
        if resp.status_code != 200:
            logger.error("list failed page %s: %s", page, resp.text[:200])
            break
        data = resp.json().get("data") or []
        if not data:
            break
        logger.info("page %s: got %s invoices", page, len(data))
        invoices.extend(data)
        page += 1
        time.sleep(0.5)
    logger.info("total invoices fetched: %s", len(invoices))
    return invoices

def get_invoice_with_items(invoice_id: str):
    """
    يجلب تفاصيل الفاتورة مع البنود دفعة واحدة
    باستخدام with[]=InvoiceItem
    """
    headers = {"apikey": DAFTRA_APIKEY}
    # جرب جميع الفروع
    for branch in range(1, 10):
        resp = requests.get(
            f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{invoice_id}",
            headers=headers,
            params={"with[]": "InvoiceItem"},
            timeout=30
        )
        if resp.status_code != 200:
            continue
        body = resp.json().get("data") or {}
        invoice = body.get("Invoice") or {}
        items   = invoice.pop("InvoiceItem", [])
        # إذا نجحنا في جلب رقم الفاتورة
        if invoice.get("id"):
            return invoice, items
    logger.warning("no details for invoice %s", invoice_id)
    return None, []

def upsert(table: str, payload: dict):
    """وظيفة بسيطة لعمل upsert في Supabase."""
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
    invoices = get_all_invoices()
    total_inv = total_items = 0

    for inv_summary in invoices:
        inv_id = str(inv_summary.get("id"))
        invoice, items = get_invoice_with_items(inv_id)
        if not invoice:
            continue

        # جهّز payload لحفظ الفاتورة
        inv_uuid = generate_uuid_from_number(inv_id)
        inv_pl = {
            "id":                 inv_uuid,
            "invoice_no":         invoice.get("no",""),
            "total":              safe(invoice.get("total",0), float, 0.0),
            "invoice_date":       invoice.get("date",""),
            "client_business_name": invoice.get("client_business_name",""),
            "customer_id":        invoice.get("client_id") or invoice.get("customer_id",""),
            "summary_paid":       safe(invoice.get("paid_amount",0), float, 0.0),
            "summary_unpaid":     max(0.0, safe(invoice.get("total",0), float, 0.0) - safe(invoice.get("paid_amount",0), float, 0.0)),
        }
        if upsert("invoices", inv_pl):
            total_inv += 1

            # حفظ البنود
            for it in items:
                it_uuid = generate_uuid_from_number(f"{it.get('id')}-{inv_id}")
                it_pl = {
                    "id":          it_uuid,
                    "invoice_id":  inv_uuid,
                    "product_id":  it.get("product_id",""),
                    "product_code": it.get("product_code",""),
                    "quantity":    safe(it.get("quantity",0), float, 0.0),
                    "unit_price":  safe(it.get("unit_price", it.get("price",0)), float, 0.0),
                    "total_price": safe(it.get("quantity",0), float, 0.0) * safe(it.get("unit_price", it.get("price",0)), float, 0.0)
                }
                if upsert("invoice_items", it_pl):
                    total_items += 1

        # لتخفيف الضغط
        time.sleep(0.2)

    logger.info("✅ done: invoices=%s, items=%s", total_inv, total_items)

if __name__ == "__main__":
    sync_invoices()
