import os
import requests
import time
import logging
import hashlib

# تفعيل تتبُّع الأخطاء والتفاصيل في اللوج
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# تحميل متغيرات البيئة
DAFTRA_URL    = os.getenv("DAFTRA_URL").rstrip('/')
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL").rstrip('/')
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number: str) -> str:
    """توليد UUID ثابت بناءً على نص الإدخال."""
    h = hashlib.md5(number.encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def safe_float(v, default=0.0):
    """تحويل آمن إلى float."""
    try:
        if v is None or v == "":
            return default
        return float(str(v).replace(",", ""))
    except Exception as e:
        logger.error(f"safe_float('{v}') failed: {e}", exc_info=True)
        return default

def safe_string(v, max_length=None):
    """تحويل آمن إلى string وقص للطول الأقصى."""
    try:
        s = "" if v is None else str(v).strip()
        return s if not max_length or len(s) <= max_length else s[:max_length]
    except Exception as e:
        logger.error(f"safe_string('{v}') failed: {e}", exc_info=True)
        return ""

def upsert(table: str, payload: dict) -> bool:
    """إدخال أو تعديل سجل في Supabase باستخدام on_conflict=id و merge-duplicates."""
    url = f"{SUPABASE_URL}/rest/v1/{table}?on_conflict=id"
    headers = {
        "apikey":       SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer":       "resolution=merge-duplicates"
    }
    logger.debug(f"▶️ UPSERT {table}: {payload}")
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    logger.debug(f"↩️ Response ({resp.status_code}): {resp.text}")
    if resp.status_code not in (200, 201, 409):
        logger.error(f"❌ upsert {table} failed [{resp.status_code}]: {resp.text}")
        return False
    return True

def get_invoice_full_details(inv_id: str):
    """جلب تفاصيل الفاتورة الكاملة ومعها البنود دفعة واحدة."""
    headers = {"apikey": DAFTRA_APIKEY}
    for branch in range(1, 10):
        resp = requests.get(
            f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{inv_id}",
            headers=headers,
            params={"with[]": "InvoiceItem"},
            timeout=30
        )
        logger.debug(f"GET show branch={branch}, resp {resp.status_code}")
        if resp.status_code != 200:
            continue
        data = resp.json().get("data") or {}
        inv = data.get("Invoice") or data
        items = inv.pop("InvoiceItem", [])
        inv["branch"] = branch
        return inv, items
    logger.warning(f"⚠️ no full details for invoice {inv_id}")
    return {}, []

def sync_invoices():
    """
    المزامنة الرئيسية:
    1) يجلب ملخص الفواتير مع البنود
    2) يستدعي show للحصول على total و branch الحقيقي
    3) يحفظ الفاتورة وبنودها في Supabase
    """
    headers = {"apikey": DAFTRA_APIKEY}
    page = 1
    total_invoices = 0
    total_items = 0

    while True:
        resp = requests.get(
            f"{DAFTRA_URL}/v2/api/entity/invoice/list/1",
            headers=headers,
            params={"page": page, "limit": 100, "with": "InvoiceItem"},
            timeout=30
        )
        logger.debug(f"GET list page {page}: {resp.status_code}")
        if resp.status_code != 200:
            logger.error(f"❌ Failed to fetch list page {page}: {resp.text}")
            break

        invoices = resp.json().get("data", [])
        if not invoices:
            logger.info("✅ No more invoices.")
            break

        logger.info(f"📄 page {page}: got {len(invoices)} invoices")
        for inv_summary in invoices:
            inv_id = str(inv_summary.get("id"))
            inv_uuid = generate_uuid_from_number(inv_id)

            # جلب التفاصيل الحقيقية
            inv, items = get_invoice_full_details(inv_id)
            if not inv:
                continue

            # سجل تحذير لو فاتورة بدون بنود
            if len(items) == 0:
                logger.error(f"❌ Invoice {inv_id} has NO items but should have at least one!")
            logger.info(f"Invoice {inv_id} has {len(items)} items")

            # بناء payload لحفظ الفاتورة
            inv_payload = {
                "id":                   inv_uuid,
                "invoice_no":           safe_string(inv.get("no", "")),
                "total":                safe_float(inv.get("total", 0)),
                "invoice_date":         safe_string(inv.get("date", "")),
                "branch":               inv.get("branch"),
                "client_business_name": safe_string(inv.get("client_business_name", ""), 255),
                "customer_id":          safe_string(inv.get("client_id") or inv.get("customer_id", "")),
                "summary_paid":         safe_float(inv.get("paid_amount", 0)),
                "summary_unpaid":       max(
                                          0.0,
                                          safe_float(inv.get("total", 0))
                                          - safe_float(inv.get("paid_amount", 0))
                                       )
            }

            if upsert("invoices", inv_payload):
                total_invoices += 1

                # حفظ البنود
                for it in items:
                    item_uuid = generate_uuid_from_number(f"{it.get('id')}-{inv_id}")
                    it_payload = {
                        "id":           item_uuid,
                        "invoice_id":   inv_uuid,
                        "product_id":   safe_string(it.get("product_id", "")),
                        "product_code": safe_string(it.get("product_code", "")),
                        "quantity":     safe_float(it.get("quantity", 0)),
                        "unit_price":   safe_float(it.get("unit_price", it.get("price", 0))),
                        "total_price":  safe_float(it.get("quantity", 0))
                                         * safe_float(it.get("unit_price", it.get("price", 0)))
                    }
                    if upsert("invoice_items", it_payload):
                        total_items += 1

        page += 1
        time.sleep(0.2)

    logger.info(f"✅ Done. Invoices saved: {total_invoices}, Items saved: {total_items}")

if __name__ == "__main__":
    sync_invoices()
