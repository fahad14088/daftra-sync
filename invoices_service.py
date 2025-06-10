import requests
import time
import uuid

# إعدادات
DAFTRA_URL = "https://shadowpeace.daftra.com"
DAFTRA_HEADERS = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
SUPABASE_URL = "https://wuqbovrurauffztbkbse.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.…GOGSPyFo"

def fetch_with_retry(url, headers, max_retries=3, timeout=30):
    for retry in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            time.sleep((retry + 1) * 5)
        except Exception:
            time.sleep((retry + 1) * 5)
    return None

def sync_invoices():
    """
    1) يبني خريطة daftra_product_id -> product_code من جدول products
    2) يجلب صفحات فواتير المبيعات من دفترة
    3) يحفظ كل فاتورة وبنودها مع ربط product_code الصحيح
    """
    # بناء الخريطة مرة واحدة
    map_resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/products?select=daftra_product_id,product_code",
        headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}"},
        timeout=10
    )
    prod_map = {
        rec["daftra_product_id"]: rec["product_code"]
        for rec in (map_resp.json() if map_resp.status_code == 200 else [])
    }

    total_invoices = 0
    total_items = 0
    page = 1
    limit = 20

    while True:
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit={limit}&type=sales"
        data = fetch_with_retry(url, DAFTRA_HEADERS)
        invs = data.get("data", []) if data else []
        if not invs:
            break

        for inv in invs:
            inv_uuid = str(uuid.uuid4())
            inv_payload = {
                "id": inv_uuid,
                "invoice_no": inv.get("no", ""),
                "invoice_date": inv.get("date", ""),
                "total": str(inv.get("total", 0))
            }
            requests.post(
                f"{SUPABASE_URL}/rest/v1/invoices",
                headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"},
                json=inv_payload,
                timeout=10
            )
            total_invoices += 1

            items = inv.get("invoice_item") or []
            if not isinstance(items, list):
                items = [items]

            for it in items:
                pid = str(it.get("product_id"))
                qty = float(it.get("quantity") or 0)
                price = float(it.get("unit_price") or 0)
                if qty <= 0:
                    continue

                item_payload = {
                    "id": str(uuid.uuid4()),
                    "invoice_id": inv_uuid,
                    "product_id": pid,
                    "product_code": prod_map.get(pid),
                    "quantity": str(qty),
                    "unit_price": str(price),
                    "total_price": str(qty * price)
                }
                requests.post(
                    f"{SUPABASE_URL}/rest/v1/invoice_items",
                    headers={"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json"},
                    json=item_payload,
                    timeout=10
                )
                total_items += 1

        page += 1
        time.sleep(1)

    return {"invoices": total_invoices, "items": total_items}
