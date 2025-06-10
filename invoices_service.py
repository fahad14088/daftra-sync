import os
import requests
import time
import uuid

# قراءة الإعدادات من المتغيرات البيئية
DAFTRA_URL     = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY  = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": DAFTRA_APIKEY}
HEADERS_SB     = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def fetch_with_retry(url, headers, retries=3, timeout=30):
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep((i+1)*5)
    return None

def sync_invoices():
    # خريطة daftra_product_id -> product_code
    resp_map = requests.get(
        f"{SUPABASE_URL}/rest/v1/products?select=daftra_product_id,product_code",
        headers=HEADERS_SB,
        timeout=10
    )
    prod_map = {
        rec["daftra_product_id"]: rec["product_code"]
        for rec in (resp_map.json() if resp_map.status_code==200 else [])
    }

    total_inv = 0
    total_itm = 0
    page = 1
    limit = 20

    while True:
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit={limit}&type=sales"
        data = fetch_with_retry(url, HEADERS_DAFTRA)
        invs = data.get("data", []) if data else []
        if not invs:
            break

        for inv in invs:
            inv_uuid = str(uuid.uuid4())
            inv_payload = {
                "id": inv_uuid,
                "invoice_no":   inv.get("no",""),
                "invoice_date": inv.get("date",""),
                "total":        str(inv.get("total",0))
            }
            requests.post(f"{SUPABASE_URL}/rest/v1/invoices", headers=HEADERS_SB, json=inv_payload, timeout=10)
            total_inv += 1

            items = inv.get("invoice_item") or []
            if not isinstance(items, list):
                items = [items]

            for it in items:
                pid = str(it.get("product_id"))
                qty = float(it.get("quantity") or 0)
                price = float(it.get("unit_price") or 0)
                if qty<=0: continue

                item_payload = {
                    "id":           str(uuid.uuid4()),
                    "invoice_id":   inv_uuid,
                    "product_id":   pid,
                    "product_code": prod_map.get(pid),
                    "quantity":     str(qty),
                    "unit_price":   str(price),
                    "total_price":  str(qty*price)
                }
                requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items", headers=HEADERS_SB, json=item_payload, timeout=10)
                total_itm += 1

        page += 1
        time.sleep(1)

    return {"invoices": total_inv, "items": total_itm}
