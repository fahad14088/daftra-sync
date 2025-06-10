# invoices_service.py
import os, requests, time, uuid

DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

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
        except:
            pass
        time.sleep((i+1)*5)
    return {}

def sync_invoices():
    # 1) بني خريطة المنتجات
    resp_map = requests.get(
        f"{SUPABASE_URL}/rest/v1/products?select=daftra_product_id,product_code",
        headers=HEADERS_SB, timeout=10
    )
    prod_map = {r["daftra_product_id"]: r["product_code"]
                for r in (resp_map.json() if resp_map.status_code==200 else [])}

    total_inv = 0
    total_itm = 0
    limit = 20
    branches = [1, 2]

    for branch_id in branches:
        page = 1
        while True:
            list_url = (
                f"{DAFTRA_URL}/v2/api/entity/invoice/list/1"
                f"?filter[branch_id]={branch_id}&page={page}&limit={limit}"
            )
            data = fetch_with_retry(list_url, HEADERS_DAFTRA)
            invs = data.get("data", [])
            print(f"> Branch {branch_id} Page {page}: {len(invs)} invoices")
            if not invs:
                break

            for raw in invs:
                inv_id = raw.get("id")
                inv_no = raw.get("no")
                if not inv_id:
                    continue

                # حاول تجيب البنود من raw أولًا
                items = raw.get("invoice_item") or []
                if not items:
                    # fallback: جلب التفاصيل من المسار الصحيح
                    det = fetch_with_retry(f"{DAFTRA_URL}/v2/api/entity/invoice/{inv_id}.json", HEADERS_DAFTRA)
                    inv_data = det.get("data", {}).get("Invoice", {})
                    items = inv_data.get("InvoiceItem") or inv_data.get("invoice_item") or []
                    raw = inv_data

                # 2) upsert الفاتورة
                inv_payload = {
                    "id":           str(inv_id),
                    "invoice_no":   inv_no or "",
                    "invoice_date": raw.get("date",""),
                    "total":        str(raw.get("total",0))
                }
                requests.post(
                    f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=id",
                    headers=HEADERS_SB,
                    json=inv_payload
                )
                total_inv += 1

                # 3) مسح البنود القديمة ثم إعادة الإضافة
                requests.delete(
                    f"{SUPABASE_URL}/rest/v1/invoice_items?invoice_id=eq.{inv_id}",
                    headers=HEADERS_SB
                )
                if not isinstance(items, list):
                    items = [items]
                for it in items:
                    qty = float(it.get("quantity") or 0)
                    if qty<=0:
                        continue
                    pid = str(it.get("product_id"))
                    item_payload = {
                        "id":           str(it.get("id") or uuid.uuid4()),
                        "invoice_id":   str(inv_id),
                        "product_id":   pid,
                        "product_code": prod_map.get(pid),
                        "quantity":     str(qty),
                        "unit_price":   str(it.get("unit_price") or 0),
                        "total_price":  str(qty * float(it.get("unit_price") or 0))
                    }
                    requests.post(
                        f"{SUPABASE_URL}/rest/v1/invoice_items",
                        headers=HEADERS_SB,
                        json=item_payload
                    )
                    total_itm += 1

            page += 1
            time.sleep(1)

    print(f"✅ Done: {total_inv} invoices, {total_itm} items")
    return {"invoices": total_inv, "items": total_itm}

if __name__=="__main__":
    sync_invoices()
