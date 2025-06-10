# invoices_service.py
import os
import requests
import time

DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": DAFTRA_APIKEY}
HEADERS_SB     = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
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
    return None

def sync_invoices():
    # خريطة المنتجات
    resp_map = requests.get(f"{SUPABASE_URL}/rest/v1/products?select=daftra_product_id,product_code",
                            headers=HEADERS_SB, timeout=10)
    prod_map = {r["daftra_product_id"]: r["product_code"] for r in (resp_map.json() if resp_map.status_code==200 else [])}

    total_inv = 0
    total_itm = 0
    page = 1
    limit = 20

    while True:
        # 1) جلب قائمة الفواتير
        list_url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit={limit}&type=sales"
        data = fetch_with_retry(list_url, HEADERS_DAFTRA) or {}
        invs = data.get("data", [])
        print(f"> Page {page}: {len(invs)} invoices")
        if not invs:
            break

        for inv in invs:
            inv_id = inv.get("id")
            print(f"> Fetch details for invoice {inv_id}")
            # 2) جلب تفاصيل الفاتورة (لتحتوي البنود)
            det = fetch_with_retry(f"{DAFTRA_URL}/v2/api/entity/invoice/show/1/{inv_id}", HEADERS_DAFTRA) or {}
            invoice = det.get("data", {}).get("Invoice", {})
            if not invoice:
                print("! No Invoice object returned, skipping")
                continue

            # upsert الفاتورة
            inv_payload = {
                "id":           str(inv_id),
                "invoice_no":   invoice.get("no",""),
                "invoice_date": invoice.get("date",""),
                "total":        str(invoice.get("total",0))
            }
            print(f">> upsert invoice: {inv_payload}")
            requests.post(f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=invoice_no",
                          headers=HEADERS_SB, json=inv_payload, timeout=10)
            total_inv += 1

            # البنود
            items = invoice.get("InvoiceItem", []) or []
            print(f"  items count: {len(items)}")
            for it in items:
                item_id = str(it.get("id") or f"{inv_id}_{it.get('product_id')}")
                pid     = str(it.get("product_id"))
                qty     = float(it.get("quantity") or 0)
                price   = float(it.get("unit_price") or 0)
                if qty<=0:
                    continue

                item_payload = {
                    "id":           item_id,
                    "invoice_id":   str(inv_id),
                    "product_id":   pid,
                    "product_code": prod_map.get(pid),
                    "quantity":     str(qty),
                    "unit_price":   str(price),
                    "total_price":  str(qty*price)
                }
                print(f"  -> upsert item: {item_payload}")
                requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=invoice_id,product_id",
                              headers=HEADERS_SB, json=item_payload, timeout=10)
                total_itm += 1

        page += 1
        time.sleep(1)

    print(f"✅ Done: {total_inv} invoices, {total_itm} items")
    return {"invoices": total_inv, "items": total_itm}

if __name__=="__main__":
    sync_invoices()
