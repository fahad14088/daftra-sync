# invoices_service.py

import os
import requests
import time
import uuid
from datetime import datetime
from config import BASE_URL, HEADERS         # HEADERS = {"apikey": ...}
from sync_utils import get_last_sync_time, update_sync_time

# Supabase settings
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADERS_SB = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json"
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
    # 1) load last sync date
    sd = get_last_sync_time("sales_invoices") or ""
    try:
        last_date = datetime.fromisoformat(sd)
    except:
        last_date = datetime(2000,1,1)

    # 2) build products map
    pm = requests.get(
        f"{SUPABASE_URL}/rest/v1/products?select=daftra_product_id,product_code",
        headers=HEADERS_SB, timeout=10
    ).json()
    prod_map = {r["daftra_product_id"]: r["product_code"] for r in pm}

    total_inv = total_itm = 0
    branches = [1,2]
    limit    = 20

    for br in branches:
        page = 1
        while True:
            list_url = f"{BASE_URL}v2/api/entity/invoice/list/1?filter[branch_id]={br}&page={page}&limit={limit}"
            data = fetch_with_retry(list_url, HEADERS)
            invs = data.get("data", [])
            if not invs:
                break

            for im in invs:
                inv_id = str(im.get("id"))
                dt = im.get("date","")
                try:
                    d = datetime.fromisoformat(dt)
                except:
                    continue
                if d <= last_date:
                    continue

                # 3) fetch details with correct endpoint
                det = fetch_with_retry(f"{BASE_URL}v2/api/entity/invoice/show/{br}/{inv_id}", HEADERS)
                inv = det.get("data",{}).get("Invoice",{})
                if not inv:
                    continue

                # 4) upsert invoice
                inv_pl = {
                    "id":           inv_id,
                    "invoice_no":   inv.get("no",""),
                    "invoice_date": inv.get("date",""),
                    "total":        str(inv.get("total",0))
                }
                requests.post(
                    f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=id",
                    headers=HEADERS_SB,
                    json=inv_pl
                )
                total_inv += 1

                # 5) delete old items
                requests.delete(
                    f"{SUPABASE_URL}/rest/v1/invoice_items?invoice_id=eq.{inv_id}",
                    headers=HEADERS_SB
                )

                # 6) insert new items
                items = inv.get("InvoiceItem") or inv.get("invoice_item", []) or []
                if not isinstance(items, list):
                    items = [items]
                for it in items:
                    qty = float(it.get("quantity") or 0)
                    if qty <= 0:
                        continue
                    pid = str(it.get("product_id"))
                    item_pl = {
                        "id":           str(it.get("id") or uuid.uuid4()),
                        "invoice_id":   inv_id,
                        "product_id":   pid,
                        "product_code": prod_map.get(pid),
                        "quantity":     str(qty),
                        "unit_price":   str(it.get("unit_price") or 0),
                        "total_price":  str(qty * float(it.get("unit_price") or 0))
                    }
                    requests.post(
                        f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=id",
                        headers=HEADERS_SB,
                        json=item_pl
                    )
                    total_itm += 1

            page += 1
            time.sleep(1)

    # 7) update sync time
    update_sync_time("sales_invoices", datetime.now().isoformat())
    print(f"✅ Done: {total_inv} invoices, {total_itm} items")
    return {"invoices": total_inv, "items": total_itm}

if __name__ == "__main__":
    sync_invoices()
