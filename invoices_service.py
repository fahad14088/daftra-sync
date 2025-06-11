# invoices_service.py

import os
import requests
import time
import uuid
from datetime import datetime

# إعدادات من البيئة
BASE_URL      = os.getenv("DAFTRA_URL", "https://shadowpeace.daftra.com/") + "v2/api/entity/"
DAFTRA_KEY    = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": DAFTRA_KEY}
HEADERS_SB     = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json"
}

def fetch_with_retry(url, headers, retries=3, timeout=30):
    for i in range(retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        except:
            pass
        time.sleep((i+1)*5)
    return {}

def sync_invoices():
    # 1) بناء خريطة المنتجات
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/products?select=daftra_product_id,product_code",
        headers=HEADERS_SB,
        timeout=10
    )
    prod_map = {r["daftra_product_id"]: r["product_code"] for r in (resp.json() if resp.status_code == 200 else [])}

    total_inv = total_itm = 0
    branches = [1, 2]   # عدّل حسب IDs للفروع عندك
    limit    = 20

    for branch_id in branches:
        page = 1
        while True:
            list_url = (
                f"{BASE_URL}invoice/list/1"
                f"?filter[branch_id]={branch_id}&page={page}&limit={limit}"
            )
            data = fetch_with_retry(list_url, HEADERS_DAFTRA)
            invs = data.get("data", [])
            if not invs:
                break

            for raw in invs:
                inv_id = str(raw.get("id"))
                if not inv_id:
                    continue

                # 2) upsert الفاتورة على الـ id
                inv_pl = {
                    "id":           inv_id,
                    "invoice_no":   raw.get("no", ""),
                    "invoice_date": raw.get("date", ""),
                    "total":        str(raw.get("total") or 0)
                }
                requests.post(
                    f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=id",
                    headers=HEADERS_SB,
                    json=inv_pl
                )
                total_inv += 1

                # 3) حذف البنود القديمة
                requests.delete(
                    f"{SUPABASE_URL}/rest/v1/invoice_items?invoice_id=eq.{inv_id}",
                    headers=HEADERS_SB
                )

                # 4) إضافة البنود الجديدة
                # حاول تجيب من raw أولاً، وإلا جلب التفاصيل
                items = raw.get("invoice_item") or []
                if not items:
                    det = fetch_with_retry(f"{BASE_URL}invoice/{inv_id}.json", HEADERS_DAFTRA)
                    inv_data = det.get("data", {}).get("Invoice", {})
                    items = inv_data.get("invoice_item") or []

                if not isinstance(items, list):
                    items = [items]

                for it in items:
                    qty = float(it.get("quantity") or 0)
                    price = float(it.get("unit_price") or 0)
                    if qty <= 0:
                        continue
                    pid = str(it.get("product_id"))
                    item_pl = {
                        "id":           str(it.get("id") or uuid.uuid4()),
                        "invoice_id":   inv_id,
                        "product_id":   pid,
                        "product_code": prod_map.get(pid),
                        "quantity":     str(qty),
                        "unit_price":   str(price),
                        "total_price":  str(qty * price)
                    }
                    requests.post(
                        f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=id",
                        headers=HEADERS_SB,
                        json=item_pl
                    )
                    total_itm += 1

            page += 1
            time.sleep(1)

    print(f"✅ Done: {total_inv} invoices, {total_itm} items")
    return {"invoices": total_inv, "items": total_itm}

if __name__ == "__main__":
    sync_invoices()
