# invoices_service.py

import os
import requests
import time
import uuid
from datetime import datetime

# إعداد المتغيرات من البيئة
DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": DAFTRA_APIKEY}
HEADERS_SB     = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json"
}

def fetch_with_retry(url, headers, attempts=3, timeout=30):
    for i in range(attempts):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
        except:
            pass
        time.sleep((i+1)*5)
    return {}

def sync_invoices():
    # خريطة daftra_product_id → product_code
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/products?select=daftra_product_id,product_code",
        headers=HEADERS_SB,
        timeout=10
    )
    prod_map = {r["daftra_product_id"]: r["product_code"] for r in resp.json()}

    total_inv = total_itm = 0
    branches = [1, 2]   # عدّل حسب الفروع لديك
    limit    = 20

    for branch_id in branches:
        page = 1
        while True:
            # جلب قائمة الفواتير
            url = (
                f"{DAFTRA_URL}/v2/api/entity/invoice/list/1"
                f"?filter[branch_id]={branch_id}&page={page}&limit={limit}"
            )
            data = fetch_with_retry(url, HEADERS_DAFTRA)
            invs = data.get("data", [])
            if not invs:
                break

            for inv_meta in invs:
                inv_id = str(inv_meta.get("id"))
                # upsert الفاتورة
                inv_payload = {
                    "id":            inv_id,
                    "invoice_no":    inv_meta.get("no", ""),
                    "invoice_date":  inv_meta.get("date", ""),
                    "total":         str(inv_meta.get("total", 0))
                }
                requests.post(
                    f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=id",
                    headers=HEADERS_SB,
                    json=inv_payload
                )
                total_inv += 1

                # حذف البنود القديمة
                requests.delete(
                    f"{SUPABASE_URL}/rest/v1/invoice_items?invoice_id=eq.{inv_id}",
                    headers=HEADERS_SB
                )

                # إضافة البنود الجديدة
                items = inv_meta.get("invoice_item") or []
                if not isinstance(items, list):
                    items = [items]
                for it in items:
                    qty = float(it.get("quantity") or 0)
                    price = float(it.get("unit_price") or 0)
                    if qty <= 0:
                        continue

                    item_payload = {
                        "id":           str(it.get("id") or uuid.uuid4()),
                        "invoice_id":   inv_id,
                        "product_id":   str(it.get("product_id")),
                        "product_code": prod_map.get(str(it.get("product_id"))),
                        "quantity":     str(qty),
                        "unit_price":   str(price),
                        "total_price":  str(qty * price)
                    }
                    requests.post(
                        f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=id",
                        headers=HEADERS_SB,
                        json=item_payload
                    )
                    total_itm += 1

            page += 1
            time.sleep(1)

    print(f"✅ تم مزامنة {total_inv} فاتورة و{total_itm} بند.")
    return {"invoices": total_inv, "items": total_itm}

if __name__ == "__main__":
    sync_invoices()
