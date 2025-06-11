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
    "apikey":       SUPABASE_KEY,
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
    # خريطة المنتجات
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/products?select=daftra_product_id,product_code",
        headers=HEADERS_SB, timeout=10
    )
    prod_map = {r["daftra_product_id"]: r["product_code"] for r in resp.json()}

    total_inv = total_itm = 0
    branches = [1, 2]  # عدّل حسب فروعك
    limit    = 20

    for branch_id in branches:
        page = 1
        while True:
            list_url = (
                f"{DAFTRA_URL}/v2/api/entity/invoice/list/1"
                f"?filter[branch_id]={branch_id}&page={page}&limit={limit}"
            )
            data = fetch_with_retry(list_url, HEADERS_DAFTRA)
            invs = data.get("data", [])
            if not invs:
                break

            for raw in invs:
                inv_id = str(raw.get("id"))
                # إذا الفاتورة موجودة فعلاً، تجاهلها
                exists = requests.get(
                    f"{SUPABASE_URL}/rest/v1/invoices?select=id&invoice_id=eq.{inv_id}",
                    headers=HEADERS_SB
                ).json()
                if exists:
                    continue

                # جلب تفاصيل الفاتورة
                det = fetch_with_retry(
                    f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch_id}/{inv_id}",
                    HEADERS_DAFTRA
                )
                inv = det.get("data", {}).get("Invoice", {})
                if not inv:
                    continue

                # حفظ الفاتورة
                inv_payload = {
                    "id":           inv_id,
                    "invoice_no":   inv.get("no", ""),
                    "invoice_date": inv.get("date", ""),
                    "total":        str(inv.get("total", 0))
                }
                requests.post(
                    f"{SUPABASE_URL}/rest/v1/invoices",
                    headers=HEADERS_SB,
                    json=inv_payload
                )
                total_inv += 1

                # حفظ البنود بدون حذف، يعتمد على id فريد
                items = inv.get("invoice_item") or []
                if not isinstance(items, list):
                    items = [items]
                for it in items:
                    qty = float(it.get("quantity") or 0)
                    if qty <= 0:
                        continue
                    pid = str(it.get("product_id"))
                    item_id = str(it.get("id") or uuid.uuid4())
                    # تأكد من عدم التكرار
                    exists_item = requests.get(
                        f"{SUPABASE_URL}/rest/v1/invoice_items?select=id&id=eq.{item_id}",
                        headers=HEADERS_SB
                    ).json()
                    if exists_item:
                        continue

                    item_payload = {
                        "id":           item_id,
                        "invoice_id":   inv_id,
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

if __name__ == "__main__":
    sync_invoices()
