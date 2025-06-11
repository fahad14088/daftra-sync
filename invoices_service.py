import os
import requests
import time
import uuid
from datetime import datetime
from config import BASE_URL, HEADERS  # HEADERS يحوي apikey دفترة
from sync_utils import get_last_sync_time, update_sync_time

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS_SB = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
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

    # جلب آخر تاريخ تزامن
    last_date_str = get_last_sync_time("sales_invoices")
    try:
        last_date = datetime.fromisoformat(last_date_str)
    except:
        last_date = datetime(2000,1,1)

    total_inv = total_itm = 0
    branches = [1,2]  # عدل للـ branch_id اللي عندك

    for branch_id in branches:
        page = 1
        while True:
            url = (
                f"{BASE_URL}v2/api/entity/invoice/list/1"
                f"?filter[branch_id]={branch_id}&page={page}&limit=20"
            )
            data = fetch_with_retry(url, HEADERS)
            invs = data.get("data", [])
            if not invs:
                break

            for inv_meta in invs:
                inv_id = str(inv_meta.get("id"))
                inv_date = inv_meta.get("date","")
                try:
                    dt = datetime.fromisoformat(inv_date)
                except:
                    continue
                if dt <= last_date:
                    continue

                # جلب التفاصيل
                det = fetch_with_retry(f"{BASE_URL}v2/api/entity/invoice/{inv_id}.json", HEADERS)
                invoice = det.get("data",{}).get("Invoice",{})
                if not invoice:
                    continue

                # upsert الفاتورة
                inv_payload = {
                    "id":          inv_id,
                    "invoice_no":  invoice.get("no",""),
                    "invoice_date": inv_date,
                    "total":        str(invoice.get("total",0))
                }
                requests.post(
                    f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=id",
                    headers=HEADERS_SB,
                    json=inv_payload
                )
                total_inv += 1

                # مسح البنود القديمة
                requests.delete(
                    f"{SUPABASE_URL}/rest/v1/invoice_items?invoice_id=eq.{inv_id}",
                    headers=HEADERS_SB
                )

                # إضافة البنود الجديدة
                items = invoice.get("invoice_item") or []
                if not isinstance(items, list):
                    items = [items]
                for it in items:
                    qty   = float(it.get("quantity") or 0)
                    price = float(it.get("unit_price") or 0)
                    if qty<=0:
                        continue
                    pid = str(it.get("product_id"))
                    item_payload = {
                        "id":           str(it.get("id") or uuid.uuid4()),
                        "invoice_id":   inv_id,
                        "product_id":   pid,
                        "product_code": prod_map.get(pid),
                        "quantity":     str(qty),
                        "unit_price":   str(price),
                        "total_price":  str(qty*price)
                    }
                    requests.post(
                        f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=id",
                        headers=HEADERS_SB,
                        json=item_payload
                    )
                    total_itm += 1

            page += 1
            time.sleep(1)

    update_sync_time("sales_invoices", datetime.now().isoformat())
    print(f"✅ تم مزامنة {total_inv} فاتورة و{total_itm} بند.")
    return {"invoices": total_inv, "items": total_itm}

if __name__ == "__main__":
    sync_invoices()

