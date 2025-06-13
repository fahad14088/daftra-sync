import os
import requests
import time
from datetime import datetime
from sync_utils import get_last_sync_time, update_sync_time
from config import BASE_URL, HEADERS

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

EXPECTED_TYPE = 0
PAGE_LIMIT = 20
BRANCH_IDS = [1, 2]


def safe_float(val, default=0.0):
    try:
        return float(str(val).replace(",", "")) if val not in (None, "") else default
    except:
        return default


def fetch_with_retry(url, headers, max_retries=3, timeout=30):
    for retry in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            time.sleep((retry + 1) * 2)
        except:
            time.sleep((retry + 1) * 2)
    return None


def fetch_all():
    inserted = 0
    last_date_str = get_last_sync_time("sales_invoices")
    try:
        last_date = datetime.fromisoformat(last_date_str)
    except:
        last_date = datetime(2000, 1, 1)

    for branch_id in BRANCH_IDS:
        page = 1
        while True:
            url = f"{BASE_URL}v2/api/entity/invoice/list/1?filter[branch_id]={branch_id}&page={page}&limit={PAGE_LIMIT}"
            data = fetch_with_retry(url, HEADERS)
            if data is None:
                break

            invoice_list = data.get("data", [])
            if not invoice_list:
                break

            has_new_invoices = False

            for invoice in invoice_list:
                inv_id = invoice.get("id")
                inv_no = invoice.get("no", "بدون رقم")
                inv_date = invoice.get("date")
                inv_type = invoice.get("type")
                store_id = invoice.get("store_id")

                try:
                    inv_type = int(inv_type)
                    created_at = datetime.strptime(inv_date, "%Y-%m-%d")
                except:
                    continue

                if inv_type != EXPECTED_TYPE or created_at <= last_date:
                    continue

                has_new_invoices = True

                url_details = f"{BASE_URL}v2/api/entity/invoice/{inv_id}"
                inv_details = fetch_with_retry(url_details, HEADERS)
                if inv_details is None:
                    continue

                items = inv_details.get("invoice_item", [])
                if not isinstance(items, list):
                    items = [items] if items else []

                total_amount = safe_float(inv_details.get("summary_total"))

                # save invoice to supabase
                payload = {
                    "id": str(inv_id),
                    "invoice_no": inv_no,
                    "invoice_date": inv_date,
                    "invoice_type": EXPECTED_TYPE,
                    "branch": str(branch_id),
                    "store": str(store_id or "unknown"),
                    "total": total_amount
                }
                resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoices", headers=HEADERS_SUPABASE, json=payload)
                if resp.status_code >= 300:
                    print(f"❌ فشل حفظ الفاتورة {inv_id}: {resp.text}")
                else:
                    print(f"✅ تم حفظ الفاتورة {inv_id}")

                # delete old items first
                del_resp = requests.delete(f"{SUPABASE_URL}/rest/v1/invoice_items?invoice_id=eq.{inv_id}", headers=HEADERS_SUPABASE)

                # insert new items
                for item in items:
                    product_id = item.get("product_id")
                    quantity = safe_float(item.get("quantity"))
                    unit_price = safe_float(item.get("unit_price"))
                    if product_id and quantity > 0:
                        item_payload = {
                            "id": f"{inv_id}-{item.get('id')}",
                            "invoice_id": str(inv_id),
                            "product_id": str(product_id),
                            "quantity": quantity,
                            "unit_price": unit_price,
                            "total_price": quantity * unit_price
                        }
                        item_resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items", headers=HEADERS_SUPABASE, json=item_payload)
                        if item_resp.status_code >= 300:
                            print(f"❌ فشل حفظ البند {inv_id}-{item.get('id')}: {item_resp.text}")
                        else:
                            print(f"🟢 تم حفظ البند {inv_id}-{item.get('id')}")

                inserted += 1

            if not has_new_invoices or len(invoice_list) < PAGE_LIMIT:
                break
            page += 1
            time.sleep(1)

    update_sync_time("sales_invoices", datetime.now().isoformat())
    print(f"\n✅ تم حفظ {inserted} فاتورة مبيعات جديدة.")
    return True
