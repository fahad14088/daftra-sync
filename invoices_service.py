# invoices_service.py

import requests
import time
from datetime import datetime
from config import BASE_URL, HEADERS
from database.db_manager import create_connection
from sync_utils import get_last_sync_time, update_sync_time

def fetch_with_retry(url, headers, max_retries=3, timeout=30):
    """جلب البيانات مع retry وتجاوز الأخطاء مؤقتًا"""
    for retry in range(max_retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            print(f"⚠️ Unexpected status {r.status_code} from {url}")
        except Exception as e:
            print(f"⚠️ Fetch error: {e}")
        time.sleep((retry + 1) * 5)
    return None

def fetch_all():
    conn    = create_connection()
    cursor  = conn.cursor()
    inserted = 0

    # جلب آخر تاريخ مزامنة
    last_date_str = get_last_sync_time("sales_invoices")
    try:
        last_date = datetime.fromisoformat(last_date_str)
    except:
        last_date = datetime(2000,1,1)

    branches = [1, 2]        # عدل عليها حسب فروعك
    limit    = 20

    for branch_id in branches:
        page = 1
        while True:
            list_url = (
                f"{BASE_URL}v2/api/entity/invoice/list/1"
                f"?filter[branch_id]={branch_id}&page={page}&limit={limit}"
            )
            data = fetch_with_retry(list_url, HEADERS)
            if not data:
                break
            invoice_list = data.get("data", [])
            if not invoice_list:
                break

            for inv in invoice_list:
                inv_id   = inv.get("id")
                inv_no   = inv.get("no", "")
                inv_date = inv.get("date", "")
                try:
                    created = datetime.fromisoformat(inv_date)
                except:
                    continue
                if created <= last_date:
                    continue

                # جلب التفاصيل
                detail_url = f"{BASE_URL}v2/api/entity/invoice/{inv_id}.json"
                det = fetch_with_retry(detail_url, HEADERS)
                if not det:
                    continue
                inv_data = det.get("data", {}).get("Invoice", {})
                items    = inv_data.get("invoice_item") or []

                # upsert الفاتورة
                cursor.execute(
                    "INSERT OR REPLACE INTO invoices (id, created_at, invoice_type, branch, total) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (inv_id, inv_date, 0, str(branch_id), float(inv_data.get("total") or 0))
                )

                # إحذف بنود سابقة ثم أضف الجديدة
                cursor.execute("DELETE FROM invoice_items WHERE invoice_id = ?", (inv_id,))
                count = 0
                if not isinstance(items, list):
                    items = [items]
                for it in items:
                    qty  = float(it.get("quantity") or 0)
                    price= float(it.get("unit_price") or 0)
                    if qty <= 0:
                        continue
                    cursor.execute(
                        "INSERT INTO invoice_items (invoice_id, product_id, quantity, unit_price) "
                        "VALUES (?, ?, ?, ?)",
                        (inv_id, str(it.get("product_id")), qty, price)
                    )
                    count += 1

                print(f"💾 Invoice {inv_no}: inserted, {count} items")
                inserted += 1

            conn.commit()
            page += 1
            time.sleep(1)

    conn.close()
    # حدِّث وقت التزامن لآخر فاتورة فعلًا
    update_sync_time("sales_invoices", datetime.now().isoformat())
    print(f"✅ Done: {inserted} new invoices synced.")
    return True

if __name__ == "__main__":
    fetch_all()
