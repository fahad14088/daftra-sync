import time
import requests
import logging
import os

from config import BASE_URL, BRANCH_IDS, PAGE_LIMIT, EXPECTED_TYPE, HEADERS_DAFTRA, HEADERS_SUPABASE, SUPABASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_with_retry(url, headers, params=None, retries=3, delay=2):
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"⚠️ محاولة {attempt+1} فشلت: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"⚠️ محاولة {attempt+1} فشلت: {str(e)}")
        time.sleep(delay)
    return None

def fetch_invoice_details(invoice_id):
    url = f"{BASE_URL}/v2/api/entity/invoice/view/{invoice_id}"
    data = fetch_with_retry(url, HEADERS_DAFTRA)
    return data.get("data", {}).get("Invoice", {})

def fetch_all():
    all_invoices = []
    all_items = []

    for branch in BRANCH_IDS:
        page = 1
        while True:
            url = f"{BASE_URL}/v2/api/entity/invoice/list/1"
            params = {
                "filter[branch_id]": branch,
                "page": page,
                "limit": PAGE_LIMIT
            }
            data = fetch_with_retry(url, HEADERS_DAFTRA, params=params)
            if data is None:
                logger.warning(f"⚠️ فشل في جلب البيانات للفرع {branch} الصفحة {page}")
                break

            items = data.get("data") or []
            if not isinstance(items, list):
                items = [items]

            valid_items = [inv for inv in items if int(inv.get("type", -1)) == EXPECTED_TYPE]
            logger.info(f"📄 فرع {branch} - صفحة {page} فيها {len(valid_items)} فاتورة")

            for raw_inv in valid_items:
                inv_id = raw_inv["id"]
                inv = fetch_invoice_details(inv_id)
                if not inv:
                    continue

                all_invoices.append({
                    "id": str(inv["id"]),
                    "invoice_no": inv.get("no"),
                    "invoice_date": inv.get("date"),
                    "created_at": inv.get("created_at"),
                    "customer_id": inv.get("contact_id"),
                    "total": inv.get("total", 0),
                    "branch": inv.get("branch_id"),
                    "client_id": inv.get("client_id"),
                    "client_business_name": inv.get("client_business_name"),
                    "client_city": inv.get("client_city"),
                    "summary_paid": inv.get("summary_paid", 0),
                    "summary_unpaid": inv.get("summary_unpaid", 0),
                })

                for item in inv.get("InvoiceItem", []):
                    all_items.append({
                        "id": str(item.get("id") or f"{inv['id']}-{item.get('product_id')}"),
                        "invoice_id": str(inv["id"]),
                        "product_id": str(item.get("product_id")),
                        "product_code": item.get("product_code"),
                        "description": item.get("description"),
                        "quantity": item.get("quantity", 0),
                        "unit_price": item.get("unit_price", 0),
                        "total_price": item.get("total", 0),
                        "client_business_name": inv.get("client_business_name"),
                    })

            if len(items) < 10:
                logger.info(f"✅ انتهينا من فواتير فرع {branch}، عدد الصفحات: {page}")
                break

            page += 1
            time.sleep(1)

    logger.info(f"📦 عدد الفواتير اللي بنعالجها: {len(all_invoices)}")

    # حفظ في Supabase
    if all_invoices:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=HEADERS_SUPABASE,
            json=all_invoices
        )

    if all_items:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/invoice_items",
            headers=HEADERS_SUPABASE,
            json=all_items
        )

    logger.info(f"✅ تم حفظ {len(all_invoices)} فاتورة مبيعات جديدة.")
    logger.info(f"✅ الفواتير: {len(all_invoices)} فاتورة، {len(all_items)} بند")

    return {
        "invoices": len(all_invoices),
        "items": len(all_items)
    }
