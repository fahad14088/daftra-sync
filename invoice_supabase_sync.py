import time
import requests
import logging
import os
import uuid
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
            if not valid_items:
                break

            for inv in valid_items:
                inv_id = str(inv["id"])
                all_invoices.append({
                    "id": inv_id,
                    "invoice_no": inv.get("no"),
                    "invoice_date": inv.get("date"),
                    "created_at": inv.get("created_at"),
                    "customer_id": str(inv.get("contact_id")) if inv.get("contact_id") else None,
                    "branch": inv.get("branch_id"),
                    "total": inv.get("total", 0),
                    "client_id": str(inv.get("contact_id")) if inv.get("contact_id") else None,
                    "client_business_name": inv.get("Contact", {}).get("business_name"),
                    "client_city": inv.get("Contact", {}).get("city"),
                    "summary_paid": inv.get("summary", {}).get("paid"),
                    "summary_unpaid": inv.get("summary", {}).get("unpaid")
                })

                for item in inv.get("InvoiceItem", []):
                    all_items.append({
                        "id": str(uuid.uuid4()),
                        "invoice_id": inv_id,
                        "product_id": str(item.get("product_id")),
                        "product_code": item.get("product_code"),
                        "client_business_name": inv.get("Contact", {}).get("business_name"),
                        "quantity": item.get("quantity", 0),
                        "unit_price": item.get("unit_price", 0),
                        "total_price": item.get("total", 0)
                    })

            if len(items) < 10:
                logger.info(f"✅ انتهينا من فواتير فرع {branch}، عدد الصفحات: {page}")
                break

            page += 1
            time.sleep(1)

    logger.info(f"📦 عدد الفواتير اللي بنعالجها: {len(all_invoices)}")

    if all_invoices:
        for i in range(0, len(all_invoices), 500):
            chunk = all_invoices[i:i+500]
            requests.post(
                f"{SUPABASE_URL}/rest/v1/invoices",
                headers=HEADERS_SUPABASE,
                json=chunk
            )

    if all_items:
        for i in range(0, len(all_items), 500):
            chunk = all_items[i:i+500]
            requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items",
                headers=HEADERS_SUPABASE,
                json=chunk
            )

    logger.info(f"✅ تم حفظ {len(all_invoices)} فاتورة مبيعات جديدة.")
    logger.info(f"✅ الفواتير: {len(all_invoices)} فاتورة، {len(all_items)} بند")

    return {
        "invoices": len(all_invoices),
        "items": len(all_items)
    }
