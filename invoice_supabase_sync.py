import time
import requests
import logging
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
    time.sleep(0.4)  # تأخير لتفادي حظر دفترة
    if not data or "Invoice" not in data.get("data", {}):
        return None
    return data["data"]["Invoice"]

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

            invoice_ids = [inv.get("id") for inv in items if int(inv.get("type", -1)) == EXPECTED_TYPE]
            logger.info(f"📄 فرع {branch} - صفحة {page} فيها {len(invoice_ids)} فاتورة مؤهلة")

            for invoice_id in invoice_ids:
                inv = fetch_invoice_details(invoice_id)
                if not inv:
                    continue

                all_invoices.append({
                    "id": str(inv.get("id")),
                    "invoice_no": inv.get("no"),
                    "invoice_date": inv.get("date"),
                    "customer_id": str(inv.get("contact_id")),
                    "total": inv.get("total"),
                    "branch": inv.get("branch_id"),
                    "created_at": inv.get("created_at"),
                    "client_id": str(inv.get("Contact", {}).get("id")),
                    "client_business_name": inv.get("Contact", {}).get("business_name"),
                    "client_city": inv.get("Contact", {}).get("city"),
                    "summary_paid": inv.get("summary", {}).get("paid"),
                    "summary_unpaid": inv.get("summary", {}).get("unpaid")
                })

                for item in inv.get("InvoiceItem", []):
                    all_items.append({
                        "id": str(item.get("id")),
                        "invoice_id": str(inv.get("id")),
                        "product_id": str(item.get("product_id")),
                        "product_code": item.get("product_code"),
                        "quantity": item.get("quantity"),
                        "unit_price": item.get("unit_price"),
                        "total_price": item.get("total"),
                        "client_business_name": inv.get("Contact", {}).get("business_name")
                    })

            if len(items) < 10:
                logger.info(f"✅ انتهينا من فرع {branch} صفحة {page}")
                break

            page += 1
            time.sleep(1)

    logger.info(f"📦 عدد الفواتير اللي بنعالجها: {len(all_invoices)}")

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
    logger.info(f"✅ البنود: {len(all_items)} بند")

    return {
        "invoices": len(all_invoices),
        "items": len(all_items)
    }
