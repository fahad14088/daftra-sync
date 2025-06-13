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
    return data.get("data", {}).get("Invoice", {}) if data else {}

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
                logger.warning(f"⚠️ فشل في جلب الصفحة {page} للفرع {branch}")
                break

            items = data.get("data") or []
            if not isinstance(items, list):
                items = [items]

            for inv in items:
                if int(inv.get("type", -1)) != EXPECTED_TYPE:
                    continue

                invoice_id = inv.get("id")
                detailed = fetch_invoice_details(invoice_id)
                if not detailed:
                    continue

                all_invoices.append({
                    "id": str(detailed.get("id")),
                    "invoice_no": detailed.get("no"),
                    "invoice_date": detailed.get("date"),
                    "created_at": detailed.get("created_at"),
                    "total": detailed.get("total"),
                    "branch": detailed.get("branch_id"),
                    "customer_id": detailed.get("contact_id"),
                    "client_business_name": detailed.get("client_business_name"),
                    "summary_paid": detailed.get("summary_paid"),
                    "summary_unpaid": detailed.get("summary_unpaid"),
                })

                for item in detailed.get("InvoiceItem", []):
                    all_items.append({
                        "id": str(item.get("id")),
                        "invoice_id": detailed.get("id"),
                        "product_id": item.get("product_id"),
                        "product_code": item.get("product_code"),
                        "description": item.get("description"),
                        "quantity": item.get("quantity"),
                        "unit_price": item.get("unit_price"),
                        "total_price": item.get("total"),
                        "client_business_name": detailed.get("client_business_name")
                    })

            logger.info(f"📄 فرع {branch} - صفحة {page} فيها {len(items)} فاتورة")
            if len(items) < 10:
                logger.info(f"✅ انتهينا من فرع {branch}")
                break

            page += 1
            time.sleep(1)

    logger.info(f"📦 عدد الفواتير اللي بنعالجها: {len(all_invoices)}")

    # إرسال إلى Supabase
    if all_invoices:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=id",
            headers=HEADERS_SUPABASE,
            json=all_invoices
        )

    if all_items:
        requests.post(
            f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=id",
            headers=HEADERS_SUPABASE,
            json=all_items
        )

    logger.info(f"✅ تم حفظ {len(all_invoices)} فاتورة، و{len(all_items)} بند.")
    return {"invoices": len(all_invoices), "items": len(all_items)}
