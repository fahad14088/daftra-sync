import time
import requests
import logging
from config import BASE_URL, BRANCH_IDS, PAGE_LIMIT, EXPECTED_TYPE, HEADERS_DAFTRA, SUPABASE_URL, HEADERS_SUPABASE

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

def fetch_invoice_details(inv_id):
    url = f"{BASE_URL}/v2/api/entity/invoice/{inv_id}?include=invoice_item"
    return fetch_with_retry(url, HEADERS_DAFTRA)

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
                invoice_data = fetch_invoice_details(inv["id"])
                if not invoice_data or not isinstance(invoice_data.get("invoice_item"), list):
                    logger.error(f"❌ فشل قراءة البنود للفاتورة {inv['id']}")
                    continue

                print(f"📑 فاتورة {inv['id']} فيها {len(invoice_data['invoice_item'])} بند")

                all_invoices.append({
                    "id": str(invoice_data["id"]),
                    "invoice_no": invoice_data["no"],
                    "invoice_date": invoice_data["date"],
                    "customer_id": invoice_data.get("client_id"),
                    "total": invoice_data.get("summary_total", 0),
                    "branch": invoice_data.get("branch_id"),
                    "created_at": invoice_data.get("created"),
                    "client_id": invoice_data.get("client_id"),
                    "client_business_name": invoice_data.get("client_business_name"),
                    "client_city": invoice_data.get("client_city"),
                    "summary_paid": invoice_data.get("summary_paid"),
                    "summary_unpaid": invoice_data.get("summary_unpaid")
                })

                for item in invoice_data["invoice_item"]:
                    all_items.append({
                        "id": str(item["id"]),
                        "invoice_id": str(invoice_data["id"]),
                        "quantity": item.get("quantity", 0),
                        "unit_price": item.get("unit_price", 0),
                        "total_price": item.get("subtotal", 0),
                        "product_id": item.get("product_id"),
                        "product_code": item.get("item"),
                        "client_business_name": invoice_data.get("client_business_name")
                    })

            if len(items) < 10:
                logger.info(f"✅ انتهينا من فواتير فرع {branch}، عدد الصفحات: {page}")
                break

            page += 1
            time.sleep(1)

    logger.info(f"📦 عدد الفواتير اللي بنعالجها: {len(all_invoices)}")

    if all_invoices:
        requests.post(f"{SUPABASE_URL}/rest/v1/invoices", headers=HEADERS_SUPABASE, json=all_invoices)

    if all_items:
        requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items", headers=HEADERS_SUPABASE, json=all_items)

    logger.info(f"✅ تم حفظ {len(all_invoices)} فاتورة، و {len(all_items)} بند مبيعات.")
    return {"invoices": len(all_invoices), "items": len(all_items)}
