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
                logger.warning(f"\u26a0\ufe0f \u0645\u062d\u0627\u0648\u0644\u0629 {attempt+1} \u0641\u0634\u0644\u062a: {response.status_code} - {response.text}")
        except Exception as e:
            logger.warning(f"\u26a0\ufe0f \u0645\u062d\u0627\u0648\u0644\u0629 {attempt+1} \u0641\u0634\u0644\u062a: {str(e)}")
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
                logger.warning(f"\u26a0\ufe0f \u0641\u0634\u0644 \u0641\u064a \u062c\u0644\u0628 \u0627\u0644\u0628\u064a\u0627\u0646\u0627\u062a \u0644\u0644\u0641\u0631\u0639 {branch} \u0627\u0644\u0635\u0641\u062d\u0629 {page}")
                break

            items = data.get("data") or []
            if not isinstance(items, list):
                items = [items]

            valid_items = [inv for inv in items if int(inv.get("type", -1)) == EXPECTED_TYPE]
            logger.info(f"\ud83d\udcc4 \u0641\u0631\u0639 {branch} - \u0635\u0641\u062d\u0629 {page} \u0641\u064a\u0647\u0627 {len(valid_items)} \u0641\u0627\u062a\u0648\u0631\u0629")
            if not valid_items:
                break

            for inv in valid_items:
                invoice_data = fetch_invoice_details(inv["id"])
                if not invoice_data or not isinstance(invoice_data.get("invoice_item"), list):
                    logger.error(f"\u274c \u0641\u0634\u0644 \u0642\u0631\u0627\u0621\u0629 \u0627\u0644\u0628\u0646\u0648\u062f \u0644\u0644\u0641\u0627\u062a\u0648\u0631\u0629 {inv['id']}")
                    continue

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
                logger.info(f"\u2705 \u0627\u0646\u062a\u0647\u064a\u0646\u0627 \u0645\u0646 \u0641\u0648\u0627\u062a\u064a\u0631 \u0641\u0631\u0639 {branch} \u060c \u0639\u062f\u062f \u0627\u0644\u0635\u0641\u062d\u0627\u062a: {page}")
                break

            page += 1
            time.sleep(1)

    logger.info(f"\ud83d\udce6 \u0639\u062f\u062f \u0627\u0644\u0641\u0648\u0627\u062a\u064a\u0631 \u0627\u0644\u0644\u064a \u0628\u0646\u0639\u0627\u0644\u062c\u0647\u0627: {len(all_invoices)}")

    if all_invoices:
        requests.post(f"{SUPABASE_URL}/rest/v1/invoices", headers=HEADERS_SUPABASE, json=all_invoices)

    if all_items:
        requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items", headers=HEADERS_SUPABASE, json=all_items)

    logger.info(f"\u2705 \u062a\u0645 \u062d\u0641\u0638 {len(all_invoices)} \u0641\u0627\u062a\u0648\u0631\u0629\u060c \u0648 {len(all_items)} \u0628\u0646\u062f \u0645\u0628\u064a\u0639\u0627\u062a.")
    return {"invoices": len(all_invoices), "items": len(all_items)}
