import time
import requests
import logging
import os

from config import BASE_URL, BRANCH_IDS, PAGE_LIMIT, EXPECTED_TYPE, HEADERS_DAFTRA, HEADERS_SUPABASE, SUPABASE_URL

# إعدادات التسجيل
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
                invoice_data = {
                    "id": inv_id,
                    "invoice_no": inv["no"],
                    "invoice_date": inv["date"],
                    "created_at": inv.get("created_at"),
                    "contact_id": inv.get("contact_id"),
                    "branch_id": inv.get("branch_id"),
                    "staff_id": inv.get("staff_id"),
                    "total": inv.get("total", 0),
                    "invoice_type": inv.get("type", 0)
                }
                all_invoices.append(invoice_data)

                for item in inv.get("InvoiceItem", []):
                    item_data = {
                        "id": f"{inv_id}-{item.get('id')}",
                        "invoice_id": inv_id,
                        "product_id": item.get("product_id"),
                        "description": item.get("description"),
                        "quantity": item.get("quantity", 0),
                        "unit_price": item.get("unit_price", 0),
                        "total_price": item.get("total", 0)
                    }
                    all_items.append(item_data)

            if len(items) < 10:
                logger.info(f"✅ انتهينا من فواتير فرع {branch}، عدد الصفحات: {page}")
                break

            page += 1
            time.sleep(1)

    logger.info(f"📦 عدد الفواتير اللي بنعالجها: {len(all_invoices)}")

    # حفظ الفواتير إلى Supabase (سجل بسجل)
    for inv in all_invoices:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=HEADERS_SUPABASE,
            json=inv
        )
        if resp.status_code not in (200, 201):
            logger.warning(f"⚠️ فشل حفظ الفاتورة {inv.get('id')}: {resp.status_code} - {resp.text}")

    # حفظ البنود إلى Supabase (سجل بسجل)
    for item in all_items:
        resp = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoice_items",
            headers=HEADERS_SUPABASE,
            json=item
        )
        if resp.status_code not in (200, 201):
            logger.warning(f"⚠️ فشل حفظ البند للفاتورة {item.get('invoice_id')}: {resp.status_code} - {resp.text}")

    logger.info(f"✅ تم حفظ {len(all_invoices)} فاتورة مبيعات جديدة.")
    logger.info(f"✅ الفواتير: {len(all_invoices)} فاتورة، {len(all_items)} بند")

    return {
        "invoices": len(all_invoices),
        "items": len(all_items)
    }
