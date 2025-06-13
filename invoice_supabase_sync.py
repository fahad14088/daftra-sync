import time
import requests
import logging
from config import BASE_URL, BRANCH_IDS, PAGE_LIMIT, EXPECTED_TYPE, HEADERS_DAFTRA

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

def get_all_invoices():
    invoices = []
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
            invoices.extend(valid_items)
            logger.info(f"📄 فرع {branch} - صفحة {page} فيها {len(valid_items)} فاتورة")

            if len(items) < 10:
                logger.info(f"✅ انتهينا من فواتير فرع {branch}، عدد الصفحات: {page}")
                break

            page += 1
            time.sleep(2)

    logger.info(f"📦 عدد الفواتير اللي بنعالجها: {len(invoices)}")
    return invoices
