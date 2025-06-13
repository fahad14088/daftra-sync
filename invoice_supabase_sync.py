import time
import requests
import logging
import os
from config import BASE_URL, BRANCH_IDS, HEADERS_DAFTRA, HEADERS_SUPABASE, SUPABASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_invoice_details(inv_id):
    url = f"{BASE_URL}/v2/api/entity/invoice/view/{inv_id}"
    try:
        res = requests.get(url, headers=HEADERS_DAFTRA, timeout=15)
        if res.status_code == 200:
            return res.json().get("data", {}).get("Invoice", {})
        else:
            logger.warning(f"⚠️ فشل عرض الفاتورة {inv_id}: {res.status_code} - {res.text}")
    except Exception as e:
        logger.warning(f"⚠️ خطأ في تحميل الفاتورة {inv_id}: {e}")
    return None

def fetch_all():
    all_invoices = []
    all_items = []
    invoice_ids = []

    for branch in BRANCH_IDS:
        page = 1
        while True:
            url = f"{BASE_URL}/v2/api/entity/invoice/list/1"
            params = {
                "filter[branch_id]": branch,
                "page": page,
                "limit": 100
            }
            try:
                res = requests.get(url, headers=HEADERS_DAFTRA, params=params)
                if res.status_code != 200:
                    logger.warning(f"⚠️ صفحة {page} فرع {branch} فشلت: {res.status_code}")
                    break
                items = res.json().get("data", [])
                valid_ids = [str(i["id"]) for i in items if int(i.get("type", -1)) == 0]
                if not valid_ids:
                    break
                invoice_ids.extend(valid_ids)
                logger.info(f"📄 فرع {branch} - صفحة {page} فيها {len(valid_ids)} فاتورة")
                if len(items) < 10:
                    break
                page += 1
                time.sleep(1)
            except Exception as e:
                logger.error(f"❌ خطأ في صفحة {page} فرع {branch}: {e}")
                break

    logger.info(f"📦 عدد الفواتير اللي بنجيب تفاصيلها: {len(invoice_ids)}")

    for inv_id in invoice_ids:
        inv = fetch_invoice_details(inv_id)
        if not inv:
            continue

        all_invoices.append({
            "id": str(inv["id"]),
            "invoice_no": inv.get("no"),
            "invoice_date": inv.get("date"),
            "customer_id": str(inv.get("contact_id", "")),
            "total": inv.get("total", 0),
            "branch": inv.get("branch_id"),
            "created_at": inv.get("created_at"),
            "client_id": str(inv.get("Contact", {}).get("id", "")),
            "client_business_name": inv.get("Contact", {}).get("business_name"),
            "client_city": inv.get("Contact", {}).get("city"),
            "summary_paid": inv.get("summary", {}).get("paid", 0),
            "summary_unpaid": inv.get("summary", {}).get("unpaid", 0),
        })

        for item in inv.get("InvoiceItem", []):
            all_items.append({
                "id": f"{inv['id']}_{item.get('product_id', '')}",
                "invoice_id": str(inv["id"]),
                "product_id": str(item.get("product_id", "")),
                "product_code": item.get("product_code", ""),
                "quantity": item.get("quantity", 0),
                "unit_price": item.get("unit_price", 0),
                "total_price": item.get("total", 0),
                "client_business_name": inv.get("Contact", {}).get("business_name"),
            })

    logger.info(f"✅ تم تجهيز {len(all_invoices)} فاتورة و {len(all_items)} بند")

    if all_invoices:
        r1 = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=HEADERS_SUPABASE,
            json=all_invoices
        )
        logger.info(f"📤 حفظ فواتير: {r1.status_code}")

    if all_items:
        r2 = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoice_items",
            headers=HEADERS_SUPABASE,
            json=all_items
        )
        logger.info(f"📤 حفظ بنود: {r2.status_code}")

    return {
        "invoices": len(all_invoices),
        "items": len(all_items)
    }
