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
        logger.warning(f"❌ لم يتم العثور على بيانات الفاتورة {invoice_id}")
        return None
    
    invoice = data["data"]["Invoice"]
    
    # إضافة تسجيل لفهم بنية البيانات
    logger.info(f"🔍 الفاتورة {invoice_id}: البنود = {len(invoice.get('InvoiceItem', []))}")
    logger.info(f"🔍 المجموع = {invoice.get('total')}")
    logger.info(f"🔍 مفاتيح البيانات المتاحة: {list(invoice.keys())}")
    
    # تسجيل بعض مفاتيح البنود إذا كانت موجودة
    items = invoice.get('InvoiceItem', [])
    if items and len(items) > 0:
        logger.info(f"🔍 مفاتيح البند الأول: {list(items[0].keys())}")
    
    return invoice

def safe_convert_to_float(value, default=0.0):
    """تحويل آمن للأرقام"""
    try:
        if value is None or value == "":
            return default
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_convert_to_string(value, default=""):
    """تحويل آمن للنصوص"""
    if value is None:
        return default
    return str(value)

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

                # استخراج بيانات الفاتورة مع التحقق من صحة البيانات
                invoice_data = {
                    "id": safe_convert_to_string(inv.get("id")),
                    "invoice_no": safe_convert_to_string(inv.get("no")),
                    "invoice_date": safe_convert_to_string(inv.get("date")),
                    "customer_id": safe_convert_to_string(inv.get("contact_id")),
                    "total": safe_convert_to_float(inv.get("total")),
                    "branch": safe_convert_to_string(inv.get("branch_id")),
                    "created_at": safe_convert_to_string(inv.get("created_at")),
                    "client_id": safe_convert_to_string(inv.get("Contact", {}).get("id")),
                    "client_business_name": safe_convert_to_string(inv.get("Contact", {}).get("business_name")),
                    "client_city": safe_convert_to_string(inv.get("Contact", {}).get("city")),
                    "summary_paid": safe_convert_to_float(inv.get("summary", {}).get("paid")),
                    "summary_unpaid": safe_convert_to_float(inv.get("summary", {}).get("unpaid"))
                }
                
                all_invoices.append(invoice_data)

                # البحث عن البنود بأسماء مختلفة محتملة
                invoice_items = inv.get("InvoiceItem", [])
                if not invoice_items:
                    # جرب أسماء أخرى محتملة للبنود
                    invoice_items = (inv.get("items", []) or 
                                   inv.get("Items", []) or 
                                   inv.get("invoiceItems", []) or
                                   inv.get("invoice_items", []) or
                                   inv.get("LineItems", []) or
                                   inv.get("line_items", []))

                logger.info(f"📋 الفاتورة {inv.get('id')}: وُجد {len(invoice_items)} بند")

                # إذا لم نجد بنود، اطبع كل مفاتيح الفاتورة لفهم البنية
                if not invoice_items:
                    logger.warning(f"⚠️ لم يتم العثور على بنود في الفاتورة {inv.get('id')}")
                    logger.info(f"🔍 جميع مفاتيح الفاتورة: {list(inv.keys())}")
                    # تحقق من وجود مفاتيح تحتوي على كلمة item
                    item_keys = [key for key in inv.keys() if 'item' in key.lower()]
                    if item_keys:
                        logger.info(f"🔍 مفاتيح تحتوي على 'item': {item_keys}")

                for item in invoice_items:
                    # تأكد من وجود البيانات الأساسية
                    if not item.get("id"):
                        logger.warning(f"⚠️ بند بدون ID في الفاتورة {inv.get('id')}")
                        continue
                    
                    item_data = {
                        "id": safe_convert_to_string(item.get("id")),
                        "invoice_id": safe_convert_to_string(inv.get("id")),
                        "product_id": safe_convert_to_string(item.get("product_id")),
                        "product_code": safe_convert_to_string(item.get("product_code")),
                        "quantity": safe_convert_to_float(item.get("quantity")),
                        "unit_price": safe_convert_to_float(item.get("unit_price")),
                        "total_price": safe_convert_to_float(item.get("total")),
                        "client_business_name": safe_convert_to_string(inv.get("Contact", {}).get("business_name"))
                    }
                    
                    all_items.append(item_data)

            if len(items) < 10:
                logger.info(f"✅ انتهينا من فرع {branch} صفحة {page}")
                break

            page += 1
            time.sleep(1)

    logger.info(f"📦 إجمالي الفواتير: {len(all_invoices)}")
    logger.info(f"📦 إجمالي البنود: {len(all_items)}")

    # حفظ الفواتير مع التحقق من النجاح
    if all_invoices:
        try:
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoices",
                headers=HEADERS_SUPABASE,
                json=all_invoices
            )
            if response.status_code == 201:
                logger.info(f"✅ تم حفظ {len(all_invoices)} فاتورة بنجاح")
            else:
                logger.error(f"❌ فشل حفظ الفواتير: {response.status_code}")
                logger.error(f"❌ تفاصيل الخطأ: {response.text}")
        except Exception as e:
            logger.error(f"❌ خطأ في حفظ الفواتير: {str(e)}")

    # حفظ البنود مع التحقق من النجاح
    if all_items:
        try:
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items",
                headers=HEADERS_SUPABASE,
                json=all_items
            )
            if response.status_code == 201:
                logger.info(f"✅ تم حفظ {len(all_items)} بند بنجاح")
            else:
                logger.error(f"❌ فشل حفظ البنود: {response.status_code}")
                logger.error(f"❌ تفاصيل الخطأ: {response.text}")
        except Exception as e:
            logger.error(f"❌ خطأ في حفظ البنود: {str(e)}")
    else:
        logger.warning("⚠️ لم يتم العثور على أي بنود لحفظها")

    logger.info(f"✅ تم حفظ {len(all_invoices)} فاتورة مبيعات جديدة.")
    logger.info(f"✅ البنود: {len(all_items)} بند")

    return {
        "invoices": len(all_invoices),
        "items": len(all_items)
    }
