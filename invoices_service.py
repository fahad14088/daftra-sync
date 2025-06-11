import os
import requests
import time
import uuid
import logging
from datetime import datetime
import hashlib
import json
import traceback  # تم إضافة هذا السطر

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

# المتغيرات
DAFTRA_URL     = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY  = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number):
    hash_input  = f"invoice-{number}".encode('utf-8')
    hash_digest = hashlib.md5(hash_input).hexdigest()
    return f"{hash_digest[:8]}-{hash_digest[8:12]}-{hash_digest[12:16]}-{hash_digest[16:20]}-{hash_digest[20:32]}"

def safe_float(value, default=0.0):
    """تحويل آمن للرقم"""
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ""))
    except Exception as e:
        logger.error(f"❌ خطأ في تحويل القيمة '{value}' إلى رقم: {e}", exc_info=True)
        return default

def safe_string(value, max_length=None):
    """تحويل آمن للنص"""
    try:
        if value is None:
            return ""
        result = str(value).strip()
        if max_length and len(result) > max_length:
            result = result[:max_length]
        return result
    except Exception as e:
        logger.error(f"❌ خطأ في تحويل القيمة '{value}' إلى نص: {e}", exc_info=True)
        return ""

def get_all_invoices_complete():
    """جلب جميع الفواتير من جميع الصفحات"""
    logger.info("📥 جلب جميع الفواتير...")
    headers = {"apikey": DAFTRA_APIKEY}
    all_invoices = []
    page = 1

    while True:
        try:
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit=100"
            logger.info(f"📄 الصفحة {page}")
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                logger.error(f"❌ خطأ في الصفحة {page}: {response.text}")
                break

            data     = response.json()
            invoices = data.get("data", [])
            if not invoices:
                logger.info("✅ انتهت الفواتير")
                break

            logger.info(f"📊 وجدت {len(invoices)} فاتورة")
            all_invoices.extend(invoices)
            page += 1
            time.sleep(1)

        except Exception as e:
            logger.error(f"❌ خطأ في جلب الصفحة {page}: {e}", exc_info=True)
            break

    logger.info(f"📋 إجمالي الفواتير: {len(all_invoices)}")
    return all_invoices

def get_invoice_full_details(invoice_id):
    """جلب تفاصيل الفاتورة الكاملة"""
    headers = {"apikey": DAFTRA_APIKEY}
    for branch in range(1, 10):
        try:
            url      = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{invoice_id}"
            response = requests.get(url, headers=headers, timeout=30)
            if response.status_code != 200:
                continue

            data = response.json().get("data", {})
            # جرب مفاتيح مختلفة
            invoice_data = None
            if isinstance(data, dict):
                if data.get("Invoice"):
                    invoice_data = data["Invoice"]
                elif data.get("invoice_item") or data.get("InvoiceItem"):
                    invoice_data = data
            if invoice_data and invoice_data.get("id"):
                logger.debug(f"✅ وجدت تفاصيل الفاتورة {invoice_id} في الفرع {branch}")
                return invoice_data

        except Exception as e:
            logger.error(f"❌ خطأ أثناء جلب تفاصيل الفاتورة {invoice_id} من الفرع {branch}: {e}", exc_info=True)
            continue

    logger.warning(f"⚠️ لم أجد تفاصيل للفاتورة {invoice_id}")
    return None

def save_invoice_complete(invoice_summary, invoice_details=None):
    """حفظ الفاتورة بجميع البيانات"""
    try:
        invoice_id   = str(invoice_summary["id"])
        invoice_uuid = generate_uuid_from_number(invoice_id)
        headers = {
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type":  "application/json"
        }
        source_data = invoice_details or invoice_summary

        payload = {
            "id":                 invoice_uuid,
            "invoice_no":         safe_string(source_data.get("no", "")),
            "total":              safe_float(source_data.get("total", 0)),
            "invoice_date":       safe_string(source_data.get("date", "")),
            "client_business_name": safe_string(source_data.get("client_business_name", ""), 255),
            "customer_id":        safe_string(source_data.get("customer_id", "")),
        }

        if source_data.get("paid_amount") is not None:
            paid_amount  = safe_float(source_data.get("paid_amount", 0))
            total_amount = safe_float(source_data.get("total", 0))
            payload["summary_paid"]   = paid_amount
            payload["summary_unpaid"] = max(0, total_amount - paid_amount)

        if invoice_details:
            payload["notes"]      = safe_string(invoice_details.get("notes", ""), 500)
            payload["created_at"] = safe_string(invoice_details.get("created_at", ""))

        clean_payload = {k: v for k, v in payload.items() if v not in [None, "", "None"]}

        logger.info(f"💾 حفظ الفاتورة {invoice_id} - المبلغ: {clean_payload.get('total', 0)}")
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=id",
            headers=headers,
            json=clean_payload,
            timeout=30
        )
        if response.status_code in (200, 201, 409):
            logger.info(f"✅ تم حفظ الفاتورة {invoice_id}")
            return invoice_uuid
        else:
            logger.error(f"❌ فشل حفظ الفاتورة {invoice_id}: {response.status_code} {response.text}")
            return None

    except Exception as e:
        logger.error(f"❌ خطأ في حفظ الفاتورة {invoice_summary.get('id')}: {e}", exc_info=True)
        return None

def save_invoice_items_complete(invoice_uuid, invoice_id, details):
    """حفظ بنود الفاتورة بشكل كامل"""
    # حدّد المفتاح الصحيح للبنود
    for key in ("invoice_item", "InvoiceItem", "invoice_items"):
        if details.get(key) is not None:
            items = details[key]
            break
    else:
        logger.warning(f"⚠️ لم أجد بنود للفاتورة {invoice_id} تحت أي مفتاح متوقع")
        return 0

    if not isinstance(items, list):
        items = [items]

    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json"
    }
    saved_count = 0
    logger.info(f"📦 حفظ {len(items)} بند للفاتورة {invoice_id}")

    for i, item in enumerate(items, 1):
        try:
            quantity   = safe_float(item.get("quantity", 0))
            unit_price = safe_float(item.get("unit_price", item.get("price", 0)))
            if quantity <= 0:
                continue

            raw_id    = item.get("id") or f"{invoice_id}-{i}"
            item_uuid = generate_uuid_from_number(f"item-{raw_id}-{invoice_id}")

            payload = {
                "id":          item_uuid,
                "invoice_id":  invoice_uuid,
                "quantity":    quantity,
                "unit_price":  unit_price,
                "total_price": quantity * unit_price,
                "product_id":  safe_string(item.get("product_id", "")),
                "product_code": safe_string(item.get("product_code", "")),
            }

            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=id",
                headers=headers,
                json=payload,
                timeout=30
            )
            if response.status_code in (200, 201, 409):
                saved_count += 1
            else:
                logger.error(f"❌ فشل حفظ البند {i}: {response.status_code} {response.text}")

        except Exception as e:
            logger.error(f"❌ خطأ في البند {i}: {e}", exc_info=True)

    logger.info(f"✅ تم حفظ {saved_count} بند")
    return saved_count

def sync_invoices():
    """المزامنة الشاملة النهائية"""
    logger.info("🚀 بدء المزامنة الشاملة...")
    result = {"invoices": 0, "items": 0, "errors": []}

    try:
        all_invoices = get_all_invoices_complete()
        if not all_invoices:
            logger.error("❌ لا توجد فواتير!")
            return result

        logger.info(f"📋 معالجة {len(all_invoices)} فاتورة...")
        for i, invoice in enumerate(all_invoices, 1):
            try:
                invoice_id = str(invoice["id"])
                if i % 100 == 0:
                    logger.info(f"🔄 معالجة {i}/{len(all_invoices)}: الفاتورة {invoice_id}")

                details = get_invoice_full_details(invoice_id)
                invoice_uuid = save_invoice_complete(invoice, details)
                if invoice_uuid:
                    result["invoices"] += 1
                    if details:
                        saved_items = save_invoice_items_complete(invoice_uuid, invoice_id, details)
                        result["items"] += saved_items

                if i % 50 == 0:
                    time.sleep(2)

            except Exception as e:
                msg = f"خطأ في الفاتورة {invoice.get('id')}: {e}"
                result["errors"].append(msg)
                logger.error(f"❌ {msg}", exc_info=True)

        # ملخص النتائج
        logger.info("=" * 60)
        logger.info(f"📊 إجمالي الفواتير: {len(all_invoices)}")
        logger.info(f"✅ فواتير محفوظة: {result['invoices']}")
        logger.info(f"📦 بنود محفوظة: {result['items']}")
        logger.info(f"❌ أخطاء: {len(result['errors'])}")
        if len(all_invoices):
            rate = (result['invoices'] / len(all_invoices)) * 100
            logger.info(f"🏆 معدل النجاح: {rate:.1f}%")
        logger.info("=" * 60)

        return result

    except Exception as e:
        err = f"خطأ عام: {e}"
        result["errors"].append(err)
        logger.error(f"💥 {err}", exc_info=True)
        return result

if __name__ == "__main__":
    sync_invoices()
