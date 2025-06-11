import os
import requests
import time
import uuid
import logging
import hashlib

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)

DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

def generate_uuid_from_number(number):
    h = hashlib.md5(f"invoice-{number}".encode()).hexdigest()
    return f"{h[:8]}-{h[8:12]}-{h[12:16]}-{h[16:20]}-{h[20:32]}"

def safe_float(v, default=0.0):
    try:
        if v is None or v == "": return default
        return float(str(v).replace(",", ""))
    except Exception as e:
        logger.error(f"❌ safe_float('{v}') failed: {e}", exc_info=True)
        return default

def safe_string(v, max_length=None):
    try:
        s = "" if v is None else str(v).strip()
        return s if not max_length or len(s) <= max_length else s[:max_length]
    except Exception as e:
        logger.error(f"❌ safe_string('{v}') failed: {e}", exc_info=True)
        return ""

def get_all_invoices_complete():
    headers = {"apikey": DAFTRA_APIKEY}
    all_invoices, page = [], 1
    while True:
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit=100"
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            logger.error(f"❌ list page {page} failed: {resp.text}")
            break
        data = resp.json().get("data", [])
        if not data:
            break
        all_invoices.extend(data)
        logger.info(f"📄 page {page}: got {len(data)} invoices")
        page += 1
        time.sleep(1)
    logger.info(f"📋 total invoices: {len(all_invoices)}")
    return all_invoices

def get_invoice_full_details(invoice_id):
    headers = {"apikey": DAFTRA_APIKEY}
    for branch in range(1, 10):
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/show/{branch}/{invoice_id}"
        resp = requests.get(url, headers=headers, timeout=30)
        if resp.status_code != 200:
            continue
        payload = resp.json().get("data", {})
        if not isinstance(payload, dict):
            continue
        # debug: log what keys arrived
        logger.debug(f"📦 invoice {invoice_id} data keys: {list(payload.keys())}")
        return payload
    logger.warning(f"⚠️ no full details for invoice {invoice_id}")
    return None

def save_invoice_complete(summary, details=None):
    inv_id = str(summary["id"])
    inv_uuid = generate_uuid_from_number(inv_id)
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json"
    }
    src = details.get("Invoice") if details and details.get("Invoice") else summary
    payload = {
        "id":    inv_uuid,
        "invoice_no": safe_string(src.get("no", "")),
        "total": safe_float(src.get("total", 0)),
        "invoice_date": safe_string(src.get("date", "")),
        "client_business_name": safe_string(src.get("client_business_name", ""), 255),
        "customer_id": safe_string(src.get("customer_id", "")),
    }
    if src.get("paid_amount") is not None:
        paid = safe_float(src.get("paid_amount", 0))
        payload["summary_paid"]   = paid
        payload["summary_unpaid"] = max(0, payload["total"] - paid)
    if details:
        if details.get("notes"):
            payload["notes"] = safe_string(details["notes"], 500)
        if details.get("created_at"):
            payload["created_at"] = safe_string(details["created_at"])
    # remove empty
    clean = {k: v for k, v in payload.items() if v not in ("", None)}
    resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoices?on_conflict=id",
                         headers=headers, json=clean, timeout=30)
    if resp.status_code in (200,201,409):
        logger.info(f"✅ saved invoice {inv_id}")
        return inv_uuid
    logger.error(f"❌ failed save invoice {inv_id}: {resp.status_code} {resp.text}")
    return None

def save_invoice_items_complete(inv_uuid, inv_id, details):
    # pick items under any of these keys
    for key in ("invoice_item", "InvoiceItem", "invoice_items"):
        if details.get(key) is not None:
            items = details[key]
            break
    else:
        logger.warning(f"⚠️ no items for invoice {inv_id}")
        return 0

    if not isinstance(items, list):
        items = [items]
    headers = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json"
    }
    saved = 0
    for idx, it in enumerate(items, 1):
        qty = safe_float(it.get("quantity", 0))
        price = safe_float(it.get("unit_price", it.get("price", 0)))
        if qty <= 0:
            continue
        raw = it.get("id") or f"{inv_id}-{idx}"
        item_uuid = generate_uuid_from_number(f"item-{raw}-{inv_id}")
        pl = {
            "id":         item_uuid,
            "invoice_id": inv_uuid,
            "quantity":   qty,
            "unit_price": price,
            "total_price": qty * price,
            "product_id":  safe_string(it.get("product_id", "")),
            "product_code": safe_string(it.get("product_code", "")),
        }
        resp = requests.post(f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=id",
                             headers=headers, json=pl, timeout=30)
        if resp.status_code in (200,201,409):
            saved += 1
        else:
            logger.error(f"❌ item {idx} save failed: {resp.status_code} {resp.text}")
    logger.info(f"✅ saved {saved} items for invoice {inv_id}")
    return saved

def sync_invoices():
    logger.info("🚀 start sync")
    result = {"invoices":0, "items":0, "errors":[]}
    invs = get_all_invoices_complete()
    if not invs:
        return result
    for i, inv in enumerate(invs,1):
        try:
            inv_id = str(inv["id"])
            if i % 100 == 0:
                logger.info(f"🔄 processing {i}/{len(invs)} inv {inv_id}")
            details = get_invoice_full_details(inv_id)
            inv_uuid = save_invoice_complete(inv, details)
            if inv_uuid:
                result["invoices"] += 1
                if details:
                    cnt = save_invoice_items_complete(inv_uuid, inv_id, details)
                    result["items"] += cnt
            if i % 50 == 0:
                time.sleep(2)
        except Exception as e:
            msg = f"error inv {inv.get('id')}: {e}"
            result["errors"].append(msg)
            logger.error(msg, exc_info=True)
    logger.info(f"✅ done: saved {result['invoices']} invoices, {result['items']} items, errors {len(result['errors'])}")
    return result

if __name__ == "__main__":
    sync_invoices()
