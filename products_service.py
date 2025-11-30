import os
import requests
import time

DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": DAFTRA_APIKEY}
HEADERS_SB     = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
}

# ====== إعدادات موحدة (نفس config.py عندك) ======
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES     = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY     = int(os.getenv("RETRY_DELAY", "2"))


# ====== Request helpers ======
def fetch_with_retry(url, headers, retries=3, timeout=30):
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            print(f"> GET {url} → {r.status_code}")
            if r.status_code == 200:
                return r.json()
        except Exception as e:
            print("! fetch error:", e)
        time.sleep((i + 1) * 5)
    return None


def supabase_request_with_retry(
    method,
    url,
    headers=None,
    json=None,
    retries=MAX_RETRIES,
    timeout=REQUEST_TIMEOUT
):
    """
    نفس فكرة fetch_with_retry لكن لـ Supabase.
    - يعيد المحاولة على timeouts/connection errors
    - لو كل المحاولات فشلت يرفع Exception (ونحن بنمسكه في مكان الاستدعاء عشان ما يرجع Page 1)
    """
    last_err = None
    for i in range(retries):
        try:
            r = requests.request(method, url, headers=headers, json=json, timeout=timeout)
            return r
        except Exception as e:
            last_err = e
            msg = str(e)
            print(f"! Supabase {method} error (try {i+1}/{retries}):", msg)
            if i == retries - 1:
                raise last_err
            time.sleep(RETRY_DELAY * (i + 1))


def safe_number(value):
    try:
        return float(value)
    except:
        return 0


def safe_text(value):
    return str(value) if value is not None else ""


def sync_products():
    created_count = 0
    updated_count = 0
    page = 1
    limit = 50

    while True:
        url = f"{DAFTRA_URL}/v2/api/entity/product/list/1?page={page}&limit={limit}"
        data = fetch_with_retry(
            url,
            HEADERS_DAFTRA,
            retries=MAX_RETRIES,
            timeout=REQUEST_TIMEOUT
        )

        items = data.get("data", []) if data else []
        print(f"> Page {page}: found {len(items)} items")
        if not items:
            break

        for raw in items:
            prod = raw.get("Product") if isinstance(raw, dict) and "Product" in raw else raw

            pid = prod.get("id")
            if not pid:
                print("! skipping item without id:", prod)
                continue

            code = (
                prod.get("code")
                or prod.get("product_code")
                or prod.get("supplier_code")
                or ""
            )

            payload = {
                "product_id":        pid,
                "daftra_product_id": str(pid),
                "product_code":      safe_text(code),
                "name":              safe_text(prod.get("name", "")),
                "stock_balance":     safe_number(prod.get("stock_balance", 0)),
                "buy_price":         safe_number(prod.get("buy_price", 0)),
                "average_price":     safe_number(prod.get("average_price", 0)),
                "minimum_price":     safe_number(prod.get("minimum_price", 0)),
                "supplier_code":     safe_text(prod.get("supplier_code", ""))
            }

            payload = {k: v for k, v in payload.items() if v is not None and k != "id"}

            print(">> upsert product:", payload)

            # ====== مهم: لو فشل Supabase لا نكسر اللوب ولا نرجع Page 1 ======
            try:
                resp = supabase_request_with_retry(
                    "POST",
                    f"{SUPABASE_URL}/rest/v1/products?on_conflict=product_id",
                    headers={**HEADERS_SB, "Prefer": "resolution=merge-duplicates"},
                    json=payload,
                )
            except Exception as e:
                print("! upsert failed, skipping product:", pid, "| error:", e)
                continue  # يكمل على المنتج اللي بعده

            # أمان إضافي لو رجّع None لأي سبب
            if resp is None:
                print("! upsert got no response, skipping product:", pid)
                continue

            print(f"   → {resp.status_code} | {resp.text}")
            if resp.status_code == 201:
                created_count += 1
            elif resp.status_code == 200:
                updated_count += 1

        page += 1
        time.sleep(1)

    total = created_count + updated_count
    print(f"\n✅ تم رفع {created_count} منتج جديد")
    print(f"🔁 تم تحديث {updated_count} منتج موجود")
    print(f"📦 الإجمالي: {total} منتج\n")

    return {"synced": total}


def fix_invoice_items_product_id_using_code():
    print("🔧 تصحيح شامل للبنود (product_id + product_code) من المنتجات...")

    # 1. تحميل المنتجات
    url_products = f"{SUPABASE_URL}/rest/v1/products?select=product_id,product_code,name"
    try:
        res = supabase_request_with_retry("GET", url_products, headers=HEADERS_SB)
    except Exception as e:
        print("❌ فشل في جلب المنتجات بسبب خطأ اتصال:", e)
        return

    if res is None or res.status_code != 200:
        print("❌ فشل في جلب المنتجات")
        return

    code_map = {}
    for p in res.json():
        pid = p.get("product_id")
        code = p.get("product_code", "").strip()
        name = p.get("name", "").strip()

        if pid:
            if code:
                code_map[code] = {"product_id": pid, "product_code": code}
            if name and name not in code_map:
                code_map[name] = {"product_id": pid, "product_code": code}

    print(f"📦 عدد المنتجات المحملة: {len(code_map)}")

    # 2. تحديث البنود
    limit = 1000
    offset = 0
    total_updated = 0

    while True:
        url_items = f"{SUPABASE_URL}/rest/v1/invoice_items?select=id,product_id,product_code&limit={limit}&offset={offset}"

        try:
            res = supabase_request_with_retry("GET", url_items, headers=HEADERS_SB)
        except Exception as e:
            print("❌ فشل في جلب البنود بسبب خطأ اتصال:", e)
            break

        if res is None or res.status_code != 200:
            print("❌ فشل في جلب البنود")
            break

        items = res.json()
        if not items:
            break

        for row in items:
            item_id = row["id"]
            current_pid = row.get("product_id")
            current_code = row.get("product_code", "").strip()

            match = code_map.get(current_code)
            if not match:
                continue

            new_pid = match["product_id"]
            new_code = match["product_code"]

            if str(current_pid) != str(new_pid) or current_code != new_code:
                patch_url = f"{SUPABASE_URL}/rest/v1/invoice_items?id=eq.{item_id}"
                patch_payload = {
                    "product_id": new_pid,
                    "product_code": new_code
                }

                # ====== مهم: لو PATCH فشل ما نرجع من البداية ======
                try:
                    res_patch = supabase_request_with_retry(
                        "PATCH",
                        patch_url,
                        headers=HEADERS_SB,
                        json=patch_payload
                    )
                except Exception as e:
                    print("! patch failed, skipping item:", item_id, "| error:", e)
                    continue

                if res_patch is None:
                    print("! patch got no response, skipping item:", item_id)
                    continue

                if res_patch.status_code in [200, 204]:
                    print(f"✅ بند {item_id} ← product_id = {new_pid} ، code = {new_code}")
                    total_updated += 1

        offset += limit

    print(f"\n✅ تم تحديث {total_updated} بند بنجاح.")
