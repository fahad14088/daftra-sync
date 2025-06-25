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
        data = fetch_with_retry(url, HEADERS_DAFTRA)
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
                "product_id":        str(pid),
                "daftra_product_id": str(pid),
                "product_code":      safe_text(code),
                "name":              safe_text(prod.get("name", "")),
                "stock_balance":     safe_number(prod.get("stock_balance", 0)),
                "buy_price":         safe_number(prod.get("buy_price", 0)),
                "average_price":     safe_number(prod.get("average_price", 0)),
                "minimum_price":     safe_number(prod.get("minimum_price", 0)),
                "supplier_code":     safe_text(prod.get("supplier_code", ""))
            }

            print(">> upsert product:", payload)
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/products?on_conflict=product_id",
                headers={**HEADERS_SB, "Prefer": "resolution=merge-duplicates"},
                json=payload,
                timeout=10
            )
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


def fix_invoice_items_using_product_id():
    print("🔧 تصحيح كود المنتج في البنود باستخدام product_id...")

    url_products = f"{SUPABASE_URL}/rest/v1/products?select=product_id,product_code"
    res = requests.get(url_products, headers=HEADERS_SB)
    if res.status_code != 200:
        print("❌ فشل في جلب المنتجات")
        return

    # بناء قاموس المنتجات
    product_map = {}
    for p in res.json():
        pid = str(p.get("product_id", "")).strip()
        code = p.get("product_code")
        if pid and code:
            product_map[pid] = code

    print(f"📦 عدد المنتجات المحملة: {len(product_map)}")

    limit = 1000
    offset = 0
    total_updated = 0

    while True:
        url_items = f"{SUPABASE_URL}/rest/v1/invoice_items?select=id,product_id&limit={limit}&offset={offset}"
        res = requests.get(url_items, headers=HEADERS_SB)
        if res.status_code != 200:
            print("❌ فشل في جلب البنود")
            break

        batch = res.json()
        if not batch:
            break

        print(f"🔍 فحص {len(batch)} بند من offset={offset}")
        for row in batch:
            item_id = row["id"]
            pid = str(row.get("product_id", "")).strip()
            actual_code = product_map.get(pid)  # ✅ الطريقة الصحيحة بدون لف ودوران

            if actual_code:
                patch_url = f"{SUPABASE_URL}/rest/v1/invoice_items?id=eq.{item_id}"
                patch_payload = {"product_code": actual_code}
                res_patch = requests.patch(patch_url, headers=HEADERS_SB, json=patch_payload)
                print(f"🔄 تحديث بند {item_id} ← {pid} → {actual_code} → {res_patch.status_code}")
                if res_patch.status_code in [200, 204]:
                    total_updated += 1
            else:
                print(f"⚠️ لم يتم العثور على كود لـ product_id={pid}")

        offset += limit
        time.sleep(0.5)

    print(f"✅ تم تحديث {total_updated} بند بنجاح.")
