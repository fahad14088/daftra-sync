import os
import requests
import time

# إعدادات من البيئة
DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": DAFTRA_APIKEY}
HEADERS_SB     = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
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


def sync_products():
    total = 0
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
            # فك التغليف لو جاي بالشكل {"Product": { ... }}
            prod = raw.get("Product") if isinstance(raw, dict) and "Product" in raw else raw

            pid = prod.get("id")
            if not pid:
                print("! skipping item without id:", prod)
                continue

            # استخرج الكود من أي حقل متوفر
            code = (
                prod.get("code")
                or prod.get("product_code")
                or prod.get("supplier_code")
                or ""
            )

            payload = {
                "daftra_product_id": str(pid),
                "product_code":       code,
                "name":               prod.get("name", ""),
                "stock_balance":      str(prod.get("stock_balance", 0)),
                "buy_price":          str(prod.get("buy_price", 0)),
                "average_price":      str(prod.get("average_price", 0)),
                "minimum_price":      str(prod.get("minimum_price", 0)),
                "supplier_code":      prod.get("supplier_code", "")
            }

            print(">> upsert product:", payload)
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/products?on_conflict=daftra_product_id",
                headers=HEADERS_SB,
                json=payload,
                timeout=10
            )
            print(f"   → {resp.status_code}")
            if resp.status_code in (200, 201):
                total += 1

        page += 1
        time.sleep(1)

    print(f"✅ Done sync_products: {total} records")
    return {"synced": total}


def fix_invoice_items_based_on_product_name():
    """تصحيح كود المنتج في البنود إذا كان مكتوب فيه الاسم بدلاً من الكود"""
    print("🔧 بدء تصحيح البنود في جدول invoice_items حسب الاسم الموجود في product_code...")

    # 1. جلب المنتجات من Supabase
    url_products = f"{SUPABASE_URL}/rest/v1/products?select=name,product_code"
    res = requests.get(url_products, headers=HEADERS_SB)
    if res.status_code != 200:
        print("❌ فشل في جلب المنتجات")
        return

    product_map = {
        p["name"].strip(): p["product_code"]
        for p in res.json()
        if p.get("name") and p.get("product_code")
    }

    # 2. جلب البنود من Supabase
    url_items = f"{SUPABASE_URL}/rest/v1/invoice_items?select=id,product_code"
    res = requests.get(url_items, headers=HEADERS_SB)
    if res.status_code != 200:
        print("❌ فشل في جلب البنود")
        return

    updated = []
    for row in res.json():
        item_id = row["id"]
        current_value = (row.get("product_code") or "").strip()
        actual_code = product_map.get(current_value)
        if actual_code:
            updated.append({
                "id": item_id,
                "product_code": actual_code
            })

    if not updated:
        print("✅ لا توجد بنود بحاجة تصحيح.")
        return

    # 3. إرسال التحديثات
    url_update = f"{SUPABASE_URL}/rest/v1/invoice_items?on_conflict=id"
    res = requests.post(url_update, headers=HEADERS_SB, json=updated)
    if res.status_code in [200, 201]:
        print(f"✅ تم تحديث {len(updated)} بند بنجاح.")
    else:
        print(f"❌ فشل في تحديث البنود: {res.status_code} - {res.text}")


if __name__ == "__main__":
    sync_products()
    fix_invoice_items_based_on_product_name()
