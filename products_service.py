import os
import requests
import time

# إعدادات البيئة
DAFTRA_URL    = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": DAFTRA_APIKEY}
HEADERS_SB = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
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
        time.sleep((i + 1) * 3)
    return None

def sync_products():
    print("🚀 بدء مزامنة المنتجات...")
    total = 0
    page = 1
    limit = 50

    while True:
        url = f"{DAFTRA_URL}/v2/api/entity/product/list/1?page={page}&limit={limit}"
        data = fetch_with_retry(url, HEADERS_DAFTRA)
        items = data.get("data", []) if data else []
        print(f"> الصفحة {page}: تم العثور على {len(items)} منتج")
        if not items:
            break

        for raw in items:
            prod = raw.get("Product") if isinstance(raw, dict) and "Product" in raw else raw
            pid = prod.get("id")
            if not pid:
                print("! منتج بدون id - تم تجاهله")
                continue

            code = (
                prod.get("code") or
                prod.get("product_code") or
                prod.get("supplier_code") or
                ""
            )

            payload = {
                "product_id": str(pid),
                "daftra_product_id": str(pid),
                "product_code": code,
                "name": prod.get("name", ""),
                "stock_balance": str(prod.get("stock_balance", 0)),
                "buy_price": str(prod.get("buy_price", 0)),
                "average_price": str(prod.get("average_price", 0)),
                "minimum_price": str(prod.get("minimum_price", 0)),
                "supplier_code": prod.get("supplier_code", "")
            }

            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/products?on_conflict=product_id&prefer=resolution=merge-duplicates",
                headers=HEADERS_SB,
                json=payload,
                timeout=10
            )
            print(f">> منتج {pid} → {resp.status_code}")
            if resp.status_code in (200, 201):
                total += 1

        page += 1
        time.sleep(1)

    print(f"✅ تم مزامنة {total} منتجًا بنجاح")

def update_invoice_items_product_code():
    print("🔧 بدء تحديث كود المنتجات في بنود الفواتير...")
    url_products = f"{SUPABASE_URL}/rest/v1/products?select=product_id,product_code"
    res = requests.get(url_products, headers=HEADERS_SB)
    if res.status_code != 200:
        print("❌ فشل في جلب المنتجات")
        return

    product_map = {
        str(p["product_id"]).strip(): p["product_code"]
        for p in res.json()
        if p.get("product_id") and p.get("product_code")
    }

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

        for row in batch:
            item_id = row["id"]
            pid = str(row.get("product_id", "")).strip()
            actual_code = product_map.get(pid)
            if actual_code:
                patch_url = f"{SUPABASE_URL}/rest/v1/invoice_items?id=eq.{item_id}"
                patch_payload = {"product_code": actual_code}
                res_patch = requests.patch(patch_url, headers=HEADERS_SB, json=patch_payload)
                print(f"🔄 بند {item_id} ← {res_patch.status_code}")
                if res_patch.status_code in [200, 204]:
                    total_updated += 1
            else:
                print(f"⚠️ لا يوجد كود للـ product_id={pid}")

        offset += limit
        time.sleep(0.5)

    print(f"✅ تم تحديث {total_updated} بند بنجاح.")

if __name__ == "__main__":
    sync_products()
    update_invoice_items_product_code()

