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
    # يخبر PostgREST بعمل merge للسجلات الموجودة
    "Prefer": "resolution=merge-duplicates"
}

def fetch_with_retry(url, headers, retries=3, timeout=30):
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
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
        if not items:
            break

        for prod in items:
            code = prod.get("code") or prod.get("product_code") or prod.get("supplier_code") or ""
            payload = {
                "daftra_product_id": str(prod.get("id")),
                "product_code":       code,
                "name":               prod.get("name", ""),
                "stock_balance":      str(prod.get("stock_balance", 0)),
                "buy_price":          str(prod.get("buy_price", 0)),
                "average_price":      str(prod.get("average_price", 0)),
                "minimum_price":      str(prod.get("minimum_price", 0)),
                "supplier_code":      prod.get("supplier_code", "")
            }

            # upsert عبر on_conflict على العمود daftra_product_id
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/products?on_conflict=daftra_product_id",
                headers=HEADERS_SB,
                json=payload,
                timeout=10
            )
            if resp.status_code in (200, 201):
                total += 1

        page += 1
        time.sleep(1)

    return {"synced": total}

if __name__ == "__main__":
    res = sync_products()
    print(f"✅ تم مزامنة المنتجات: {res['synced']} سجل")
