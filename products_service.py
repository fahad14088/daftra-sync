import os
import requests
import time

# قراءة الإعدادات من المتغيرات البيئية
DAFTRA_URL     = os.getenv("DAFTRA_URL")
DAFTRA_APIKEY  = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL   = os.getenv("SUPABASE_URL")
SUPABASE_KEY   = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": DAFTRA_APIKEY}
HEADERS_SB     = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def fetch_with_retry(url, headers, retries=3, timeout=30):
    for i in range(retries):
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if r.status_code == 200:
                return r.json()
        except Exception:
            pass
        time.sleep((i+1)*5)
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

        for p in items:
            payload = {
                "daftra_product_id": str(p.get("id")),
                "product_code":       p.get("code", ""),
                "name":              p.get("name", ""),
                "stock_balance":     str(p.get("stock_balance", 0))
            }
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/products",
                headers=HEADERS_SB,
                json=payload,
                timeout=10
            )
            if resp.status_code in (200,201):
                total += 1

        page += 1
        time.sleep(1)

    return {"synced": total}
