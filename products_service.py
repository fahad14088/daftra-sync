import requests
import time

# إعدادات
DAFTRA_URL = "https://shadowpeace.daftra.com"
DAFTRA_HEADERS = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
SUPABASE_URL = "https://wuqbovrurauffztbkbse.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.…GOGSPyFo"  # اختصرته هنا

def fetch_with_retry(url, headers, max_retries=3, timeout=30):
    for retry in range(max_retries):
        try:
            resp = requests.get(url, headers=headers, timeout=timeout)
            if resp.status_code == 200:
                return resp.json()
            # إعادة المحاولة مع تأخير متزايد
            time.sleep((retry + 1) * 5)
        except Exception:
            time.sleep((retry + 1) * 5)
    return None

def sync_products():
    """
    1) يجلب دفعات من دفترة إلى أن تنتهي الصفحات
    2) يكتب في عمود daftra_product_id و product_code في جدول products
    """
    total_synced = 0
    page = 1
    limit = 50

    while True:
        url = f"{DAFTRA_URL}/v2/api/entity/product/list/1?page={page}&limit={limit}"
        data = fetch_with_retry(url, DAFTRA_HEADERS)
        items = data.get("data", []) if data else []
        if not items:
            break

        for prod in items:
            payload = {
                "daftra_product_id": str(prod.get("id")),
                "product_code": prod.get("code", ""),
                "name": prod.get("name", ""),
                "stock_balance": str(prod.get("stock_balance", 0))
            }
            resp = requests.post(
                f"{SUPABASE_URL}/rest/v1/products",
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                    "Content-Type": "application/json"
                },
                json=payload,
                timeout=10
            )
            if resp.status_code in (200, 201):
                total_synced += 1

        page += 1
        time.sleep(1)

    return {"synced": total_synced}
