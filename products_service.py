import requests
import time

# إعدادات
DAFTRA_URL = "https://shadowpeace.daftra.com"
DAFTRA_HEADERS = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
SUPABASE_URL = "https://wuqbovrurauffztbkbse.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind1cWJvdnJ1cmF1ZmZ6dGJrYnNlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzg3MTA0NywiZXhwIjoyMDYzNDQ3MDQ3fQ.6ekq6VV2gcyw4uOHfscO9vIzUBSGDk_yweiGOGSPyFo"

async def sync_products():
    """دالة سحب المنتجات من دفترة وحفظها في Supabase"""
    total_synced = 0
    page = 1
    limit = 20
    debug_info = []
    start_time = time.time()
    
    try:
        while True:
            # تحقق من الوقت
            if time.time() - start_time > 1500:  # 25 دقيقة
                debug_info.append("⏰ توقف بسبب انتهاء الوقت")
                break
                
            url = f"{DAFTRA_URL}/v2/api/entity/product/list/1?page={page}&limit={limit}"
            
            response = requests.get(url, headers=DAFTRA_HEADERS, timeout=30)
            
            if response.status_code != 200:
                debug_info.append(f"❌ فشل الصفحة {page}: {response.status_code}")
                break
                
            data = response.json()
            products = data.get("data", [])
            
            if not products:
                debug_info.append(f"⏹️ انتهت المنتجات في الصفحة {page}")
                break
                
            for product in products:
                try:
                    product_data = {
                        "name": product.get("name", ""),
                        "product_code": product.get("product_code", ""),
                        "brand": product.get("brand", ""),
                        "stock_balance": str(product.get("stock_balance", 0)),
                        "buy_price": str(product.get("buy_price", 0)),
                        "minimum_price": str(product.get("minimum_price", 0)),
                        "average_price": str(product.get("average_price", 0)),
                        "supplier_code": product.get("supplier_code", "")
                    }
                    
                    # تحقق من وجود المنتج
                    check_response = requests.get(
                        f"{SUPABASE_URL}/rest/v1/products?product_code=eq.{product_data['product_code']}",
                        headers={
                            "apikey": SUPABASE_KEY,
                            "Authorization": f"Bearer {SUPABASE_KEY}",
                            "Content-Type": "application/json",
                            "Prefer": "count=exact"
                        },
                        timeout=10
                    )
                    
                    count = check_response.headers.get("content-range", "").split("/")[-1]
                    
                    if int(count or 0) == 0:
                        # إضافة المنتج
                        insert_response = requests.post(
                            f"{SUPABASE_URL}/rest/v1/products",
                            headers={
                                "apikey": SUPABASE_KEY,
                                "Authorization": f"Bearer {SUPABASE_KEY}",
                                "Content-Type": "application/json"
                            },
                            json=product_data,
                            timeout=10
                        )
                        
                        if insert_response.status_code == 201:
                            total_synced += 1
                
                except Exception as e:
                    continue
            
            page += 1
            time.sleep(0.1)
            
    except Exception as e:
        debug_info.append(f"❌ خطأ عام: {str(e)}")
    
    return {
        "total_synced": total_synced,
        "pages_processed": page - 1,
        "duration": f"{time.time() - start_time:.2f} ثانية"
    }
