import requests
import time
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# إعداد نظام السجلات
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_log.txt', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# إعدادات الاتصالات
DAFTRA_URL = "https://shadowpeace.daftra.com"
DAFTRA_HEADERS = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
SUPABASE_URL = "https://wuqbovrurauffztbkbse.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind1cWJvdnJ1cmF1ZmZ6dGJrYnNlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzg3MTA0NywiZXhwIjoyMDYzNDQ3MDQ3fQ.6ekq6VV2gcyw4uOHfscO9vIzUBSGDk_yweiGOGSPyFo"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

def fetch_with_retry(url: str, headers: Dict, max_retries: int = 3, timeout: int = 30) -> Optional[Dict]:
    """جلب البيانات مع إعادة المحاولة"""
    for retry in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                if retry < max_retries - 1:
                    time.sleep((retry + 1) * 5)
                    continue
        except Exception as e:
            if retry < max_retries - 1:
                time.sleep((retry + 1) * 5)
                continue
    return None

def test_connections() -> bool:
    """فحص الاتصال بـ Daftra و Supabase"""
    # فحص Daftra
    daftra_test = fetch_with_retry(
        f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page=1&limit=1",
        DAFTRA_HEADERS,
        max_retries=1
    )
    
    if daftra_test is None:
        return False
    
    # فحص Supabase
    try:
        supabase_response = requests.get(
            f"{SUPABASE_URL}/rest/v1/invoices?limit=1",
            headers=SUPABASE_HEADERS,
            timeout=10
        )
        return supabase_response.status_code == 200
    except:
        return False

async def sync_invoices():
    """دالة سحب فواتير المبيعات من دفترة وحفظها في Supabase"""
    total_synced = 0
    debug_info = []
    start_time = time.time()
    
    try:
        expected_type = 0  # فواتير المبيعات
        limit = 5
        debug_info.append("🧾 بدء سحب الفواتير")
        
        page = 1
        max_pages = 3
        
        while page <= max_pages:
            if time.time() - start_time > 600:
                debug_info.append("⏰ توقف بسبب انتهاء الوقت")
                break
                
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit={limit}"
            debug_info.append(f"🔄 جلب الصفحة {page}")
            
            data = fetch_with_retry(url, DAFTRA_HEADERS)
            if data is None:
                debug_info.append(f"❌ فشل جلب الصفحة {page}")
                break
            
            invoice_list = data.get("data", [])
            if not invoice_list:
                debug_info.append(f"⏹️ الصفحة {page} فارغة")
                break
            
            debug_info.append(f"📋 وجدت {len(invoice_list)} فاتورة في الصفحة {page}")
            
            for invoice in invoice_list:
                try:
                    inv_id = invoice.get("id")
                    inv_no = invoice.get("no", "بدون رقم")
                    inv_date = invoice.get("date")
                    inv_type = invoice.get("type")
                    store_id = invoice.get("store_id")
                    branch_id = invoice.get("branch_id", 1)
                    
                    debug_info.append(f"🔍 معالجة فاتورة {inv_no} - نوع: {inv_type}")
                    
                    try:
                        inv_type = int(inv_type)
                    except (ValueError, TypeError):
                        debug_info.append(f"⚠️ نوع فاتورة غير صالح: {inv_type}")
                        continue
                    
                    if inv_type != expected_type:
                        debug_info.append(f"⏭️ تخطي فاتورة نوع {inv_type}")
                        continue
                    
                    # جلب تفاصيل الفاتورة
                    url_details = f"{DAFTRA_URL}/v2/api/entity/invoice/{inv_id}"
                    debug_info.append(f"🔍 جلب تفاصيل فاتورة {inv_no}")
                    inv_details = fetch_with_retry(url_details, DAFTRA_HEADERS)
                    
                    if inv_details is None:
                        debug_info.append(f"❌ فشل جلب تفاصيل فاتورة {inv_no}")
                        continue
                    
                    # تحضير بيانات الفاتورة
                    invoice_data = {
                        "id": str(inv_id),
                        "created_at": str(inv_date or ""),
                        "invoice_type": str(expected_type),
                        "branch": str(branch_id),
                        "store": str(store_id or ""),
                        "total": str(inv_details.get("summary_total", 0)),
                        "customer_id": str(invoice.get("customer_id", "")),
                        "invoice_no": str(inv_no)
                    }
                    
                    debug_info.append(f"💾 محاولة حفظ فاتورة {inv_no}")
                    
                    # حفظ الفاتورة في Supabase
                    insert_response = requests.post(
                        f"{SUPABASE_URL}/rest/v1/invoices",
                        headers=SUPABASE_HEADERS,
                        json=invoice_data,
                        timeout=10
                    )
                    
                    if insert_response.status_code == 201:
                        total_synced += 1
                        debug_info.append(f"✅ نجح حفظ فاتورة {inv_no}")
                        
                        # حفظ عناصر الفاتورة
                        items = inv_details.get("invoice_item", [])
                        if not isinstance(items, list):
                            items = [items] if items else []
                        
                        items_added = 0
                        for item in items:
                            product_id = item.get("product_id")
                            quantity = item.get("quantity", 0)
                            unit_price = item.get("unit_price", 0)
                            
                            if product_id and float(quantity or 0) > 0:
                                item_data = {
                                    "invoice_id": str(inv_id),
                                    "product_id": str(product_id),
                                    "quantity": str(quantity),
                                    "unit_price": str(unit_price)
                                }
                                
                                item_response = requests.post(
                                    f"{SUPABASE_URL}/rest/v1/invoice_items",
                                    headers=SUPABASE_HEADERS,
                                    json=item_data,
                                    timeout=10
                                )
                                
                                if item_response.status_code == 201:
                                    items_added += 1
                        
                        debug_info.append(f"💾 حفظ {items_added} عنصر للفاتورة {inv_no}")
                    else:
                        debug_info.append(f"❌ فشل حفظ فاتورة {inv_no}: {insert_response.status_code}")
                    
                except Exception as e:
                    debug_info.append(f"❌ خطأ في معالجة فاتورة: {str(e)}")
                    continue
            
            page += 1
            time.sleep(1)
    
    except Exception as e:
        debug_info.append(f"❌ خطأ عام: {str(e)}")
    
    return {
        "total_synced": total_synced,
        "duration": f"{time.time() - start_time:.2f} ثانية",
        "debug_info": debug_info
    }
