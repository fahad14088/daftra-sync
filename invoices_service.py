# invoices_service.py - الكود الكامل مع إرجاع تفاصيل الأخطاء
import requests
import time
from datetime import datetime
import uuid

# إعدادات
DAFTRA_URL = "https://shadowpeace.daftra.com"
DAFTRA_HEADERS = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
SUPABASE_URL = "https://wuqbovrurauffztbkbse.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind1cWJvdnJ1cmF1ZmZ6dGJrYnNlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzg3MTA0NywiZXhwIjoyMDYzNDQ3MDQ3fQ.6ekq6VV2gcyw4uOHfscO9vIzUBSGDk_yweiGOGSPyFo"

def fetch_with_retry(url, headers, max_retries=2, timeout=10):
    for retry in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response.json()
        except:
            if retry < max_retries - 1:
                time.sleep(2)
    return None

def save_invoice_to_supabase(invoice_data):
    try:
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json"
            },
            json=invoice_data,
            timeout=10
        )
        return response
    except:
        return None

def save_item_to_supabase(item_data):
    try:
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoice_items",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Content-Type": "application/json"
            },
            json=item_data,
            timeout=10
        )
        
        # إرجاع تفاصيل الاستجابة
        return {
            "status_code": response.status_code,
            "text": response.text,
            "success": response.status_code == 201
        }
        
    except Exception as e:
        return {
            "status_code": 0,
            "text": str(e),
            "success": False
        }

async def sync_invoices():
    total_synced = 0
    items_saved = 0
    debug_info = []
    start_time = time.time()
    
    try:
        debug_info.append("🧪 اختبار حفظ فاتورة مع العناصر مع تفاصيل الأخطاء")
        
        # جلب فاتورة واحدة للاختبار
        url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page=1&limit=1"
        data = fetch_with_retry(url, DAFTRA_HEADERS)
        
        if data is None:
            debug_info.append("❌ فشل الاتصال بدفترة")
            return {
                "total_synced": 0,
                "items_saved": 0,
                "duration": f"{time.time() - start_time:.2f} ثانية",
                "debug_info": debug_info
            }
        
        invoice_list = data.get("data", [])
        if not invoice_list:
            debug_info.append("❌ لا توجد فواتير")
            return {
                "total_synced": 0,
                "items_saved": 0,
                "duration": f"{time.time() - start_time:.2f} ثانية",
                "debug_info": debug_info
            }
        
        # البحث عن فاتورة مبيعات
        invoice = None
        for inv in invoice_list:
            if str(inv.get("type")) == "0":
                invoice = inv
                break
        
        if not invoice:
            debug_info.append("❌ لا توجد فواتير مبيعات")
            return {
                "total_synced": 0,
                "items_saved": 0,
                "duration": f"{time.time() - start_time:.2f} ثانية",
                "debug_info": debug_info
            }
        
        inv_id = invoice.get("id")
        inv_no = invoice.get("no", f"TEST-{inv_id}")
        debug_info.append(f"🔍 اختبار فاتورة {inv_no}")
        
        # فحص الوجود
        check_url = f"{SUPABASE_URL}/rest/v1/invoices?invoice_no=eq.{inv_no}"
        check_response = requests.get(
            check_url,
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}"
            },
            timeout=5
        )
        
        if check_response.status_code == 200 and len(check_response.json()) > 0:
            # الفاتورة موجودة
            existing_invoice = check_response.json()[0]
            invoice_uuid = existing_invoice.get("id")
            debug_info.append(f"⏭️ فاتورة {inv_no} موجودة - UUID: {invoice_uuid}")
        else:
            # حفظ فاتورة جديدة
            inv_date = invoice.get("date")
            if inv_date and len(str(inv_date)) >= 10:
                invoice_date = str(inv_date)[:10]
            else:
                invoice_date = datetime.now().strftime("%Y-%m-%d")
            
            invoice_uuid = str(uuid.uuid4())
            
            invoice_data = {
                "id": invoice_uuid,
                "invoice_no": str(inv_no),
                "invoice_date": invoice_date,
                "customer_id": str(invoice.get("customer_id")) if invoice.get("customer_id") else None,
                "total": str(invoice.get("total", "100.0")),
                "branch": str(invoice.get("branch_id", 1))
            }
            
            debug_info.append(f"💾 حفظ فاتورة جديدة {inv_no}")
            insert_response = save_invoice_to_supabase(invoice_data)
            
            if insert_response and insert_response.status_code == 201:
                total_synced += 1
                debug_info.append(f"✅ نجح حفظ فاتورة {inv_no}")
            else:
                error_msg = "خطأ غير معروف"
                if insert_response:
                    error_msg = f"كود {insert_response.status_code}: {insert_response.text[:100]}"
                debug_info.append(f"❌ فشل حفظ الفاتورة: {error_msg}")
                return {
                    "total_synced": 0,
                    "items_saved": 0,
                    "duration": f"{time.time() - start_time:.2f} ثانية",
                    "debug_info": debug_info
                }
        
        # جلب تفاصيل الفاتورة
        details_url = f"{DAFTRA_URL}/v2/api/entity/invoice/{inv_id}"
        debug_info.append(f"🔍 جلب تفاصيل الفاتورة")
        
        inv_details = fetch_with_retry(details_url, DAFTRA_HEADERS)
        if inv_details is None:
            debug_info.append("❌ فشل جلب تفاصيل الفاتورة")
            return {
                "total_synced": total_synced,
                "items_saved": 0,
                "duration": f"{time.time() - start_time:.2f} ثانية",
                "debug_info": debug_info
            }
        
        debug_info.append("✅ نجح جلب تفاصيل الفاتورة")
        
        # معالجة العناصر
        items = inv_details.get("invoice_item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        
        debug_info.append(f"📦 وجدت {len(items)} عنصر في الفاتورة")
        
        if len(items) == 0:
            debug_info.append("⚠️ لا توجد عناصر في هذه الفاتورة")
            return {
                "total_synced": total_synced,
                "items_saved": 0,
                "duration": f"{time.time() - start_time:.2f} ثانية",
                "debug_info": debug_info
            }
        
        # حفظ العناصر مع إرجاع تفاصيل الأخطاء
        for i, item in enumerate(items):
            product_id = item.get("product_id")
            quantity = item.get("quantity", 0)
            unit_price = item.get("unit_price", 0)
            
            debug_info.append(f"📝 عنصر {i+1}: منتج {product_id}, كمية {quantity}, سعر {unit_price}")
            
            if quantity and float(quantity) > 0:
                total_price = float(quantity) * float(unit_price or 0)
                
                # البيانات الصحيحة
                item_data = {
                    "id": str(uuid.uuid4()),
                    "invoice_id": invoice_uuid,
                    "product_id": str(product_id) if product_id else None,
                    "quantity": str(quantity),
                    "unit_price": str(unit_price or 0),
                    "total_price": str(total_price)
                }
                
                debug_info.append(f"💾 حفظ عنصر: منتج {product_id}")
                debug_info.append(f"📤 البيانات: {item_data}")
                
                item_response = save_item_to_supabase(item_data)
                
                if item_response and item_response.get("success"):
                    items_saved += 1
                    debug_info.append(f"✅ نجح حفظ عنصر منتج {product_id}")
                else:
                    status = item_response.get('status_code', 'unknown') if item_response else 'no response'
                    error_text = item_response.get('text', 'no details')[:300] if item_response else 'no response'
                    debug_info.append(f"❌ فشل حفظ عنصر منتج {product_id}: كود {status}")
                    debug_info.append(f"📝 تفاصيل الخطأ: {error_text}")
            else:
                debug_info.append(f"⏭️ تخطي عنصر غير صالح")
        
        debug_info.append(f"📊 النتيجة: حُفظ {items_saved} من أصل {len(items)} عنصر")
        
    except Exception as e:
        debug_info.append(f"❌ خطأ عام: {str(e)}")
    
    return {
        "total_synced": total_synced,
        "items_saved": items_saved,
        "duration": f"{time.time() - start_time:.2f} ثانية",
        "debug_info": debug_info
    }
