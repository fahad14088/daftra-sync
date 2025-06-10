# invoices_service.py - الكود المحسن مع جميع الأعمدة الجديدة
import requests
import time
from datetime import datetime
import uuid

# إعدادات
DAFTRA_URL = "https://shadowpeace.daftra.com"
DAFTRA_HEADERS = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
SUPABASE_URL = "https://wuqbovrurauffztbkbse.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind1cWJvdnJ1cmF1ZmZ6dGJrYnNlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzg3MTA0NywiZXhwIjoyMDYzNDQ3MDQ3fQ.6ekq6VV2gcyw4uOHfscO9vIzUBSGDk_yweiGOGSPyFo"

def fetch_with_retry(url, headers, max_retries=3, timeout=30):
    """نفس دالتك للإعادة مع الانتظار"""
    for retry in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 5
                    time.sleep(wait_time)
                    continue
        except requests.exceptions.Timeout:
            if retry < max_retries - 1:
                wait_time = (retry + 1) * 5
                time.sleep(wait_time)
                continue
        except Exception as e:
            if retry < max_retries - 1:
                wait_time = (retry + 1) * 5
                time.sleep(wait_time)
                continue
    return None

def get_customer_details(customer_id):
    """جلب تفاصيل العميل من دفترة"""
    if not customer_id:
        return None, None, None
        
    url = f"{DAFTRA_URL}/v2/api/entity/customer/{customer_id}"
    customer_data = fetch_with_retry(url, DAFTRA_HEADERS)
    
    if customer_data:
        return (
            customer_data.get("business_name", ""),
            customer_data.get("city", ""),
            customer_data.get("id")
        )
    return None, None, None

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
            timeout=15
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
        return response
    except:
        return None

def get_all_branches():
    """نفس دالتك للفروع"""
    branches = [
        {"id": 1, "name": "Main"},
        {"id": 2, "name": "العويضة"}
    ]
    return [branch["id"] for branch in branches]

async def sync_invoices():
    total_synced = 0
    items_saved = 0
    debug_info = []
    start_time = time.time()
    
    try:
        debug_info.append("🧾 بدء تحميل فواتير المبيعات مع جميع البيانات")
        
        # إعدادات مثل كودك
        expected_type = 0
        limit = 20
        
        # جلب الفروع
        branches = get_all_branches()
        
        for branch_id in branches:
            page = 1
            debug_info.append(f"🏢 جلب فواتير الفرع {branch_id}")
            
            while True:
                # فحص الوقت - توقف بعد 15 دقيقة
                if time.time() - start_time > 900:
                    debug_info.append("⏰ توقف بسبب انتهاء الوقت (15 دقيقة)")
                    break
                
                # نفس URL من كودك
                url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?filter[branch_id]={branch_id}&page={page}&limit={limit}"
                debug_info.append(f"🔄 جلب الصفحة {page} من الفرع {branch_id}")
                
                data = fetch_with_retry(url, DAFTRA_HEADERS)
                if data is None:
                    debug_info.append(f"❌ فشل استرجاع البيانات من الصفحة {page}")
                    break
                
                invoice_list = data.get("data", [])
                if not invoice_list:
                    debug_info.append(f"🏁 لا توجد فواتير إضافية في الصفحة {page}")
                    break
                
                debug_info.append(f"📋 تم العثور على {len(invoice_list)} فاتورة في الصفحة {page}")
                
                has_new_invoices = False
                
                for invoice in invoice_list:
                    try:
                        inv_id = invoice.get("id")
                        inv_no = invoice.get("no", "بدون رقم")
                        inv_date = invoice.get("date")
                        inv_type = invoice.get("type")
                        store_id = invoice.get("store_id")
                        customer_id = invoice.get("customer_id")
                        
                        # فحص نوع الفاتورة
                        try:
                            inv_type = int(inv_type)
                        except (ValueError, TypeError):
                            continue
                        
                        if inv_type != expected_type:
                            continue
                        
                        has_new_invoices = True
                        
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
                            continue  # فاتورة موجودة
                        
                        # جلب تفاصيل الفاتورة
                        url_details = f"{DAFTRA_URL}/v2/api/entity/invoice/{inv_id}"
                        inv_details = fetch_with_retry(url_details, DAFTRA_HEADERS)
                        
                        if inv_details is None:
                            debug_info.append(f"❌ فشل في جلب تفاصيل الفاتورة {inv_id}")
                            continue
                        
                        # جلب تفاصيل العميل
                        client_business_name, client_city, client_id = get_customer_details(customer_id)
                        
                        # تحضير بيانات الفاتورة مع جميع الأعمدة الجديدة
                        if inv_date and len(str(inv_date)) >= 10:
                            invoice_date = str(inv_date)[:10]
                        else:
                            invoice_date = datetime.now().strftime("%Y-%m-%d")
                        
                        invoice_uuid = str(uuid.uuid4())
                        total_amount = float(inv_details.get("summary_total") or 0)
                        summary_paid = float(inv_details.get("summary_paid") or 0)
                        summary_unpaid = total_amount - summary_paid
                        
                        invoice_data = {
                            "id": invoice_uuid,
                            "invoice_no": str(inv_no),
                            "invoice_date": invoice_date,
                            "customer_id": str(customer_id) if customer_id else None,
                            "total": str(total_amount),
                            "branch": str(branch_id),
                            # الأعمدة الجديدة
                            "client_id": str(client_id) if client_id else None,
                            "client_business_name": client_business_name,
                            "client_city": client_city,
                            "summary_paid": str(summary_paid),
                            "summary_unpaid": str(summary_unpaid)
                        }
                        
                        # حفظ الفاتورة
                        insert_response = save_invoice_to_supabase(invoice_data)
                        
                        if insert_response and insert_response.status_code == 201:
                            total_synced += 1
                            debug_info.append(f"✅ حفظ فاتورة {inv_no} - عميل: {client_business_name or 'غير محدد'}")
                            
                            # حفظ العناصر
                            items = inv_details.get("invoice_item", [])
                            if not isinstance(items, list):
                                items = [items] if items else []
                            
                            items_added = 0
                            for item in items:
                                product_id = item.get("product_id")
                                quantity = float(item.get("quantity") or 0)
                                unit_price = float(item.get("unit_price") or 0)
                                
                                if product_id and quantity > 0:
                                    total_price = quantity * unit_price
                                    
                                    item_data = {
                                        "id": str(uuid.uuid4()),
                                        "invoice_id": invoice_uuid,
                                        "product_id": str(product_id),
                                        "quantity": str(quantity),
                                        "unit_price": str(unit_price),
                                        "total_price": str(total_price)
                                    }
                                    
                                    item_response = save_item_to_supabase(item_data)
                                    
                                    if item_response and item_response.status_code == 201:
                                        items_added += 1
                                        items_saved += 1
                            
                            debug_info.append(f"💾 تم حفظ {items_added} عنصر للفاتورة {inv_id}")
                        
                    except Exception as e:
                        debug_info.append(f"❌ خطأ في معالجة الفاتورة {inv_id}: {e}")
                        continue
                
                # نفس منطق التوقف الذكي من كودك
                if not has_new_invoices:
                    debug_info.append(f"🏁 لا توجد فواتير جديدة في الصفحة {page}")
                    break
                
                if len(invoice_list) < limit:
                    debug_info.append(f"🏁 وصلنا للصفحة الأخيرة ({page})")
                    break
                
                page += 1
                
                # تقرير تقدم كل 5 صفحات
                if page % 5 == 0:
                    debug_info.append(f"📊 تقرير: {page} صفحة، {total_synced} فاتورة، {items_saved} عنصر")
        
    except Exception as e:
        debug_info.append(f"❌ خطأ عام: {str(e)}")
    
    return {
        "total_synced": total_synced,
        "items_saved": items_saved,
        "duration": f"{time.time() - start_time:.2f} ثانية",
        "debug_info": debug_info[-25:]  # آخر 25 رسالة
    }
