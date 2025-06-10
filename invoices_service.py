# invoices_service.py - الكود المحسن مع طريقة حماية من التعليق مثل كودك
import requests
import time
from datetime import datetime
import uuid

# إعدادات
DAFTRA_URL = "https://shadowpeace.daftra.com"
DAFTRA_HEADERS = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
SUPABASE_URL = "https://wuqbovrurauffztbkbse.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind1cWJvdnJ1cmF1ZmZ6dGJrYnNlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzg3MTA0NywiZXhwIjoyMDYzNDQ3MDQ3fQ.6ekq6VV2gcyw4uOHfscO9vIzUBSGDk_yweiGOGSPyFo"

# نفس دالة fetch_with_retry من كودك
def fetch_with_retry(url, headers, max_retries=3, timeout=30):
    """محاولة جلب البيانات مع إعادة المحاولة في حالة فشل الاتصال"""
    for retry in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f"⚠️ كود استجابة غير متوقع: {response.status_code}")
                if retry < max_retries - 1:
                    wait_time = (retry + 1) * 5
                    print(f"⏱️ انتظار {wait_time} ثوانٍ قبل إعادة المحاولة...")
                    time.sleep(wait_time)
                    continue
        
        except requests.exceptions.Timeout:
            print(f"⚠️ انتهت مهلة الاتصال")
            if retry < max_retries - 1:
                wait_time = (retry + 1) * 5
                print(f"⏱️ انتظار {wait_time} ثوانٍ قبل إعادة المحاولة...")
                time.sleep(wait_time)
                continue
        
        except Exception as e:
            print(f"❌ خطأ غير متوقع: {e}")
            if retry < max_retries - 1:
                wait_time = (retry + 1) * 5
                print(f"⏱️ انتظار {wait_time} ثوانٍ قبل إعادة المحاولة...")
                time.sleep(wait_time)
                continue
    
    return None

def get_product_code_from_supabase(product_id):
    """جلب product_code من جدول المنتجات باستخدام product_id"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/products?id=eq.{product_id}&select=product_code"
        response = requests.get(
            url,
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}"
            },
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            if data and len(data) > 0:
                return data[0].get("product_code")
        return None
    except:
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
        return response
    except:
        return None

# نفس دالة الفروع من كودك
def get_all_branches():
    branches = [
        {"id": 1, "name": "Main"},
        {"id": 2, "name": "العويضة"}
    ]
    branch_ids = [branch["id"] for branch in branches]
    print(f"✅ استخدام الفروع المحددة: {branch_ids}")
    return branch_ids

async def sync_invoices():
    total_synced = 0
    items_saved = 0
    debug_info = []
    start_time = time.time()
    
    try:
        debug_info.append("🧾 بدء تحميل فواتير المبيعات مع product_code")
        
        # إعدادات مثل كودك
        expected_type = 0
        limit = 20
        
        # جلب الفروع
        branches = get_all_branches()
        
        for branch_id in branches:
            page = 1
            debug_info.append(f"🏢 جلب فواتير الفرع {branch_id}")
            
            while True:
                # فحص الوقت - توقف بعد 10 دقائق لتجنب التعليق
                if time.time() - start_time > 600:
                    debug_info.append("⏰ توقف بسبب انتهاء الوقت (10 دقائق)")
                    break
                
                # نفس URL من كودك مع filter[branch_id]
                url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?filter[branch_id]={branch_id}&page={page}&limit={limit}"
                
                debug_info.append(f"🔄 جلب الصفحة {page} من الفرع {branch_id}")
                
                # استخدام نفس دالة fetch_with_retry من كودك
                data = fetch_with_retry(url, DAFTRA_HEADERS)
                
                if data is None:
                    debug_info.append(f"❌ فشل استرجاع البيانات من الصفحة {page} بعد عدة محاولات")
                    break
                
                invoice_list = data.get("data", [])
                if not invoice_list:
                    debug_info.append(f"🏁 لا توجد فواتير إضافية في الصفحة {page}")
                    break
                
                debug_info.append(f"📋 تم العثور على {len(invoice_list)} فاتورة في الصفحة {page}")
                
                # متغير للتحقق من الفواتير الجديدة مثل كودك
                has_new_invoices = False
                
                for invoice in invoice_list:
                    try:
                        inv_id = invoice.get("id")
                        inv_no = invoice.get("no", "بدون رقم")
                        inv_date = invoice.get("date")
                        inv_type = invoice.get("type")
                        store_id = invoice.get("store_id")
                        
                        # نفس التحقق من نوع الفاتورة من كودك
                        try:
                            inv_type = int(inv_type)
                        except (ValueError, TypeError):
                            continue
                        
                        if inv_type != expected_type:
                            continue
                        
                        has_new_invoices = True
                        
                        # فحص الوجود في Supabase
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
                            continue  # فاتورة موجودة - تخطي
                        
                        # جلب تفاصيل الفاتورة مع نفس طريقة كودك
                        url_details = f"{DAFTRA_URL}/v2/api/entity/invoice/{inv_id}"
                        inv_details = fetch_with_retry(url_details, DAFTRA_HEADERS)
                        
                        if inv_details is None:
                            debug_info.append(f"❌ فشل في جلب تفاصيل الفاتورة {inv_id}")
                            continue
                        
                        # تحضير بيانات الفاتورة
                        if inv_date and len(str(inv_date)) >= 10:
                            invoice_date = str(inv_date)[:10]
                        else:
                            invoice_date = datetime.now().strftime("%Y-%m-%d")
                        
                        invoice_uuid = str(uuid.uuid4())
                        total_amount = float(inv_details.get("summary_total") or 0)
                        
                        invoice_data = {
                            "id": invoice_uuid,
                            "invoice_no": str(inv_no),
                            "invoice_date": invoice_date,
                            "customer_id": str(invoice.get("customer_id")) if invoice.get("customer_id") else None,
                            "total": str(total_amount),
                            "branch": str(branch_id)
                        }
                        
                        # حفظ الفاتورة
                        insert_response = save_invoice_to_supabase(invoice_data)
                        
                        if insert_response and insert_response.status_code == 201:
                            total_synced += 1
                            debug_info.append(f"✅ حفظ فاتورة {inv_no} - المبلغ: {total_amount}")
                            
                            # نفس معالجة العناصر من كودك مع إضافة product_code
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
                                    
                                    # جلب product_code من جدول المنتجات
                                    product_code = get_product_code_from_supabase(product_id)
                                    
                                    item_data = {
                                        "id": str(uuid.uuid4()),
                                        "invoice_id": invoice_uuid,
                                        "product_id": str(product_id),
                                        "product_code": product_code,  # إضافة product_code للربط
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
                
                # نفس المنطق من كودك للتوقف الذكي
                if not has_new_invoices:
                    debug_info.append(f"🏁 لا توجد فواتير جديدة في الصفحة {page}")
                    break
                
                # نفس فحص النهاية من كودك
                if len(invoice_list) < limit:
                    debug_info.append(f"🏁 وصلنا للصفحة الأخيرة ({page})")
                    break
                
                page += 1
                
                # تقرير تقدم كل 5 صفحات
                if page % 5 == 0:
                    debug_info.append(f"📊 تقرير: معالجة {page} صفحة، حفظ {total_synced} فاتورة، {items_saved} عنصر")
        
    except Exception as e:
        debug_info.append(f"❌ خطأ عام: {str(e)}")
    
    return {
        "total_synced": total_synced,
        "items_saved": items_saved,
        "duration": f"{time.time() - start_time:.2f} ثانية",
        "debug_info": debug_info[-30:]  # آخر 30 رسالة
    }
