# invoices_service.py - الكود الكامل المصحح
import requests
import time
from datetime import datetime
import uuid

# إعدادات
DAFTRA_URL = "https://shadowpeace.daftra.com"
DAFTRA_HEADERS = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
SUPABASE_URL = "https://wuqbovrurauffztbkbse.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind1cWJvdnJ1cmF1ZmZ6dGJrYnNlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzg3MTA0NywiZXhwIjoyMDYzNDQ3MDQ3fQ.6ekq6VV2gcyw4uOHfscO9vIzUBSGDk_yweiGOGSPyFo"

def fetch_with_retry(url, headers, max_retries=2, timeout=15):
    for retry in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if response.status_code == 200:
                return response.json()
            else:
                if retry < max_retries - 1:
                    time.sleep(3)
                    continue
        except Exception as e:
            if retry < max_retries - 1:
                time.sleep(3)
                continue
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
    except Exception as e:
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
    except Exception as e:
        return None

async def sync_invoices():
    total_synced = 0
    debug_info = []
    start_time = time.time()
    
    try:
        debug_info.append("🧾 بدء مزامنة جميع الفواتير")
        
        limit = 50
        max_pages = 100
        page = 1
        
        while page <= max_pages:
            # فحص الوقت (30 دقيقة كحد أقصى)
            if time.time() - start_time > 1800:
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
                debug_info.append(f"⏹️ الصفحة {page} فارغة - انتهت الفواتير")
                break
            
            debug_info.append(f"📋 وجدت {len(invoice_list)} فاتورة في الصفحة {page}")
            
            for invoice in invoice_list:
                try:
                    inv_id = invoice.get("id")
                    inv_no = invoice.get("no", f"INV-{inv_id}")
                    inv_type = invoice.get("type")
                    
                    # فقط فواتير المبيعات (نوع 0)
                    if str(inv_type) != "0":
                        continue
                    
                    debug_info.append(f"🔍 معالجة فاتورة {inv_no}")
                    
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
                        debug_info.append(f"⏭️ فاتورة {inv_no} موجودة مسبقاً")
                        continue
                    
                    # جلب تفاصيل الفاتورة
                    details_url = f"{DAFTRA_URL}/v2/api/entity/invoice/{inv_id}"
                    inv_details = fetch_with_retry(details_url, DAFTRA_HEADERS)
                    
                    if inv_details is None:
                        debug_info.append(f"❌ فشل جلب تفاصيل {inv_no}")
                        continue
                    
                    # تحضير البيانات
                    inv_date = invoice.get("date")
                    if inv_date and len(str(inv_date)) >= 10:
                        invoice_date = str(inv_date)[:10]
                    else:
                        invoice_date = datetime.now().strftime("%Y-%m-%d")
                    
                    invoice_data = {
                        "invoice_no": str(inv_no),
                        "invoice_date": invoice_date,
                        "customer_id": str(invoice.get("customer_id")) if invoice.get("customer_id") else None,
                        "total": str(inv_details.get("summary_total", 0)),
                        "branch": str(invoice.get("branch_id", 1))
                    }
                    
                    debug_info.append(f"💾 حفظ فاتورة {inv_no}")
                    
                    # حفظ الفاتورة
                    insert_response = save_invoice_to_supabase(invoice_data)
                    
                    if insert_response and insert_response.status_code == 201:
                        total_synced += 1
                        debug_info.append(f"✅ نجح حفظ {inv_no}")
                        
                        # الحصول على ID الفاتورة المحفوظة
                        saved_invoice = insert_response.json()
                        if saved_invoice and len(saved_invoice) > 0:
                            invoice_uuid = saved_invoice[0].get("id")
                            debug_info.append(f"🔑 Invoice UUID: {invoice_uuid}")
                            
                            # حفظ عناصر الفاتورة
                            items = inv_details.get("invoice_item", [])
                            if not isinstance(items, list):
                                items = [items] if items else []
                            
                            debug_info.append(f"📦 وجدت {len(items)} عنصر في الفاتورة")
                            
                            items_saved = 0
                            for item in items:
                                try:
                                    product_id = item.get("product_id")
                                    quantity = item.get("quantity", 0)
                                    unit_price = item.get("unit_price", 0)
                                    
                                    debug_info.append(f"   - عنصر: المنتج {product_id}, الكمية {quantity}")
                                    
                                    if product_id and float(quantity or 0) > 0:
                                        total_price = float(quantity) * float(unit_price)
                                        
                                        item_data = {
                                            "invoice_id": invoice_uuid,
                                            "product_id": str(product_id),
                                            "quantity": str(quantity),
                                            "unit_price": str(unit_price),
                                            "total_price": str(total_price)
                                        }
                                        
                                        item_response = save_item_to_supabase(item_data)
                                        if item_response and item_response.status_code == 201:
                                            items_saved += 1
                                            debug_info.append(f"   ✅ حُفظ عنصر المنتج {product_id}")
                                        else:
                                            error_msg = "unknown error"
                                            if item_response:
                                                error_msg = f"status {item_response.status_code}: {item_response.text[:100]}"
                                            debug_info.append(f"   ❌ فشل حفظ عنصر المنتج {product_id}: {error_msg}")
                                    else:
                                        debug_info.append(f"   ⏭️ تخطي عنصر غير صالح")
                                except Exception as e:
                                    debug_info.append(f"   ❌ خطأ في معالجة عنصر: {str(e)}")
                            
                            debug_info.append(f"💾 حفظ {items_saved} من أصل {len(items)} عنصر")
                        else:
                            debug_info.append(f"❌ لم يتم الحصول على UUID للفاتورة {inv_no}")
                    else:
                        error_msg = "خطأ غير معروف"
                        if insert_response:
                            error_msg = f"كود {insert_response.status_code}"
                            if insert_response.text:
                                error_msg += f" - {insert_response.text[:100]}"
                        
                        debug_info.append(f"❌ فشل حفظ {inv_no}: {error_msg}")
                    
                    # استراحة بين الفواتير
                    time.sleep(1)
                    
                except Exception as e:
                    debug_info.append(f"❌ خطأ في معالجة فاتورة: {str(e)}")
                    continue
            
            page += 1
            time.sleep(2)  # استراحة بين الصفحات
            
            # تقرير كل 10 صفحات
            if page % 10 == 0:
                debug_info.append(f"📊 تقرير: معالجة {page} صفحة، حفظ {total_synced} فاتورة")
        
    except Exception as e:
        debug_info.append(f"❌ خطأ عام: {str(e)}")
    
    return {
        "total_synced": total_synced,
        "duration": f"{time.time() - start_time:.2f} ثانية",
        "debug_info": debug_info[-100:]  # آخر 100 رسالة
    }
