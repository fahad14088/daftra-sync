import time
import requests
import logging
from typing import Optional, Dict, Any, List
from config import BASE_URL, BRANCH_IDS, PAGE_LIMIT, EXPECTED_TYPE, HEADERS_DAFTRA, HEADERS_SUPABASE, SUPABASE_URL

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_with_retry(url: str, headers: Dict, params: Optional[Dict] = None, retries: int = 3, delay: int = 2) -> Optional[Dict]:
    """جلب البيانات مع إعادة المحاولة وتحسين معالجة الأخطاء"""
    last_error = None
    
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                logger.warning(f"🔍 الفاتورة غير موجودة (404): {url}")
                return None  # لا تعيد المحاولة للفواتير غير الموجودة
            elif response.status_code == 429:
                # Too Many Requests - انتظر وقت أطول
                wait_time = delay * (2 ** attempt)  # Exponential backoff
                logger.warning(f"⏳ تم تجاوز الحد المسموح - انتظار {wait_time}s")
                time.sleep(wait_time)
                continue
            else:
                last_error = f"{response.status_code} - {response.text[:200]}"
                logger.warning(f"⚠️ محاولة {attempt+1} فشلت: {last_error}")
                
        except requests.exceptions.Timeout:
            last_error = "انتهت مهلة الاتصال"
            logger.warning(f"⚠️ محاولة {attempt+1} فشلت: {last_error}")
        except requests.exceptions.ConnectionError:
            last_error = "خطأ في الاتصال"
            logger.warning(f"⚠️ محاولة {attempt+1} فشلت: {last_error}")
        except Exception as e:
            last_error = str(e)
            logger.warning(f"⚠️ محاولة {attempt+1} فشلت: {last_error}")
        
        if attempt < retries - 1:  # لا تنتظر في المحاولة الأخيرة
            time.sleep(delay)
    
    logger.error(f"❌ فشل نهائي بعد {retries} محاولات: {last_error}")
    return None

def validate_invoice_data(invoice: Dict) -> bool:
    """التحقق من صحة بيانات الفاتورة"""
    required_fields = ['id', 'no', 'date']
    
    for field in required_fields:
        if not invoice.get(field):
            logger.warning(f"⚠️ حقل مطلوب مفقود: {field}")
            return False
    
    return True

def fetch_invoice_details(invoice_id: str) -> Optional[Dict]:
    """جلب تفاصيل الفاتورة مع تحسين معالجة الأخطاء"""
    if not invoice_id:
        logger.warning("❌ معرف الفاتورة فارغ")
        return None
    
    url = f"{BASE_URL}/v2/api/entity/invoice/view/{invoice_id}"
    data = fetch_with_retry(url, HEADERS_DAFTRA)
    
    # تأخير لتفادي حظر دفترة
    time.sleep(0.5)
    
    if not data:
        return None
    
    # التحقق من بنية الاستجابة
    if "data" not in data:
        logger.warning(f"❌ بنية استجابة غير متوقعة للفاتورة {invoice_id}")
        return None
    
    invoice = data["data"].get("Invoice")
    if not invoice:
        logger.warning(f"❌ لا توجد بيانات فاتورة في الاستجابة {invoice_id}")
        return None
    
    # التحقق من صحة البيانات
    if not validate_invoice_data(invoice):
        logger.warning(f"❌ بيانات الفاتورة {invoice_id} غير صحيحة")
        return None
    
    # معلومات تشخيصية
    logger.info(f"✅ الفاتورة {invoice_id}: البنود = {len(invoice.get('InvoiceItem', []))}")
    
    return invoice

def extract_invoice_items(invoice: Dict) -> List[Dict]:
    """استخراج بنود الفاتورة من مصادر متعددة محتملة"""
    possible_item_keys = [
        'InvoiceItem', 'items', 'Items', 'invoiceItems', 
        'invoice_items', 'LineItems', 'line_items', 'details'
    ]
    
    for key in possible_item_keys:
        items = invoice.get(key, [])
        if items and isinstance(items, list):
            logger.info(f"🎯 تم العثور على البنود في: {key}")
            return items
    
    # البحث في المستوى الأعمق
    for key, value in invoice.items():
        if isinstance(value, list) and value:
            # تحقق من أن العنصر الأول يبدو كبند فاتورة
            first_item = value[0]
            if isinstance(first_item, dict) and any(field in first_item for field in ['product_id', 'quantity', 'unit_price']):
                logger.info(f"🎯 تم العثور على البنود في: {key} (تحقق عميق)")
                return value
    
    return []

def safe_convert_to_float(value: Any, default: float = 0.0) -> float:
    """تحويل آمن للأرقام مع معالجة أفضل للأخطاء"""
    if value is None or value == "":
        return default
    
    try:
        # معالجة النصوص التي تحتوي على فواصل
        if isinstance(value, str):
            value = value.replace(',', '').strip()
        return float(value)
    except (ValueError, TypeError) as e:
        logger.debug(f"تحويل رقم فاشل: {value} -> {default} ({e})")
        return default

def safe_convert_to_string(value: Any, default: str = "") -> str:
    """تحويل آمن للنصوص"""
    if value is None:
        return default
    return str(value).strip()

def save_to_supabase(data: List[Dict], table_name: str, data_type: str) -> bool:
    """حفظ البيانات في Supabase مع معالجة أفضل للأخطاء"""
    if not data:
        logger.warning(f"⚠️ لا توجد بيانات {data_type} للحفظ")
        return False
    
    try:
        # تقسيم البيانات إلى دفعات لتجنب حدود الحجم
        batch_size = 100
        total_saved = 0
        
        for i in range(0, len(data), batch_size):
            batch = data[i:i + batch_size]
            
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/{table_name}",
                headers=HEADERS_SUPABASE,
                json=batch,
                timeout=60
            )
            
            if response.status_code == 201:
                total_saved += len(batch)
                logger.info(f"✅ تم حفظ دفعة {data_type}: {len(batch)} عنصر")
            else:
                logger.error(f"❌ فشل حفظ دفعة {data_type}: {response.status_code}")
                logger.error(f"❌ تفاصيل الخطأ: {response.text[:500]}")
                return False
        
        logger.info(f"✅ تم حفظ إجمالي {total_saved} {data_type} بنجاح")
        return True
        
    except Exception as e:
        logger.error(f"❌ خطأ في حفظ {data_type}: {str(e)}")
        return False

def fetch_all() -> Dict[str, int]:
    """الدالة الرئيسية المحسنة"""
    all_invoices = []
    all_items = []
    failed_invoices = []

    logger.info(f"🚀 بدء جلب البيانات للفروع: {BRANCH_IDS}")

    for branch in BRANCH_IDS:
        branch_invoices = 0
        branch_items = 0
        page = 1
        
        logger.info(f"📂 معالجة الفرع: {branch}")
        
        while True:
            url = f"{BASE_URL}/v2/api/entity/invoice/list/1"
            params = {
                "filter[branch_id]": branch,
                "page": page,
                "limit": PAGE_LIMIT
            }
            
            data = fetch_with_retry(url, HEADERS_DAFTRA, params=params)
            if data is None:
                logger.warning(f"⚠️ فشل في جلب البيانات للفرع {branch} الصفحة {page}")
                break

            items = data.get("data", [])
            if not isinstance(items, list):
                items = [items] if items else []

            # فلترة الفواتير حسب النوع المطلوب
            invoice_ids = [
                inv.get("id") for inv in items 
                if inv.get("id") and int(inv.get("type", -1)) == EXPECTED_TYPE
            ]
            
            logger.info(f"📄 فرع {branch} - صفحة {page}: {len(invoice_ids)} فاتورة مؤهلة من أصل {len(items)}")

            for invoice_id in invoice_ids:
                try:
                    inv = fetch_invoice_details(invoice_id)
                    if not inv:
                        failed_invoices.append(invoice_id)
                        continue

                    # استخراج بيانات الفاتورة
                    contact = inv.get("Contact", {})
                    summary = inv.get("summary", {})
                    
                    invoice_data = {
                        "id": safe_convert_to_string(inv.get("id")),
                        "invoice_no": safe_convert_to_string(inv.get("no")),
                        "invoice_date": safe_convert_to_string(inv.get("date")),
                        "customer_id": safe_convert_to_string(inv.get("contact_id")),
                        "total": safe_convert_to_float(inv.get("total")),
                        "branch": safe_convert_to_string(inv.get("branch_id")),
                        "created_at": safe_convert_to_string(inv.get("created_at")),
                        "client_id": safe_convert_to_string(contact.get("id")),
                        "client_business_name": safe_convert_to_string(contact.get("business_name")),
                        "client_city": safe_convert_to_string(contact.get("city")),
                        "summary_paid": safe_convert_to_float(summary.get("paid")),
                        "summary_unpaid": safe_convert_to_float(summary.get("unpaid"))
                    }
                    
                    all_invoices.append(invoice_data)
                    branch_invoices += 1

                    # استخراج البنود
                    invoice_items = extract_invoice_items(inv)
                    
                    for item in invoice_items:
                        if not item.get("id"):
                            continue
                        
                        item_data = {
                            "id": safe_convert_to_string(item.get("id")),
                            "invoice_id": safe_convert_to_string(inv.get("id")),
                            "product_id": safe_convert_to_string(item.get("product_id")),
                            "product_code": safe_convert_to_string(item.get("product_code")),
                            "quantity": safe_convert_to_float(item.get("quantity")),
                            "unit_price": safe_convert_to_float(item.get("unit_price")),
                            "total_price": safe_convert_to_float(item.get("total")),
                            "client_business_name": safe_convert_to_string(contact.get("business_name"))
                        }
                        
                        all_items.append(item_data)
                        branch_items += 1

                except Exception as e:
                    logger.error(f"❌ خطأ في معالجة الفاتورة {invoice_id}: {str(e)}")
                    failed_invoices.append(invoice_id)

            # تحقق من نهاية الصفحات
            if len(items) < PAGE_LIMIT:
                logger.info(f"✅ انتهاء الفرع {branch} - الفواتير: {branch_invoices}, البنود: {branch_items}")
                break

            page += 1
            time.sleep(1)  # تأخير بين الصفحات

    # تقرير النتائج
    logger.info(f"📊 النتائج النهائية:")
    logger.info(f"✅ إجمالي الفواتير: {len(all_invoices)}")
    logger.info(f"✅ إجمالي البنود: {len(all_items)}")
    logger.info(f"❌ الفواتير الفاشلة: {len(failed_invoices)}")
    
    if failed_invoices:
        logger.warning(f"⚠️ الفواتير التي فشلت: {failed_invoices[:10]}...")  # عرض أول 10 فقط

    # حفظ البيانات
    invoices_saved = save_to_supabase(all_invoices, "invoices", "الفواتير")
    items_saved = save_to_supabase(all_items, "invoice_items", "البنود")

    return {
        "invoices": len(all_invoices),
        "items": len(all_items),
        "failed": len(failed_invoices),
        "invoices_saved": invoices_saved,
        "items_saved": items_saved
    }

if __name__ == "__main__":
    try:
        result = fetch_all()
        logger.info(f"🎉 تمت العملية بنجاح: {result}")
    except KeyboardInterrupt:
        logger.info("⏹️ تم إيقاف العملية بواسطة المستخدم")
    except Exception as e:
        logger.error(f"💥 خطأ عام في التطبيق: {str(e)}")
