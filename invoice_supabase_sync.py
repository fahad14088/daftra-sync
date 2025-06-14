import time
import requests
import logging
from datetime import datetime
from typing import List, Dict, Any, Optional
from config import BASE_URL, BRANCH_IDS, PAGE_LIMIT, EXPECTED_TYPE, HEADERS_DAFTRA, SUPABASE_URL, HEADERS_SUPABASE

# إعداد نظام التسجيل المحسن
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler('daftra_sync.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataValidator:
    """فئة للتحقق من صحة البيانات قبل الإرسال"""
    
    @staticmethod
    def validate_invoice(invoice: Dict[str, Any]) -> Dict[str, Any]:
        """التحقق من صحة بيانات الفاتورة وتنظيفها"""
        validated = {}
        
        # التحقق من المعرف الفريد
        if not invoice.get('id'):
            raise ValueError("معرف الفاتورة مطلوب")
        validated['id'] = str(invoice['id'])
        
        # التحقق من رقم الفاتورة
        validated['invoice_no'] = str(invoice.get('no', ''))
        
        # التحقق من تاريخ الفاتورة
        invoice_date = invoice.get('date')
        if invoice_date:
            try:
                # تحويل التاريخ إلى صيغة ISO إذا لم يكن كذلك
                if isinstance(invoice_date, str) and 'T' not in invoice_date:
                    validated['invoice_date'] = f"{invoice_date}T00:00:00"
                else:
                    validated['invoice_date'] = invoice_date
            except:
                validated['invoice_date'] = None
        else:
            validated['invoice_date'] = None
        
        # التحقق من البيانات الرقمية
        validated['customer_id'] = invoice.get('client_id')
        validated['total'] = float(invoice.get('summary_total', 0))
        validated['branch'] = invoice.get('branch_id')
        validated['summary_paid'] = float(invoice.get('summary_paid', 0))
        validated['summary_unpaid'] = float(invoice.get('summary_unpaid', 0))
        
        # التحقق من البيانات النصية
        validated['client_business_name'] = str(invoice.get('client_business_name', ''))[:255]
        validated['client_city'] = str(invoice.get('client_city', ''))[:100]
        
        # إضافة تاريخ الإنشاء
        created_at = invoice.get('created')
        if created_at:
            try:
                if isinstance(created_at, str) and 'T' not in created_at:
                    validated['created_at'] = f"{created_at}T00:00:00"
                else:
                    validated['created_at'] = created_at
            except:
                validated['created_at'] = datetime.now().isoformat()
        else:
            validated['created_at'] = datetime.now().isoformat()
        
        return validated
    
    @staticmethod
    def validate_invoice_item(item: Dict[str, Any], invoice_id: str, client_name: str = '') -> Dict[str, Any]:
        """التحقق من صحة بيانات بند الفاتورة وتنظيفها"""
        validated = {}
        
        # التحقق من المعرف الفريد
        if not item.get('id'):
            raise ValueError("معرف البند مطلوب")
        validated['id'] = str(item['id'])
        
        # ربط البند بالفاتورة
        validated['invoice_id'] = str(invoice_id)
        
        # التحقق من البيانات الرقمية
        validated['quantity'] = float(item.get('quantity', 0))
        validated['unit_price'] = float(item.get('unit_price', 0))
        validated['total_price'] = float(item.get('subtotal', 0))
        
        # معلومات المنتج
        validated['product_id'] = item.get('product_id')
        validated['product_code'] = str(item.get('item', ''))[:100]
        validated['client_business_name'] = str(client_name)[:255]
        
        return validated

class SupabaseClient:
    """فئة محسنة للتعامل مع Supabase"""
    
    def __init__(self, base_url: str, headers: Dict[str, str]):
        self.base_url = base_url
        self.headers = headers
        self.batch_size = 100  # حجم الدفعة
    
    def upsert_data(self, table: str, data: List[Dict[str, Any]], label: str = "") -> Dict[str, int]:
        """إدراج أو تحديث البيانات في دفعات"""
        if not data:
            logger.warning(f"لا توجد بيانات لإرسالها إلى جدول {table}")
            return {"success": 0, "failed": 0}
        
        success_count = 0
        failed_count = 0
        failed_records = []
        
        # تقسيم البيانات إلى دفعات
        for i in range(0, len(data), self.batch_size):
            batch = data[i:i + self.batch_size]
            batch_num = (i // self.batch_size) + 1
            total_batches = (len(data) + self.batch_size - 1) // self.batch_size
            
            logger.info(f"🔄 {label} - معالجة الدفعة {batch_num}/{total_batches} ({len(batch)} سجل)")
            
            try:
                # استخدام upsert مع تحديد المفتاح الأساسي
                url = f"{self.base_url}/rest/v1/{table}"
                
                # إعداد headers للupsert
                upsert_headers = self.headers.copy()
                upsert_headers['Prefer'] = 'resolution=merge-duplicates'
                
                response = requests.post(url, headers=upsert_headers, json=batch)
                
                if response.status_code in [200, 201]:
                    success_count += len(batch)
                    logger.info(f"✅ {label} - نجحت الدفعة {batch_num}: {len(batch)} سجل")
                else:
                    failed_count += len(batch)
                    failed_records.extend(batch)
                    logger.error(f"❌ {label} - فشلت الدفعة {batch_num}: {response.status_code}")
                    logger.error(f"تفاصيل الخطأ: {response.text}")
                    
                    # محاولة إرسال السجلات واحداً تلو الآخر في حالة فشل الدفعة
                    self._retry_individual_records(table, batch, label)
                    
            except Exception as e:
                failed_count += len(batch)
                failed_records.extend(batch)
                logger.error(f"❌ {label} - استثناء في الدفعة {batch_num}: {str(e)}")
            
            # توقف قصير بين الدفعات لتجنب تحميل الخادم
            time.sleep(0.5)
        
        # حفظ السجلات الفاشلة للمراجعة
        if failed_records:
            self._save_failed_records(table, failed_records, label)
        
        logger.info(f"📊 {label} - النتيجة النهائية: ناجحة {success_count}, فاشلة {failed_count}")
        return {"success": success_count, "failed": failed_count}
    
    def _retry_individual_records(self, table: str, batch: List[Dict], label: str):
        """إعادة محاولة إرسال السجلات واحداً تلو الآخر"""
        logger.info(f"🔄 {label} - محاولة إرسال السجلات منفردة...")
        
        url = f"{self.base_url}/rest/v1/{table}"
        headers = self.headers.copy()
        headers['Prefer'] = 'resolution=merge-duplicates'
        
        for record in batch:
            try:
                response = requests.post(url, headers=headers, json=[record])
                if response.status_code in [200, 201]:
                    logger.debug(f"✅ {label} - نجح السجل {record.get('id', 'غير محدد')}")
                else:
                    logger.warning(f"⚠️ {label} - فشل السجل {record.get('id', 'غير محدد')}: {response.text}")
            except Exception as e:
                logger.warning(f"⚠️ {label} - استثناء في السجل {record.get('id', 'غير محدد')}: {str(e)}")
    
    def _save_failed_records(self, table: str, failed_records: List[Dict], label: str):
        """حفظ السجلات الفاشلة في ملف للمراجعة"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"failed_{table}_{timestamp}.json"
        
        try:
            import json
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(failed_records, f, ensure_ascii=False, indent=2)
            logger.info(f"💾 {label} - تم حفظ {len(failed_records)} سجل فاشل في {filename}")
        except Exception as e:
            logger.error(f"❌ فشل في حفظ السجلات الفاشلة: {str(e)}")

def fetch_with_retry(url: str, headers: Dict[str, str], params: Optional[Dict] = None, retries: int = 3, delay: int = 2) -> Optional[Dict]:
    """جلب البيانات مع إعادة المحاولة في حالة الفشل"""
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"⚠️ محاولة {attempt+1} فشلت: {response.status_code} - {response.text}")
        except requests.exceptions.Timeout:
            logger.warning(f"⚠️ محاولة {attempt+1} انتهت المهلة الزمنية")
        except Exception as e:
            logger.warning(f"⚠️ محاولة {attempt+1} فشلت: {str(e)}")
        
        if attempt < retries - 1:  # لا نتوقف في المحاولة الأخيرة
            time.sleep(delay * (attempt + 1))  # زيادة وقت التوقف مع كل محاولة
    
    return None

def fetch_invoice_details(inv_id: str) -> Optional[Dict]:
    """جلب تفاصيل الفاتورة مع البنود"""
    url = f"{BASE_URL}/v2/api/entity/invoice/{inv_id}?include=invoice_item"
    return fetch_with_retry(url, HEADERS_DAFTRA)

def fetch_all():
    """الدالة الرئيسية لجلب جميع البيانات وحفظها في Supabase"""
    logger.info("🚀 بدء عملية جلب البيانات من دفترة...")
    
    # إنشاء عميل Supabase محسن
    supabase_client = SupabaseClient(SUPABASE_URL, HEADERS_SUPABASE)
    
    # إنشاء مدقق البيانات
    validator = DataValidator()
    
    all_invoices = []
    all_items = []
    processing_stats = {
        "total_invoices_processed": 0,
        "total_items_processed": 0,
        "validation_errors": 0,
        "api_errors": 0
    }
    
    for branch in BRANCH_IDS:
        logger.info(f"🏢 بدء معالجة الفرع {branch}")
        page = 1
        branch_invoices = 0
        branch_items = 0
        
        while True:
            url = f"{BASE_URL}/v2/api/entity/invoice/list/1"
            params = {
                "filter[branch_id]": branch,
                "page": page,
                "limit": PAGE_LIMIT
            }
            
            logger.info(f"📄 جلب الصفحة {page} للفرع {branch}...")
            data = fetch_with_retry(url, HEADERS_DAFTRA, params=params)
            
            if data is None:
                logger.error(f"❌ فشل في جلب البيانات للفرع {branch} الصفحة {page}")
                processing_stats["api_errors"] += 1
                break
            
            items = data.get("data") or []
            if not isinstance(items, list):
                items = [items]
            
            # تصفية الفواتير حسب النوع المطلوب
            valid_items = [inv for inv in items if int(inv.get("type", -1)) == EXPECTED_TYPE]
            logger.info(f"📋 فرع {branch} - صفحة {page}: {len(valid_items)} فاتورة صالحة من أصل {len(items)}")
            
            if not valid_items:
                logger.info(f"✅ انتهاء فواتير الفرع {branch} في الصفحة {page}")
                break
            
            # معالجة كل فاتورة
            for inv in valid_items:
                try:
                    # جلب تفاصيل الفاتورة
                    invoice_data = fetch_invoice_details(inv["id"])
                    if not invoice_data:
                        logger.error(f"❌ فشل في جلب تفاصيل الفاتورة {inv['id']}")
                        processing_stats["api_errors"] += 1
                        continue
                    
                    # التحقق من وجود البنود
                    invoice_items = invoice_data.get("invoice_item")
                    if not isinstance(invoice_items, list):
                        logger.warning(f"⚠️ الفاتورة {inv['id']} لا تحتوي على بنود صالحة")
                        invoice_items = []
                    
                    logger.info(f"📑 الفاتورة {inv['id']}: {len(invoice_items)} بند")
                    
                    # التحقق من صحة بيانات الفاتورة
                    try:
                        validated_invoice = validator.validate_invoice(invoice_data)
                        all_invoices.append(validated_invoice)
                        branch_invoices += 1
                        processing_stats["total_invoices_processed"] += 1
                    except ValueError as e:
                        logger.error(f"❌ خطأ في التحقق من الفاتورة {inv['id']}: {str(e)}")
                        processing_stats["validation_errors"] += 1
                        continue
                    
                    # معالجة بنود الفاتورة
                    client_name = invoice_data.get("client_business_name", "")
                    for item in invoice_items:
                        try:
                            validated_item = validator.validate_invoice_item(
                                item, 
                                invoice_data["id"], 
                                client_name
                            )
                            all_items.append(validated_item)
                            branch_items += 1
                            processing_stats["total_items_processed"] += 1
                        except ValueError as e:
                            logger.error(f"❌ خطأ في التحقق من البند {item.get('id', 'غير محدد')}: {str(e)}")
                            processing_stats["validation_errors"] += 1
                            continue
                
                except Exception as e:
                    logger.error(f"❌ خطأ عام في معالجة الفاتورة {inv['id']}: {str(e)}")
                    processing_stats["api_errors"] += 1
                    continue
            
            # التحقق من انتهاء الصفحات
            if len(items) < PAGE_LIMIT:
                logger.info(f"✅ انتهاء فواتير الفرع {branch} - إجمالي الصفحات: {page}")
                break
            
            page += 1
            time.sleep(1)  # توقف قصير بين الصفحات
        
        logger.info(f"📊 إحصائيات الفرع {branch}: {branch_invoices} فاتورة، {branch_items} بند")
    
    # طباعة الإحصائيات النهائية
    logger.info("📊 إحصائيات المعالجة النهائية:")
    logger.info(f"   - الفواتير المعالجة: {processing_stats['total_invoices_processed']}")
    logger.info(f"   - البنود المعالجة: {processing_stats['total_items_processed']}")
    logger.info(f"   - أخطاء التحقق: {processing_stats['validation_errors']}")
    logger.info(f"   - أخطاء API: {processing_stats['api_errors']}")
    
    # حفظ البيانات في Supabase
    results = {}
    
    if all_invoices:
        logger.info(f"🔄 بدء رفع {len(all_invoices)} فاتورة إلى Supabase...")
        results["invoices"] = supabase_client.upsert_data("invoices", all_invoices, "الفواتير")
    else:
        logger.warning("⚠️ لا توجد فواتير للرفع")
        results["invoices"] = {"success": 0, "failed": 0}
    
    if all_items:
        logger.info(f"🔄 بدء رفع {len(all_items)} بند إلى Supabase...")
        results["items"] = supabase_client.upsert_data("invoice_items", all_items, "بنود الفواتير")
    else:
        logger.warning("⚠️ لا توجد بنود للرفع")
        results["items"] = {"success": 0, "failed": 0}
    
    # تقرير نهائي
    logger.info("🎉 انتهاء العملية - التقرير النهائي:")
    logger.info(f"   📋 الفواتير: {results['invoices']['success']} نجحت، {results['invoices']['failed']} فشلت")
    logger.info(f"   📝 البنود: {results['items']['success']} نجح، {results['items']['failed']} فشل")
    
    return {
        "processing_stats": processing_stats,
        "upload_results": results,
        "summary": {
            "total_invoices": len(all_invoices),
            "total_items": len(all_items),
            "successful_invoices": results["invoices"]["success"],
            "successful_items": results["items"]["success"]
        }
    }

if __name__ == "__main__":
    try:
        result = fetch_all()
        print("✅ العملية اكتملت بنجاح")
        print(f"النتائج: {result['summary']}")
    except Exception as e:
        logger.error(f"❌ خطأ عام في تشغيل البرنامج: {str(e)}")
        raise

