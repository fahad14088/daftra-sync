import time
import requests
import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import os

# إعداد المتغيرات مباشرة
BASE_URL = os.getenv("DAFTRA_URL", "https://shadowpeace.daftra.com") + "/v2/api"
DAFTRA_API_KEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/") + "/rest/v1"
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {
    "apikey": DAFTRA_API_KEY,
    "Content-Type": "application/json"
}

HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

EXPECTED_TYPE = 0  # للمبيعات
PAGE_LIMIT = 50
BRANCH_IDS = [2, 1]
BATCH_SIZE = 50
MAX_RETRIES = 3
RETRY_DELAY = 2

# إعداد نظام التسجيل
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
    def validate_invoice(invoice: Dict[str, Any]) -> bool:
        """التحقق من صحة بيانات الفاتورة"""
        required_fields = ['id']
        return all(field in invoice and invoice[field] is not None for field in required_fields)
    
    @staticmethod
    def validate_item(item: Dict[str, Any]) -> bool:
        """التحقق من صحة بيانات البند"""
        required_fields = ['id']
        return all(field in item and item[field] is not None for field in required_fields)
    
    @staticmethod
    def clean_invoice_data(invoice: Dict[str, Any]) -> Dict[str, Any]:
        """تنظيف وتحويل بيانات الفاتورة - أسماء الحقول الصحيحة"""
        cleaned = {
            'id': str(invoice.get('id', '')),
            "invoice_id": str(invoice.get("id", "")),  # إضافة عمود invoice_id هنا
            'invoice_no': str(invoice.get('no', '')),
            'invoice_date': DataValidator.format_date(invoice.get('date')),
            'customer_id': str(invoice.get('client_id', '')),
            'summary_total': float(invoice.get('summary_total', 0)),
            'branch': int(invoice.get('store_id', 0)),
            'client_business_name': str(invoice.get('client_business_name', ''))[:255],
            'client_city': str(invoice.get('client_city', ''))[:100],
            'summary_paid': float(invoice.get('summary_paid', 0)),
            'summary_unpaid': float(invoice.get('summary_unpaid', 0)),

            # ✅ إضافات فقط (بدون تعديل أي سطر قديم)
            'staff_id': int(invoice.get('staff_id', 0)),
            'staff_name': str(invoice.get('staff_name', ''))[:255],

            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
        }
        return cleaned
    
    @staticmethod
    def clean_item_data(item: Dict[str, Any], invoice_id: str, client_name: str, supabase_client=None) -> Dict[str, Any]:
        """تنظيف وتحويل بيانات البند - مع تصحيح product_code"""
        
        product_id = str(item.get('product_id', ''))
        # الكود الخطأ من API دفترة
        wrong_code = str(item.get('item', ''))[:50]
        
        # جلب الكود الصحيح من جدول products
        if product_id and supabase_client:
            correct_code = supabase_client.get_correct_product_code(product_id)
            # استخدم الكود الصحيح إذا وُجد، وإلا استخدم الخطأ كـ backup
            product_code = correct_code if correct_code else wrong_code
            
            # تسجيل التصحيح إذا تم
            if correct_code and correct_code != wrong_code:
                logger.info(f"تم تصحيح الكود للمنتج {product_id}: '{wrong_code}' → '{correct_code}'")
        else:
            product_code = wrong_code
        subtotal_pre_tax_item = float(item.get('unit_price', 0)) * float(item.get('quantity', 0))

        cleaned = {
            'id': str(item.get('id', '')),
            'invoice_id': str(invoice_id),
            'quantity': float(item.get('quantity', 0)),
            'unit_price': float(item.get('unit_price', 0)),
            'subtotal': subtotal_pre_tax_item,
            'product_id': product_id,
            'product_code': product_code,  # الكود الصحيح الآن
            'client_business_name': str(client_name)[:255],
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        return cleaned
    
    @staticmethod
    def format_date(date_str: Any) -> Optional[str]:
        """تحويل التاريخ إلى صيغة ISO"""
        if not date_str:
            return None
        
        try:
            if isinstance(date_str, str):
                # محاولة تحويل التاريخ من صيغ مختلفة
                for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%d/%m/%Y']:
                    try:
                        dt = datetime.strptime(date_str, fmt)
                        return dt.isoformat()
                    except ValueError:
                        continue
            return str(date_str)
        except Exception:
            return None


class SupabaseClient:
    """عميل محسن للتعامل مع Supabase"""
    
    def __init__(self):
        self.base_url = SUPABASE_URL
        self.headers = HEADERS_SUPABASE
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def get_correct_product_code(self, product_id: str) -> str:
        """جلب الكود الصحيح من جدول products بناءً على product_id"""
        if not product_id:
            return ""
        
        try:
            # العلاقة الصحيحة: invoice_items.product_id = products.product_id
            url = f"{self.base_url}/products?product_id=eq.{product_id}&select=product_code"
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 200:
                products = response.json()
                if products and len(products) > 0:
                    correct_code = products[0].get('product_code', '')
                    if correct_code:
                        return correct_code.strip()
                        
        except Exception as e:
            logger.error(f"خطأ في جلب كود المنتج {product_id}: {e}")
        
        return ""
    
    def fix_existing_product_codes(self) -> Dict[str, int]:
        """تصحيح أكواد المنتجات للبيانات الموجودة في قاعدة البيانات"""
        logger.info("بدء تصحيح أكواد المنتجات الموجودة...")
        
        stats = {'fixed_count': 0, 'total_checked': 0, 'errors': 0}
        
        try:
            # جلب البنود على دفعات لتجنب تحميل كمية كبيرة من البيانات
            limit = 1000
            offset = 0
            
            while True:
                # جلب دفعة من البنود
                items_url = f"{self.base_url}/invoice_items?select=id,product_id,product_code&limit={limit}&offset={offset}"
                response = self.session.get(items_url, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"فشل في جلب البنود: {response.status_code}")
                    break
                
                items = response.json()
                
                if not items:
                    logger.info("انتهاء معالجة جميع البنود")
                    break
                
                logger.info(f"معالجة دفعة من {len(items)} بند (إجمالي: {stats['total_checked']})")
                
                # معالجة كل بند في الدفعة
                updates_batch = []
                
                for item in items:
                    stats['total_checked'] += 1
                    
                    product_id = item.get('product_id')
                    current_code = item.get('product_code', '')
                    
                    if product_id:
                        # جلب الكود الصحيح
                        correct_code = self.get_correct_product_code(product_id)
                        
                        # إذا كان الكود مختلف، حضّر للتحديث
                        if correct_code and correct_code != current_code:
                            updates_batch.append({
                                'id': item['id'],
                                'product_code': correct_code,
                                'current_code': current_code
                            })
                            
                            logger.info(f"سيتم تصحيح البند {item['id']}: '{current_code}' → '{correct_code}'")
                
                # تطبيق التحديثات على الدفعة
                for update in updates_batch:
                    try:
                        update_url = f"{self.base_url}/invoice_items?id=eq.{update['id']}"
                        update_data = {"product_code": update['product_code']}
                        
                        update_response = self.session.patch(
                            update_url, 
                            json=update_data, 
                            timeout=30
                        )
                        
                        if update_response.status_code == 204:
                            stats['fixed_count'] += 1
                            logger.info(f"تم تصحيح البند {update['id']}")
                        else:
                            stats['errors'] += 1
                            logger.error(f"فشل تحديث البند {update['id']}: {update_response.status_code}")
                            
                    except Exception as e:
                        stats['errors'] += 1
                        logger.error(f"خطأ في تحديث البند {update['id']}: {e}")
                
                # انتظار قصير بين الدفعات
                if updates_batch:
                    time.sleep(1)
                
                offset += limit
                
                # تحديث التقدم
                if stats['total_checked'] % 5000 == 0:
                    logger.info(f"تقدم العملية: فحص {stats['total_checked']} بند، تصحيح {stats['fixed_count']} بند")
        
        except Exception as e:
            logger.error(f"خطأ عام في تصحيح الأكواد: {e}")
            stats['errors'] += 1
        
        # التقرير النهائي
        logger.info("تقرير تصحيح أكواد المنتجات:")
        logger.info(f"   - إجمالي البنود المفحوصة: {stats['total_checked']}")
        logger.info(f"   - البنود المُصححة: {stats['fixed_count']}")
        logger.info(f"   - الأخطاء: {stats['errors']}")
        
        if stats['fixed_count'] > 0:
            logger.info(f"تم تصحيح {stats['fixed_count']} بند بنجاح!")
        else:
            logger.info("لا توجد أكواد تحتاج تصحيح")
        
        return stats
    
    def upsert_batch(self, table: str, data: List[Dict[str, Any]]) -> tuple[int, int]:
        """إدراج أو تحديث دفعة من البيانات مع حل مشكلة التكرار"""
        if not data:
            return 0, 0
        
        # إضافة معاملة للتعامل مع البيانات المكررة
        url = f"{self.base_url}/{table}?on_conflict=id"
        
        # إعداد headers خاصة للـ upsert
        upsert_headers = {
            **self.headers,
            "Prefer": "resolution=merge-duplicates,return=minimal"
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(url, json=data, headers=upsert_headers, timeout=30)
                
                if response.status_code in [200, 201]:
                    logger.info(f"تم حفظ/تحديث {len(data)} سجل في جدول {table}")
                    return len(data), 0
                elif response.status_code == 409:
                    # في حالة التكرار، جرب مرة أخرى مع تحديث فقط
                    logger.warning(f"بيانات مكررة في {table}، محاولة التحديث...")
                    update_headers = {
                        **self.headers,
                        "Prefer": "resolution=ignore-duplicates,return=minimal"
                    }
                    response = self.session.post(url, json=data, headers=update_headers, timeout=30)
                    if response.status_code in [200, 201]:
                        logger.info(f"تم تحديث {len(data)} سجل في جدول {table}")
                        return len(data), 0
                else:
                    logger.error(f"خطأ في حفظ {table}: {response.status_code} - {response.text}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"خطأ في الاتصال مع Supabase (محاولة {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    
        return 0, len(data)


class DaftraClient:
    """عميل محسن للتعامل مع API دفترة"""
    
    def __init__(self):
        self.base_url = BASE_URL
        self.headers = HEADERS_DAFTRA
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    # ✅ إضافة فقط: جلب الموظفين كخريطة id->name
    def fetch_staff_map(self) -> Dict[str, str]:
        staff_map = {}
        page = 1
        limit = 100

        while True:
            url = os.getenv("DAFTRA_URL", "https://shadowpeace.daftra.com") + f"/api2/staff?limit={limit}&page={page}"
            try:
                r = self.session.get(url, timeout=30)
                if r.status_code != 200:
                    break

                data = r.json().get("data", [])
                if not data:
                    break

                for row in data:
                    s = row.get("Staff", {})
                    sid = str(s.get("id", ""))
                    name = str(s.get("name", "")).strip()
                    if sid and name:
                        staff_map[sid] = name

                page += 1
                time.sleep(0.3)
            except Exception as e:
                logger.error(f"خطأ بجلب الموظفين: {e}")
                break

        logger.info(f"تم تحميل {len(staff_map)} موظف من دفترة")
        return staff_map
    
    def fetch_invoices(self, branch_id: int, page: int = 1) -> Dict[str, Any]:
        """جلب قائمة الفواتير من فرع معين"""
        url = f"{self.base_url}/entity/invoice/list/1"
        params = {
            'filter[type]': EXPECTED_TYPE,
            'filter[branch_id]': branch_id,
            'page': page,
            'limit': PAGE_LIMIT
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"خطأ في جلب الفواتير: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"خطأ في الاتصال مع دفترة (محاولة {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    
        return {}
    
    def fetch_invoice_details(self, invoice_id: str) -> Dict[str, Any]:
        """جلب تفاصيل فاتورة واحدة مع البنود"""
        url = f"{self.base_url}/entity/invoice/{invoice_id}?include=InvoiceItem"
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"خطأ في جلب تفاصيل الفاتورة {invoice_id}: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"خطأ في الاتصال مع دفترة (محاولة {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    
        return {}


def fetch_missing_items(daftra_client: DaftraClient, supabase_client: SupabaseClient) -> Dict[str, int]:
    """جلب البنود المفقودة للفواتير الموجودة بدون بنود"""
    logger.info("البحث عن الفواتير بدون بنود...")
    
    stats = {'items_saved': 0, 'items_failed': 0}
    
    try:
        # جلب الفواتير اللي ماها بنود
        invoices_url = f"{supabase_client.base_url}/invoices?select=id,client_business_name"
        invoices_response = supabase_client.session.get(invoices_url)
        
        if invoices_response.status_code != 200:
            return stats
            
        all_invoices = invoices_response.json()
        missing_invoices = []
        
        for invoice in all_invoices:
            invoice_id = invoice['id']
            items_url = f"{supabase_client.base_url}/invoice_items?invoice_id=eq.{invoice_id}&select=id&limit=1"
            items_response = supabase_client.session.get(items_url)
            
            if items_response.status_code == 200:
                items = items_response.json()
                if len(items) == 0:
                    missing_invoices.append(invoice)
        
        logger.info(f"وُجد {len(missing_invoices)} فاتورة بدون بنود")
        
        if not missing_invoices:
            return stats
        
        items_batch = []
        
        for invoice in missing_invoices:
            invoice_id = invoice['id']
            client_name = invoice.get('client_business_name', '')
            
            invoice_details = daftra_client.fetch_invoice_details(invoice_id)
            
            if not invoice_details:
                continue
            
            items = invoice_details.get('invoice_item', [])
            
            for item in items:
                if DataValidator.validate_item(item):
                    # تمرير supabase_client للحصول على الكود الصحيح
                    cleaned_item = DataValidator.clean_item_data(item, invoice_id, client_name, supabase_client)
                    items_batch.append(cleaned_item)
            
            if len(items_batch) >= BATCH_SIZE:
                saved, failed = supabase_client.upsert_batch('invoice_items', items_batch)
                stats['items_saved'] += saved
                stats['items_failed'] += failed
                items_batch = []
        
        if items_batch:
            saved, failed = supabase_client.upsert_batch('invoice_items', items_batch)
            stats['items_saved'] += saved
            stats['items_failed'] += failed
        
        logger.info(f"تم جلب {stats['items_saved']} بند مفقود")
        
    except Exception as e:
        logger.error(f"خطأ في جلب البنود المفقودة: {e}")
    
    return stats


def process_branch_invoices(daftra_client: DaftraClient, supabase_client: SupabaseClient, branch_id: int) -> Dict[str, int]:
    """معالجة فواتير فرع واحد"""
    logger.info(f"بدء معالجة الفرع {branch_id}")

    # ✅ إضافة فقط: تحميل الموظفين مرة واحدة
    staff_map = daftra_client.fetch_staff_map()
    
    stats = {
        'invoices_processed': 0,
        'items_processed': 0,
        'invoices_saved': 0,
        'items_saved': 0,
        'invoices_failed': 0,
        'items_failed': 0
    }
    
    page = 1
    invoices_batch = []
    items_batch = []
    
    while True:
        logger.info(f"جلب الصفحة {page} للفرع {branch_id}...")
        
        response_data = daftra_client.fetch_invoices(branch_id, page)
        
        if not response_data or 'data' not in response_data:
            logger.warning(f"لا توجد بيانات في الصفحة {page} للفرع {branch_id}")
            break
            
        invoices = response_data['data']
        
        if not invoices:
            logger.info(f"انتهاء فواتير الفرع {branch_id} في الصفحة {page}")
            break
        
        valid_invoices = 0
        
        for invoice in invoices:
            inv = invoice.get("Invoice", invoice)  # ✅ فك تغليف الفاتورة

            invoice_id = inv["id"]

            # تحقق هل الفاتورة موجودة مسبقًا في قاعدة البيانات
            check_url = f"{SUPABASE_URL}/invoices?id=eq.{invoice_id}&select=id"
            res_check = requests.get(check_url, headers=HEADERS_SUPABASE)
            
            if res_check.status_code == 200 and res_check.json():
                logger.info(f"الفاتورة {invoice_id} موجودة مسبقًا وتم تجاهلها")
                #continue

            # جلب تفاصيل الفاتورة مع البنود
            invoice_details = daftra_client.fetch_invoice_details(str(inv['id']))
            if not invoice_details:
                logger.warning(f"فشل في جلب تفاصيل الفاتورة {inv['id']}")
                continue

            details_invoice = invoice_details.get("Invoice", {})  # ✅ خذ تفاصيل Invoice فقط

            # دمج البيانات الأساسية مع التفاصيل
            full_invoice = {**inv, **details_invoice}

            # ✅ إضافة فقط: ربط staff_id بالاسم ووضعه داخل الفاتورة
            sid = str(full_invoice.get("staff_id", 0))
            full_invoice["staff_name"] = staff_map.get(sid, "")
            
            # تنظيف بيانات الفاتورة
            try:
                cleaned_invoice = DataValidator.clean_invoice_data(full_invoice)
                invoices_batch.append(cleaned_invoice)
                valid_invoices += 1
                
                # معالجة بنود الفاتورة
                items = invoice_details.get('invoice_item', [])
                client_name = full_invoice.get('client_business_name', '')
                
                for item in items:
                    if DataValidator.validate_item(item):
                        # تمرير supabase_client للحصول على الكود الصحيح
                        cleaned_item = DataValidator.clean_item_data(item, inv['id'], client_name, supabase_client)
                        items_batch.append(cleaned_item)
                        
            except Exception as e:
                logger.error(f"خطأ في معالجة الفاتورة {inv.get('id', 'غير معروف')}: {e}")
                continue
        
        logger.info(f"فرع {branch_id} - صفحة {page}: {valid_invoices} فاتورة صالحة من أصل {len(invoices)}")
        stats['invoices_processed'] += valid_invoices
        stats['items_processed'] += len(items_batch)
        
        # حفظ الفواتير أولاً عند الوصول للحد الأقصى
        if len(invoices_batch) >= BATCH_SIZE:
            # حفظ الفواتير أولاً
            saved, failed = supabase_client.upsert_batch('invoices', invoices_batch)
            stats['invoices_saved'] += saved
            stats['invoices_failed'] += failed
            invoices_batch = []
            
            # انتظار قصير للتأكد من حفظ الفواتير
            time.sleep(1)
            
            # ثم حفظ البنود المرتبطة
            if items_batch:
                saved, failed = supabase_client.upsert_batch('invoice_items', items_batch)
                stats['items_saved'] += saved
                stats['items_failed'] += failed
                items_batch = []
        
        page += 1
    
    # حفظ الدفعات المتبقية - الفواتير أولاً
    if invoices_batch:
        saved, failed = supabase_client.upsert_batch('invoices', invoices_batch)
        stats['invoices_saved'] += saved
        stats['invoices_failed'] += failed
        
        # انتظار قصير
        time.sleep(1)
        
    # ثم حفظ البنود المتبقية
    if items_batch:
        saved, failed = supabase_client.upsert_batch('invoice_items', items_batch)
        stats['items_saved'] += saved
        stats['items_failed'] += failed
    
    logger.info(f"إحصائيات الفرع {branch_id}: {stats['invoices_processed']} فاتورة، {stats['items_processed']} بند")
    return stats


def main():
    """الدالة الرئيسية"""
    logger.info("بدء عملية جلب البيانات من دفترة...")
    
    # التحقق من المتغيرات المطلوبة
    if not all([DAFTRA_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        logger.error("متغيرات البيئة مفقودة!")
        return
    
    # إنشاء العملاء
    daftra_client = DaftraClient()
    supabase_client = SupabaseClient()
    
    # تصحيح البيانات القديمة أولاً
    logger.info("بدء تصحيح أكواد المنتجات للبيانات الموجودة...")
    fix_stats = supabase_client.fix_existing_product_codes()
    
    # إحصائيات إجمالية
    total_stats = {
        'invoices_processed': 0,
        'items_processed': 0,
        'invoices_saved': 0,
        'items_saved': 0,
        'invoices_failed': 0,
        'items_failed': 0
    }
    
    # معالجة كل فرع (للبيانات الجديدة)
    for branch_id in BRANCH_IDS:
        try:
            branch_stats = process_branch_invoices(daftra_client, supabase_client, branch_id)
            
            # تجميع الإحصائيات
            for key in total_stats:
                total_stats[key] += branch_stats[key]
                
        except Exception as e:
            logger.error(f"خطأ في معالجة الفرع {branch_id}: {e}")
    
    # التقرير النهائي
    logger.info("إحصائيات المعالجة النهائية:")
    logger.info(f"   - البيانات القديمة المُصححة: {fix_stats['fixed_count']}")
    logger.info(f"   - الفواتير المعالجة (جديدة): {total_stats['invoices_processed']}")
    logger.info(f"   - البنود المعالجة (جديدة): {total_stats['items_processed']}")
    logger.info(f"   - الفواتير المحفوظة: {total_stats['invoices_saved']}")
    logger.info(f"   - البنود المحفوظة: {total_stats['items_saved']}")
    logger.info(f"   - أخطاء الفواتير: {total_stats['invoices_failed']}")
    logger.info(f"   - أخطاء البنود: {total_stats['items_failed']}")
    
    if total_stats['invoices_processed'] == 0 and fix_stats['fixed_count'] == 0:
        logger.warning("لا توجد فواتير للمعالجة ولا بيانات للتصحيح")
    
    logger.info("انتهاء العملية - التقرير النهائي:")
    logger.info(f"   البيانات المُصححة: {fix_stats['fixed_count']} بند")
    logger.info(f"   الفواتير الجديدة: {total_stats['invoices_saved']} نجحت، {total_stats['invoices_failed']} فشلت")
    logger.info(f"   البنود الجديدة: {total_stats['items_saved']} نجح، {total_stats['items_failed']} فشل")


# إضافة alias للتوافق مع main.py
fetch_all = main

if __name__ == "__main__":
    main()
