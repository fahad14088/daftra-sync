import time
import requests
import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import os

# إعداد المتغيرات مباشرة
BASE_URL = os.getenv("DAFTRA_URL", "https://shadowpeace.daftra.com" ) + "/v2/api"
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
BRANCH_IDS = [2, 3]
BATCH_SIZE = 100
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
            'invoice_no': str(invoice.get('no', '')),
            'invoice_date': DataValidator.format_date(invoice.get('date')),
            'customer_id': str(invoice.get('client_id', '')),  # تم التصحيح
            'summary_total': float(invoice.get('summary_total', 0)),  # تم التصحيح
            'branch': int(invoice.get('store_id', 0)),  # تم التصحيح
            'client_business_name': str(invoice.get('client_business_name', ''))[:255],
            'client_city': str(invoice.get('client_city', ''))[:100],
            'summary_paid': float(invoice.get('summary_paid', 0)),
            'summary_unpaid': float(invoice.get('summary_unpaid', 0)),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        return cleaned
    
    @staticmethod
    def clean_item_data(item: Dict[str, Any], invoice_id: str, client_name: str) -> Dict[str, Any]:
        """تنظيف وتحويل بيانات البند - أسماء الحقول الصحيحة"""
        cleaned = {
            'id': str(item.get('id', '')),
            'invoice_id': str(invoice_id),
            'quantity': float(item.get('quantity', 0)),
            'unit_price': float(item.get('unit_price', 0)),
            'subtotal': float(item.get('subtotal', 0)),
            'product_id': str(item.get('product_id', '')),
            'product_code': str(item.get('item', ''))[:50],  # تم التصحيح
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
    
    def upsert_batch(self, table: str, data: List[Dict[str, Any]]) -> tuple[int, int]:
        """إدراج أو تحديث دفعة من البيانات"""
        if not data:
            return 0, 0
        
        url = f"{self.base_url}/{table}"
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(url, json=data, timeout=30)
                
                if response.status_code in [200, 201]:
                    logger.info(f"✅ تم حفظ {len(data)} سجل في جدول {table}")
                    return len(data), 0
                else:
                    logger.error(f"❌ خطأ في حفظ {table}: {response.status_code} - {response.text}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ خطأ في الاتصال مع Supabase (محاولة {attempt + 1}): {e}")
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
                    logger.error(f"❌ خطأ في جلب الفواتير: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ خطأ في الاتصال مع دفترة (محاولة {attempt + 1}): {e}")
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
                    logger.error(f"❌ خطأ في جلب تفاصيل الفاتورة {invoice_id}: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ خطأ في الاتصال مع دفترة (محاولة {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    
        return {}

def process_branch_invoices(daftra_client: DaftraClient, supabase_client: SupabaseClient, branch_id: int) -> Dict[str, int]:
    """معالجة فواتير فرع واحد"""
    logger.info(f"🏢 بدء معالجة الفرع {branch_id}")
    
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
        logger.info(f"📄 جلب الصفحة {page} للفرع {branch_id}...")
        
        response_data = daftra_client.fetch_invoices(branch_id, page)
        
        if not response_data or 'data' not in response_data:
            logger.warning(f"⚠️ لا توجد بيانات في الصفحة {page} للفرع {branch_id}")
            break
            
        invoices = response_data['data']
        
        if not invoices:
            logger.info(f"✅ انتهاء فواتير الفرع {branch_id} في الصفحة {page}")
            break
        
        valid_invoices = 0
        
        for invoice in invoices:
            if not DataValidator.validate_invoice(invoice):
                continue
            
            # جلب تفاصيل الفاتورة مع البنود
            invoice_details = daftra_client.fetch_invoice_details(str(invoice['id']))
            
            if not invoice_details:
                logger.warning(f"⚠️ فشل في جلب تفاصيل الفاتورة {invoice['id']}")
                continue
            
            # دمج البيانات الأساسية مع التفاصيل
            full_invoice = {**invoice, **invoice_details}
            
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
                        cleaned_item = DataValidator.clean_item_data(item, invoice['id'], client_name)
                        items_batch.append(cleaned_item)
                        
            except Exception as e:
                logger.error(f"❌ خطأ في معالجة الفاتورة {invoice.get('id', 'غير معروف')}: {e}")
                continue
        
        logger.info(f"📋 فرع {branch_id} - صفحة {page}: {valid_invoices} فاتورة صالحة من أصل {len(invoices)}")
        stats['invoices_processed'] += valid_invoices
        stats['items_processed'] += len(items_batch)
        
        # حفظ الدفعات عند الوصول للحد الأقصى
        if len(invoices_batch) >= BATCH_SIZE:
            saved, failed = supabase_client.upsert_batch('invoices', invoices_batch)
            stats['invoices_saved'] += saved
            stats['invoices_failed'] += failed
            invoices_batch = []
            
        if len(items_batch) >= BATCH_SIZE:
            saved, failed = supabase_client.upsert_batch('invoice_items', items_batch)
            stats['items_saved'] += saved
            stats['items_failed'] += failed
            items_batch = []
        
        page += 1
        
        # حماية من الحلقات اللانهائية
        if page > 100:
            logger.warning(f"⚠️ تم الوصول للحد الأقصى من الصفحات للفرع {branch_id}")
            break
    
    # حفظ الدفعات المتبقية
    if invoices_batch:
        saved, failed = supabase_client.upsert_batch('invoices', invoices_batch)
        stats['invoices_saved'] += saved
        stats['invoices_failed'] += failed
        
    if items_batch:
        saved, failed = supabase_client.upsert_batch('invoice_items', items_batch)
        stats['items_saved'] += saved
        stats['items_failed'] += failed
    
    logger.info(f"📊 إحصائيات الفرع {branch_id}: {stats['invoices_processed']} فاتورة، {stats['items_processed']} بند")
    return stats

def main():
    """الدالة الرئيسية"""
    logger.info("🚀 بدء عملية جلب البيانات من دفترة...")
    
    # التحقق من المتغيرات المطلوبة
    if not all([DAFTRA_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        logger.error("❌ متغيرات البيئة مفقودة!")
        return
    
    # إنشاء العملاء
    daftra_client = DaftraClient()
    supabase_client = SupabaseClient()
    
    # إحصائيات إجمالية
    total_stats = {
        'invoices_processed': 0,
        'items_processed': 0,
        'invoices_saved': 0,
        'items_saved': 0,
        'invoices_failed': 0,
        'items_failed': 0
    }
    
    # معالجة كل فرع
    for branch_id in BRANCH_IDS:
        try:
            branch_stats = process_branch_invoices(daftra_client, supabase_client, branch_id)
            
            # تجميع الإحصائيات
            for key in total_stats:
                total_stats[key] += branch_stats[key]
                
        except Exception as e:
            logger.error(f"❌ خطأ في معالجة الفرع {branch_id}: {e}")
    
    # التقرير النهائي
    logger.info("📊 إحصائيات المعالجة النهائية:")
    logger.info(f"   - الفواتير المعالجة: {total_stats['invoices_processed']}")
    logger.info(f"   - البنود المعالجة: {total_stats['items_processed']}")
    logger.info(f"   - الفواتير المحفوظة: {total_stats['invoices_saved']}")
    logger.info(f"   - البنود المحفوظة: {total_stats['items_saved']}")
    logger.info(f"   - أخطاء الفواتير: {total_stats['invoices_failed']}")
    logger.info(f"   - أخطاء البنود: {total_stats['items_failed']}")
    
    if total_stats['invoices_processed'] == 0:
        logger.warning("⚠️ لا توجد فواتير للمعالجة")
    
    logger.info("🎉 انتهاء العملية - التقرير النهائي:")
    logger.info(f"   📋 الفواتير: {total_stats['invoices_saved']} نجحت، {total_stats['invoices_failed']} فشلت")
    logger.info(f"   📝 البنود: {total_stats['items_saved']} نجح، {total_stats['items_failed']} فشل")

# إضافة alias للتوافق مع main.py
fetch_all = main

if __name__ == "__main__":
    main()

