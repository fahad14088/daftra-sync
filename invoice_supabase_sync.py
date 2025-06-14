import time
import requests
import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Optional
import os

# إعداد المتغيرات مباشرة
BASE_URL = os.getenv("DAFTRA_URL", "https://shadowpeace.daftra.com/v2/api")
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

# إعداد نظام التسجيل المفصل
logging.basicConfig(
    level=logging.DEBUG, 
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
        """تنظيف وتحويل بيانات الفاتورة"""
        cleaned = {
            'id': str(invoice.get('id', '')),
            'invoice_no': str(invoice.get('no', '')),
            'invoice_date': DataValidator.format_date(invoice.get('date')),
            'customer_id': str(invoice.get('customer_id', '')),
            'total': float(invoice.get('total', 0)),
            'branch': int(invoice.get('store_id', 0)),
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
        """تنظيف وتحويل بيانات البند"""
        cleaned = {
            'id': str(item.get('id', '')),
            'invoice_id': str(invoice_id),
            'quantity': float(item.get('quantity', 0)),
            'unit_price': float(item.get('unit_price', 0)),
            'total_price': float(item.get('total_price', 0)),
            'product_id': str(item.get('product_id', '')),
            'product_code': str(item.get('product_code', ''))[:50],
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
        logger.info(f"🔗 Supabase URL: {self.base_url}")
        logger.info(f"🔑 Supabase Key: {SUPABASE_KEY[:20]}...")
    
    def upsert_batch(self, table: str, data: List[Dict[str, Any]]) -> tuple[int, int]:
        """إدراج أو تحديث دفعة من البيانات"""
        if not data:
            return 0, 0
        
        url = f"{self.base_url}/{table}"
        logger.info(f"📤 محاولة حفظ {len(data)} سجل في جدول {table}")
        logger.debug(f"🔗 URL: {url}")
        logger.debug(f"📋 عينة من البيانات: {json.dumps(data[0], indent=2, ensure_ascii=False)}")
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(url, json=data, timeout=30)
                
                logger.info(f"📊 استجابة Supabase: {response.status_code}")
                logger.debug(f"📄 محتوى الاستجابة: {response.text}")
                
                if response.status_code in [200, 201]:
                    logger.info(f"✅ تم حفظ {len(data)} سجل في جدول {table}")
                    return len(data), 0
                else:
                    logger.error(f"❌ خطأ في حفظ {table}: {response.status_code}")
                    logger.error(f"📄 تفاصيل الخطأ: {response.text}")
                    
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
        logger.info(f"🔗 Daftra URL: {self.base_url}")
        logger.info(f"🔑 Daftra API Key: {DAFTRA_API_KEY[:20]}...")
    
    def fetch_invoices(self, branch_id: int, page: int = 1) -> Dict[str, Any]:
        """جلب الفواتير من فرع معين"""
        url = f"{self.base_url}/entity/invoice/list/1"
        params = {
            'filter[type]': EXPECTED_TYPE,
            'filter[branch_id]': branch_id,
            'page': page,
            'limit': PAGE_LIMIT
        }
        
        logger.info(f"📡 طلب API: {url}")
        logger.info(f"📋 المعاملات: {params}")
        logger.debug(f"🔑 Headers: {self.headers}")
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                logger.info(f"📊 استجابة دفترة: {response.status_code}")
                logger.debug(f"📄 محتوى الاستجابة: {response.text[:500]}...")
                
                if response.status_code == 200:
                    data = response.json()
                    logger.info(f"📋 عدد الفواتير المستلمة: {len(data.get('data', []))}")
                    return data
                else:
                    logger.error(f"❌ خطأ في جلب الفواتير: {response.status_code}")
                    logger.error(f"📄 تفاصيل الخطأ: {response.text}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ خطأ في الاتصال مع دفترة (محاولة {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
            except json.JSONDecodeError as e:
                logger.error(f"❌ خطأ في تحليل JSON: {e}")
                logger.error(f"📄 محتوى الاستجابة: {response.text}")
                    
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
        
        if not response_data:
            logger.warning(f"⚠️ لا توجد استجابة من API للصفحة {page} للفرع {branch_id}")
            break
            
        if 'data' not in response_data:
            logger.warning(f"⚠️ لا يوجد مفتاح 'data' في الاستجابة للصفحة {page} للفرع {branch_id}")
            logger.debug(f"📄 محتوى الاستجابة: {json.dumps(response_data, indent=2, ensure_ascii=False)}")
            break
            
        invoices = response_data['data']
        
        if not invoices:
            logger.info(f"✅ انتهاء فواتير الفرع {branch_id} في الصفحة {page}")
            break
        
        valid_invoices = 0
        
        for invoice in invoices:
            logger.debug(f"📋 معالجة الفاتورة: {json.dumps(invoice, indent=2, ensure_ascii=False)}")
            
            if not DataValidator.validate_invoice(invoice):
                logger.warning(f"⚠️ فاتورة غير صالحة: {invoice}")
                continue
                
            # تنظيف بيانات الفاتورة
            try:
                cleaned_invoice = DataValidator.clean_invoice_data(invoice)
                invoices_batch.append(cleaned_invoice)
                valid_invoices += 1
                logger.debug(f"✅ تم تنظيف الفاتورة: {cleaned_invoice}")
                
                # معالجة بنود الفاتورة
                items = invoice.get('items', [])
                client_name = invoice.get('client_business_name', '')
                
                for item in items:
                    if DataValidator.validate_item(item):
                        cleaned_item = DataValidator.clean_item_data(item, invoice['id'], client_name)
                        items_batch.append(cleaned_item)
                        logger.debug(f"✅ تم تنظيف البند: {cleaned_item}")
                        
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
    logger.info(f"🔍 التحقق من متغيرات البيئة...")
    logger.info(f"   - DAFTRA_API_KEY: {'✅ موجود' if DAFTRA_API_KEY else '❌ مفقود'}")
    logger.info(f"   - SUPABASE_URL: {'✅ موجود' if SUPABASE_URL else '❌ مفقود'}")
    logger.info(f"   - SUPABASE_KEY: {'✅ موجود' if SUPABASE_KEY else '❌ مفقود'}")
    
    if not all([DAFTRA_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        logger.error("❌ متغيرات البيئة مفقودة!")
        return
    
    # إنشاء العملاء
    try:
        daftra_client = DaftraClient()
        supabase_client = SupabaseClient()
    except Exception as e:
        logger.error(f"❌ خطأ في إنشاء العملاء: {e}")
        return
    
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
            import traceback
            logger.error(f"📄 تفاصيل الخطأ: {traceback.format_exc()}")
    
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

