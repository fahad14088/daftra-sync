import time
import requests
import logging
import json
import os
from datetime import datetime
from typing import Dict, List, Any, Optional

# إعداد التسجيل
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('customers_sync.log', encoding='utf-8')
    ]
)
logger = logging.getLogger(__name__)

# إعداد المتغيرات - نفس طريقة الفواتير
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

BATCH_SIZE = 50
PAGE_LIMIT = 50
MAX_RETRIES = 3
RETRY_DELAY = 2

class DataValidator:
    """فئة للتحقق من صحة البيانات قبل الإرسال"""
    
    @staticmethod
    def validate_customer(customer: Dict[str, Any]) -> bool:
        """التحقق من صحة بيانات العميل"""
        required_fields = ['id']
        return all(field in customer and customer[field] is not None for field in required_fields)
    
    @staticmethod
    def clean_customer_data(customer: Dict[str, Any]) -> Dict[str, Any]:
        """تنظيف وتحويل بيانات العميل - نفس طريقة الفواتير"""
        cleaned = {
            'id': str(customer.get('id', '')),
            'customer_code': str(customer.get('code', '')),
            'name': str(customer.get('name', ''))[:255],
            'phone': str(customer.get('phone', ''))[:50],
            'email': str(customer.get('email', ''))[:255],
            'gender': str(customer.get('gender', ''))[:10],
            'birth_date': DataValidator.format_date(customer.get('birth_date')),
            'city': str(customer.get('city', ''))[:100],
            'region': str(customer.get('region', ''))[:100],
            'address': str(customer.get('address', ''))[:500],
            'total_spent': float(customer.get('total_spent', 0)),
            'total_invoices': int(customer.get('total_invoices', 0)),
            'max_order_value': float(customer.get('max_order_value', 0)),
            'average_order_value': float(customer.get('average_order_value', 0)),
            'payment_total': float(customer.get('payment_total', 0)),
            'last_order_date': DataValidator.format_date(customer.get('last_order_date')),
            'order_frequency_days': int(customer.get('order_frequency_days', 0)),
            'is_active': bool(customer.get('is_active', True)),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        return cleaned
    
    @staticmethod
    def format_date(date_str: Any) -> Optional[str]:
        """تحويل التاريخ إلى صيغة ISO - نفس طريقة الفواتير"""
        if not date_str or date_str == '0000-00-00':
            return None
        
        try:
            if isinstance(date_str, str):
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
    """عميل محسن للتعامل مع Supabase - نفس طريقة الفواتير"""
    
    def __init__(self):
        self.base_url = SUPABASE_URL
        self.headers = HEADERS_SUPABASE
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def upsert_batch(self, table: str, data: List[Dict[str, Any]]) -> tuple[int, int]:
        """إدراج أو تحديث دفعة من البيانات مع حل مشكلة التكرار"""
        if not data:
            return 0, 0
        
        url = f"{self.base_url}/{table}?on_conflict=id"
        
        upsert_headers = {
            **self.headers,
            "Prefer": "resolution=merge-duplicates,return=minimal"
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.post(url, json=data, headers=upsert_headers, timeout=30)
                
                if response.status_code in [200, 201]:
                    logger.info(f"✅ تم حفظ/تحديث {len(data)} سجل في جدول {table}")
                    return len(data), 0
                elif response.status_code == 409:
                    logger.warning(f"⚠️ بيانات مكررة في {table}، محاولة التحديث...")
                    update_headers = {
                        **self.headers,
                        "Prefer": "resolution=ignore-duplicates,return=minimal"
                    }
                    response = self.session.post(url, json=data, headers=update_headers, timeout=30)
                    if response.status_code in [200, 201]:
                        logger.info(f"✅ تم تحديث {len(data)} سجل في جدول {table}")
                        return len(data), 0
                else:
                    logger.error(f"❌ خطأ في حفظ {table}: {response.status_code} - {response.text}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ خطأ في الاتصال مع Supabase (محاولة {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    
        return 0, len(data)

class DaftraClient:
    """عميل محسن للتعامل مع API دفترة - نفس طريقة الفواتير"""
    
    def __init__(self):
        self.base_url = BASE_URL
        self.headers = HEADERS_DAFTRA
        self.session = requests.Session()
        self.session.headers.update(self.headers)
    
    def fetch_customers(self, page: int = 1) -> Dict[str, Any]:
        """جلب قائمة العملاء - نفس طريقة الفواتير"""
        url = f"{self.base_url}/entity/client/list"  # تغيير هنا فقط
        params = {
            'page': page,
            'limit': PAGE_LIMIT
        }
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    return response.json()
                else:
                    logger.error(f"❌ خطأ في جلب العملاء: {response.status_code}")
                    
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ خطأ في الاتصال مع دفترة (محاولة {attempt + 1}): {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY)
                    
        return {}

def process_customers(daftra_client: DaftraClient, supabase_client: SupabaseClient) -> Dict[str, int]:
    """معالجة العملاء - نفس طريقة الفواتير"""
    logger.info("👥 بدء معالجة العملاء")
    
    stats = {
        'customers_processed': 0,
        'customers_saved': 0,
        'customers_failed': 0
    }
    
    page = 1
    customers_batch = []
    
    while True:
        logger.info(f"📄 جلب الصفحة {page} للعملاء...")
        
        response_data = daftra_client.fetch_customers(page)
        
        if not response_data or 'data' not in response_data:
            logger.warning(f"⚠️ لا توجد بيانات في الصفحة {page}")
            break
            
        customers = response_data['data']
        
        if not customers:
            logger.info(f"✅ انتهاء العملاء في الصفحة {page}")
            break
        
        valid_customers = 0
        
        for customer in customers:
            if not DataValidator.validate_customer(customer):
                continue
            
            try:
                cleaned_customer = DataValidator.clean_customer_data(customer)
                customers_batch.append(cleaned_customer)
                valid_customers += 1
                        
            except Exception as e:
                logger.error(f"❌ خطأ في معالجة العميل {customer.get('id', 'غير معروف')}: {e}")
                continue
        
        logger.info(f"📋 صفحة {page}: {valid_customers} عميل صالح من أصل {len(customers)}")
        stats['customers_processed'] += valid_customers
        
        # حفظ العملاء عند الوصول للحد الأقصى
        if len(customers_batch) >= BATCH_SIZE:
            saved, failed = supabase_client.upsert_batch('customers', customers_batch)
            stats['customers_saved'] += saved
            stats['customers_failed'] += failed
            customers_batch = []
        
        page += 1
    
    # حفظ العملاء المتبقين
    if customers_batch:
        saved, failed = supabase_client.upsert_batch('customers', customers_batch)
        stats['customers_saved'] += saved
        stats['customers_failed'] += failed
    
    logger.info(f"📊 إحصائيات العملاء: {stats['customers_processed']} عميل")
    return stats

def main():
    """الدالة الرئيسية - نفس طريقة الفواتير"""
    logger.info("🚀 بدء عملية جلب العملاء من دفترة...")
    
    # التحقق من المتغيرات المطلوبة
    if not all([DAFTRA_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
        logger.error("❌ متغيرات البيئة مفقودة!")
        return {'customers_saved': 0, 'customers_processed': 0, 'customers_failed': 0}
    
    # إنشاء العملاء
    daftra_client = DaftraClient()
    supabase_client = SupabaseClient()
    
    # معالجة العملاء
    try:
        stats = process_customers(daftra_client, supabase_client)
        
        # التقرير النهائي
        logger.info("📊 إحصائيات المعالجة النهائية:")
        logger.info(f"   - العملاء المعالجين: {stats['customers_processed']}")
        logger.info(f"   - العملاء المحفوظين: {stats['customers_saved']}")
        logger.info(f"   - أخطاء العملاء: {stats['customers_failed']}")
        
        if stats['customers_processed'] == 0:
            logger.warning("⚠️ لا توجد عملاء للمعالجة")
        
        logger.info("🎉 انتهاء العملية - التقرير النهائي:")
        logger.info(f"   👥 العملاء: {stats['customers_saved']} نجح، {stats['customers_failed']} فشل")
        
        return stats
        
    except Exception as e:
        logger.error(f"❌ خطأ في معالجة العملاء: {e}")
        return {'customers_saved': 0, 'customers_processed': 0, 'customers_failed': 0}

# إضافة alias للتوافق مع main.py
fetch_all = main

if __name__ == "__main__":
    main()
