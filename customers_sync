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
        logging.FileHandler('customers_sync.log')
    ]
)
logger = logging.getLogger(__name__)

# إعداد المتغيرات
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
    "Prefer": "resolution=merge-duplicates"
}

BATCH_SIZE = 50
PAGE_LIMIT = 20

class SupabaseClient:
    def __init__(self, base_url: str, headers: Dict[str, str]):
        self.base_url = base_url
        self.headers = headers
        self.session = requests.Session()
        self.session.headers.update(headers)

    def upsert_batch(self, table: str, data: List[Dict[str, Any]]) -> tuple[int, int]:
        """حفظ دفعة من البيانات مع معالجة التكرار"""
        if not data:
            return 0, 0

        url = f"{self.base_url}/{table}?on_conflict=id"
        
        try:
            # محاولة أولى: merge-duplicates
            headers_with_upsert = {**self.headers, "Prefer": "resolution=merge-duplicates"}
            response = self.session.post(url, json=data, headers=headers_with_upsert, timeout=30)
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ تم حفظ/تحديث {len(data)} سجل في جدول {table}")
                return len(data), 0
            elif response.status_code == 409:
                # محاولة ثانية: ignore-duplicates
                headers_with_ignore = {**self.headers, "Prefer": "resolution=ignore-duplicates"}
                response = self.session.post(url, json=data, headers=headers_with_ignore, timeout=30)
                
                if response.status_code in [200, 201]:
                    logger.info(f"✅ تم تجاهل التكرار وحفظ البيانات الجديدة في جدول {table}")
                    return len(data), 0
                else:
                    logger.error(f"❌ خطأ في حفظ {table}: {response.status_code} - {response.text}")
                    return 0, len(data)
            else:
                logger.error(f"❌ خطأ في حفظ {table}: {response.status_code} - {response.text}")
                return 0, len(data)
                
        except Exception as e:
            logger.error(f"❌ خطأ في الاتصال بـ Supabase: {str(e)}")
            return 0, len(data)

class DaftraCustomersSync:
    def __init__(self):
        self.base_url = BASE_URL
        self.headers = HEADERS_DAFTRA
        self.supabase_client = SupabaseClient(SUPABASE_URL, HEADERS_SUPABASE)
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def fetch_customers_page(self, page: int = 1) -> Optional[Dict[str, Any]]:
        """جلب صفحة من العملاء من دفترة"""
        url = f"{self.base_url}/entity/client/list/1"
        params = {
            'page': page,
            'limit': PAGE_LIMIT
        }
        
        logger.info(f"📄 جلب الصفحة {page} من العملاء...")
        
        for attempt in range(3):
            try:
                response = self.session.get(url, params=params, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    if data and 'data' in data:
                        customers = data['data']
                        logger.info(f"📋 صفحة {page}: {len(customers)} عميل")
                        return data
                    else:
                        logger.warning(f"⚠️ لا توجد بيانات في الصفحة {page}")
                        return None
                else:
                    logger.error(f"❌ خطأ في جلب العملاء: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"❌ خطأ في الاتصال (محاولة {attempt + 1}): {str(e)}")
                if attempt < 2:
                    time.sleep(2)
                    
        return None

    @staticmethod
    def clean_customer_data(customer: Dict[str, Any]) -> Dict[str, Any]:
        """تنظيف وتحويل بيانات العميل للصيغة المطلوبة"""
        cleaned = {
            'id': str(customer.get('id', '')),
            'customer_code': str(customer.get('code', '')),
            'name': str(customer.get('name', ''))[:255],
            'phone': str(customer.get('phone', ''))[:50],
            'email': str(customer.get('email', ''))[:255],
            'gender': str(customer.get('gender', ''))[:10],
            'birth_date': customer.get('birth_date'),
            'city': str(customer.get('city', ''))[:100],
            'region': str(customer.get('region', ''))[:100],
            'address': str(customer.get('address', ''))[:500],
            'total_spent': float(customer.get('total_spent', 0)),
            'total_invoices': int(customer.get('total_invoices', 0)),
            'max_order_value': float(customer.get('max_order_value', 0)),
            'average_order_value': float(customer.get('average_order_value', 0)),
            'payment_total': float(customer.get('payment_total', 0)),
            'last_order_date': customer.get('last_order_date'),
            'order_frequency_days': int(customer.get('order_frequency_days', 0)),
            'is_active': bool(customer.get('is_active', True)),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        # تنظيف التواريخ
        if cleaned['birth_date'] and cleaned['birth_date'] != '0000-00-00':
            try:
                # التحقق من صحة التاريخ
                datetime.strptime(cleaned['birth_date'], '%Y-%m-%d')
            except:
                cleaned['birth_date'] = None
        else:
            cleaned['birth_date'] = None
            
        if cleaned['last_order_date'] and cleaned['last_order_date'] != '0000-00-00':
            try:
                # التحقق من صحة التاريخ
                datetime.strptime(cleaned['last_order_date'], '%Y-%m-%d')
            except:
                cleaned['last_order_date'] = None
        else:
            cleaned['last_order_date'] = None
        
        return cleaned

    def sync_customers(self):
        """مزامنة جميع العملاء من دفترة إلى Supabase"""
        logger.info("🚀 بدء عملية مزامنة العملاء...")
        
        page = 1
        customers_batch = []
        total_stats = {
            'customers_processed': 0,
            'customers_saved': 0,
            'customers_failed': 0
        }
        
        while True:
            # جلب صفحة من العملاء
            response_data = self.fetch_customers_page(page)
            
            if not response_data or 'data' not in response_data:
                logger.info(f"✅ انتهاء العملاء في الصفحة {page}")
                break
                
            customers = response_data['data']
            
            if not customers:
                logger.info(f"✅ لا توجد عملاء في الصفحة {page}")
                break
            
            # معالجة العملاء
            for customer in customers:
                try:
                    cleaned_customer = self.clean_customer_data(customer)
                    customers_batch.append(cleaned_customer)
                    total_stats['customers_processed'] += 1
                    
                except Exception as e:
                    logger.error(f"❌ خطأ في معالجة العميل {customer.get('id', 'unknown')}: {str(e)}")
                    total_stats['customers_failed'] += 1
            
            # حفظ الدفعة عند الوصول للحد الأقصى
            if len(customers_batch) >= BATCH_SIZE:
                saved, failed = self.supabase_client.upsert_batch('customers', customers_batch)
                total_stats['customers_saved'] += saved
                total_stats['customers_failed'] += failed
                customers_batch = []
                
                # انتظار قصير لتجنب الضغط على الخادم
                time.sleep(1)
            
            page += 1
            
            # حد أقصى للصفحات (حماية)
            if page > 1000:
                logger.warning(f"⚠️ تم الوصول للحد الأقصى من الصفحات")
                break
        
        # حفظ العملاء المتبقين
        if customers_batch:
            saved, failed = self.supabase_client.upsert_batch('customers', customers_batch)
            total_stats['customers_saved'] += saved
            total_stats['customers_failed'] += failed
        
        # طباعة الإحصائيات النهائية
        logger.info("📊 إحصائيات المعالجة النهائية:")
        logger.info(f"   - العملاء المعالجين: {total_stats['customers_processed']}")
        logger.info(f"   - العملاء المحفوظين: {total_stats['customers_saved']}")
        logger.info(f"   - أخطاء العملاء: {total_stats['customers_failed']}")
        
        if total_stats['customers_processed'] == 0:
            logger.warning("⚠️ لا توجد عملاء للمعالجة")
        
        logger.info("🎉 انتهاء عملية مزامنة العملاء - التقرير النهائي:")
        logger.info(f"   👥 العملاء: {total_stats['customers_saved']} نجح، {total_stats['customers_failed']} فشل")
        
        return total_stats

def main():
    """الدالة الرئيسية"""
    try:
        print("🔄 مزامنة العملاء...")
        print(f"SUPABASE={SUPABASE_URL}")
        
        # التحقق من المتغيرات المطلوبة
        if not all([DAFTRA_API_KEY, SUPABASE_URL, SUPABASE_KEY]):
            logger.error("❌ متغيرات البيئة مفقودة!")
            return
        
        # إنشاء مثيل المزامنة وتشغيلها
        sync = DaftraCustomersSync()
        stats = sync.sync_customers()
        
        print(f"✅ تم الانتهاء! العملاء المحفوظين: {stats['customers_saved']}")
        
    except Exception as e:
        logger.error(f"❌ خطأ عام: {str(e)}")
        print(f"❌ خطأ عام: {str(e)}")

if __name__ == "__main__":
    main()

# إضافة alias للتوافق مع main.py
fetch_all = main

