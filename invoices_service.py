import requests
import time
import asyncio
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple

# إعداد نظام السجلات
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('sync_log.txt', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# إعدادات الاتصالات
DAFTRA_URL = "https://shadowpeace.daftra.com"
DAFTRA_HEADERS = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
SUPABASE_URL = "https://wuqbovrurauffztbkbse.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind1cWJvdnJ1cmF1ZmZ6dGJrYnNlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzg3MTA0NywiZXhwIjoyMDYzNDQ3MDQ3fQ.6ekq6VV2gcyw4uOHfscO9vIzUBSGDk_yweiGOGSPyFo"

SUPABASE_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json"
}

class SyncStats:
    """كلاس إحصائيات المزامنة"""
    def __init__(self):
        self.start_time = time.time()
        self.total_invoices_processed = 0
        self.total_invoices_synced = 0
        self.total_items_synced = 0
        self.skipped_invoices = 0
        self.errors = []
        self.warnings = []
        self.success_details = []
        
    def add_success(self, invoice_no: str, items_count: int = 0):
        self.total_invoices_synced += 1
        self.total_items_synced += items_count
        self.success_details.append(f"✅ {invoice_no} ({items_count} عنصر)")
        
    def add_skip(self, invoice_no: str, reason: str):
        self.skipped_invoices += 1
        self.warnings.append(f"⏭️ {invoice_no}: {reason}")
        
    def add_error(self, error_msg: str):
        self.errors.append(f"❌ {error_msg}")
        logger.error(error_msg)
        
    def add_warning(self, warning_msg: str):
        self.warnings.append(f"⚠️ {warning_msg}")
        logger.warning(warning_msg)
        
    def get_summary(self) -> Dict:
        duration = time.time() - self.start_time
        total_processed = max(self.total_invoices_processed, 1)
        
        return {
            "success": True,
            "duration_seconds": round(duration, 2),
            "duration_formatted": f"{duration//60:.0f}د {duration%60:.0f}ث",
            "total_processed": self.total_invoices_processed,
            "total_synced": self.total_invoices_synced,
            "total_items": self.total_items_synced,
            "skipped": self.skipped_invoices,
            "success_rate": f"{(self.total_invoices_synced/total_processed*100):.1f}%",
            "errors_count": len(self.errors),
            "warnings_count": len(self.warnings),
            "avg_items_per_invoice": round(self.total_items_synced / max(self.total_invoices_synced, 1), 1)
        }

def fetch_with_retry(url: str, headers: Dict, max_retries: int = 3, timeout: int = 30) -> Optional[Dict]:
    """جلب البيانات مع إعادة المحاولة ومعالجة أفضل للأخطاء"""
    for retry in range(max_retries):
        try:
            logger.info(f"📡 جلب البيانات من: {url} (محاولة {retry + 1})")
            response = requests.get(url, headers=headers, timeout=timeout)
            
            if response.status_code == 200:
                logger.info("✅ تم جلب البيانات بنجاح")
                return response.json()
                
            elif response.status_code == 429:  # Rate limit
                wait_time = (retry + 1) * 10
                logger.warning(f"⚠️ Rate limit - انتظار {wait_time} ثانية")
                time.sleep(wait_time)
                
            elif response.status_code == 401:
                logger.error("❌ خطأ في المصادقة - تحقق من API key")
                break
                
            elif response.status_code == 404:
                logger.error(f"❌ الرابط غير موجود: {url}")
                break
                
            else:
                logger.warning(f"⚠️ HTTP {response.status_code} - محاولة {retry + 1}")
                if retry < max_retries - 1:
                    time.sleep((retry + 1) * 5)
                    
        except requests.exceptions.Timeout:
            logger.warning(f"⏱️ انتهت مهلة الانتظار - محاولة {retry + 1}")
        except requests.exceptions.ConnectionError:
            logger.warning(f"🔌 خطأ في الاتصال - محاولة {retry + 1}")
        except Exception as e:
            logger.error(f"❌ خطأ غير متوقع: {str(e)}")
            
        if retry < max_retries - 1:
            sleep_time = (retry + 1) * 5
            logger.info(f"😴 انتظار {sleep_time} ثانية قبل إعادة المحاولة...")
            time.sleep(sleep_time)
    
    logger.error(f"❌ فشل جلب البيانات من {url} بعد {max_retries} محاولات")
    return None

def test_connections() -> bool:
    """فحص الاتصال بـ Daftra و Supabase"""
    print("🔍 فحص الاتصالات...")
    logger.info("بدء فحص الاتصالات")
    
    # فحص Daftra
    logger.info("فحص الاتصال بدفترة...")
    daftra_test = fetch_with_retry(
        f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page=1&limit=1",
        DAFTRA_HEADERS,
        max_retries=2,
        timeout=15
    )
    
    if daftra_test is not None:
        print("✅ الاتصال بدفترة يعمل بشكل صحيح")
        logger.info("✅ اتصال دفترة ناجح")
    else:
        print("❌ فشل الاتصال بدفترة")
        logger.error("❌ فشل اتصال دفترة")
        return False
    
    # فحص Supabase
    logger.info("فحص الاتصال بـ Supabase...")
    try:
        supabase_response = requests.get(
            f"{SUPABASE_URL}/rest/v1/invoices?limit=1",
            headers=SUPABASE_HEADERS,
            timeout=15
        )
        
        if supabase_response.status_code == 200:
            print("✅ الاتصال بـ Supabase يعمل بشكل صحيح")
            logger.info("✅ اتصال Supabase ناجح")
        else:
            print(f"❌ فشل الاتصال بـ Supabase: {supabase_response.status_code}")
            logger.error(f"❌ فشل اتصال Supabase: {supabase_response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ خطأ في الاتصال بـ Supabase: {str(e)}")
        logger.error(f"❌ خطأ اتصال Supabase: {str(e)}")
        return False
    
    logger.info("✅ جميع الاتصالات تعمل بشكل صحيح")
    return True

def check_invoice_exists(invoice_id: str) -> bool:
    """فحص وجود الفاتورة في Supabase"""
    try:
        url = f"{SUPABASE_URL}/rest/v1/invoices?id=eq.{invoice_id}&select=id"
        response = requests.get(url, headers=SUPABASE_HEADERS, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            return len(data) > 0
    except Exception as e:
        logger.warning(f"خطأ في فحص وجود الفاتورة {invoice_id}: {str(e)}")
    
    return False

def save_invoice_to_supabase(invoice_data: Dict) -> Tuple[bool, str]:
    """حفظ فاتورة واحدة في Supabase"""
    try:
        response = requests.post(
            f"{SUPABASE_URL}/rest/v1/invoices",
            headers=SUPABASE_HEADERS,
            json=invoice_data,
            timeout=15
        )
        
        if response.status_code == 201:
            return True, "تم الحفظ بنجاح"
        else:
            error_text = response.text[:200] if response.text else "خطأ غير معروف"
            return False, f"HTTP {response.status_code}: {error_text}"
            
    except Exception as e:
        return False, f"خطأ في الحفظ: {str(e)}"

def save_invoice_items(items_data: List[Dict]) -> int:
    """حفظ عناصر الفواتير في Supabase"""
    if not items_data:
        return 0
        
    success_count = 0
    batch_size = 10
    
    # حفظ العناصر في دفعات
    for i in range(0, len(items_data), batch_size):
        batch = items_data[i:i + batch_size]
        
        try:
            response = requests.post(
                f"{SUPABASE_URL}/rest/v1/invoice_items",
                headers=SUPABASE_HEADERS,
                json=batch,
                timeout=15
            )
            
            if response.status_code == 201:
                success_count += len(batch)
            else:
                logger.warning(f"فشل حفظ دفعة عناصر: {response.status_code}")
                
        except Exception as e:
            logger.error(f"خطأ في حفظ عناصر الفواتير: {str(e)}")
    
    return success_count

def validate_and_prepare_invoice(invoice: Dict, details: Dict) -> Optional[Dict]:
    """التحقق من صحة بيانات الفاتورة وتحضيرها"""
    try:
        inv_id = invoice.get("id")
        if not inv_id:
            return None
            
        # التحقق من نوع الفاتورة (فواتير المبيعات = 0)
        inv_type = invoice.get("type")
        try:
            inv_type = int(inv_type)
            if inv_type != 0:
                return None
        except (ValueError, TypeError):
            return None
        
        # تحضير بيانات الفاتورة
        invoice_data = {
            "id": str(inv_id),
            "created_at": str(invoice.get("date") or datetime.now().isoformat()),
            "invoice_type": "0",
            "branch": str(invoice.get("branch_id", 1)),
            "store": str(invoice.get("store_id") or ""),
            "total": str(details.get("summary_total", 0)),
            "customer_id": str(invoice.get("customer_id") or ""),
            "invoice_no": str(invoice.get("no", f"INV-{inv_id}"))
        }
        
        return invoice_data
        
    except Exception as e:
        logger.error(f"خطأ في التحقق من بيانات الفاتورة: {str(e)}")
        return None

def prepare_invoice_items(invoice_id: str, details: Dict) -> List[Dict]:
    """تحضير عناصر الفاتورة"""
    items_data = []
    
    try:
        items = details.get("invoice_item", [])
        if not isinstance(items, list):
            items = [items] if items else []
        
        for item in items:
            product_id = item.get("product_id")
            quantity = item.get("quantity", 0)
            unit_price = item.get("unit_price", 0)
            
            # التحقق من صحة البيانات
            if product_id and float(quantity or 0) > 0:
                item_data = {
                    "invoice_id": str(invoice_id),
                    "product_id": str(product_id),
                    "quantity": str(quantity),
                    "unit_price": str(unit_price)
                }
                items_data.append(item_data)
                
    except Exception as e:
        logger.error(f"خطأ في تحضير عناصر الفاتورة {invoice_id}: {str(e)}")
    
    return items_data

async def process_single_invoice(invoice: Dict, stats: SyncStats, check_existing: bool = False) -> Optional[Dict]:
    """معالجة فاتورة واحدة"""
    inv_id = invoice.get("id")
    inv_no = invoice.get("no", f"INV-{inv_id}")
    
    try:
        logger.info(f"🔄 معالجة فاتورة {inv_no}")
        
        # فحص الوجود إذا كان مطلوباً
        if check_existing and check_invoice_exists(str(inv_id)):
            stats.add_skip(inv_no, "موجودة مسبقاً")
            return None
        
        # جلب تفاصيل الفاتورة
        details_url = f"{DAFTRA_URL}/v2/api/entity/invoice/{inv_id}"
        details = fetch_with_retry(details_url, DAFTRA_HEADERS)
        
        if not details:
            stats.add_error(f"فشل جلب تفاصيل فاتورة {inv_no}")
            return None
        
        # التحقق وتحضير البيانات
        invoice_data = validate_and_prepare_invoice(invoice, details)
        if not invoice_data:
            stats.add_skip(inv_no, "بيانات غير صالحة أو ليست فاتورة مبيعات")
            return None
        
        # حفظ الفاتورة
        success, message = save_invoice_to_supabase(invoice_data)
        if not success:
            stats.add_error(f"فشل حفظ فاتورة {inv_no}: {message}")
            return None
        
        # تحضير وحفظ العناصر
        items_data = prepare_invoice_items(inv_id, details)
        items_saved = save_invoice_items(items_data) if items_data else 0
        
        # إضافة إلى الإحصائيات
        stats.add_success(inv_no, items_saved)
        logger.info(f"✅ تم حفظ فاتورة {inv_no} مع {items_saved} عنصر")
        
        return {
            "invoice_no": inv_no,
            "items_count": items_saved,
            "total": invoice_data.get("total", 0)
        }
        
    except Exception as e:
        stats.add_error(f"خطأ في معالجة فاتورة {inv_no}: {str(e)}")
        return None

async def sync_invoices(max_pages: int = 3, limit: int = 5, check_existing: bool = False, max_duration: int = 600) -> Dict:
    """الدالة الرئيسية لمزامنة الفواتير"""
    
    print("🚀 بدء مزامنة الفواتير...")
    logger.info("=" * 50)
    logger.info("بدء عملية مزامنة الفواتير")
    logger.info(f"إعدادات المزامنة: max_pages={max_pages}, limit={limit}, check_existing={check_existing}")
    
    # فحص الاتصالات
    if not test_connections():
        error_msg = "فشل فحص الاتصالات الأولي"
        logger.error(error_msg)
        return {"success": False, "error": error_msg}
    
    stats = SyncStats()
    
    try:
        for page in range(1, max_pages + 1):
            # فحص انتهاء الوقت
            if time.time() - stats.start_time > max_duration:
                stats.add_warning(f"توقف بسبب انتهاء الوقت المحدد ({max_duration} ثانية)")
                break
            
            print(f"\n📄 معالجة الصفحة {page}/{max_pages}...")
            logger.info(f"بدء معالجة الصفحة {page}")
            
            # جلب قائمة الفواتير
            url = f"{DAFTRA_URL}/v2/api/entity/invoice/list/1?page={page}&limit={limit}"
            data = fetch_with_retry(url, DAFTRA_HEADERS)
            
            if not data:
                stats.add_error(f"فشل جلب الصفحة {page}")
                continue
            
            invoice_list = data.get("data", [])
            if not invoice_list:
                print(f"⏹️ الصفحة {page} فارغة - انتهاء المعالجة")
                logger.info(f"الصفحة {page} فارغة")
                break
            
            print(f"📋 وُجد {len(invoice_list)} فاتورة في الصفحة {page}")
            logger.info(f"وُجد {len(invoice_list)} فاتورة في الصفحة {page}")
            
            # معالجة كل فاتورة
            for i, invoice in enumerate(invoice_list, 1):
                try:
                    stats.total_invoices_processed += 1
                    print(f"  🔄 معالجة فاتورة {i}/{len(invoice_list)}: {invoice.get('no', 'غير معروف')}")
                    
                    result = await process_single_invoice(invoice, stats, check_existing)
                    
                    if result:
                        print(f"    ✅ نجحت - {result['items_count']} عنصر")
                    
                    # راحة قصيرة بين الفواتير
                    await asyncio.sleep(0.5)
                    
                except Exception as e:
                    error_msg = f"خطأ في معالجة فاتورة {invoice.get('no', 'غير معروف')}: {str(e)}"
                    stats.add_error(error_msg)
                    print(f"    ❌ فشلت")
            
            # راحة بين الصفحات
            if page < max_pages:
                print("😴 راحة بين الصفحات...")
                await asyncio.sleep(2)
            
    except Exception as e:
        error_msg = f"خطأ عام في المزامنة: {str(e)}"
        stats.add_error(error_msg)
        logger.error(error_msg)
    
    # النتائج النهائية
    summary = stats.get_summary()
    
    print("\n" + "="*50)
    print("📊 نتائج المزامنة:")
    print(f"✅ تم معالجة: {summary['total_processed']} فاتورة")
    print(f"💾 تم حفظ: {summary['total_synced']} فاتورة")
    print(f"📦 عناصر: {summary['total_items']} عنصر")
    print(f"⏭️ تم تخطي: {summary['skipped']} فاتورة")
    print(f"⏱️ المدة: {summary['duration_formatted']}")
    print(f"📈 معدل النجاح: {summary['success_rate']}")
    print(f"📊 متوسط العناصر لكل فاتورة: {summary['avg_items_per_invoice']}")
    
    if stats.errors:
        print(f"\n❌ الأخطاء ({len(stats.errors)}):")
        for error in stats.errors[-5:]:  # آخر 5 أخطاء
            print(f"  {error}")
    
    if stats.warnings:
        print(f"\n⚠️ التحذيرات ({len(stats.warnings)}):")
        for warning in stats.warnings[-3:]:  # آخر 3 تحذيرات
            print(f"  {warning}")
    
    logger.info("انتهاء عملية المزامنة")
    logger.info(f"النتائج: {summary}")
    logger.info("=" * 50)
    
    return {
        "success": True,
        "summary": summary,
        "recent_errors": stats.errors[-10:],
        "recent_warnings": stats.warnings[-10:],
        "recent_success": stats.success_details[-10:]
    }

# دالة للاستدعاء من الخارج
def run_sync(max_pages: int = 3, limit: int = 5, check_existing: bool = False) -> Dict:
    """تشغيل المزامنة (للاستدعاء من main.py)"""
    try:
        return asyncio.run(sync_invoices(max_pages, limit, check_existing))
    except Exception as e:
        logger.error(f"خطأ في تشغيل المزامنة: {str(e)}")
        return {
            "success": False,
            "error": str(e),
            "summary": {"total_synced": 0, "total_processed": 0}
        }

if __name__ == "__main__":
    # تشغيل مباشر للاختبار
    print("🧪 تشغيل الاختبار المباشر...")
    result = run_sync(max_pages=2, limit=3, check_existing=False)
    
    if result["success"]:
        print("\n🎉 اكتملت المزامنة بنجاح!")
    else:
        print(f"\n❌ فشلت المزامنة: {result.get('error', 'خطأ غير معروف')}")
