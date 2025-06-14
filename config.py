# ملف الإعدادات المحدث - config.py
# تم تحديثه ليتوافق مع الكود المحسن لمزامنة دفترة مع Supabase

import os

# =============================================================================
# إعدادات API دفترة
# =============================================================================

# رابط API دفترة الأساسي
# يجب أن يكون بالصيغة: https://shadowpeace.daftara.com/api2
BASE_URL = os.getenv("DAFTRA_URL", "https://shadowpeace.daftara.com/api2")

# مفتاح API الخاص بدفترة
# يمكن الحصول عليه من إعدادات الحساب في دفترة
DAFTRA_API_KEY = os.getenv("DAFTRA_APIKEY")

# التحقق من وجود مفتاح API دفترة
if not DAFTRA_API_KEY:
    raise ValueError("❌ مفتاح API دفترة مطلوب. يرجى تعيين متغير البيئة DAFTRA_APIKEY")

# =============================================================================
# إعدادات Supabase
# =============================================================================

# رابط مشروع Supabase
# يجب أن يكون بالصيغة: https://your-project.supabase.co
SUPABASE_BASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/")
SUPABASE_URL = f"{SUPABASE_BASE_URL}/rest/v1" if SUPABASE_BASE_URL else ""

# مفتاح API الخاص بـ Supabase (service_role key للعمليات الخلفية)
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# التحقق من وجود إعدادات Supabase
if not SUPABASE_BASE_URL:
    raise ValueError("❌ رابط Supabase مطلوب. يرجى تعيين متغير البيئة SUPABASE_URL")

if not SUPABASE_KEY:
    raise ValueError("❌ مفتاح API Supabase مطلوب. يرجى تعيين متغير البيئة SUPABASE_KEY")

# =============================================================================
# إعدادات Headers للطلبات
# =============================================================================

# Headers لطلبات API دفترة
# يجب استخدام 'apikey' وليس 'APIKEY' أو 'Authorization'
HEADERS_DAFTRA = {
    "apikey": DAFTRA_API_KEY,
    "Content-Type": "application/json"
}

# Headers لطلبات Supabase
# تم تحسينها للعمل مع الكود المحدث
HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"  # تم تغييرها من resolution=merge-duplicates
}

# =============================================================================
# إعدادات البيانات والتصفية
# =============================================================================

# نوع الفاتورة المطلوب جلبها
# 0 = عروض الأسعار، 1 = فواتير المبيعات، 2 = فواتير الشراء
# تم تصحيحها من 0 إلى 1 لجلب فواتير المبيعات
EXPECTED_TYPE = 1

# عدد السجلات في كل صفحة
# تم تحسينه لتوازن بين السرعة واستهلاك الذاكرة
PAGE_LIMIT = 50  # تم تقليله من 100 لتحسين الاستقرار

# معرفات الفروع المراد جلب البيانات منها
# حسب المعرفة المتوفرة: الفرع 2 = العويضة، الفرع 3 = الرئيسي
# تم تحديثها لتتوافق مع المعرفة الصحيحة
BRANCH_IDS = [2, 3]  # تم إزالة الفرع 1 وإبقاء الفروع الصحيحة

# =============================================================================
# إعدادات إضافية للكود المحسن
# =============================================================================

# حجم الدفعة لإرسال البيانات إلى Supabase
# يحدد عدد السجلات التي يتم إرسالها في طلب واحد
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

# عدد محاولات إعادة الطلب في حالة الفشل
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))

# وقت التأخير بين المحاولات (بالثواني)
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "2"))

# مهلة انتظار الطلبات (بالثواني)
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# تفعيل/إلغاء تفعيل حفظ السجلات الفاشلة
SAVE_FAILED_RECORDS = os.getenv("SAVE_FAILED_RECORDS", "true").lower() == "true"

# مستوى التسجيل (DEBUG, INFO, WARNING, ERROR)
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# =============================================================================
# إعدادات قاعدة البيانات
# =============================================================================

# أسماء الجداول في Supabase
TABLE_INVOICES = "invoices"
TABLE_INVOICE_ITEMS = "invoice_items"

# =============================================================================
# التحقق من صحة الإعدادات
# =============================================================================

def validate_config():
    """التحقق من صحة جميع الإعدادات المطلوبة"""
    errors = []
    
    # التحقق من رابط دفترة
    if not BASE_URL or not BASE_URL.startswith("https://"):
        errors.append("رابط دفترة غير صحيح")
    
    # التحقق من رابط Supabase
    if not SUPABASE_URL or not SUPABASE_URL.startswith("https://"):
        errors.append("رابط Supabase غير صحيح")
    
    # التحقق من معرفات الفروع
    if not BRANCH_IDS or not isinstance(BRANCH_IDS, list):
        errors.append("معرفات الفروع غير صحيحة")
    
    # التحقق من نوع الفاتورة
    if EXPECTED_TYPE not in [0]:
        errors.append("نوع الفاتورة غير صحيح")
    
    if errors:
        raise ValueError(f"❌ أخطاء في الإعدادات: {', '.join(errors)}")
    
    return True

# تشغيل التحقق عند استيراد الملف
validate_config()

# =============================================================================
# معلومات إضافية للمطورين
# =============================================================================

# معلومات النسخة والتحديث
CONFIG_VERSION = "2.0"
LAST_UPDATED = "2024-06-14"

# رسالة تأكيد تحميل الإعدادات
print(f"✅ تم تحميل إعدادات config v{CONFIG_VERSION} بنجاح")
print(f"🔗 دفترة: {BASE_URL}")
print(f"🔗 Supabase: {SUPABASE_URL}")
print(f"📊 الفروع: {BRANCH_IDS}")
print(f"📋 نوع الفاتورة: {EXPECTED_TYPE}")

