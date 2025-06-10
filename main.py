import os
from datetime import datetime, date
import time
import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import logging

# إعداد السجلات
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# استيراد الخدمات
try:
    from invoices_service import run_sync as sync_invoices_sync, test_connections
    logger.info("✅ تم تحميل خدمة الفواتير")
except ImportError as e:
    logger.error(f"❌ خطأ في استيراد invoices_service: {e}")

try:
    from products_service import sync_products
    logger.info("✅ تم تحميل خدمة المنتجات")
except ImportError as e:
    logger.warning(f"⚠️ لم يتم العثور على products_service: {e}")

# إنشاء التطبيق
app = FastAPI(
    title="🏪 Daftra Sync API",
    description="نظام مزامنة البيانات من دفترة إلى Supabase على Railway",
    version="2.1.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# إضافة CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# متغيرات النظام
app_start_time = time.time()
last_sync_results = {}

# نماذج البيانات
class SyncRequest(BaseModel):
    max_pages: int = Field(default=3, ge=1, le=20, description="عدد الصفحات")
    limit: int = Field(default=5, ge=1, le=50, description="عدد العناصر لكل صفحة")
    check_existing: bool = Field(default=False, description="فحص العناصر الموجودة")

@app.on_event("startup")
async def startup_event():
    """بدء التشغيل"""
    logger.info("🚂 تشغيل Daftra Sync على Railway")
    logger.info("🗄️ قاعدة البيانات: Supabase")
    
    # فحص الاتصالات
    try:
        connections_ok = test_connections()
        if connections_ok:
            logger.info("✅ جميع الاتصالات تعمل")
        else:
            logger.warning("⚠️ مشاكل في الاتصالات")
    except Exception as e:
        logger.error(f"❌ خطأ في فحص الاتصالات: {e}")

@app.get("/")
async def home():
    """الصفحة الرئيسية"""
    uptime = time.time() - app_start_time
    uptime_str = f"{uptime//3600:.0f}س {(uptime%3600)//60:.0f}د {uptime%60:.0f}ث"
    
    return {
        "🏪": "Daftra Sync API",
        "📍": "Railway Platform", 
        "🗄️": "Supabase Database",
        "⏱️": uptime_str,
        "🕐": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "🚀": "نظام مزامنة دفترة",
        "endpoints": {
            "🔄": {
                "/sync-invoices": "مزامنة فواتير المبيعات",
                "/sync-invoices-quick": "مزامنة سريعة",
                "/sync-invoices-test": "اختبار المزامنة",
                "/sync-products": "مزامنة المنتجات"
            },
            "📊": {
                "/health": "صحة النظام",
                "/status": "حالة قاعدة البيانات", 
                "/logs": "آخر العمليات"
            },
            "📚": {
                "/docs": "وثائق Swagger",
                "/redoc": "وثائق ReDoc"
            }
        }
    }

@app.get("/health")
async def health_check():
    """فحص صحة النظام"""
    try:
        connections_ok = test_connections()
        uptime = time.time() - app_start_time
        
        return {
            "status": "🟢 صحي" if connections_ok else "🟡 مشاكل جزئية",
            "platform": "🚂 Railway",
            "database": "📊 Supabase", 
            "uptime": f"{uptime:.0f} ثانية",
            "connections": {
                "daftra_api": "✅" if connections_ok else "❌",
                "supabase_db": "✅" if connections_ok else "❌"
            },
            "timestamp": datetime.now().isoformat(),
            "environment": os.environ.get("RAILWAY_ENVIRONMENT", "production")
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"خطأ صحة النظام: {str(e)}")

@app.get("/sync-invoices")
async def sync_invoices():
    """مزامنة الفواتير - الإعداد الافتراضي"""
    try:
        logger.info("🔄 بدء مزامنة الفواتير")
        
        start_time = time.time()
        result = sync_invoices_sync(max_pages=3, limit=5, check_existing=False)
        duration = time.time() - start_time
        
        if result.get("success", False):
            summary = result.get("summary", {})
            
            response_data = {
                "success": True,
                "message": f"✅ تم سحب {summary.get('total_synced', 0)} فاتورة بنجاح",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "service": "📋 invoices",
                "platform": "🚂 Railway → 📊 Supabase",
                "duration": summary.get("duration_formatted", f"{duration:.1f}ث"),
                "statistics": {
                    "معالج": summary.get("total_processed", 0),
                    "محفوظ": summary.get("total_synced", 0),
                    "عناصر": summary.get("total_items", 0),
                    "معدل_النجاح": summary.get("success_rate", "0%"),
                    "أخطاء": summary.get("errors_count", 0),
                    "تحذيرات": summary.get("warnings_count", 0)
                },
                "details": {
                    "avg_items_per_invoice": summary.get("avg_items_per_invoice", 0),
                    "recent_errors": result.get("recent_errors", [])[-2:] if result.get("recent_errors") else []
                }
            }
            
            # حفظ النتائج
            last_sync_results.update(response_data)
            logger.info(f"✅ نجحت المزامنة: {summary.get('total_synced', 0)} فاتورة")
            
            return response_data
            
        else:
            error_msg = result.get("error", "خطأ غير معروف")
            logger.error(f"❌ فشلت المزامنة: {error_msg}")
            
            error_response = {
                "success": False,
                "message": f"❌ فشلت المزامنة: {error_msg}",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "service": "📋 invoices",
                "platform": "🚂 Railway"
            }
            
            last_sync_results.update(error_response)
            raise HTTPException(status_code=500, detail=error_response)
            
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"خطأ غير متوقع: {str(e)}"
        logger.error(error_msg)
        
        error_response = {
            "success": False,
            "message": f"❌ {error_msg}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        last_sync_results.update(error_response)
        raise HTTPException(status_code=500, detail=error_response)

@app.get("/sync-invoices-quick")
async def sync_invoices_quick():
    """مزامنة سريعة - للاختبار"""
    try:
        logger.info("⚡ مزامنة سريعة")
        
        result = sync_invoices_sync(max_pages=1, limit=2, check_existing=False)
        
        if result.get("success", False):
            summary = result.get("summary", {})
            return {
                "success": True,
                "message": f"⚡ مزامنة سريعة: {summary.get('total_synced', 0)} فاتورة",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "platform": "🚂 Railway ⚡",
                "duration": summary.get("duration_formatted", "غير محدد"),
                "quick_stats": {
                    "processed": summary.get("total_processed", 0),
                    "synced": summary.get("total_synced", 0),
                    "items": summary.get("total_items", 0)
                }
            }
        else:
            return {
                "success": False,
                "message": f"❌ فشل الاختبار: {result.get('error', 'خطأ غير معروف')}",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "platform": "🚂 Railway"
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": f"❌ خطأ في الاختبار: {str(e)}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

@app.get("/sync-invoices-test")
async def test_sync():
    """اختبار شامل للمزامنة"""
    test_results = {
        "🧪": "اختبار المزامنة",
        "⏰": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "🚂": "Railway Platform"
    }
    
    try:
        # 1. فحص الاتصالات
        logger.info("🔍 فحص الاتصالات...")
        connections_ok = test_connections()
        test_results["🔗"] = "✅ الاتصالات سليمة" if connections_ok else "❌ مشاكل في الاتصالات"
        
        if not connections_ok:
            test_results["❌"] = "فشل فحص الاتصالات"
            test_results["💡"] = [
                "تحقق من اتصال الإنترنت",
                "تحقق من Daftra API key", 
                "تحقق من Supabase connection"
            ]
            return test_results
        
        # 2. اختبار مزامنة فاتورة واحدة
        logger.info("🧪 اختبار مزامنة...")
        result = sync_invoices_sync(max_pages=1, limit=1, check_existing=False)
        
        if result.get("success", False):
            summary = result.get("summary", {})
            test_results["✅"] = "نجح الاختبار"
            test_results["📊"] = {
                "معالج": summary.get("total_processed", 0),
                "محفوظ": summary.get("total_synced", 0),
                "مدة": summary.get("duration_formatted", "غير محدد")
            }
        else:
            test_results["❌"] = f"فشل الاختبار: {result.get('error', 'خطأ غير معروف')}"
        
        return test_results
        
    except Exception as e:
        test_results["❌"] = f"خطأ في الاختبار: {str(e)}"
        return test_results

@app.get("/sync-products")
async def sync_products_endpoint():
    """مزامنة المنتجات"""
    try:
        if 'sync_products' not in globals():
            return {
                "success": False,
                "message": "❌ خدمة المنتجات غير متاحة",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "note": "تأكد من وجود ملف products_service.py"
            }
        
        logger.info("🛍️ بدء مزامنة المنتجات")
        result = await sync_products()
        
        return {
            "success": True,
            "message": f"🛍️ تم سحب {result.get('total_synced', 0)} منتج",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "service": "products",
            "platform": "🚂 Railway → 📊 Supabase",
            "details": result
        }
        
    except Exception as e:
        error_msg = f"خطأ في مزامنة المنتجات: {str(e)}"
        logger.error(error_msg)
        return {
            "success": False,
            "message": f"❌ {error_msg}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

@app.get("/status")
async def database_status():
    """حالة قاعدة البيانات والنظام"""
    try:
        connections_ok = test_connections()
        uptime = time.time() - app_start_time
        
        return {
            "🗄️": {
                "provider": "Supabase",
                "project": "wuqbovrurauffztbkbse",
                "status": "🟢 متصل" if connections_ok else "🔴 منقطع",
                "tables": ["invoices", "invoice_items", "products"],
                "url": "https://supabase.com/dashboard/project/wuqbovrurauffztbkbse"
            },
            "🌐": {
                "daftra_api": "🟢 متصل" if connections_ok else "🔴 منقطع",
                "base_url": "https://shadowpeace.daftra.com"
            },
            "🚂": {
                "platform": "Railway",
                "environment": os.environ.get("RAILWAY_ENVIRONMENT", "production"),
                "project": "1336874a-1120-4f18-87c2-b5b9b1e0d439",
                "uptime": f"{uptime//60:.0f} دقيقة"
            },
            "⏰": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في حالة قاعدة البيانات: {str(e)}")

@app.get("/logs")
async def recent_logs():
    """آخر العمليات"""
    uptime_minutes = (time.time() - app_start_time) // 60
    
    return {
        "📋": "آخر العمليات",
        "🕐": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "⏱️": f"{uptime_minutes:.0f} دقيقة",
        "🚂": "Railway + Supabase",
        "📊": last_sync_results if last_sync_results else {
            "message": "لم يتم تنفيذ أي مزامنة بعد",
            "suggestion": "جرب /sync-invoices-test للاختبار"
        }
    }

# للتشغيل المحلي والـ Railway
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    
    print("🚂 تشغيل على Railway...")
    print(f"📍 البورت: {port}")
    print("🗄️ قاعدة البيانات: Supabase")
    print("🌐 الروابط:")
    print(f"   • الرئيسية: http://localhost:{port}/")
    print(f"   • الوثائق: http://localhost:{port}/docs")
    print(f"   • الصحة: http://localhost:{port}/health")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
