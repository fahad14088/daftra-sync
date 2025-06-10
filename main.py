import os
from datetime import datetime, date
import time
import asyncio
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastui import FastUI, AnyComponent, prebuilt_html, components as c
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
import logging

# إعداد السجلات
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# استيراد الخدمات
try:
    from invoices_service import run_sync as sync_invoices_sync, test_connections
    logger.info("✅ تم تحميل خدمة الفواتير بنجاح")
except ImportError as e:
    logger.error(f"❌ خطأ في استيراد خدمة الفواتير: {e}")

# إنشاء التطبيق
app = FastAPI(
    title="Daftra Sync API",
    description="نظام مزامنة البيانات من دفترة إلى Supabase",
    version="2.0.0",
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

class SyncResponse(BaseModel):
    success: bool
    message: str
    timestamp: str
    service: str
    duration: str
    total_processed: int
    total_synced: int
    errors_count: int
    warnings_count: int
    details: Optional[Dict[str, Any]] = None

@app.on_event("startup")
async def startup_event():
    """أحداث بدء التشغيل"""
    logger.info("🚀 بدء تشغيل Daftra Sync API على Railway")
    
    # فحص الاتصالات
    try:
        connections_ok = test_connections()
        if connections_ok:
            logger.info("✅ اتصال Supabase يعمل بشكل صحيح")
        else:
            logger.warning("⚠️ مشاكل في الاتصال")
    except Exception as e:
        logger.error(f"❌ خطأ في فحص الاتصالات: {e}")

@app.get("/")
async def home():
    """الصفحة الرئيسية"""
    uptime = time.time() - app_start_time
    uptime_str = f"{uptime//3600:.0f}س {(uptime%3600)//60:.0f}د"
    
    return {
        "service": "🏪 Daftra Sync API",
        "version": "2.0.0",
        "status": "🟢 يعمل على Railway",
        "database": "📊 Supabase متصل",
        "uptime": uptime_str,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "endpoints": {
            "sync": {
                "/sync-invoices": "مزامنة فواتير المبيعات",
                "/sync-invoices-quick": "مزامنة سريعة (3 فواتير)",
                "/sync-invoices-test": "اختبار المزامنة"
            },
            "monitoring": {
                "/health": "فحص صحة النظام",
                "/status": "حالة قاعدة البيانات",
                "/logs": "آخر العمليات"
            },
            "docs": {
                "/docs": "وثائق API",
                "/redoc": "وثائق مفصلة"
            }
        },
        "supabase_project": "wuqbovrurauffztbkbse",
        "railway_project": "1336874a-1120-4f18-87c2-b5b9b1e0d439"
    }

@app.get("/health")
async def health_check():
    """فحص صحة النظام"""
    try:
        connections_ok = test_connections()
        uptime = time.time() - app_start_time
        
        return {
            "status": "healthy" if connections_ok else "degraded",
            "platform": "Railway",
            "database": "Supabase",
            "uptime_seconds": round(uptime, 2),
            "connections": {
                "daftra_api": connections_ok,
                "supabase_db": connections_ok
            },
            "timestamp": datetime.now().isoformat(),
            "environment": os.environ.get("RAILWAY_ENVIRONMENT", "production")
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"خطأ في فحص الصحة: {str(e)}")

@app.get("/sync-invoices", response_model=SyncResponse)
async def sync_invoices_endpoint():
    """مزامنة الفواتير - الإعدادات الافتراضية"""
    try:
        logger.info("🔄 بدء مزامنة الفواتير على Railway")
        
        start_time = time.time()
        result = sync_invoices_sync(max_pages=3, limit=5, check_existing=False)
        duration = time.time() - start_time
        
        if result.get("success", False):
            summary = result.get("summary", {})
            response = SyncResponse(
                success=True,
                message=f"✅ تم سحب {summary.get('total_synced', 0)} فاتورة بنجاح",
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                service="invoices",
                duration=summary.get("duration_formatted", f"{duration:.1f}ث"),
                total_processed=summary.get("total_processed", 0),
                total_synced=summary.get("total_synced", 0),
                errors_count=summary.get("errors_count", 0),
                warnings_count=summary.get("warnings_count", 0),
                details={
                    "platform": "Railway",
                    "database": "Supabase",
                    "total_items": summary.get("total_items", 0),
                    "success_rate": summary.get("success_rate", "0%"),
                    "recent_errors": result.get("recent_errors", [])[-2:]
                }
            )
            
            # حفظ النتائج
            last_sync_results.update(response.dict())
            logger.info(f"✅ مزامنة ناجحة: {response.total_synced} فاتورة")
            
            return response
        else:
            error_msg = result.get("error", "خطأ غير معروف")
            logger.error(f"❌ فشلت المزامنة: {error_msg}")
            raise HTTPException(status_code=500, detail=error_msg)
            
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"خطأ في المزامنة: {str(e)}"
        logger.error(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)

@app.get("/sync-invoices-quick")
async def sync_invoices_quick():
    """مزامنة سريعة - فاتورة واحدة للاختبار"""
    try:
        logger.info("⚡ مزامنة سريعة")
        
        result = sync_invoices_sync(max_pages=1, limit=3, check_existing=False)
        
        if result.get("success", False):
            summary = result.get("summary", {})
            return {
                "success": True,
                "message": f"⚡ مزامنة سريعة: {summary.get('total_synced', 0)} فاتورة",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "duration": summary.get("duration_formatted", "غير محدد"),
                "details": summary
            }
        else:
            return {
                "success": False,
                "message": f"❌ فشلت المزامنة السريعة: {result.get('error', 'خطأ غير معروف')}",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
            
    except Exception as e:
        return {
            "success": False,
            "message": f"❌ خطأ في المزامنة السريعة: {str(e)}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

@app.get("/sync-invoices-test")
async def test_sync():
    """اختبار الاتصال والمزامنة"""
    try:
        # فحص الاتصالات أولاً
        connections_ok = test_connections()
        
        if not connections_ok:
            return {
                "success": False,
                "message": "❌ فشل فحص الاتصالات",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "suggestions": [
                    "تحقق من اتصال الإنترنت",
                    "تحقق من صحة API keys",
                    "تحقق من حالة خوادم Daftra و Supabase"
                ]
            }
        
        # اختبار مزامنة فاتورة واحدة
        result = sync_invoices_sync(max_pages=1, limit=1, check_existing=False)
        
        return {
            "success": result.get("success", False),
            "message": "🧪 اختبار المزامنة اكتمل",
            "connections": "✅ الاتصالات تعمل",
            "sync_result": result.get("summary", {}),
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
    except Exception as e:
        return {
            "success": False,
            "message": f"❌ خطأ في الاختبار: {str(e)}",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

@app.get("/status")
async def database_status():
    """حالة قاعدة البيانات"""
    try:
        connections_ok = test_connections()
        
        return {
            "database": {
                "provider": "Supabase",
                "project_id": "wuqbovrurauffztbkbse",
                "status": "🟢 متصل" if connections_ok else "🔴 غير متصل",
                "tables": ["invoices", "invoice_items", "products"],
                "last_check": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            },
            "api": {
                "daftra_status": "🟢 متصل" if connections_ok else "🔴 غير متصل",
                "base_url": "https://shadowpeace.daftra.com"
            },
            "platform": {
                "hosting": "Railway",
                "environment": os.environ.get("RAILWAY_ENVIRONMENT", "production"),
                "project_id": "1336874a-1120-4f18-87c2-b5b9b1e0d439"
            }
        }
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في جلب حالة قاعدة البيانات: {str(e)}")

@app.get("/logs")
async def recent_logs():
    """آخر العمليات والسجلات"""
    return {
        "last_sync": last_sync_results if last_sync_results else "لم يتم تنفيذ مزامنة بعد",
        "uptime": f"{(time.time() - app_start_time)//60:.0f} دقيقة",
        "platform": "Railway + Supabase",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

# الحصول على البورت من البيئة (Railway)
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    
    print("🚂 تشغيل على Railway...")
    print(f"📍 البورت: {port}")
    print("🗄️ قاعدة البيانات: Supabase")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=port,
        log_level="info"
    )
