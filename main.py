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
    from products_service import sync_products
    from invoices_service import run_sync as sync_invoices_sync, test_connections
    logger.info("✅ تم تحميل جميع الخدمات بنجاح")
except ImportError as e:
    logger.error(f"❌ خطأ في استيراد الخدمات: {e}")
    raise

# إنشاء التطبيق
app = FastAPI(
    title="Daftra Sync API",
    description="نظام مزامنة البيانات من دفترة إلى قاعدة البيانات",
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

# نماذج البيانات
class SyncRequest(BaseModel):
    max_pages: int = Field(default=3, ge=1, le=20, description="عدد الصفحات (1-20)")
    limit: int = Field(default=5, ge=1, le=50, description="عدد العناصر لكل صفحة (1-50)")
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

class User(BaseModel):
    id: int
    name: str
    dob: date = Field(title='Date of Birth')

class SystemStatus(BaseModel):
    status: str
    daftra_connection: bool
    supabase_connection: bool
    last_check: str
    uptime: str

# متغيرات النظام
app_start_time = time.time()
last_sync_results = {}

# المستخدمون التجريبيون
users = [
    User(id=1, name='John', dob=date(1990, 1, 1)),
    User(id=2, name='Jack', dob=date(1991, 1, 1)),
]

@app.on_event("startup")
async def startup_event():
    """أحداث بدء التشغيل"""
    logger.info("🚀 بدء تشغيل Daftra Sync API")
    
    # فحص الاتصالات عند البدء
    try:
        connections_ok = test_connections()
        if connections_ok:
            logger.info("✅ جميع الاتصالات تعمل بشكل صحيح")
        else:
            logger.warning("⚠️ مشاكل في بعض الاتصالات")
    except Exception as e:
        logger.error(f"❌ خطأ في فحص الاتصالات: {e}")

@app.get("/", response_model=Dict[str, Any])
async def home():
    """الصفحة الرئيسية للـ API"""
    uptime = time.time() - app_start_time
    uptime_str = f"{uptime//3600:.0f}س {(uptime%3600)//60:.0f}د"
    
    return {
        "service": "Daftra Sync API",
        "version": "2.0.0", 
        "status": "🟢 يعمل",
        "uptime": uptime_str,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "endpoints": {
            "sync": {
                "/sync-products": "سحب المنتجات من دفترة",
                "/sync-invoices": "سحب فواتير المبيعات من دفترة",
                "/sync-invoices-advanced": "سحب الفواتير مع إعدادات متقدمة"
            },
            "monitoring": {
                "/status": "حالة النظام والاتصالات",
                "/sync-status": "حالة آخر عمليات المزامنة",
                "/health": "فحص صحة النظام"
            },
            "docs": {
                "/docs": "وثائق API التفاعلية",
                "/redoc": "وثائق ReDoc"
            }
        },
        "last_sync": last_sync_results.get("timestamp", "لم يتم تنفيذ مزامنة بعد")
    }

@app.get("/health")
async def health_check():
    """فحص صحة النظام"""
    try:
        connections_ok = test_connections()
        uptime = time.time() - app_start_time
        
        return {
            "status": "healthy" if connections_ok else "degraded",
            "uptime_seconds": round(uptime, 2),
            "connections": {
                "daftra": True,  # سيتم التحقق الفعلي
                "supabase": True  # سيتم التحقق الفعلي
            },
            "timestamp": datetime.now().isoformat()
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"خطأ في فحص الصحة: {str(e)}")

@app.get("/status", response_model=SystemStatus)
async def system_status():
    """حالة النظام والاتصالات"""
    try:
        connections_ok = test_connections()
        uptime = time.time() - app_start_time
        uptime_str = f"{uptime//3600:.0f}س {(uptime%3600)//60:.0f}د"
        
        return SystemStatus(
            status="🟢 يعمل" if connections_ok else "🟡 مشاكل جزئية",
            daftra_connection=True,  # سيتم فحص فعلي
            supabase_connection=True,  # سيتم فحص فعلي
            last_check=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            uptime=uptime_str
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"خطأ في جلب حالة النظام: {str(e)}")

@app.get("/sync-status")
async def sync_status():
    """حالة آخر عمليات المزامنة"""
    if not last_sync_results:
        return {
            "message": "لم يتم تنفيذ أي عملية مزامنة بعد",
            "suggestions": [
                "استخدم /sync-invoices لمزامنة الفواتير",
                "استخدم /sync-products لمزامنة المنتجات"
            ]
        }
    
    return {
        "last_sync": last_sync_results,
        "status": "✅ آخر مزامنة تمت بنجاح" if last_sync_results.get("success") else "❌ آخر مزامنة فشلت"
    }

@app.get("/sync-products")
async def products_endpoint(background_tasks: BackgroundTasks):
    """نقطة نهاية سحب المنتجات"""
    try:
        logger.info("🔄 بدء مزامنة المنتجات")
        
        # تنفيذ المزامنة
        result = await sync_products()
        
        # حفظ النتائج
        sync_result = {
            "success": True,
            "message": f"تم سحب {result.get('total_synced', 0)} منتج جديد",
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "service": "products",
            "duration": result.get("duration", "غير محدد"),
            "total_processed": result.get("total_processed", 0),
            "total_synced": result.get("total_synced", 0),
            "errors_count": len(result.get("errors", [])),
            "warnings_count": len(result.get("warnings", [])),
            "details": result
        }
        
        last_sync_results.update(sync_result)
        logger.info(f"✅ مزامنة المنتجات اكتملت: {sync_result['total_synced']} منتج")
        
        return sync_result
        
    except Exception as e:
        error_msg = f"خطأ في مزامنة المنتجات: {str(e)}"
        logger.error(error_msg)
        
        error_result = {
            "success": False,
            "message": error_msg,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "service": "products",
            "duration": "0s",
            "total_processed": 0,
            "total_synced": 0,
            "errors_count": 1,
            "warnings_count": 0
        }
        
        last_sync_results.update(error_result)
        raise HTTPException(status_code=500, detail=error_result)

@app.get("/sync-invoices", response_model=SyncResponse)
async def invoices_endpoint():
    """نقطة نهاية سحب فواتير المبيعات (إعدادات افتراضية)"""
    return await sync_invoices_advanced(SyncRequest())

@app.post("/sync-invoices-advanced", response_model=SyncResponse)
async def sync_invoices_advanced(sync_request: SyncRequest):
    """نقطة نهاية سحب الفواتير مع إعدادات متقدمة"""
    try:
        logger.info(f"🔄 بدء مزامنة الفواتير مع الإعدادات: {sync_request.dict()}")
        
        # تنفيذ المزامنة
        start_time = time.time()
        result = sync_invoices_sync(
            max_pages=sync_request.max_pages,
            limit=sync_request.limit,
            check_existing=sync_request.check_existing
        )
        duration = time.time() - start_time
        
        # تحضير الاستجابة
        if result.get("success", False):
            summary = result.get("summary", {})
            response = SyncResponse(
                success=True,
                message=f"تم سحب {summary.get('total_synced', 0)} فاتورة جديدة بنجاح",
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                service="invoices",
                duration=summary.get("duration_formatted", f"{duration:.1f}ث"),
                total_processed=summary.get("total_processed", 0),
                total_synced=summary.get("total_synced", 0),
                errors_count=summary.get("errors_count", 0),
                warnings_count=summary.get("warnings_count", 0),
                details={
                    "pages_processed": sync_request.max_pages,
                    "items_per_page": sync_request.limit,
                    "check_existing_enabled": sync_request.check_existing,
                    "total_items_synced": summary.get("total_items", 0),
                    "success_rate": summary.get("success_rate", "0%"),
                    "avg_items_per_invoice": summary.get("avg_items_per_invoice", 0),
                    "recent_errors": result.get("recent_errors", [])[-3:],
                    "recent_warnings": result.get("recent_warnings", [])[-3:]
                }
            )
        else:
            response = SyncResponse(
                success=False,
                message=f"فشلت المزامنة: {result.get('error', 'خطأ غير معروف')}",
                timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                service="invoices",
                duration=f"{duration:.1f}ث",
                total_processed=0,
                total_synced=0,
                errors_count=1,
                warnings_count=0,
                details={"error": result.get("error")}
            )
        
        # حفظ النتائج
        last_sync_results.update(response.dict())
        
        logger.info(f"{'✅' if response.success else '❌'} مزامنة الفواتير: {response.message}")
        
        if not response.success:
            raise HTTPException(status_code=500, detail=response.dict())
            
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        error_msg = f"خطأ غير متوقع في مزامنة الفواتير: {str(e)}"
        logger.error(error_msg)
        
        error_response = SyncResponse(
            success=False,
            message=error_msg,
            timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            service="invoices",
            duration="0s",
            total_processed=0,
            total_synced=0,
            errors_count=1,
            warnings_count=0
        )
        
        last_sync_results.update(error_response.dict())
        raise HTTPException(status_code=500, detail=error_response.dict())

@app.get("/sync-invoices-quick")
async def sync_invoices_quick():
    """مزامنة سريعة للفواتير (صفحة واحدة، 3 فواتير)"""
    quick_request = SyncRequest(max_pages=1, limit=3, check_existing=False)
    return await sync_invoices_advanced(quick_request)

# واجهة FastUI
@app.get("/api/", response_model=FastUI, response_model_exclude_none=True)
def dashboard() -> list[AnyComponent]:
    """لوحة المراقبة الرئيسية"""
    uptime = time.time() - app_start_time
    uptime_str = f"{uptime//3600:.0f}س {(uptime%3600)//60:.0f}د"
    
    return [
        c.Page(
            components=[
                c.Heading(text='🏪 Daftra Sync Dashboard', level=1),
                c.Text(text='نظام مزامنة البيانات من دفترة إلى قاعدة البيانات'),
                
                # معلومات النظام
                c.Div(
                    components=[
                        c.Heading(text='📊 معلومات النظام', level=3),
                        c.Table(
                            data=[
                                {'المفتاح': 'حالة النظام', 'القيمة': '🟢 يعمل'},
                                {'المفتاح': 'وقت التشغيل', 'القيمة': uptime_str},
                                {'المفتاح': 'آخر مزامنة', 'القيمة': last_sync_results.get('timestamp', 'لم يتم بعد')},
                            ],
                            columns=[
                                {'field': 'المفتاح', 'title': 'المفتاح'},
                                {'field': 'القيمة', 'title': 'القيمة'},
                            ]
                        )
                    ]
                ),
                
                # إحصائيات آخر مزامنة
                c.Div(
                    components=[
                        c.Heading(text='📈 آخر مزامنة', level=3),
                        c.Text(text=last_sync_results.get('message', 'لم يتم تنفيذ مزامنة بعد'))
                    ] if last_sync_results else [
                        c.Heading(text='📈 آخر مزامنة', level=3),
                        c.Text(text='لم يتم تنفيذ أي عملية مزامنة بعد')
                    ]
                ),
                
                # روابط المزامنة
                c.Div(
                    components=[
                        c.Heading(text='🔄 عمليات المزامنة', level=3),
                        c.Text(text='استخدم الروابط التالية لتنفيذ المزامنة:'),
                        c.Text(text='• GET /sync-products - مزامنة المنتجات'),
                        c.Text(text='• GET /sync-invoices - مزامنة الفواتير'),
                        c.Text(text='• POST /sync-invoices-advanced - مزامنة متقدمة'),
                    ]
                )
            ]
        ),
    ]

@app.get('/ui/{path:path}')
async def html_landing():
    """صفحة الواجهة"""
    return HTMLResponse(prebuilt_html(title='Daftra Sync Dashboard'))

# تشغيل الخادم
if __name__ == "__main__":
    print("🚀 تشغيل Daftra Sync API...")
    print("📍 الخادم سيعمل على: http://localhost:8000")
    print("📚 الوثائق متاحة على: http://localhost:8000/docs")
    print("📊 لوحة المراقبة: http://localhost:8000/ui")
    
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
