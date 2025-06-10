# main.py - الإصدار المبسط
from datetime import date
import time
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastui import FastUI, AnyComponent, prebuilt_html, components as c
from pydantic import BaseModel, Field

# استيراد الخدمات
try:
    from products_service import sync_products
except:
    async def sync_products():
        return {"total_synced": 0, "error": "products_service not available"}

try:
    from invoices_service import sync_invoices
except:
    async def sync_invoices():
        return {"total_synced": 0, "error": "invoices_service not available"}

app = FastAPI()

class User(BaseModel):
    id: int
    name: str
    dob: date = Field(title='Date of Birth')

users = [
    User(id=1, name='John', dob=date(1990, 1, 1)),
    User(id=2, name='Jack', dob=date(1991, 1, 1)),
]

@app.get("/sync-products")
async def products_endpoint():
    try:
        result = await sync_products()
        return {
            "message": f"تم سحب {result.get('total_synced', 0)} منتج جديد",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "service": "products",
            **result
        }
    except Exception as e:
        return {
            "message": f"خطأ في مزامنة المنتجات: {str(e)}",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "service": "products",
            "total_synced": 0
        }

@app.get("/sync-invoices")
async def invoices_endpoint():
    try:
        result = await sync_invoices()
        return {
            "message": f"تم سحب {result.get('total_synced', 0)} فاتورة جديدة",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "service": "invoices",
            **result
        }
    except Exception as e:
        return {
            "message": f"خطأ في مزامنة الفواتير: {str(e)}",
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
            "service": "invoices",
            "total_synced": 0
        }

@app.get("/api/", response_model=FastUI, response_model_exclude_none=True)
def users_table() -> list[AnyComponent]:
    return [
        c.Page(
            components=[
                c.Heading(text='Daftra Sync API', level=2),
                c.Text(text='نظام مزامنة البيانات من دفترة'),
            ]
        ),
    ]

@app.get("/")
async def home():
    return {
        "message": "Daftra Sync API", 
        "status": "running",
        "endpoints": [
            "/sync-products - سحب المنتجات",
            "/sync-invoices - سحب فواتير المبيعات",
            "/sync-customers - قريباً"
        ]
    }

@app.get('/{path:path}')
async def html_landing():
    return HTMLResponse(prebuilt_html(title='Daftra Sync'))
