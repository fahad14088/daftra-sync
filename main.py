from datetime import date
import time
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastui import FastUI, AnyComponent, prebuilt_html, components as c
from pydantic import BaseModel, Field

# استيراد خدمة المنتجات
from products_service import sync_products

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
    """نقطة نهاية سحب المنتجات"""
    result = await sync_products()
    return {
        "message": f"تم سحب {result['total_synced']} منتج جديد",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "service": "products",
        **result
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
            "/sync-invoices - قريباً",
            "/sync-customers - قريباً"
        ]
    }

@app.get('/{path:path}')
async def html_landing():
    return HTMLResponse(prebuilt_html(title='Daftra Sync'))
