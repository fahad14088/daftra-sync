from datetime import date
import time
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastui import FastUI, AnyComponent, prebuilt_html, components as c
from pydantic import BaseModel, Field

# استيراد الخدمات
from products_service import sync_products

app = FastAPI()

@app.get("/sync-products")
async def products_endpoint():
    """نقطة نهاية المنتجات"""
    result = await sync_products()
    return {
        "message": f"تم سحب {result['total_synced']} منتج جديد",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        **result
    }

@app.get("/")
async def home():
    return {"message": "Daftra Sync API", "status": "running"}

@app.get('/{path:path}')
async def html_landing():
    return HTMLResponse(prebuilt_html(title='Daftra Sync'))
