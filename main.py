from datetime import date
import requests

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastui import FastUI, AnyComponent, prebuilt_html, components as c
from fastui.components.display import DisplayMode, DisplayLookup
from fastui.events import GoToEvent, BackEvent
from pydantic import BaseModel, Field

app = FastAPI()

class User(BaseModel):
    id: int
    name: str
    dob: date = Field(title='Date of Birth')

# define some users
users = [
    User(id=1, name='John', dob=date(1990, 1, 1)),
    User(id=2, name='Jack', dob=date(1991, 1, 1)),
]

@app.get("/api/", response_model=FastUI, response_model_exclude_none=True)
def users_table() -> list[AnyComponent]:
    return [
        c.Page(
            components=[
                c.Heading(text='Users', level=2),
                c.Table(
                    data=users,
                    columns=[
                        DisplayLookup(field='name', on_click=GoToEvent(url='/user/{id}/')),
                        DisplayLookup(field='dob', mode=DisplayMode.date),
                    ],
                ),
            ]
        ),
    ]

@app.get("/api/user/{user_id}/", response_model=FastUI, response_model_exclude_none=True)
def user_profile(user_id: int) -> list[AnyComponent]:
    try:
        user = next(u for u in users if u.id == user_id)
    except StopIteration:
        raise HTTPException(status_code=404, detail="User not found")
    return [
        c.Page(
            components=[
                c.Heading(text=user.name, level=2),
                c.Link(components=[c.Text(text='Back')], on_click=BackEvent()),
                c.Details(data=user),
            ]
        ),
    ]

@app.post("/sync-products")
async def sync_products():
    """سحب المنتجات من دفترة وإرسالها لـ Supabase"""
    
    daftra_url = "https://shadowpeace.daftra.com"
    daftra_headers = {"apikey": "024ee6d1c1bf36dcbee7978191d81df23cc11a3b"}
    
    supabase_url = "https://wuqbovrurauffztbkbse.supabase.co"
    supabase_key = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Ind1cWJvdnJ1cmF1ZmZ6dGJrYnNlIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc0Nzg3MTA0NywiZXhwIjoyMDYzNDQ3MDQ3fQ.6ekq6VV2gcyw4uOHfscO9vIzUBSGDk_yweiGOGSPyFo"
    
    total_synced = 0
    page = 1
    limit = 20
    
    while page <= 1:
        try:
            response = requests.get(
                f"{daftra_url}/v2/api/entity/product/list/1?page={page}&limit={limit}",
                headers=daftra_headers
            )
            
            if response.status_code != 200:
                break
                
            data = response.json()
            products = data.get("data", [])
            
            if not products:
                break
                
            for product in products:
                product_data = {
                    "name": product.get("name", ""),
                    "product_code": product.get("product_code", ""),
                    "brand": product.get("brand", ""),
                    "stock_balance": str(product.get("stock_balance", 0)),
                    "buy_price": str(product.get("buy_price", 0)),
                    "minimum_price": str(product.get("minimum_price", 0)),
                    "average_price": str(product.get("average_price", 0)),
                    "supplier_code": product.get("supplier_code", "")
                }
                
                check_response = requests.get(
                    f"{supabase_url}/rest/v1/products?product_code=eq.{product_data['product_code']}",
                    headers={
                        "apikey": supabase_key,
                        "Authorization": f"Bearer {supabase_key}",
                        "Content-Type": "application/json",
                        "Prefer": "count=exact"
                    }
                )
                
                count = check_response.headers.get("content-range", "").split("/")[-1]
                
                if int(count or 0) == 0:
                    insert_response = requests.post(
                        f"{supabase_url}/rest/v1/products",
                        headers={
                            "apikey": supabase_key,
                            "Authorization": f"Bearer {supabase_key}",
                            "Content-Type": "application/json"
                        },
                        json=product_data
                    )
                    
                    if insert_response.status_code == 201:
                        total_synced += 1
            
            page += 1
            
        except Exception as e:
            break
    
    return {"message": f"تم سحب {total_synced} منتج بنجاح", "total_synced": total_synced}

@app.get('/{path:path}')
async def html_landing() -> HTMLResponse:
    return HTMLResponse(prebuilt_html(title='FastUI Demo'))
