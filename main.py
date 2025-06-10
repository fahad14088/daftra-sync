# main.py
import os
import sys
from products_service import sync_products
from invoices_service import sync_invoices
# …

def main():
    print(f"🔄 مزامنة المنتجات... URL={os.getenv('DAFTRA_URL')}")
    r1 = sync_products()
    print(f"✅ المنتجات: {r1['synced']} سجل")

    print(f"🔄 مزامنة الفواتير... SUPABASE={os.getenv('SUPABASE_URL')}")
    r2 = sync_invoices()
    print(f"✅ الفواتير: {r2['invoices']} فاتورة، {r2['items']} بند")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"❌ خطأ عام: {e}")
        sys.exit(1)
