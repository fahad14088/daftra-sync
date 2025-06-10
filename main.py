import sys
from products_service import sync_products
from invoices_service import sync_invoices

def main():
    print("🔄 مزامنة المنتجات...")
    r1 = sync_products()
    print(f"✅ المنتجات: {r1['synced']} سجل")

    print("\n🔄 مزامنة الفواتير...")
    r2 = sync_invoices()
    print(f"✅ الفواتير: {r2['invoices']} فاتورة، {r2['items']} بند")

if __name__=="__main__":
    try:
        main()
    except Exception as e:
        print("❌ خطأ عام:", e)
        sys.exit(1)
