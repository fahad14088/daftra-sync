import os
import sys
from products_service import sync_products, fix_invoice_items_using_product_id
from invoice_supabase_sync import fetch_all as sync_invoices, fetch_missing_items

def main():
    print(f"🔄 مزامنة المنتجات... URL={os.getenv('DAFTRA_URL')}")
    r1 = sync_products()
    print(f"✅ المنتجات: {r1['synced']} سجل")

    # ✅ تصحيح البنود القديمة بعد جلب المنتجات مباشرة
    try:
        print("🔧 تصحيح البنود القديمة باستخدام product_id...")
        fix_invoice_items_using_product_id()
    except Exception as e:
        print(f"❌ خطأ أثناء التصحيح (القديم): {e}")

    print(f"🔄 مزامنة الفواتير... SUPABASE={os.getenv('SUPABASE_URL')}")
    r2 = sync_invoices()
    print(f"✅ الفواتير: {r2['invoices']} فاتورة، {r2['items']} بند")
    
    # جلب البنود المفقودة
    print(f"🔍 البحث عن البنود المفقودة...")
    try:
        from invoice_supabase_sync import DaftraClient, SupabaseClient
        
        daftra_client = DaftraClient()
        supabase_client = SupabaseClient()
        missing_stats = fetch_missing_items(daftra_client, supabase_client)
        
        print(f"✅ البنود المفقودة: {missing_stats['items_saved']} تم جلبها")
    except Exception as e:
        print(f"❌ خطأ في البنود المفقودة: {e}")
    
    # ✅ تصحيح البنود بعد جلب فواتير جديدة
    try:
        print("🔧 تصحيح البنود الجديدة باستخدام product_id...")
        fix_invoice_items_using_product_id()
    except Exception as e:
        print(f"❌ خطأ أثناء التصحيح (الجديد): {e}")

    # مزامنة العملاء
    try:
        print(f"🔄 مزامنة العملاء...")
        from customers_sync import main as sync_customers
        r3 = sync_customers()
        print(f"✅ العملاء: {r3['customers_saved']} عميل")
    except Exception as e:
        print(f"❌ خطأ في العملاء: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(f"❌ خطأ عام: {e}")
        sys.exit(1)
