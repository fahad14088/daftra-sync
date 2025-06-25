def fix_invoice_items_using_product_id():
    print("🔧 تصحيح كود المنتج في البنود باستخدام product_id...")

    url_products = f"{SUPABASE_URL}/rest/v1/products?select=product_id,product_code"
    res = requests.get(url_products, headers=HEADERS_SB)
    if res.status_code != 200:
        print("❌ فشل في جلب المنتجات")
        return

    # بناء قاموس المنتجات
    product_map = {}
    for p in res.json():
        pid = str(p.get("product_id", "")).strip()
        code = p.get("product_code")
        if pid and code:
            product_map[pid] = code

    print(f"📦 عدد المنتجات المحملة: {len(product_map)}")

    limit = 1000
    offset = 0
    total_updated = 0

    while True:
        url_items = f"{SUPABASE_URL}/rest/v1/invoice_items?select=id,product_id&limit={limit}&offset={offset}"
        res = requests.get(url_items, headers=HEADERS_SB)
        if res.status_code != 200:
            print("❌ فشل في جلب البنود")
            break

        batch = res.json()
        if not batch:
            break

        print(f"🔍 فحص {len(batch)} بند من offset={offset}")
        for row in batch:
            item_id = row["id"]
            pid_raw = row.get("product_id", "")
            pid_clean = str(pid_raw).strip()

            actual_code = product_map.get(pid_clean)

            if actual_code:
                patch_url = f"{SUPABASE_URL}/rest/v1/invoice_items?id=eq.{item_id}"
                patch_payload = {"product_code": actual_code}
                res_patch = requests.patch(patch_url, headers=HEADERS_SB, json=patch_payload)
                print(f"🔄 تحديث بند {item_id} ← {pid_clean} → {actual_code} → {res_patch.status_code}")
                if res_patch.status_code in [200, 204]:
                    total_updated += 1
            else:
                print(f"⚠️ لم يتم العثور على كود لـ product_id={pid_clean} رغم أنه موجود في جدول المنتجات")
                مشابهة = [k for k in product_map if pid_clean in k or k in pid_clean]
                if مشابهة:
                    print(f"🔎 مفاتيح مشابهة موجودة: {مشابهة}")

        offset += limit
        time.sleep(0.5)

    print(f"✅ تم تحديث {total_updated} بند بنجاح.")
