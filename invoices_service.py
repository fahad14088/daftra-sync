# invoices_service_fixed.py

import os
import requests
import time
import uuid
import logging
from datetime import datetime
from typing import Dict, List, Optional

# إعداد التسجيل
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class InvoiceSyncFixed:
    def __init__(self):
        self.daftra_url = os.getenv("DAFTRA_URL")
        self.daftra_apikey = os.getenv("DAFTRA_APIKEY") 
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        # Headers
        self.headers_daftra = {
            "apikey": self.daftra_apikey,
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        self.headers_supabase = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json"
        }
        
        # إحصائيات
        self.stats = {
            "invoices_processed": 0,
            "invoices_saved": 0,
            "items_saved": 0,
            "errors": []
        }

    def test_connections(self):
        """اختبار الاتصالات قبل البدء"""
        logger.info("🔍 اختبار الاتصالات...")
        
        # اختبار دفترة
        try:
            test_url = f"{self.daftra_url}/v2/api/entity/invoice/list/1?page=1&limit=1"
            response = requests.get(test_url, headers=self.headers_daftra, timeout=10)
            logger.info(f"📱 دفترة - Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                logger.info(f"📊 عينة من البيانات: {data}")
            else:
                logger.error(f"❌ فشل الاتصال بدفترة: {response.text}")
                return False
        except Exception as e:
            logger.error(f"❌ خطأ في الاتصال بدفترة: {e}")
            return False
        
        # اختبار Supabase
        try:
            test_url = f"{self.supabase_url}/rest/v1/invoices?select=count"
            response = requests.get(test_url, headers=self.headers_supabase, timeout=10)
            logger.info(f"🗄️ Supabase - Status: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"❌ فشل الاتصال بـ Supabase: {response.text}")
                return False
        except Exception as e:
            logger.error(f"❌ خطأ في الاتصال بـ Supabase: {e}")
            return False
        
        logger.info("✅ جميع الاتصالات تعمل بشكل صحيح!")
        return True

    def get_all_invoices_from_daftra(self):
        """جلب جميع الفواتير من دفترة - بدون فلترة الفروع"""
        logger.info("📥 جاري جلب جميع الفواتير من دفترة...")
        all_invoices = []
        
        # جرب بدون فلتر الفرع أولاً
        page = 1
        while True:
            try:
                # URL مبسط بدون فلتر الفرع
                url = f"{self.daftra_url}/v2/api/entity/invoice/list/1?page={page}&limit=50"
                
                logger.info(f"🔍 جلب الصفحة {page}: {url}")
                
                response = requests.get(url, headers=self.headers_daftra, timeout=30)
                
                if response.status_code != 200:
                    logger.error(f"❌ خطأ في جلب الصفحة {page}: {response.status_code} - {response.text}")
                    break
                
                data = response.json()
                invoices = data.get("data", [])
                
                logger.info(f"📊 الصفحة {page}: وجدت {len(invoices)} فاتورة")
                
                if not invoices:
                    logger.info("✅ انتهت جميع الفواتير")
                    break
                
                all_invoices.extend(invoices)
                page += 1
                
                # استراحة بين الطلبات
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"❌ خطأ في جلب الصفحة {page}: {e}")
                break
        
        logger.info(f"📋 إجمالي الفواتير المجلبة: {len(all_invoices)}")
        return all_invoices

    def get_invoice_details(self, invoice_id: str):
        """جلب تفاصيل الفاتورة - جرب طرق متعددة"""
        # جرب مع branch_id = 1 أولاً
        for branch_id in [1, 2, 3]:  # جرب عدة فروع
            try:
                url = f"{self.daftra_url}/v2/api/entity/invoice/show/{branch_id}/{invoice_id}"
                response = requests.get(url, headers=self.headers_daftra, timeout=30)
                
                if response.status_code == 200:
                    data = response.json()
                    invoice_data = data.get("data", {}).get("Invoice", {})
                    if invoice_data:
                        logger.debug(f"✅ تم جلب تفاصيل الفاتورة {invoice_id} من الفرع {branch_id}")
                        return invoice_data
                
            except Exception as e:
                logger.debug(f"❌ فشل جلب الفاتورة {invoice_id} من الفرع {branch_id}: {e}")
                continue
        
        logger.warning(f"⚠️ لم يتم العثور على تفاصيل الفاتورة: {invoice_id}")
        return None

    def save_invoice_to_supabase(self, invoice_data: Dict) -> bool:
        """حفظ الفاتورة في Supabase"""
        try:
            # تحضير البيانات
            invoice_payload = {
                "id": str(invoice_data["id"]),
                "invoice_no": str(invoice_data.get("no", "")),
                "invoice_date": invoice_data.get("date"),
                "customer_id": str(invoice_data.get("customer_id", "")),
                "client_business_name": str(invoice_data.get("client_business_name", "")),
                "total": float(invoice_data.get("total", 0)),
                "created_at": datetime.now().isoformat()
            }
            
            # إزالة الحقول الفارغة
            invoice_payload = {k: v for k, v in invoice_payload.items() 
                             if v is not None and v != "" and v != "None"}
            
            logger.debug(f"💾 حفظ الفاتورة: {invoice_payload}")
            
            response = requests.post(
                f"{self.supabase_url}/rest/v1/invoices",
                headers=self.headers_supabase,
                json=invoice_payload,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                logger.info(f"✅ تم حفظ الفاتورة: {invoice_data['id']}")
                self.stats["invoices_saved"] += 1
                return True
            elif response.status_code == 409:
                logger.info(f"ℹ️ الفاتورة موجودة مسبقاً: {invoice_data['id']}")
                return True
            else:
                logger.error(f"❌ فشل حفظ الفاتورة {invoice_data['id']}: {response.status_code} - {response.text}")
                self.stats["errors"].append(f"فاتورة {invoice_data['id']}: {response.text}")
                return False
                
        except Exception as e:
            logger.error(f"❌ خطأ في حفظ الفاتورة {invoice_data.get('id', 'unknown')}: {e}")
            self.stats["errors"].append(f"فاتورة {invoice_data.get('id', 'unknown')}: {e}")
            return False

    def save_invoice_items_to_supabase(self, invoice_id: str, items: List[Dict]) -> int:
        """حفظ بنود الفاتورة"""
        if not items:
            return 0
        
        saved_count = 0
        
        for item in items:
            try:
                # تحضير بيانات البند
                item_payload = {
                    "id": str(item.get("id", str(uuid.uuid4()))),
                    "invoice_id": str(invoice_id),
                    "product_id": str(item.get("product_id", "")),
                    "quantity": float(item.get("quantity", 0)),
                    "unit_price": float(item.get("unit_price", 0)),
                    "total_price": float(item.get("quantity", 0)) * float(item.get("unit_price", 0)),
                    "created_at": datetime.now().isoformat()
                }
                
                # تجاهل البنود بكمية صفر
                if item_payload["quantity"] <= 0:
                    continue
                
                logger.debug(f"💾 حفظ البند: {item_payload}")
                
                response = requests.post(
                    f"{self.supabase_url}/rest/v1/invoice_items",
                    headers=self.headers_supabase,
                    json=item_payload,
                    timeout=30
                )
                
                if response.status_code in [200, 201]:
                    saved_count += 1
                    logger.debug(f"✅ تم حفظ البند: {item_payload['id']}")
                elif response.status_code == 409:
                    logger.debug(f"ℹ️ البند موجود مسبقاً: {item_payload['id']}")
                    saved_count += 1
                else:
                    logger.error(f"❌ فشل حفظ البند: {response.status_code} - {response.text}")
                    self.stats["errors"].append(f"بند {item_payload['id']}: {response.text}")
                    
            except Exception as e:
                logger.error(f"❌ خطأ في حفظ البند: {e}")
                self.stats["errors"].append(f"بند: {e}")
        
        self.stats["items_saved"] += saved_count
        return saved_count

    def sync_all_invoices(self):
        """المزامنة الكاملة - الدالة الرئيسية"""
        logger.info("🚀 بدء عملية المزامنة الكاملة...")
        
        # اختبار الاتصالات أولاً
        if not self.test_connections():
            logger.error("❌ فشل في اختبار الاتصالات!")
            return self.stats
        
        # جلب جميع الفواتير
        all_invoices = self.get_all_invoices_from_daftra()
        
        if not all_invoices:
            logger.warning("⚠️ لم يتم العثور على أي فواتير!")
            return self.stats
        
        logger.info(f"📋 سيتم معالجة {len(all_invoices)} فاتورة")
        
        # معالجة كل فاتورة
        for i, invoice_summary in enumerate(all_invoices, 1):
            try:
                invoice_id = str(invoice_summary.get("id"))
                logger.info(f"🔄 معالجة الفاتورة {i}/{len(all_invoices)}: {invoice_id}")
                
                # جلب تفاصيل الفاتورة
                invoice_details = self.get_invoice_details(invoice_id)
                
                if not invoice_details:
                    logger.warning(f"⚠️ لم يتم العثور على تفاصيل الفاتورة: {invoice_id}")
                    continue
                
                # حفظ الفاتورة
                if self.save_invoice_to_supabase(invoice_details):
                    # حفظ البنود
                    items = invoice_details.get("invoice_item", [])
                    if not isinstance(items, list):
                        items = [items] if items else []
                    
                    if items:
                        saved_items = self.save_invoice_items_to_supabase(invoice_id, items)
                        logger.info(f"✅ تم حفظ {saved_items} بند للفاتورة {invoice_id}")
                    else:
                        logger.info(f"ℹ️ لا توجد بنود للفاتورة {invoice_id}")
                
                self.stats["invoices_processed"] += 1
                
                # استراحة قصيرة
                time.sleep(0.5)
                
            except Exception as e:
                logger.error(f"❌ خطأ في معالجة الفاتورة {invoice_summary.get('id', 'unknown')}: {e}")
                self.stats["errors"].append(f"فاتورة {invoice_summary.get('id', 'unknown')}: {e}")
        
        # طباعة النتائج النهائية
        self.print_final_results()
        return self.stats

    def print_final_results(self):
        """طباعة النتائج النهائية"""
        logger.info("=" * 80)
        logger.info("🎯 النتائج النهائية:")
        logger.info(f"📊 الفواتير المعالجة: {self.stats['invoices_processed']}")
        logger.info(f"✅ الفواتير المحفوظة: {self.stats['invoices_saved']}")
        logger.info(f"📦 البنود المحفوظة: {self.stats['items_saved']}")
        logger.info(f"❌ عدد الأخطاء: {len(self.stats['errors'])}")
        
        if self.stats['errors']:
            logger.error("🚨 الأخطاء:")
            for error in self.stats['errors'][:5]:  # أول 5 أخطاء
                logger.error(f"  - {error}")
            if len(self.stats['errors']) > 5:
                logger.error(f"  ... و {len(self.stats['errors']) - 5} أخطاء أخرى")
        
        logger.info("=" * 80)

def main():
    """تشغيل المزامنة"""
    try:
        sync_service = InvoiceSyncFixed()
        result = sync_service.sync_all_invoices()
        
        if result["invoices_saved"] > 0:
            logger.info("🎉 تمت المزامنة بنجاح!")
        else:
            logger.warning("⚠️ لم يتم حفظ أي فواتير - تحقق من الإعدادات!")
        
        return result
        
    except Exception as e:
        logger.error(f"💥 خطأ فادح: {e}")
        raise

if __name__ == "__main__":
    main()
