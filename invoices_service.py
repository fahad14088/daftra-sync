# invoices_service_improved.py

import os
import requests
import time
import uuid
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
import json

# إعداد نظام التسجيل
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('invoices_sync.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class InvoiceSyncService:
    def __init__(self):
        self.daftra_url = os.getenv("DAFTRA_URL")
        self.daftra_apikey = os.getenv("DAFTRA_APIKEY")
        self.supabase_url = os.getenv("SUPABASE_URL")
        self.supabase_key = os.getenv("SUPABASE_KEY")
        
        if not all([self.daftra_url, self.daftra_apikey, self.supabase_url, self.supabase_key]):
            raise ValueError("❌ المتغيرات البيئية غير مكتملة!")
        
        self.headers_daftra = {"apikey": self.daftra_apikey}
        self.headers_supabase = {
            "apikey": self.supabase_key,
            "Authorization": f"Bearer {self.supabase_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }
        
        self.success_count = {"invoices": 0, "items": 0}
        self.error_count = {"invoices": 0, "items": 0}
        self.errors = []

    def make_request(self, method: str, url: str, headers: Dict, data: Optional[Dict] = None, retries: int = 3) -> Tuple[bool, Dict]:
        """طلب HTTP محسن مع إعادة المحاولة ومعالجة الأخطاء"""
        for attempt in range(retries):
            try:
                if method.upper() == 'GET':
                    response = requests.get(url, headers=headers, timeout=30)
                elif method.upper() == 'POST':
                    response = requests.post(url, headers=headers, json=data, timeout=30)
                else:
                    raise ValueError(f"HTTP method غير مدعوم: {method}")
                
                # تسجيل تفاصيل الطلب
                logger.debug(f"🔄 {method} {url} - Status: {response.status_code}")
                
                if response.status_code in [200, 201]:
                    return True, response.json()
                elif response.status_code == 409:
                    logger.warning(f"⚠️ تضارب في البيانات: {response.text}")
                    return False, {"error": "duplicate", "message": response.text}
                else:
                    logger.error(f"❌ خطأ في الطلب: {response.status_code} - {response.text}")
                    return False, {"error": "http_error", "status": response.status_code, "message": response.text}
                    
            except requests.exceptions.Timeout:
                logger.warning(f"⏰ انتهت مهلة الطلب، المحاولة {attempt + 1}/{retries}")
            except requests.exceptions.ConnectionError:
                logger.warning(f"🔌 خطأ في الاتصال، المحاولة {attempt + 1}/{retries}")
            except Exception as e:
                logger.error(f"❌ خطأ غير متوقع: {str(e)}")
                
            if attempt < retries - 1:
                time.sleep((attempt + 1) * 2)  # تأخير متزايد
        
        return False, {"error": "max_retries_exceeded"}

    def get_product_mapping(self) -> Dict[str, str]:
        """جلب خريطة المنتجات من Supabase"""
        logger.info("🔍 جاري جلب خريطة المنتجات...")
        
        success, data = self.make_request(
            'GET',
            f"{self.supabase_url}/rest/v1/products?select=daftra_product_id,product_code",
            self.headers_supabase
        )
        
        if success and isinstance(data, list):
            product_map = {}
            for product in data:
                if product.get("daftra_product_id") and product.get("product_code"):
                    product_map[str(product["daftra_product_id"])] = product["product_code"]
            
            logger.info(f"✅ تم جلب {len(product_map)} منتج")
            return product_map
        else:
            logger.error("❌ فشل في جلب خريطة المنتجات")
            return {}

    def invoice_exists(self, invoice_id: str) -> bool:
        """التحقق من وجود الفاتورة"""
        success, data = self.make_request(
            'GET',
            f"{self.supabase_url}/rest/v1/invoices?select=id&id=eq.{invoice_id}",
            self.headers_supabase
        )
        return success and isinstance(data, list) and len(data) > 0

    def item_exists(self, item_id: str) -> bool:
        """التحقق من وجود البند"""
        success, data = self.make_request(
            'GET',
            f"{self.supabase_url}/rest/v1/invoice_items?select=id&id=eq.{item_id}",
            self.headers_supabase
        )
        return success and isinstance(data, list) and len(data) > 0

    def validate_invoice_data(self, invoice_data: Dict) -> bool:
        """التحقق من صحة بيانات الفاتورة"""
        required_fields = ['id', 'invoice_no']
        for field in required_fields:
            if not invoice_data.get(field):
                logger.error(f"❌ حقل مطلوب مفقود في الفاتورة: {field}")
                return False
        return True

    def validate_item_data(self, item_data: Dict) -> bool:
        """التحقق من صحة بيانات البند"""
        required_fields = ['id', 'invoice_id', 'quantity']
        for field in required_fields:
            if not item_data.get(field):
                logger.error(f"❌ حقل مطلوب مفقود في البند: {field}")
                return False
        
        # التحقق من أن الكمية رقم موجب
        try:
            quantity = float(item_data['quantity'])
            if quantity <= 0:
                logger.error(f"❌ كمية غير صحيحة: {quantity}")
                return False
        except (ValueError, TypeError):
            logger.error(f"❌ كمية غير صالحة: {item_data['quantity']}")
            return False
        
        return True

    def save_invoice(self, invoice_data: Dict) -> bool:
        """حفظ الفاتورة مع معالجة محسنة للأخطاء"""
        try:
            # التحقق من صحة البيانات
            if not self.validate_invoice_data(invoice_data):
                return False
            
            # تحويل البيانات للتأكد من التوافق
            clean_data = {
                "id": str(invoice_data["id"]),
                "invoice_no": str(invoice_data.get("invoice_no", "")),
                "invoice_date": invoice_data.get("invoice_date"),
                "customer_id": str(invoice_data.get("customer_id", "")),
                "total": invoice_data.get("total", 0),
                "created_at": datetime.now().isoformat()
            }
            
            # إزالة الحقول الفارغة
            clean_data = {k: v for k, v in clean_data.items() if v is not None and v != ""}
            
            success, result = self.make_request(
                'POST',
                f"{self.supabase_url}/rest/v1/invoices",
                self.headers_supabase,
                clean_data
            )
            
            if success:
                self.success_count["invoices"] += 1
                logger.info(f"✅ تم حفظ الفاتورة: {invoice_data['id']}")
                return True
            else:
                if result.get("error") == "duplicate":
                    logger.info(f"ℹ️ الفاتورة موجودة مسبقاً: {invoice_data['id']}")
                    return True
                else:
                    self.error_count["invoices"] += 1
                    error_msg = f"فشل حفظ الفاتورة {invoice_data['id']}: {result}"
                    self.errors.append(error_msg)
                    logger.error(f"❌ {error_msg}")
                    return False
                    
        except Exception as e:
            self.error_count["invoices"] += 1
            error_msg = f"خطأ في حفظ الفاتورة {invoice_data.get('id', 'unknown')}: {str(e)}"
            self.errors.append(error_msg)
            logger.error(f"❌ {error_msg}")
            return False

    def save_invoice_items(self, invoice_id: str, items: List[Dict], product_map: Dict[str, str]) -> bool:
        """حفظ بنود الفاتورة مع معالجة محسنة"""
        if not items:
            logger.info(f"ℹ️ لا توجد بنود للفاتورة: {invoice_id}")
            return True
        
        success_items = 0
        failed_items = 0
        
        for item in items:
            try:
                # إنشاء ID فريد للبند
                item_id = str(item.get("id", str(uuid.uuid4())))
                
                # التحقق من وجود البند
                if self.item_exists(item_id):
                    logger.info(f"ℹ️ البند موجود مسبقاً: {item_id}")
                    continue
                
                # تحضير بيانات البند
                quantity = float(item.get("quantity", 0))
                unit_price = float(item.get("unit_price", 0))
                product_id = str(item.get("product_id", ""))
                
                item_data = {
                    "id": item_id,
                    "invoice_id": str(invoice_id),
                    "product_id": product_id,
                    "product_code": product_map.get(product_id, ""),
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "total_price": quantity * unit_price,
                    "created_at": datetime.now().isoformat()
                }
                
                # التحقق من صحة البيانات
                if not self.validate_item_data(item_data):
                    failed_items += 1
                    continue
                
                # حفظ البند
                success, result = self.make_request(
                    'POST',
                    f"{self.supabase_url}/rest/v1/invoice_items",
                    self.headers_supabase,
                    item_data
                )
                
                if success:
                    success_items += 1
                    self.success_count["items"] += 1
                    logger.debug(f"✅ تم حفظ البند: {item_id}")
                else:
                    failed_items += 1
                    self.error_count["items"] += 1
                    error_msg = f"فشل حفظ البند {item_id}: {result}"
                    self.errors.append(error_msg)
                    logger.error(f"❌ {error_msg}")
                    
            except Exception as e:
                failed_items += 1
                self.error_count["items"] += 1
                error_msg = f"خطأ في معالجة البند: {str(e)}"
                self.errors.append(error_msg)
                logger.error(f"❌ {error_msg}")
        
        logger.info(f"📊 الفاتورة {invoice_id}: {success_items} بند نجح، {failed_items} بند فشل")
        return success_items > 0

    def fetch_invoices_from_daftra(self, branch_id: int, page: int = 1, limit: int = 20) -> List[Dict]:
        """جلب الفواتير من دفترة"""
        list_url = (
            f"{self.daftra_url}/v2/api/entity/invoice/list/1"
            f"?filter[branch_id]={branch_id}&page={page}&limit={limit}"
        )
        
        success, data = self.make_request('GET', list_url, self.headers_daftra)
        
        if success and isinstance(data, dict):
            return data.get("data", [])
        else:
            logger.error(f"❌ فشل جلب الفواتير من الفرع {branch_id}, الصفحة {page}")
            return []

    def fetch_invoice_details(self, branch_id: int, invoice_id: str) -> Optional[Dict]:
        """جلب تفاصيل الفاتورة"""
        detail_url = f"{self.daftra_url}/v2/api/entity/invoice/show/{branch_id}/{invoice_id}"
        
        success, data = self.make_request('GET', detail_url, self.headers_daftra)
        
        if success and isinstance(data, dict):
            return data.get("data", {}).get("Invoice", {})
        else:
            logger.error(f"❌ فشل جلب تفاصيل الفاتورة {invoice_id}")
            return None

    def sync_invoices(self, branches: List[int] = None, limit: int = 20):
        """مزامنة الفواتير - الدالة الرئيسية"""
        if not branches:
            branches = [1, 2]  # الفروع الافتراضية
        
        logger.info(f"🚀 بدء مزامنة الفواتير للفروع: {branches}")
        
        # جلب خريطة المنتجات
        product_map = self.get_product_mapping()
        
        total_processed = 0
        
        for branch_id in branches:
            logger.info(f"🏪 معالجة الفرع: {branch_id}")
            page = 1
            
            while True:
                # جلب قائمة الفواتير
                invoices = self.fetch_invoices_from_daftra(branch_id, page, limit)
                
                if not invoices:
                    logger.info(f"✅ انتهت فواتير الفرع {branch_id}")
                    break
                
                logger.info(f"📄 معالجة {len(invoices)} فاتورة من الصفحة {page}")
                
                for invoice_summary in invoices:
                    try:
                        invoice_id = str(invoice_summary.get("id"))
                        
                        # التحقق من وجود الفاتورة
                        if self.invoice_exists(invoice_id):
                            logger.debug(f"⏭️ تخطي الفاتورة الموجودة: {invoice_id}")
                            continue
                        
                        # جلب تفاصيل الفاتورة
                        invoice_details = self.fetch_invoice_details(branch_id, invoice_id)
                        
                        if not invoice_details:
                            continue
                        
                        # حفظ الفاتورة
                        invoice_saved = self.save_invoice({
                            "id": invoice_id,
                            "invoice_no": invoice_details.get("no", ""),
                            "invoice_date": invoice_details.get("date"),
                            "customer_id": invoice_details.get("customer_id"),
                            "total": float(invoice_details.get("total", 0))
                        })
                        
                        if invoice_saved:
                            # حفظ بنود الفاتورة
                            items = invoice_details.get("invoice_item", [])
                            if not isinstance(items, list):
                                items = [items] if items else []
                            
                            self.save_invoice_items(invoice_id, items, product_map)
                        
                        total_processed += 1
                        
                        # استراحة قصيرة لتجنب الضغط على الخادم
                        time.sleep(0.5)
                        
                    except Exception as e:
                        error_msg = f"خطأ في معالجة الفاتورة: {str(e)}"
                        self.errors.append(error_msg)
                        logger.error(f"❌ {error_msg}")
                
                page += 1
                time.sleep(1)  # استراحة بين الصفحات
        
        # تقرير النتائج
        self.print_summary(total_processed)
        
        return {
            "total_processed": total_processed,
            "success": self.success_count,
            "errors": self.error_count,
            "error_details": self.errors
        }

    def print_summary(self, total_processed: int):
        """طباعة ملخص العملية"""
        logger.info("=" * 60)
        logger.info("📊 ملخص عملية المزامنة:")
        logger.info(f"📋 إجمالي الفواتير المعالجة: {total_processed}")
        logger.info(f"✅ الفواتير المحفوظة بنجاح: {self.success_count['invoices']}")
        logger.info(f"✅ البنود المحفوظة بنجاح: {self.success_count['items']}")
        logger.info(f"❌ الفواتير الفاشلة: {self.error_count['invoices']}")
        logger.info(f"❌ البنود الفاشلة: {self.error_count['items']}")
        
        if self.errors:
            logger.error("🚨 الأخطاء المسجلة:")
            for i, error in enumerate(self.errors[:10], 1):  # أول 10 أخطاء
                logger.error(f"  {i}. {error}")
            if len(self.errors) > 10:
                logger.error(f"  ... و {len(self.errors) - 10} أخطاء أخرى")
        
        logger.info("=" * 60)

def main():
    """الدالة الرئيسية"""
    try:
        service = InvoiceSyncService()
        result = service.sync_invoices(branches=[1, 2], limit=50)
        
        if result["errors"]["invoices"] == 0 and result["errors"]["items"] == 0:
            logger.info("🎉 تمت المزامنة بنجاح بدون أخطاء!")
        else:
            logger.warning("⚠️ تمت المزامنة مع بعض الأخطاء. راجع السجلات للتفاصيل.")
        
        return result
        
    except Exception as e:
        logger.error(f"💥 خطأ فادح في المزامنة: {str(e)}")
        raise

if __name__ == "__main__":
    main()
