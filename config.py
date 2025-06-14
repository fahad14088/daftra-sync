import os

BASE_URL = os.getenv("DAFTRA_URL", "https://shadowpeace.daftara.com/api2" )
DAFTRA_API_KEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL", "").rstrip("/") + "/rest/v1"
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {
    "apikey": DAFTRA_API_KEY,
    "Content-Type": "application/json"
}

HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "return=minimal"
}

EXPECTED_TYPE = 0
PAGE_LIMIT = 50
BRANCH_IDS = [2, 3]

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "2"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
SAVE_FAILED_RECORDS = os.getenv("SAVE_FAILED_RECORDS", "true").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

TABLE_INVOICES = "invoices"
TABLE_INVOICE_ITEMS = "invoice_items"
