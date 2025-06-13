import os

BASE_URL = os.getenv("DAFTRA_URL")
API_KEY = os.getenv("DAFTRA_APIKEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADERS_DAFTRA = {"apikey": API_KEY}
HEADERS_SUPABASE = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

EXPECTED_TYPE = 0
PAGE_LIMIT = 100
BRANCH_IDS = [1, 2, 3]
