from datetime import datetime

def get_last_sync_time():
    # خله يرجع تاريخ قديم مؤقتًا عشان يجلب كل الفواتير من جديد
    return datetime(2024, 1, 1)

def update_sync_time(new_time: str):
    print(f"🔁 تم تحديث وقت التزامن إلى: {new_time}")
