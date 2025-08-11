import sqlite3
from supabase import create_client, Client
import os
from dotenv import load_dotenv

load_dotenv()

# ========================
# CONFIG
# ========================
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "db", "air_quality.db")
SUPABASE_URL = os.getenv("SUPABASE_URL")  # Set in .env
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")  # Set in .env
TABLE_NAME = "air_quality_data"
BATCH_SIZE = 500

# ========================
# CONNECT
# ========================
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# ========================
# GET DATA
# ========================
cursor.execute("SELECT * FROM air_quality_data")
rows = cursor.fetchall()
columns = [desc[0] for desc in cursor.description]

print(f"🗂 Columns in table: {columns}")
print(f"📦 Found {len(rows)} records in local DB.")

# Remove 'id' from columns
if "id" in columns:
    id_index = columns.index("id")
else:
    id_index = None

# ========================
# INSERT INTO SUPABASE
# ========================
def insert_batch(batch):
    # Convert tuple to dict (skip id)
    records = []
    for row in batch:
        if id_index is not None:
            row = row[:id_index] + row[id_index+1:]
        records.append(dict(zip([c for c in columns if c != "id"], row)))

    # Check existing records to prevent duplicates
    timestamps = [rec["datetime_utc"] for rec in records]
    cities = [rec["city"] for rec in records]

    existing = supabase.table(TABLE_NAME) \
        .select("city,datetime_utc") \
        .in_("datetime_utc", timestamps) \
        .in_("city", cities) \
        .execute()

    existing_pairs = {(r["city"], r["datetime_utc"]) for r in existing.data}

    new_records = [rec for rec in records if (rec["city"], rec["datetime_utc"]) not in existing_pairs]

    if new_records:
        supabase.table(TABLE_NAME).insert(new_records).execute()
        print(f"✅ Inserted {len(new_records)} new rows.")
    else:
        print("⏩ Skipped batch (all duplicates).")

# ========================
# PROCESS IN BATCHES
# ========================
for i in range(0, len(rows), BATCH_SIZE):
    batch = rows[i:i+BATCH_SIZE]
    insert_batch(batch)

print("🎉 Migration complete!")
