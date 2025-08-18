# fetch_from_supabase.py

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
supabase_url = os.getenv("SUPABASE_URL")
supabase_key = os.getenv("SUPABASE_SERVICE_KEY")

supabase = create_client(supabase_url, supabase_key)

# Config
TABLE_NAME = "air_quality_data"
BATCH_SIZE = 1000

print("📥 Fetching data from Supabase in batches...")
all_data = []
offset = 0

while True:
    # Get rows in batches
    response = supabase.table(TABLE_NAME).select("*").range(offset, offset + BATCH_SIZE - 1).execute()
    
    if not response.data:  # No more rows
        break
    
    all_data.extend(response.data)
    offset += BATCH_SIZE
    print(f"✅ Retrieved {len(all_data)} rows so far...")

# Convert to DataFrame
df = pd.DataFrame(all_data)
print(f"🎯 Total rows fetched: {len(df)}")

# Sort by datetime_utc if present
if "datetime_utc" in df.columns:
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], errors="coerce")
    df.sort_values(by="datetime_utc", inplace=True)

# Save locally in Parquet format
output_file = "air_quality_raw.parquet"
df.to_parquet(output_file, index=False)
print(f"💾 Saved data locally to {output_file}")
