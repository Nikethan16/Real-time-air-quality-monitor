import os
import pandas as pd
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

def get_supabase_client() -> Client:
    return create_client(SUPABASE_URL, SUPABASE_KEY)

def fetch_data(table: str, limit: int = 1000) -> pd.DataFrame:
    """Fetch data from a Supabase table"""
    supabase = get_supabase_client()
    data = (
        supabase.table(table)
        .select("*")
        .order("datetime_utc", desc=True)
        .limit(limit)
        .execute()
    )
    if not data.data:
        return pd.DataFrame()
    return pd.DataFrame(data.data)

def fetch_city_predictions(city: str, limit: int = 100) -> pd.DataFrame:
    """Fetch AQI results for a city"""
    df = fetch_data("aqi_results", limit=limit)
    if df.empty:
        return df
    return df[df["city"].str.lower() == city.lower()]
