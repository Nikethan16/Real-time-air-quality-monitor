import os
import time
import requests
from supabase import create_client, Client
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta

# --------------------
# CONFIG
# --------------------
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
TABLE_NAME = "air_quality_data"

CITIES = {
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Hyderabad": (17.3850, 78.4867),
}

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

now_utc = datetime.now(timezone.utc)
start_utc = now_utc - timedelta(hours=24)


# --------------------
# Retry-enabled Fetch Function
# --------------------
def fetch_with_retry(url, params, retries=3, delay=5, label="API"):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, params=params, timeout=15)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.Timeout:
            print(f"⏳ {label} timeout (attempt {attempt}/{retries})")
        except requests.exceptions.RequestException as e:
            print(f"⚠️ {label} fetch error: {e} (attempt {attempt}/{retries})")
        time.sleep(delay)
    return None


# --------------------
# API calls
# --------------------
def fetch_weather(lat, lon, start, end):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "pressure_msl",
            "surface_pressure",
            "cloudcover",
            "windspeed_10m",
            "winddirection_10m",
            "uv_index",
            "uv_index_clear_sky"
        ]),
        "start": start.isoformat(timespec="minutes"),
        "end": end.isoformat(timespec="minutes"),
        "timezone": "UTC"
    }
    return fetch_with_retry(url, params, label="Weather")


def fetch_pollutants(lat, lon, start, end):
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join([
            "pm10",
            "pm2_5",
            "carbon_monoxide",
            "carbon_dioxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",
            "methane"
        ]),
        "start": start.isoformat(timespec="minutes"),
        "end": end.isoformat(timespec="minutes"),
        "timezone": "UTC"
    }
    return fetch_with_retry(url, params, label="Pollutants")


# --------------------
# Merge Weather & Pollutants
# --------------------
def merge_data(city, weather, pollutants):
    now_utc_str = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    timestamps_weather = weather["hourly"]["time"]
    timestamps_pollutants = pollutants["hourly"]["time"]

    valid_times = sorted(
        t for t in set(timestamps_weather) & set(timestamps_pollutants)
        if t <= now_utc_str
    )

    merged_records = []
    for t in valid_times:
        idx_w = timestamps_weather.index(t)
        idx_p = timestamps_pollutants.index(t)
        merged_records.append({
            "city": city,
            "datetime_utc": t,
            "datetime_ist": (
                datetime.fromisoformat(t.replace("Z", "+00:00"))
                .astimezone(timezone(timedelta(hours=5, minutes=30)))
                .isoformat()
            ),
            "temperature_2m": weather["hourly"]["temperature_2m"][idx_w],
            "relative_humidity_2m": weather["hourly"]["relative_humidity_2m"][idx_w],
            "dew_point_2m": weather["hourly"]["dew_point_2m"][idx_w],
            "apparent_temperature": weather["hourly"]["apparent_temperature"][idx_w],
            "pressure_msl": weather["hourly"]["pressure_msl"][idx_w],
            "surface_pressure": weather["hourly"]["surface_pressure"][idx_w],
            "cloudcover": weather["hourly"]["cloudcover"][idx_w],
            "windspeed_10m": weather["hourly"]["windspeed_10m"][idx_w],
            "winddirection_10m": weather["hourly"]["winddirection_10m"][idx_w],
            "pm10": pollutants["hourly"]["pm10"][idx_p],
            "pm2_5": pollutants["hourly"]["pm2_5"][idx_p],
            "carbon_monoxide": pollutants["hourly"]["carbon_monoxide"][idx_p],
            "carbon_dioxide": pollutants["hourly"]["carbon_dioxide"][idx_p],
            "nitrogen_dioxide": pollutants["hourly"]["nitrogen_dioxide"][idx_p],
            "sulphur_dioxide": pollutants["hourly"]["sulphur_dioxide"][idx_p],
            "ozone": pollutants["hourly"]["ozone"][idx_p],
            "uv_index": weather["hourly"]["uv_index"][idx_w],
            "uv_index_clear_sky": weather["hourly"]["uv_index_clear_sky"][idx_w],
            "methane": pollutants["hourly"]["methane"][idx_p],
        })
    return merged_records


# --------------------
# Insert into Supabase
# --------------------
def insert_if_new(records):
    new_recs = []
    for rec in records:
        exists = supabase.table(TABLE_NAME).select("id").eq("city", rec["city"]).eq("datetime_utc", rec["datetime_utc"]).execute()
        if len(exists.data) == 0:
            new_recs.append(rec)
    if new_recs:
        supabase.table(TABLE_NAME).insert(new_recs).execute()
    return len(new_recs), len(records)


# --------------------
# MAIN SCRIPT
# --------------------
print(f"🚀 Starting live data update ({start_utc.isoformat()} → {now_utc.isoformat()})")

for city, (lat, lon) in CITIES.items():
    for attempt in range(1, 4):  # Up to 3 retries per city
        print(f"\n📡 Fetching data for {city} (attempt {attempt}/3)...")
        weather = fetch_weather(lat, lon, start_utc, now_utc)
        pollutants = fetch_pollutants(lat, lon, start_utc, now_utc)

        if weather and pollutants:
            merged = merge_data(city, weather, pollutants)
            inserted, checked = insert_if_new(merged)
            print(f"✅ {city}: {inserted} new records inserted ({checked} checked)")
            break  # Success, move to next city
        else:
            print(f"⚠️ {city} fetch failed, retrying...")
            time.sleep(5)

    else:
        print(f"❌ Skipping {city} after 3 failed attempts.")

print("\n🎉 Live data collection completed!")
