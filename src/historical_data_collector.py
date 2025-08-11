import os
import sqlite3
import requests
import pandas as pd
from datetime import datetime

# === CONFIG ===
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "db", "air_quality.db")
CITIES = {
    "Delhi": {"lat": 28.7041, "lon": 77.1025},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867}
}
START_DATE = "2025-05-10"
END_DATE = "2025-08-10"

# === API ENDPOINTS ===
WEATHER_API = "https://archive-api.open-meteo.com/v1/archive"
POLLUTANTS_API = "https://air-quality-api.open-meteo.com/v1/air-quality"


def fetch_weather_data(city, lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "pressure_msl",
            "surface_pressure",
            "cloudcover",
            "windspeed_10m",
            "winddirection_10m"
        ],
        "timezone": "Asia/Kolkata"
    }
    r = requests.get(WEATHER_API, params=params)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame(data["hourly"])
    df.rename(columns={"time": "datetime_ist"}, inplace=True)
    df["datetime_utc"] = pd.to_datetime(df["datetime_ist"]).dt.tz_localize("Asia/Kolkata").dt.tz_convert("UTC")
    df["datetime_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["city"] = city
    return df


def fetch_pollutants_data(city, lat, lon):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": [
            "pm10",
            "pm2_5",
            "carbon_monoxide",
            "carbon_dioxide",
            "nitrogen_dioxide",
            "sulphur_dioxide",
            "ozone",
            "uv_index",
            "uv_index_clear_sky",
            "ammonia",
            "methane"
        ],
        "timezone": "Asia/Kolkata"
    }
    r = requests.get(POLLUTANTS_API, params=params)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame(data["hourly"])
    df.rename(columns={"time": "datetime_ist"}, inplace=True)
    df["datetime_utc"] = pd.to_datetime(df["datetime_ist"]).dt.tz_localize("Asia/Kolkata").dt.tz_convert("UTC")
    df["datetime_utc"] = df["datetime_utc"].dt.strftime("%Y-%m-%d %H:%M:%S")
    df["city"] = city
    return df


def insert_into_db(df):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cols = [
        "city", "datetime_utc", "datetime_ist",
        "temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature",
        "pressure_msl", "surface_pressure", "cloudcover", "windspeed_10m", "winddirection_10m",
        "pm10", "pm2_5", "carbon_monoxide", "carbon_dioxide", "nitrogen_dioxide",
        "sulphur_dioxide", "ozone", "uv_index", "uv_index_clear_sky", "ammonia", "methane"
    ]

    insert_query = f"""
    INSERT OR IGNORE INTO air_quality_data
    ({", ".join(cols)})
    VALUES ({", ".join(["?"] * len(cols))});
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, [row.get(col) for col in cols])

    conn.commit()
    conn.close()


if __name__ == "__main__":
    all_data = []

    for city, coords in CITIES.items():
        print(f"📥 Fetching weather data for {city}...")
        weather_df = fetch_weather_data(city, coords["lat"], coords["lon"])
        print(f"📥 Fetching pollutants data for {city}...")
        pollutants_df = fetch_pollutants_data(city, coords["lat"], coords["lon"])

        # Merge on city + datetime_utc
        merged_df = pd.merge(
            weather_df, pollutants_df,
            on=["city", "datetime_utc", "datetime_ist"],
            how="outer"
        )

        print(f"✅ {city}: {len(merged_df)} rows")
        insert_into_db(merged_df)

    print("🎯 Data collection complete. All records stored in SQLite DB.")
