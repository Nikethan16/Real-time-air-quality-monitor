import os
import sqlite3

# Single database file for all cities
DB_PATH = os.path.join(os.path.dirname(__file__), "air_quality.db")

def create_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    CREATE TABLE IF NOT EXISTS air_quality_data (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        city TEXT NOT NULL,
        datetime_utc TEXT NOT NULL,
        datetime_ist TEXT NOT NULL,
        temperature_2m REAL,
        relative_humidity_2m REAL,
        dew_point_2m REAL,
        apparent_temperature REAL,
        pressure_msl REAL,
        surface_pressure REAL,
        cloudcover REAL,
        windspeed_10m REAL,
        winddirection_10m REAL,
        pm10 REAL,
        pm2_5 REAL,
        carbon_monoxide REAL,
        carbon_dioxide REAL,
        nitrogen_dioxide REAL,
        sulphur_dioxide REAL,
        ozone REAL,
        uv_index REAL,
        uv_index_clear_sky REAL,
        ammonia REAL,
        methane REAL,
        UNIQUE(city, datetime_utc)  -- prevent duplicates for same city & timestamp
    );
    """)

    conn.commit()
    conn.close()
    print(f"✅ Database initialized at {DB_PATH}")

if __name__ == "__main__":
    create_table()
