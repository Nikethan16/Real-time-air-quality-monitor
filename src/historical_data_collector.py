import os
import sqlite3
import requests
import sys
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

OPENWEATHERMAP_API_KEY = os.getenv('OPENWEATHERMAP_API_KEY')

# --- City Coordinates for OpenWeatherMap ---
CITIES_COORDINATES = {
    'delhi': {'lat': 28.7041, 'lon': 77.1025},
    'hyderabad': {'lat': 17.3850, 'lon': 78.4867}
}

def get_db_connection(city_name):
    db_file = f"db/{city_name}.db"
    return sqlite3.connect(db_file)

def fetch_historical_data(lat: float, lon: float, start_timestamp: int, end_timestamp: int):
    """Fetches historical air quality data from OpenWeatherMap."""
    url = f"http://api.openweathermap.org/data/2.5/air_pollution/history?lat={lat}&lon={lon}&start={start_timestamp}&end={end_timestamp}&appid={OPENWEATHERMAP_API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching historical data: {e}")
        return None

def insert_data_into_db(data: dict, city: str):
    """Inserts historical data into the specified city's database."""
    conn = get_db_connection(city)
    cursor = conn.cursor()

    try:
        for record in data['list']:
            aqi = record['main']['aqi']
            components = record['components']
            timestamp = datetime.fromtimestamp(record['dt']).strftime('%Y-%m-%d %H:%M:%S')

            cursor.execute("""
                INSERT INTO air_quality (city, aqi, o3, co, so2, pm25, pm10, timestamp)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (city, aqi, components.get('o3'), components.get('co'), components.get('so2'), components.get('pm2_5'), components.get('pm10'), timestamp))

        conn.commit()
        print(f"Successfully inserted {len(data['list'])} historical records for {city}.")
    except Exception as e:
        print(f"Error inserting historical data: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    if not OPENWEATHERMAP_API_KEY:
        raise ValueError("OPENWEATHERMAP_API_KEY environment variable not set.")
    if len(sys.argv) < 2:
        print("Usage: python historical_data_collector_v2.py <city_name>")
        sys.exit(1)

    city_name = sys.argv[1].lower()
    if city_name not in CITIES_COORDINATES:
        print(f"Error: Coordinates for '{city_name}' not found.")
        sys.exit(1)

    # Get timestamps for start of year to current date
    end_date = datetime.now()
    start_date = datetime(2025, 1, 1) # Start of the year
    end_timestamp = int(end_date.timestamp())
    start_timestamp = int(start_date.timestamp())

    coords = CITIES_COORDINATES[city_name]
    print(f"Fetching historical data for {city_name.capitalize()} from {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")

    historical_data = fetch_historical_data(coords['lat'], coords['lon'], start_timestamp, end_timestamp)
    if historical_data:
        insert_data_into_db(historical_data, city_name)