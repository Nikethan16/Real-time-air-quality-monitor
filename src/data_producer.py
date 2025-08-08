# src/data_producer.py
import os
import json
import time
import requests
import sqlite3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Database file path
DB_FILE = 'db/aqi.db'

def get_air_quality_data(city: str):
    """Fetches real-time air quality data from the AQICN API."""
    api_key = os.getenv('AQICN_API_KEY')
    if not api_key:
        raise ValueError("AQICN_API_KEY environment variable not set.")

    base_url = "https://api.waqi.info/feed/"
    url = f"{base_url}{city}/?token={api_key}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data for {city}: {e}")
        return None

def insert_data(data: dict, city: str):
    """Inserts a new data record into the SQLite database."""
    if not data or data.get('status') != 'ok':
        print("Received bad data, skipping database insert.")
        return

    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()

    try:
        aqi = data['data']['aqi']
        iaqi = data['data']['iaqi']
        timestamp = data['data']['time']['s']

        # Use .get() with a default value to handle missing pollutant data
        o3 = iaqi.get('o3', {}).get('v')
        co = iaqi.get('co', {}).get('v')
        so2 = iaqi.get('so2', {}).get('v')
        pm25 = iaqi.get('pm25', {}).get('v')
        pm10 = iaqi.get('pm10', {}).get('v')

        cursor.execute("""
            INSERT INTO air_quality (city, aqi, o3, co, so2, pm25, pm10, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (city, aqi, o3, co, so2, pm25, pm10, timestamp))

        conn.commit()
        print(f"[{timestamp}] Inserted data for {city} into DB.")
    except Exception as e:
        print(f"Error inserting data for {city}: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    cities = ['delhi', 'hyderabad', 'banglore']
    print("Starting data producer. Press Ctrl+C to stop.")
    while True:
        for city in cities:
            aqi_data = get_air_quality_data(city)
            insert_data(aqi_data, city)  # Call the new function
        time.sleep(60)