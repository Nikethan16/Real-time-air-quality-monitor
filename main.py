from src.data_fetcher import fetch_aqi

city = "Hyderabad"
result = fetch_aqi(city)
if result:
    print(f"AQI in {result['city']}: {result['aqi']} at {result['timestamp']}")