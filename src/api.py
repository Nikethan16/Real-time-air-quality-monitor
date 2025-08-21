from fastapi import FastAPI, HTTPException
from .utils import fetch_data, fetch_city_predictions

app = FastAPI(title="Air Quality API", version="1.0")

@app.get("/")
def root():
    return {"message": "Air Quality API is running!"}

@app.get("/raw")
def get_raw_data(limit: int = 100):
    df = fetch_data("air_quality_data", limit=limit)
    return df.to_dict(orient="records")

@app.get("/predictions/{city}")
def get_city_predictions(city: str, limit: int = 100):
    df = fetch_city_predictions(city, limit=limit)
    if df.empty:
        raise HTTPException(status_code=404, detail=f"No predictions found for {city}")
    return df.to_dict(orient="records")
