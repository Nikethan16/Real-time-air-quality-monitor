# src/main.py

import uvicorn
import sqlite3
import pandas as pd
import joblib
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict

# --- Configuration ---
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_FILE_DELHI = os.path.join(PROJECT_ROOT, '../db/delhi.db')
DB_FILE_HYDERABAD = os.path.join(PROJECT_ROOT, '../db/hyderabad.db')
MODEL_FILE_DELHI = os.path.join(PROJECT_ROOT, '../models/delhi_model.joblib')
MODEL_FILE_HYDERABAD = os.path.join(PROJECT_ROOT, '../models/hyderabad_model.joblib')

# --- Pydantic Model for API Response ---
class AirQualityRecord(BaseModel):
    id: int
    city: str
    aqi: int
    o3: Optional[float]
    co: Optional[float]
    so2: Optional[float]
    pm25: Optional[float]
    pm10: Optional[float]
    timestamp: str
    anomaly_score: float

# --- API App and Model Loading ---
app = FastAPI()
try:
    MODELS: Dict[str, joblib] = {
        'delhi': joblib.load(MODEL_FILE_DELHI),
        'hyderabad': joblib.load(MODEL_FILE_HYDERABAD)
    }
    print("Anomaly detection models loaded successfully.")
except FileNotFoundError:
    MODELS = {}
    print("Warning: Anomaly detection models not found. Run train_model.py for each city.")

def get_db_connection(city: str):
    """Creates and returns a database connection based on city."""
    city = city.lower()
    if city == 'delhi':
        db_path = DB_FILE_DELHI
    elif city == 'hyderabad':
        db_path = DB_FILE_HYDERABAD
    else:
        raise ValueError(f"Database not found for city: {city}")
    
    if not os.path.exists(db_path):
        raise HTTPException(status_code=500, detail=f"Database file not found for city: {city}")

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

@app.get("/api/latest-aqi/{city}", response_model=List[AirQualityRecord])
def get_latest_aqi(city: str):
    try:
        conn = get_db_connection(city)
        cursor = conn.cursor()

        query = """
            SELECT * FROM air_quality
            ORDER BY timestamp DESC
            LIMIT 1;
        """
        cursor.execute(query)
        latest_record = cursor.fetchone()
        conn.close()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    if latest_record is None:
        return []
    
    latest_record_dict = [dict(latest_record)]

    if city.lower() in MODELS:
        model = MODELS[city.lower()]
        df = pd.DataFrame(latest_record_dict)
        
        features = ['aqi', 'o3', 'co', 'so2', 'pm25', 'pm10']
        df[features] = df[features].fillna(0)
        
        df['anomaly_score'] = model.decision_function(df[features])
        
        for record in latest_record_dict:
            record['anomaly_score'] = df.loc[df['city'] == record['city'], 'anomaly_score'].iloc[0]

    return latest_record_dict

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)