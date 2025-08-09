# src/train_model.py
import sqlite3
import pandas as pd
from sklearn.ensemble import IsolationForest
import joblib
import sys

def get_historical_data(db_file):
    """Extracts historical data from the specified database."""
    conn = sqlite3.connect(db_file)
    # Select all columns and remove any rows where aqi is null
    query = "SELECT * FROM air_quality WHERE aqi IS NOT NULL"
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

def train_anomaly_model(df, model_file):
    """Trains an Isolation Forest model and saves it."""
    print("Training model...")
    # Select only the numerical features for training
    features = ['aqi', 'o3', 'co', 'so2', 'pm25', 'pm10']
    # Fill missing values with 0 before training
    df[features] = df[features].fillna(0)

    model = IsolationForest(contamination='auto', random_state=42)
    model.fit(df[features])
    joblib.dump(model, model_file)
    print(f"Model trained and saved to {model_file}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python train_model.py <city_name>")
        sys.exit(1)

    city_name = sys.argv[1].lower()
    db_file = f"db/{city_name}.db"
    model_file = f"models/{city_name}_model.joblib"

    historical_data = get_historical_data(db_file)
    if not historical_data.empty:
        train_anomaly_model(historical_data, model_file)
    else:
        print(f"No data available in {db_file} to train the model.")