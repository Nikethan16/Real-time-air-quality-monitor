# src/api_pipeline.py
"""
Production-ready AQI pipeline:
- Fetches last 24h of raw rows from `air_quality_data` in Supabase for each city
- Computes CPCB-style AQI subindices & AQI
- Builds engineered features used at training
- Loads models (city-specific fallback to generic) and forecasts 1h/2h/3h
- Classifies AQI categories
- Detects anomalies (rolling z-score; IsolationForest fallback)
- Writes results into `aqi_results` (dynamic schema detection + upsert)
"""

import os
import sys
import math
import pickle
import logging
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from sklearn.ensemble import IsolationForest
from catboost import CatBoostRegressor

# ---- Init ----
load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_KEY = os.getenv("SUPABASE_SERVICE_KEY")

# Robust path to the models folder, assuming it's one level up from the src folder
MODELS_FOLDER = os.getenv("MODELS_FOLDER", os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'models')))

if not SUPABASE_URL or not SUPABASE_SERVICE_KEY:
    logging.error("SUPABASE_URL and SUPABASE_SERVICE_KEY must be set (env or .env).")
    sys.exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_KEY)

# expected raw columns in air_quality_data (your schema)
RAW_COLS = [
    "id", "city", "datetime_utc", "datetime_ist",
    "temperature_2m", "relative_humidity_2m", "dew_point_2m",
    "apparent_temperature", "pressure_msl", "surface_pressure",
    "cloudcover", "windspeed_10m", "winddirection_10m",
    "pm10", "pm2_5", "carbon_monoxide", "carbon_dioxide",
    "nitrogen_dioxide", "sulphur_dioxide", "ozone",
    "uv_index", "uv_index_clear_sky", "methane"
]

# -------------------------
# AQI: CPCB-style subindex helpers
# -------------------------
def _linear_subindex(C, bp):
    if C is None or (isinstance(C, float) and math.isnan(C)):
        return np.nan
    for Clow, Chigh, Ilow, Ihigh in bp:
        if Clow <= C <= Chigh:
            return ((Ihigh - Ilow) / (Chigh - Clow)) * (C - Clow) + Ilow
    return np.nan

def aqi_pm25(c):  # CPCB breakpoints µg/m³
    bp = [(0,30,0,50),(31,60,51,100),(61,90,101,200),(91,120,201,300),(121,250,301,400),(251,500,401,500)]
    return _linear_subindex(c, bp)

def aqi_pm10(c):
    bp = [(0,50,0,50),(51,100,51,100),(101,250,101,200),(251,350,201,300),(351,430,301,400),(431,600,401,500)]
    return _linear_subindex(c, bp)

def aqi_no2(c):
    bp = [(0,40,0,50),(41,80,51,100),(81,180,101,200),(181,280,201,300),(281,400,301,400),(401,600,401,500)]
    return _linear_subindex(c, bp)

def aqi_so2(c):
    bp = [(0,40,0,50),(41,80,51,100),(81,380,101,200),(381,800,201,300),(801,1600,301,400),(1601,2000,401,500)]
    return _linear_subindex(c, bp)

def aqi_co(c):
    bp = [(0.0,1.0,0,50),(1.1,2.0,51,100),(2.1,10.0,101,200),(10.1,17.0,201,300),(17.1,34.0,301,400),(34.1,50.0,401,500)]
    return _linear_subindex(c, bp)

def aqi_o3(c):
    bp = [(0,50,0,50),(51,100,51,100),(101,168,101,200),(169,208,201,300),(209,748,301,500),(749,1000,401,500)]
    return _linear_subindex(c, bp)

def aqi_category(aqi):
    if pd.isna(aqi): return None
    aqi = float(aqi)
    if aqi <= 50: return "Good"
    if aqi <= 100: return "Satisfactory"
    if aqi <= 200: return "Moderate"
    if aqi <= 300: return "Poor"
    if aqi <= 400: return "Very Poor"
    return "Severe"

# -------------------------
# Feature engineering (mirror training)
# -------------------------
def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # ensure datetime and sort
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    df = df.sort_values("datetime_utc").reset_index(drop=True)

    # pressure diff
    df["pressure_diff"] = df["pressure_msl"].astype(float) - df["surface_pressure"].astype(float)

    # sum gases (simple sum of measured gases)
    df["sum_gases"] = (
        df["carbon_monoxide"].astype(float).fillna(0) +
        df["carbon_dioxide"].astype(float).fillna(0) +
        df["nitrogen_dioxide"].astype(float).fillna(0) +
        df["sulphur_dioxide"].astype(float).fillna(0) +
        df["ozone"].astype(float).fillna(0) +
        df["methane"].astype(float).fillna(0)
    )

    # rolling means (24 entries window assumed hourly; min_periods=1 to allow partial windows)
    df["rolling_mean_pm2_5_24h"] = df["pm2_5"].astype(float).rolling(window=24, min_periods=1).mean()
    df["rolling_mean_pm10_24h"] = df["pm10"].astype(float).rolling(window=24, min_periods=1).mean()

    # pollutant ratio
    df["pollutant_ratio_pm2_5_pm10"] = df.apply(
        lambda r: (float(r["pm2_5"]) / float(r["pm10"])) if (pd.notna(r["pm2_5"]) and pd.notna(r["pm10"]) and float(r["pm10"]) != 0) else np.nan,
        axis=1
    )

    # temperature range
    if "temperature_2m" in df.columns and "dew_point_2m" in df.columns:
        df["temp_range"] = df["temperature_2m"].astype(float) - df["dew_point_2m"].astype(float)
        df["humidity_temp_interaction"] = df["relative_humidity_2m"].astype(float) * df["temp_range"].astype(float)
    else:
        df["temp_range"] = np.nan
        df["humidity_temp_interaction"] = np.nan

    # time features
    df["hour"] = df["datetime_utc"].dt.hour
    df["day_of_week"] = df["datetime_utc"].dt.weekday
    df["month"] = df["datetime_utc"].dt.month

    # wind category (low/medium/high): thresholds (m/s)
    if "windspeed_10m" in df.columns:
        df["wind_speed_category"] = pd.cut(
            df["windspeed_10m"].astype(float),
            bins=[-0.1, 2.5, 6, np.inf],
            labels=["low", "medium", "high"],
        )
    else:
        df["wind_speed_category"] = np.nan

    return df

# -------------------------
# AQI compute block
# -------------------------
def compute_aqi_block(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["pm2_5_index"] = df["pm2_5"].astype(float).apply(aqi_pm25)
    df["pm10_index"] = df["pm10"].astype(float).apply(aqi_pm10)
    df["nitrogen_dioxide_index"] = df["nitrogen_dioxide"].astype(float).apply(aqi_no2)
    df["sulphur_dioxide_index"] = df["sulphur_dioxide"].astype(float).apply(aqi_so2)
    df["carbon_monoxide_index"] = df["carbon_monoxide"].astype(float).apply(aqi_co)
    df["ozone_index"] = df["ozone"].astype(float).apply(aqi_o3)

    subcols = [
        "pm2_5_index",
        "pm10_index",
        "nitrogen_dioxide_index",
        "sulphur_dioxide_index",
        "carbon_monoxide_index",
        "ozone_index",
    ]
    df["aqi"] = df[subcols].max(axis=1, skipna=True)
    df["aqi_category"] = df["aqi"].apply(aqi_category)

    # dominant pollutant (column name)
    def _dominant(r):
        cand = {c: r.get(c, np.nan) for c in subcols}
        cand = {k: v for k, v in cand.items() if pd.notna(v)}
        return max(cand, key=cand.get) if cand else None

    df["dominant_pollutant"] = df.apply(_dominant, axis=1)
    return df

# -------------------------
# Model helpers
# -------------------------
def load_model(city: str, horizon: str):
    """
    Look for models in this order:
      - models/<city>_<horizon>_catboost.pkl
      - models/<city>_<horizon>_catboost.cbm
      - models/aqi_pred_<horizon>.pkl
      - models/aqi_pred_<horizon>.cbm
    Returns loaded model or None.
    """
    attempts = [
        f"{MODELS_FOLDER}/{city.lower()}_{horizon}_catboost.pkl",
        f"{MODELS_FOLDER}/{city.lower()}_{horizon}_catboost.cbm",
        f"{MODELS_FOLDER}/aqi_pred_{horizon}.pkl",
        f"{MODELS_FOLDER}/aqi_pred_{horizon}.cbm",
    ]
    for p in attempts:
        if os.path.exists(p):
            try:
                if p.endswith(".pkl"):
                    with open(p, "rb") as f:
                        return pickle.load(f)
                else:
                    m = CatBoostRegressor()
                    m.load_model(p)
                    return m
            except Exception as e:
                logging.warning("Failed to load model %s: %s", p, e)
    return None

def align_features_to_model(X: pd.DataFrame, model) -> pd.DataFrame:
    if model is None:
        return X.copy()
    X_al = X.copy()
    model_features = None
    if hasattr(model, "feature_names_") and getattr(model, "feature_names_", None):
        model_features = list(model.feature_names_)
    elif hasattr(model, "feature_names_in_"):
        model_features = list(model.feature_names_in_)
    if model_features is None:
        return X_al
    # drop extras & add missing with 0
    extra = [c for c in X_al.columns if c not in model_features]
    if extra:
        X_al = X_al.drop(columns=extra)
    missing = [c for c in model_features if c not in X_al.columns]
    for m in missing:
        X_al.loc[:, m] = 0.0
    X_al = X_al[model_features]
    return X_al

# -------------------------
# Anomaly helpers
# -------------------------
def rolling_z_anomaly(arr: np.ndarray, threshold=2.5, min_points=8):
    if len(arr) < min_points:
        return None
    s = pd.Series(arr)
    mean = s[:-1].mean()
    std = s[:-1].std(ddof=0)
    if std == 0 or np.isnan(std):
        return None
    z = (s.iloc[-1] - mean) / std
    return bool(abs(z) > threshold)

def isolation_anomaly(arr: np.ndarray, contamination=0.05):
    if len(arr) < 8:
        return None
    iso = IsolationForest(contamination=contamination, random_state=42)
    labels = iso.fit_predict(arr.reshape(-1, 1))
    return labels[-1] == -1

# -------------------------
# Dynamic schema fetch (best-effort)
# -------------------------
def get_table_columns(table_name: str) -> set:
    # Fetch one row to learn keys
    try:
        resp = supabase.table(table_name).select("*").limit(1).execute()
        if resp.data and isinstance(resp.data, list):
            return set(resp.data[0].keys())
    except Exception as e:
        logging.warning("Failed to get columns for %s, falling back to a safe set: %s", table_name, e)
    # Fallback to a conservative set of expected columns
    return {
        "id","pm10_index","nitrogen_dioxide_index","sulphur_dioxide_index",
        "carbon_monoxide_index","ozone_index","inserted_at","datetime_utc",
        "datetime_ist","aqi","aqi_1h_pred","aqi_2h_pred","aqi_3h_pred",
        "anomaly","pm2_5_index","city","aqi_1h_pred_category",
        "aqi_2h_pred_category","aqi_3h_pred_category",
        "dominant_pollutant","aqi_category"
    }

# -------------------------
# NEW: Robust unique-city discovery (paginates to avoid PostgREST page cap)
# -------------------------
def fetch_unique_cities(since_utc: datetime) -> list[str]:
    """Return a sorted unique list of cities with recent data (since_utc).
    
    This is much more efficient as it filters for recent records first.
    """
    cities: set[str] = set()
    page_size = 1000  # A reasonable page size for this query
    offset = 0

    while True:
        try:
            resp = (
                supabase
                .table("air_quality_data")
                .select("city")
                .gte("datetime_utc", since_utc.isoformat())
                .order("city")
                .range(offset, offset + page_size - 1)
                .execute()
            )
        except Exception as e:
            logging.error("Could not fetch city page: %s", e)
            break
        
        rows = resp.data or []
        if not rows:
            break

        for r in rows:
            c = (r.get("city") or "").strip()
            if c:
                cities.add(c)
        
        if len(rows) < page_size:
            break
            
        offset += page_size
    
    sorted_cities = sorted(cities)
    logging.info("Discovered %d cities with recent data: %s", len(sorted_cities), ", ".join(sorted_cities))
    return sorted_cities

# -------------------------
# Per-city processing
# -------------------------
def process_city(city: str, since_utc: datetime, valid_columns: set):
    try:
        resp = (
            supabase.table("air_quality_data")
            .select("*")
            .eq("city", city)
            .gte("datetime_utc", since_utc.isoformat())
            .order("datetime_utc", desc=True)
            .execute()
        )
    except Exception as e:
        logging.error("Supabase fetch error for %s: %s", city, e)
        return None

    rows = resp.data or []
    if not rows:
        logging.info("%s: no rows in last 24h", city)
        return None

    df = pd.DataFrame(rows)
    keep = [c for c in RAW_COLS if c in df.columns]
    if not keep:
        logging.warning("%s: none of expected raw cols present", city)
        return None
    df = df[keep].copy()

    # compute AQI block & features
    df = compute_aqi_block(df)
    df = engineer_features(df)

    # aggregate last 6h into feature vector (numeric mean)
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    if not numeric:
        logging.warning("%s: no numeric columns to aggregate", city)
        return None
    X_agg = df[numeric].mean(numeric_only=True).to_frame().T

    # attach last timestamp
    latest_ts = pd.to_datetime(df["datetime_utc"].iloc[-1], utc=True)

    # include city one-hot column name style used in training if any
    X_for_model = X_agg.copy()
    X_for_model[f"city_{city}"] = 1

    preds: dict[str, float] = {}
    preds_cat: dict[str, str | None] = {}

    # load & predict for each horizon
    for horizon, col_name in [("h1","aqi_1h_pred"),("h2","aqi_2h_pred"),("h3","aqi_3h_pred")]:
        model = load_model(city, horizon)
        if model is None:
            logging.info("%s: model for %s not found", city, horizon)
            continue
        X_model = align_features_to_model(X_for_model, model)
        try:
            yhat = model.predict(X_model)
            val = float(yhat[0]) if hasattr(yhat, "__len__") else float(yhat)
            preds[col_name] = max(0.0, val)
            preds_cat[f"{col_name}_category"] = aqi_category(val)
        except Exception as e:
            logging.warning("%s: predicting %s failed: %s", city, horizon, e)
            continue

    if not preds:
        logging.warning("%s: no predictions produced", city)
        return None

    # anomaly detection on recent computed aqi series
    recent_aqi = df["aqi"].dropna().values
    anom_flag = rolling_z_anomaly(recent_aqi, threshold=2.5, min_points=8)
    if anom_flag is None:
        iso_flag = isolation_anomaly(recent_aqi, contamination=0.05)
        anom_flag = bool(iso_flag) if iso_flag is not None else None

    latest_actual = df["aqi"].iloc[-1] if not df["aqi"].isna().all() else None
    latest_cat = aqi_category(latest_actual) if latest_actual is not None else None
    latest_dom = df["dominant_pollutant"].iloc[-1] if "dominant_pollutant" in df.columns else None

    out = {
        "city": city,
        "datetime_utc": latest_ts.isoformat(),
        "datetime_ist": latest_ts.tz_convert("Asia/Kolkata").isoformat(),
        "pm10_index": float(df["pm10_index"].iloc[-1]) if pd.notna(df["pm10_index"].iloc[-1]) else None,
        "pm2_5_index": float(df["pm2_5_index"].iloc[-1]) if pd.notna(df["pm2_5_index"].iloc[-1]) else None,
        "nitrogen_dioxide_index": float(df["nitrogen_dioxide_index"].iloc[-1]) if "nitrogen_dioxide_index" in df.columns and pd.notna(df["nitrogen_dioxide_index"].iloc[-1]) else None,
        "sulphur_dioxide_index": float(df["sulphur_dioxide_index"].iloc[-1]) if "sulphur_dioxide_index" in df.columns and pd.notna(df["sulphur_dioxide_index"].iloc[-1]) else None,
        "carbon_monoxide_index": float(df["carbon_monoxide_index"].iloc[-1]) if "carbon_monoxide_index" in df.columns and pd.notna(df["carbon_monoxide_index"].iloc[-1]) else None,
        "ozone_index": float(df["ozone_index"].iloc[-1]) if "ozone_index" in df.columns and pd.notna(df["ozone_index"].iloc[-1]) else None,
        "aqi": float(latest_actual) if latest_actual is not None else None,
        "aqi_category": latest_cat,
        "dominant_pollutant": latest_dom,
        "anomaly": int(anom_flag) if anom_flag is not None else None,
        "inserted_at": datetime.now(timezone.utc).isoformat(),
    }

    # attach predictions and categories
    for k, v in preds.items():
        out[k] = v
    for k, v in preds_cat.items():
        out[k] = v

    # attach last raw pollutant values (optional contextual columns)
    for col in ["pm2_5","pm10","nitrogen_dioxide","sulphur_dioxide","carbon_monoxide","ozone"]:
        if col in df.columns:
            out[f"latest_{col}"] = float(df[col].iloc[-1]) if pd.notna(df[col].iloc[-1]) else None

    # filter to only valid columns
    out_filtered = {k: v for k, v in out.items() if k in valid_columns}

    return out_filtered

# -------------------------
# Orchestration
# -------------------------
def run_pipeline():
    logging.info("Starting AQI pipeline")

    valid_columns = get_table_columns("aqi_results")
    logging.info("aqi_results columns (sample): %s", sorted(list(valid_columns))[:20])

    # last 24 hours
    since_utc = datetime.now(timezone.utc) - timedelta(hours=24)
    
    # fetch distinct city list from raw table (robust against pagination limits)
    try:
        cities = fetch_unique_cities(since_utc)
    except Exception as e:
        logging.error("Could not fetch city list: %s", e)
        return

    if not cities:
        logging.warning("No cities with recent data found to process.")
        return

    rows_to_write = []
    for city in cities:
        logging.info("Processing city: %s", city)
        try:
            out = process_city(city, since_utc, valid_columns)
            if out:
                rows_to_write.append(out)
        except Exception as e:
            logging.exception("Processing failed for %s: %s", city, e)

    if not rows_to_write:
        logging.info("No rows to write.")
        return

    # write to supabase (one-by-one upsert with insert fallback)
    for row in rows_to_write:
        try:
            supabase.table("aqi_results").upsert(row).execute()
            logging.info("Upserted: %s @ %s", row.get("city"), row.get("datetime_utc"))
        except Exception as e:
            logging.warning("Upsert failed for %s: %s. Trying insert.", row.get("city"), e)
            try:
                supabase.table("aqi_results").insert(row).execute()
                logging.info("Inserted: %s @ %s", row.get("city"), row.get("datetime_utc"))
            except Exception as e2:
                logging.error("Insert failed for %s: %s", row.get("city"), e2)

    logging.info("Pipeline complete.")

# -------------------------
# Entrypoint
# -------------------------
if __name__ == "__main__":
    run_pipeline()