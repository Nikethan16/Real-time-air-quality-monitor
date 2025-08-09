# src/dashboard.py

import streamlit as st
import pandas as pd
import requests
import time

# --- Dashboard Title and Configuration ---
st.set_page_config(page_title="Real-time Air Quality Dashboard", layout="wide")
st.title("Live Urban Air Quality Monitor")
st.markdown("---")

# --- Location Data ---
CITY_LOCATIONS = {
    'delhi': {'lat': 28.7041, 'lon': 77.1025},
    'hyderabad': {'lat': 17.3850, 'lon': 78.4867}
}

# --- Sidebar for City Selection ---
st.sidebar.header("Select a City")
selected_city = st.sidebar.selectbox("Choose a City:", list(CITY_LOCATIONS.keys()))

# --- Function to Fetch Data from API ---
@st.cache_data(ttl=5)
def get_latest_aqi_data(city):
    try:
        response = requests.get(f"http://localhost:8000/api/latest-aqi/{city}")
        response.raise_for_status()
        return pd.DataFrame(response.json())
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data from API: {e}")
        return pd.DataFrame()

# --- Main Dashboard Layout ---
def display_dashboard():
    st.header(f"Real-time Data for {selected_city.capitalize()}")
    df = get_latest_aqi_data(selected_city)

    if df.empty:
        st.warning("No data available from the API.")
        return

    latest_record = df.iloc[0]
    anomaly_score = latest_record['anomaly_score']

    # --- Display Metrics ---
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(label="Current AQI", value=f"{latest_record['aqi']}")
    with col2:
        st.metric(label="Anomaly Score", value=f"{anomaly_score:.2f}")
    with col3:
        if anomaly_score < 0:
            st.error("⚠️ **ANOMALY DETECTED** ⚠️")
        else:
            st.success("🟢 **Normal Reading** 🟢")

    # --- Display Charts ---
    st.markdown("---")
    st.subheader("Pollutant Levels")
    pollutants_df = pd.DataFrame({
        'Pollutant': ['O3', 'CO', 'SO2', 'PM2.5', 'PM10'],
        'Value': [latest_record['o3'], latest_record['co'], latest_record['so2'], latest_record['pm25'], latest_record['pm10']]
    })
    st.bar_chart(pollutants_df.set_index('Pollutant'))

    # --- Raw Data Table ---
    st.markdown("---")
    st.subheader("Raw Data Table")
    st.dataframe(df)

# --- Real-time Rerunning Loop ---
if __name__ == "__main__":
    while True:
        display_dashboard()
        time.sleep(5)
        st.rerun()