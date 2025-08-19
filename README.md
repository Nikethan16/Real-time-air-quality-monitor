🌫️ Real-Time Air Quality Monitoring & Forecasting Dashboard

📌 Project Overview

Air pollution is a major concern in modern cities, directly impacting public health and quality of life. This project provides a real-time air quality monitoring system that:

Collects raw environmental data (via Open-Meteo API
).

Computes Air Quality Index (AQI) for multiple cities.

Uses a pre-trained CatBoost model to forecast AQI for +1h, +2h, and +3h horizons.

Detects anomalies in pollution spikes.

Visualizes results in an interactive Streamlit dashboard with charts, trends, and maps.

🎯 Key Features

✅ Automated Data Pipeline

GitHub Actions workflows collect data every 2 hours and store it in Supabase.

AQI values and forecasts are computed and updated automatically.

✅ Machine Learning Forecasting

Uses a CatBoost regression model trained on historical pollutant and weather data.

Provides short-term AQI forecasts (+1h, +2h, +3h).

✅ Anomaly Detection

Identifies unusual spikes in AQI, highlighting potential pollution events.

✅ Interactive Dashboard (built with Streamlit + Plotly)

Live AQI trends per city.

Historical trends with anomaly highlights.

Pollutant-level breakdown (PM2.5, PM10, etc.).

City comparison on a geographic map.

Export options for CSV/Excel.

🛠️ Tech Stack

Data Collection: Open-Meteo API, GitHub Actions

Data Storage: Supabase (PostgreSQL)

Machine Learning: CatBoost, Scikit-learn

Dashboard: Streamlit, Plotly

Automation: GitHub Actions CI/CD

Environment: Python 3.10+


🚀 Deployment

This dashboard is deployed on Streamlit Cloud.
You can view the live version here:

https://real-time-air-quality-monitor.streamlit.app/

⚙️ Local Setup

Clone repo

git clone https://github.com/Nikethan16/Real-time-air-quality-monitor

cd real-time-air-quality-monitor


Install dependencies

pip install -r requirements.txt


Set environment variables
Create .env file:

SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_KEY=your_supabase_service_key


Run Streamlit dashboard

streamlit run src/dashboard.py

📊 Real-World Usefulness

Public Health Awareness → Citizens can check current AQI and avoid outdoor activity during high pollution.

Government & NGOs → Early alerts for pollution spikes and anomaly detection.

Smart Cities → Integration into IoT systems for adaptive traffic control or industrial monitoring.

Students/Researchers → Hands-on example of end-to-end ML pipeline with real-world impact.

👨‍💻 Author

Developed by Nikethan

🌐 LinkedIn - https://www.linkedin.com/in/srinikethan-chundru/

 | GitHub - https://github.com/Nikethan16

🚀 Always exploring data-driven solutions for real-world problems.

📜 License

This project is licensed under the MIT License – free to use and adapt.

⚡ In short: This project shows how machine learning + real-time data pipelines + dashboards can be combined to make pollution monitoring accessible, actionable, and scalable.