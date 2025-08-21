#  Real-Time Air Quality Monitor & Forecast Dashboard 

## 🎥 Demo

- 🌍 **Live Dashboard** → [Streamlit App](https://real-time-air-quality-monitor.streamlit.app/)
  
- 🔗 **Github** → [GitHub Repo](https://github.com/Nikethan16/Real-time-air-quality-monitor)

---

##  Overview

This project delivers a real-time air quality monitoring system with the following capabilities:

- **Automated ETL Pipeline**: GitHub Actions fetches live environmental data from Open-Meteo and stores it in Supabase.
- **Data Processing & Forecasting**: Calculates AQI, classifies categories, forecasts +1h/+2h/+3h using CatBoost, detects anomalies, and stores results in Supabase.
- **Dashboard Visualization**: Interactive Streamlit app with real-time KPIs, trends, pollutant breakdowns, maps, and export options.
- **Future-Ready API & Containerization**: Includes a Dockerized FastAPI backend for exposing AQI data to external applications or microservices.
- **Robust CI/CD**: GitHub Actions automates data ingestion, ML inference, and dashboard deployment.

> “Many popular projects’ README use lists, visual aids, and contribution guidelines — these improve comprehension and adoption.” :contentReference[oaicite:0]{index=0}

---

##  Table of Contents

- [Features](#-features)  
- [Architecture](#-architecture)  
- [Tech Stack](#-tech-stack)  
- [Demo](#-demo)  
- [Getting Started](#-getting-started)  
  - [1. Clone & Setup](#1-clone--setup)  
  - [2. Environment](#2-environment)  
  - [3. Run Dashboard](#3-run-dashboard)  
  - [4. (Optional) Run FastAPI](#4-optional-run-fastapi)  
  - [5. (Optional) Docker](#5-optional-docker)  
- [Usage Examples](#-usage-examples)  
- [Potential Use Cases](#-real-world-use-cases)  
- [Contribute](#-contribute)  
- [Author & License](#-author--license)

---

##  Features

- ⏱ **Live Data Ingestion**: Automatically collects data from Open-Meteo every 2 hours via GitHub Actions.
-  **AQI & Forecasting**: Uses CatBoost to compute AQI and short-term forecasts.
-  **Anomaly Detection**: Flags sudden spikes in air pollution for alerting.
-  **Rich Dashboard**:  
  - Real-time AQI metrics  
  - City-level comparisons and maps  
  - Pollutant trends and exportable data  
-  **API Server (FastAPI)**: Provides endpoints for `AQI`, pollutant breakdowns, and health checks.
-  **Dockerized Deployment**: Easily run the dashboard + API together anywhere using Docker.

---

##  Architecture

```text
[ Open-Meteo API ] → GitHub Actions ETL → [ Supabase DB ]
                                ↓
                            Processing → [ aqi_results table ]
                                ↓                  ↓
                     Streamlit Dashboard     FastAPI REST API (Dockerized)

```
---


## 🛠 Tech Stack
| **Component**       | **Technology**                          |
| ------------------- | --------------------------------------- |
| **Data Collection** | Open-Meteo API, GitHub Actions          |
| **Storage**         | Supabase (PostgreSQL)                   |
| **Processing**      | Python, CatBoost, Scikit-learn          |
| **Dashboard**       | Streamlit, Plotly                       |
| **API Backend**     | FastAPI, Uvicorn                        |
| **Deployment**      | Docker, GitHub Actions, Streamlit Cloud |
| **Utilities**       | Pandas, Numpy, python-dotenv, Humanize  |

---

## ⚡Getting Started
### 1. Clone & Setup
```bash
git clone https://github.com/Nikethan16/Real-time-air-quality-monitor.git
cd Real-time-air-quality-monitor
```
### 2. Environment

Create a .env file with Supabase credentials:
```bash
SUPABASE_URL=your_supabase_url
SUPABASE_SERVICE_KEY=your_supabase_service_key
```
Install Python dependencies:
```bash
pip install -r requirements.txt
```

### 3. Run Dashboard
```bash
streamlit run src/dashboard.py
```

### 4. (Optional) Run FastAPI
```bash 
uvicorn src.api:app --reload --port 8000 
```
Visit → [FastApi](http://localhost:8000/docs) to explore the API.

### 5. (Optional) Docker
```bash
docker build -t air-quality-app .
docker run -p 8501:8501 -p 8000:8000 --env-file .env air-quality-app
```
Streamlit Dashboard → [Streamlit](http://localhost:8501/)

FastAPI Docs → [FastApi](http://localhost:8000/docs/)

---

## 📊 Usage Examples

* View live **AQI trends** & anomalies for cities like Delhi & Mumbai.
* Export **CSV/Excel data** for research or reporting.
* Integrate **FastAPI** into a mobile app or IoT device for real-time AQI.

---

## 🌍 Real-World Use Cases

* 🏥 Public Health → Citizens & policymakers can track air quality & respond quickly.
* 🏙️ Smart Cities → Integrate alerts into IoT systems & urban dashboards.
* 🎓 Research → Open dataset + ML forecasts for environmental studies.
* 📺 Smart Displays → Embed interactive maps & AQI KPIs in smart city portals.

---


## 🤝Contribute

Contributions are welcome 🎉

* Open issues / feature requests
* Submit PRs for improvements (models, UI, API, docs)

## 🧑‍💻 Author & License

👤 Srinikethan Chandru

* [LinkedIn](https://www.linkedin.com/in/srinikethan-chundru/) ↗️
* [GitHub](https://github.com/Nikethan16) ↗️

📜 Licensed under the MIT License – free to use & adapt.