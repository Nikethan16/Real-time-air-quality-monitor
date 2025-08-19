# dashboard.py
import os
from io import BytesIO
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from dotenv import load_dotenv
from streamlit_autorefresh import st_autorefresh
from supabase import create_client
import humanize

# -------------------------
# Config / init
# -------------------------
st.set_page_config(page_title="Air Quality Dashboard", page_icon="🌫️", layout="wide")
load_dotenv()

# Prefer Streamlit Cloud secrets; fallback to environment for local dev
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Missing Supabase credentials. Set SUPABASE_URL and SUPABASE_KEY (or SUPABASE_SERVICE_KEY).")
    st.stop()

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# City coordinates (for map)
CITY_COORDS = {
    "Delhi": (28.6139, 77.2090),
    "Mumbai": (19.0760, 72.8777),
    "Hyderabad": (17.3850, 78.4867),
}

# -----------------
# Helpers
# -----------------
def get_aqi_category(aqi_val) -> tuple[str, str, str]:
    """
    Return (category, rgba_color, advisory_message) for an AQI value.
    Accepts None/NaN safely.
    """
    try:
        aqi = 0 if aqi_val is None or (isinstance(aqi_val, float) and np.isnan(aqi_val)) else int(round(float(aqi_val)))
    except Exception:
        aqi = 0

    if aqi <= 50:
        return "Good", "rgba(34,197,94,1)", "Air quality is considered safe."
    elif aqi <= 100:
        return "Satisfactory", "rgba(132,204,22,1)", "Minor breathing discomfort possible."
    elif aqi <= 200:
        return "Moderate", "rgba(250,204,21,1)", "Sensitive groups should limit prolonged exertion."
    elif aqi <= 300:
        return "Poor", "rgba(249,115,22,1)", "Breathing discomfort for most people."
    elif aqi <= 400:
        return "Very Poor", "rgba(239,68,68,1)", "Avoid outdoor activity; use masks if outside."
    else:
        return "Severe", "rgba(139,92,246,1)", "Serious health impacts; stay indoors."

def add_aqi_band_shapes(fig: go.Figure, x_min, x_max):
    """Add shaded AQI bands (uses proper rgba colors)."""
    if pd.isna(x_min) or pd.isna(x_max):
        return
    aqi_bands = [
        (0,   50,  "rgba(34,197,94,0.15)"),
        (51,  100, "rgba(132,204,22,0.15)"),
        (101, 200, "rgba(250,204,21,0.15)"),
        (201, 300, "rgba(249,115,22,0.15)"),
        (301, 400, "rgba(239,68,68,0.15)"),
        (401, 500, "rgba(139,92,246,0.15)"),
    ]
    for low, high, color in aqi_bands:
        fig.add_shape(
            type="rect",
            x0=x_min, x1=x_max,
            y0=low, y1=high,
            fillcolor=color,
            line_width=0,
            layer="below",
        )
    fig.update_yaxes(range=[0, 500])

def fmt_aqi(v) -> str:
    """Format AQI safely; returns '—' if invalid."""
    try:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            return "—"
        return f"{int(round(float(v)))}"
    except Exception:
        return "—"

def to_excel_bytes(df: pd.DataFrame) -> bytes:
    """
    Excel export helper:
    - Converts tz-aware datetimes to tz-naive (Excel doesn't support tz).
    - Uses openpyxl (add `openpyxl` to requirements).
    """
    df_copy = df.copy()
    # Normalize timezone-aware datetimes → naive
    for col in df_copy.select_dtypes(include=["datetimetz"]).columns:
        df_copy[col] = df_copy[col].dt.tz_convert(None)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_copy.to_excel(writer, index=False)
    return buf.getvalue()

def download_button(df: pd.DataFrame, label: str, file_name: str, *, kind: str = "csv", key: str):
    """Single place to create unique, safe download buttons."""
    if df is None or df.empty:
        st.button(label, disabled=True, key=f"disabled_{key}")
        return
    if kind == "csv":
        data = df.to_csv(index=False).encode("utf-8")
        mime = "text/csv"
    else:
        data = to_excel_bytes(df)
        mime = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    st.download_button(label=label, data=data, file_name=file_name, mime=mime, key=key)

@st.cache_data(ttl=60, show_spinner=False)
def load_city_data(city: str, limit: int = 2000) -> pd.DataFrame:
    """Load recent rows for a single city from aqi_results."""
    res = (
        supabase.table("aqi_results")
        .select("*")
        .eq("city", city)
        .order("datetime_utc", desc=True)
        .limit(limit)
        .execute()
    )
    df = pd.DataFrame(res.data or [])
    if df.empty:
        return df

    # Parse/normalize columns
    df["datetime_utc"] = pd.to_datetime(df["datetime_utc"], utc=True, errors="coerce")
    df = df.dropna(subset=["datetime_utc"])
    # Display tz (IST)
    df["dt_disp"] = df["datetime_utc"].dt.tz_convert("Asia/Kolkata")

    if "aqi" in df.columns:
        df["aqi"] = pd.to_numeric(df["aqi"], errors="coerce")

    if "anomaly" in df.columns:
        df["anomaly"] = pd.to_numeric(df["anomaly"], errors="coerce").fillna(0).astype(int)

    # Predictions (optional)
    for col in ["aqi_1h_pred", "aqi_2h_pred", "aqi_3h_pred"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

@st.cache_data(ttl=60, show_spinner=False)
def load_latest_rows(cities: list[str]) -> pd.DataFrame:
    """Get a single latest row per city."""
    latest = []
    for c in cities:
        resp = (
            supabase.table("aqi_results")
            .select("*")
            .eq("city", c)
            .order("datetime_utc", desc=True)
            .limit(1)
            .execute()
        )
        if resp.data:
            latest.append(resp.data[0])
    return pd.DataFrame(latest)

# -----------------
# Sidebar
# -----------------
st.sidebar.title("Controls")

sidebar_city = st.sidebar.selectbox("Default City", list(CITY_COORDS.keys()), index=0)
auto_refresh = st.sidebar.checkbox("Auto-refresh", True)
refresh_mins = st.sidebar.slider("Refresh interval (minutes)", 1, 30, 5)
anomaly_only = st.sidebar.checkbox("Show anomalies only", False)

if auto_refresh:
    st_autorefresh(interval=refresh_mins * 60 * 1000, key="autorefresh")

# -----------------
# Layout / Tabs
# -----------------
st.title("🌍 Real-time Air Quality Dashboard")
st.caption("Powered by Supabase • Local times are shown in Asia/Kolkata")

tab_overview, tab_forecast, tab_trends, tab_pollutants, tab_map = st.tabs(
    ["📊 Overview", "🔮 Forecast", "📈 Trends", "🧪 Pollutants", "🗺️ Map"]
)

# -----------------
# OVERVIEW
# -----------------
with tab_overview:
    st.subheader("Latest AQI Snapshot (All Cities)")
    cols = st.columns(len(CITY_COORDS))

    latest_df = load_latest_rows(list(CITY_COORDS.keys()))
    if latest_df.empty:
        st.info("No data available yet.")
    else:
        # Render KPI cards
        for i, c in enumerate(CITY_COORDS.keys()):
            row = latest_df[latest_df.get("city") == c]
            with cols[i]:
                if row.empty:
                    st.info(f"No data for {c}.")
                else:
                    r = row.iloc[0]
                    aqi = pd.to_numeric(r.get("aqi"), errors="coerce")
                    aqi_round = None if pd.isna(aqi) else int(round(aqi))
                    aqi_display = "—" if aqi_round is None else str(aqi_round)
                    cat, color, msg = get_aqi_category(aqi_round)

                    ts = pd.to_datetime(r["datetime_utc"], utc=True, errors="coerce")
                    updated = "—"
                    if pd.notna(ts):
                        updated = humanize.naturaltime(datetime.now(timezone.utc) - ts)

                    st.markdown(
                        f"""
                        <div style="
                            background:{color};
                            padding:20px;
                            border-radius:16px;
                            min-height:220px;
                            color:white;
                            box-shadow:0 8px 24px rgba(0,0,0,0.2);
                            display:flex;flex-direction:column;justify-content:space-between;
                        ">
                            <div>
                                <h3 style="margin:0;font-size:22px">{c}</h3>
                                <p style="margin:2px 0 0;font-size:14px;font-weight:500;opacity:.9">{cat}</p>
                            </div>
                            <div style="text-align:center;margin:10px 0;">
                                <div style="font-size:48px;font-weight:800;line-height:1">{aqi_display}</div>
                            </div>
                            <div style="font-size:13px;opacity:.95">
                                <p style="margin:0">{msg}</p>
                                <p style="margin:0">Updated {updated}</p>
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )

        # Last retrieved timestamp
        if "datetime_utc" in latest_df.columns and not latest_df["datetime_utc"].isna().all():
            last_retrieved = pd.to_datetime(latest_df["datetime_utc"], utc=True, errors="coerce").max()
            if pd.notna(last_retrieved):
                st.info(f"🕒 Last data retrieved at: {last_retrieved.strftime('%Y-%m-%d %H:%M UTC')}")

        # Export latest snapshot table
        st.markdown("### Export Latest Snapshot Table")
        export_latest = latest_df.copy()
        if "aqi" in export_latest.columns:
            export_latest["aqi"] = pd.to_numeric(
                export_latest["aqi"], errors="coerce"
            ).round().astype("Int64")
        download_button(
            export_latest, "⬇️ Latest Snapshot (CSV)", "overview_latest_snapshot.csv",
            kind="csv", key="overview_latest_csv",
        )
        download_button(
            export_latest, "⬇️ Latest Snapshot (Excel)", "overview_latest_snapshot.xlsx",
            kind="excel", key="overview_latest_xlsx",
        )

# -----------------
# FORECAST
# -----------------
with tab_forecast:
    tab_city = st.selectbox(
        "City for Forecast view",
        list(CITY_COORDS.keys()),
        index=list(CITY_COORDS.keys()).index(sidebar_city),
        key="forecast_city_select",
    )
    df_city = load_city_data(tab_city, limit=2000)

    st.subheader(f"Forecast – {tab_city}")
    if df_city.empty:
        st.info("No data available.")
    else:
        df_plot = df_city.copy()
        if anomaly_only and "anomaly" in df_plot.columns:
            df_plot = df_plot[df_plot["anomaly"] == 1]

        if df_plot.empty:
            st.info("No rows match the current filter (anomalies only).")
        else:
            fig = go.Figure()
            # Actual AQI
            fig.add_trace(
                go.Scatter(
                    x=df_plot["dt_disp"],
                    y=df_plot["aqi"].apply(lambda v: int(round(v)) if pd.notna(v) else None),
                    mode="lines+markers",
                    name="AQI",
                )
            )
            # Optional predictions if present
            pred_names = {
                "aqi_1h_pred": "+1h (pred)",
                "aqi_2h_pred": "+2h (pred)",
                "aqi_3h_pred": "+3h (pred)",
            }
            for col, name in pred_names.items():
                if col in df_plot.columns:
                    fig.add_trace(
                        go.Scatter(
                            x=df_plot["dt_disp"],
                            y=df_plot[col],
                            mode="lines",
                            name=name,
                            line=dict(dash="dot"),
                        )
                    )

            add_aqi_band_shapes(fig, df_plot["dt_disp"].min(), df_plot["dt_disp"].max())
            fig.update_layout(
                yaxis_title="AQI",
                xaxis_title="Time",
                template="plotly_dark",
                hovermode="x unified",
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            st.plotly_chart(fig, use_container_width=True)

        # Exports (Forecast)
        st.markdown("### Export (Forecast)")
        download_button(
            df_plot, "⬇️ Current Filtered (CSV)", f"{tab_city}_forecast_filtered.csv",
            kind="csv", key="forecast_filtered_csv",
        )
        download_button(
            df_plot, "⬇️ Current Filtered (Excel)", f"{tab_city}_forecast_filtered.xlsx",
            kind="excel", key="forecast_filtered_xlsx",
        )
        download_button(
            df_city, "⬇️ All Rows (CSV)", f"{tab_city}_forecast_all.csv",
            kind="csv", key="forecast_all_csv",
        )
        download_button(
            df_city, "⬇️ All Rows (Excel)", f"{tab_city}_forecast_all.xlsx",
            kind="excel", key="forecast_all_xlsx",
        )
        if "anomaly" in df_city.columns:
            anomalies = df_city[df_city["anomaly"] == 1]
            if not anomalies.empty:
                download_button(
                    anomalies, "⬇️ Anomalies Only (CSV)", f"{tab_city}_forecast_anomalies.csv",
                    kind="csv", key="forecast_anoms_csv",
                )
                download_button(
                    anomalies, "⬇️ Anomalies Only (Excel)", f"{tab_city}_forecast_anomalies.xlsx",
                    kind="excel", key="forecast_anoms_xlsx",
                )

    # Disclaimer at bottom of forecast tab
    st.caption("⚠️ Disclaimer: Forecasted AQI values are based on available data and models. "
               "They are not 100% accurate and may vary from actual conditions.")

# -----------------
# TRENDS
# -----------------
with tab_trends:
    tab_city_tr = st.selectbox(
        "City for Trends view",
        list(CITY_COORDS.keys()),
        index=list(CITY_COORDS.keys()).index(sidebar_city),
        key="trends_city_select",
    )
    df_city_tr = load_city_data(tab_city_tr, limit=2000)

    st.subheader(f"Trends – {tab_city_tr}")
    if df_city_tr.empty:
        st.info("No data available.")
    else:
        df_plot_tr = df_city_tr.copy()
        if anomaly_only and "anomaly" in df_plot_tr.columns:
            df_plot_tr = df_plot_tr[df_plot_tr["anomaly"] == 1]

        if df_plot_tr.empty:
            st.info("No rows match the current filter (anomalies only).")
        else:
            fig = go.Figure()
            # AQI line
            fig.add_trace(
                go.Scatter(
                    x=df_plot_tr["dt_disp"],
                    y=df_plot_tr["aqi"].apply(lambda v: int(round(v)) if pd.notna(v) else None),
                    mode="lines",
                    name="AQI",
                )
            )
            # Highlight anomaly points if not filtering to anomalies
            if "anomaly" in df_plot_tr.columns and not anomaly_only:
                ann = df_plot_tr[df_plot_tr["anomaly"] == 1]
                if not ann.empty:
                    fig.add_trace(
                        go.Scatter(
                            x=ann["dt_disp"],
                            y=ann["aqi"].apply(lambda v: int(round(v)) if pd.notna(v) else None),
                            mode="markers",
                            marker=dict(color="red", size=10, symbol="x"),
                            name="Anomaly",
                        )
                    )

            add_aqi_band_shapes(fig, df_plot_tr["dt_disp"].min(), df_plot_tr["dt_disp"].max())
            fig.update_layout(
                yaxis_title="AQI",
                xaxis_title="Time",
                template="plotly_dark",
                hovermode="x unified",
            )
            st.plotly_chart(fig, use_container_width=True)

        # Exports (Trends)
        st.markdown("### Export (Trends)")
        download_button(
            df_plot_tr, "⬇️ Current Filtered (CSV)", f"{tab_city_tr}_trends_filtered.csv",
            kind="csv", key="trends_filtered_csv",
        )
        download_button(
            df_plot_tr, "⬇️ Current Filtered (Excel)", f"{tab_city_tr}_trends_filtered.xlsx",
            kind="excel", key="trends_filtered_xlsx",
        )
        download_button(
            df_city_tr, "⬇️ All Rows (CSV)", f"{tab_city_tr}_trends_all.csv",
            kind="csv", key="trends_all_csv",
        )
        download_button(
            df_city_tr, "⬇️ All Rows (Excel)", f"{tab_city_tr}_trends_all.xlsx",
            kind="excel", key="trends_all_xlsx",
        )
        if "anomaly" in df_city_tr.columns:
            anomalies_tr = df_city_tr[df_city_tr["anomaly"] == 1]
            if not anomalies_tr.empty:
                st.markdown("#### Detected Anomalies")
                st.dataframe(anomalies_tr, use_container_width=True)
                download_button(
                    anomalies_tr, "⬇️ Anomalies Only (CSV)", f"{tab_city_tr}_trends_anomalies.csv",
                    kind="csv", key="trends_anoms_csv",
                )
                download_button(
                    anomalies_tr, "⬇️ Anomalies Only (Excel)", f"{tab_city_tr}_trends_anomalies.xlsx",
                    kind="excel", key="trends_anoms_xlsx",
                )

# -----------------
# POLLUTANTS
# -----------------
with tab_pollutants:
    tab_city_pol = st.selectbox(
        "City for Pollutants view",
        list(CITY_COORDS.keys()),
        index=list(CITY_COORDS.keys()).index(sidebar_city),
        key="pollutants_city_select",
    )
    df_city_pol = load_city_data(tab_city_pol, limit=2000)

    st.subheader(f"Pollutant Breakdown – {tab_city_pol}")
    if df_city_pol.empty:
        st.info("No data available.")
    else:
        exclude_cols = {"city", "datetime_utc", "dt_disp", "aqi", "anomaly", "id"}
        pollutant_cols = [c for c in df_city_pol.columns if c not in exclude_cols and str(df_city_pol[c].dtype) != "object"]

        if not pollutant_cols:
            st.info("No pollutant columns available in this dataset.")
        else:
            selected = st.multiselect("Select pollutants", pollutant_cols, default=pollutant_cols[: min(3, len(pollutant_cols))])
            if selected:
                fig = go.Figure()
                for pcol in selected:
                    fig.add_trace(go.Scatter(x=df_city_pol["dt_disp"], y=df_city_pol[pcol], mode="lines", name=pcol))
                fig.update_layout(
                    yaxis_title="Concentration",
                    xaxis_title="Time",
                    template="plotly_dark",
                    hovermode="x unified",
                )
                st.plotly_chart(fig, use_container_width=True)

# -----------------
# MAP (Last Snapshot)
# -----------------
with tab_map:
    st.subheader("City AQI Map (Last Snapshot)")
    latest = load_latest_rows(list(CITY_COORDS.keys()))
    if latest.empty:
        st.info("No latest snapshot available.")
    else:
        df_latest = latest.copy()
        # AQI + coordinates
        df_latest["aqi"] = pd.to_numeric(df_latest["aqi"], errors="coerce").round().astype("Int64")
        df_latest["lat"] = df_latest["city"].map(lambda x: CITY_COORDS.get(x, (None, None))[0])
        df_latest["lon"] = df_latest["city"].map(lambda x: CITY_COORDS.get(x, (None, None))[1])

        # Marker size bounded for readability
        msize = df_latest["aqi"].astype(float).fillna(0) / 10.0
        msize = msize.clip(lower=8, upper=22)

        fig = go.Figure()
        fig.add_trace(
            go.Scattergeo(
                lon=df_latest["lon"],
                lat=df_latest["lat"],
                text=df_latest.apply(lambda r: f"{r['city']}<br>AQI: {fmt_aqi(r['aqi'])}", axis=1),
                mode="markers+text",
                marker=dict(
                    size=msize,
                    color=df_latest["aqi"].astype(float),
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="AQI"),
                ),
                textposition="bottom center",
                name="Cities",
            )
        )
        fig.update_geos(projection_type="natural earth", showcountries=True, showland=True, fitbounds="locations")
        fig.update_layout(template="plotly_dark", margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)

        st.markdown("### Export Last Snapshot")
        download_button(
            df_latest, "⬇️ Last Snapshot (CSV)", "last_snapshot.csv",
            kind="csv", key="map_snapshot_csv",
        )
        download_button(
            df_latest, "⬇️ Last Snapshot (Excel)", "last_snapshot.xlsx",
            kind="excel", key="map_snapshot_xlsx",
        )
