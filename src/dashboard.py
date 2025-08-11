import streamlit as st
import pandas as pd
import numpy as np
import time

# ------------------------
# Page Config
# ------------------------
st.set_page_config(
    page_title="📊 My Interactive Dashboard",
    page_icon="📈",
    layout="wide"
)

# ------------------------
# Custom CSS for styling & popup
# ------------------------
st.markdown("""
    <style>
        /* Main container adjustments */
        .main {
            background-color: #f7f9fc;
            padding: 2rem;
        }
        /* Card-like look for each metric */
        .metric-card {
            background-color: white;
            padding: 1.2rem;
            border-radius: 12px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.08);
            text-align: center;
        }
        /* Popup modal styling */
        .popup-overlay {
            position: fixed;
            top: 0;
            left: 0;
            width: 100%;
            height: 100%;
            background: rgba(0,0,0,0.5);
            z-index: 9999;
            display: flex;
            align-items: center;
            justify-content: center;
        }
        .popup-content {
            background: white;
            padding: 2rem;
            border-radius: 10px;
            max-width: 500px;
            width: 90%;
            box-shadow: 0 4px 15px rgba(0,0,0,0.2);
            animation: fadeIn 0.3s ease-in-out;
        }
        @keyframes fadeIn {
            from {opacity: 0; transform: scale(0.95);}
            to {opacity: 1; transform: scale(1);}
        }
    </style>
""", unsafe_allow_html=True)

# ------------------------
# Popup Feature
# ------------------------
if "show_popup" not in st.session_state:
    st.session_state.show_popup = False

def toggle_popup():
    st.session_state.show_popup = not st.session_state.show_popup

# ------------------------
# Sidebar
# ------------------------
with st.sidebar:
    st.title("⚙️ Controls")
    date_range = st.date_input("Select Date Range", [])
    if st.button("Show Info Popup"):
        toggle_popup()

# ------------------------
# Main Title
# ------------------------
st.title("📊 Interactive Dashboard with Popup")
st.markdown("Welcome to your enhanced Streamlit dashboard!")

# ------------------------
# Fake Data
# ------------------------
np.random.seed(42)
data = pd.DataFrame({
    "Date": pd.date_range("2024-01-01", periods=30),
    "Sales": np.random.randint(100, 1000, size=30),
    "Revenue": np.random.randint(5000, 20000, size=30),
    "Customers": np.random.randint(10, 200, size=30)
})

# ------------------------
# Metrics
# ------------------------
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Total Sales", f"{data['Sales'].sum():,}")
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Total Revenue", f"${data['Revenue'].sum():,}")
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("Total Customers", f"{data['Customers'].sum():,}")
    st.markdown('</div>', unsafe_allow_html=True)

# ------------------------
# Chart
# ------------------------
st.subheader("📈 Sales Over Time")
st.line_chart(data.set_index("Date")[["Sales", "Revenue"]])

# ------------------------
# Data Table
# ------------------------
st.subheader("📋 Detailed Data")
st.dataframe(data)

# ------------------------
# Popup Display
# ------------------------
if st.session_state.show_popup:
    st.markdown(
        """
        <div class="popup-overlay" onclick="window.location.reload()">
            <div class="popup-content">
                <h3>ℹ️ Dashboard Information</h3>
                <p>This dashboard provides an interactive view of your business data. You can:</p>
                <ul>
                    <li>Track sales, revenue, and customers</li>
                    <li>View performance trends over time</li>
                    <li>Export and filter data</li>
                </ul>
                <p>Click anywhere outside to close this popup.</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )
