import streamlit as st
from pymongo import MongoClient
import pandas as pd
import matplotlib.pyplot as plt
from urllib.parse import quote_plus

# --------------------------------------------------------
# Streamlit Setup
# --------------------------------------------------------
st.set_page_config(page_title="Weather Data Dashboard", layout="wide")

st.title("🌦️ Weather Data Dashboard")

# --------------------------------------------------------
# Azure Cosmos DB Credentials
# --------------------------------------------------------
USERNAME = "broadws"
PASSWORD = "$unyPoly25!"

USERNAME_ENCODED = quote_plus(USERNAME)
PASSWORD_ENCODED = quote_plus(PASSWORD)

MONGO_URI = (
    f"mongodb+srv://{USERNAME_ENCODED}:{PASSWORD_ENCODED}"
    "@dsa508-test2-mongodb-weather.global.mongocluster.cosmos.azure.com/"
    "?tls=true&authMechanism=SCRAM-SHA-256&retrywrites=false&maxIdleTimeMS=120000"
)

# --------------------------------------------------------
# Connect to Cosmos DB
# --------------------------------------------------------
try:
    client = MongoClient(MONGO_URI)
    db = client["dsa508-test2-mongodb-weather"]
    collection = db["weatherdata"]

    st.success("✅ Connected to Azure Cosmos DB Successfully!")

    data = list(collection.find({}, {"_id": 0}))
    df = pd.DataFrame(data)

except Exception as e:
    st.error(f"❌ Connection failed: {e}")
    st.stop()

if df.empty:
    st.warning("⚠️ No data found in database.")
    st.stop()

# --------------------------------------------------------
# Recursive JSON Flattening
# --------------------------------------------------------
def flatten_dict(d, parent_key="", sep="_"):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

df = pd.DataFrame([flatten_dict(record) for record in df.to_dict(orient="records")])

# --------------------------------------------------------
# Timestamp Cleanup
# --------------------------------------------------------
if "ts" in df.columns:
    df["timestamp"] = pd.to_datetime(df["ts"], errors="coerce")
else:
    st.error("❌ Timestamp column missing.")
    st.stop()

df = df.dropna(subset=["timestamp"])
df["timestamp"] = df["timestamp"].dt.to_pydatetime()

# --------------------------------------------------------
# Numeric Cleanup
# --------------------------------------------------------
numeric_fields = [
    "airTemperature_value",
    "pressure_value",
    "wind_speed_rate"
]

for col in numeric_fields:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

# Filter out bad sentinel values (optional)
df["airTemperature_value"] = df["airTemperature_value"].replace([999.9, -999.9], pd.NA)
df["pressure_value"] = df["pressure_value"].replace([9999, 99999], pd.NA)
df["wind_speed_rate"] = df["wind_speed_rate"].replace([999.9], pd.NA)

# --------------------------------------------------------
# Display Cleaned Data
# --------------------------------------------------------
st.subheader("✅ Cleaned Data Preview")
st.dataframe(df.head())

# --------------------------------------------------------
# Date Range Selector
# --------------------------------------------------------
st.subheader("📆 Select Date Range to View")

min_date = df["timestamp"].min().date()
max_date = df["timestamp"].max().date()

date_range = st.date_input(
    "Select date range:",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

if isinstance(date_range, tuple):
    start_date, end_date = date_range
else:
    start_date, end_date = min_date, max_date

df = df[(df["timestamp"].dt.date >= start_date) & (df["timestamp"].dt.date <= end_date)]
df_sorted = df.sort_values("timestamp")

# --------------------------------------------------------
# Outlier Filtering + Rolling Smoothing
# --------------------------------------------------------

st.header("📈 Visualizations")

# Remove clearly invalid extreme values
df_sorted = df_sorted[
    (df_sorted["airTemperature_value"] > -80) & (df_sorted["airTemperature_value"] < 60) &
    (df_sorted["pressure_value"] > 850) & (df_sorted["pressure_value"] < 1080) &
    (df_sorted["wind_speed_rate"] >= 0) & (df_sorted["wind_speed_rate"] < 60)
]

# Rolling smoothing (window=6 ~ smoothing across ~ 6 observations)
df_sorted["temp_smooth"] = df_sorted["airTemperature_value"].rolling(6, min_periods=1).mean()
df_sorted["pressure_smooth"] = df_sorted["pressure_value"].rolling(6, min_periods=1).mean()
df_sorted["wind_smooth"] = df_sorted["wind_speed_rate"].rolling(6, min_periods=1).mean()

# --------------------------------------------------------
# Temperature Plot (Smoothed)
# --------------------------------------------------------
if "airTemperature_value" in df_sorted.columns:
    st.subheader("🌡️ Temperature Over Time (Smoothed)")
    plt.figure(figsize=(10, 4))
    plt.plot(df_sorted["timestamp"], df_sorted["temp_smooth"], color="orange", linewidth=2)
    plt.xlabel("Time")
    plt.ylabel("Temperature (°C)")
    plt.title("Temperature Trends (Smoothed)")
    st.pyplot(plt)

# --------------------------------------------------------
# Pressure Plot (Smoothed)
# --------------------------------------------------------
if "pressure_value" in df_sorted.columns:
    st.subheader("🫧 Pressure Over Time (Smoothed)")
    plt.figure(figsize=(10, 4))
    plt.plot(df_sorted["timestamp"], df_sorted["pressure_smooth"], color="blue", linewidth=2)
    plt.xlabel("Time")
    plt.ylabel("Pressure (hPa)")
    plt.title("Pressure Trends (Smoothed)")
    st.pyplot(plt)

# --------------------------------------------------------
# Wind Speed Plot (Smoothed)
# --------------------------------------------------------
if "wind_speed_rate" in df_sorted.columns:
    st.subheader("💨 Wind Speed Over Time (Smoothed)")
    wind_df = df_sorted.dropna(subset=["wind_smooth"])

    if wind_df.empty:
        st.warning("No valid wind speed values after filtering.")
    else:
        plt.figure(figsize=(10, 4))
        plt.plot(wind_df["timestamp"], wind_df["wind_smooth"], color="purple", linewidth=2)
        plt.xlabel("Time")
        plt.ylabel("Wind Speed (m/s)")
        plt.title("Wind Speed Trends (Smoothed)")
        st.pyplot(plt)

# Keep your existing extra visuals below


st.subheader("🌡️ Daily Average Temperature")
daily_temp = df.groupby(df['timestamp'].dt.date)['airTemperature_value'].mean()

plt.figure(figsize=(10,4))
plt.bar(daily_temp.index, daily_temp.values, color="orange")
plt.xlabel("Date")
plt.ylabel("Avg Temperature (°C)")
plt.title("Daily Average Temperature")
st.pyplot(plt)


st.subheader("🌬️ Temperature vs Wind Speed")

temp_wind = df.dropna(subset=["airTemperature_value", "wind_speed_rate"])
plt.figure(figsize=(6,4))
plt.scatter(temp_wind["airTemperature_value"], temp_wind["wind_speed_rate"], alpha=0.4)
plt.xlabel("Temperature (°C)")
plt.ylabel("Wind Speed (m/s)")
plt.title("Temperature vs Wind Speed")
st.pyplot(plt)


st.subheader("📊 Temperature Distribution")

plt.figure(figsize=(6,4))
df["airTemperature_value"].hist(bins=30, color="tomato")
plt.xlabel("Temperature (°C)")
plt.ylabel("Frequency")
plt.title("Temperature Distribution")
st.pyplot(plt)
