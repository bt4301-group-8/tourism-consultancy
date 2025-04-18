import streamlit as st
import pandas as pd
import altair as alt
from glob import glob
from prophet import Prophet
import os

# Page Configuration
st.set_page_config(
    page_title="Country Forecast Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Sidebar Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Overview", "Data Explorer", "Visualizations"])

# Load Data
@st.cache_data
def load_data():
    base_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../backend/data/countries"))
    file_paths = glob(os.path.join(base_path, "*.csv"))

    st.write(f"Reading from: {base_path}")
    st.write(f"Found CSV files: {file_paths}")

    df_list = []
    for path in file_paths:
        if os.path.exists(path):
            temp_df = pd.read_csv(path)
            temp_df["month_year"] = pd.to_datetime(temp_df["month_year"])
            df_list.append(temp_df)
        else:
            st.warning(f"File not found: {path}")

    if df_list:
        return pd.concat(df_list, ignore_index=True)
    else:
        st.error("No CSV files found.")
        return pd.DataFrame()

df = load_data()

# Overview Page
if page == "Overview":
    st.title("🌍 Dashboard Overview")
    st.write("Welcome to the forecast dashboard!")
    st.metric("Countries Tracked", df['country'].nunique())
    st.metric("Total Data Points", len(df))

# Data Explorer Page
elif page == "Data Explorer":
    st.title("🗂️ Data Explorer")
    st.dataframe(df)

    country_filter = st.selectbox("Filter by Country", df['country'].unique())
    filtered_df = df[df['country'] == country_filter]
    st.write(f"Filtered Data for {country_filter}")
    st.dataframe(filtered_df)

# Visualizations Page
elif page == "Visualizations":
    st.title("📈 Forecast Visualizations")
    st.write("Select a country to view historical and forecasted data.")

    # Country selection
    country_list = df['country'].unique()
    selected_country = st.selectbox("Choose Country", country_list)

    # Filter by selected country
    country_df = df[df["country"] == selected_country].sort_values("month_year")

    # Historical data
    historical_df = country_df[["month_year", "num_visitors"]].dropna().copy()
    historical_df["Type"] = "historical"
    historical_df = historical_df.rename(columns={"num_visitors": "Value"})

    # Check if predicted_visitors exists
    if "predicted_visitors" in country_df.columns and country_df["predicted_visitors"].notna().sum() > 1:
        # Use existing forecasted values
        last_hist_date = historical_df["month_year"].max()
        last_hist_value = historical_df.loc[historical_df["month_year"] == last_hist_date, "Value"].values[0]

        forecast_values = country_df["predicted_visitors"].dropna().values
        forecast_dates = pd.date_range(
            start=last_hist_date + pd.DateOffset(months=1),
            periods=5 * 12, 
            freq='MS'  # Monthly frequency
        )

        forecast_df = pd.DataFrame({
            "month_year": [last_hist_date] + list(forecast_dates),
            "Value": [last_hist_value] + list(forecast_values[:60]),  # Use forecasted values for 60 months
            "Type": "forecasted"
        })

    else:
        # Perform real-time forecast using Prophet
        st.info("Generating forecast using Prophet model...")

        ts_df = country_df[["month_year", "num_visitors"]].dropna().rename(
            columns={"month_year": "ds", "num_visitors": "y"}
        )

        model = Prophet()
        model.fit(ts_df)

        future = model.make_future_dataframe(periods=5 * 12, freq='MS')  # Forecast 5 years of monthly data
        forecast = model.predict(future)

        forecast["Type"] = forecast["ds"].apply(
            lambda x: "historical" if x <= ts_df["ds"].max() else "forecasted"
        )
        forecast_df = forecast[["ds", "yhat", "Type"]].rename(
            columns={"ds": "month_year", "yhat": "Value"}
        )

    # Combine both datasets
    combined_df = pd.concat([historical_df, forecast_df[forecast_df["Type"] == "forecasted"]], ignore_index=True)

    # Altair chart
    chart = alt.Chart(combined_df).mark_line(point=True).encode(
        x=alt.X("month_year:T", title="Date"),
        y=alt.Y("Value:Q", title="Number of Visitors"),
        color=alt.Color("Type:N", title="Data Type"),
        tooltip=["month_year:T", "Type:N", "Value:Q"],
        strokeDash=alt.condition(
            alt.datum.Type == 'forecasted',
            alt.value([5, 5]),  # Dotted line for forecasted
            alt.value([0, 0])   # Solid line for historical
        )
    ).properties(
        title=f"{selected_country}: Historical vs Forecasted Visitors",
        width='container',
        height=400
    )

    st.altair_chart(chart, use_container_width=True)

    # Data Table (Full view)
    st.subheader("📋 Data Table")
    if "predicted_visitors" in country_df.columns:
        merged_view = pd.merge(
            country_df[["month_year", "num_visitors"]], 
            forecast_df[["month_year", "Value"]].rename(columns={"Value": "predicted_visitors"}), 
            on="month_year", how="outer"
        ).sort_values("month_year")
    else:
        merged_view = combined_df[["month_year", "Value", "Type"]].sort_values("month_year")

    st.dataframe(merged_view)

# Footer
st.markdown("---")
st.markdown("Company Name")
