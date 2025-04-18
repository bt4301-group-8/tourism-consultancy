import streamlit as st
import pandas as pd
import altair as alt
from glob import glob
import os
import mlflow
import mlflow.xgboost
from mlflow.tracking import MlflowClient
from datetime import datetime
from dateutil.relativedelta import relativedelta

# mlflow.set_tracking_uri("http://127.0.0.1:9080")

# --- Page Configuration ---
st.set_page_config(
    page_title="Country Forecast Dashboard",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Sidebar Navigation ---
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Overview", "Data Explorer", "Visualizations"])

# --- Load Data ---
@st.cache_data
def load_data():
    try:
        script_dir = os.path.dirname(__file__)
        base_path = os.path.abspath(os.path.join(script_dir, "../../backend/data/countries"))
    except NameError:
        base_path = os.path.abspath("../../backend/data/countries")

    file_paths = glob(os.path.join(base_path, "*.csv"))

    if not file_paths:
        st.error(f"No CSV files found in the expected directory: {base_path}. Please ensure the path is correct and CSV files exist.")
        return pd.DataFrame(columns=['month_year', 'country', 'num_visitors'])

    df_list = []
    required_cols = ['month_year', 'country', 'num_visitors']

    for path in file_paths:
        if os.path.exists(path):
            try:
                temp_df = pd.read_csv(path)
                if not all(col in temp_df.columns for col in required_cols):
                    st.warning(f"File {os.path.basename(path)} is missing required columns ({required_cols}). Skipping.")
                    continue

                try:
                    temp_df["month_year"] = pd.to_datetime(temp_df["month_year"], errors='coerce')
                    original_rows = len(temp_df)
                    temp_df.dropna(subset=["month_year"], inplace=True)
                    if len(temp_df) < original_rows:
                        st.warning(f"Removed {original_rows - len(temp_df)} rows with invalid dates in {os.path.basename(path)}.")

                    temp_df["num_visitors"] = pd.to_numeric(temp_df["num_visitors"], errors='coerce')

                    if 'country' not in temp_df.columns:
                        country_name = os.path.basename(path).split('_')[0].lower()
                        temp_df['country'] = country_name
                        st.info(f"Inferred country '{country_name}' for file {os.path.basename(path)}.")

                    df_list.append(temp_df)

                except Exception as date_err:
                    st.warning(f"Could not process dates in file {path}: {date_err}. Skipping file.")

            except Exception as e:
                st.warning(f"Error reading or processing file {path}: {e}")
        else:
            st.warning(f"File path reported by glob does not exist: {path}")

    if df_list:
        combined_df = pd.concat(df_list, ignore_index=True)
        combined_df["month_year"] = pd.to_datetime(combined_df["month_year"])
        combined_df["num_visitors"] = pd.to_numeric(combined_df["num_visitors"], errors='coerce')
        combined_df['country'] = combined_df['country'].astype(str).str.lower()
        combined_df.sort_values(by=['country', 'month_year'], inplace=True)
        return combined_df
    else:
        st.error("No valid CSV data could be loaded.")
        return pd.DataFrame(columns=required_cols)

df = load_data()

# --- MLflow Model Names ---
MODEL_NAMES = {
    "brunei": "brunei_visitor_model",
    "cambodia": "cambodia_visitor_model",
    "indonesia": "indonesia_visitor_model",
    "laos": "laos_visitor_model",
    "malaysia": "malaysia_visitor_model",
    "myanmar": "myanmar_visitor_model",
    "philippines": "philippines_visitor_model",
    "singapore": "singapore_visitor_model",
    "thailand": "thailand_visitor_model",
    "vietnam": "vietnam_visitor_model",
}

# --- Function to get the registered model URI by version ---
def get_registered_model_uri_by_version(model_name, version="2"):  # Default to version 2
    return f"models:/{model_name}/{version}"

# --- Overview Page ---
if page == "Overview":
    st.title("🌍 Country Visitor Forecast Dashboard")
    st.markdown("""
        Welcome to the Country Visitor Forecast Dashboard.
        Use the sidebar to navigate between different sections:
        - **Overview:** This page.
        - **Data Explorer:** View the raw data and filter by country.
        - **Visualizations:** View historical visitor trends and future forecasts for selected countries.
    """)

    if not df.empty:
        st.subheader("📈 Key Metrics")

        col1, col2, col3 = st.columns(3)

        # Column 1 - Countries Tracked
        col1.metric(
            label="Countries Tracked",
            value=f"{df['country'].nunique()}",
            delta=None
        )

        # Column 2 - Total Records
        col2.metric(
            label="Total Data Points",
            value=f"{len(df)}",
            delta=None
        )

        # Column 3 - Date Range
        col3.markdown(f"""
            <div style='padding-top: 15px'>
                <strong>Date Range</strong><br>
                <span style='color:#1f77b4; font-size: 20px; font-weight: bold;'>
                    {df['month_year'].min().strftime('%Y-%m')} to {df['month_year'].max().strftime('%Y-%m')}
                </span>
            </div>
        """, unsafe_allow_html=True)

    else:
        st.warning("No data loaded. Please check the data source configuration.")


# --- Data Explorer Page ---
elif page == "Data Explorer":
    st.title("🗂️ Data Explorer")

    if df.empty:
        st.warning("No data available to explore.")
    else:
        st.subheader("Raw Data")
        st.dataframe(df)
        st.divider()

        # --- Filtering ---
        st.subheader("Filter Data")
        available_countries = sorted(df['country'].unique())
        country_filter = st.selectbox("Filter by Country", available_countries)

        if country_filter:
            filtered_df_hist = df[df['country'] == country_filter].copy()
            st.write(f"Filtered Historical Data for {country_filter.title()}")
            st.dataframe(filtered_df_hist)
            st.divider()

# --- Visualizations Page ---
elif page == "Visualizations":
    st.title("📊 Visualizations & Future Forecast")

    if df.empty:
        st.warning("No data available for visualization.")
    else:
        available_countries = sorted(df['country'].unique())
        country_filter = st.selectbox("Select Country for Visualization", available_countries)

        if country_filter:
            hist_df = df[df['country'] == country_filter].copy()
            hist_df.dropna(subset=['month_year', 'num_visitors'], inplace=True)
            hist_df.sort_values('month_year', inplace=True)

            if hist_df.empty:
                st.warning(f"No historical data found for {country_filter.title()} after filtering.")
            else:
                st.subheader(f"Historical Visitor Data for {country_filter.title()}")
                hist_chart = alt.Chart(hist_df).mark_line(point=True).encode(
                    x='month_year:T',
                    y='num_visitors:Q',
                    tooltip=['month_year', 'num_visitors']
                ).properties(
                    title=f"Historical Visitors for {country_filter.title()}"
                ).interactive()
                st.altair_chart(hist_chart, use_container_width=True)
                st.divider()

                st.subheader(f"🔮 Future Visitor Forecast (Next 5 Years)")

                model_name = MODEL_NAMES.get(country_filter.lower())

                if model_name:
                    model_version = "2"  # Load version 2 (you can adjust this)
                    model_uri = f"models:/{model_name}/{model_version}"
                    st.info(f"Loading registered MLflow XGBoost model (version {model_version}) from: '{model_uri}'...")
                    try:
                        model = mlflow.xgboost.load_model(model_uri)  # Consistent loading with XGBoost

                        try:
                            forecast_data = pd.date_range(
                                start=hist_df['month_year'].max(),
                                periods=60,
                                freq='M'
                            )
                            forecast_df = pd.DataFrame(forecast_data, columns=['month_year'])
                            forecast_df['country'] = country_filter # Ensure 'country' column is present if your model used it

                            forecast_df['num_visitors'] = model.predict(forecast_df)

                            forecast_chart = alt.Chart(forecast_df).mark_line().encode(
                                x='month_year:T',
                                y='num_visitors:Q',
                                tooltip=['month_year', 'num_visitors']
                            ).properties(
                                title=f"Visitor Forecast for {country_filter.title()} (Next 5 Years)"
                            ).interactive()

                            st.altair_chart(forecast_chart, use_container_width=True)
                        except Exception as e:
                            st.error(f"Error during forecasting: {e}")
                    except Exception as e:
                        st.error(f"Error loading registered XGBoost model from MLflow: {e}")
                else:
                    st.error(f"No registered model name found for {country_filter}.")

# --- Footer ---
st.markdown("---")
st.markdown("Company Name")
