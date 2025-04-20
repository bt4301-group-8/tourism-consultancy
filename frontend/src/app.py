import streamlit as st
import pandas as pd
import altair as alt
from glob import glob
import os
import mlflow
import mlflow.xgboost
import mlflow.pyfunc
from mlflow.tracking import MlflowClient
from datetime import datetime
from dateutil.relativedelta import relativedelta
import numpy as np
from datetime import datetime
import pandas as pd
import numpy as np

mlflow.set_tracking_uri("http://127.0.0.1:9080")

def generate_random_walk_features(historical_df, n_months=5):
    last_test_date = pd.to_datetime(historical_df['month_year'].max())
    future_dates = [last_test_date + pd.DateOffset(months=i) for i in range(1, n_months + 1)]
    future_df = pd.DataFrame({'month_year': future_dates})

    # Take same months from previous year as base
    previous_year_dates = [d - pd.DateOffset(years=1) for d in future_dates]
    base_df = historical_df[historical_df['month_year'].isin(previous_year_dates)].copy()

    if len(base_df) < len(future_dates):
        raise ValueError("Not enough matching historical data from the previous year to base random walk on.")

    base_df = base_df.reset_index(drop=True)
    future_df = future_df.reset_index(drop=True)

    # Merge features
    for col in historical_df.columns:
        # Keeping int columns else, unable to predict
        if col not in ['month_year', 'country', 'num_visitors', 'is_monsoon_season', 'google_trend_score', 'google_trend_score_lag1']:
            base_col_values = base_df[col].copy()

            # Apply random walk: +/- up to 5% per feature
            noise = np.random.normal(loc=0.0, scale=0.03, size=len(base_col_values))  # ~3% variation
            walked_values = base_col_values * (1 + noise)

            future_df[col] = walked_values
    future_df['is_monsoon_season'] = base_df['is_monsoon_season'].copy()
    future_df['google_trend_score'] = base_df['google_trend_score'].copy()
    future_df['google_trend_score_lag1'] = base_df['google_trend_score_lag1'].copy()


    # Copy other required fields
    if 'country' in historical_df.columns:
        future_df['country'] = historical_df['country'].iloc[-1]

    return future_df

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

                st.subheader(f"🔮 Visitor Forecast for {country_filter.title()}")

                model_name = MODEL_NAMES.get(country_filter.lower())

                client = MlflowClient()

                all_versions = client.search_model_versions(f"name = '{model_name}'")

                latest = max(all_versions, key=lambda mv: int(mv.version))
                model_metadata = [latest]
                latest_model_version = model_metadata[0].version

                if model_name: # retrieve latest version
                    model_version = latest_model_version
                    model_uri = f"models:/{model_name}/{model_version}"
                    try:
                        st.info(f"Consistently Loading registered MLflow XGBoost model (version {model_version}) from: '{model_uri}'...")
                        model = mlflow.pyfunc.load_model(model_uri)  # Consistent loading with XGBoost
                        try:
                            mv = client.get_model_version(
                                name=model_name,
                                version=model_version
                            )
                            run_id = mv.run_id

                            local_test_path = client.download_artifacts(
                                run_id,
                                "test_data/test_set.csv"
                            )
                            
                            test_df = pd.read_csv(local_test_path)

                            n=len(test_df)
                            hist_df['month_year'] = pd.to_datetime(hist_df['month_year'])
                            hist_df = hist_df.sort_values('month_year', ascending=True).reset_index(drop=True)
                            hist_df_full = hist_df.copy()
                            hist_df = hist_df.iloc[:-n]
                            forecast_data = pd.date_range(
                                start=hist_df['month_year'].max()+ pd.offsets.MonthBegin(1),
                                periods=n,
                                freq='MS'
                            )

                            forecast_df = pd.DataFrame(forecast_data, columns=['month_year'])
                            forecast_df['country'] = country_filter
                            #predict on test data
                            forecast_df['num_visitors'] = model.predict(test_df)
                            forecast_df['num_visitors'] = np.expm1(forecast_df['num_visitors'])
                            forecast_df['num_visitors'] = forecast_df['num_visitors'] \
                                .apply(lambda x: float(f"{x:.3g}"))
                            #print table
                            st.subheader("Predicted value for the past few months")
                            st.dataframe(forecast_df.head())

                            synthetic_df = generate_random_walk_features(hist_df_full, n_months=5)
                            # st.dataframe(synthetic_df.head())
                            #forecast on synthetic data
                            synthetic_df['num_visitors'] = model.predict(synthetic_df)
                            synthetic_df['num_visitors'] = np.expm1(synthetic_df['num_visitors'])
                            synthetic_df['num_visitors'] = synthetic_df['num_visitors'] \
                                .apply(lambda x: float(f"{x:.3g}"))
                            #print forecast
                            synthetic_df_table = synthetic_df[['month_year', 'country', 'num_visitors']]
                            st.subheader("Forecasting value for the upcoming months")
                            st.dataframe(synthetic_df_table.head())
                            last_hist = hist_df.iloc[-1:]
                            forecast_df = pd.concat([last_hist, forecast_df], ignore_index=True)
                            test_df = pd.concat([last_hist, test_df], ignore_index=True)
                            last_forecast = forecast_df.iloc[-1:]
                            synthetic_df = pd.concat([last_forecast, synthetic_df], ignore_index=True)
                            
                            # Label the type of data
                            hist_df_full['type'] = 'historical'
                            forecast_df['type'] = 'predicted'
                            synthetic_df['type'] = 'forecasting'

                            # Combine historical and forecast data
                            combined_df = pd.concat([hist_df_full[['month_year', 'num_visitors', 'type']],
                                                    forecast_df[['month_year', 'num_visitors', 'type']],
                                                    synthetic_df[['month_year', 'num_visitors', 'type']]])

                            # Plot combined chart
                            combined_chart = alt.Chart(combined_df).mark_line(point=True).encode(
                                x=alt.X('month_year:T',
                                        axis=alt.Axis(format='%Y/%-m/%-d', title='Date')),
                                y='num_visitors:Q',
                                color=alt.Color(
                                    'type:N',
                                    scale=alt.Scale(
                                        domain=['historical', 'predicted', 'forecasting'],
                                        range=['#87CEFA',  # light blue
                                            '#1E90FF',  # blue
                                            '#98FB98']  # green
                                    ),
                                    legend=alt.Legend(title='Data Type')
                                ),
                                strokeDash=alt.condition(
                                    alt.datum.type == 'historical',
                                    alt.value([0, 0]),   # solid for historical
                                    alt.value([5, 5])    # dotted for others
                                ),
                                tooltip=['month_year:T', 'num_visitors:Q', 'type:N']
                            ).properties(
                                title=f"Historical and Forecasted Visitors for {country_filter.title()}"
                            ).interactive()



                            st.altair_chart(combined_chart, use_container_width=True)

                        except Exception as e:
                            st.error(f"Error during forecasting: {e}")
                    except Exception as e:
                        st.error(f"Error loading registered XGBoost model from MLflow: {e}")
                else:
                    st.error(f"No registered model name found for {country_filter}.")
