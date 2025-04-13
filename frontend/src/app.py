import streamlit as st
import pandas as pd
import altair as alt

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
    df = pd.read_csv("test_data.csv")
    df["month_year"] = pd.to_datetime(df["month_year"])
    return df

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
    st.write("Select a country to view historical and 5-year forecasted data.")

    # Country selection
    country_list = df['country'].unique()
    selected_country = st.selectbox("Choose Country", country_list)

    # Filter by selected country
    country_df = df[df["country"] == selected_country].sort_values("month_year")

    # Split into historical and forecasted sections
    historical_df = country_df[["month_year", "historical data"]].dropna().copy()
    historical_df["Type"] = "historical"
    historical_df = historical_df.rename(columns={"historical data": "Value"})

    # Last historical point
    last_hist_date = historical_df["month_year"].max()
    last_hist_value = historical_df.loc[historical_df["month_year"] == last_hist_date, "Value"].values[0]

    # Forecast values
    forecast_values = country_df["predicted_value"].dropna().values

    # Forecast dates excluding the overlap point
    forecast_dates = pd.date_range(
        start=last_hist_date + pd.DateOffset(years=1),
        periods=len(forecast_values) - 1,
        freq='YS'
    )

    # Create forecast dataframe starting from last historical point
    forecast_df = pd.DataFrame({
        "month_year": [last_hist_date] + list(forecast_dates),
        "Value": [last_hist_value] + list(forecast_values[1:]),
        "Type": "forecasted"
    })

    # Combine into one long-form dataframe
    combined_df = pd.concat([historical_df, forecast_df], ignore_index=True)

    # Altair chart
    chart = alt.Chart(combined_df).mark_line(point=True).encode(
        x=alt.X("month_year:T", title="Date"),
        y=alt.Y("Value:Q", title="Value"),
        color=alt.Color("Type:N", title="Data Type"),
        tooltip=["month_year:T", "Type:N", "Value:Q"],
        strokeDash=alt.condition(
            alt.datum.Type == 'forecasted',
            alt.value([5, 5]),
            alt.value([0, 0])
        )
    ).properties(
        title=f"{selected_country}: Historical vs Forecasted Data",
        width='container',
        height=400
    )

    st.altair_chart(chart, use_container_width=True)

    # Data Table (Full view)
    st.subheader("📋 Data Table")
    merged_view = pd.merge(
        country_df[["month_year", "historical data"]],
        forecast_df[["month_year", "Value"]].rename(columns={"Value": "predicted_value"}),
        on="month_year",
        how="outer"
    ).sort_values("month_year")
    st.dataframe(merged_view)

# Footer
st.markdown("---")
st.markdown("Company Name")
