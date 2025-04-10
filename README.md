# Tourism Consultancy - SEA Tourism Volume Forecasting Engine

## Project Overview

This project implements a data-driven forecasting engine designed to predict monthly tourist arrivals for Southeast Asian (SEA) countries. It leverages a multivariate approach, integrating diverse data sources including social media trends, online review sentiment, economic indicators, and web search behavior to provide more accurate and actionable insights compared to traditional methods relying solely on historical data.

The goal is to equip the SEA Tourism Board and related stakeholders (governments, travel agencies, hospitality businesses, investors) with robust forecasts to optimize resource allocation, strategize marketing efforts, refine pricing, and mitigate risks associated with tourism demand fluctuations.

---

## Problem Statement

Traditional tourism volume forecasting methods, often limited to historical arrival data and seasonal trends, lack the granularity to account for dynamic, real-time factors. Economic shifts (exchange rates, GDP), evolving social media narratives, online review sentiment, and global events significantly impact travel decisions. This leads to inaccurate predictions, resulting in suboptimal infrastructure planning, inefficient marketing spend, and missed revenue opportunities across the SEA tourism ecosystem.

---

## Solution: Multivariate Predictive Model

We address this challenge by developing a predictive model that synthesizes data from multiple streams:

1.  **Economic Indicators:** Macroeconomic factors influencing travel affordability and propensity.
2.  **Social Media Signals:** Real-time pulse of public interest and sentiment towards SEA destinations captured from platforms like Instagram and Reddit.
3.  **Online Review Sentiment:** Aggregated opinions and experiences shared on platforms like TripAdvisor.
4.  **Web Search & Demand Data:** Indicators of active travel planning and interest captured via search engine trends and website traffic analysis.

By integrating these diverse features, the model aims to capture the complex interplay of factors driving tourism volume, leading to improved forecast accuracy.

---

## Technical Architecture & Stack

```mermaid
graph LR

    %% Define Core Stack Nodes (arranged conceptually vertical)
    P[Python]
    MDB(MongoDB)
    SB[Supabase]
    XG(XGBoost)

    %% Define Scraping Tools Nodes (arranged conceptually vertical)
    S1[Instagrapi]
    S2[RedditAPI/PRAW]
    S3[Custom Scrapers]

    %% Define Main Pipeline Nodes
    A[Data Sources <br/> Instagram, Reddit, <br/> TripAdvisor, Google, Econ APIs] --> B(Raw Data Storage <br/> MongoDB);
    B --> C{Data Processing <br/> Feature Engineering <br/> Python, Pandas, Vader};
    C --> D[Structured Features <br/> Supabase / PostgreSQL];
    D --> E[Model Training <br/> Prediction <br/> XGBoost];


    %% --- STYLING ---

    %% Main pipeline nodes: Dark fill, white text, distinct colored borders
    style A fill:#2D2D2D,stroke:#5599FF,stroke-width:2px,color:#FFF %% Blue border
    style B fill:#2D2D2D,stroke:#4CAF50,stroke-width:2px,color:#FFF %% Green border
    style C fill:#222,stroke:#FF9800,stroke-width:2px,color:#FFF  
    style D fill:#2D2D2D,stroke:#00BCD4,stroke-width:2px,color:#FFF %% Cyan border
    style E fill:#2D2D2D,stroke:#9C27B0,stroke-width:2px,color:#FFF %% Purple border

    %% Core Stack Nodes: Dark fill, white text, consistent colored border (e.g., Teal)
    style P fill:#3C3C3C,stroke:#26A69A,stroke-width:2px,color:#FFF %% Teal border
    style MDB fill:#3C3C3C,stroke:#26A69A,stroke-width:2px,color:#FFF %% Teal border
    style SB fill:#3C3C3C,stroke:#26A69A,stroke-width:2px,color:#FFF %% Teal border
    style XG fill:#3C3C3C,stroke:#26A69A,stroke-width:2px,color:#FFF %% Teal border

    %% Scraping Tools Nodes: Dark fill, white text, consistent colored border (e.g., Pink)
    style S1 fill:#3C3C3C,stroke:#EC407A,stroke-width:2px,color:#FFF %% Pink border
    style S2 fill:#3C3C3C,stroke:#EC407A,stroke-width:2px,color:#FFF %% Pink border
    style S3 fill:#3C3C3C,stroke:#EC407A,stroke-width:2px,color:#FFF %% Pink border
```

**Core Technologies Summary:**

*   **Scraping:** `Instagrapi`, `PRAW`, `requests`, `BeautifulSoup`/`Scrapy`
*   **Raw Storage:** `MongoDB`
*   **Processing:** `Python`, `Pandas`, `Vader`
*   **Feature Storage:** `Supabase` (`PostgreSQL`)
*   **Modeling:** `XGBoost`, `HyperOpt`, `Dmatrix`

---

## Data Pipeline & Methodology

1.  **Data Acquisition:** Scheduled scripts run periodically to scrape data from Instagram, Reddit, TripAdvisor, and Google (or other economic data sources). Raw data (posts, comments, reviews, economic figures, search trends) is collected.
2.  **Raw Data Ingestion:** Scraped data is pushed into MongoDB collections, preserving the original structure as much as possible. Each source might have its own collection (e.g., `instagram_posts`, `reddit_comments`, `tripadvisor_reviews`, `economic_indicators`).
3.  **Data Cleaning & Preprocessing:** Python scripts retrieve data from MongoDB. Tasks include:
    *   Text cleaning (removing URLs, special characters, normalization).
    *   Date/Time parsing and standardization.
    *   Handling missing values (imputation or removal based on strategy).
    *   Type conversion.
4.  **Feature Engineering:** Key features are extracted and calculated:
    *   **Social Media Metrics:** Aggregating counts (mentions, likes, comments) per country/destination per month.
    *   **Sentiment Analysis:** Applying Vader to text data (captions, comments, reviews) to generate sentiment scores (e.g., compound score).
    *   **Weighted Sentiment:** Calculating engagement-weighted sentiment scores (e.g., `sentiment_score * log(likes + comments + 1)`).
    *   **Economic Data:** Aligning economic indicators (Exchange Rate, GDP, Inflation) to monthly frequency, potentially involving interpolation or forward/backward filling.
    *   **Demand Indicators:** Processing Google Search Volume data and website traffic metrics, aggregating monthly.
    *   **Temporal Aggregation:** Grouping all data by `(country, year, month)`.
5.  **Structured Data Loading:** The processed, aggregated, and feature-engineered data is loaded into Supabase (PostgreSQL) tables. This structured format facilitates efficient querying for model training. The schema likely includes columns for each feature and the target variable, indexed by country and month.
6.  **Model Training:**
    *   Data is queried from Supabase for the relevant time period and countries.
    *   Features (X-Variables) and the Target Output (Y-Pred) are prepared.
    *   An XGBoost Regressor model is trained on the historical feature set to predict `visitors_per_month`.
    *   Hyperparameter tuning (e.g., using GridSearchCV or RandomizedSearchCV) is performed to optimize model performance.
    *   Model evaluation uses appropriate metrics for forecasting (e.g., RMSE, MAE, MAPE).
7.  **Prediction:** The trained XGBoost model is saved. It can then be loaded to predict future monthly visitor numbers based on the latest available feature data ingested through the pipeline.

---

## Target Output (Y-Pred)

*   **`visitors_per_month`**: The predicted number of tourist arrivals for a specific SEA country in a given month.

---

## Key Features (X-Variables)

Features are aggregated monthly per country:

*   **Economic Factors:**
    *   `exchange_rate_usd`: Local currency units per USD.
    *   `gdp_growth_rate`: Quarterly or Annual GDP growth (potentially interpolated to monthly).
    *   `inflation_rate`: Monthly or Annual inflation rate.
*   **Social Media & Web Data (Instagram):**
    *   `instagram_weighted_sentiment`: Monthly average sentiment score weighted by engagement (likes/comments).
*   **Social Media & Web Data (Reddit):**
    *   `reddit_mentions_count`: Monthly count of relevant posts/comments.
    *   `reddit_avg_sentiment`: Monthly average Vader sentiment score of posts/comments.
    *   *(Potentially)* `reddit_score_weighted_sentiment`: Sentiment weighted by post/comment scores.
*   **TripAdvisor & Review Sentiments:**
    *   `tripadvisor_mentions_count`: Monthly count of new reviews for relevant destinations/attractions.
    *   `tripadvisor_avg_rating`: Monthly average star rating.
    *   `tripadvisor_avg_sentiment`: Monthly average Vader sentiment score of review text.
*   **Demand Indicators:**
    *   `google_search_volume`: Monthly index/volume for relevant tourism keywords (e.g., "flights to [Country]", "[Country] vacation").
    *   `tourism_website_traffic`: Monthly traffic data for national tourism board websites (if available).

---

## Modeling Approach

Using XGBoost for time-series forecasting with the following process:

Feature selection
Hyperparameter tuning
Cross-validation
Model training by country
Model artifacts stored in trained_models/ directory by country.

## Repository Structure
```bash
├── backend/                  # Core prediction engine
│   ├── configs/              # Configuration files
│   ├── data/                 # Data storage directory
│   │   ├── processed/        # Transformed data
│   │   └── raw/              # Raw scraped data
│   ├── env/                  # Python environment
│   ├── notebooks/            # Jupyter notebooks for exploration
│   ├── src/                  # Source code
│   │   ├── data_pipeline.py  # Main ETL orchestration
│   │   ├── currency/         # Currency conversion utilities
│   │   ├── google_trends/    # Google search data collection
│   │   ├── instagram/        # Instagram scraping modules
│   │   ├── model/            # ML modeling components
│   │   ├── mongodb/          # MongoDB connection and queries
│   │   ├── reddit/           # Reddit data collection
│   │   ├── sentiment_analyzer/# NLP sentiment analysis
│   │   ├── services/         # Utility services
│   │   ├── supabase/         # Supabase DB operations
│   │   ├── tiktok/           # TikTok data collection (future)
│   │   └── tripadvisor/      # TripAdvisor scraping modules
│   └── trained_models/       # Country-specific models
├── frontend/                 # Visualization interface
└── .env                      # Environment variables
```