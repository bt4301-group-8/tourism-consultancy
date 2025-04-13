from backend.src.mongodb.db import mongodb
from backend.src.supabase.supabase import supabase
from backend.src.sentiment_analyzer.sentiment_insta import InstagramProcessor
from backend.src.sentiment_analyzer.sentiment_reddit import RedditDataProcessor
import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler

class DataProcessor:
    def __init__(self):
        self.mongodb = mongodb
        self.supabase = supabase
        self.processor_instagram = InstagramProcessor(self.mongodb)
        self.processor_reddit = RedditDataProcessor(self.mongodb)
        #convert to dataframe
        self.instagram_df = pd.DataFrame(self.mongodb.find_all("posts", "instagram"))
        self.reddit_df = pd.DataFrame(self.mongodb.find_all("posts", "reddit_submissions"))
        self.tripadvisor_df = pd.DataFrame(self.mongodb.find_all("posts", "tripadvisor"))
        self.labels_df = pd.DataFrame(self.mongodb.find_all("labels", "visitor_count"))
        self.currency_df = pd.DataFrame(self.mongodb.find_all("factors", "currency"))
        self.trends_df = pd.DataFrame(self.mongodb.find_all("factors", "google_trends"))
        
    def read_from_mongodb(self, db_name, collection_name):
        data = self.mongodb.find_all(db_name, collection_name)
        return data
    
    def insert_to_supabase(self, table_name, data: dict):
        self.supabase.insert_data(table_name, data)

    def insert_calendar_data(self, date_from: str, date_to: str):
        self.supabase.insert_calendar_data(date_from, date_to)

    def get_as_dict(self, df):
        if df is None:
            return {}
        return df.to_dict(orient="records")
    
    def get_all_dicts(self):
        instagram_dict = self.get_as_dict(self.instagram_df)
        reddit_dict = self.get_as_dict(self.reddit_df)
        tripadvisor_dict = self.get_as_dict(self.tripadvisor_df)
        labels_dict = self.get_as_dict(self.labels_df)
        currency_dict = self.get_as_dict(self.currency_df)
        trends_df = self.get_as_dict(self.trends_df)
        return instagram_dict, reddit_dict, tripadvisor_dict, labels_dict, currency_dict, trends_df

    def _get_sentiment_instagram(self):
        self.instagram_df = self.processor_instagram.run_analysis()
    
    def _get_sentiment_reddit(self):
        self.reddit_df = self.processor_reddit.run_analysis()

    # #helper function for data cleaning
    # def fill_country_month_grid(df):
    #     """
    #     Create country, month_year combinations based on the global month range.
    #     df : Must contain 'country' and 'month_year' columns (datetime or string)
    #     """
    #     df = df.copy()
    #     # Ensure proper datetime format
    #     df["month_year"] = pd.to_datetime(df["month_year"]).dt.to_period("M").dt.to_timestamp()
    #     # Generate full month range and unique countries
    #     month_range = pd.date_range(
    #         start=df["month_year"].min(),
    #         end=df["month_year"].max(),
    #         freq="MS"
    #     )
    #     countries = df["country"].unique()
    #     # Create full country-month index
    #     expanded_rows = [
    #         {"country": country, "month_year": month}
    #         for country in countries
    #         for month in month_range
    #     ]
    #     expected_df = pd.DataFrame(expanded_rows)
    #     # Merge with original data
    #     merged_df = expected_df.merge(
    #         df,
    #         on=["country", "month_year"],
    #         how="left"
    #     )
    #     return merged_df


    def engineer_google_trends(self):
        """engineer on self.trends_df"""
        df = self.trends_df.copy()
        # Rename and format date
        df = df.rename(columns={"value": "google_trend_score"})
        df["month_year"] = pd.to_datetime(df["month_year"]).dt.strftime('%Y-%m')
        # # cleaning
        # df = fill_country_month_grid(df)
        # df["google_trend_score"] = (
        #     df
        #     .groupby("country")["google_trend_score"]
        #     .apply(lambda group: (
        #         group.interpolate(method='linear', limit_direction='both')  # interpolate
        #             .fillna(method='ffill')                                # fill leading NaNs
        #             .fillna(method='bfill')                                # fill trailing NaNs
        #     ))
        #     .reset_index(level=0, drop=True)
        # )
        # Lag value by 1 month
        df = df.sort_values(by=["country", "month_year"]).reset_index(drop=True)
        df["google_trend_score_lag1"] = (
            df.groupby("country")["google_trend_score"].shift(1)
        )
        self.trends_df = df
    

    def engineer_currency(self):
        """Engineer features on self.currency_df: rename, map, transform, and lag."""

        # Rename columns to standardized names
        self.currency_df = self.currency_df.rename(columns={
            "Currency": "country",
            "YearMonth": "month_year",
            "AverageRate": "avg_currency_rate"
        })

        # Map currency codes to country names
        currency_to_country = {
            "BND": "Brunei",
            "IDR": "Indonesia",
            "KHR": "Cambodia",
            "LAK": "Laos",
            "MMK": "Myanmar",
            "MYR": "Malaysia",
            "PHP": "Philippines",
            "SGD": "Singapore",
            "THB": "Thailand",
            "VND": "Vietnam"
        }
        self.currency_df["country"] = self.currency_df["country"].map(currency_to_country)

        # Convert month-year to proper format
        self.currency_df["month_year"] = pd.to_datetime(self.currency_df["month_year"]).dt.strftime('%Y-%m')

        # Log-transform exchange rates
        self.currency_df["avg_currency_rate"] = np.log1p(self.currency_df["avg_currency_rate"])

        # Sort before lag
        self.currency_df = self.currency_df.sort_values(by=["country", "month_year"]).reset_index(drop=True)

        # Add 1-month lag
        self.currency_df["avg_currency_rate_lag1"] = (
            self.currency_df.groupby("country")["avg_currency_rate"].shift(1)
        )

    def engineer_instagram(self):
        df = self.instagram_df.copy()
        # capitalise the first letter of countries
        df["country"] = df["country"].str.title()
        #change month_year format to align with other datasets
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['month_year'] = df['date'].dt.strftime('%Y-%m')
        
        # Weight the raw sentiment using like count
        total_likes_per_group = df.groupby(["country", "month_year"])["like_count"].transform("sum")
        df["like_percentage"] = df["like_count"] / total_likes_per_group
        df["weighted_sentiment_score"] = df["sentiment_score"] * df["like_percentage"]
        #only keeping the sentiment score
        columns_to_keep = [
            "month_year",
            "country",
            "sentiment_score",
            "weighted_sentiment_score"
        ]
        df = df[columns_to_keep]
        #agggregate sentiment score
        ig_sentiment = df.groupby(
            ["country", "month_year"]
        )["weighted_sentiment_score"].sum().reset_index()
        ig_sentiment.columns = ["country", "month_year", "ig_sentiment"]
        ig_sentiment['month_year'] = pd.to_datetime(ig_sentiment['month_year'])
        # Interpolate and fill missing
        ig_sentiment = ig_sentiment.sort_values(['country', 'month_year'])
        ig_sentiment["ig_sentiment"] = (
            ig_sentiment
            .groupby("country")["ig_sentiment"]
            .apply(lambda group: (
                group.interpolate(method='linear', limit_direction='both')  # interpolate
                    .fillna(method='ffill')                                # fill leading NaNs
                    .fillna(method='bfill')                                # fill trailing NaNs
            ))
            .reset_index(level=0, drop=True)
        )

        #standardise across each country for entire dataset
        ig_sentiment["ig_sentiment_z"] = (
            ig_sentiment.groupby("country")["ig_sentiment"]
            .transform(lambda x: (x - x.mean()) / x.std())
        )

        #lag by 1 moth
        ig_sentiment['ig_sentiment_lag1'] = (
            ig_sentiment.groupby('country')['ig_sentiment']
            .shift(periods=1)  # Shift down by 1 row
        )
        self.instagram_df = ig_sentiment

    # def engineer_reddit(self):
    #     df = self.reddit_df.copy()

    #     # Keep only relevant columns
    #     columns_to_keep = ["created_at", "score", "vader_compound", "country"]
    #     df = df[columns_to_keep]

    #     # Rename and convert time
    #     df = df.rename(columns={
    #         "created_at": "month_year",
    #         "vader_compound": "reddit_sentiment",
    #         "score": "popularity"
    #     })
    #     df["month_year"] = pd.to_datetime(df["month_year"], format="%m/%d/%y").dt.strftime("%Y-%m")

    #     # Weight the raw sentiment using popularity
    #     total_popularity_per_group = df.groupby(["country", "month_year"])["popularity"].transform("sum")
    #     df["popularity_percentage"] = df["popularity"] / total_popularity_per_group
    #     df["reddit_sentiment"] = df["reddit_sentiment"] * df["popularity_percentage"]

    #     # Drop unused columns and aggregate
    #     df = df.drop(columns=["popularity", "popularity_percentage"])
    #     df = df.groupby(["country", "month_year"]).agg({
    #         "reddit_sentiment": "sum"
    #     }).reset_index()

    #     # Interpolate and fill missing values
    #     df = df.sort_values(["country", "month_year"])
    #     df["reddit_sentiment"] = (
    #         df.groupby("country")["reddit_sentiment"]
    #         .apply(lambda group: (
    #             group.interpolate(method='linear', limit_direction='both')
    #                 .fillna(method='ffill')
    #                 .fillna(method='bfill')
    #         ))
    #         .reset_index(level=0, drop=True)
    #     )

    #     # Lag by 1 month
    #     df["reddit_sentiment_lag1"] = (
    #         df.groupby("country")["reddit_sentiment"]
    #         .shift(1)
    #     )

    #     self.reddit_df = df


    def engineer_tripadvisor(self):
        df = self.tripadvisor_df.copy()
        print(df.shape)
        # Convert trip_date to datetime and extract month-year
        df["trip_date"] = pd.to_datetime(df["trip_date"], format="%b-%y", errors="coerce")
        df["month_year"] = df["trip_date"].dt.strftime("%Y-%m")
        columns_to_keep = [
            "month_year",
            "country",
            "rating"
        ]
        df = df[columns_to_keep]
        print(df.shape)
        self.tripadvisor_df = df
        # aggregate to find average monthly rating
        # review_agg = df.groupby(["country", "month_year"])["rating"].mean().reset_index()
        # print(review_agg.shape)
        # review_agg = review_agg.rename(columns={"rating": "trip_advisor_rating"})
        # review_agg['month_year'] = pd.to_datetime(review_agg['month_year'])

        # # interpolate, forward fill, then backward fill within each country group
        # review_agg["trip_advisor_rating"] = (
        #     review_agg
        #     .groupby("country")["trip_advisor_rating"]
        #     .apply(lambda group: (
        #         group.interpolate(method='linear', limit_direction='both')  # interpolate
        #             .fillna(method='ffill')                                # fill leading NaNs
        #             .fillna(method='bfill')                                # fill trailing NaNs
        #     ))
        #     .reset_index(level=0, drop=True)
        # )
        # print(review_agg.shape)
        # self.tripadvisor_df = review_agg


    def engineer_labels(self):
        df = self.labels_df.copy()
        #Rename and format date
        df = df.rename(columns={"value": "num_visitors"})
        df["month_year"] = pd.to_datetime(df["month_year"]).dt.strftime("%Y-%m")
        # log transform
        df["log_visitors"] = np.log1p(df['num_visitors'])
        # interpolate, forward fill, then backward fill within each country group
        df["log_visitors"] = (
            df
            .groupby("country")["log_visitors"]
            .apply(lambda group: (
                group.interpolate(method='linear', limit_direction='both')  # interpolate
                    .fillna(method='ffill')                                # fill leading NaNs
                    .fillna(method='bfill')                                # fill trailing NaNs
            ))
            .reset_index(level=0, drop=True)
        )
        df.drop(columns=["num_visitors"], inplace=True)
        self.labels_df = df


    def process(self):
        self._get_sentiment_instagram()
        self._get_sentiment_reddit()
        """code for feature engineering"""
        self.engineer_google_trends()
        self.engineer_currency()
        self.engineer_instagram()
        # self.engineer_reddit()
        self.engineer_tripadvisor()
        self.engineer_labels()
        #TODO: drop irrelvant columns and rename before ingesting data to supabase, refer to schema for more details

        """code for ingesting data to supabase"""
        instagram_dict, reddit_dict, tripadvisor_dict, labels_dict, currency_dict, trends_df = self.get_all_dicts()
        # self.insert_to_supabase("instagram", instagram_dict)
        # self.insert_to_supabase("reddit", reddit_dict)
        # self.insert_to_supabase("tripadvisor", tripadvisor_dict)
        # self.insert_to_supabase("labels", labels_dict)
        # self.insert_to_supabase("currency", currency_dict)
        # self.insert_to_supabase("google_trends", trends_df)

# if __name__ == "__main__":
#     data_processor = DataProcessor()