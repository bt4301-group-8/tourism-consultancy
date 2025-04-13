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
        self.instagram_df = self.mongodb.find_all("posts", "instagram")
        self.reddit_df = self.mongodb.find_all("posts", "reddit_submissions")
        self.tripadvisor_df = self.mongodb.find_all("posts", "tripadvisor")
        self.labels_df = self.mongodb.find_all("labels", "visitor_count")
        self.currency_df = self.mongodb.find_all("factors", "currency")
        self.trends_df = self.mongodb.find_all("factors", "google_trends")
        
    def read_from_mongodb(self, db_name, collection_name):
        data = self.mongodb.find_all(db_name, collection_name)
        return data
    
    def insert_to_supabase(self, table_name, data: dict):
        self.supabase.insert_data(table_name, data)

    def insert_calendar_data(self, date_from: str, date_to: str):
        self.supabase.insert_calendar_data(date_from, date_to)

    def get_as_dict(df):
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

    def engineer_google_trends(self):
        """engineer on self.trends_df"""
        self.trends_df["month_year"] = pd.to_datetime(self.trends_df["month_year"]).dt.strftime('%Y-%m')
        self.trends_df = self.trends_df.sort_values(by=["country", "month_year"]).reset_index(drop=True)
        
        # Lag value by 1 month
        self.trends_df["google_trend_score_lag1"] = (
            self.trends_df.groupby("country")["value"].shift(1)
        )
        
        # Rename value to a more informative column name
        self.trends_df = self.trends_df.rename(columns={"value": "google_trend_score"})
    

    def engineer_currency(self):
        """Engineer features from self.currency_df into a clean, country-level currency DataFrame."""
        self.currency_df.rename(columns={
            "YearMonth": "month_year",
            "Currency": "currencycode",
            "AverageRate": "average_currency_rate"
        }, inplace=True)

        sea_currency_map = {
            "SGD": "Singapore",
            "MYR": "Malaysia",
            "THB": "Thailand",
            "IDR": "Indonesia",
            "VND": "Vietnam",
            "PHP": "Philippines",
            "MMK": "Myanmar",
            "KHR": "Cambodia",
            "LAK": "Laos",
            "BND": "Brunei"
        }
        self.currency_df['country'] = self.currency_df['currencycode'].map(sea_currency_map)
        self.currency_df["month_year"] = pd.to_datetime(self.currency_df["month_year"]).dt.strftime('%Y-%m')
        self.currency_df = self.currency_df[["month_year", "country", "average_currency_rate"]]
        self.currency_df = self.currency_df.sort_values(by=["country", "month_year"]).reset_index(drop=True)
        self.currency_df["average_currency_rate_lag1"] = (
            self.currency_df.groupby("country")["average_currency_rate"].shift(1)
        )

    # def engineer_instagram(self):
    #     df = self.instagram_df.copy()

    #     # Keep relevant columns and rename
    #     df = df.rename(columns={
    #         # "created_at": "month_year",
    #         "vader_compound": "ig_sentiment",
    #         "like_count": "likes"
    #     })
    #     df["month_year"] = pd.to_datetime(df["month_year"], format="%Y-%m-%d").dt.strftime("%Y-%m")

    #     # Weight the raw sentiment using like count
    #     total_likes_per_group = df.groupby(["country", "month_year"])["likes"].transform("sum")
    #     df["like_percentage"] = df["likes"] / total_likes_per_group
    #     df["ig_sentiment"] = df["ig_sentiment"] * df["like_percentage"]

    #     df = df.drop(columns=["likes", "like_percentage"])
    #     df = df.groupby(["country", "month_year"]).agg({
    #         "ig_sentiment": "sum"
    #     }).reset_index()

    #     # Interpolate and fill missing
    #     df = df.sort_values(["country", "month_year"])
    #     df["ig_sentiment"] = (
    #         df.groupby("country")["ig_sentiment"]
    #         .apply(lambda group: (
    #             group.interpolate(method='linear', limit_direction='both')
    #                 .fillna(method='ffill')
    #                 .fillna(method='bfill')
    #         ))
    #         .reset_index(level=0, drop=True)
    #     )

    #     df["ig_sentiment_lag1"] = (
    #         df.groupby("country")["ig_sentiment"]
    #         .shift(1)
    #     )

    #     self.instagram_df = df

    def engineer_instagram(self):
        df = self.instagram_df.copy()
        df["month_year"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m")

        df["likes"] = df["like_count"]
        total_likes_per_group = df.groupby(["country", "month_year"])["likes"].transform("sum")
        df["like_percentage"] = df["likes"] / total_likes_per_group
        df["ig_sentiment"] = df["sentiment_score"] * df["like_percentage"]

        df = df.groupby(["country", "month_year"]).agg({
            "ig_sentiment": "sum"
        }).reset_index()

        df = df.sort_values(["country", "month_year"])
        df["ig_sentiment"] = (
            df.groupby("country")["ig_sentiment"]
            .apply(lambda group: (
                group.interpolate(method='linear', limit_direction='both')
                    .fillna(method='ffill')
                    .fillna(method='bfill')
            ))
            .reset_index(level=0, drop=True)
        )

        # Step 6: Add lag feature
        df["ig_sentiment_lag1"] = (
            df.groupby("country")["ig_sentiment"]
            .shift(1)
        )

        self.instagram_df = df


    def engineer_reddit(self):
        df = self.reddit_df.copy()

        # Keep only relevant columns
        columns_to_keep = ["created_at", "score", "vader_compound", "country"]
        df = df[columns_to_keep]

        # Rename and convert time
        df = df.rename(columns={
            "created_at": "month_year",
            "vader_compound": "reddit_sentiment",
            "score": "popularity"
        })
        df["month_year"] = pd.to_datetime(df["month_year"], format="%m/%d/%y").dt.strftime("%Y-%m")

        # Weight the raw sentiment using popularity
        total_popularity_per_group = df.groupby(["country", "month_year"])["popularity"].transform("sum")
        df["popularity_percentage"] = df["popularity"] / total_popularity_per_group
        df["reddit_sentiment"] = df["reddit_sentiment"] * df["popularity_percentage"]

        # Drop unused columns and aggregate
        df = df.drop(columns=["popularity", "popularity_percentage"])
        df = df.groupby(["country", "month_year"]).agg({
            "reddit_sentiment": "sum"
        }).reset_index()

        # Interpolate and fill missing values
        df = df.sort_values(["country", "month_year"])
        df["reddit_sentiment"] = (
            df.groupby("country")["reddit_sentiment"]
            .apply(lambda group: (
                group.interpolate(method='linear', limit_direction='both')
                    .fillna(method='ffill')
                    .fillna(method='bfill')
            ))
            .reset_index(level=0, drop=True)
        )

        # Lag by 1 month
        df["reddit_sentiment_lag1"] = (
            df.groupby("country")["reddit_sentiment"]
            .shift(1)
        )

        self.reddit_df = df


    def engineer_tripadvisor(self):
        df = self.tripadvisor_df.copy()  # Work on a copy to preserve the original

        # Rename columns for consistency
        df = df.rename(columns={
            "created_at": "month_year",
            "vader_compound": "tripadvisor_sentiment"
        })

        # Convert to "YYYY-MM" format for monthly granularity
        df["month_year"] = pd.to_datetime(df["month_year"]).dt.strftime("%Y-%m")

        # Group by country and month, take average sentiment per month
        df = df.groupby(["country", "month_year"]).agg({
            "tripadvisor_sentiment": "mean"
        }).reset_index()

        # Handle missing values
        df = df.sort_values(["country", "month_year"])
        df["tripadvisor_sentiment"] = (
            df.groupby("country")["tripadvisor_sentiment"]
            .apply(lambda group: (
                group.interpolate(method='linear', limit_direction='both')
                    .fillna(method='ffill')
                    .fillna(method='bfill')
            ))
            .reset_index(level=0, drop=True)
        )

        # Add a 1-month lag feature for sentiment
        df["tripadvisor_sentiment_lag1"] = (
            df.groupby("country")["tripadvisor_sentiment"]
            .shift(1)
        )

        self.tripadvisor_df = df  # Store back the engineered DataFrame

    def engineer_labels(self):
        df = self.labels_df.copy()

        # Ensure proper time format
        df["month_year"] = pd.to_datetime(df["month_year"]).dt.strftime("%Y-%m")

        # Rename value column
        df = df.rename(columns={"value": "visitor_count"})

        # Sort for lagging
        df = df.sort_values(["country", "month_year"])

        # Add 1-month lag
        df["visitor_count_lag1"] = (
            df.groupby("country")["visitor_count"].shift(1)
        )

        self.labels_df = df


    def process(self):
        self._get_sentiment_instagram()
        self._get_sentiment_reddit()
        """code for feature engineering"""
        self.engineer_google_trends()
        self.engineer_currency()
        self.engineer_instagram()
        self.engineer_reddit()
        self.engineer_tripadvisor()
        self.engineer_labels()

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