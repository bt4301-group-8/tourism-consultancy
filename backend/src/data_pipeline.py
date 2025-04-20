from backend.src.mongodb.db import mongodb
from backend.src.supabase.supabase import supabase
from backend.src.sentiment_analyzer.sentiment_insta import InstagramProcessor
from backend.src.sentiment_analyzer.sentiment_reddit import RedditDataProcessor
import pandas as pd
import numpy as np
import math

class DataProcessor:
    def __init__(self):
        self.mongodb = mongodb
        self.supabase = supabase
        self.processor_instagram = InstagramProcessor(self.mongodb)
        self.processor_reddit = RedditDataProcessor(self.mongodb)
        self.instagram_df = pd.DataFrame(self.mongodb.find_all("posts", "instagram"))
        self.reddit_df = pd.DataFrame(self.mongodb.find_all("posts", "reddit_submissions"))
        self.tripadvisor_df = pd.DataFrame(self.mongodb.find_all("posts", "tripadvisor"))
        self.labels_df = pd.DataFrame(self.mongodb.find_all("labels", "visitor_count"))
        self.currency_df = pd.DataFrame(self.mongodb.find_all("factors", "currency"))
        self.trends_df = pd.DataFrame(self.mongodb.find_all("factors", "google_trends"))
        
    def read_from_mongodb(self, db_name, collection_name):
        data = self.mongodb.find_all(db_name, collection_name)
        return data
    
    def insert_to_supabase(self, table_name, data):
        serialized_data = self.serialize_dates(data)

        try:
            existing = self.supabase.get_data(table_name)
            existing_keys = {(row["country"], row["month_year"]) for row in existing}
        except Exception as e:
            print(f"[ERROR] Failed to fetch existing keys from '{table_name}': {e}")
            existing_keys = set()

        try:
            calendar = self.supabase.get_data("calendar")
            valid_months = {row["month_year"] for row in calendar}
        except Exception as e:
            print(f"[ERROR] Failed to fetch calendar month_years: {e}")
            valid_months = set()

        if not existing_keys:
            filtered_data = [record for record in serialized_data if record['month_year'] in valid_months]
        else:
            new_keys = {(record['country'], record['month_year']) for record in serialized_data if 'country' in record}
            filtered_data = [
                record for record in serialized_data
                if (record.get('country'), record['month_year']) not in existing_keys
                and record['month_year'] in valid_months
            ]

        if not filtered_data:
            print(f"[SKIPPED] No valid new records to insert into '{table_name}'.")
            return

        cleaned_data = self.sanitize_json_records(filtered_data)
        self.supabase.insert_data(table_name, cleaned_data)
        print(f"[INSERTED] {len(cleaned_data)} records into '{table_name}'.")

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
        df = df.sort_values(by=["country", "month_year"]).reset_index(drop=True)
        df["google_trend_score_lag1"] = (
            df.groupby("country")["google_trend_score"].shift(1)
        )
        self.trends_df = df[["country", "month_year", "google_trend_score", "google_trend_score_lag1"]]
    

    def engineer_currency(self):
        """Engineer features on self.currency_df: rename, map, transform, and lag."""

        self.currency_df = self.currency_df.rename(columns={
            "Currency": "country",
            "YearMonth": "month_year",
            "AverageRate": "avg_currency_rate"
        })

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
        self.currency_df["month_year"] = pd.to_datetime(self.currency_df["month_year"]).dt.strftime('%Y-%m')
        self.currency_df["log_avg_currency_rate"] = np.log1p(self.currency_df["avg_currency_rate"])
        self.currency_df = self.currency_df.sort_values(by=["country", "month_year"]).reset_index(drop=True)
        self.currency_df["log_avg_currency_rate_lag1"] = (
            self.currency_df.groupby("country")["log_avg_currency_rate"].shift(1)
        )
        self.currency_df = self.currency_df[["country", "month_year", "log_avg_currency_rate", "log_avg_currency_rate_lag1"]]

    def engineer_instagram(self):
        df = self.instagram_df.copy()
        df["country"] = df["country"].str.title()
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df['month_year'] = df['date'].dt.strftime('%Y-%m')
        total_likes_per_group = df.groupby(["country", "month_year"])["like_count"].transform("sum")
        df["like_percentage"] = df["like_count"] / total_likes_per_group
        df["weighted_sentiment_score"] = df["sentiment_score"] * df["like_percentage"]
        columns_to_keep = [
            "month_year",
            "country",
            "sentiment_score",
            "weighted_sentiment_score"
        ]
        df = df[columns_to_keep]
        ig_sentiment = df.groupby(
            ["country", "month_year"]
        )["weighted_sentiment_score"].sum().reset_index()
        ig_sentiment.columns = ["country", "month_year", "ig_sentiment"]
        ig_sentiment['month_year'] = pd.to_datetime(ig_sentiment['month_year'])
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
        self.instagram_df = ig_sentiment[["country", "month_year", "ig_sentiment", "ig_sentiment_lag1", "ig_sentiment_z"]]

    def engineer_reddit(self):
        df = self.reddit_df.copy()

        # Step 1: Format date
        df["month_year"] = pd.to_datetime(df["month_year"], format="%m-%Y", errors="coerce").dt.strftime("%Y-%m")

        # Step 2: Ensure 'mentioned_countries' is list-like
        df["mentioned_countries"] = df["mentioned_countries"].apply(
            lambda x: [i.strip().title() for i in x.split(",")] if isinstance(x, str) else []
        )

        # Step 3: Explode countries
        df = df.explode("mentioned_countries").rename(columns={"mentioned_countries": "country"})
        df = df[df["country"].notna() & (df["country"] != "")]

        # Step 4: Compute weighted sentiment
        df["reddit_sentiment"] = df["upvote_ratio"] * df["sentiment_score"]

        # Step 5: Aggregate by country and month
        avg_sentiment = (
            df.groupby(["country", "month_year"])["reddit_sentiment"]
            .mean()
            .reset_index()
        )

        # Step 6: Filter SEA countries only
        sea_countries = {
            "Singapore", "Malaysia", "Thailand", "Indonesia", "Vietnam",
            "Philippines", "Myanmar", "Cambodia", "Laos", "Brunei"
        }
        avg_sentiment = avg_sentiment[avg_sentiment["country"].isin(sea_countries)]

        # Step 7: Interpolation
        avg_sentiment["month_year"] = pd.to_datetime(avg_sentiment["month_year"])
        avg_sentiment = avg_sentiment.sort_values(["country", "month_year"])
        avg_sentiment["reddit_sentiment"] = (
            avg_sentiment
            .groupby("country")["reddit_sentiment"]
            .apply(lambda group: (
                group.interpolate(method="linear", limit_direction="both")
                    .fillna(method="ffill")
                    .fillna(method="bfill")
            ))
            .reset_index(level=0, drop=True)
        )

        # Step 8: Z-score
        avg_sentiment["reddit_sentiment_z"] = (
            avg_sentiment.groupby("country")["reddit_sentiment"]
            .transform(lambda x: (x - x.mean()) / x.std())
        )

        # Step 9: Lag
        avg_sentiment["reddit_sentiment_lag1"] = (
            avg_sentiment.groupby("country")["reddit_sentiment_z"]
            .shift(1)
        )

        # Step 10: Final selection
        self.reddit_df = avg_sentiment[[
            "month_year", "country", "reddit_sentiment_z", "reddit_sentiment_lag1"
        ]]


    def engineer_tripadvisor(self):
        df = self.tripadvisor_df.copy()
        # Convert trip_date to datetime and extract month-year
        df["trip_date"] = pd.to_datetime(df["trip_date"], format="%b-%y", errors="coerce")
        df["month_year"] = df["trip_date"].dt.strftime("%Y-%m")
        columns_to_keep = [
            "month_year",
            "country",
            "rating"
        ]
        df = df[columns_to_keep]
        df["rating"] = pd.to_numeric(df["rating"], errors="coerce")
        review_agg = df.groupby(["country", "month_year"])["rating"].mean().reset_index()
        review_agg = review_agg.rename(columns={"rating": "trip_advisor_rating"})
        review_agg['month_year'] = pd.to_datetime(review_agg['month_year'])

        # interpolate, forward fill, then backward fill within each country group
        review_agg["trip_advisor_rating"] = (
            review_agg
            .groupby("country")["trip_advisor_rating"]
            .apply(lambda group: (
                group.interpolate(method='linear', limit_direction='both')  # interpolate
                    .fillna(method='ffill')                                # fill leading NaNs
                    .fillna(method='bfill')                                # fill trailing NaNs
            ))
            .reset_index(level=0, drop=True)
        )
        review_agg["trip_advisor_rating_lag1"] = (
            review_agg.groupby("country")["trip_advisor_rating"].shift(1)
        )

        self.tripadvisor_df = review_agg[[
            "trip_advisor_rating", 
            "trip_advisor_rating_lag1", 
            "country", 
            "month_year"
        ]]

    def engineer_labels(self):
        df = self.labels_df.copy()
        #Rename and format date
        df = df.rename(columns={"value": "num_visitors"})
        df["num_visitors"] = df["num_visitors"].round().astype(int)
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
        self.labels_df = df
        self.labels_df = self.labels_df[["country", "month_year", "log_visitors", "num_visitors"]]

    def serialize_dates(self, data):
        """Convert any pandas.Timestamp in a list of dicts to string."""
        for record in data:
            for key, value in record.items():
                if isinstance(value, pd.Timestamp):
                    record[key] = value.strftime('%Y-%m')
        return data

    def sanitize_json_records(self, data):
        """Convert non-serializable float values to None (e.g., NaN, inf)."""
        sanitized = []
        for record in data:
            clean_record = {}
            for key, value in record.items():
                if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
                    clean_record[key] = None
                else:
                    clean_record[key] = value
            sanitized.append(clean_record)
        return sanitized


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
        self.insert_to_supabase("instagram_post", instagram_dict)
        self.insert_to_supabase("reddit_post", reddit_dict)
        self.insert_to_supabase("tripadvisor_post", tripadvisor_dict)
        self.insert_to_supabase("visitor_count", labels_dict)
        self.insert_to_supabase("currency", currency_dict)
        self.insert_to_supabase("google_trends", trends_df)

# if __name__ == "__main__":
#     data_processor = DataProcessor()