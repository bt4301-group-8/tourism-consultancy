from backend.src.mongodb.db import mongodb
from backend.src.supabase.supabase import supabase
from backend.src.sentiment_analyzer.sentiment_insta import InstagramProcessor
from backend.src.sentiment_analyzer.sentiment_reddit import RedditDataProcessor
import pandas as pd

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
        pass

    def engineer_currency(self):
        """engineer on self.currency_df"""
        pass

    def engineer_instagram(self):
        pass

    def engineer_reddit(self):
        pass

    def engineer_tripadvisor(self):
        pass

    def engineer_labels(self):
        pass

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