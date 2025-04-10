import pandas as pd
import json
import re
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from googletrans import Translator
from tqdm import tqdm
import time
import langid

nltk.download('vader_lexicon')

class InstagramProcessor:
    def __init__(self, mongodb):
        self.mongodb = mongodb
        self.df = None
        self.analyzer = SentimentIntensityAnalyzer()
        self.translator = langid

    def load_data(self):
        # try:
        #     # with open(self.file_path, 'r', encoding='utf-8') as f:
        #     #     data = json.load(f)
        #     dfs = []
        #     for country, posts in data.items():
        #         df = pd.json_normalize(posts)
        #         df['country'] = country
        #         dfs.append(df)
        #     self.df = pd.concat(dfs, ignore_index=True)
        # except FileNotFoundError:
        #     print(f"Error: File not found at {self.file_path}")
        #     self.df = None
        # except json.JSONDecodeError:
        #     print(f"Error: Invalid JSON format in {self.file_path}")
        #     self.df = None
        self.df = self.mongodb.find_all("posts", "instagram")

    def preprocess_data(self):
        if self.df is None:
            return
        if 'location' in self.df.columns:
            self.df = self.df.drop('location', axis=1)
        if 'date' in self.df.columns:
            self.df['date'] = pd.to_datetime(self.df['date'])
            self.df['date_only'] = self.df['date'].dt.date
            self.df['time_only'] = self.df['date'].dt.time
            self.df['month'] = self.df['date'].dt.month

    def clean_text(self, text):
        text = re.sub(r'[^\w\s,.!?;]', '', text)
        text = re.sub(r'http\S+|www\S+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    def clean_captions(self):
        if self.df is None:
            return
        if 'caption' in self.df.columns:
            self.df['cleaned_caption'] = self.df['caption'].apply(self.clean_text)
        else:
            print("Warning: No 'caption' column found for cleaning.")
            self.df['cleaned_caption'] = '' # Or handle differently

    def detect_language(self, text):
        try:
            lang, _ = self.translator.classify(text)
            return lang
        except Exception as e:
            return 'unknown'

    def translate_to_english(self, text):
        try:
            return self.translator.translate(text, dest='en').text
        except Exception as e:
            return None

    def analyze_and_translate_languages(self):
        if self.df is None or 'cleaned_caption' not in self.df.columns:
            return

        self.df['language'] = self.df['cleaned_caption'].apply(self.detect_language)
        non_english_df = self.df[self.df['language'] != 'en'].copy()
        to_drop = []
        total_non_english = non_english_df.shape[0]
        processed_count = 0

        start_time = time.time()
        for index, row in non_english_df.iterrows():
            if row['language'] == 'unknown':
                to_drop.append(index)
            else:
                translated_text = self.translate_to_english(row["cleaned_caption"])
                if translated_text:
                    new_language = self.detect_language(translated_text)
                    if new_language == 'en':
                        non_english_df.at[index, "cleaned_caption"] = translated_text
                        non_english_df.at[index, "language"] = 'en'
                    else:
                        to_drop.append(index)
                else:
                    to_drop.append(index)

            processed_count += 1
            if processed_count % 10 == 0 and processed_count > 0:
                time_taken = time.time() - start_time
                print(f"Time taken to process {processed_count} non-English rows for translation: {time_taken:.4f} seconds")
                start_time = time.time()
        print(f"dropping {len(to_drop)} rows")
        non_english_df.drop(to_drop, inplace=True)
        english_df = self.df[self.df['language'] == 'en'].copy()
        self.df = pd.concat([english_df, non_english_df], ignore_index=True)

    def analyze_sentiment(self):
        if self.df is None or 'cleaned_caption' not in self.df.columns:
            return
        self.df["sentiment_score"] = self.df["cleaned_caption"].apply(lambda text: self.analyzer.polarity_scores(text)["compound"])

    # def save_to_json(self, output_file):
    #     if self.df is None:
    #         return
    #     self.df.to_json(output_file, orient="records", date_format="iso")

    def get_as_dict(self):
        """Return the processed data as a dictionary"""
        if self.df is None:
            return {}
        return self.df.to_dict(orient="records")

    def run_analysis(self):
        self.load_data()
        if self.df is not None:
            self.preprocess_data()
            self.clean_captions()
            self.analyze_and_translate_languages()
            self.analyze_sentiment()
            # self.save_to_json(output_file)
            # self.df.to_json
            # result_dict = self.get_as_dict()
            # print(f"\nAnalysis complete. Output saved to {output_file}")
            return self.df


# processor_instagram = InstagramProcessor('country_posts.json')
# processor_instagram.run_analysis(output_file="sentiment_analysis_instagram_timed.json")
