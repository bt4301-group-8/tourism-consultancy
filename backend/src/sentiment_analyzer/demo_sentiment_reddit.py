import pandas as pd
import json
import re
import nltk
from googletrans import Translator
from tqdm import tqdm
import time
from nltk.sentiment.vader import SentimentIntensityAnalyzer
import langid
nltk.download('vader_lexicon')

class RedditDataProcessor:
    """
    A class to process Reddit post data, including cleaning, language detection,
    translation, and sentiment analysis with periodic time updates.
    """
    def __init__(self, mongodb):
        """
        Initializes the RedditDataProcessor with the path to the JSON file.

        Args:
            json_file (str): The path to the JSON file containing Reddit data.
        """
        # self.json_file = json_file
        self.mongodb = mongodb
        self.df = self._load_and_preprocess_data()
        self.translator = langid
        self.sentiment_analyzer = SentimentIntensityAnalyzer()

    def _load_and_preprocess_data(self):
        """
        Loads JSON data from the file and performs initial preprocessing.

        Returns:
            pd.DataFrame: The initial preprocessed DataFrame.
        """
        try:
            # with open(self.json_file, 'r', encoding='utf-8') as f:
            #     data = json.load(f)
            df = self.mongodb.find_all("demo", "posts.reddit_submissions")

            # Convert `created_utc` to datetime
            df['created_utc'] = pd.to_datetime(df['created_utc'], unit='s', utc=True)

            # Convert list columns to comma-separated strings
            for col in ['mentioned_countries', 'mentioned_cities']:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')

            # Drop duplicates
            df = df.drop_duplicates(keep="first")

            # Split into separate 'date' and 'time' columns
            df['date_only'] = df['created_utc'].dt.date
            df['time_only'] = df['created_utc'].dt.time

            # Extract month
            df['month'] = df['created_utc'].dt.month
            return df
        except FileNotFoundError:
            print(f"Error: File not found at {self.json_file}")
            return None
        except json.JSONDecodeError:
            print(f"Error: Invalid JSON format in {self.json_file}")
            return None

    def clean_text(self, text):
        """
        Cleans the input text by removing emojis, URLs, and extra spaces.

        Args:
            text (str): The text to clean.

        Returns:
            str: The cleaned text.
        """
        text = re.sub(r'[^\w\s,.!?;]', '', text)
        text = re.sub(r'http\S+|www\S+', '', text)
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    def detect_language(self, text):
        try:
            lang, _ = self.translator.classify(text)
            return lang
        except Exception as e:
            return 'unknown'

    def translate_to_english(self, text):
        """
        Translates the input text to English using Google Translate.

        Args:
            text (str): The text to translate.

        Returns:
            str: The translated text in English or None on error.
        """
        try:
            text = self.translator.translate(text, dest='en').text
            return text
        except Exception:
            return None

    def analyze_and_translate_languages(self):
        """
        Analyzes and translates non-English text in the DataFrame to English,
        reporting time taken every 10 rows.

        Returns:
            pd.DataFrame: The DataFrame with translated text and updated language labels.
        """
        if self.df is None or 'selftext' not in self.df.columns:
            return self.df

        self.df['cleaned_caption'] = self.df['selftext'].apply(self.clean_text)
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
                        self.df.at[index, "cleaned_caption"] = translated_text
                        self.df.at[index, "language"] = 'en'
                    else:
                        to_drop.append(index)
                else:
                    to_drop.append(index)

            processed_count += 1
            if processed_count % 10 == 0 and processed_count > 0:
                time_taken = time.time() - start_time
                print(f"Time taken to process {processed_count} non-English rows for translation: {time_taken:.4f} seconds")
                start_time = time.time()

        self.df.drop(to_drop, inplace=True)
        return self.df

    def analyze_sentiment(self, text_column='cleaned_caption', output_column='sentiment_score'):
        """
        Performs sentiment analysis on the specified text column using VADER.

        Args:
            text_column (str): The name of the column containing the text to analyze.
            output_column (str): The name of the column to store the sentiment scores.

        Returns:
            pd.DataFrame: The DataFrame with sentiment scores added.
        """
        if self.df is None or text_column not in self.df.columns:
            return self.df
        self.df[output_column] = self.df[text_column].apply(lambda text: self.sentiment_analyzer.polarity_scores(text)["compound"])
        return self.df

    # def save_to_json(self, output_file="processed_reddit_data.json"):
    #     """
    #     Saves the processed DataFrame to a JSON file.

    #     Args:
    #         output_file (str): The name of the output JSON file.
    #     """
    #     if self.df is not None:
    #         self.df.to_json(output_file, orient="records", date_format="iso")
    #         print(f"Processed data saved to '{output_file}'")

    def get_as_dict(self):
        """Return the processed data as a dictionary"""
        if self.df is None:
            return {}
        return self.df.to_dict(orient="records")

    def run_analysis(self):
        """
        Runs the complete analysis pipeline.

        Args:
            output_file (str): The name of the output JSON file.
        """
        print("loading data")
        self.df = self._load_and_preprocess_data()
        print(f"data loaded, length: {len(self.df)}")
        if self.df is not None:
            print("starting analysis")
            self.analyze_and_translate_languages()
            print("analysing sentiment")
            self.analyze_sentiment()
            # self.save_to_json(output_file)
            # print(f"\nAnalysis complete. Output saved to {output_file}")
            return self.df


# processor_reddit = RedditDataProcessor('reddit_posts.json')
# processor_reddit.run_analysis(output_file="sentiment_analysis_reddit_timed.json")

