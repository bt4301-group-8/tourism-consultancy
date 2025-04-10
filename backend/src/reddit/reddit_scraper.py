import praw
import pandas as pd
from datetime import datetime
import time
from typing import List, Dict
import os
from dotenv import load_dotenv
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from textblob import TextBlob

# Load environment variables
load_dotenv()

class RedditScraper:
    def __init__(self):
        # Initialize Reddit API client
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        self.vader = SentimentIntensityAnalyzer()
        
        # General tourism subreddits
        self.general_subreddits = [
            'travel', 'solotravel', 'backpacking', 'TravelHacks', 'wanderlust', 'asean'
        ]
        
        self.date_start = int(datetime.strptime('2022-01-01', '%Y-%m-%d').timestamp())
        self.date_end = int(datetime.strptime('2025-01-01', '%Y-%m-%d').timestamp())

        # ASEAN countries with keywords and specific subreddits
        self.countries = {
            'Brunei': {'keywords': ['brunei', 'bn'], 'subreddits': ['Brunei']},
            'Indonesia': {'keywords': ['indonesia', 'id'], 'subreddits': ['indonesia', 'Bali', 'jakarta']},
            'Cambodia': {'keywords': ['cambodia', 'kh'], 'subreddits': ['cambodia', 'phnompenh']},
            'Laos': {'keywords': ['laos', 'la'], 'subreddits': ['laos']},
            'Myanmar': {'keywords': ['myanmar', 'mm'], 'subreddits': ['myanmar', 'yangon']},
            'Malaysia': {'keywords': ['malaysia', 'my'], 'subreddits': ['malaysia', 'KualaLumpur', 'penang']},
            'Philippines': {'keywords': ['philippines', 'ph'], 'subreddits': ['Philippines', 'manila', 'cebu']},
            'Singapore': {'keywords': ['singapore', 'sg'], 'subreddits': ['singapore']},
            'Thailand': {'keywords': ['thailand', 'th'], 'subreddits': ['Thailand', 'Bangkok', 'ChiangMai']},
            'Vietnam': {'keywords': ['vietnam', 'vn'], 'subreddits': ['vietnam', 'Hanoi', 'saigon']}
        }


    def get_sentiment_scores(self, text: str) -> Dict:
        if text is None:
            text = ""
        vader_scores = self.vader.polarity_scores(text)
        blob = TextBlob(text)
        textblob_polarity = blob.sentiment.polarity
        return {
            'vader_compound': vader_scores['compound'],
            'vader_pos': vader_scores['pos'],
            'vader_neu': vader_scores['neu'],
            'vader_neg': vader_scores['neg'],
            'textblob_polarity': textblob_polarity
        }

    def scrape_reddit(self, search_keywords: List[str], subreddits: List[str], countries_name: str):
        all_posts = []
        unique_subreddits = list(set(subreddits))  # Remove duplicates

        for keyword in search_keywords:
            for subreddit in unique_subreddits:
                try:
                    print(f"\n🔎 Searching for '{keyword}' in r/{subreddit}...")
                    subreddit_instance = self.reddit.subreddit(subreddit)
                    search_results = subreddit_instance.search(
                        keyword,
                        syntax='lucene',
                        time_filter='all'
                    )

                    for post in search_results:
                        created_time = post.created_utc
                        if self.date_start <= created_time <= self.date_end:
                            full_text = f"{post.title} {post.selftext}"
                            sentiment_scores = self.get_sentiment_scores(full_text)
                            post_data = {
                                'post_id': post.id,
                                'created_at': datetime.fromtimestamp(post.created_utc).strftime('%m/%d/%y'),
                                'title': post.title,
                                'text': post.selftext,
                                'author': str(post.author),
                                'score': post.score,
                                'upvote_ratio': post.upvote_ratio,
                                'num_comments': post.num_comments,
                                'subreddit': str(post.subreddit),
                                'source': 'Reddit',
                                'content_type': 'submission',
                                'keyword': keyword,
                                **sentiment_scores,
                                'parent_id': '',
                                'country': countries_name
                            }
                            all_posts.append(post_data)

                            post.comments.replace_more(limit=0)
                            for comment in post.comments.list():
                                if comment.body:
                                    comment_time = comment.created_utc
                                    if self.date_start <= comment_time <= self.date_end:
                                        comment_scores = self.get_sentiment_scores(comment.body)
                                        comment_data = {
                                            'post_id': comment.id,
                                            'created_at': datetime.fromtimestamp(comment_time).strftime('%m/%d/%y'),
                                            'title': '',
                                            'text': comment.body,
                                            'author': str(comment.author),
                                            'score': comment.score,
                                            'upvote_ratio': comment.upvote_ratio,
                                            'num_comments': '',
                                            'subreddit': str(post.subreddit),
                                            'source': 'Reddit',
                                            'content_type': 'comment',
                                            'keyword': keyword,
                                            **comment_scores,
                                            'parent_id': post.id,
                                            'country': countries_name
                                        }
                                        all_posts.append(comment_data)

                    time.sleep(2)

                except Exception as e:
                    print(f"⚠️ Error scraping r/{subreddit} for '{keyword}': {e}")
                    continue

        df = pd.DataFrame(all_posts)
        output_file = f'reddit_{countries_name}_2022_2025.csv'
        df.to_csv(output_file, index=False)
        print(f"\n✅ Data saved to {output_file}")
        print(f"📊 Total entries for {countries_name}: {len(df)}")

        if len(df) > 0:
            print("\n📈 Average Sentiment Scores:")
            print(f"VADER Compound: {df['vader_compound'].mean():.3f}")
            print(f"TextBlob Polarity: {df['textblob_polarity'].mean():.3f}")
        else:
            print(f"❌ No posts found for {countries_name} within the specified date range.")

    def run_all(self):
        for countries_name, countries_info in self.countries.items():
            print(f"\n======================")
            print(f"🔍 Starting scrape for: {countries_name.upper()}")
            print(f"======================")
            full_subreddits = self.general_subreddits + countries_info['subreddits']
            self.scrape_reddit(
                search_keywords=countries_info['keywords'],
                subreddits=full_subreddits,
                countries_name=countries_name
            )

def main():
    scraper = RedditScraper()
    scraper.run_all()

if __name__ == "__main__":
    main()
