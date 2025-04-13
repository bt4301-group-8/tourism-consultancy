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
        self.reddit = praw.Reddit(
            client_id=os.getenv('REDDIT_CLIENT_ID'),
            client_secret=os.getenv('REDDIT_CLIENT_SECRET'),
            user_agent=os.getenv('REDDIT_USER_AGENT')
        )
        
        self.vader = SentimentIntensityAnalyzer()

        self.general_subreddits = [
            'travel', 'solotravel', 'backpacking', 'TravelHacks', 'wanderlust', 'asean'
        ]

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

    def scrape_reddit(self, search_keywords: List[str], subreddits: List[str], countries_name: str,
                      date_start: int, date_end: int) -> List[Dict]:
        all_posts = []
        unique_subreddits = list(set(subreddits))

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
                        if date_start <= created_time <= date_end:
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
                                comment_time = comment.created_utc
                                if date_start <= comment_time <= date_end:
                                    comment_scores = self.get_sentiment_scores(comment.body)
                                    comment_data = {
                                        'post_id': comment.id,
                                        'created_at': datetime.fromtimestamp(comment_time).strftime('%m/%d/%y'),
                                        'title': '',
                                        'text': comment.body,
                                        'author': str(comment.author),
                                        'score': comment.score,
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

        return all_posts

    def scrape_all_countries_for_period(self, start_date: str, end_date: str) -> pd.DataFrame:
        """Scrape all countries for the given date period and return one DataFrame"""
        date_start = int(datetime.strptime(start_date, '%Y-%m-%d').timestamp())
        date_end = int(datetime.strptime(end_date, '%Y-%m-%d').timestamp())
        
        all_data = []
        for country, info in self.countries.items():
            print(f"\n======================")
            print(f"🔍 Starting scrape for: {country.upper()}")
            print(f"======================")
            full_subreddits = self.general_subreddits + info['subreddits']
            posts = self.scrape_reddit(info['keywords'], full_subreddits, country, date_start, date_end)
            all_data.extend(posts)
        
        df = pd.DataFrame(all_data)
        # save data into dsv, commment out for helper function
        output_file = f'reddit_ASEAN_{start_date}_to_{end_date}.csv'
        df.to_csv(output_file, index=False)
        print(f"\n✅ All data saved to {output_file}")
        print(f"📊 Total entries: {len(df)}")
        return df

if __name__ == "__main__":
    scraper = RedditScraper()
    df_combined = scraper.scrape_all_countries_for_period("2022-01-01", "2025-01-31")
