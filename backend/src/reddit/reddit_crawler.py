import asyncpraw
import asyncio
import json
from dotenv import load_dotenv
import os
from pathlib import Path
import logging
import re
from datetime import datetime, timedelta
import sys
import time

# Path configs
BASE_DIR = Path(__file__).resolve().parent.parent.parent.parent
env_path = BASE_DIR / '.env'
city_geo_path = BASE_DIR / 'backend' / 'configs' / 'city_geo.json'
sys.path.append(str(BASE_DIR))

from backend.src.reddit.utils import convert_unix_time

class RedditCrawler:
    def __init__(self):
        """Initialize the Reddit crawler with necessary configs and data."""
        self.logger = logging.getLogger(__name__)
        
        load_dotenv(dotenv_path=env_path)
        self.reddit_client_id = os.getenv("REDDIT_CLIENT_ID")
        self.reddit_client_secret = os.getenv("REDDIT_CLIENT_SECRET")
        self.reddit_password = os.getenv("REDDIT_PASSWORD")
        self.reddit_user_agent = os.getenv("REDDIT_USER_AGENT")
        self.reddit_username = os.getenv("REDDIT_USERNAME")
        
        if not all([self.reddit_client_id, self.reddit_client_secret, self.reddit_user_agent]):
            raise ValueError("Reddit API credentials are not properly configured in .env file")
        
        # List of subreddits to scrape
        self.subreddits = ['travel'] 
        # 'wanderlust', 'solotravel',
        # 'travelhacks', 'backpacking', 'campingandhiking', 
        # 'vandwellers', 'roadtrip', 'vagabond', 'adventures'
        
        # Load city and country data
        with open(city_geo_path, 'r') as f:
            self.city_geo = json.load(f)
        
        # Extract unique countries from city_geo.json
        self.countries = set(city_data['country'] for city_data in self.city_geo.values())
        
        # Create a mapping of cities to their countries
        self.city_to_country = {city.replace('_', ' '): city_data['country'] 
                               for city, city_data in self.city_geo.items()}
        
        # Store results
        self.submissions_data = []
        self.comments_data = []

    def find_matches(self, text):
        """
        Find mentions of countries and cities in text.
        
        Args:
            text (str): Text to search in
        
        Returns:
            tuple: (matching_countries, matching_cities)
        """
        # Check for country mentions
        matching_countries = [
            country for country in self.countries 
            if re.search(r'\b' + re.escape(country.replace('_', ' ')) + r'\b', 
                         text, 
                         re.IGNORECASE)
        ]

        # Check for city mentions
        matching_cities = [
            city for city in self.city_to_country.keys()
            if re.search(r'\b' + re.escape(city) + r'\b', 
                         text, 
                         re.IGNORECASE)
        ]

        # Add countries for matched cities
        for city in matching_cities:
            country = self.city_to_country[city]
            if country not in matching_countries:
                matching_countries.append(country)

        # Remove duplicate matching countries
        matching_countries = list(dict.fromkeys(matching_countries))
        
        return matching_countries, matching_cities

    async def init_reddit_client(self):
        """Initialize and return the Reddit client."""
        return asyncpraw.Reddit(
            client_id=self.reddit_client_id,
            client_secret=self.reddit_client_secret,
            password=self.reddit_password,
            user_agent=self.reddit_user_agent,
            username=self.reddit_username
        )

    async def scrape_subreddits(self, days_ago=7):
        """
        Scrape submissions and comments from specified subreddits from the past X days.
        
        Args:
            days_ago (int): Number of days in the past to scrape (default: 7)
        
        Returns:
            tuple: (submissions_data, comments_data)
        """
        self.submissions_data = []
        self.comments_data = []
        
        # Calculate the timestamp for stated number of days ago
        past_date = datetime.now() - timedelta(days=days_ago)
        past_timestamp = past_date.timestamp()
        
        self.logger.info(f"Scraping submissions from the past {days_ago} days (since {past_date.strftime('%Y-%m-%d')})")

        reddit = await self.init_reddit_client()

        try:
            for subreddit_name in self.subreddits:
                self.logger.info(f"Scraping subreddit: r/{subreddit_name}")
                try:
                    subreddit = await reddit.subreddit(subreddit_name)
                    await subreddit.load()

                    submission_count = 0
                    relevant_submission_count = 0
                    
                    # Scrape all submissions from the past X days
                    async for submission in subreddit.new(limit=None):
                        # Add a delay to avoid hitting rate limits
                        await asyncio.sleep(2)
                        
                        # Skip submissions older than X days
                        if submission.created_utc < past_timestamp:
                            self.logger.info(f"Reached submissions older than {days_ago} days. Stopping for r/{subreddit_name}.")
                            break
                        
                        submission_count += 1
                        
                        # Search in title and selftext
                        search_text = f"{submission.title} {submission.selftext}"
                        
                        # Find country and city mentions in the submission
                        matching_countries, matching_cities = self.find_matches(search_text)
                        
                        # Process submission if any country or city is mentioned
                        if matching_countries:
                            relevant_submission_count += 1
                            
                            submission_info = {
                                "submission_id": submission.id,
                                "author": submission.author.name if submission.author else None,
                                "created_utc": submission.created_utc,
                                "month_year": convert_unix_time(submission.created_utc),
                                "name": submission.name,
                                "num_comments": submission.num_comments,
                                "score": submission.score,
                                "selftext": submission.selftext,
                                "subreddit_name": subreddit_name,
                                "title": submission.title,
                                "upvote_ratio": submission.upvote_ratio,
                                "mentioned_countries": matching_countries,
                                "mentioned_cities": matching_cities
                            }
                            self.submissions_data.append(submission_info)

                            # Scrape comments for this submission
                            try:
                                # Documentation requires to load submission before accessing comments 
                                await submission.load()
                                # Flatten comment forest
                                await submission.comments.replace_more(limit=0)
                                
                                # Collect all comments belonging to a submission
                                all_comments = await submission.comments.list()
                                
                                for comment in all_comments:
                                    # avoid hitting rate limit
                                    await asyncio.sleep(0.5)
                                    
                                    # Remove automoderator comments
                                    if comment.author and comment.author.name != "AutoModerator":
                                        # For each comment, search for country/city mentions in its text
                                        comment_matching_countries, comment_matching_cities = self.find_matches(
                                            comment.body
                                        )
                                        
                                        # Combine submission mentions with comment mentions
                                        # Create new lists to avoid modifying the submission's lists
                                        combined_countries = list(matching_countries)
                                        combined_cities = list(matching_cities)
                                        
                                        # Add comment-specific mentions
                                        for country in comment_matching_countries:
                                            if country not in combined_countries:
                                                combined_countries.append(country)
                                        
                                        for city in comment_matching_cities:
                                            if city not in combined_cities:
                                                combined_cities.append(city)
                                        
                                        comment_info = {
                                            "comment_id": comment.id,
                                            "submission_id": submission.id,
                                            "author": comment.author.name if comment.author else None,
                                            "body": comment.body,
                                            "created_utc": comment.created_utc,
                                            "month_year": convert_unix_time(comment.created_utc),
                                            "score": comment.score,
                                            "subreddit_name": subreddit_name,
                                            "mentioned_countries": combined_countries,
                                            "mentioned_cities": combined_cities
                                        }
                                        self.comments_data.append(comment_info)

                            except Exception as e:
                                self.logger.warning(f"Error scraping comments for submission '{submission.title}': {e}")
                                continue
                    
                    self.logger.info(f"Processed {submission_count} submissions from r/{subreddit_name}. Found {relevant_submission_count} relevant submissions.")

                except Exception as e:
                    self.logger.error(f"Error scraping subreddit {subreddit_name}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"An error occurred during scraping: {e}")
        
        finally:
            await reddit.close()
        
        self.logger.info(f"Total submissions collected: {len(self.submissions_data)}")
        self.logger.info(f"Total comments collected: {len(self.comments_data)}")
        
        return self.submissions_data, self.comments_data

    def save_results(self, output_dir=None):
        """
        Save submissions and comments to separate JSON files
        
        Args:
            output_dir (Path, optional): Directory to save files in. Defaults to backend/data.
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        if not output_dir:
            output_dir = Path(__file__).resolve().parent.parent.parent / 'data'
            
        output_dir.mkdir(parents=True, exist_ok=True)

        submissions_path = output_dir / f'reddit_submissions_{timestamp}.json'
        with open(submissions_path, 'w') as f:
            json.dump(self.submissions_data, f, indent=4)
        self.logger.info(f"Submissions saved to {submissions_path}")

        comments_path = output_dir / f'reddit_comments_{timestamp}.json'
        with open(comments_path, 'w') as f:
            json.dump(self.comments_data, f, indent=4)
        self.logger.info(f"Comments saved to {comments_path}")

    async def test_connection(self):
        """Test Reddit connection"""
        reddit = await self.init_reddit_client()
        
        try:
            user = await reddit.user.me()
            self.logger.info(f"Connected to Reddit as: {user}")
            return True
        except Exception as e:
            self.logger.error(f"Error connecting to Reddit: {e}")
            return False
        finally:
            await reddit.close()

async def main():
    """Run the Reddit crawler as a standalone script"""
    # Set up logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    logger.info("Starting Reddit scraping")
    
    crawler = RedditCrawler()
    await crawler.test_connection()
    await crawler.scrape_subreddits(days_ago=7)
    # crawler.save_results()
    
    logger.info("Scraping complete")

if __name__ == "__main__":
    asyncio.run(main())