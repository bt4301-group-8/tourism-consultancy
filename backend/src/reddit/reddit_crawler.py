import asyncpraw
import asyncio
import json
from dotenv import load_dotenv
import os
from pathlib import Path
import logging
from collections import defaultdict
import re
from datetime import datetime

# Logging configs
handler = logging.StreamHandler()
handler.setLevel(logging.DEBUG)
for logger_name in ("praw", "prawcore"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    logger.addHandler(handler)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[handler]
)

# Path configs
env_path = Path(__file__).resolve().parent.parent.parent.parent / '.env'
city_geo_path = Path(__file__).resolve().parent.parent.parent / 'configs' / 'city_geo.json'

# Load environment variables
load_dotenv(dotenv_path=env_path)
reddit_client_id = os.getenv("REDDIT_CLIENT_ID")
reddit_client_secret = os.getenv("REDDIT_CLIENT_SECRET")
reddit_password = os.getenv("REDDIT_PASSWORD")
reddit_user_agent = os.getenv("REDDIT_USER_AGENT")
reddit_username = os.getenv("REDDIT_USERNAME")

# List of subreddits to scrape, can add or remove if needed
subreddits = ['travel', 'wanderlust', 'solotravel',
              'travelhacks', 'backpacking', 'campingandhiking', 
              'vandwellers', 'roadtrip', 'vagabond', 'adventures']


with open(city_geo_path, 'r') as f:
    city_geo = json.load(f)

# Extract unique countries from city_geo.json
countries = set(city_data['country'] for city_data in city_geo.values())

# Create a mapping of cities to their countries
# This is important for later
city_to_country = {city.replace('_', ' '): city_data['country'] for city, city_data in city_geo.items()}

async def scrape_subreddits():
    """
    Scrape submissions and comments from specified subreddits
    Filters submissions by country and city mentions
    
    Returns:
    - submissions_data: List of dictionaries containing submission details
    - comments_data: List of dictionaries containing comment details
    """
    reddit = asyncpraw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        password=reddit_password,
        user_agent=reddit_user_agent,
        username=reddit_username
    )

    submissions_data = []
    comments_data = []

    try:
        for subreddit_name in subreddits:
            logging.info(f"Scraping subreddit: r/{subreddit_name}")
            try:
                subreddit = await reddit.subreddit(subreddit_name)
                await subreddit.load()

                # Track submissions per country to set a limit for number of submissions scraped 
                # from a particular country for more even distribution
                country_submission_counts = {country: 0 for country in countries}

                # Scrape all submissions that mention any country/city in city_geo.json
                async for submission in subreddit.new(limit=None):
                    # Search in title and selftext
                    search_text = f"{submission.title} {submission.selftext}"

                    # Check for country mentions
                    matching_countries = [
                        country for country in countries 
                        if re.search(r'\b' + re.escape(country.replace('_', ' ')) + r'\b', 
                                     search_text, 
                                     re.IGNORECASE)
                    ]

                    # There might be the case where a submission only mentions the city 
                    # but not the country.
                    # We should check for city mentions and add corresponding countries
                    matching_cities = [
                        city for city in city_to_country.keys()
                        if re.search(r'\b' + re.escape(city) + r'\b', 
                                     search_text, 
                                     re.IGNORECASE)
                    ]

                    # Add countries for matched cities
                    matching_countries.extend([
                        city_to_country[city] for city in matching_cities 
                        if city_to_country[city] not in matching_countries
                    ])

                    # Remove duplicate matching countries
                    matching_countries = list(dict.fromkeys(matching_countries))

                    # Process if country is mentioned in submission text
                    for country in matching_countries:
                        if country_submission_counts[country] < 100:
                            submission_info = {
                                "id": submission.id,
                                "author": submission.author.name if submission.author else None,
                                "created_utc": submission.created_utc,
                                "name": submission.name,
                                "num_comments": submission.num_comments,
                                "score": submission.score,
                                "selftext": submission.selftext,
                                "subreddit": subreddit_name,
                                "title": submission.title,
                                "upvote_ratio": submission.upvote_ratio,
                                "mentioned_countries": matching_countries,
                                "mentioned_cities": matching_cities
                            }
                            submissions_data.append(submission_info)

                            country_submission_counts[country] += 1

                            # Scrape comments for this submission
                            try:
                                # Documentation requires to load submission before accessing comments 
                                await submission.load()
                                # Flatten comment forest
                                await submission.comments.replace_more(limit=0)
                                
                                # Collect all comments belonging to a submission
                                all_comments = await submission.comments.list()
                                for comment in all_comments:
                                    # Remove automoderator comments
                                    if comment.author and comment.author.name != "AutoModerator":
                                        comment_info = {
                                            "id": comment.id,
                                            "link_id": submission.id,
                                            "author": comment.author.name if comment.author else None,
                                            "body": comment.body,
                                            "created_utc": comment.created_utc,
                                            "score": comment.score,
                                            "subreddit": subreddit_name,
                                            "subreddit_ID": subreddit.display_name,
                                            "mentioned_countries": matching_countries,
                                            "mentioned_cities": matching_cities
                                        }
                                        comments_data.append(comment_info)

                            except Exception as e:
                                logging.warning(f"Error scraping comments for submission '{submission.title}': {e}")
                                continue

                    # Set a limit of maximum of 100 submissions per country per subreddit
                    if all(count >= 100 for count in country_submission_counts.values()):
                        break

            except Exception as e:
                logging.error(f"Error scraping subreddit {subreddit_name}: {e}")
                continue
    
    except Exception as e:
        logging.error(f"An error occurred during scraping: {e}")
    
    finally:
        await reddit.close()
    
    return submissions_data, comments_data

def save_results(submissions_data, comments_data):
    """
    Save submissions and comments to separate JSON files
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    output_dir = Path(__file__).resolve().parent.parent.parent / 'data'
    output_dir.mkdir(parents=True, exist_ok=True)

    submissions_path = output_dir / f'reddit_submissions_{timestamp}.json'
    with open(submissions_path, 'w') as f:
        json.dump(submissions_data, f, indent=4)
    logging.info(f"Submissions saved to {submissions_path}")

    comments_path = output_dir / f'reddit_comments_{timestamp}.json'
    with open(comments_path, 'w') as f:
        json.dump(comments_data, f, indent=4)
    logging.info(f"Comments saved to {comments_path}")

async def test_connection():
    """
    Test Reddit connection
    """
    reddit = asyncpraw.Reddit(
        client_id=reddit_client_id,
        client_secret=reddit_client_secret,
        password=reddit_password,
        user_agent=reddit_user_agent,
        username=reddit_username
    )
    
    try:
        user = await reddit.user.me()
        print(f"Connected as: {user}")
    except Exception as e:
        logging.error(f"Error connecting to Reddit: {e}")
    finally:
        await reddit.close()

async def main():
    logging.info("Starting scraping")
    await test_connection()
    submissions_data, comments_data = await scrape_subreddits()
    
    logging.info("Scraping complete")
    save_results(submissions_data, comments_data)

if __name__ == "__main__":
    asyncio.run(main())