from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import asyncio
from pathlib import Path
import sys
import os
import logging
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
import json
import re

# Define base directory (project root)
BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

sys.path.append(str(BASE_DIR))

from backend.src.reddit.reddit_crawler import RedditCrawler
from backend.src.reddit.utils import convert_unix_time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

mongo_uri = os.getenv("MONGO_URI")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'reddit_demo_etl_pipeline',
    default_args=default_args,
    description='Demo Reddit ETL pipeline for Singapore data with 20 post limit',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['reddit', 'demo', 'etl', 'singapore'],
)

class SingaporeRedditCrawler(RedditCrawler):
    """Modified Reddit crawler that scrapes Singapore and limits post to 20"""
    
    def __init__(self):
        super().__init__()
        # Change to singapore only
        self.countries = {'singapore'}
        self.max_posts = 20
        
    async def scrape_subreddits(self, days_ago=7):
        """
        Scrape submissions and comments from specified subreddits from the past X days,
        filtering for Singapore only and limiting to max_posts submissions.
        Collects all comments for each submission.
        
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
        
        self.logger.info(f"Scraping up to {self.max_posts} Singapore submissions from the past {days_ago} days (since {past_date.strftime('%Y-%m-%d')})")

        reddit = await self.init_reddit_client()

        try:
            post_count = 0
            for subreddit_name in self.subreddits:
                self.logger.info(f"Scraping subreddit: r/{subreddit_name} for Singapore mentions")
                try:
                    subreddit = await reddit.subreddit(subreddit_name)
                    await subreddit.load()

                    submission_count = 0
                    relevant_submission_count = 0
                    
                    # Scrape all submissions from the past X days
                    async for submission in subreddit.new(limit=100):  # Higher limit to find Singapore posts
                        # Skip submissions older than X days
                        if submission.created_utc < past_timestamp:
                            self.logger.info(f"Reached submissions older than {days_ago} days. Stopping for r/{subreddit_name}.")
                            break
                        
                        submission_count += 1
                        
                        # Search in title and selftext
                        search_text = f"{submission.title} {submission.selftext}"
                        
                        # Look specifically for Singapore mentions
                        if re.search(r'\bsingapore\b', search_text, re.IGNORECASE):
                            relevant_submission_count += 1
                            post_count += 1
                            
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
                                "mentioned_countries": ["singapore"],
                                "mentioned_cities": ["singapore"]
                            }
                            self.submissions_data.append(submission_info)

                            # Scrape all comments for this submission, regardless of Singapore mentions
                            self.logger.info(f"Collecting all comments for submission '{submission.title[:30]}...'")
                            comment_count = 0
                            
                            try:
                                # Load submission before accessing comments 
                                await submission.load()
                                # Flatten comment forest
                                await submission.comments.replace_more(limit=0)
                                
                                # Collect all comments belonging to a submission
                                all_comments = await submission.comments.list()
                                
                                for comment in all_comments:
                                    # Avoid hitting rate limit
                                    await asyncio.sleep(2)
                                    
                                    # Skip automoderator comments
                                    if comment.author and comment.author.name != "AutoModerator":
                                        comment_count += 1
                                        
                                        # Check if comment specifically mentions Singapore
                                        singapore_in_comment = re.search(r'\bsingapore\b', comment.body, re.IGNORECASE)
                                        
                                        comment_info = {
                                            "comment_id": comment.id,
                                            "submission_id": submission.id,
                                            "author": comment.author.name if comment.author else None,
                                            "body": comment.body,
                                            "created_utc": comment.created_utc,
                                            "month_year": convert_unix_time(comment.created_utc),
                                            "score": comment.score,
                                            "subreddit_name": subreddit_name,
                                            "mentioned_countries": ["singapore"],
                                            "mentioned_cities": ["singapore"] if singapore_in_comment else []
                                        }
                                        self.comments_data.append(comment_info)
                                
                                self.logger.info(f"Collected {comment_count} comments for this submission")

                            except Exception as e:
                                self.logger.warning(f"Error scraping comments for submission '{submission.title}': {e}")
                                continue
                            
                            # Break after reaching the max posts limit
                            if post_count >= self.max_posts:
                                self.logger.info(f"Reached max post limit of {self.max_posts} submissions. Stopping.")
                                break
                    
                    self.logger.info(f"Processed {submission_count} submissions from r/{subreddit_name}. Found {relevant_submission_count} Singapore related submissions.")
                    
                    # Break after reaching the max posts limit
                    if post_count >= self.max_posts:
                        break

                except Exception as e:
                    self.logger.error(f"Error scraping subreddit {subreddit_name}: {e}")
                    continue
        
        except Exception as e:
            self.logger.error(f"An error occurred during scraping: {e}")
        
        finally:
            await reddit.close()
        
        self.logger.info(f"Singapore submissions collected: {len(self.submissions_data)} (max limit: {self.max_posts})")
        self.logger.info(f"Total comments collected for these submissions: {len(self.comments_data)}")
        
        return self.submissions_data, self.comments_data

def extract_reddit_data(**context):
    """Extract data from Reddit API for Singapore mentions only, limited to 20 submissions"""
    logging.info("Starting Reddit data extraction for Singapore only")
    
    crawler = SingaporeRedditCrawler()
    
    # Create new event loop for running async code
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        connection_successful = loop.run_until_complete(crawler.test_connection())
        if not connection_successful:
            raise ValueError("Failed to connect to Reddit API. Check your credentials.")
            
        submissions_data, comments_data = loop.run_until_complete(crawler.scrape_subreddits(days_ago=7))
    finally:
        loop.close()
    
    context['ti'].xcom_push(key='submissions_data', value=json.dumps(submissions_data))
    context['ti'].xcom_push(key='comments_data', value=json.dumps(comments_data))
    
    logging.info(f"Extracted {len(submissions_data)} Singapore submissions (max: 20) and {len(comments_data)} associated comments")
    return len(submissions_data), len(comments_data)

def transform_data(**context):
    """Validate and transform Reddit data, setting all month_year to 2025-03"""
    logging.info("Starting data validation and transformation")
    
    # Get data from extract stage
    submissions_data = json.loads(context['ti'].xcom_pull(task_ids='extract_reddit_data', key='submissions_data'))
    comments_data = json.loads(context['ti'].xcom_pull(task_ids='extract_reddit_data', key='comments_data'))
    
    today = datetime.now()
    first_day_of_current_month = datetime(today.year, today.month, 1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    previous_month = last_day_of_previous_month.strftime("%Y-%m")
    
    # Validate submissions
    valid_submissions = []
    submission_ids = set()
    
    for submission in submissions_data:
        if "submission_id" not in submission:
            logging.warning(f"Found submission without submission_id field - skipping")
            continue
            
        # Check for duplicate submission IDs
        if submission["submission_id"] in submission_ids:
            logging.info(f"Skipping duplicate submission ID: {submission['submission_id']}")
            continue
            
        submission_ids.add(submission["submission_id"])
        
        # Ensure Singapore is in the mentioned countries
        if "singapore" not in [c.lower() for c in submission.get("mentioned_countries", [])]:
            submission["mentioned_countries"] = ["singapore"]
        
        # Set month_year to previous month
        submission["month_year"] = previous_month
            
        valid_submissions.append(submission)
    
    # Validate comments
    valid_comments = []
    comment_ids = set()
    
    for comment in comments_data:
        if "comment_id" not in comment:
            logging.warning(f"Found comment without comment_id field - skipping")
            continue
            
        # Check for duplicate comment IDs
        if comment["comment_id"] in comment_ids:
            logging.info(f"Skipping duplicate comment ID: {comment['comment_id']}")
            continue
            
        comment_ids.add(comment["comment_id"])
        
        # Ensure Singapore is in the mentioned countries
        if "singapore" not in [c.lower() for c in comment.get("mentioned_countries", [])]:
            comment["mentioned_countries"] = ["singapore"]
        
        # Set month_year to previous month
        comment["month_year"] = previous_month
            
        valid_comments.append(comment)
    
    # pass to load
    context['ti'].xcom_push(key='transformed_submissions', value=json.dumps(valid_submissions))
    context['ti'].xcom_push(key='transformed_comments', value=json.dumps(valid_comments))
    
    logging.info(f"Valid Singapore submissions: {len(valid_submissions)}/{len(submissions_data)}, Valid comments: {len(valid_comments)}/{len(comments_data)}")
    
    return len(valid_submissions), len(valid_comments)

def load_to_mongodb(**context):
    """Load Singapore Reddit data to MongoDB"""
    start_time = datetime.now()
    logging.info(f"Starting Singapore data loading to MongoDB at {start_time}")
    
    # Get data from transform task
    submissions_data = json.loads(context['ti'].xcom_pull(task_ids='transform_data', key='transformed_submissions'))
    comments_data = json.loads(context['ti'].xcom_pull(task_ids='transform_data', key='transformed_comments'))
    
    logging.info(f"Data size: {len(submissions_data)} Singapore submissions (max: 20) with {len(comments_data)} associated comments")
    
    if not submissions_data and not comments_data:
        logging.warning("No Singapore data to load. Skipping MongoDB operations.")
        return 0, 0
    
    try:
        logging.info(f"Connecting to MongoDB")
        client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        logging.info("MongoDB connection successful")
        
        submissions_collection = client.demo.posts.reddit_submissions
        comments_collection = client.demo.posts.reddit_comments
        
        submission_results = []
        comment_results = []
        
        if submissions_data:
            logging.info(f"Processing {len(submissions_data)} Singapore submissions")
            for i, submission in enumerate(submissions_data):
                try:
                    # Upsert to update existing or insert new records
                    result = submissions_collection.update_one(
                        {"submission_id": submission["submission_id"]},
                        {"$set": submission},
                        upsert=True
                    )
                    submission_results.append(result.upserted_id or result.modified_count)
                    
                except Exception as e:
                    logging.error(f"Error inserting submission {submission.get('submission_id', 'unknown')}: {e}")
        
        if comments_data:
            logging.info(f"Processing {len(comments_data)} comments associated with the Singapore submissions")
            for i, comment in enumerate(comments_data):
                try:
                    # Upsert to update existing or insert new records
                    result = comments_collection.update_one(
                        {"comment_id": comment["comment_id"]},
                        {"$set": comment},
                        upsert=True
                    )
                    comment_results.append(result.upserted_id or result.modified_count)
                    
                except Exception as e:
                    logging.error(f"Error inserting comment {comment.get('comment_id', 'unknown')}: {e}")
        
        total_time = datetime.now() - start_time
        submission_success_rate = (len(submission_results) / len(submissions_data) * 100) if submissions_data else 0
        comment_success_rate = (len(comment_results) / len(comments_data) * 100) if comments_data else 0
        
        logging.info(f"MongoDB loading completed in {total_time}")
        logging.info(f"Singapore submissions: {len(submission_results)}/{len(submissions_data)} loaded successfully ({submission_success_rate:.1f}%)")
        logging.info(f"Associated comments: {len(comment_results)}/{len(comments_data)} loaded successfully ({comment_success_rate:.1f}%)")
        
    except Exception as e:
        logging.error(f"Error during MongoDB operations: {e}")
        raise
    finally:
        if 'client' in locals():
            client.close()
            logging.info("MongoDB connection closed")
    
    return len(submission_results), len(comment_results)

extract_task = PythonOperator(
    task_id='extract_reddit_data',
    python_callable=extract_reddit_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_mongodb',
    python_callable=load_to_mongodb,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task