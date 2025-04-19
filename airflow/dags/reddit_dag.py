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

# Define base directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)
sys.path.append(str(BASE_DIR))

from backend.src.reddit.reddit_crawler import RedditCrawler


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
    'reddit_etl_pipeline',
    default_args=default_args,
    description='Reddit data ETL pipeline that scrapes travel subreddits weekly and uploads to MongoDB collections',
    schedule_interval='0 4 1 * *',  # Stagger
    start_date=datetime(2025, 4, 1),
    catchup=False,
)

def extract_reddit_data(**context):
    """Extract data from Reddit API for submissions and comments over the last 7 days"""
    logging.info("Starting Reddit data extraction")
    
    crawler = RedditCrawler()
    
    # Airflow python operators run with synchronous context, does not natively support async
    # Create new event loop and set it as current, then run the async function within the loop, then close the loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        # Test connection first
        connection_successful = loop.run_until_complete(crawler.test_connection())
        if not connection_successful:
            raise ValueError("Failed to connect to Reddit API. Check your credentials.")
            
        # Run the scraper
        submissions_data, comments_data = loop.run_until_complete(crawler.scrape_subreddits(days_ago=7))
    finally:
        loop.close()
    
    # Store data to pass on to transform stage
    context['ti'].xcom_push(key='submissions_data', value=json.dumps(submissions_data))
    context['ti'].xcom_push(key='comments_data', value=json.dumps(comments_data))
    
    logging.info(f"Extracted {len(submissions_data)} submissions and {len(comments_data)} comments")
    return len(submissions_data), len(comments_data)

def transform_data(**context):
    """Validate and transform Reddit data"""
    logging.info("Starting data validation and transformation")
    
    # Get data from extract stage
    submissions_data = json.loads(context['ti'].xcom_pull(task_ids='extract_reddit_data', key='submissions_data'))
    comments_data = json.loads(context['ti'].xcom_pull(task_ids='extract_reddit_data', key='comments_data'))
    
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
            
        # Ensure all required fields are present
        required_fields = ["author", "created_utc", "month_year", "title", "mentioned_countries"]
        missing_fields = [field for field in required_fields if field not in submission or submission[field] is None]
        
        if missing_fields:
            logging.warning(f"Submission {submission.get('submission_id')} missing fields: {missing_fields} - keeping anyway")
        
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
            
        # Ensure all required fields are present
        required_fields = ["submission_id", "author", "created_utc", "body"]
        missing_fields = [field for field in required_fields if field not in comment or comment[field] is None]
        
        if missing_fields:
            logging.warning(f"Comment {comment.get('comment_id')} missing fields: {missing_fields} - keeping anyway")
            
        valid_comments.append(comment)
    
    # Store data to pass on to load stage
    context['ti'].xcom_push(key='transformed_submissions', value=json.dumps(valid_submissions))
    context['ti'].xcom_push(key='transformed_comments', value=json.dumps(valid_comments))
    
    logging.info(f"Validation completed. Valid submissions: {len(valid_submissions)}/{len(submissions_data)}, Valid comments: {len(valid_comments)}/{len(comments_data)}")
    logging.info(f"Removed {len(submissions_data) - len(valid_submissions)} duplicate submissions and {len(comments_data) - len(valid_comments)} duplicate comments")
    
    return len(valid_submissions), len(valid_comments)

def load_to_mongodb(**context):
    """Load data to MongoDB with progress tracking"""
    start_time = datetime.now()
    logging.info(f"Starting data loading to MongoDB at {start_time}")
    
    # Get data from transform task
    submissions_data = json.loads(context['ti'].xcom_pull(task_ids='transform_data', key='transformed_submissions'))
    comments_data = json.loads(context['ti'].xcom_pull(task_ids='transform_data', key='transformed_comments'))
    
    logging.info(f"Data size: {len(submissions_data)} submissions, {len(comments_data)} comments")
    
    if not submissions_data and not comments_data:
        logging.warning("No data to load. Skipping MongoDB operations.")
        return 0, 0
    
    try:
        logging.info(f"Connecting to MongoDB at {mongo_uri.split('@')[-1] if mongo_uri and '@' in mongo_uri else 'configured URI'}")
        client = MongoClient(mongo_uri, server_api=ServerApi('1'))
        client.admin.command('ping')
        logging.info("MongoDB connection successful")
        
        submissions_collection = client.posts.reddit_submissions
        comments_collection = client.posts.reddit_comments
        
        submission_results = []
        comment_results = []
        submission_count = len(submissions_data)
        comment_count = len(comments_data)
        progress_interval = max(1, min(100, int(submission_count / 10))) if submission_count > 0 else 1
        
        if submissions_data:
            logging.info(f"Processing {submission_count} submissions")
            for i, submission in enumerate(submissions_data):
                try:
                    # Upsert to update existing or insert new records
                    # but by right there should be no updates of existing records
                    result = submissions_collection.update_one(
                        {"submission_id": submission["submission_id"]},
                        {"$set": submission},
                        upsert=True
                    )
                    submission_results.append(result.upserted_id or result.modified_count)
                    
                    # Log progress at intervals
                    if (i + 1) % progress_interval == 0 or i + 1 == submission_count:
                        elapsed = datetime.now() - start_time
                        percent = ((i + 1) / submission_count) * 100
                        logging.info(f"Submissions progress: {i+1}/{submission_count} ({percent:.1f}%) - Time elapsed: {elapsed}")
                        
                except Exception as e:
                    logging.error(f"Error inserting submission {submission.get('submission_id', 'unknown')}: {e}")
        
        if comments_data:
            comment_start_time = datetime.now()
            logging.info(f"Processing {comment_count} comments")
            progress_interval = max(1, min(500, int(comment_count / 20))) if comment_count > 0 else 1
            
            for i, comment in enumerate(comments_data):
                try:
                    # Upsert to update existing or insert new records
                    # but by right there should be no updates of existing records
                    result = comments_collection.update_one(
                        {"comment_id": comment["comment_id"]},
                        {"$set": comment},
                        upsert=True
                    )
                    comment_results.append(result.upserted_id or result.modified_count)
                    
                    # Log progress at intervals
                    if (i + 1) % progress_interval == 0 or i + 1 == comment_count:
                        elapsed = datetime.now() - comment_start_time
                        total_elapsed = datetime.now() - start_time
                        percent = ((i + 1) / comment_count) * 100
                        logging.info(f"Comments progress: {i+1}/{comment_count} ({percent:.1f}%) - Time elapsed: {elapsed} (total: {total_elapsed})")
                        
                except Exception as e:
                    logging.error(f"Error inserting comment {comment.get('comment_id', 'unknown')}: {e}")
        
        total_time = datetime.now() - start_time
        success_rate_submissions = (len(submission_results) / submission_count * 100) if submission_count > 0 else 0
        success_rate_comments = (len(comment_results) / comment_count * 100) if comment_count > 0 else 0
        
        logging.info(f"MongoDB loading completed in {total_time}")
        logging.info(f"Submissions: {len(submission_results)}/{submission_count} loaded successfully ({success_rate_submissions:.1f}%)")
        logging.info(f"Comments: {len(comment_results)}/{comment_count} loaded successfully ({success_rate_comments:.1f}%)")
        
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