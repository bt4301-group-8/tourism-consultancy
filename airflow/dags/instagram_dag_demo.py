import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import json
from dotenv import load_dotenv
from pathlib import Path

# Define base directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

sys.path.append(str(BASE_DIR))

from backend.src.instagram.instagram_crawler import InstagramCrawler
from backend.src.instagram.utils import reorganize_tourism_data

mongo_uri = os.getenv("MONGO_URI")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'instagram_demo_etl_pipeline',
    default_args=default_args,
    description='Demo ETL pipeline for Instagram data from Singapore only (strictly max 10 posts)',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['instagram', 'demo', 'etl', 'singapore'],
) as dag:
    
    def extract_singapore_instagram_data(**kwargs):
        """Extract Instagram data for Singapore only, limited to 10 posts"""
        # Only target Singapore for this demo
        city_hashtags = ["singapore"]
        
        print(f"Starting Instagram data extraction for Singapore only (max 10 posts)")
        
        try:
            # Initialize and login to Instagram
            crawler = InstagramCrawler(
                delay_range=[10, 15],  # Use a higher delay to avoid rate limits
                session_json_path=f"{BASE_DIR}/backend/configs/session.json",
                city_geo_json_path=f"{BASE_DIR}/backend/configs/city_geo.json",
                existing_city_posts_path=f"{BASE_DIR}/backend/data/city_posts.json"
            )
            
            # Create a custom method to extract exactly 10 posts max without depending on existing data
            def get_singapore_data_limited():
                """Get Singapore data from hashtags and location methods, strictly limited to 10 posts total"""
                all_posts = []
                
                # First try by hashtag (5 posts)
                try:
                    print("Fetching posts via hashtag method...")
                    hashtags_master_dict = {}
                    hashtags_master_dict = crawler.get_info_by_hashtags(
                        hashtags=city_hashtags,
                        hashtags_master_dict=hashtags_master_dict,
                        search_type="recent",
                        amount=10  # Limit to 5 posts by hashtag
                    )
                    
                    # Add posts to our collection
                    if "singapore" in hashtags_master_dict:
                        hashtag_posts = hashtags_master_dict["singapore"]
                        print(f"Retrieved {len(hashtag_posts)} posts via hashtag")
                        all_posts.extend(hashtag_posts)
                except Exception as e:
                    print(f"Error in hashtag retrieval: {e}")
                
                # Then try by location (5 more posts or less to reach total of 10)
                remain_posts = 10 - len(all_posts)
                if remain_posts > 0:
                    try:
                        print(f"Fetching {remain_posts} more posts via location method...")
                        location_master_dict = {}
                        location_master_dict = crawler.get_info_by_location(
                            city_name="singapore",
                            location_master_dict=location_master_dict,
                            search_type="recent",
                            amount=remain_posts  # Limit to remaining posts needed
                        )
                        
                        # Add posts to our collection
                        if "singapore" in location_master_dict:
                            location_posts = location_master_dict["singapore"]
                            print(f"Retrieved {len(location_posts)} posts via location")
                            all_posts.extend(location_posts)
                    except Exception as e:
                        print(f"Error in location retrieval: {e}")
                
                # Enforce the limit of 10
                if len(all_posts) > 10:
                    all_posts = all_posts[:10]
                    
                print(f"Final post count: {len(all_posts)} (max: 10)")
                return all_posts
            
            def serialize_datetime(obj):
                if isinstance(obj, datetime):
                    return obj.isoformat()
                return str(obj)
            
            # Get exactly 10 posts (or fewer if not available)
            singapore_posts = get_singapore_data_limited()
            
            # Pre-process the posts to convert datetime objects to strings
            for post in singapore_posts:
                if isinstance(post.get('date'), datetime):
                    post['date'] = post['date'].isoformat()
            
            # Save just these posts to a demo file (don't modify the original data file)
            demo_file = f"{BASE_DIR}/backend/data/singapore_demo_posts.json"
            with open(demo_file, "w") as f:
                json.dump(singapore_posts, f, indent=4, default=serialize_datetime)
                
            print(f"Extracted {len(singapore_posts)} posts for Singapore saved to {demo_file}")
            
            # Return just the posts we extracted, not in a nested structure
            return json.dumps(singapore_posts, default=serialize_datetime)
            
        except Exception as e:
            print(f"Error during Instagram data extraction: {e}")
            raise
    
    def transform_singapore_instagram_data(**kwargs):
        """Transform Instagram data for Singapore only, setting all month_year to previous month"""
        ti = kwargs['ti']
        posts_json = ti.xcom_pull(task_ids='extract_singapore_instagram_data')
        singapore_posts = json.loads(posts_json)
        
        flattened_posts = []
        
        try:
            print(f"Transforming {len(singapore_posts)} Singapore posts")
            
            # Calculate previous month (2025-03)
            today = datetime.now()
            first_day_of_current_month = datetime(today.year, today.month, 1)
            last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
            previous_month = last_day_of_previous_month.strftime("%Y-%m")
            
            print(f"Setting all posts to previous month: {previous_month}")
            
            for post in singapore_posts:
                # Set month_year to previous month regardless of actual date
                post["month_year"] = previous_month
                post["country"] = "singapore"
                flattened_posts.append(post)
            
            print(f"Transformed {len(flattened_posts)} Singapore posts to month {previous_month}")
            
            # Save the flattened Singapore posts
            flattened_posts_path = f"{BASE_DIR}/backend/data/flattened_singapore_demo_posts.json"
            with open(flattened_posts_path, 'w') as f:
                json.dump(flattened_posts, f, indent=2)
                
            return json.dumps(flattened_posts)
            
        except Exception as e:
            print(f"Error during Instagram data transformation: {e}")
            raise
    
    def load_to_mongodb(**kwargs):
        """Load transformed Instagram data (Singapore only) to MongoDB"""
        ti = kwargs['ti']
        posts_json = ti.xcom_pull(task_ids='transform_singapore_instagram_data')
        posts = json.loads(posts_json)
        
        print(f"Preparing to load {len(posts)} Singapore Instagram posts to MongoDB")
        
        if not posts:
            print("No Singapore posts to load. Skipping MongoDB operations.")
            return "No data to load"
        
        try:
            client = MongoClient(
                mongo_uri,
                ssl=True,
                tlsAllowInvalidCertificates=True,
                retryWrites=True,
                connectTimeoutMS=30000,
                socketTimeoutMS=30000,
                serverSelectionTimeoutMS=30000
            )
            
            print("Testing MongoDB connection...")
            client.admin.command('ping')
            print("MongoDB connection successful")
    
            instagram_collection = client.demo.posts.instagram
            
            inserted_count = 0
            updated_count = 0
            error_count = 0
            batch_size = 10
            
            # Process posts in batches
            for i in range(0, len(posts), batch_size):
                batch = posts[i:i+batch_size]
                print(f"Processing batch {i//batch_size + 1}/{(len(posts) + batch_size - 1)//batch_size}")
                
                for post in batch:
                    try:
                        # Create a unique identifier for each post
                        # Using caption as unique identifier since Instagram doesn't provide stable IDs
                        caption = post.get("caption", "")
                        if not caption:
                            continue
                            
                        # Add timestamp for tracking
                        post["processed_at"] = datetime.now().isoformat()
                        
                        # Update or insert the post
                        result = instagram_collection.update_one(
                            {"caption": caption, "country": post["country"]},
                            {"$set": post},
                            upsert=True
                        )
                        
                        if result.upserted_id:
                            inserted_count += 1
                        elif result.modified_count > 0:
                            updated_count += 1
                            
                    except Exception as e:
                        print(f"Error processing post: {str(e)[:200]}...")
                        error_count += 1
                        continue
                
                from time import sleep
                sleep(2)
            
            client.close()
            print(f"MongoDB loading completed. Inserted: {inserted_count}, Updated: {updated_count}, Errors: {error_count}")
            
            return f"Inserted: {inserted_count}, Updated: {updated_count}, Errors: {error_count}"
            
        except Exception as e:
            print(f"Error during MongoDB operations: {str(e)[:200]}...")
            print("Please check your MongoDB connection string and network settings")
            
            fallback_path = f"{BASE_DIR}/backend/data/singapore_posts_fallback.json"
            with open(fallback_path, 'w') as f:
                json.dump(posts, f, indent=2)
                
            print(f"Data saved to fallback file: {fallback_path}")
            
            return f"Error: MongoDB connection failed. Data saved to fallback file."
    
    extract_task = PythonOperator(
        task_id='extract_singapore_instagram_data',
        python_callable=extract_singapore_instagram_data,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform_singapore_instagram_data',
        python_callable=transform_singapore_instagram_data,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongodb,
        provide_context=True,
    )
    
    extract_task >> transform_task >> load_task