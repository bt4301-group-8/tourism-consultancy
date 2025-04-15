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

# MongoDB connection details
mongo_uri = os.getenv("MONGO_URI")
mongodb_name = os.getenv("MONGODB_NAME")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
with DAG(
    'instagram_etl_pipeline',
    default_args=default_args,
    description='Monthly ETL pipeline for Instagram data from Southeast Asian cities',
    schedule_interval='0 2 1 * *',  # Run at 2 AM on the 1st day of each month
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['instagram', 'etl'],
) as dag:
    
    def extract_instagram_data(**kwargs):
        """Extract data from Instagram for Southeast Asian cities"""
        city_hashtags = [
            # Indonesia
            "bali", "jakarta",
            # Brunei
            "bandarseribegawan",
            # Thailand
            "bangkok", "phuket",
            # Cambodia
            "siemreap",
            # Philippines
            "cebu", "davao", "manila",
            # Vietnam
            "hanoi", "hochiminh",
            # Malaysia
            "kualalumpur", "johorbahru", "ipoh", "malacca",
            # Laos
            "luangprabang", "vientiane",
            # Myanmar
            "mandalay", "yangon",
            # Singapore
            "singapore"
        ]
        
        # Initialize and login to Instagram
        try:
            crawler = InstagramCrawler(
                delay_range=[2, 4],  # Use a slightly higher delay to avoid rate limits
                session_json_path=f"{BASE_DIR}/backend/configs/session.json",
                city_geo_json_path=f"{BASE_DIR}/backend/configs/city_geo.json",
                exisiting_city_posts_path=f"{BASE_DIR}/backend/data/city_posts.json"
            )
            
            # Fetch data for hashtags
            hashtags_master_dict = {}
            crawler.get_and_write_to_city_posts_hashtag(
                hashtags=city_hashtags,
                hashtags_master_dict=hashtags_master_dict,
                search_type="recent",
                amount=20  # Limit to 20 posts per hashtag to avoid rate limiting
            )
            
            # Also fetch data for specific locations if needed
            for city in city_hashtags:
                try:
                    location_master_dict = {}
                    crawler.get_and_write_to_city_posts_location(
                        city_name=city,
                        location_master_dict=location_master_dict,
                        search_type="recent",
                        amount=20  # Limit to 20 posts per location
                    )
                except Exception as e:
                    print(f"Error fetching location data for {city}: {e}")
                    continue
            
            # City posts are saved directly to file by the crawler
            city_posts_path = f"{BASE_DIR}/backend/data/city_posts.json"
            
            # Check if data was collected
            with open(city_posts_path, 'r') as f:
                city_posts = json.load(f)
                
            post_count = sum(len(posts) for posts in city_posts.values())
            print(f"Extracted {post_count} posts from {len(city_posts)} cities")
            
            return "success", post_count
            
        except Exception as e:
            print(f"Error during Instagram data extraction: {e}")
            raise
    
    def transform_instagram_data(**kwargs):
        """Transform Instagram data into country-based flattened format"""
        city_posts_path = f"{BASE_DIR}/backend/data/city_posts.json"
        flattened_posts = []
        
        try:
            # First reorganize data by country using the utility function
            reorganize_tourism_data(input_json_path=city_posts_path)
            
            # Load the country-organized data
            country_posts_path = f"{BASE_DIR}/backend/data/country_posts.json"
            with open(country_posts_path, 'r') as f:
                country_posts = json.load(f)
            
            # Flatten the data structure to have each post include its country
            for country, posts in country_posts.items():
                for post in posts:
                    # Add country and month_year to each post
                    if "date" in post:
                        try:
                            # Handle different date formats
                            if isinstance(post["date"], str):
                                # Parse string date if it's in ISO format
                                date_obj = datetime.fromisoformat(post["date"].replace('Z', '+00:00'))
                            else:
                                # Use the date directly if it's already a datetime object
                                date_obj = post["date"]
                                
                            # Format as YYYY-MM
                            post["month_year"] = date_obj.strftime("%Y-%m")
                        except Exception as e:
                            print(f"Error parsing date {post.get('date')}: {e}")
                            # Default to current month if date parsing fails
                            post["month_year"] = datetime.now().strftime("%Y-%m")
                    else:
                        # Default to current month if no date
                        post["month_year"] = datetime.now().strftime("%Y-%m")
                        
                    post["country"] = country
                    flattened_posts.append(post)
            
            print(f"Transformed {len(flattened_posts)} posts")
            
            # Save flattened data to a temporary file for debugging and as a checkpoint
            flattened_posts_path = f"{BASE_DIR}/backend/data/flattened_instagram_posts.json"
            with open(flattened_posts_path, 'w') as f:
                json.dump(flattened_posts, f, indent=2)
                
            # Pass the data to the next step
            return json.dumps(flattened_posts)
            
        except Exception as e:
            print(f"Error during Instagram data transformation: {e}")
            raise
    
    def load_to_mongodb(**kwargs):
        """Load transformed Instagram data to MongoDB"""
        ti = kwargs['ti']
        posts_json = ti.xcom_pull(task_ids='transform_instagram_data')
        posts = json.loads(posts_json)
        
        print(f"Preparing to load {len(posts)} Instagram posts to MongoDB")
        
        if not posts:
            print("No posts to load. Skipping MongoDB operations.")
            return "No data to load"
        
        try:
            # Connect to MongoDB
            client = MongoClient(mongo_uri)
            db = client[mongodb_name]
            instagram_collection = db.posts.instagram
            
            # Track statistics
            inserted_count = 0
            updated_count = 0
            error_count = 0
            
            # Process posts in batches to avoid overwhelming the database
            batch_size = 100
            
            for i in range(0, len(posts), batch_size):
                batch = posts[i:i+batch_size]
                
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
                        print(f"Error processing post: {e}")
                        error_count += 1
                
                print(f"Processed batch {i//batch_size + 1}/{(len(posts) + batch_size - 1)//batch_size}")
            
            client.close()
            print(f"MongoDB loading completed. Inserted: {inserted_count}, Updated: {updated_count}, Errors: {error_count}")
            
            return f"Inserted: {inserted_count}, Updated: {updated_count}, Errors: {error_count}"
            
        except Exception as e:
            print(f"Error during MongoDB operations: {e}")
            raise
    
    extract_task = PythonOperator(
        task_id='extract_instagram_data',
        python_callable=extract_instagram_data,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform_instagram_data',
        python_callable=transform_instagram_data,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongodb,
        provide_context=True,
    )
    
    extract_task >> transform_task >> load_task