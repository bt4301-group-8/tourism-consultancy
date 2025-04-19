import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import json
import pandas as pd
from dotenv import load_dotenv
from pathlib import Path

# Define base directory (project root)
BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# Add backend directory to Python path for imports
sys.path.append(str(BASE_DIR))

from backend.src.google_trends.client.client import RestClient

mongo_uri = os.getenv("MONGO_URI")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# countries are manually defined in the post_data dictionaries in the original GoogleTrendsScraper class
# initialise a similar class for the demo that just includes Singapore
class GoogleTrendsDemoScraper:
    def __init__(self):
        load_dotenv()
        self.login = os.getenv('SEO_LOGIN')
        self.password = os.getenv('SEO_PASSWORD')
        self.client = RestClient(self.login, self.password)
        self.tasks = []
        self.results = []

    def create_singapore_task(self, date_from, date_to, category_code=67):
        """Create a task to fetch Google Trends data for Singapore only"""
        post_data = dict()
        post_data[len(post_data)] = dict(
            date_from=date_from,
            date_to=date_to,
            keywords=["Singapore"],
            category_code=category_code,
        )

        response = self.client.post("/v3/keywords_data/google_trends/explore/task_post", post_data)
        if response["status_code"] == 20000:
            print(f"Task created successfully: {response}")
        else:
            print(f"Error creating task. Code: {response['status_code']} Message: {response['status_message']}")

    def wait_for_tasks(self, wait_seconds=60):
        """Wait for tasks to complete"""
        print(f"Waiting {wait_seconds} seconds for tasks to complete...")
        import time
        time.sleep(wait_seconds)

    def fetch_results(self):
        """Fetch results from completed tasks"""
        response = self.client.get("/v3/keywords_data/google_trends/explore/tasks_ready")
        print(f"Found {len(response.get('tasks', []))} ready tasks")
        
        if response['status_code'] == 20000:
            for task in response['tasks']:
                if task.get('result'):
                    for result_info in task['result']:
                        endpoint = result_info.get('endpoint')
                        if endpoint:
                            result = self.client.get(endpoint)
                            self.results.append(result)
            print(f"Successfully fetched {len(self.results)} results")
        else:
            print(f"Error fetching results: {response['status_message']}")

    def convert_to_dataframe(self):
        """Process results and convert to DataFrame with Singapore data only"""
        rows = []

        def timestamp_to_month_year(ts):
            return datetime.utcfromtimestamp(ts).strftime('%Y-%m')

        for entry in self.results:
            tasks = entry.get('tasks', [])
            for task in tasks:
                for result in task.get('result', []):
                    for item in result.get('items', []):
                        keywords = item.get('keywords', [])
                        # We only expect "Singapore" in the keywords
                        for data_point in item.get('data', []):
                            month_year = timestamp_to_month_year(data_point['timestamp'])
                            for idx, country in enumerate(keywords):
                                country_value = data_point['values'][idx]
                                rows.append({
                                    'month_year': month_year,
                                    'country': country,
                                    'value': country_value
                                })

        if not rows:
            print("No data was retrieved. Please check if the tasks returned any results.")
            return pd.DataFrame()
            
        df = pd.DataFrame(rows)
        df = df.drop_duplicates(subset=['month_year', 'country'], keep='first')
        
        # Sort and format the data
        df['month_year'] = pd.to_datetime(df['month_year'])
        df.sort_values(by=['month_year', 'country'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df['month_year'] = df['month_year'].dt.strftime('%Y-%m')
        
        return df

def calculate_date_range():
    """Calculate the date range for the previous month."""
    today = datetime.now()
    
    first_day_of_current_month = datetime(today.year, today.month, 1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = datetime(last_day_of_previous_month.year, 
                                           last_day_of_previous_month.month, 1)
    
    return first_day_of_previous_month.date().strftime('%Y-%m-%d'), last_day_of_previous_month.date().strftime('%Y-%m-%d')

def extract_singapore_trends_data(**kwargs):
    """Extract Google Trends data for Singapore for the past month."""
    date_from, date_to = calculate_date_range()
    print(f"Extracting Google Trends data for Singapore from {date_from} to {date_to}")
    
    scraper = GoogleTrendsDemoScraper()
    
    scraper.create_singapore_task(date_from=date_from, date_to=date_to)
    scraper.wait_for_tasks()
    scraper.fetch_results()
    
    df = scraper.convert_to_dataframe()
    
    if df is None or df.empty:
        raise ValueError("No Google Trends data was extracted for Singapore")
    
    # Ensure JSON-serializable
    result = []
    for _, row in df.iterrows():
        record = {
            'YearMonth': str(row['month_year']),
            'Country': str(row['country']),
            'Value': float(row['value'])
        }
        result.append(record)
    
    return json.dumps(result)

def transform_trends_data(**kwargs):
    """Transform Google Trends data to match MongoDB schema with basic validation."""
    ti = kwargs['ti']
    trends_json = ti.xcom_pull(task_ids='extract_singapore_trends_data')
    
    trends_data = json.loads(trends_json)
    
    valid_data = []
    invalid_count = 0
    
    for record in trends_data:
        # Check required fields
        if not all(key in record for key in ['YearMonth', 'Country', 'Value']):
            invalid_count += 1
            continue
        
        # Check value type
        try:
            record['Value'] = float(record['Value'])
        except (ValueError, TypeError):
            invalid_count += 1
            continue
            
        # Convert to string
        record['YearMonth'] = str(record['YearMonth'])
        record['Country'] = str(record['Country'])
        
        valid_data.append(record)
    
    print(f"Data validation: {len(valid_data)} valid records, {invalid_count} invalid records filtered out")
    
    if not valid_data:
        raise ValueError("No valid records after transformation")
    
    return json.dumps(valid_data)
    
def load_to_mongodb(**kwargs):
    """Load transformed Google Trends data to MongoDB."""
    ti = kwargs['ti']
    transformed_data_json = ti.xcom_pull(task_ids='transform_trends_data')
    transformed_data = json.loads(transformed_data_json)
    
    client = MongoClient(mongo_uri)
    google_trends_collection = client.demo.factors.google_trends
    
    if transformed_data:
        inserted_count = 0
        updated_count = 0
        
        for item in transformed_data:
            # Use upsert=True to insert if not exists or update if exists
            result = google_trends_collection.update_one(
                {'YearMonth': item['YearMonth'], 'Country': item['Country']},
                {'$set': {'Value': item['Value']}},
                upsert=True
            )
            
            # Track inserts vs updates
            if result.upserted_id:
                inserted_count += 1
            elif result.modified_count > 0:
                updated_count += 1
    
        return f"Processed {len(transformed_data)} Google Trends records for Singapore. Inserted {inserted_count} new records, updated {updated_count} existing records."
    
    return "No Google Trends data to load"

with DAG(
    'google_trends_demo_etl_pipeline',
    default_args=default_args,
    description='Demo ETL pipeline for Google Trends data for Singapore only',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['google_trends', 'demo', 'etl'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_singapore_trends_data',
        python_callable=extract_singapore_trends_data,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform_trends_data',
        python_callable=transform_trends_data,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongodb,
        provide_context=True,
    )
    
    extract_task >> transform_task >> load_task