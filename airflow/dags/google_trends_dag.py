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

from backend.src.google_trends.google_trends_scraper import GoogleTrendsScraper

mongo_uri = os.getenv("MONGO_URI")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def calculate_date_range():
    """Calculate the date range for the previous month."""
    today = datetime.now()
    first_day_of_current_month = datetime(today.year, today.month, 1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = datetime(last_day_of_previous_month.year, 
                                           last_day_of_previous_month.month, 1)
    
    return (
        first_day_of_previous_month.strftime('%Y-%m-%d'), 
        last_day_of_previous_month.strftime('%Y-%m-%d')
    )

def extract_google_trends_data(**kwargs):
    """Extract Google Trends data for the past month."""
    date_from, date_to = calculate_date_range()
    
    scraper = GoogleTrendsScraper()
    scraper.create_tasks(date_from=date_from, date_to=date_to)
    scraper.fetch_results()
    df = scraper.convert_to_csv()
    
    if df is None or df.empty:
        raise ValueError("No Google Trends data was extracted")
    
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

def transform_google_trends_data(**kwargs):
    """Transform Google Trends data to match MongoDB schema with basic validation."""
    ti = kwargs['ti']
    google_trends_json = ti.xcom_pull(task_ids='extract_google_trends_data')
    
    trends_data = json.loads(google_trends_json)
    
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
    transformed_data_json = ti.xcom_pull(task_ids='transform_google_trends_data')
    transformed_data = json.loads(transformed_data_json)
    
    client = MongoClient(mongo_uri)
    google_trends_collection = client.factors.google_trends
    
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
    
        return f"Processed {len(transformed_data)} Google Trends records. Inserted {inserted_count} new records, updated {updated_count} existing records."
    
    return "No Google Trends data to load"

# Works best during SG time 2pm-6pm
with DAG(
    'google_trends_etl_pipeline',
    default_args=default_args,
    description='Monthly ETL pipeline for Southeast Asian Google Trends data',
    schedule_interval='0 0 1 * *',  # 1am first day each month
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['google_trends', 'etl'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_google_trends_data',
        python_callable=extract_google_trends_data,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform_google_trends_data',
        python_callable=transform_google_trends_data,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongodb,
        provide_context=True,
    )
    
    extract_task >> transform_task >> load_task