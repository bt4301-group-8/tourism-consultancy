import os
import sys
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from pymongo import MongoClient
import json
from dotenv import load_dotenv
from pathlib import Path
import pandas as pd

# Define base directory (project root)
BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# Add backend directory to Python path for imports
sys.path.append(str(BASE_DIR))

from backend.src.currency.currency_scraper import CurrencyScraper

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

def calculate_date_range():
    """Calculate the date range for the previous month."""
    # Today's date
    today = datetime.now()
    
    first_day_of_current_month = datetime(today.year, today.month, 1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = datetime(last_day_of_previous_month.year, 
                                           last_day_of_previous_month.month, 1)
    
    return first_day_of_previous_month.date(), last_day_of_previous_month.date()

def extract_currency_data(**kwargs):
    """Extract currency data for the past month."""
    start_date, end_date = calculate_date_range()
    
    # Create a CurrencyScraper instance with the date range
    scraper = CurrencyScraper(
        start_date=start_date,
        end_date=end_date
    )
    
    # Scrape the data
    monthly_avg_df = scraper.scrape()
    
    if monthly_avg_df is None or monthly_avg_df.empty:
        raise ValueError("No currency data was extracted")
    
    # Fix for recursion error: Convert DataFrame to a list of dictionaries manually
    # Ensure all data types are JSON serializable
    result = []
    for _, row in monthly_avg_df.iterrows():
        record = {
            'YearMonth': str(row['YearMonth']),
            'Currency': str(row['Currency']),
            'AverageRate': float(row['AverageRate'])
        }
        result.append(record)
    
    # Convert to JSON string
    return json.dumps(result)

def transform_currency_data(**kwargs):
    """Transform currency data to match MongoDB schema."""
    ti = kwargs['ti']
    monthly_avg_json = ti.xcom_pull(task_ids='extract_currency_data')
    
    # Convert JSON string back to a list of dictionaries
    monthly_avg_data = json.loads(monthly_avg_json)
    
    # Data is already transformed in the extract step, just pass it through
    # This maintains the task structure but avoids redundant transformations
    return monthly_avg_json

def load_to_mongodb(**kwargs):
    """Load transformed currency data to MongoDB."""
    ti = kwargs['ti']
    transformed_data_json = ti.xcom_pull(task_ids='transform_currency_data')
    transformed_data = json.loads(transformed_data_json)
    
    # Connect to MongoDB
    client = MongoClient(mongo_uri)
    db = client[mongodb_name]
    currency_collection = db.factors.currency
    
    # Batch insert documents into the currency collection
    # Each record will have its own document as shown in the screenshot
    if transformed_data:
        inserted_count = 0
        updated_count = 0
        
        for item in transformed_data:
            # Check if this currency and month combination already exists
            existing = currency_collection.find_one({
                'YearMonth': item['YearMonth'],
                'Currency': item['Currency']
            })
            
            if existing:
                # Update existing record
                result = currency_collection.update_one(
                    {'YearMonth': item['YearMonth'], 'Currency': item['Currency']},
                    {'$set': {'AverageRate': item['AverageRate']}}
                )
                if result.modified_count > 0:
                    updated_count += 1
            else:
                # Insert new record
                result = currency_collection.insert_one(item)
                if result.inserted_id:
                    inserted_count += 1
    
        return f"Processed {len(transformed_data)} currency records. Inserted {inserted_count} new records, updated {updated_count} existing records."
    
    return "No currency data to load"

# Create DAG
with DAG(
    'currency_etl_pipeline',
    default_args=default_args,
    description='Monthly ETL pipeline for Southeast Asian currency exchange rates',
    schedule_interval='0 0 1 * *',  # Run at midnight on the 1st day of each month
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['currency', 'etl'],
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_currency_data',
        python_callable=extract_currency_data,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform_currency_data',
        python_callable=transform_currency_data,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongodb,
        provide_context=True,
    )
    
    # Set task dependencies
    extract_task >> transform_task >> load_task