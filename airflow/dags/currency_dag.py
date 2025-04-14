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

# Define base directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# Add backend directory
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
    today = datetime.now()
    
    first_day_of_current_month = datetime(today.year, today.month, 1)
    last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
    first_day_of_previous_month = datetime(last_day_of_previous_month.year, 
                                           last_day_of_previous_month.month, 1)
    
    return first_day_of_previous_month.date(), last_day_of_previous_month.date()

def extract_currency_data(**kwargs):
    """Extract currency data for the past month."""
    start_date, end_date = calculate_date_range()
    
    scraper = CurrencyScraper(
        start_date=start_date,
        end_date=end_date
    )
    
    monthly_avg_df = scraper.scrape()
    
    if monthly_avg_df is None or monthly_avg_df.empty:
        raise ValueError("No currency data was extracted")
    
    # Ensure JSON serializable
    result = []
    for _, row in monthly_avg_df.iterrows():
        record = {
            'YearMonth': str(row['YearMonth']),
            'Currency': str(row['Currency']),
            'AverageRate': float(row['AverageRate'])
        }
        result.append(record)
    
    return json.dumps(result)

def transform_currency_data(**kwargs):
    """Transform currency data to match MongoDB schema."""
    ti = kwargs['ti']
    monthly_avg_json = ti.xcom_pull(task_ids='extract_currency_data')
    monthly_avg_data = json.loads(monthly_avg_json)
    
    transformed_data = []
    invalid_records = 0
    
    for item in monthly_avg_data:
        # Validate fields
        if not all(key in item for key in ['YearMonth', 'Currency', 'AverageRate']):
            print(f"Skipping record missing required fields: {item}")
            invalid_records += 1
            continue
        
        # Validate data types
        try:
            if not isinstance(item['Currency'], str) or len(item['Currency']) == 0:
                raise ValueError(f"Invalid Currency: {item['Currency']}")
            item['AverageRate'] = float(item['AverageRate'])
            transformed_data.append(item)
            
        except (ValueError, TypeError) as e:
            print(f"Validation error in record {item}: {e}")
            invalid_records += 1
    
    print(f"Validation completed: {len(transformed_data)} valid records, {invalid_records} invalid records")
    
    return json.dumps(transformed_data)

def load_to_mongodb(**kwargs):
    """Load transformed currency data to MongoDB."""
    ti = kwargs['ti']
    transformed_data_json = ti.xcom_pull(task_ids='transform_currency_data')
    transformed_data = json.loads(transformed_data_json)
    
    client = MongoClient(mongo_uri)
    db = client[mongodb_name]
    currency_collection = db.factors.currency
    
    if transformed_data:
        inserted_count = 0
        updated_count = 0
        
        for item in transformed_data:
            # Use upsert to handle both insertion and updates
            result = currency_collection.update_one(
                {'YearMonth': item['YearMonth'], 'Currency': item['Currency']},
                {'$set': item},
                upsert=True
            )
            
            if result.upserted_id:
                inserted_count += 1
            elif result.modified_count > 0:
                updated_count += 1
    
        return f"Processed {len(transformed_data)} currency records. Inserted {inserted_count} new records, updated {updated_count} existing records."
    
    return "No currency data to load"

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
    
    extract_task >> transform_task >> load_task