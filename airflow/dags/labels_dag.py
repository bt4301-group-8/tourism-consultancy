
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

# Define base directory
BASE_DIR = Path(__file__).resolve().parent.parent.parent
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# Add base directory
sys.path.append(str(BASE_DIR))

from backend.src.visitor_scraper.visitor_scraper import VisitorScraper

mongo_uri = os.getenv("MONGO_URI")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# List of countries to extract visitor data for
countries = [
    "Thailand",
    "Singapore",
    "Malaysia",
    "Indonesia",
    "Vietnam",
    "Philippines",
    "Cambodia",
    "Laos",
    "Myanmar",
    "Brunei"
]

with DAG(
    'visitor_labels_etl_pipeline',
    default_args=default_args,
    description='Monthly ETL pipeline for visitor statistics from Southeast Asian countries',
    schedule_interval='0 4 1 * *',  # Staggered to 4am to avoid resource clash
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['visitors', 'labels', 'etl'],
) as dag:
    
    def extract_visitor_data(**kwargs):
        """Extract visitor data for Southeast Asian countries for the previous month only"""
        print("Starting visitor data extraction")
        
        # Get last month in YYYY-MM format
        today = datetime.now()
        first_day_of_current_month = datetime(today.year, today.month, 1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        last_month = last_day_of_previous_month.strftime('%Y-%m')
        
        print(f"Extracting visitor data for the month: {last_month}")
        
        scraper = VisitorScraper()
        
        previous_month_data = []
        
        # Extract data for each country
        for country in countries:
            try:
                print(f"Extracting visitor data for {country}")
                
                visitor_json = scraper.get_visitors(country)
                visitor_data = json.loads(visitor_json)
                
                # Filter for the previous month only
                for record in visitor_data:
                    # Check if the record is for last month
                    if record.get('month_year') == last_month:
                        previous_month_data.append(record)
                
                print(f"Found {sum(1 for r in previous_month_data if r['country'] == country)} records for {country} in {last_month}")
                
            except Exception as e:
                print(f"Error extracting visitor data for {country}: {e}")
        
        print(f"Extracted a total of {len(previous_month_data)} visitor records for {last_month}")
        
        # to pull for next task
        return json.dumps(previous_month_data)
    
    def transform_visitor_data(**kwargs):
        """Transform visitor data to match MongoDB schema"""
        ti = kwargs['ti']
        visitor_data_json = ti.xcom_pull(task_ids='extract_visitor_data')
        visitor_data = json.loads(visitor_data_json)
        
        print(f"Transforming {len(visitor_data)} visitor records")
        
        transformed_data = []
        for record in visitor_data:
            month_year = record.get('month_year')
            
            # Match schema
            transformed_record = {
                'country': record.get('country'),
                'month_year': month_year,
                'value': float(record.get('value', 0))
            }
            transformed_data.append(transformed_record)
        
        print(f"Transformed {len(transformed_data)} visitor records")
        
        # to pull for next task
        return json.dumps(transformed_data)
    
    def load_to_mongodb(**kwargs):
        """Load visitor data to MongoDB"""
        ti = kwargs['ti']
        transformed_data_json = ti.xcom_pull(task_ids='transform_visitor_data')
        transformed_data = json.loads(transformed_data_json)
        
        print(f"Loading {len(transformed_data)} visitor records to MongoDB")
        
        if not transformed_data:
            print("No visitor data to load. Skipping MongoDB operations.")
            return "No data to load"
        
        try:
            # Connect to MongoDB
            client = MongoClient(mongo_uri)
            
            # Target collection
            visitor_collection = client.labels.visitor_count
            
            inserted_count = 0
            updated_count = 0
            
            # Get the month we're processing from the first record
            if transformed_data:
                current_month = transformed_data[0].get('month_year')
            else:
                # Last check if no data found
                today = datetime.now()
                first_day_of_current_month = datetime(today.year, today.month, 1)
                last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
                current_month = last_day_of_previous_month.strftime('%Y-%m')
            
            print(f"Processing visitor data for period: {current_month}")
            
            for record in transformed_data:
                try:
                    # Upsert can handle both new and existing records
                    result = visitor_collection.update_one(
                        {
                            'country': record['country'],
                            'month_year': record['month_year']
                        },
                        {'$set': record},
                        upsert=True
                    )
                    
                    if result.upserted_id:
                        inserted_count += 1
                    elif result.modified_count > 0:
                        updated_count += 1
                        
                except Exception as e:
                    print(f"Error processing record: {e}")
                    print(f"Record: {record}")
            
            # Verify num records added
            verification_count = visitor_collection.count_documents({
                'month_year': current_month
            })
            
            client.close()
            print(f"MongoDB loading completed. Inserted: {inserted_count}, Updated: {updated_count}")
            print(f"Verification: Collection now contains {verification_count} records for {current_month}")
            
            return f"Inserted: {inserted_count}, Updated: {updated_count}, Verified: {verification_count}"
        
        except Exception as e:
            print(f"Error during MongoDB operations: {e}")
            raise
    
    extract_task = PythonOperator(
        task_id='extract_visitor_data',
        python_callable=extract_visitor_data,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform_visitor_data',
        python_callable=transform_visitor_data,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load_to_mongodb',
        python_callable=load_to_mongodb,
        provide_context=True,
    )
    
    extract_task >> transform_task >> load_task