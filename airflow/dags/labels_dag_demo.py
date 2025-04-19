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

# Only process Singapore data for the demo
country = "Singapore"

with DAG(
    'visitor_labels_demo_etl_pipeline',
    default_args=default_args,
    description=f'Demo ETL pipeline for {country} visitor statistics only',
    schedule_interval=None,  # Set to None for manual triggering
    start_date=datetime(2025, 4, 1),
    catchup=False,
    tags=['visitors', 'labels', 'demo', 'etl'],
) as dag:
    
    def extract_singapore_visitor_data(**kwargs):
        """Extract visitor data for Singapore for the previous month only"""
        print(f"Starting visitor data extraction for {country}")
        
        # Calculate previous month in YYYY-MM format
        today = datetime.now()
        first_day_of_current_month = datetime(today.year, today.month, 1)
        last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
        previous_month = last_day_of_previous_month.strftime('%Y-%m')
        
        print(f"Targeting visitor data for the month: {previous_month}")
        
        scraper = VisitorScraper()
        
        try:
            visitor_json = scraper.get_visitors(country)
            visitor_data = json.loads(visitor_json)
            
            # Filter for previous month only
            filtered_data = []
            for record in visitor_data:
                # Convert ISO date format to YYYY-MM for comparison
                record_date = datetime.fromisoformat(record.get('month_year').replace('Z', '+00:00'))
                record_month_year = record_date.strftime('%Y-%m')
                
                if record_month_year == previous_month:
                    filtered_data.append(record)
            
            print(f"Found {len(filtered_data)} records for {country} in {previous_month}")
            
            if not filtered_data:
                print(f"No visitor data found for {country} in {previous_month}. Checking for most recent data.")
                
                # If no data for the previous month, get the most recent data
                df = pd.DataFrame(visitor_data)
                df['month_year'] = pd.to_datetime(df['month_year'])
                df = df.sort_values('month_year', ascending=False).head(1)
                
                most_recent_data = df.to_dict('records')
                
                # Convert datetime back to string format
                for record in most_recent_data:
                    record['month_year'] = record['month_year'].strftime('%Y-%m')
                
                print(f"Using most recent data from {most_recent_data[0]['month_year'] if most_recent_data else 'N/A'}")
                
                return json.dumps(most_recent_data)
            
            return json.dumps(filtered_data)
            
        except Exception as e:
            print(f"Error extracting visitor data for {country}: {e}")
            raise
    
    def transform_visitor_data(**kwargs):
        """Transform visitor data to match MongoDB schema"""
        ti = kwargs['ti']
        visitor_data_json = ti.xcom_pull(task_ids='extract_singapore_visitor_data')
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
            client = MongoClient(mongo_uri)
            visitor_collection = client.demo.labels.visitor_count
            
            inserted_count = 0
            updated_count = 0
            
            processed_months = set()
            
            for record in transformed_data:
                try:
                    processed_months.add(record['month_year'])
                    
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
            
            # Verify records added for each month
            verification_results = []
            for month in processed_months:
                count = visitor_collection.count_documents({
                    'month_year': month,
                    'country': country
                })
                verification_results.append(f"{month}: {count}")
            
            client.close()
            print(f"MongoDB loading completed. Inserted: {inserted_count}, Updated: {updated_count}")
            print(f"Verification results: {', '.join(verification_results)}")
            
            return f"Inserted: {inserted_count}, Updated: {updated_count}, Verified: {', '.join(verification_results)}"
        
        except Exception as e:
            print(f"Error during MongoDB operations: {e}")
            raise
    
    extract_task = PythonOperator(
        task_id='extract_singapore_visitor_data',
        python_callable=extract_singapore_visitor_data,
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