# airflow imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# third party imports
import pendulum
from dotenv import load_dotenv
from pathlib import Path
import json

# standard library imports
from datetime import timedelta
import os
import logging
import sys

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# environment set up
BASE_DIR = Path(__file__).resolve().parent.parent.parent # go up from dags -> airflow -> tourism-consultancy
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# add project root to sys.path for importing modules
sys.path.insert(0, str(BASE_DIR))

# import the dataprocessor module from data_pipeline
try:
    from backend.src.mongodb_to_supabase_pipeline.data_pipeline_demo import DataProcessor
except ImportError as e:
    logging.error(f"Failed to import DataProcessor: {e}")
    # define a dummy DataProcessor class
    class DataProcessor:
        def __init__(self): logging.error("DataProcessor dummy class used due to import error.")
        def _get_sentiment_instagram(self): pass
        def _get_sentiment_reddit(self): pass
        def engineer_google_trends(self): pass
        def engineer_currency(self): pass
        def engineer_instagram(self): pass
        def engineer_reddit(self): pass
        def engineer_tripadvisor(self): pass
        def engineer_labels(self): pass
        def get_all_dicts(self): return {}, {}, {}, {}, {}, {} # Return dummy dicts
        def insert_to_supabase(self, table_name, data): logging.error("DataProcessor dummy class used.")

# default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# define the DAG
with DAG(
    dag_id='mongodb_to_supabase_feature_etl_pipeline',
    default_args=default_args,
    description='DAG for extracting, transforming, and loading data from MongoDB to Supabase',
    # schedule interval to 1st of the month at noon
    schedule_interval='0 12 1 * *',
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    tags=['mongodb', 'supabase', 'feature_engineering']
) as dag:

    # set up functions for the DAG tasks
    def extract_and_transform_data(**context):
        """
        Extracts data (via DataProcessor init), runs sentiment analysis,
        engineers features, and pushes results to XComs.
        """
        logging.info("Starting data extraction and transformation...")
        try:
            # extracting data from MongoDB via DataProcessor init method
            processor = DataProcessor()
            logging.info("DataProcessor initialized successfully, data loaded from MongoDB.")
            
            # run sentiment analysis
            logging.info("Running sentiment analysis...")
            processor._get_sentiment_instagram()
            processor._get_sentiment_reddit()
            logging.info("Sentiment analysis completed.")

            # engineer features
            logging.info("Engineering features...")
            processor.engineer_google_trends()
            processor.engineer_currency()
            processor.engineer_instagram()
            processor.engineer_reddit()
            processor.engineer_tripadvisor()
            processor.engineer_labels()
            logging.info("Feature engineering completed.")

            # get all the dictioanries
            logging.info("Getting all dictionaries...")
            instagram_dict, reddit_dict, tripadvisor_dict, labels_dict, currency_dict, google_trends_dict = processor.get_all_dicts()
            logging.info("All dictionaries retrieved.")

            # push data to XComs, use json.dumps for serialization of dictionaries to JSON strings
            logging.info("Pushing data to XComs...")
            context['ti'].xcom_push(key='google_trends_data', value=json.dumps(google_trends_dict))
            context['ti'].xcom_push(key='currency_data', value=json.dumps(currency_dict))
            context['ti'].xcom_push(key='instagram_data', value=json.dumps(instagram_dict))
            context['ti'].xcom_push(key='reddit_data', value=json.dumps(reddit_dict))
            context['ti'].xcom_push(key='tripadvisor_data', value=json.dumps(tripadvisor_dict))
            context['ti'].xcom_push(key='labels_data', value=json.dumps(labels_dict))
            logging.info("Data pushed to XComs.")

            return f"""length of google_trends_dict: {len(google_trends_dict)}
                length of currency_dict: {len(currency_dict)}
                length of instagram_dict: {len(instagram_dict)}
                length of reddit_dict: {len(reddit_dict)}
                length of tripadvisor_dict: {len(tripadvisor_dict)}
                length of labels_dict: {len(labels_dict)}
                """

        except Exception as e:
            logging.error(f"An error occurred during data extraction and transformation: {e}")
            raise e # re-raise the exception to fail the task

    def load_to_supabase(**context):
        """
        Pulls processed data from XComs and loads it into respective Supabase tables.
        """
        logging.info("Starting data loading to Supabase...")
        try:
            # pull data from XComs
            logging.info("Pulling data from XComs...")
            google_trends_data = json.loads(context['ti'].xcom_pull(key='google_trends_data'))
            currency_data = json.loads(context['ti'].xcom_pull(key='currency_data'))
            instagram_data = json.loads(context['ti'].xcom_pull(key='instagram_data'))
            reddit_data = json.loads(context['ti'].xcom_pull(key='reddit_data'))
            tripadvisor_data = json.loads(context['ti'].xcom_pull(key='tripadvisor_data'))
            labels_data = json.loads(context['ti'].xcom_pull(key='labels_data'))
            logging.info("Data pulled from XComs successfully.")

            # initialise DataProcessor again to use insert_to_supabase method
            processor = DataProcessor()

            # define supabase table name mappings to the data pulled from XComs
            table_mapping = {
                'google_trends': google_trends_data,
                'currency': currency_data,
                'instagram_post': instagram_data,
                'reddit_post': reddit_data,
                'tripadvisor_post': tripadvisor_data,
                'visitor_count': labels_data
            }

            # insert data into respective tables
            for table_name, data in table_mapping.items():
                if data:
                    logging.info(f"Inserting data into {table_name} table...")
                    processor.insert_to_supabase(table_name, data)
                    logging.info(f"Data inserted into {table_name} table successfully.")
                else:
                    logging.warning(f"No data found for {table_name} table. Skipping insertion.")

            logging.info("Data loading to Supabase completed successfully.")
        
        except Exception as e:
            logging.error(f"An error occurred during data loading to Supabase: {e}")
            raise e # re-raise the exception to fail the task
    
    # define the tasks
    extract_and_transform_task = PythonOperator(
        task_id='extract_and_transform_data',
        python_callable=extract_and_transform_data,
        provide_context=True
    )

    load_to_supabase_task = PythonOperator(
        task_id='load_to_supabase',
        python_callable=load_to_supabase,
        provide_context=True,
    )

    # set task dependencies
    extract_and_transform_task >> load_to_supabase_task