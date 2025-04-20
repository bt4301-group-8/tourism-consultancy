# airflow imports
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
# from airflow.sensors.external_task import ExternalTaskSensor # Optional: Uncomment if waiting for data pipeline

# third party imports
import pendulum
from dotenv import load_dotenv
from pathlib import Path

# standard library imports
from datetime import timedelta, datetime
import os
import logging
import sys

# setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# environment set up
# Adjust the path depth as needed based on your project structure
BASE_DIR = Path(__file__).resolve().parent.parent.parent # go up from dags -> airflow -> tourism-consultancy
env_path = BASE_DIR / '.env'
load_dotenv(dotenv_path=env_path)

# add project root to sys.path for importing modules
sys.path.insert(0, str(BASE_DIR))

# import the MLPipeline class
try:
    from backend.src.ml_training_pipeline.ml_pipeline import MLPipeline # Ensure the logger used by CountryModelTrainer is configured if needed
except ImportError as e:
    logging.error(f"Failed to import MLPipeline: {e}")
    # Define a dummy MLPipeline class for DAG parsing if import fails
    class MLPipeline:
        def __init__(self): logging.error("MLPipeline dummy class used due to import error.")
        def run(self): logging.error("MLPipeline dummy run method called.")

# default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10) # Increased delay for potentially long ML task
}

# define the DAG
with DAG(
    dag_id='country_ml_model_training_pipeline',
    default_args=default_args,
    description='DAG for training country-specific visitor prediction models using MLflow',
    # TODO: schedule every year
    start_date=pendulum.datetime(2025, 4, 2, tz="UTC"), # Start date after data pipeline
    catchup=False,
    tags=['ml', 'training', 'xgboost', 'mlflow']
) as dag:

    def run_ml_pipeline_task(**context):
        """
        Initializes and runs the MLPipeline.
        """
        logging.info("Starting ML training pipeline...")
        try:
            ml_pipeline_instance = MLPipeline()
            logging.info("MLPipeline initialized.")
            ml_pipeline_instance.run()
            logging.info("ML training pipeline finished successfully.")
        except Exception as e:
            logging.error(f"ML training pipeline failed: {e}", exc_info=True)
            raise # Re-raise the exception to fail the Airflow task

    # Define the main training task
    train_models_task = PythonOperator(
        task_id='run_ml_pipeline',
        python_callable=run_ml_pipeline_task,
    )

    # Set task dependencies (Uncomment if using ExternalTaskSensor)
    # wait_for_data_pipeline >> train_models_task
