import argparse
import pandas as pd

from backend.src.supabase_retriever import supabase_retriever
from backend.src.model.country_splitter import CountrySplitter
from backend.src.model.country_model_trainer import CountryModelTrainer
from backend.src.services import logger


class MLPipeline:
    """
    This class orchestrates the entire machine learning pipeline.
    It retrieves data, splits it by country, trains models, and saves the results.
    """

    def __init__(self):
        self.supabase_retriever = supabase_retriever
        self.supabase_retriever.get_processed_data()
        self.country_splitter = CountrySplitter()
        self.country_splitter.split_into_countries()
        self.country_model_trainer = CountryModelTrainer()
        self.logger = logger

    def detect_input_drift(
        self,
        csv_path: str = None,
        run_id: str = None,
    ):
        if not csv_path:
            self.logger.info(
                "[ML Pipeline] no CSV path provided. Skipping input drift detection."
            )
            return
        if not run_id:
            self.logger.info(
                "ML Pipeline] no run ID provided. Skipping input drift detection."
            )
            return
        # load the new data
        data = pd.read_csv(csv_path)
        new_data = data.tail(3)
        # print the result of the input drift detection
        self.country_model_trainer.monitor_input_drift(
            run_id=run_id,
            new_data=new_data,
        )

    def run(
        self,
        csv_path: str = None,
    ):
        self.country_model_trainer.run_training(csv_path=csv_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ML pipeline for tourism data")
    parser.add_argument(
        "--retrain",
        type=bool,
        default=False,
        help="Whether to retrain the model. Default is False.",
    )
    parser.add_argument(
        "--csv_path",
        type=str,
        default=None,
        help="Optional path to a CSV file. If not provided, ALL models will be trained for ALL countries.",
    )
    parser.add_argument(
        "--run_id",
        type=str,
        default=None,
        help="Optional run ID for monitoring input drift. If not provided, input drift detection will be skipped.",
    )

    args = parser.parse_args()
    ml_pipeline = MLPipeline()
    ml_pipeline.detect_input_drift(csv_path=args.csv_path, run_id=args.run_id)
    if args.retrain:
        ml_pipeline.run(csv_path=args.csv_path)
    else:
        ml_pipeline.logger.info(
            "Skipping retraining. Use --retrain True to retrain the model."
        )
        ml_pipeline.logger.info("ML pipeline completed successfully.")
