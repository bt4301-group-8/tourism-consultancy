import argparse

from backend.src.supabase_retriever import supabase_retriever
from backend.src.model.country_splitter import CountrySplitter
from backend.src.model.country_model_trainer import CountryModelTrainer


class MLPipeline:
    """
    This class orchestrates the entire machine learning pipeline.
    It retrieves data, splits it by country, trains models, and saves the results.
    """

    def __init__(self):
        self.supabase_retriever = supabase_retriever
        self.supabase_retriever.get_processed_data()
        self.country_splitter = CountrySplitter()
        self.country_model_trainer = CountryModelTrainer()

    def run(
        self,
        csv_path: str = None,
    ):
        self.country_splitter.split_into_countries()
        self.country_model_trainer.run_training(csv_path=csv_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ML pipeline for tourism data")
    parser.add_argument(
        "--csv_path",
        type=str,
        default=None,
        help="Optional path to a CSV file. If not provided, a model will be trained for all countries.",
    )

    args = parser.parse_args()
    ml_pipeline = MLPipeline()
    ml_pipeline.run(csv_path=args.csv_path)
