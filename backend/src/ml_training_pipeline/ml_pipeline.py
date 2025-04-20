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

    def run(self):
        self.country_splitter.split_into_countries()
        self.country_model_trainer.run_training()

if __name__ == "__main__":
    ml_pipeline = MLPipeline()
    ml_pipeline.run()