import pandas as pd
import os
from backend.src.supabase_retriever import supabase_retriever


class CountrySplitter:
    """
    This class is responsible for splitting country names into their respective parts.
    """

    def __init__(
        self,

        final_df_path: str = "backend/data/processed_data.csv",
        output_dir: str = "backend/data/countries",
    ):
        self.final_df_path = final_df_path
        self.output_dir = output_dir
        self.df = pd.read_csv(final_df_path)

    def split_into_countries(
        self,
        directory: str = "backend/data/countries",
        trailing_name: str = "_final_df.csv",
    ) -> None:
        countries = self.df["country"].unique()
        os.makedirs(directory, exist_ok=True)
        # split by country and save individual files
        for country in countries:
            # filter data for this country
            country_df = self.df[self.df["country"] == country]
            # create filename with country name
            file_path = f"{directory}/{country.lower()}{trailing_name}"
            country_df.to_csv(file_path, index=False)


# if __name__ == "__main__":
#     splitter = CountrySplitter()
#     splitter.split_into_countries()