import pandas as pd
import os


class CountrySplitter:
    """
    This class is responsible for splitting country names into their respective parts.
    """

    def __init__(
        self,
        final_df_path: str = "backend/data/processed_data.csv",
    ):
        self.final_df_path = final_df_path
        self.df = pd.read_csv(final_df_path)
        self.df["month_year"] = pd.to_datetime(self.df["month_year"], errors="coerce")
        self.df.dropna(subset=["month_year"], inplace=True)
        # filter out data after cutoff date and prior cutoff date
        cutoff_date = pd.to_datetime("2025-02-01")
        prior_cutoff_date = pd.to_datetime("2022-01-01")
        self.df = self.df[self.df["month_year"] > prior_cutoff_date]
        self.df = self.df[self.df["month_year"] <= cutoff_date]

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


if __name__ == "__main__":
    splitter = CountrySplitter()
    splitter.split_into_countries()
