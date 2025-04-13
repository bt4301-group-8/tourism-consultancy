import pandas as pd
from backend.src.supabase.supabase import supabase

class supabase_retriever:
    def __init__(self):
        self.supabase = supabase

    def get_all(self, table_name):
        data = self.supabase.table(table_name).select("*").execute()
        return data.data

    def get_by_id(self, table_name, record_id):
        data = self.supabase.table(table_name).select("*").eq("id", record_id).execute()
        return data.data

    def combine_on_month_year_and_country(self):
        calendar = pd.DataFrame(self.get_all("calendar"))
        instagram = pd.DataFrame(self.get_all("instagram"))
        reddit = pd.DataFrame(self.get_all("reddit_submissions"))
        tripadvisor = pd.DataFrame(self.get_all("tripadvisor"))
        labels = pd.DataFrame(self.get_all("visitor_count"))
        currency = pd.DataFrame(self.get_all("currency"))
        trends = pd.DataFrame(self.get_all("google_trends"))

        dfs = [calendar, instagram, reddit, tripadvisor, labels, currency, trends]

        for i, df in enumerate(dfs):
            if 'month_year' not in df.columns:
                raise ValueError(f"Table {i} missing 'month_year'")

        from functools import reduce
        merged_df = reduce(lambda left, right: pd.merge(left, right, on=['month_year', 'country'], how='outer'), dfs)

        merged_df.to_csv("processed_data/final_df.csv", index=False)

        return merged_df