import pandas as pd
from backend.src.supabase.supabase import supabase
import os


class SupabaseRetriever:
    def __init__(self):
        self.supabase = supabase

    def get_processed_data(self):
        calendar = pd.DataFrame(self.supabase.get_data("calendar"))
        instagram = pd.DataFrame(self.supabase.get_data("instagram_post"))
        reddit = pd.DataFrame(self.supabase.get_data("reddit_post"))
        tripadvisor = pd.DataFrame(self.supabase.get_data("tripadvisor_post"))
        labels = pd.DataFrame(self.supabase.get_data("visitor_count"))
        currency = pd.DataFrame(self.supabase.get_data("currency"))
        trends = pd.DataFrame(self.supabase.get_data("google_trends"))

        for df in [instagram, reddit, tripadvisor, labels, currency, trends]:
            if "country" in df.columns:
                df["country"] = df["country"].str.lower()

        base = (
            labels[["country", "month_year", "num_visitors", "log_visitors"]]
            .drop_duplicates()
            .copy()
        )

        datasets = [
            ("calendar", calendar),
            ("instagram_post", instagram),
            ("reddit_post", reddit),
            ("tripadvisor_post", tripadvisor),
            ("currency", currency),
            ("google_trends", trends),
        ]

        for name, df in datasets:
            if "month_year" not in df.columns:
                raise ValueError(f"[ERROR] '{name}' is missing 'month_year'.")

            post_id_cols = [col for col in df.columns if "id" in col.lower()]
            if post_id_cols:
                df = df.drop(columns=post_id_cols)

            if name == "calendar":
                base = pd.merge(base, df, on="month_year", how="left")
            else:
                if "country" not in df.columns:
                    raise ValueError(f"[ERROR] '{name}' is missing 'country'.")
                base = pd.merge(base, df, on=["country", "month_year"], how="left")

        base["month_year"] = pd.to_datetime(base["month_year"])
        base = base.sort_values(["country", "month_year"]).reset_index(drop=True)
        # apply interpolation + forward/backfill within each country to each numeric column
        base = (
            base.groupby("country")
            .apply(
                lambda g: g.select_dtypes(
                    include="number"
                )  # Process only numeric columns
                .interpolate(method="linear", limit_direction="both")
                .combine_first(g)  # Merge back with original data
                .ffill()
                .bfill()
            )
            .reset_index(drop=True)
        )
        save_dir = "backend/data"
        os.makedirs(save_dir, exist_ok=True)
        base.to_csv(os.path.join(save_dir, "processed_data.csv"), index=False)
        return base


supabase_retriever = SupabaseRetriever()
