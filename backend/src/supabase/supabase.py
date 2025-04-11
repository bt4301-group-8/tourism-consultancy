from dotenv import load_dotenv
import os
from supabase import create_client, Client
import pandas as pd

class SupabaseClient:
    def __init__(self):
        load_dotenv()
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_KEY")
        self.supabase: Client = create_client(self.url, self.key)

    def get_data(self, table_name: str) -> pd.DataFrame:
        data = self.supabase.table(table_name).select("*").execute()
        return pd.DataFrame(data.data)
    
    def insert_calendar_data(self, date_from: str, date_to: str):
        date_range = pd.date_range(start=date_from, end=date_to, freq='MS')
        month_year = date_range.strftime('%Y-%m')
        data = {
            'month_year': month_year,
            'month': date_range.month,
            'year': date_range.year,
            'is_monsoon_season': [1 if month in [11, 12, 1, 2, 3, 4] else 0 for month in date_range.month],
            'is_school_holiday': [1 if month in [6, 7, 8, 12] else 0 for month in date_range.month],
        }
        df = pd.DataFrame(data)
        existing_month_years = self.supabase.table("calendar").select("month_year").in_("month_year", month_year).execute()
        existing_month_years = [row['month_year'] for row in existing_month_years.data]
        
        new_data = df[~df['month_year'].isin(existing_month_years)]
        
        if new_data.empty:
            print("No new data to insert. All month_years already exist.")
            return
        
        response = self.supabase.table("calendar").insert(new_data.to_dict(orient="records")).execute()
        return response
    
    def insert_data(self, table_name: str, data: dict):
        response = self.supabase.table(table_name).insert(data).execute()
        return response

supabase = SupabaseClient()