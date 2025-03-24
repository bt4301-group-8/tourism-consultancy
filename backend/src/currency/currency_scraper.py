import pandas as pd
import datetime
import time
from tqdm import tqdm
import os

class CurrencyScraper:
    def __init__(self, start_date, end_date, delay=2):
        self.currencies = ['BND', 'MMK', 'KHR', 'IDR', 'LAK', 'MYR', 'PHP', 'SGD', 'THB', 'VND']
        self.start_date = start_date
        self.end_date = end_date
        self.delay = delay

    def generate_weekly_dates(self):
        weekly_dates = pd.date_range(
            start=self.start_date, end=self.end_date, freq='W-MON'
        ).strftime('%Y-%m-%d').tolist()
        return weekly_dates

    def fetch_weekly_data(self, date_str):
        url = f'https://www.xe.com/currencytables/?from=USD&date={date_str}'
        try:
            tables = pd.read_html(url)
            if tables and len(tables) > 0:
                weekly_df = tables[0]
                weekly_df['Date'] = date_str
                return weekly_df
        except Exception as e:
            print(f"Error fetching data for {date_str}: {e}")
        return None

    def scrape(self):
        weekly_dates = self.generate_weekly_dates()
        df_list = []

        for date_str in tqdm(weekly_dates, desc="Fetching weekly currency data"):
            weekly_df = self.fetch_weekly_data(date_str)
            if weekly_df is not None:
                df_list.append(weekly_df)
            time.sleep(self.delay)

        if not df_list:
            print("No data collected")
            return None

        combined_df = pd.concat(df_list, ignore_index=True)
        currency_col = 'Currency Code' if 'Currency Code' in combined_df.columns else 'Currency'
        value_col = 'Units per USD' if 'Units per USD' in combined_df.columns else 'Rate'

        filtered_df = combined_df[combined_df[currency_col].isin(self.currencies)].copy()
        filtered_df[value_col] = pd.to_numeric(filtered_df[value_col], errors='coerce')
        filtered_df['Date'] = pd.to_datetime(filtered_df['Date'])
        filtered_df['YearMonth'] = filtered_df['Date'].dt.to_period('M')

        monthly_avg = filtered_df.groupby(['YearMonth', currency_col])[value_col].mean().reset_index()
        monthly_avg.columns = ['YearMonth', 'Currency', 'AverageRate']

        return monthly_avg

    def save_to_csv(self, df, filepath):
        root_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        path = os.path.join(root_path, 'extracted_data', filepath)
        df.to_csv(path, index=False)
        print(f"Monthly averages saved to {path}")

if __name__ == "__main__":
    scraper = CurrencyScraper(
        start_date=datetime.date(2022, 1, 1),
        end_date=datetime.date(2025, 3, 23)
    )

    monthly_avg_df = scraper.scrape()
    if monthly_avg_df is not None:
        scraper.save_to_csv(monthly_avg_df, 'southeast_asia_currency_monthly_avg.csv')