from backend.src.google_trends.client.client import RestClient
import pandas as pd
import time
from datetime import datetime
import os
from dotenv import load_dotenv

class GoogleTrendsScraper:
    def __init__(self):
        load_dotenv()
        self.login = os.getenv('SEO_LOGIN')
        self.password = os.getenv('SEO_PASSWORD')
        self.client = RestClient(self.login, self.password)
        self.tasks = []
        self.results = []

    def create_tasks(self, date_from, date_to, category_code=67):
        post_data_one = dict()
        post_data_one[len(post_data_one)] = dict(
            date_from=date_from,
            date_to=date_to,
            keywords=[
                "Singapore",
                "Thailand",
                "Malaysia",
                "Indonesia",
                "Vietnam",
            ],
            category_code=category_code,
        )

        post_data_two = dict()
        post_data_two[len(post_data_two)] = dict(
            date_from=date_from,
            date_to=date_to,
            keywords=[
                "Singapore",
                "Philippines",
                "Cambodia",
                "Myanmar",
            ],
            category_code=category_code,
        )

        post_data_three = dict()
        post_data_three[len(post_data_three)] = dict(
            date_from=date_from,
            date_to=date_to,
            keywords=[
                "Singapore",
                "Laos",
                "Brunei"
            ],
            category_code=category_code,
        )

        posts = [post_data_one, post_data_two, post_data_three]
        for post in posts:
            response = self.client.post("/v3/keywords_data/google_trends/explore/task_post", post)
            if response["status_code"] == 20000:
                print(response)
            else:
                print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))

    def wait_for_tasks(self, wait_seconds=60):
        print(f"Waiting {wait_seconds} seconds for tasks to complete...")
        time.sleep(wait_seconds)

    def fetch_results(self):
        response = self.client.get("/v3/keywords_data/google_trends/explore/tasks_ready")
        print(len(response['tasks']))
        if response['status_code'] == 20000:
            for task in response['tasks']:
                if task['result']:
                    for result_info in task['result']:
                        endpoint = result_info.get('endpoint')
                        if endpoint:
                            result = self.client.get(endpoint)
                            self.results.append(result)
            print(self.results)
            print("Results fetched successfully.")
        else:
            print(f"Error fetching results: {response['status_message']}")

    def convert_to_csv(self):
        sg_values = {}
        rows = []

        def timestamp_to_month_year(ts):
            return datetime.utcfromtimestamp(ts).strftime('%Y-%m')

        for entry in self.results:
            tasks = entry.get('tasks', [])
            for task in tasks:
                for result in task.get('result', []):
                    for item in result.get('items', []):
                        keywords = item.get('keywords', [])
                        singapore_index = keywords.index('Singapore')
                        for data_point in item.get('data', []):
                            month_year = timestamp_to_month_year(data_point['timestamp'])
                            singapore_value = data_point['values'][singapore_index]
                            sg_values.setdefault(month_year, []).append(singapore_value)

        avg_sg_values = {month: sum(vals) / len(vals) for month, vals in sg_values.items()}

        for entry in self.results:
            tasks = entry.get('tasks', [])
            for task in tasks:
                for result in task.get('result', []):
                    for item in result.get('items', []):
                        keywords = item.get('keywords', [])
                        for data_point in item.get('data', []):
                            month_year = timestamp_to_month_year(data_point['timestamp'])
                            singapore_avg_value = avg_sg_values[month_year]
                            for idx, country in enumerate(keywords):
                                country_value = data_point['values'][idx]
                                rows.append({
                                    'month_year': month_year,
                                    'country': country,
                                    'value': (country_value / singapore_avg_value) * singapore_avg_value
                                })

        df = pd.DataFrame(rows)
        if df.empty:
            print("No data was retrieved. Please check if the tasks returned any results.")
            return pd.DataFrame()
        df = df.drop_duplicates(subset=['month_year', 'country'], keep='first')
        df['month_year'] = pd.to_datetime(df['month_year'])
        df.sort_values(by=['month_year', 'country'], inplace=True)
        df.reset_index(drop=True, inplace=True)
        df['month_year'] = df['month_year'].dt.strftime('%Y-%m')
        
        os.makedirs('extracted_data', exist_ok=True)
        # df.to_csv('extracted_data/google_trends_data.csv', index=False)
        # df.to_json('extracted_data/google_trends_data.json', orient='records', indent=4)
        return df
    
    def get_data(self):
        self.create_tasks("2022-01-01", "2025-04-08")
        self.wait_for_tasks()
        self.fetch_results()
        df = self.convert_to_csv()
        return df

if __name__ == "__main__":
    scraper = GoogleTrendsScraper()
    scraper.get_data()