import os
import json
import pandas as pd
import requests
from dotenv import load_dotenv

class VisitorScraper:
    def __init__(self):
        load_dotenv()
        self.api_url = os.environ.get("VISITOR_URL")
        if not self.api_url:
            raise ValueError("Missing VISITOR_URL in environment variables")

    def _build_payload(self, country_name: str) -> dict:
        return {
            "query": {
                "country": country_name
            }
        }

    def get_visitors(self, country_name: str):
        payload = self._build_payload(country_name)

        try:
            response = requests.post(self.api_url, json=payload)
            response.raise_for_status()
        except requests.RequestException as e:
            raise RuntimeError(f"Request failed: {e}")

        try:
            data = response.json()
        except json.JSONDecodeError:
            raise ValueError("Failed to parse JSON from response")

        filtered = [
            entry for entry in data.get("data", [])
            if entry.get("country", "").lower() == country_name.lower()
        ]

        if not filtered:
            raise ValueError(f"No visitor data found for country: {country_name}")

        df = pd.DataFrame(filtered)
        df["month_year"] = pd.to_datetime(df["month_year"])
        df = df.sort_values("month_year").reset_index(drop=True)

        return df.to_json(orient="records", date_format="iso")