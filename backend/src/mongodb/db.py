from pymongo import MongoClient
import os
from dotenv import load_dotenv
import json
import urllib.parse
import pandas as pd

class MongoDB:
    def __init__(self):
        load_dotenv()
        username = os.getenv("MONGODB_USERNAME")
        password = urllib.parse.quote_plus(os.getenv("MONGODB_PASSWORD"))
        self.uri = f"mongodb+srv://{username}:{password}@cluster0.hw5xcgp.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
        self.client = MongoClient(self.uri)

    def insert_json_into_db(self, filepath, db_name, collection_name):
        db = self.client[db_name]
        collection = db[collection_name]
        with open(filepath, 'r') as file:
            data_list = json.load(file)
        if isinstance(data_list, list):
            result = collection.insert_many(data_list)
            print("Inserted document IDs:", result.inserted_ids)
        else:
            print("Error: JSON data is not a list.")

    def find_all(self, db_name, collection_name):
        db = self.client[db_name]
        collection = db[collection_name]
        documents = collection.find()
        df = pd.DataFrame(documents, index=None)
        return df
    
mongodb = MongoDB()