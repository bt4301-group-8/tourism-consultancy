from pymongo import MongoClient
import os
from dotenv import load_dotenv
import json

uri = os.getenv('MONGODB_URI')
client = MongoClient(uri)

def insert_json_into_db(filepath, db_name, collection_name):
    db = client[db_name]
    collection = db[collection_name]
    with open(filepath, 'r') as file:
        data_list = json.load(file)
    if isinstance(data_list, list):
        result = collection.insert_many(data_list)
        print("Inserted document IDs:", result.inserted_ids)
    else:
        print("Error: JSON data is not a list.")