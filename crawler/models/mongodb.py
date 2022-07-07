from dotenv import load_dotenv
from pymongo import MongoClient
import os

load_dotenv()

MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')

client = MongoClient(host=MONGO_HOST,
                     username=MONGO_USER,
                     password=MONGO_PASSWORD
                    )

db = client[MONGO_DATABASE]

def insert_youbike_data_to_mongo(city, data):
    if city == "taipei":
        try:
            collection = db["taipei"]
            collection.insert_one(data)
            return True
        except Exception as e:
            return e

    elif city == "taichung":
        try:
            collection = db["taichung"]
            collection.insert_one(data)
            return True
        except Exception as e:
            return e

    else:
        return False

def insert_weather_data_to_mongo(source, data):
    if source == "precipitation":
        try:
            collection = db["precipitation"]
            collection.insert_one(data)
            return True

        except Exception as e:
            return e

    elif source == "weather":
        try:
            collection = db["weather"]
            collection.insert_one(data)
            return True

        except Exception as e:
            return e

    else:
        return False