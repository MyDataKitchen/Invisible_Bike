from dotenv import load_dotenv
from pymongo import MongoClient
from datetime import datetime
import pymongo
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

def insert_s3_temp_to_mongo(source, data):

    if source == "taipei":
        collection = db["taipei"]
        collection.insert_one(data)
        return True

    elif source == "taichung":
        collection = db["taichung"]
        collection.insert_one(data)
        return True

    elif source == "weather":
        collection = db["weather"]
        collection.insert_one(data)
        return True

    elif source == "precipitation":
        collection = db["precipitation"]
        collection.insert_one(data)
        return True

    return False


def insert_temp_data(source, data):
    collection = db["temp"]
    updated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    try:
        collection.insert_one({"_id": f"{ source }_temp", "created_at": updated_time, "item": data })
    except pymongo.errors.DuplicateKeyError:
        query = {"_id": f"{ source }_temp"}
        updated_data = {"$set": {"created_at": updated_time, "item": data}}
        collection.update_one(query, updated_data)


def get_temp_data(source):
    collection = db["temp"]
    result = collection.find_one({"_id": f"{ source }_temp"})
    return result


def delete_data(source, filename):
    try:
        collection = db[source]
        query = {"filename": filename}
        collection.delete_one(query)
        return True

    except Exception as e:
        print(e)
        return False

def insert_log(source, data):
    collection = db["log"]
    updated_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    collection.insert_one({"filename": f"{ source }_log", "created_at": updated_time, "log": data })







