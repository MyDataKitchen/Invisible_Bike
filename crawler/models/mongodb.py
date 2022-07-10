from dotenv import load_dotenv
from pymongo import MongoClient
import os
import signal
import time

load_dotenv()

MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')
TIME_LIMIT = os.getenv('MONGO_QUERY_TIME_LIMIT')

client = MongoClient(host=MONGO_HOST,
                     username=MONGO_USER,
                     password=MONGO_PASSWORD
                    )

db = client[MONGO_DATABASE]

def insert_youbike_data_to_mongo(city, data):
    def handle_timeout(signum, frame):
        raise TimeoutError

    signal.signal(signal.SIGALRM, handle_timeout)
    signal.alarm(int(TIME_LIMIT))

    if city == "taipei":
        print("taipei")
        try:
            try:
                collection = db["taipei"]
                collection.insert_one(data)
                signal.alarm(0)
                return True

            except TimeoutError:
                print("It took too long to query a MongoBD database")
                signal.alarm(0)
                return False

        except Exception as e:
            print(e)
            return False

    elif city == "taichung":
        print("taichung")
        try:
            try:
                collection = db["taichung"]
                collection.insert_one(data)
                signal.alarm(0)
                return True

            except TimeoutError:
                print("It took too long to query a MongoBD database")
                signal.alarm(0)
                return False

        except Exception as e:
            print(e)
            return False

    else:
        return False

def insert_weather_data_to_mongo(source, data):
    def handle_timeout(signum, frame):
        raise TimeoutError

    signal.signal(signal.SIGALRM, handle_timeout)
    signal.alarm(int(TIME_LIMIT))

    if source == "precipitation":
        print("precipitation")
        try:
            try:
                collection = db["precipitation"]
                collection.insert_one(data)
                signal.alarm(0)
                return True

            except TimeoutError:
                print("It took too long to query a MongoBD database")
                signal.alarm(0)
                return False

        except Exception as e:
            print(e)
            return False

    elif source == "weather":
        print("weather")
        try:
            try:
                collection = db["weather"]
                collection.insert_one(data)
                signal.alarm(0)
                return True

            except TimeoutError:
                print("It took too long to query a MongoBD database")
                signal.alarm(0)
                return False

        except Exception as e:
            print(e)
            return False

    else:
        return False

def get_temp_object():
    try:
        collection = db["temp"]
        temp_object = collection.find_one()

        if temp_object == None:
            return None
        else:
            return temp_object

    except Exception as e:
        print(e)
        return False

def insert_temp_object(data):

    def handle_timeout(signum, frame):
        raise TimeoutError

    signal.signal(signal.SIGALRM, handle_timeout)
    signal.alarm(int(TIME_LIMIT))

    try:
        try:
            collection = db["temp"]
            collection.insert_one(data)
            signal.alarm(0)
            return False

        except TimeoutError:
            print("It took too long to query a MongoBD database")
            signal.alarm(0)
            return False

    except Exception as e:
        print(e)
        return False




