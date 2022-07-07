from crawler.model.s3 import put_data_to_s3
from datetime import datetime as dt
from dotenv import load_dotenv
from crawler.model.mysql import get_latest_log, insert_crawler_log
from crawler.model.mongodb import insert_weather_data_to_mongo
from datetime import timezone, timedelta
import requests
import json
import os
import time
import sys

load_dotenv()

WHEATHER_URL = os.getenv('WHEATHER_URL')
S3_BUCKET = os.getenv('BUCKET')
SQL_TABLE = os.getenv('WHEATHER_TABLE')
S3_DIRECTORY_PATH = os.getenv('WHEATHER_DIRECTORY_PATH')
FILE_NAME = os.getenv('WHEATHER_FILE_NAME')

def request_data(url):
    respone = requests.get(url)
    data = respone.json()
    updated_time = data['records']['location'][0]['time']['obsTime']
    size = sys.getsizeof(json.dumps(data))
    size = round(size / 1024, 0)
    return data, updated_time, size


def datetime():
    tz = timezone(timedelta(hours=+8))
    now = dt.now(tz)
    dt_string = now.strftime("%Y%m%d_%H:%M")
    return dt_string


def insert_data_to_s3(bucket, filename, data):
    status = put_data_to_s3(bucket, filename, json.dumps(data))
    return status


def insert_data_to_mongo(source, data):
    return insert_weather_data_to_mongo(source=source, data=data)


if __name__ == '__main__':
    date_time = datetime()
    start = time.time()
    data, updated_time, size = request_data(WHEATHER_URL)
    end = time.time()
    response_time = end - start

    start = time.time()
    latest_log = get_latest_log(SQL_TABLE) # get latest log from mysql

    datetime_request = dt.strptime(updated_time, '%Y-%m-%d %H:%M:%S')
    datetime_log = latest_log[0]['updateTime']

    if datetime_request > datetime_log:
        filename = f"{ date_time }_{ FILE_NAME }.json"
        mongo_data = {"created_at": updated_time, "item": data, "filename": filename}
        insert_data_to_mongo("weather", mongo_data)
        aws_response = insert_data_to_s3(S3_BUCKET, S3_DIRECTORY_PATH + filename, data)
        end = time.time()
        execution_time = end - start
        insert_crawler_log(SQL_TABLE, (filename, updated_time, len(data['records']['location']), size, response_time, execution_time, 1, json.dumps(aws_response)))

    else:
        filename = f"{ date_time }_{ FILE_NAME }.json"
        end = time.time()
        execution_time = end - start
        insert_crawler_log(SQL_TABLE, (filename, updated_time, len(data['records']['location']), size, response_time, execution_time, 0, None))