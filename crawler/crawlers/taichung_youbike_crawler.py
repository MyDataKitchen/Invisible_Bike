from crawler.models.s3 import put_data_to_s3
from datetime import datetime as dt
from datetime import timezone, timedelta
from dotenv import load_dotenv
from crawler.models.mysql import insert_crawler_log
from crawler.models.mongodb import insert_youbike_data_to_mongo
import requests
import json
import os
import time
import sys

load_dotenv()

YOUBIKE_URL = os.getenv('TAICHUNG_YOUBIKE_URL')
S3_BUCKET = os.getenv('BUCKET')
SQL_TABLE = os.getenv('TAICHUNG_YOUBIKE_TABLE')
S3_DIRECTORY_PATH = os.getenv('TAICHUNG_YOUBIKE_DIRECTORY_PATH')
S3_TEMP_PATH = os.getenv('TAICHUNG_YOUBIKE_TEMP_DIRECTORY_PATH')
FILE_NAME = os.getenv('TAICHUNG_YOUBIKE_FILE_NAME')


def request_data(url):
    respone = requests.get(url)
    data = respone.json()
    updated_time = datetime().strftime("%Y-%m-%d %H:%M:%S")
    size = sys.getsizeof(json.dumps(data))
    size = round( size / 1024, 0)
    return data, updated_time, size


def datetime():
    tz = timezone(timedelta(hours=+8))
    now = dt.now(tz)

    return now


def insert_data_to_s3(bucket, filename, data):
    status = put_data_to_s3(bucket, filename, json.dumps(data))
    return status


def insert_data_to_mongo(city, data):
    return insert_youbike_data_to_mongo(city=city, data=data)


if __name__ == '__main__':
    date_time = datetime().strftime("%Y%m%d_%H:%M")
    start = time.time()
    data, updated_time, size = request_data(YOUBIKE_URL)
    end = time.time()
    response_time = end - start

    start = time.time()

    try:
        f = open('../temp/temp_data.json')
        temp = json.load(f)
        if data == temp:
            filename = f"{date_time}_{FILE_NAME}.json"
            end = time.time()
            execution_time = end - start
            insert_crawler_log(SQL_TABLE, (filename, updated_time, len(data['retVal']), size, response_time, execution_time, 0, None))

        else:
            filename = f"{date_time}_{FILE_NAME}.json"
            mongo_data = {"created_at": updated_time, "item": data, "filename": filename}
            status = insert_data_to_mongo("taichung", mongo_data)
            if status == True:
                aws_response = insert_data_to_s3(S3_BUCKET, S3_DIRECTORY_PATH + filename, data)
            else:
                aws_response = insert_data_to_s3(S3_BUCKET, S3_TEMP_PATH + filename, data)
            end = time.time()
            execution_time = end - start
            insert_crawler_log(SQL_TABLE, (filename, updated_time, len(data['retVal']), size, response_time, execution_time, 1, json.dumps(aws_response)))

            try:
                with open("../temp/temp_data.json", "w") as outfile:
                    json_object = json.dumps(data)
                    outfile.write(json_object)
            except:
                path = "../temp"
                os.mkdir(path)
                with open("../temp/temp_data.json", "w") as outfile:
                    json_object = json.dumps(data)
                    outfile.write(json_object)


    except FileNotFoundError:

        filename = f"{date_time}_{FILE_NAME}.json"
        mongo_data = {"created_at": updated_time, "item": data, "filename": filename}
        status = insert_data_to_mongo("taichung", mongo_data)
        if status == True:
            aws_response = insert_data_to_s3(S3_BUCKET, S3_DIRECTORY_PATH + filename, data)
        else:
            aws_response = insert_data_to_s3(S3_BUCKET, S3_TEMP_PATH + filename, data)
        end = time.time()
        execution_time = end - start
        insert_crawler_log(SQL_TABLE, (filename, updated_time, len(data['retVal']), size, response_time, execution_time, 1, json.dumps(aws_response)))

        try:
            with open("../temp/temp_data.json", "w") as outfile:
                json_object = json.dumps(data)
                outfile.write(json_object)
        except:
            path = "../temp"
            os.mkdir(path)
            with open("../temp/temp_data.json", "w") as outfile:
                json_object = json.dumps(data)
                outfile.write(json_object)