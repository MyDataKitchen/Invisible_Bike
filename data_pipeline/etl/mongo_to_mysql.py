from data_pipeline.models.mysql import insert_data_to_record, get_data_processed_record
from data_pipeline.models.mongodb import get_temp_data, insert_temp_data, delete_data, insert_log
from sqlalchemy import create_engine
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime as dt
import pandas as pd
import os
import threading
import subprocess

load_dotenv()

MONGO_HOST = os.getenv('MONGO_HOST')
MONGO_USER = os.getenv('MONGO_USER')
MONGO_PASSWORD = os.getenv('MONGO_PASSWORD')
MONGO_DATABASE = os.getenv('MONGO_DATABASE')

client = MongoClient(MONGO_HOST,
                     username=MONGO_USER,
                     password=MONGO_PASSWORD
                    )


SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')

engine = create_engine(f"mysql+pymysql://{ SQL_USER }:{ SQL_PASSWORD }@{ SQL_HOST }/{ SQL_DATABASE }",
                       pool_size=10, max_overflow=5, pool_pre_ping=True)


def colors(proportion):
    colors = ['red', 'yellow', 'green']
    if proportion >= 0 and proportion < 30:
        return colors[0]
    elif proportion >= 30 and proportion < 60:
        return colors[1]
    elif proportion >= 60 and proportion <= 100:
        return colors[2]


def station_event(datetime, temp, data):
    datetime = datetime
    stationId = int(data['sno'])
    total = int(data['tot'])
    availableSpace = int(data['sbi'])
    emptySpace = int(data['bemp'])
    status = int(data['act'])
    try:
        if temp['availableSpace'] > availableSpace:
            outPerMinute = abs(temp['availableSpace'] - availableSpace)
            temp['availableSpace'] = availableSpace
            inPerMinute = 0
        elif temp['availableSpace'] < availableSpace:
            inPerMinute = abs(availableSpace - temp['availableSpace'])
            temp['availableSpace'] = availableSpace
            outPerMinute = 0
        else:
            inPerMinute = 0
            outPerMinute = 0
    except:
        temp = {}
        temp['availableSpace'] = availableSpace
        inPerMinute = 0
        outPerMinute = 0

    proportion = int((int(availableSpace) / total) * 100)
    color = colors(proportion)

    try:
        if color == 'red':
            if temp['color'] == 'red':
                temp['shortageDuration'] += 1
                shortageDuration = temp['shortageDuration']
            else:
                temp['color'] = 'red'
                temp['shortageDuration'] = 1
                shortageDuration = temp['shortageDuration']

        else:
            temp['color'] = color
            temp['shortageDuration'] = 0
            shortageDuration = temp['shortageDuration']
    except:
        if color == 'red':
            temp['color'] = color
            temp['shortageDuration'] = 1
            shortageDuration = temp['shortageDuration']
        else:
            temp['color'] = color
            temp['shortageDuration'] = 0
            shortageDuration = temp['shortageDuration']

    return {'datetime': datetime, 'stationId': stationId, 'total': total, 'availableSpace': availableSpace,
            'emptySpace': emptySpace, 'status': status, 'outPerMinute': outPerMinute, 'inPerMinute': inPerMinute,
            'color': color, 'shortageDuration': shortageDuration}


def taipei_youbike_etl():

    import time
    db = client[MONGO_DATABASE]
    col = db['taipei']

    while True:
        temp_time = get_data_processed_record("taipei")
        if temp_time == []:
            temp_time = "2022-07-10 00:00:00"
        else:
            temp_time = temp_time[0][0].strftime('%Y-%m-%d %H:%M:%S')

        events = col.find({'created_at': {'$gt': temp_time}}, allow_disk_use=True).sort("created_at", 1)

        for event in events:
            while True:
                try:
                    engine.connect()
                except:
                    log = {"status": "RDS connection failed"}
                    insert_log("taipei_youbike_etl", log)
                    time.sleep(60) # sleep 1 minute
                    continue
                break

            filename = event['filename']
            print(f"thread_1 - { filename }")
            start = time.time()
            if get_temp_data("taipei") == None:
                temp_data = {}
            else:
                temp_data = get_temp_data("taipei")['item']

            data = []
            origin_data = len(event['item'])

            data_error = 0

            for station in event['item']:
                station_id = station['sno']
                datetime = station['srcUpdateTime']
                try:
                    temp = temp_data[station_id]
                except:
                    temp_data[station_id] = {}
                    temp = {}

                if len(station) != 18:
                    data_error = 1

                result = station_event(datetime, temp, station)
                data.append(result)
                temp_data[station_id] = result

            df = pd.DataFrame(data)
            final_data = len(df)
            table_name = "EventTaipei"
            df.to_sql(table_name, engine, if_exists='append', chunksize=2000, index=False)

            insert_temp_data("taipei", temp_data)
            updated_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            delete_data("taipei", filename)
            end = time.time()
            execution_time = end - start
            insert_data_to_record((event['filename'], updated_time, event['created_at'], "taipei", round(execution_time, 6), origin_data, final_data, data_error))

        try:
            events.next()
        except StopIteration:
            events.rewind()
            time.sleep(30)


def taichung_youbike_etl():

    import time
    db = client[MONGO_DATABASE]
    col = db['taichung']

    while True:

        temp_time = get_data_processed_record("taichung")
        if temp_time == []:
            temp_time = "2022-07-10 00:00:00"
        else:
            temp_time = temp_time[0][0].strftime('%Y-%m-%d %H:%M:%S')

        events = col.find({'created_at': {'$gt': temp_time}}, allow_disk_use=True).sort("created_at", 1)

        for event in events:
            while True:
                try:
                    engine.connect()
                except:
                    log = {"status": "RDS connection failed"}
                    insert_log("taichung_youbike_etl", log)
                    time.sleep(60) # sleep 1 minute
                    continue
                break

            filename = event['filename']
            print(f"thread_2 - {filename}")
            start = time.time()
            if get_temp_data("taichung") == None:
                temp_data = {}
            else:
                temp_data = get_temp_data("taichung")['item']

            data = []
            origin_data = len(event['item']['retVal'])

            data_error = 0

            for station in event['item']['retVal']:
                station_id = station['sno']
                datetime = event['item']['updated_at']
                try:
                    temp = temp_data[station_id]
                except:
                    temp_data[station_id] = {}
                    temp = {}

                if len(station) != 16:
                    data_error = 1

                result = station_event(datetime, temp, station)
                data.append(result)
                temp_data[station_id] = result

            df = pd.DataFrame(data)
            final_data = len(df)
            table_name = "EventTaichung"
            df.to_sql(table_name, engine, if_exists='append', chunksize=2000, index=False)

            insert_temp_data("taichung", temp_data)
            updated_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            delete_data("taichung", filename)
            end = time.time()
            execution_time = end - start
            insert_data_to_record((event['filename'], updated_time, event['created_at'], "taichung", round(execution_time, 6), origin_data, final_data, data_error))

        try:
            events.next()
        except StopIteration:
            events.rewind()
            time.sleep(30)


if __name__ == '__main__':
    subprocess.run(["python3", "s3_temp_to_mongo.py"])

    thread_1 = threading.Thread(target=taipei_youbike_etl)
    thread_2 = threading.Thread(target=taichung_youbike_etl)

    thread_1.start()
    thread_2.start()

    thread_1.join()
    thread_2.join()


