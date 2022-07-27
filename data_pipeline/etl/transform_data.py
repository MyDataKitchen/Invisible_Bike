import pandas as pd
import os
import threading
import subprocess
import time
from data_pipeline.models.mysql import insert_data_to_record, get_data_processed_record
from data_pipeline.models.mongo import get_temp_data, insert_temp_data, delete_data, insert_log
from sqlalchemy import create_engine
from pymongo import MongoClient
from dotenv import load_dotenv
from datetime import datetime as dt
from collections import defaultdict

TAIPEI_STATION_DATA_LENGTH = 18
TAICHUNG_STATION_DATA_LENGTH = 16

TAIPEI_TABLE = "EventTaipei"
TAICHUNG_TABLE = "EventTaichung"

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
                       pool_size=20, max_overflow=5, pool_pre_ping=True)


def validation_mysql_connection(source):
    while True:
        try:
            engine.connect()
        except:
            log = {"status": "RDS connection failed"}
            insert_log(f"{ source }_youbike_cleaning", log)
            time.sleep(60)  # sleep 1 minute
            continue

        return True


def shortage_status(proportion):
    colors = ['red', 'yellow', 'green']
    if proportion >= 0 and proportion < 30:
        return colors[0]
    elif proportion >= 30 and proportion < 60:
        return colors[1]
    elif proportion >= 60 and proportion <= 100:
        return colors[2]


def station_event(datetime, temp, data):
    station_id = int(data['sno'])
    total = int(data['tot'])
    available_space = int(data['sbi'])
    empty_space = int(data['bemp'])
    status = int(data['act'])

    if temp == None:
        temp = defaultdict(int)

    if temp['availableSpace'] > available_space:
        out_per_minute = abs(temp['availableSpace'] - available_space)
        temp['availableSpace'] = available_space
        in_per_minute = 0
    elif temp['availableSpace'] < available_space:
        in_per_minute = abs(available_space - temp['availableSpace'])
        temp['availableSpace'] = available_space
        out_per_minute = 0
    elif temp['availableSpace'] == available_space:
        temp['availableSpace'] = available_space
        in_per_minute = 0
        out_per_minute = 0

    proportion = int((int(available_space) / total) * 100)
    color = shortage_status(proportion)

    if color == 'red':
        if temp['color'] == 'red':
            temp['shortageDuration'] += 1
            shortage_duration = temp['shortageDuration']
        else:
            temp['color'] = 'red'
            temp['shortageDuration'] = 1
            shortage_duration = temp['shortageDuration']
    else:
        temp['color'] = color
        temp['shortageDuration'] = 0
        shortage_duration = temp['shortageDuration']

    return {'datetime': datetime, 'stationId': station_id, 'total': total, 'availableSpace': available_space,
            'emptySpace': empty_space, 'status': status, 'outPerMinute': out_per_minute, 'inPerMinute': in_per_minute,
            'color': color, 'shortageDuration': shortage_duration}


def etl_youbike(source):
    mongodb = client[MONGO_DATABASE]
    col = mongodb[source]

    while True:
        temp_time = get_data_processed_record(source)

        if temp_time == []:
            temp_time = "2022-07-10 00:00:00"
        else:
            temp_time = temp_time[0][0].strftime('%Y-%m-%d %H:%M:%S')

        events = col.find({'created_at': {'$gt': temp_time}}, allow_disk_use=True).sort("created_at", 1)

        for event in events:
            validation_mysql_connection(source)
            filename = event['filename']
            print(f"Thread_Transform_{ source } - { filename }")
            start = time.time()
            if get_temp_data(source) is None:
                temp_data = {}
            else:
                temp_data = get_temp_data(source)['item']

            data = []
            origin_data = {'taipei': len(event['item']), 'taichung': len(event['item']['retVal'])}
            events = {'taipei': event['item'], 'taichung': event['item']['retVal']}

            data_error = 0

            for station in events[source]:
                station_id = station['sno']
                datetime = {'taipei': station['srcUpdateTime'], 'taichung': event['item']['updated_at']}
                try:
                    temp = temp_data[station_id]
                except:
                    temp = {}

                data_length = {'taipei': TAIPEI_STATION_DATA_LENGTH, 'taichung': TAICHUNG_STATION_DATA_LENGTH}
                if len(station) != data_length[source]:
                    data_error = 1

                result = station_event(datetime[source], temp, station)
                data.append(result)
                temp_data[station_id] = result

            df_stations = pd.DataFrame(data)
            final_data = len(df_stations)

            table_name = {'taipei': TAIPEI_TABLE, 'taichung': TAICHUNG_TABLE}
            df_stations.to_sql(table_name[source], engine, if_exists='append', chunksize=2000, index=False)

            insert_temp_data(source, temp_data)
            updated_time = dt.now().strftime("%Y-%m-%d %H:%M:%S")
            delete_data(source, filename)
            end = time.time()
            execution_time = end - start
            insert_data_to_record((event['filename'], updated_time, event['created_at'], source, round(execution_time, 6), origin_data, final_data, data_error))

        try:
            events.next()
        except StopIteration:
            events.rewind()
            time.sleep(30)


if __name__ == '__main__':
    subprocess.run(["python3", "backup_data.py"], check=True)

    tasks = [(etl_youbike, "taipei"), (etl_youbike, "taichung")]
    threads = []

    for task in tasks:
        threads.append(threading.Thread(target=task[0], args=(task[1],)))

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()