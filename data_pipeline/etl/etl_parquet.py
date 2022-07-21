from data_pipeline.models.mysql import get_event_data, insert_parquet_record
from data_pipeline.models.s3 import insert_parquet_to_s3
from sqlalchemy import create_engine
from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import datetime as dt
import pandas as pd
import os
import time
import threading
import json
import schedule

load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')

engine = create_engine(f"mysql+pymysql://{ SQL_USER }:{ SQL_PASSWORD }@{ SQL_HOST }/{ SQL_DATABASE }",
                       pool_size=10, max_overflow=5, pool_pre_ping=True)


def json_converter(data):
    keys = ['日期', '時間', '日期時間', '借用站編號', '借用站名稱', '停車位數量', '可借用腳踏車數量', '空位數量', '借出數量', '歸還數量', '腳踏車供應狀況', '缺車的時間長度', '緯度',
     '經度', '區域']
    json_data = [dict(zip(keys, event)) for event in data]
    return json_data


def get_time():
    dt1 = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
    dt2 = (datetime.utcnow()+dt.timedelta(days=1)).replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
    now = dt1.strftime("%Y-%m-%d")
    next = dt2.strftime("%Y-%m-%d")
    date = dt1.strftime("%Y-%m-%d")
    return now, next, date


def taipei_youbike_etl():
    start_time, end_time, date = get_time()
    start = time.time()
    query = f"SELECT DATE_FORMAT(EventTaipei.datetime, '%%Y-%%m-%%d') AS date, DATE_FORMAT(EventTaipei.datetime, '%%H:%%i:00') AS time, DATE_FORMAT(EventTaipei.datetime, '%%Y-%%m-%%d %%H:%%i:00') AS datetime, EventTaipei.stationId, YoubikeStation.name, EventTaipei.total, EventTaipei.availableSpace, EventTaipei.emptySpace, EventTaipei.outPerMinute, EventTaipei.inPerMinute, EventTaipei.color, EventTaipei.shortageDuration, YoubikeStation.lat, YoubikeStation.lon, District.name FROM `EventTaipei` INNER JOIN `YoubikeStation` ON `EventTaipei`.stationId = `YoubikeStation`.stationId INNER JOIN `District` ON `YoubikeStation`.districtId = `District`.id WHERE datetime BETWEEN str_to_date('{ start_time }', '%%Y-%%m-%%d') AND str_to_date('{ end_time }', '%%Y-%%m-%%d')"
    data = get_event_data(query)
    end = time.time()
    query_time = end - start

    start = time.time()
    json_data = json_converter(data)
    df = pd.DataFrame(json_data)
    end = time.time()
    convert_time = end - start

    start = time.time()
    city = "taipei"
    filename = f"{ city }/{ date }_{ city }.parquet"
    print(f"thread_1 - {filename}")
    aws_response = insert_parquet_to_s3(df, filename)
    end = time.time()
    insert_time = end - start

    dt = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
    current_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    insert_parquet_record((f"{ date }_{ city }.parquet", current_time, city, round(query_time, 4), round(convert_time, 4), round(insert_time, 4), json.dumps(aws_response)))

    return True


def taichung_youbike_etl():
    start_time, end_time, date = get_time()
    start = time.time()
    query = f"SELECT DATE_FORMAT(EventTaichung.datetime, '%%Y-%%m-%%d') AS date, DATE_FORMAT(EventTaichung.datetime, '%%H:%%i:00') AS time, DATE_FORMAT(EventTaichung.datetime, '%%Y-%%m-%%d %%H:%%i:00') AS datetime, EventTaichung.stationId, YoubikeStation.name, EventTaichung.total, EventTaichung.availableSpace, EventTaichung.emptySpace, EventTaichung.outPerMinute, EventTaichung.inPerMinute, EventTaichung.color, EventTaichung.shortageDuration, YoubikeStation.lat, YoubikeStation.lon, District.name FROM `EventTaichung` INNER JOIN `YoubikeStation` ON `EventTaichung`.stationId = `YoubikeStation`.stationId INNER JOIN `District` ON `YoubikeStation`.districtId = `District`.id WHERE datetime BETWEEN str_to_date('{ start_time }', '%%Y-%%m-%%d') AND str_to_date('{ end_time }', '%%Y-%%m-%%d')"
    data = get_event_data(query)
    end = time.time()
    query_time = end - start

    start = time.time()
    json_data = json_converter(data)
    df = pd.DataFrame(json_data)
    end = time.time()
    convert_time = end - start

    start = time.time()
    city = "taichung"
    filename = f"{ city }/{ date }_{ city }.parquet"
    print(f"thread_2 - {filename}")
    aws_response = insert_parquet_to_s3(df, filename)

    end = time.time()
    insert_time = end - start

    dt = datetime.utcnow().replace(tzinfo=timezone.utc).astimezone(timezone(timedelta(hours=8)))
    current_time = dt.strftime("%Y-%m-%d %H:%M:%S")
    insert_parquet_record((f"{ date }_{ city }.parquet", current_time, city, round(float(query_time), 4), round(float(convert_time), 4), round(float(insert_time), 4), json.dumps(aws_response)))

    return True


if __name__ == '__main__':
    def job():
        thread_1 = threading.Thread(target=taipei_youbike_etl)
        thread_2 = threading.Thread(target=taichung_youbike_etl)

        thread_1.start()
        thread_2.start()

        thread_1.join()
        thread_2.join()


    schedule.every(3).minutes.do(job)

    while True:
        schedule.run_pending()
        time.sleep(1)