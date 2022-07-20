from sqlalchemy import create_engine
from dotenv import load_dotenv
import pandas as pd
import os
import datetime

load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')

engine = create_engine(f"mysql+pymysql://{ SQL_USER }:{ SQL_PASSWORD }@{ SQL_HOST }/{ SQL_DATABASE }",
                       pool_size=20, max_overflow=5, pool_pre_ping=True)

def crawler_logs(date, city):
    current_time = date
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    next_time = (dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    query = f"SELECT updateTime AS `Datetime`, dataLength AS `Data Length`, dataSize AS `Data Size`, responseTime AS `Respone Time (s)`, executionTime AS `Insert Time (s)`, insertStatus AS Status FROM { city }_youbike_logs WHERE updateTime BETWEEN str_to_date('{ current_time }' , '%%Y-%%m-%%d') AND str_to_date('{ next_time }', '%%Y-%%m-%%d')"
    df = pd.read_sql_query(query, engine)
    return df

def mysql_logs(date, city):
    current_time = date
    dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    next_time = (dt + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    query = f"SELECT datetime AS `Datetime`, query_time AS `Querying Time (s)`, convert_time AS `Transforming Time (s)`, insert_time AS `Insert Time (s)`FROM mysql_data_processed WHERE `datetime` BETWEEN str_to_date('{ current_time }' , '%%Y-%%m-%%d') AND str_to_date('{ next_time }', '%%Y-%%m-%%d') AND `data_source` = '{ city }'"
    df = pd.read_sql_query(query, engine)
    return df

def get_date():
    query = f"SELECT datetime FROM mysql_data_processed ORDER BY id DESC LIMIT 1"
    result = engine.execute(query)
    date = result.fetchall()
    result.close()
    return date[0][0].date()

