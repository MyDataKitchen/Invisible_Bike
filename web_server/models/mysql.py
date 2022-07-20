from dotenv import load_dotenv
from sqlalchemy import create_engine
import os
import pandas as pd

load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')

engine = create_engine(f"mysql+pymysql://{ SQL_USER }:{ SQL_PASSWORD }@{ SQL_HOST }/{ SQL_DATABASE }",
                       pool_size=10, max_overflow=5, pool_pre_ping=True)


def get_all_stations(city):
    query = f"SELECT `YoubikeStation`.stationId, `YoubikeStation`.name, `YoubikeStation`.address, `District`.name FROM `YoubikeStation` INNER JOIN `District` ON `YoubikeStation`.districtId = `District`.id WHERE `District`.city = '{ city }'"
    df = pd.read_sql_query(query, engine)
    stations_id = df['stationId'].tolist()
    return df, stations_id

def get_date():
    query = f"SELECT datetime FROM mysql_data_processed ORDER BY id DESC LIMIT 1"
    result = engine.execute(query)
    date = result.fetchall()
    result.close()
    return date[0][0]