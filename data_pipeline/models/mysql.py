from dotenv import load_dotenv
from sqlalchemy import create_engine
import os

load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')

engine = create_engine(f"mysql+pymysql://{ SQL_USER }:{ SQL_PASSWORD }@{ SQL_HOST }/{ SQL_DATABASE }",
                       pool_size=10, max_overflow=5, pool_pre_ping=True)

def insert_data_to_record(params):
    try:
        engine.execute(
            "INSERT INTO mongo_data_processed (filename, datetime, created_time, data_source) VALUES (%s, %s, %s, %s)", params
        )
        return True

    except Exception as e:
        print(e)
        return False

def get_data_processed_record(param):
    result = engine.execute(
        "SELECT created_time FROM mongo_data_processed WHERE data_source = %s ORDER BY id DESC LIMIT 1", param
    )
    created_time = result.fetchall()
    result.close()
    return created_time