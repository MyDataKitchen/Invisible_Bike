from dotenv import load_dotenv
import os
import pymysql

load_dotenv()

SQL_HOST = os.getenv('MYSQL_HOST')
SQL_USER = os.getenv('MYSQL_USER')
SQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
SQL_DATABASE = os.getenv('MYSQL_DATABASE')


db = pymysql.connect(host=SQL_HOST,
                     user=SQL_USER,
                     password=SQL_PASSWORD,
                     database=SQL_DATABASE)

def get_latest_log(sql_table):
    db.ping(reconnect=True)
    query = f"SELECT * FROM { sql_table } ORDER BY id DESC LIMIT 1;"
    cursor = db.cursor(pymysql.cursors.DictCursor)
    cursor.execute(query)
    results = cursor.fetchall()
    db.commit()
    cursor.close()
    return results

def insert_crawler_log(sql_table, params):
    db.ping(reconnect=True)
    query = f"INSERT INTO { sql_table } (filename, updateTime, dataLength, dataSize, responseTime, executionTime, insertStatus, awsRespone) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    cursor = db.cursor()
    cursor.execute(query, params)
    db.commit()
    cursor.close()
