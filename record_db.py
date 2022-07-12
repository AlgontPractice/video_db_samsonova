import string

from flask import Flask

from flask_jsonrpc import JSONRPC

import psycopg2
from psycopg2 import pool
import datetime

app = Flask('db_record')

# Flask-JSONRPC
jsonrpc = JSONRPC(app, '/api_db_record', enable_web_browsable_api=True)


@jsonrpc.method('index')
def index() -> str:
    return 'Welcome to Flask JSON-RPC'


@jsonrpc.method('insert_record')
def insert_record(channel: int, record_type: string, record1: int, record_path: string, datetime_start: string, datetime_stop: string, record_length: float,
                  record_extension: string, snapshot_path: string):

    #преобразование сторокового формата в datetime
    dt_start = datetime.datetime.strptime(datetime_start, '%Y-%m-%d %H:%M:%S.%f')
    dt_stop = datetime.datetime.strptime(datetime_stop, '%Y-%m-%d %H:%M:%S.%f')
    global postgresql_pool, cur
    cur = None
    try:
        postgresql_pool = psycopg2.pool.ThreadedConnectionPool(1, 20,
                                                               database="Record_bd",
                                                               user="postgres",
                                                               password="12345",
                                                               host="localhost",
                                                               port="5432"
                                                               )
        con = postgresql_pool.getconn()
        if con:
            cur = con.cursor()
            sql = "INSERT INTO record_info (id_channel, record_type, id_record, record_path, datetime_start, " \
                  "datetime_stop, record_length, record_extension, snapshot_path) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            try:
                cur.execute(sql, (
                    channel, record_type, record1, record_path, dt_start, dt_stop, record_length,
                    record_extension, snapshot_path))
            except psycopg2.DatabaseError as err:
                print("Error: ", err)
            finally:
                con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error", error)
    finally:
        cur.close()
        if postgresql_pool:
            postgresql_pool.closeall()


@jsonrpc.method('select_record')
def select_record(datetime_start: string, datetime_stop: string):

    #преобразование сторокового формата в datetime
    dt_start = datetime.datetime.strptime(datetime_start, '%Y-%m-%d %H:%M:%S.%f')
    dt_stop = datetime.datetime.strptime(datetime_stop, '%Y-%m-%d %H:%M:%S.%f')

    global record, postgresql_pool, cur
    cur = None
    try:
        postgresql_pool = psycopg2.pool.ThreadedConnectionPool(1, 20,
                                                               database="Record_bd",
                                                               user="postgres",
                                                               password="12345",
                                                               host="localhost",
                                                               port="5432"
                                                               )
        con = postgresql_pool.getconn()
        if con:
            cur = con.cursor()
            sql = "SELECT * FROM record_info WHERE datetime_start > %s AND datetime_stop < %s"
            try:
                cur.execute(sql, (dt_start, dt_stop))
                record = cur.fetchall()  # возвращает все строки
            except psycopg2.DatabaseError as err:
                print("Error: ", err)
            finally:
                if record is not None:
                    return record
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error", error)
    finally:
        cur.close()
        if postgresql_pool:
            postgresql_pool.closeall()


if __name__ == '__main__':
    app.run(port=1234, host='127.0.0.1')