import psycopg2
from psycopg2 import pool

def insert_record(channel, record_type, record, record_path, datetime_start, datetime_stop, record_length,
                  record_extension, snapshot_path):
    global postgresql_pool
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
                cur.execute(sql, (channel, record_type, record, record_path, datetime_start, datetime_stop, record_length, record_extension, snapshot_path))
            except psycopg2.DatabaseError as err:
                print("Error: ", err)
            finally:
                con.commit()
            cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error", error)
    finally:
        if postgresql_pool:
            postgresql_pool.closeall()

def select_record(dt_start, dt_stop):
    global record, postgresql_pool
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
                print(record)
            except psycopg2.DatabaseError as err:
                print("Error: ", err)
            finally:
                con.commit()
            cur.close()
            if record is not None:
                return record
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error", error)
    finally:
        if postgresql_pool:
            postgresql_pool.closeall()