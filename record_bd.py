import psycopg2
import datetime

con = psycopg2.connect(
    database="Record_bd",
    user="postgres",
    password="12345",
    host="localhost",
    port="5432"
)


def insert_record(channel, record_type, record, record_path, datetime_start, datetime_stop, record_length,
                  record_extension, snapshot_path):
    cur = con.cursor()
    try:
        cur.execute("INSERT INTO record_info (id_channel, record_type, id_record, record_path, datetime_start, "
                    "datetime_stop, record_length, record_extension, snapshot_path) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (channel, record_type, record, record_path, datetime_start, datetime_stop, record_length,
                     record_extension, snapshot_path))
    except psycopg2.DatabaseError as err:
        print("Error: ", err)
    else:
        con.commit()
    con.close()


def select_record(dt_start, dt_stop):
    global record
    cur = con.cursor()
    sql = "SELECT * FROM record_info WHERE datetime_start > %s AND datetime_stop < %s"
    try:
        cur.execute(sql, (dt_start, dt_stop))
        record = cur.fetchall()  # возвращает все строки
        print(record)
    except psycopg2.DatabaseError as err:
        print("Error: ", err)
    else:
        con.commit()
    con.close()
    if record is not None:
        return record


start = datetime.datetime(2022, 7, 6, 11, 30, 40)
stop = datetime.datetime(2022, 7, 6, 15, 22, 56)
select_record(start, stop)
# now = datetime.datetime.now()
# insert_record(1, 'con', 1, '/file', now, now, 12, 'mp4', '/file')
