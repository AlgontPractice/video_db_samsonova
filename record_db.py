from flask import Flask
from flask_jsonrpc import JSONRPC
import psycopg2
import psycopg2.extras

app = Flask('db_record')

# Flask-JSONRPC
jsonrpc = JSONRPC(app, '/api_db_record', enable_web_browsable_api=True)


@jsonrpc.method('echo')
def echo(message: str) -> str:
    return message


@jsonrpc.method('insert_record')
def insert_record(channel: int, record_type: str, id_record: str, record_path: str, datetime_start: str, datetime_stop: str, record_length: float,
                  record_extension: str, snapshot_path: str):

    global con, cur
    cur = None
    try:
        con = psycopg2.connect(
                                                               database="Record_bd",
                                                               user="postgres",
                                                               password="12345",
                                                               host="localhost",
                                                               port="5432"
                                                               )
        if con:
            cur = con.cursor()
            sql = "INSERT INTO record_info (id_channel, record_type, id_record, record_path, datetime_start, " \
                  "datetime_stop, record_length, record_extension, snapshot_path) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            try:
                cur.execute(sql, (
                    channel, record_type, id_record, record_path, datetime_start, datetime_stop, record_length,
                    record_extension, snapshot_path))
            except psycopg2.DatabaseError as err:
                print("Error: ", err)
            finally:
                con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error", error)
    finally:
        cur.close()
        con.close()


@jsonrpc.method('select_record')
def select_record(datetime_start: str, datetime_stop: str) -> list:

    global record, con, cur
    cur = None
    record = None
    try:
        con = psycopg2.connect(
                                                               database="Record_bd",
                                                               user="postgres",
                                                               password="12345",
                                                               host="localhost",
                                                               port="5432"
                                                               )
        if con:
            cur = con.cursor(cursor_factory=psycopg2.extras.DictCursor)
            sql = "SELECT * FROM record_info WHERE datetime_start > %s AND datetime_stop < %s AND record_length > %s"
            try:
                cur.execute(sql, (datetime_start, datetime_stop,0))
                record = []
                for row in cur:
                    t = dict(row)
                    t['datetime_start'] = str(t['datetime_start']).partition('.')[0]
                    t['datetime_stop'] = str(t['datetime_stop']).partition('.')[0]
                    record.append(t)
            except psycopg2.DatabaseError as err:
                print("Error: ", err)
            finally:
                if record is not None:
                    return record
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error", error)
    finally:
        cur.close()
        con.close()


if __name__ == '__main__':
    app.run(port=1234, host='0.       0.0.0')