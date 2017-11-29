import pyodbc
import pandas as pd
from Shared.common import Common


class DB:

    def __init__(self):
        pass

    @staticmethod
    def connect():
        try:
            con_str = Common.get_config('config.ini', 'db_connect', 'conn')
            conn = pyodbc.connect(con_str)
            return conn
        except Exception as ex:
            print('DB Server Connection Exception: {}'.format(ex))
            return None

    @staticmethod
    def execute(sql):
        try:
            con = DB.connect()
            cursor = con.cursor()
            cursor.execute(sql)
            cursor.commit()
        except Exception as ex:
            print('Executing to DB Exception: {}'.format(ex))

    @staticmethod
    def pandas_read(sql):
        try:
            conn = DB.connect()
            data = pd.read_sql(sql, conn)
            return data
        except Exception as ex:
            print('Read SQL Exception: {}'.format(ex))
            return None

    @staticmethod
    def bulk_insert(sql, values):
        con = DB.connect()
        cursor = con.cursor()
        try:
            cursor.executemany(sql, values)
            cursor.commit()
            print('DATA UPLOAD SUCCESSFUL !')
        except Exception as ex:
            print('DB Upload Error: {}'.format(ex))

