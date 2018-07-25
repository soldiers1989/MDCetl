import pyodbc
import pandas as pd
from Shared.common import Common
from Shared.enums import SQL as sq
import time
# from Shared.file_service import FileService as FS


class DB:

    def __init__(self):
        pass

    @staticmethod
    def connect(dev=False):
        conn = 'conn'
        if dev:
            conn = 'devconn'
        try:
            con_str = Common.get_config('config.ini', 'db_connect', conn)
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
            print('Query executed sucessfully.')
        except Exception as ex:
            print('Executing Exception: {}'.format(ex))
            print(sql)

    @staticmethod
    def pandas_read(sql):
        try:
            conn = DB.connect()
            data = pd.read_sql(sql, conn)#, parse_dates=['IntakeDate'])
            return data
        except Exception as ex:
            print('Read SQL Exception: {}'.format(ex))
            return None

    @staticmethod
    def bulk_insert(sql, values, dev=False, rtrn_msg=False):
        con = DB.connect(dev=dev)
        cursor = con.cursor()
        try:
            # cursor.fast_executemany=True
            cursor.executemany(sql, values)
            cursor.commit()
            print('Bulk Insert SUCCESSFUL !')
            if rtrn_msg:
                return 'SUCCESS'
        except Exception as ex:
            print('Bulk Insert Exception: {}\n{}'.format(ex, sql))
            if rtrn_msg:
                return 'FAILURE'

    @staticmethod
    def save_data_chunk(df, sql_insert, chunk_size=1000, capture_fails=False, fail_path_key=''):
        i = 0
        j = i + chunk_size
        total_size = len(df) + 1
        while i < total_size:
            now = int(round(time.time() * 1000))
            print('From {} to {}'.format(i, j))
            df_insert = df.iloc[i:j]
            values = Common.df_list(df_insert)
            if capture_fails:
                msg = DB.bulk_insert(sql_insert, values, rtrn_msg=True)
                if msg == 'FAILURE':
                    filename = '{}_fail_chunk_{}_to_{}.xlsx'.format(now, i, j)
                    if fail_path_key != '':
                        Common.save_as_excel(dfs=[df_insert], file_name=filename, path_key=fail_path_key)
                        print("\tCHUNK FAILED. SAVED TO {}".format(filename))
            else:
                DB.bulk_insert(sql_insert, values)
            print('-' * 150)
            i, j = i + chunk_size, j + chunk_size
            if j > total_size:
                j = total_size

    @staticmethod
    def get_table_seed(table, id_column):
        seed = 0
        sql_dc = sq.sql_get_max_id.value.format(id_column, table)
        df = DB.pandas_read(sql_dc)
        if len(df) > 0:
            seed = df.values[0][0]
        return seed

    @staticmethod
    def update_basic_name(select, key, venture_name, update):
        data = DB.pandas_read(select)
        for _, r in data.iterrows():
            basic_name = Common.get_basic_name(r['{}'.format(venture_name)])
            ven_name = r['{}'.format(venture_name)]
            basic_name = basic_name.replace("'", "\''")
            sql_update = update.format(basic_name, Common.sql_compliant(r['{}'.format(key)]))
            DB.execute(sql_update)
            print('{}({})'.format(ven_name, basic_name))

    @staticmethod
    def entity_exists(table, column, value):
        df = DB.execute(sq.sql_entity_exists.value.format(table, column, value))
        if len(df) > 0:
            return True
        else:
            return False




