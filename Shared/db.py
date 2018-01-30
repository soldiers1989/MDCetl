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
			print('Executing SC Exception: {}'.format(ex))

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
			print('Bulk Insert SUCCESSFUL !')
		except Exception as ex:
			print('Bulk Insert Exception: {}'.format(ex))

	@staticmethod
	def save_data_chunk(df, sql_insert, chunk_size=1000):
		i = 0
		j = i + chunk_size
		total_size = len(df) + 1
		while i < total_size:
			print('From {} to {}'.format(i, j))
			df_insert = df.iloc[i:j]
			values = Common.df_list(df_insert)
			DB.bulk_insert(sql_insert, values)
			i, j = i + chunk_size, j + chunk_size
			if j > total_size:
				j = total_size


