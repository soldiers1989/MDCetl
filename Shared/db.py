import pyodbc
import pandas as pd
from Shared.common import Common
from Shared.enums import SQL as sq


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
			print('Executed sucessfully.')
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
	def bulk_insert(sql, values, dev=False):
		con = DB.connect(dev=dev)
		cursor = con.cursor()
		try:
			cursor.fast_executemany=True
			cursor.executemany(sql, values)
			cursor.commit()
			print('Bulk Insert SUCCESSFUL !')
		except Exception as ex:
			print('Bulk Insert Exception: {}\n{}'.format(ex, sql))

	@staticmethod
	def save_data_chunk(df, sql_insert, chunk_size=1000):
		i = 0
		j = i + chunk_size
		total_size = len(df) + 1
		while i < total_size:
			print('From {} to {}'.format(i, j))
			df_insert = df.iloc[i:j]
			# df_insert['name'] = df_insert.apply(lambda dfs: Common.sql_compliant(dfs['name']), axis=1)
			# df_insert['short_description'] = df_insert.apply(lambda dfs: Common.sql_compliant(dfs.short_description), axis=1)
			# print(df_insert.head())
			values = Common.df_list(df_insert)
			DB.bulk_insert(sql_insert, values)
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




