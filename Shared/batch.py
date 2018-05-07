from Shared.enums import ImportStatus, SQL as sql
from Shared.common import Common as common
from Shared.db import DB
import datetime as dt
import pandas as pd


class BatchService:

	def __init__(self):
		self.sql_batch_count = common.get_config('config_sql.ini', 'db_sql_batch', 'sql_batch_count')
		self.sql_batch_select = common.get_config('config_sql.ini', 'db_sql_batch', 'sql_batch_select')
		self.sql_batch_table = common.get_config('config_sql.ini', 'db_sql_batch', 'sql_batch_table')
		self.sql_batch_delete = common.get_config('config_sql.ini', 'db_sql_batch', 'sql_batch_delete')
		self.sql_batch_insert = common.get_config('config_sql.ini', 'db_sql_batch', 'sql_batch_insert')
		self.sql_batch_single_insert = common.get_config('config_sql.ini', 'db_sql_batch', 'sql_batch_single_insert')
		self.sql_batch_search = common.get_config('config_sql.ini', 'db_sql_batch', 'sql_batch_search')
		self.update_statement = common.get_config('config_sql.ini', 'db_sql_batch', 'sql_batch_update')
		self.db = DB()
		self.year, self.quarter = common.fiscal_year_quarter()

	def create_batch_for_etl(self, datasource, systemsource, year, quarter, file_name='', full_path='', work_sheet_name=''):
		value = dict()
		value['UserId'] = 0
		value['ImportStatusId'] = 5
		value['FileName'] = file_name
		value['FullPath'] = full_path
		value['SourceSystemId'] = systemsource
		value['WorksheetName'] = work_sheet_name
		value['FileModifiedDate'] = str(dt.datetime.today())[:10]
		value['DataSourceId'] = datasource
		value['StartDate'] = str(dt.datetime.today())[:10]
		value['EndDate'] = str(dt.datetime.today())[:10]
		value['Records'] = 0
		value['StgRecords'] = 0
		value['DWRecords'] = 0
		value['IsDeleted'] = 0
		value['DateCreated'] = str(dt.datetime.today())[:10]
		value['DateUpdated'] = str(dt.datetime.today())[:10]
		value['Year'] = year
		value['Quarter'] = 'Q{}'.format(quarter)
		sqli = self.sql_batch_single_insert.format('CONFIG.ImportBatch', tuple(value.values()))
		self.db.execute(sqli)
		new_batch = self.search_batch(year, quarter, systemsource, datasource, work_sheet_name, file_name,
									  file_path=full_path)
		return new_batch

	def create_new_batch(self, datasource, systemsource, year, quarter, file_name='', full_path='', work_sheet_name=''):
		value = dict()

		value['FullPath'] = full_path
		value['FileName'] = file_name
		value['WorksheetName'] = work_sheet_name
		value['SourceSystem'] = systemsource
		value['DataSource'] = datasource
		value['ImportStatus'] = 5
		value['DateCreated'] = str(dt.datetime.today())[:10]
		value['DateModified'] = str(dt.datetime.today())[:10]
		value['FiscalQuarter'] = 'Q{}'.format(quarter)
		value['FiscalYear'] = year

		sql = self.sql_batch_single_insert.format('CONFIG.Batch', tuple(value.values()))
		self.db.execute(sql)
		new_batch = self.search_batch(year, quarter, systemsource, datasource, work_sheet_name, file_name,
									  file_path=full_path)
		return new_batch

	def search_batch(self, year, quarter, systemsource, datasource, worksheet_name='', file_name='', file_path=''):
		criteria = "FiscalYear = {} AND FiscalQuarter LIKE '{}' AND SourceSystem = {} AND DataSource = {}".format(year, "Q" + str(quarter), systemsource, datasource)
		if file_path is not '':
			criteria = criteria + ' AND FullPath = {}'.format(file_path)
		if file_name is not '':
			criteria = criteria + ' AND FileName = {}'.format(file_name)
		if worksheet_name is not '':
			criteria = criteria + ' AND WorkSheet = {}'.format(worksheet_name)
		sql = self.sql_batch_search.format('CONFIG.Batch', criteria)
		df = DB.pandas_read(sql)
		return df

	def get_bap_batch(self, year, quarter, source_system):
		sql_select = sql.sql_batch_select.value.format(year, quarter, source_system)
		batches = self.db.pandas_read(sql_select)
		return batches

	def create_bap_batch(self, dataframe, year, quarter, table, sheet, system_source):
		print('creating batches...')
		values = []
		for _, row in dataframe.iterrows():
			val = []
			data_source = row['DataSource']
			val.append(0)
			val.append(ImportStatus.COMPLETED.value)
			val.append(row['FileName'])
			val.append(row['Path'])
			val.append(row['SourceSystem'])
			val.append(sheet)
			val.append(dt.datetime.now())
			val.append(data_source)
			val.append(dt.datetime.now())
			val.append(dt.datetime.now())
			val.append(len(dataframe))
			val.append(len(dataframe))
			val.append(len(dataframe))
			val.append(0)
			val.append(dt.datetime.now())
			val.append(dt.datetime.now())
			val.append(year)
			val.append('Q{}'.format(quarter))
			values.append(val)
		self.db.bulk_insert(self.sql_batch_insert, values)
		self.update_source_batch(table, year, quarter, system_source)

	def create_batch(self, dataframe):
		values = []
		for i, df in dataframe.iterrows():
			val = dict()
			val['UserId'] = None
			val['ImportStatusId'] = 5
			val['FileName'] = df.FileName
			val['FullPath'] = df.Path
			val['SourceSystemId'] = df.SourceSystem
			val['WorksheetName'] = df.WorkSheet
			val['FileModifiedDate'] = str(dt.datetime.today())[:10]
			val['DataSourceId'] = df.DataSource
			val['StartDate'] = str(dt.datetime.today())[:10]
			val['EndDate'] = str(dt.datetime.today())[:10]
			val['Records'] = None
			val['StgRecords'] = None
			val['DWRecords'] = None
			val['IsDeleted'] = 0
			val['DateCreated'] = str(dt.datetime.today())[:10]
			val['DateUpdated'] = str(dt.datetime.today())[:10]
			val['Year'] = self.year
			val['Quarter'] = None
			values.append(val)
		df = pd.DataFrame(values, columns=values[0].keys())
		vals = common.df_list(df)
		self.db.bulk_insert(sql.sql_batch_insert.value, vals)


	def update_source_batch(self, table, year, quarter, source_system):
		df = self.get_bap_batch(year, quarter, source_system)
		if df is not None:
			print('Updating BatchID for table {}...'.format(table))
			for index, row in df.iterrows():
				sql_update = sql.sql_batch_update.value.format(table, row['BatchID'], source_system, row['DataSourceId'])
				self.db.execute(sql_update)
		print('Batch updated for table: {}'.format(table))

	def can_push_data(self, table, year, quarter, source_system):
		batch = self.get_batch(year, quarter, source_system)
		sql = common.get_config('config_sql.ini', 'db_sql_batch', 'sql_batch_table')
		if len(batch) > 0:
			sql = sql.format(table, tuple(batch))
			df = self.dal.pandas_read(sql)
			if len(df) > 0:
				return False
			else:
				return True
		else:
			return False

	def rollback_data(self, year, quarter, source_system, table_list):
		batches = self.get_batche(year, quarter, source_system)
		print(batches)
		if len(batches) > 0:
			self.get_tables_stat(batches, table_list)
			print(batches)
			are_you_sure = input(
				'Are you sure you want to rollback all this year and quarter data from the database?\t')
			if are_you_sure in CM.user_response_yes:
				for table in table_list:
					select_sql = self.sql_batch_count.format(table, batches)
					delete_sql = self.sql_batch_delete.format(table, batches)
					df = self.dal.pandas_read(select_sql)
					print(df.head())
					self.dal.execute(delete_sql)
					print('{} deleted.'.format(table))
		else:
			print('No batch was found for the year and quarter specified.')

	def get_tables_stat(self, batches, table_list):
		sql_stm = common.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_count')
		for tbl in table_list:
			sql_stm = sql_stm.format(tbl, batches)
			print('{} : {}'.format(tbl, DB.pandas_read(sql_stm)['Total'].values))

	def get_table_seed(self, table, id_column):
		seed = 0
		sql_dc = sql.sql_get_max_id.value.format(id_column, table)
		df = self.db.pandas_read(sql_dc)
		if len(df) > 0:
			seed = df.values[0][0]
		return seed



