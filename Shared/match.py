from Shared.common import Common as CM, PATH
from Shared.db import DB as db
from Shared.batch import BatchService
import datetime as dt
from Shared.enums import SQL as sql
import pandas as pd
import os
from Shared.file_service import FileService


class CompanyService:

	def __init__(self):

		self.batch = BatchService()

		self.sql_update = CM.get_config('config_sql.ini', 'db_sql_general', 'sql_update')
		self.sql_data_by_batch = CM.get_config('config_sql.ini', 'db_sql_common', 'sql_data_by_batch')

		self.sql_dim_company = CM.get_config('config_sql.ini', 'da_sql_company', 'sql_dim_company')
		self.sql_dim_company_source = CM.get_config('config_sql.ini', 'da_sql_company', 'sql_dim_company_source')
		self.sql_dim_company_insert = CM.get_config('config_sql.ini', 'da_sql_company', 'sql_dim_company_insert')
		self.sql_dim_company_source_insert = CM.get_config('config_sql.ini', 'da_sql_company', 'sql_dim_company_source_insert')
		self.sql_dim_company_source_update = CM.get_config('config_sql.ini', 'da_sql_company', 'sql_dim_company_source_update')

		self.dim_company_id = 0
		self.dim_company_source_id = 0
		self.path = CM.get_config('config.ini', 'box_file_path', 'path_bap_company_matching')

		self.file = FileService(self.path)

	def get_company(self):
		df_new_company = db.pandas_read(sql.sql_bap_quarterly_company.value)
		df_old_company = db.pandas_read(self.sql_dim_company.format('Reporting.DimCompany'))
		return df_new_company, df_old_company

	def get_existing_company(self):
		df_company = db.pandas_read(self.sql_dim_company)
		df_company_source = db.pandas_read(self.sql_dim_company_source)
		return df_company, df_company_source

	def get_company_raw_data(self, batches, raw_table):
		sql = self.sql_data_by_batch.format(raw_table, batches)
		df = self.dal.pandas_read(sql)
		return df

	def generate_basic_name(self, df):
		df['BasicName'] = df.apply(lambda dfs: CM.get_basic_name(dfs.Name), axis=1)
		return df

	def update_raw_company(self, year, quarter, source_system, raw_table):
		batch = self.bs.get_batch(year, quarter, source_system)
		com = self.get_company_raw_data(tuple(batch), raw_table)
		dfdc, dfdcs = self.get_existing_company()

		raw_company = self.generate_basic_name(com)
		dim_company = self.generate_basic_name(dfdc) if len(dfdc) > 0 else None

		for index, com in raw_company.iterrows():
			company_name = com['BasicName']
			if len(dim_company[dim_company.BasicName == company_name].CompanyID.values) > 0:
				companyid = dim_company[dim_company.BasicName == company_name].CompanyID.values[0]
				if companyid > 0:
					sql = self.sql_update.format('[Config].[CompanyDataRaw]', 'CompanyID', companyid, 'ID', com.ID)
					self.dal.execute(sql)
			else:
				print('{} >> {}'.format(com.ID, com.CompanyName))

	def move_company_data(self, year, quarter, source_system, raw_table):
		batch = self.bs.get_batch(year, quarter, source_system)
		com = self.get_company_raw_data(tuple(batch), raw_table)
		df_dim_company, df_dim_company_source = self.get_existing_company()

		raw_company = self.reformat_company_name(com)
		dim_company = self.reformat_company_name(df_dim_company) if len(df_dim_company) > 0 else None
		dim_company_source = self.reformat_company_name(df_dim_company_source) if len(df_dim_company_source) > 0 else None
		i, j, k = 0, 0, 0 # Log

		response = input('Do you want to transfer the data? [Y/N] ')
		if response.lower() in CM.user_response_yes:
			if dim_company is not None and dim_company_source is not None:
				for index, com in raw_company.iterrows():
					company_name = com['BasicName']
					print('-' * 75)
					print('{} || {}'.format(com['CompanyName'], company_name))
					if len(dim_company) > 0 and len(dim_company_source) > 0:
						try:
							cid = dim_company[dim_company.BasicName == company_name].CompanyID
							print('Company ID: {} - Company Name {}'.format(cid, com.Name))
							if company_name not in dim_company.BasicName.values and \
											company_name not in dim_company_source.BasicName.values:
								print('CASE I: NOT in DIMCOMPANY & DIMCOMPANYSOURCE')
								print('{} does not exist both in DimCompany & DimCompanySource.'.format(company_name))
								i = i + 1
								print(i)
								self.insert_dim_company(com)
								self.insert_dimcompanysource(com)
							if company_name not in dim_company.BasicName.values and \
											company_name in dim_company_source.BasicName.values:
								print('CASE II: NOT IN DIMCOMPANY BUT IN DIMCOMPANYSOURCE')
								print('{} does not exist in DimCompany but exists in DimCompanySource'.format(
									company_name))
								j = j + 1
								self.insert_dim_company(com)
								self.update_company_id(self.dimcompanyid)
							if company_name in dim_company.BasicName.values and \
											company_name not in dim_company_source.BasicName.values:
								print('CASE III: IN DIMCOMPANY & NOT IN DIMCOMPANYSOURCE')
								print('{} does not exist in DimCompanySource'.format(company_name))
								k = k + 1
								cid = dim_company[dim_company['BasicName'] == company_name]['CompanyId']
								print('Company ID: {}'.format(cid))
								self.update_company_id(cid)
						except Exception as ex:
							print(ex)

		print('\nNew Company: {}\nCompany in DCS: {}\nCompany in DC: {}'.format(i, j, k))

	def insert_dim_company(self, new_company):
		self.dim_company_id = CM.get_table_seed('CompanyID', 'Reporting.DimCompany') + 1
		date_time = str(dt.datetime.utcnow())[:-3]
		sql = self.sql_dim_company_insert.format(
			self.dim_company_id,
			new_company['CompanyName'],
			None,
			None,
			None,
			None,
			None,
			new_company['Website'],
			None,
			new_company['BatchID'],
			date_time,
			date_time)
		try:
			db.execute(sql)
			print('{} inserted in DimCompany.'.format(new_company['CompanyName'])) # Logging
		except Exception as es:
			print(es)

	def insert_dim_company_source(self, new_company):
		date_time = str(dt.datetime.utcnow())[:-3]
		self.dim_company_source_id = CM.get_table_seed('ID', 'Reporting.DimCompanySource') + 1
		sql = self.sql_dim_company_insert.format(
			self.dim_company_source_id,
			self.dim_company_id,
			new_company['CompanyName'],
			'',
			new_company['DataSource'],
			new_company['BatchID'],
			None,
			date_time, date_time)
		self.dal.execute(sql)

	def update_dim_company_source(self, new_company):
		sql = self.sql_dim_company_source_update.format(self.dim_company_id,
														self.modify_string(new_company['CompanyName']))
		db.execute(sql)

	def generate_company_matching_result(self):
		index = 0
		df_new, df_old = self.get_company()
		new_company = self.generate_basic_name(df_new)
		old_company = self.generate_basic_name(df_old)
		values = []
		for _, company in new_company.iterrows():
			company_name = company['BasicName']
			try:
				index+=1
				val = dict()
				if len(old_company[old_company.BasicName == company_name]) > 0:
					cid = old_company[old_company.BasicName == company_name].CompanyID.values[0]
					cname = old_company[old_company.BasicName == company_name].Name.values[0]
					bname = old_company[old_company.BasicName == company_name].BasicName.values[0]

					val['Basic Index'] = index
					val['Basic Index Company Name'] = company.Name
					val['Basic Name'] = company.BasicName
					val['DC Basic Name '] = bname
					val['DimCompany ID'] = cid
					val['DimCompany Name'] = cname
					values.append(val)
				else:
					val['Basic Index'] = index
					val['Basic Index Company Name'] = company.Name
					val['Basic Name'] = company.BasicName
					val['DC Basic Name '] = '-'
					val['DimCompany ID'] = '-'
					val['DimCompany Name'] = '-'
					values.append(val)
			except Exception as ex:
				print('EXCEPTION >>>{}'.format(ex))
		df = pd.DataFrame.from_dict(values, orient='columns')
		print(os.getcwd())
		CM.change_location(PATH.MATCH)
		self.file.save_as_csv(df, 'Company Matcing.xlsx', self.path, 'Company List')


if __name__ == '__main__':
	com = CompanyService()
	com.generate_company_matching_result()




