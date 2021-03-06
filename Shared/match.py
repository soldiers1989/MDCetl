from Shared.common import Common as CM, PATH
from Shared.db import DB as db
from Shared.batch import BatchService
import datetime as dt
from Shared.enums import SQL as sql
import pandas as pd
import os
import numpy as np
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

	def get_table_seed(self, table, id_column):
		seed = 0
		sql_dc = sql.sql_get_max_id.value.format(id_column, table)
		df = db.pandas_read(sql_dc)
		if len(df) > 0:
			seed = df.values[0][0]
		return seed

	def get_company(self):
		df_new_company = db.pandas_read(sql.sql_bap_quarterly_company.value)
		df_old_company = db.pandas_read(self.sql_dim_company.format('Reporting.DimCompany'))
		df_company_source = db.pandas_read(self.sql_dim_company_source.format('Reporting.DimCompanySource'))
		return df_new_company, df_old_company, df_company_source

	def get_existing_company(self):
		df_company = db.pandas_read(self.sql_dim_company.format('Reporting.DimCompany'))
		df_company_source = db.pandas_read(self.sql_dim_company_source.format('Reporting.DimCompanySource'))
		return df_company, df_company_source

	def get_company_raw_data(self, batches, raw_table):
		sql = self.sql_data_by_batch.format(raw_table, batches)
		df = self.dal.pandas_read(sql)
		return df

	def generate_basic_name(self, df):
		df['BasicName'] = df.apply(lambda dfs: CM.get_basic_name(dfs.Name), axis=1)
		return df

	def update_raw_company(self):
		dfnew, dfdc, _ = self.get_company()
		raw_company = self.generate_basic_name(dfnew)
		dim_company = self.generate_basic_name(dfdc) if len(dfdc) > 0 else None

		for index, com in raw_company.iterrows():
			try:
				company_name = com['BasicName']
				print(company_name)
				if len(dim_company[dim_company.BasicName == company_name].CompanyID.values) > 0:
					companyid = dim_company[dim_company.BasicName == company_name].CompanyID.values[0]
					if companyid > 0:
						sql_update = sql.sql_update.value.format('BAP.QuarterlyCompanyData', 'CompanyID', companyid, 'ID', com.ID)
						print(sql_update)
						db.execute(sql_update)
				else:
					print('{} >> {}'.format(com.ID, com.CompanyName))
			except Exception as ex:
				print('UPDATE ISSUE: {}'.format(ex))

	def move_company_data(self):
		df_new_company, df_dim_company, df_dim_company_source = self.get_company()

		raw_company = self.generate_basic_name(df_new_company)
		dim_company = self.generate_basic_name(df_dim_company) if len(df_dim_company) > 0 else None
		# dim_company_source = self.generate_basic_name(df_dim_company_source) if len(df_dim_company_source) > 0 else None
		i, j, k = 0, 0, 0
		print('New Company: {}\nExisting Company: {}'.format(len(raw_company), len(dim_company)))
		response = input('Do you want to move the company data? [Y/N] ')
		if response.lower() in CM.user_response_yes:
			if dim_company is not None:# and dim_company_source is not None:
				for index, com in raw_company.iterrows():
					company_name = com['BasicName']
					# print('{} | {}'.format(com['Name'], company_name))
					if len(dim_company) > 0: #and len(dim_company_source) > 0:
						try:
							cid = dim_company[dim_company.BasicName == company_name].CompanyID
							if len(cid) == 0:
								cid = 0
							elif len(cid) > 1:
								cid = dim_company[dim_company.BasicName == company_name].CompanyID.values[0]
							# print('[Company Id]: {}\t[Company Name]: {}'.format(int(cid), com.Name))
							print(company_name)
							if company_name not in dim_company.BasicName.values:# and company_name not in dim_company_source.BasicName.values:
								print('CASE I: NOT in DIMCOMPANY & DIMCOMPANYSOURCE')
								i = i + 1
								print('{}. {}'.format(i, company_name))
								self.insert_dim_company(com)
								# self.insert_dim_company_source(com)
							if company_name not in dim_company.BasicName.values:# and company_name in dim_company_source.BasicName.values:
								print('CASE II: NOT IN DIMCOMPANY ....but already taken care of.  >>> NO ACTION NECESSARY')
								#j = j + 1
								#self.insert_dim_company(com)
								## self.update_dim_company_source(self.dim_company_id, com.Name)
								#print('{}. {}'.format(j, company_name))
							if company_name in dim_company.BasicName.values:# and company_name not in dim_company_source.BasicName.values:
								print('CASE III: IN DIMCOMPANY  {}  >>> NO ACTION NECESSARY'.format(cid.values[0]))
							# 	k = k + 1
							# 	if isinstance(cid, np.int64):
							# 		companyID = cid
							# 	elif cid.values:
							# 		companyID = cid.values[0]
							# 	self.update_dim_company_source(companyID, com.Name)
							# 	print('{}. {}'.format(k, company_name))
						except Exception as ex:
							val = '>>' * 100
							print('EXCEPTION: {} {}'.format(ex, val))

		print('\nNew Company: {}\nCompanies ONLY in DCS: {}\nCompanies ONLY in DC: {}'.format(i, j, k))

	def insert_dim_company(self, new_company):
		try:
			self.dim_company_id = self.get_table_seed('Reporting.DimCompany', 'CompanyID') + 1
			date_time = str(dt.datetime.utcnow())[:-3]
			dc = dict()
			dc['aCompanyID'] = self.dim_company_id
			dc['bName'] = new_company['Name']
			dc['cDescription'] = None
			dc['dPhone'] = None
			dc['ePhone2'] = None
			dc['fFax'] = None
			dc['gEmail'] = None
			dc['hWebsite'] = new_company['Website']
			dc['iCompanyType'] = None
			dc['jBatchID'] = new_company['BatchID']
			dc['kModifiedDate'] = date_time
			dc['lCreatedDate'] = date_time
			df = pd.DataFrame.from_dict([dc], orient='columns')
			values = CM.df_list(df)
			db.bulk_insert(sql.sql_dim_company_insert.value, values)
		except Exception as es:
			print(es)

	def insert_dim_company_source(self, new_company):
		try:
			date_time = str(dt.datetime.utcnow())[:-3]
			self.dim_company_source_id = self.get_table_seed('Reporting.DimCompanySource', 'SourceCompanyID') + 1
			dc = dict()
			dc['aSourceID'] = self.dim_company_source_id
			dc['bCompanyID'] = self.dim_company_id
			dc['cName'] = new_company['Name']
			dc['dSCC'] = None
			dc['eDataSource'] = new_company['DataSource']
			dc['eBatchID'] = new_company['BatchID']
			dc['fCT'] = None
			dc['gModified'] = date_time
			dc['hCreated'] = date_time
			df = pd.DataFrame.from_dict([dc], orient='columns')
			values = CM.df_list(df)
			db.bulk_insert(sql.sql_dim_company_source_insert.value, values)
		except Exception as ex:
			print(ex)

	def update_dim_company_source(self, company_id, company_name):
		sql_update = self.sql_dim_company_source_update.format(company_id, CM.sql_friendly(company_name))
		print(sql_update)
		db.execute(sql_update)

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
					val['RIC'] = company.FileName
					val['Basic Index Company Name'] = company.Name
					val['Basic Name'] = company.BasicName
					val['DC Basic Name '] = bname
					val['DimCompany ID'] = cid
					val['DimCompany Name'] = cname
					values.append(val)
				else:
					val['Basic Index'] = index
					val['RIC'] = company.FileName
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
		self.file.save_as_csv(df, 'Company_Matching_FY18_Q3.xlsx', self.path, 'Company Matched')

	def move_annual_company_data(self):
		i, j = 0, 0
		dfac = db.pandas_read('SELECT ID, BatchID, CompanyID,[Company Name] FROM BAP.AnnualCompanyData')
		dfdc = db.pandas_read('SELECT CompanyID, CompanyName FROM Reporting.DimCompany')
		dfac['BasicName'] = dfac.apply(lambda dfs: CM.get_basic_name(dfs['Company Name']), axis=1)
		dfdc['BasicName'] = dfdc.apply(lambda dfs: CM.get_basic_name(dfs.CompanyName), axis=1)
		for i, c in dfac.iterrows():
			dfc = dfdc[dfdc['BasicName'] == c.BasicName]
			val = dict()
			if len(dfc) > 0:
				i+=1
				db.execute(sql.sql_annual_comapny_data_update.value.format(dfc.CompanyID.values[0], c.ID))
				print(sql.sql_annual_comapny_data_update.value.format(dfc.CompanyID.values[0], c.ID))
			else:
				j+=1
				print(sql.sql_dim_company_insert.value)
				new_com_id = self.batch.get_table_seed('MaRSDataCatalyst.Reporting.DimCompany', 'CompanyID') + 1
				val['CompanyID'] = new_com_id
				val['Company Name'] = c['Company Name']
				val['Description'] = None
				val['Phone'] = None
				val['Phone2'] = None
				val['Fax'] = None
				val['Email'] = None
				val['Website'] = None
				val['CompanyType'] = None
				val['BatchID'] = c.BatchID
				val['ModifiedDate'] = str(dt.datetime.utcnow())[:-3]
				val['CreatedDate'] = str(dt.datetime.utcnow())[:-3]
				df = pd.DataFrame([val], columns=val.keys())
				values = CM.df_list(df)
				db.bulk_insert(sql.sql_dim_company_insert.value, values)
				db.execute(sql.sql_annual_comapny_data_update.value.format(new_com_id, c.ID))
		print('{} exists and {} doesn not exist'.format(i, j))

	def update_tdw_basic_company(self):
		df = db.pandas_read(sql.sql_tdw_basic_company.value)
		for _, r in df.iterrows():
			basic_name = CM.get_basic_name(r.legal_name)
			sql_update = sql.sql_tdw_basic_company_update.value.format(basic_name, CM.sql_compliant(r.legal_name))
			print(sql_update)
			db.execute(sql_update)

	def update_cb_basic_company(self):
		df = db.pandas_read(sql.sql_cb_basic_company.value)
		for _, r in df.iterrows():
			basic_name = CM.get_basic_name(r['name'])
			sql_update = sql.sql_cb_basic_company_update.value.format(basic_name, CM.sql_compliant(r['org_uuid']))
			print(sql_update)
			db.execute(sql_update)

	def insert_new_venture(self):
		data = db.pandas_read(sql.sql_bap_new_company.value)
		values = CM.df_list(data)
		db.bulk_insert(sql.sql_venture_insert.value,values)

	def split_venture_former_name(self):
		df = db.pandas_read('SELECT ID, CompanyName, [Former / Alternate Names] FROM MDCRaw.BAP.VentureQuarterlyData WHERE CompanyName LIKE \'%(%\' AND FiscalYear = 2019')
		for _, row in df.iterrows():
			split = CM.venture_name_with_bracket_split(row['CompanyName'])
			# print('Current: {}\nName: {}\nAlternate: {}'.format(row['CompanyName'],split[0], split[1].replace('(','').replace(')','')))
			# print('-' * 100)
			update = '''UPDATE MDCRaw.BAP.VentureQuarterlyData SET CompanyName = \'{}\' , [Former / Alternate Names] = \'{}\' WHERE ID = {} -- {}'''
			print(update.format(split[0], split[1].replace('(','').replace(')','').replace('formerly',''),row['ID'],row['CompanyName']))


if __name__ == '__main__':
	com = CompanyService()
	# com.update_raw_company()
	# com.move_annual_company_data()
	# com.update_tdw_basic_company()
	# com.update_cb_basic_company()
	# com.insert_new_venture()
	com.split_venture_former_name()



