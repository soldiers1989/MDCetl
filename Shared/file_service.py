import os
import uuid
import pandas as pd
import datetime

from Shared.db import DB
from Shared.common import Common as COM
from Shared.enums import FileType as FT
from Shared.enums import MDCDataSource as DS
from Shared.enums import WorkSheet as WS, SourceSystemType as SS, FileType, Schema, SQL as sql, Combine, DataSourceType as DST
# import Shared.enums as enum


class FileService:

	def __init__(self, path):
		self.path = os.path.join(os.path.expanduser("~"), path)
		self.source_file = None
		self._set_folder()
		self.data_list = []
		self.year, self.quarter = COM.fiscal_year_quarter(datetime.datetime.utcnow())

	def _set_folder(self):
		os.chdir(self.path)
		self.source_file = os.listdir(self.path)

	def show_source_file(self):
		file_list = self.get_source_file()
		COM.print_list(file_list)

	def get_source_file(self):
		return [f for f in self.source_file if f[0:2] != '~$' and (f[-3:] in FileType.SPREAD_SHEET.value or f[-4:] in FileType.SPREAD_SHEET.value)]

	def read_source_file(self, ftype, data_source, combine_for:Combine, file_name='', current_path=''):
		com_a = None
		if current_path != '':
			current_path = os.path.join(os.path.expanduser("~"), current_path)
			os.chdir(current_path)
			self.source_file = os.listdir(current_path)

		file_list = self.get_source_file()

		if data_source == DS.BAP:
			if ftype == FT.SPREAD_SHEET.value:
				l_company, l_company_annual, l_program, l_program_youth = self.bap_dataframes()
				i = 0
				for fl in file_list:
					try:
						i+=1
						ds = COM.set_datasource(str(fl))
						print('{}. {}'.format(i, fl))
						prg = pd.read_excel(fl, WS.bap_program.value)
						prg.columns = self.program_columns
						prg_youth = pd.read_excel(fl, WS.bap_program_youth.value)
						prg_youth.columns = self.program_youth_columns
						com = pd.read_excel(fl, WS.bap_company.value)
						com.columns = self.quarterly_company_columns
						if ds in [DST.COMMUNI_TECH.value, DST.HAL_TECH.value]:
							com_a = pd.read_excel(fl, WS.bap_company_annual.value)
							com_a.columns = self.annual_company_columns
							l_company_annual.append(com_a)
						if combine_for == Combine.FOR_ETL:
							FileService.data_system_source(prg, prg_youth, com, com_a, os.getcwd(), str(fl), ds)

						l_program.append(prg)
						l_program_youth.append(prg_youth)
						l_company.append(com)

					except Exception as ex:
						print(ex)
				bap_program = pd.concat(l_program)
				bap_program_youth = pd.concat(l_program_youth)
				bap_company = pd.concat(l_company)
				if combine_for == Combine.FOR_ETL:
					bap_company_annual = pd.concat(l_company_annual)
					return bap_program, bap_program_youth, bap_company, bap_company_annual
				else:
					return bap_program, bap_program_youth, bap_company

		elif data_source == DS.CBINSIGHT:
			if ftype == FT.CSV:
				if file_name != '':
					return pd.read_csv(file_name)
				else:
					for fl in file_list:
						self.data_list.append(pd.read_csv(fl))
					return self.data_list
		elif data_source == DS.OSVP:
			print('')
			print('')
		elif data_source == DS.OTHER:
			if ftype == FT.SPREAD_SHEET:
				target_list = []
				self.target_list_dataframe()
				j=0
				for f in file_list:
					j += 1
					print('{}. {}'.format(j, f))
					tl = pd.read_excel(f, WS.target_list.value)#, date_parser=['Date_founded', 'Date_of_incorporation'])

					tl.insert(5, 'Venture_basic_name', None)
					datasource = COM.set_datasource(f)
					tl.insert(0, 'DataSource', datasource)
					tl.insert(0, 'Worksheet', str(WS.target_list.value))
					tl.insert(0, 'FileName', str(f))
					tl.insert(0, 'Path', self.path)
					tl.insert(0, 'CompanyID', None)
					tl.insert(0, 'BatchID', 0)
					tl['Year'] = self.year
					tl.columns = self.tl_columns
					tl['Date_founded'] = tl['Date_founded'][:10]
					tl['Date_of_incorporation'] = tl['Date_of_incorporation'][:10]
					target_list.append(tl)
					print('{} - {}'.format(len(tl.columns), tl.columns))

				df_tl = pd.concat(target_list)
				return df_tl
		elif data_source == DS.IAF:
			if ftype == FT.SPREAD_SHEET:
				iaf_list = []
				k = 0
				for ia in file_list:
					k += 1
					print('{}. {}'.format(k, ia))
					iaf = pd.read_excel(ia, sheet_name=None)
					for i in range(len(list(iaf.items()))):
						if i == 0:
							df_summary = list(iaf.items())[i][1]
						elif i > 0:
							df = list(iaf.items())[i][1][:-8].T[1:]
							iaf_list.append(df)
					df_iaf = pd.concat(iaf_list)
					df_iaf = df_iaf.where(pd.notnull(df_iaf), None)
					df_summary = df_summary.where(pd.notnull(df_summary), None)
				return df_iaf, df_summary

	def osvp_dataframes(self):
		# df = DB.pandas_read((sql.sql_columns.value.format('OSVP')))
		pass

	def bap_dataframes(self):
		df = DB.pandas_read(sql.sql_columns.value.format('BAP'))
		if df is not None:
			l_program = []
			l_program_youth = []
			l_company = []
			l_company_annual = []
			self.program_columns = list(df[df['TABLE_NAME'] == 'ProgramData']['COLUMN_NAME'][7:])
			self.program_youth_columns = list(df[df['TABLE_NAME'] == 'ProgramDataYouth']['COLUMN_NAME'][7:])
			self.quarterly_company_columns = list(df[df['TABLE_NAME'] == 'QuarterlyCompanyData']['COLUMN_NAME'][8:])
			self.annual_company_columns = list(df[df['TABLE_NAME'] == 'AnnualCompanyData']['COLUMN_NAME'][8:])
			return l_company, l_company_annual, l_program, l_program_youth
		else:
			return None, None, None, None

	def target_list_dataframe(self):
		df = DB.pandas_read(sql.sql_columns.value.format('SURVEY'))
		if df is not None:
			self.tl_columns = list(df[df['TABLE_NAME'] == 'Targetlist']['COLUMN_NAME'][1:])
			print(self.tl_columns)

	def save_as_excel(self, dfs, file_name, path_key):
		print(os.getcwd())
		print(len(dfs))
		path = COM.get_config('config.ini', 'box_file_path', path_key)
		box_path = os.path.join(os.path.expanduser("~"), path)
		os.chdir(box_path)
		try:
			writer = pd.ExcelWriter(file_name)
			j = 0
			for df in dfs:
				j += j
				sheet_name = 'SHEET {}'.format(j)
				df.to_excel(writer, sheet_name, index=False)
			writer.save()
		except Exception as ex:
			print(ex)

	def save_as_csv(self, df, file_name, path, sheet_name='SheetI'):
		try:
			os.chdir(path)
			writer = pd.ExcelWriter(file_name)
			df.to_excel(writer, sheet_name, index=False)
			writer.save()
		except Exception as es:
			print(es)

	@staticmethod
	def data_system_source(cv, cvy, cd, cda, path, file_name, datasource):

		cv.insert(0, 'SourceSystem', SS.RICPD_bap.value)
		cv.insert(0, 'DataSource', datasource)
		cv.insert(0, 'Path', path)
		cv.insert(0, 'FileName', file_name)
		cv.insert(0, 'FileID', str(uuid.uuid4()))
		cv.insert(0, 'BatchID', '0')

		cvy.insert(0, 'SourceSystem', SS.RICPDY_bap.value)
		cvy.insert(0, 'DataSource', datasource)
		cvy.insert(0, 'Path', path)
		cvy.insert(0, 'FileName', file_name)
		cvy.insert(0, 'FileID', str(uuid.uuid4()))
		cvy.insert(0, 'BatchID', '0')

		cd.insert(0, 'SourceSystem', SS.RICCD_bap.value)
		cd.insert(0, 'DataSource', datasource)
		cd.insert(0, 'Path', path)
		cd.insert(0, 'FileName', file_name)
		cd.insert(0, 'FileID', str(uuid.uuid4()))
		cd.insert(0, 'BatchID', '0')
		cd.insert(0, 'CompanyID', '0')

		if datasource in [DST.HAL_TECH.value, DST.COMMUNI_TECH.value]:
			cda.insert(0, 'SourceSystem', SS.RICACD_bap.value)
			cda.insert(0, 'DataSource', datasource)
			cda.insert(0, 'Path', path)
			cda.insert(0, 'FileName', file_name)
			cda.insert(0, 'FileID', str(uuid.uuid4()))
			cda.insert(0, 'BatchID', '0')
			cda.insert(0, 'CompanyID', '0')

