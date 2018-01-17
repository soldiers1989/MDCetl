from Shared.enums import FileType, WorkSheet, DataSourceType, SourceSystemType, SQL
from Shared.file_service import FileService as file
from Shared.common import Common
from Shared.db import DB as db
import os
import pandas as pd
import time
import datetime


class BAPValidate:

	def __init__(self):
		self._path1 = Common.get_config('config.ini', 'box_file_path', 'path_validI')
		self._path2 = Common.get_config('config.ini', 'box_file_path', 'path_validII')
		self.pathQ1 = os.path.join(os.path.expanduser(self._path1))
		self.pathQ2 = os.path.join(os.path.expanduser(self._path2))

		self.year = 2018
		self.Q1 = '\'Q1\''
		self.Q2 = '\'Q2\''
		self.Q3 = '\'Q3\''
		self.Q4 = '\'Q4\''

		self.Q1CompanyData_sheet = None
		self.Q2CompanyData_sheet = None

		self.Q1CompanyData = None
		self.Q2CompanyData = None

		self.Q1CompanyData_dc = None
		self.Q2CompanyData_dc = None

		self.Q1CompanyData_fact_ric = None
		self.Q2CompanyData_fact_ric = None

		self.Q1CompanyData_rollup = None
		self.Q2CompanyData_rollup = None

		self.source_file = None
		self.file_list = []

		self.haltech = 'Haltech'
		self.rics = ['spark', 'communitech', 'venturelab', 'haltech', 'iion', 'niagara', 'guelph', 'innovationfactory',
					 'ottawa', 'launchlab', 'mars', 'norcat', 'riccenter', 'ssmic', 'noic', 'wetec', 'alliance']

		self.batch = 'SELECT * FROM Config.ImportBatch WHERE Year = {} AND Quarter = {} ' \
					 'AND DataSourceID = {} AND SourceSystemId = {} AND ImportStatusID = 5'
		self.select = 'SELECT * FROM {} WHERE BatchID = {}'
		self.summary = dict()
		self.selectQ1 = '''SELECT  [CompanyName]  AS  [Company Name],[ReferenceID] AS [Reference ID],FormerCompanyName AS 
						[Former / Alternate Names],CRABusinessNumber AS [CRA Business Number],Address1  AS  StreetAddress,City,Province,
						Postalcode,Website,StageName AS Stage,RevenueRange AS [Annual Revenue $CAN],CompanyNumberofEmployees AS 
						[Number of Employees],FundingRaisedToDate AS [Funding Raised to Date $CAN],FundingRaisedCurrentQuarter AS 
						[Funding Raised in Current Quarter $CAN],IncorporationDate AS [Date of Incorporation],IntakeDate AS [Date of Intake],
						[High Potential y/n],[Industry_Sector] AS [Industry Sector],[AdvisoryServicesHrs] AS 
						[Number of advisory service hours provided],[VolunteerMentorHrs] AS [Volunteer mentor hours],
						[Youth] AS [Youth y/n],SocialEnterprise AS [Social Enterprise y/n],Quarter,Year AS [Fiscal Year]
						FROM Config.MaRSMaster WHERE BatchID = {}'''


		self.get_db_data()
		self.get_sheet_data()

	def read_file_source(self, path, fname):
		os.chdir(path)
		self.source_file = os.listdir(path)
		self.file_list = [f for f in self.source_file if f[0:2] != '~$' and (f[-3:] in FileType.SPREAD_SHEET.value or f[-4:] in FileType.SPREAD_SHEET.value)]
		for file in self.file_list:
			if fname in file:
				test = pd.read_excel(file, WorkSheet.bap_company_old.value)
				return test

	def get_db_data(self):
		batch1 = db.pandas_read(self.batch.format(self.year, self.Q1, DataSourceType.HAL_TECH.value, SourceSystemType.RICCD_bap.value))['BatchID']
		batch2 = db.pandas_read(self.batch.format(self.year, self.Q2, DataSourceType.HAL_TECH.value, SourceSystemType.RICCD_bap.value))['BatchID']

		self.Q1CompanyData = db.pandas_read(self.selectQ1.format(str(batch1[0]) + ' ORDER BY CompanyName'))
		self.Q2CompanyData = db.pandas_read(self.select.format('Config.CompanyDataRaw', str(batch2[0]) + ' ORDER BY CompanyName'))

		self.Q1CompanyData_fact_ric = db.pandas_read(self.select.format('Reporting.FactRICCompanyData', batch1[0]))
		self.Q2CompanyData_fact_ric = db.pandas_read(self.select.format('Reporting.FactRICCompanyData', batch2[0]))

		self.Q1CompanyData_rollup = db.pandas_read(SQL.sql_rollup_select.value.format(self.year, 1))
		self.Q2CompanyData_rollup = db.pandas_read(SQL.sql_rollup_select.value.format(self.year, 2))

	def get_sheet_data(self):
		self.Q1CompanyData_sheet = self.read_file_source(self.pathQ1, 'Haltech Q1-')
		self.Q2CompanyData_sheet = self.read_file_source(self.pathQ2, 'Haltech Q2-')

	def compare_sheet_and_db(self):
		sheet_columns_one = list(map(lambda x: str(x) + '_sheet', self.Q1CompanyData_sheet.columns))
		self.Q1CompanyData_sheet.columns = sheet_columns_one

		db_columns_one = list(map(lambda x: str(x) + '_db', self.Q1CompanyData.columns))
		self.Q1CompanyData.columns = db_columns_one

		df_one = pd.concat([self.Q1CompanyData.sort_values('Company Name_db'),
							self.Q1CompanyData_sheet.sort_values('Company Name_sheet')], axis=1)
		df_one = df_one[sorted(df_one.columns)]

		sheet_columns_two = list(map(lambda x: str(x) + '_sheet', self.Q2CompanyData_sheet.columns))
		self.Q2CompanyData_sheet.columns = sheet_columns_two

		self.Q2CompanyData = self.Q2CompanyData.iloc[:, 10:]
		db_columns_two = list(map(lambda x: str(x) + '_db', self.Q2CompanyData.columns))
		self.Q2CompanyData.columns = db_columns_two

		df_two = pd.concat([self.Q2CompanyData.sort_values('CompanyName_db'),
							self.Q2CompanyData_sheet.sort_values('Company Name_sheet')], axis=1)
		df_two = df_two[sorted(df_two.columns)]

		self.save_to_csv(df_one, df_two)

	def save_to_csv(self, df_one, df_two):
		os.chdir(self.pathQ2)
		writer = pd.ExcelWriter('HT_Sheet_db_comparison_{}.xlsx'.format(str(time.time())[:-8]))
		self.Q1CompanyData.to_excel(writer, 'Quarter 1 from database', index=False)
		self.Q1CompanyData_sheet.to_excel(writer, 'Quarter 1 from sheet', index=False)
		self.Q2CompanyData.to_excel(writer, 'Quarter 2 from database', index=False)
		self.Q2CompanyData_sheet.to_excel(writer, 'Quarter 2 from sheet', index=False)
		df_one.to_excel(writer, 'Quarter 1 Combined', index=False)
		df_two.to_excel(writer, 'Quarter 2 Combined', index=False)
		self.Q1CompanyData_fact_ric.to_excel(writer, 'FactRICCompany_Q1', index=False)
		self.Q2CompanyData_fact_ric.to_excel(writer, 'FactRICCompany_Q2', index=False)
		self.Q1CompanyData_rollup.to_excel(writer, 'Processed_Rollup_Q1', index=False)
		self.Q2CompanyData_rollup.to_excel(writer, 'Processed_Rollup_Q2', index=False)
		writer.save()

	def bap_summary(self):
		# NEW CLIENTS
		nc = 'Number of New Clients'
		ncq1_sheet = len(self.Q1CompanyData_sheet[self.Q1CompanyData_sheet['Date of Intake'] > '2017-03-31'])
		ncq1_db = len(self.Q1CompanyData[self.Q1CompanyData['Date of Intake'] > '2017-03-31'])
		ncq1_fric = len(self.Q1CompanyData_fact_ric[self.Q1CompanyData_fact_ric['IntakeDate'] > '2017-03-31'])
		x = pd.to_datetime(self.Q1CompanyData_rollup[(self.Q1CompanyData_rollup['IntakeDate'] > '2017-03-31') & (self.Q1CompanyData_rollup['FiscalYear'] == 2018) & (self.Q1CompanyData_rollup['FiscalQuarter'] == 1)]['IntakeDate'])
		ncq1_roll_up = len(x[x > '2017-03-31'])
		ncq1_sg = ''
		ncq2_sheet = len(self.Q2CompanyData_sheet[self.Q2CompanyData_sheet['Date of Intake'] > '2017-06-30'])
		ncq2_db = len(self.Q2CompanyData[self.Q2CompanyData['Date of Intake'] > '2017-06-30'])
		ncq2_fric = len(self.Q1CompanyData_fact_ric[self.Q2CompanyData_fact_ric['IntakeDate'] > '2017-06-30'])
		x = pd.to_datetime(self.Q2CompanyData_rollup[(self.Q2CompanyData_rollup['IntakeDate'] > '2017-06-30') & (self.Q2CompanyData_rollup['FiscalYear'] == 2018) & (self.Q2CompanyData_rollup['FiscalQuarter'] == 2)]['IntakeDate'])
		ncq2_roll_up = len(x[x > '2017-06-30'])
		ncq2_sg = ''
		
		# SOCIAL ENTERPRISES
		se = 'Number of New Clients'
		seq1_sheet = len(self.Q1CompanyData_sheet[self.Q1CompanyData_sheet['Social Enterprise y/n'] == 'Y'])
		seq1_db = len(self.Q1CompanyData[self.Q1CompanyData['Social Enterprise y/n'] == 'Y'])
		seq1_fric = len(self.Q1CompanyData_fact_ric[self.Q1CompanyData_fact_ric['SocialEnterprise'] == 'Y'])
		seq1_roll_up = len(self.Q1CompanyData_rollup[(self.Q1CompanyData_rollup['SocialEnterprise'] == 'y') & (self.Q1CompanyData_rollup['FiscalYear'] == 2018) & (self.Q1CompanyData_rollup['FiscalQuarter'] == 1)])
		seq1_sg = ''
		seq2_sheet = len(self.Q2CompanyData_sheet[self.Q2CompanyData_sheet['Social Enterprise y/n'] == 'Y'])
		seq2_db = len(self.Q2CompanyData[self.Q2CompanyData['Social Enterprise y/n'] == 'Y'])
		seq2_fric = len(self.Q2CompanyData_fact_ric[self.Q2CompanyData_fact_ric['SocialEnterprise'] == 'Y'])
		seq2_roll_up = len(self.Q2CompanyData_rollup[(self.Q2CompanyData_rollup['SocialEnterprise'] == 'Y') & (self.Q2CompanyData_rollup['FiscalYear'] == 2018) & (self.Q2CompanyData_rollup['FiscalQuarter'] == 2)])
		seq2_sg = ''

		# CLIENTS WHO GOT HELP
		ch = 'Number of New Clients'
		chq1_sheet = len(self.Q1CompanyData_sheet[self.Q1CompanyData_sheet['Social Enterprise y/n'] == 'Y'])
		chq1_db = len(self.Q1CompanyData[self.Q1CompanyData['Social Enterprise y/n'] == 'Y'])
		chq1_fric = len(self.Q1CompanyData_fact_ric[self.Q1CompanyData_fact_ric['SocialEnterprise'] == 'Y'])
		chq1_roll_up = len(self.Q1CompanyData_rollup[(self.Q1CompanyData_rollup['SocialEnterprise'] == 'y') & (
				self.Q1CompanyData_rollup['FiscalYear'] == 2018) & (self.Q1CompanyData_rollup['FiscalQuarter'] == 1)])
		chq1_sg = ''
		chq2_sheet = len(self.Q2CompanyData_sheet[self.Q2CompanyData_sheet['Social Enterprise y/n'] == 'Y'])
		chq2_db = len(self.Q2CompanyData[self.Q2CompanyData['Date of Intake'] > '2017-06-30'])
		chq2_fric = len(self.Q2CompanyData_fact_ric[self.Q2CompanyData_fact_ric['IntakeDate'] > '2017-06-30'])
		x = pd.to_datetime(self.Q2CompanyData_rollup[(self.Q2CompanyData_rollup['IntakeDate'] > '2017-06-30') & (
				self.Q2CompanyData_rollup['FiscalYear'] == 2018) & (self.Q2CompanyData_rollup['FiscalQuarter'] == 2)][
			                   'IntakeDate'])
		chq2_sg = ''


if __name__ == '__main__':
	bapv = BAPValidate()
	bapv.bap_summary()