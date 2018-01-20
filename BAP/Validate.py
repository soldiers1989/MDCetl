from Shared.enums import FileType, WorkSheet, DataSourceType, SourceSystemType, SQL
from Shared.file_service import FileService as file
from Shared.common import Common
from Shared.db import DB as db
import os
import pandas as pd
import time
import re


class BAPValidate:

	def __init__(self):
		self._path1 = Common.get_config('config.ini', 'box_file_path', 'path_validI')
		self._path2 = Common.get_config('config.ini', 'box_file_path', 'path_validII')
		self.pathQ1 = os.path.join(os.path.expanduser(self._path1))
		self.pathQ2 = os.path.join(os.path.expanduser(self._path2))

		self._path_quarter_one = Common.get_config('config.ini', 'box_file_path', 'path_bap_validation_quarter_one')
		self._path_quarter_two = Common.get_config('config.ini', 'box_file_path', 'path_bap_validation_quarter_two')
		self.path_quarter_one = os.path.join(os.path.expanduser(self._path_quarter_one))
		self.path_quarter_two = os.path.join(os.path.expanduser(self._path_quarter_two))

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
		self.quarter_one_files = []
		self.quarter_two_files = []
		self.dict_list = []

		self.rics = ['launchlab', 'alliance', 'communitech', 'haltech','guelph',  'iion', 'innovationfactory', 'launchlab', 'mars',
					 'niagara', 'noic', 'norcat', 'ottawa', 'ric', 'spark', 'ssmic', 'venturelab', 'wetec']


		self.batch = 'SELECT * FROM Config.ImportBatch WHERE Year = {} AND Quarter = {} ' \
					 'AND DataSourceID = {} AND SourceSystemId = {} AND ImportStatusID = 5'
		self.select = 'SELECT * FROM {} WHERE BatchID = {}'
		self.selectQ1 = '''SELECT  [CompanyName]  AS  [Company Name],[ReferenceID] AS [Reference ID],FormerCompanyName AS 
						[Former / Alternate Names],CRABusinessNumber AS [CRA Business Number],Address1  AS  StreetAddress,City,Province,
						Postalcode,Website,StageName AS Stage,RevenueRange AS [Annual Revenue $CAN],CompanyNumberofEmployees AS 
						[Number of Employees],FundingRaisedToDate AS [Funding Raised to Date $CAN],FundingRaisedCurrentQuarter AS 
						[Funding Raised in Current Quarter $CAN],IncorporationDate AS [Date of Incorporation],IntakeDate AS [Date of Intake],
						[High Potential y/n],[Industry_Sector] AS [Industry Sector],[AdvisoryServicesHrs] AS 
						[Number of advisory service hours provided],[VolunteerMentorHrs] AS [Volunteer mentor hours],
						[Youth] AS [Youth y/n],SocialEnterprise AS [Social Enterprise y/n],Quarter,Year AS [Fiscal Year]
						FROM Config.MaRSMaster WHERE CompanyName is NOT NULL 
						AND CompanyName NOT IN ('Company Name', 'Reference ID', 'Former / Alternate Names', 'CRA Business Number',
						'Street Address', 'City', 'Province', 'Postal Code', 'Website', 'Stage', 'Annual Revenue $CAN', 'Number of Employees',
						'Funding Raised to Date $CAN', 'Funding Raised in Current Quarter $CAN', 'Date of Incorporation', 'Date of Intake',
						'High Potential y/n', 'Industry Sector', 'Number of advisory service hours provided', 'Volunteer mentor hours',
						'Youth y/n', 'Social Enterprise y/n', 'Quarter', 'Fiscal Year') AND BatchID = {}
						'''

	def get_all_rics_data(self):
		valid_df_list = []
		for ric in self.rics:
			print(ric.upper())
			self.Q1CompanyData_sheet = self.read_file_source(self.path_quarter_one, ric)
			self.Q2CompanyData_sheet = self.read_file_source(self.path_quarter_two, ric)

			data_source = Common.set_datasource(ric)
			batch1 = db.pandas_read(self.batch.format(self.year, self.Q1, data_source, SourceSystemType.RICCD_bap.value))['BatchID']
			batch2 = db.pandas_read(self.batch.format(self.year, self.Q2, data_source, SourceSystemType.RICCD_bap.value))['BatchID']

			self.Q1CompanyData = db.pandas_read(self.selectQ1.format(str(batch1[0]) + ' ORDER BY CompanyName'))
			self.Q2CompanyData = db.pandas_read(
				self.select.format('Config.CompanyDataRaw', str(batch2[0]) + ' ORDER BY CompanyName'))

			self.Q1CompanyData_fact_ric = db.pandas_read(self.select.format('Reporting.FactRICCompanyData', batch1[0]))
			self.Q2CompanyData_fact_ric = db.pandas_read(self.select.format('Reporting.FactRICCompanyData', batch2[0]))

			self.Q1CompanyData_rollup = db.pandas_read(SQL.sql_rollup_select.value.format(self.year, 1))
			self.Q2CompanyData_rollup = db.pandas_read(SQL.sql_rollup_select.value.format(self.year, 2))

			valid_df_list.append(self.bap_summary())
		# for df in valid_df_list:
		# 	print(df.head(10))

	def read_file_source(self, path, ric_name):
		os.chdir(path)
		self.source_file = os.listdir(path)
		files_list = [f for f in self.source_file if f[0:2] != '~$' and (
				f[-3:] in FileType.SPREAD_SHEET.value or f[-4:] in FileType.SPREAD_SHEET.value)]
		for file_name in files_list:
			f_name = re.sub('[^A-Za-z0-9]+', '', file_name).lower()
			if ric_name in f_name:
				test = pd.read_excel(file_name, WorkSheet.bap_company_old.value)
				return test

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
		try:
			# NEW CLIENTS
			print('\t New Clients')
			nc = 'Number of New Clients'
			ncq1_sheet = self.Q1CompanyData_sheet[self.Q1CompanyData_sheet['Date of Intake'] > '2017-03-31']
			ncq1_db = self.Q1CompanyData[pd.to_datetime(self.Q1CompanyData['Date of Intake']) > '2017-03-31']
			ncq1_fric = self.Q1CompanyData_fact_ric[self.Q1CompanyData_fact_ric['IntakeDate'] > '2017-03-31']
			x = pd.to_datetime(self.Q1CompanyData_rollup[(self.Q1CompanyData_rollup['IntakeDate'] > '2017-03-31')]['IntakeDate'])
			ncq1_roll_up = x[x > '2017-03-31']
			ncq1_sg = ''
			ncq2_sheet = self.Q2CompanyData_sheet[pd.to_datetime(self.Q2CompanyData_sheet['Date of Intake']) > '2017-06-30']
			ncq2_db = self.Q2CompanyData[self.Q2CompanyData['DateOfIntake'] > '2017-06-30']
			ncq2_fric = self.Q2CompanyData_fact_ric[self.Q2CompanyData_fact_ric['IntakeDate'] > '2017-06-30']
			y = pd.to_datetime(self.Q2CompanyData_rollup[(self.Q2CompanyData_rollup['IntakeDate'] > '2017-06-30')]['IntakeDate'])
			ncq2_roll_up = y[y > '2017-06-30']
			ncq2_sg = ''
			new_clients = self.get_bap_summary([nc, ncq1_sheet, ncq1_db, ncq1_fric, ncq1_roll_up, ncq1_sg, ncq2_sheet, ncq2_db, ncq2_fric, ncq2_roll_up, ncq2_sg])
			self.dict_list.append(new_clients)

			# SOCIAL ENTERPRISES
			print('\t Social Enterprises')
			se = 'Number of Social Enterprises'
			seq1_sheet = self.Q1CompanyData_sheet[self.Q1CompanyData_sheet['Social Enterprise y/n'].astype(str) == 'Y']
			seq1_db = self.Q1CompanyData[self.Q1CompanyData['Social Enterprise y/n'] == 'Y']
			seq1_fric = self.Q1CompanyData_fact_ric[self.Q1CompanyData_fact_ric['SocialEnterprise'] == 'Y']
			seq1_roll_up = self.Q1CompanyData_rollup[(self.Q1CompanyData_rollup['SocialEnterprise'] == 'y')]
			seq1_sg = ''
			seq2_sheet = self.Q2CompanyData_sheet[self.Q2CompanyData_sheet['Social Enterprise y/n'].astype(str) == 'Y']
			seq2_db = self.Q2CompanyData[self.Q2CompanyData['SocialEnterprise'] == 'Y']
			seq2_fric = self.Q2CompanyData_fact_ric[self.Q2CompanyData_fact_ric['SocialEnterprise'] == 'Y']
			seq2_roll_up = self.Q2CompanyData_rollup[(self.Q2CompanyData_rollup['SocialEnterprise'] == 'y')]
			seq2_sg = ''
			social_enterprise = self.get_bap_summary([se, seq1_sheet, seq1_db, seq1_fric, seq1_roll_up, seq2_sg, seq2_sheet, seq2_db, seq2_fric, seq2_roll_up, seq2_sg])
			self.dict_list.append(social_enterprise)

			# CLIENTS WHO GOT HELP
			print('\t Clients who got help')
			ch = 'Number Clients who got help'
			chq1_sheet = self.Q1CompanyData_sheet[(self.Q1CompanyData_sheet['Number of advisory service hours provided'].astype(float) > 0) | (self.Q1CompanyData_sheet['Volunteer mentor hours'] > 0)]
			chq1_db = self.Q1CompanyData[pd.to_numeric(self.Q1CompanyData['Number of advisory service hours provided'] > 0) | (pd.to_numeric(self.Q1CompanyData['Volunteer mentor hours']) > 0)]
			chq1_fric = self.Q1CompanyData_fact_ric[(self.Q1CompanyData_fact_ric['AdvisoryServicesHours'] > 0) | (self.Q1CompanyData_fact_ric['VolunteerMentorHours'] > 0)]
			chq1_roll_up = self.Q1CompanyData_rollup[(self.Q1CompanyData_rollup['AdvisoryHoursYTD'] > 0) | (self.Q1CompanyData_rollup['VolunteerHoursYTD'] > 0)]
			chq1_sg = ''
			chq2_sheet = self.Q2CompanyData_sheet[(self.Q2CompanyData_sheet['Number of advisory service hours provided'].astype(float) > 0) | (self.Q2CompanyData_sheet['Volunteer mentor hours'].astype(float) > 0)]
			chq2_db = self.Q2CompanyData[(pd.to_numeric(self.Q2CompanyData['NumberOfAdvisoryServiceHoursProvided']) > 0) | (pd.to_numeric(self.Q2CompanyData['VolunteerMentorHours'])) > 0]
			chq2_fric = self.Q2CompanyData_fact_ric[(self.Q2CompanyData_fact_ric['AdvisoryServicesHours'] > 0) | (self.Q2CompanyData_fact_ric['VolunteerMentorHours'] > 0)]
			chq2_roll_up = self.Q2CompanyData_rollup[(self.Q2CompanyData_rollup['AdvisoryHoursYTD'] > 0) | (self.Q2CompanyData_rollup['VolunteerHoursYTD'] > 0)]
			chq2_sg = ''
			client_helped =self.get_bap_summary([ch, chq1_sheet, chq1_db, chq1_fric, chq1_roll_up, chq1_sg, chq2_sheet, chq2_db, chq2_fric, chq2_roll_up, chq2_sg])
			self.dict_list.append((client_helped))

			# STAGE 0 - Idea
			print('\t Stage 0 - Idea')
			s0 = 'Stage 0 - Idea'
			s0q1_sheet = chq1_sheet[(chq1_sheet['Stage'] == 'Idea')]
			s0q1_db = chq1_db[(chq1_db['Stage'] == 'Idea')]
			s0q1_fric = chq1_fric[(chq1_fric['Stage'] == 'Idea')]
			s0q1_roll_up = chq1_roll_up[(chq1_roll_up['StageFriendly'] == 'Stage 0 - Idea')]
			s0q1_sg = ''
			s0q2_sheet = chq2_sheet[(chq2_sheet['Stage'] == 'Idea')]
			s0q2_db = chq2_db[(chq2_db['Stage'] == 'Idea')]
			s0q2_fric = chq2_fric[(chq2_fric['Stage'] == 'Idea')]
			s0q2_roll_up = chq2_roll_up[(chq2_roll_up['StageFriendly'] == 'Stage 0 - Idea')]
			s0q2_sg = ''
			stage0 = self.get_bap_summary([s0, s0q1_sheet, s0q1_db, s0q1_fric, s0q1_roll_up, s0q1_sg, s0q2_sheet, s0q2_db, s0q2_fric, s0q2_roll_up, s0q2_sg])
			self.dict_list.append(stage0)
			# STAGE 1 - Discovery
			print('\t Stage 1 - Discovery')
			s1 = 'Stage 1 - Discovery'
			s1q1_sheet = chq1_sheet[(chq1_sheet['Stage'] == 'Discovery')]
			s1q1_db = chq1_db[(chq1_db['Stage'] == 'Discovery')]
			s1q1_fric = chq1_fric[(chq1_fric['Stage'] == 'Discovery')]
			s1q1_roll_up = chq1_roll_up[(chq1_roll_up['StageFriendly'] == 'Stage 1 - Discovery')]
			s1q1_sg = ''
			s1q2_sheet = chq2_sheet[(chq2_sheet['Stage'] == 'Discovery')]
			s1q2_db = chq2_db[(chq2_db['Stage'] == 'Discovery')]
			s1q2_fric = chq2_fric[(chq2_fric['Stage'] == 'Discovery')]
			s1q2_roll_up = chq2_roll_up[(chq2_roll_up['StageFriendly'] == 'Stage 1 - Discovery')]
			s1q2_sg = ''
			stage1 = self.get_bap_summary(
				[s1, s1q1_sheet, s1q1_db, s1q1_fric, s1q1_roll_up, s1q1_sg, s1q2_sheet, s1q2_db, s1q2_fric, s1q2_roll_up,
				 s1q2_sg])
			self.dict_list.append(stage1)
			# STAGE 2 - Validation
			print('\t Stage 2 - Validation')
			s2 = 'Stage 2 - Validation'
			s2q1_sheet = chq1_sheet[(chq1_sheet['Stage'] == 'Validation')]
			s2q1_db = chq1_db[(chq1_db['Stage'] == 'Validation')]
			s2q1_fric = chq1_fric[(chq1_fric['Stage'] == 'Validation')]
			s2q1_roll_up = chq1_roll_up[(chq1_roll_up['StageFriendly'] == 'Stage 2 - Validation')]
			s2q1_sg = ''
			s2q2_sheet = chq2_sheet[(chq2_sheet['Stage'] == 'Validation')]
			s2q2_db = chq2_db[(chq2_db['Stage'] == 'Validation')]
			s2q2_fric = chq2_fric[(chq2_fric['Stage'] == 'Validation')]
			s2q2_roll_up = chq2_roll_up[(chq2_roll_up['StageFriendly'] == 'Stage 2 - Validation')]
			s2q2_sg = ''
			stage2 = self.get_bap_summary(
				[s2, s2q1_sheet, s2q1_db, s2q1_fric, s2q1_roll_up, s2q1_sg, s2q2_sheet, s2q2_db, s2q2_fric, s2q2_roll_up,
				 s2q2_sg])
			self.dict_list.append(stage2)
			# STAGE 3 - Efficiency
			print('\t Stage 3 - Efficiency')
			s3 = 'Stage 3 - Efficiency'
			s3q1_sheet = chq1_sheet[(chq1_sheet['Stage'] == 'Efficiency')]
			s3q1_db = chq1_db[(chq1_db['Stage'] == 'Efficiency')]
			s3q1_fric = chq1_fric[(chq1_fric['Stage'] == 'Efficiency')]
			s3q1_roll_up = chq1_roll_up[(chq1_roll_up['StageFriendly'] == 'Stage 3 - Efficiency')]
			s3q1_sg = ''
			s3q2_sheet = chq2_sheet[(chq2_sheet['Stage'] == 'Efficiency')]
			s3q2_db = chq2_db[(chq2_db['Stage'] == 'Efficiency')]
			s3q2_fric = chq2_fric[(chq2_fric['Stage'] == 'Efficiency')]
			s3q2_roll_up = chq2_roll_up[(chq2_roll_up['StageFriendly'] == 'Stage 3 - Efficiency')]
			s3q2_sg = ''
			stage3 = self.get_bap_summary(
				[s3, s3q1_sheet, s3q1_db, s3q1_fric, s3q1_roll_up, s3q1_sg, s3q2_sheet, s3q2_db, s3q2_fric, s3q2_roll_up,
				 s3q2_sg])
			self.dict_list.append(stage3)
			# STAGE 5 - Scale
			print('\t Stage 4 - Scale')
			s4 = 'Stage 4 - Scale'
			s4q1_sheet = chq1_sheet[(chq1_sheet['Stage'] == 'Scale')]
			s4q1_db = chq1_db[(chq1_db['Stage'] == 'Scale')]
			s4q1_fric = chq1_fric[(chq1_fric['Stage'] == 'Scale')]
			s4q1_roll_up = chq1_roll_up[(chq1_roll_up['StageFriendly'] == 'Stage 4 - Scale')]
			s4q1_sg = ''
			s4q2_sheet = chq2_sheet[(chq2_sheet['Stage'] == 'Scale')]
			s4q2_db = chq2_db[(chq2_db['Stage'] == 'Scale')]
			s4q2_fric = chq2_fric[(chq2_fric['Stage'] == 'Scale')]
			s4q2_roll_up = chq2_roll_up[(chq2_roll_up['StageFriendly'] == 'Stage 4 - Scale')]
			s4q2_sg = ''
			stage4 = self.get_bap_summary(
				[s4, s4q1_sheet, s4q1_db, s4q1_fric, s4q1_roll_up, s4q1_sg, s4q2_sheet, s4q2_db, s4q2_fric, s4q2_roll_up,
				 s4q2_sg])
			self.dict_list.append(stage4)
			return pd.DataFrame(self.dict_list, columns=list(self.dict_list[0].keys()))
		except Exception as ex:
			print(ex)

	def get_bap_summary(self, lst):
		summary = dict()
		summary['Matrics'] = lst[0]
		summary['Spreadsheet - QI'] = len(lst[1])
		summary['DB - QI'] = len(lst[2])
		summary['FactRIC - QI'] = len(lst[3])
		summary['Roll up - QI'] = len(lst[4])
		summary['Schedule G - QI'] = len(lst[5])
		summary['Spreadsheet - QII'] = len(lst[6])
		summary['DB - QII'] = len(lst[7])
		summary['FactRIC - QII'] = len(lst[8])
		summary['Roll up - QII'] = len(lst[9])
		summary['Schedule G - QII'] = len(lst[10])
		return summary


if __name__ == '__main__':
	valid = BAPValidate()
	valid.get_all_rics_data()