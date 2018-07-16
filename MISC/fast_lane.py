import Shared.datasource as ds
import Shared.enums as enum
from Shared.file_service import FileService
from Shared.common import Common as common
import os
import pandas as pd
import datetime as dt

from datetime import datetime
from dateutil import parser
from dateutil.parser import parse

import turtle
import uuid


class TargetList(ds.DataSource):
	def __init__(self):
		super().__init__('', '', enum.DataSourceType.DATA_CATALYST)

		self.column = [ 'BatchID', 'CompanyID', 'Path', 'FileName', 'WorkSheet', 'DataSource', 'Email', 'Invite_First_Name',
						'Invite_Last_Name', 'Venture_Name', 'Venture_basic_name', 'Previous_Venture_Name', 'Date_left_RIC',
						'Number_Of_Founders', 'Number_Of_Youth_Founders', 'Number_Of_Canadian_Born_Founders', 'Number_Of_Female_Founders',
						'Founder_Experience', 'Date_Of_Incorporation', 'Date_founded', 'Industry_Sector', 'Social_Impact', 'Primary_RIC',
						'RIC_organization_name', 'RIC_first_Name', 'RIC_last_Name', 'RIC_person_Title', 'RIC_person_Email', 'OSVP_Status', 'CII', 'Year', 'Status']

	def update_targetlist_basic_name(self):
		self.data = self.db.pandas_read(self.enum.SQL.sql_target_list_basic_name.value)
		for _, r in self.data.iterrows():
			basicname = self.common.update_cb_basic_name(r.Venture_name)
			basicname = self.common.sql_compliant(basicname)
			self.db.execute(self.enum.SQL.sql_target_list_basic_name_update.value.format(basicname, r.ID))
			# print(self.enum.SQL.sql_venture_basic_name_update.value.format(basicname, r.ID))
			print('{}\t\t\t\t\t\t\t\t---->\t\t\t\t\t\t\t\t{}'.format(r.Venture_name, basicname))

	def read_misc_file(self):
		self.data = self.file.read_source_file(enum.FileType.SPREAD_SHEET, enum.MDCDataSource.OTHER, enum.Combine.FOR_NONE.value, current_path='/Users/mnadew/Box Sync/Innovation Economy/Projects/Survey FY18 Planning/Templates/RIC target lists completed/ETL/01 MaRS Updated')

	def push_data_to_db(self):
		# self.read_misc_file()
		path = 'Box Sync/Innovation Economy/Projects/Survey FY18 Planning/Templates/RIC target lists completed/ETL/01 MaRS Updated'
		self.common.change_working_directory(path)
		self.data = pd.read_excel('MaRS 21 target list 20180424.xlsx', sheet_name='Mars21')
		self.data['Venture_basic_name'] = self.data.apply(lambda df: self.common.update_cb_basic_name(df.Venture_Name), axis=1)
		self.data['CompanyID'] = None
		self.data['BatchID'] = 4330
		self.data['Status'] = 1
		self.data['Path'] = path
		self.data['FileName'] = 'MaRS 21 target list 20180424.xlsx'
		self.data['WorkSheet'] = 'Mars21'
		self.data['DataSource'] = 7
		self.data['OSVP_Status'] = None
		self.data['Primary_RIC'] = None
		self.data['CII'] = None
		self.data['Year'] = 2018
		# self.data.drop(columns=['Sector', 'Stage'])
		print(list(self.data.columns))
		self.data = self.data[self.column]
		print(os.getcwd())
		values = self.common.df_list(self.data)
		self.db.bulk_insert(self.enum.SQL.sql_target_list_insert.value, values=values)
		print('Target list uploaded.')

	def split_fullname(self):
		sql = '''
			SELECT *
			FROM SURVEY.Targetlist
			WHERE Targetlist.RIC_first_name IS NULL AND RIC_last_name IS NOT NULL
		'''
		df = self.db.pandas_read(sql)
		for i, r in df.iterrows():
			name = r['RIC_last_name'].split(' ')
			if len(name) == 2:
				print('UPDATE SURVEY.Targetlist SET RIC_first_name = \'{}\', RIC_last_name = \'{}\' WHERE ID = {} -- {}'.format(name[0], name[1], r['ID'], r['RIC_last_name']))

	def last_minute_issue(self):
		path = '/Users/mnadew/Box Sync/WorkBench/BAP/BAP_FY18/FY18_Q3/LMI'
		fs = FileService(path)
		_, _, data = fs.read_source_file(enum.FileType.SPREAD_SHEET.value, enum.MDCDataSource.BAP, enum.Combine.FOR_NONE.value)
		# print(data.head())
		for _, row in data.iterrows():
			update = 'UPDATE BAP.QuarterlyCompanyData SET  Stage = \'{}\' WHERE [Company Name] = \'{}\''.format(row['Stage'], self.common.sql_friendly(row['Company Name']))
			print(update)

	def match_communitech_ventures(self):
		i, j = 0, 0
		values = []
		print(os.getcwd())
		self.common.change_location(enum.PATH.FASTLANE)
		targetlist = self.db.pandas_read(enum.SQL.sql_target_list.value)
		communitech = pd.read_excel('Communitech additional companies.xlsx')
		communitech['BasicName'] = communitech.apply(lambda df: self.common.update_cb_basic_name(df['company']), axis=1)
		for _, venture in communitech.iterrows():
			venture['BasicName']
			df = targetlist[targetlist.Venture_basic_name == venture['BasicName']]
			val = dict()
			if len(df) > 0:
				i = i + 1
				val['Communitech Company'] = venture['company']
				val['Target list Company'] = df['Venture_name'].values[0]
				values.append(val)
				print('{} - MATCHED'.format(venture['BasicName']))
			else:
				j= j + 1
				val['Communitech Company'] = venture['company']
				val['Target list Company'] = '-'
				values.append(val)
				print('XXXXXXXX')
		df_match = pd.DataFrame.from_dict(values, orient='columns')
		print(df_match.head())
		print('{} Matches target lsit and {} does not'.format(i, j))
		self.file.save_as_csv(df_match, 'Communitech additional companies_Matching.xlsx', os.getcwd(), 'Communitech Additional Vs Target list')

	def match_mars_ventures(self):
		i, j = 0, 0
		values = []
		print(os.getcwd())
		self.common.change_location(enum.PATH.MaRS_FIX)
		targetlist = self.db.pandas_read(enum.SQL.sql_target_list.value)
		mars = pd.read_excel('Final FIX- MDD_AnnualSurveyFY18prePopulationTemplate_Final.xlsx')
		mars['BasicName'] = mars.apply(lambda df: self.common.update_cb_basic_name(df['Venture_Name']), axis=1)
		for _, venture in mars.iterrows():
			venture['BasicName']
			df = targetlist[targetlist.Venture_basic_name == venture['BasicName']]
			val = dict()
			if len(df) > 0:
				i = i + 1
				val['MaRS Company'] = venture['Venture_Name']
				val['Target list Company'] = df['Venture_name'].values[0]
				val['DataSource'] = list(df['Datasource'].values)
				values.append(val)
				print('{} - MATCHED'.format(venture['BasicName']))
			else:
				j = j + 1
				val['MaRS Company'] = venture['Venture_Name']
				val['Target list Company'] = '-'
				val['DataSource'] = 7
				values.append(val)
				print('XXXXXXXX')
		df_match = pd.DataFrame.from_dict(values, orient='columns')
		print(df_match.head())
		print('{} Matches target lsit and {} does not'.format(i, j))
		self.file.save_as_csv(df_match, 'MaRS additional companies_Matching.xlsx', os.getcwd(),
							  'MaRS Additional Vs Target list')

	def match_targetlist_ventures(self):
		delimiter = ','
		values = []
		columns = None
		self.common.change_location(enum.PATH.FASTLANE)
		targetlist = self.db.pandas_read(enum.SQL.sql_target_list.value)
		for _, tl in targetlist.iterrows():
			df = targetlist[targetlist.Venture_basic_name == tl['Venture_basic_name']]
			val = dict()
			if df is not None and len(df) > 1:
				val['ID'] = tl['ID']
				val['RIC'] = tl['RIC_organization_name']
				val['Venture_name'] = tl['Venture_name']
				val['Full Name'] = '{} {}'.format(tl['Invite_first_name'], tl['Invite_last_name'])
				val['Email'] = tl['Email']
				Ids = list(df.ID.values)
				rics = list(df.RIC_organization_name.values)
				val['RICs'] = delimiter.join(rics)
				val['IDs'] = delimiter.join(str(i) for i in Ids)
				values.append(val)
				if columns is None:
					columns = val.keys()
				print('{} - MATCHED'.format(val['RICs']))
		# df_match = pd.DataFrame.from_dict(values, orient='columns')
		dfs = pd.DataFrame(values, columns=values[0].keys())
		self.file.save_as_csv(dfs, 'Targetlist Matching Result_NEW.xlsx', os.getcwd(), 'Targetlist DE-dupe')

	def target_list_comapny_match_to_csv(self):
		dftl = self.db.pandas_read('SELECT ID, BatchID, CompanyID,Venture_name, Venture_basic_name FROM SURVEY.Targetlist')
		dfdc = self.db.pandas_read('SELECT CompanyID, CompanyName FROM MaRSDataCatalyst.Reporting.DimCompany')
		dfdc['BasicName'] = dfdc.apply(lambda dfs: common.get_basic_name(dfs.CompanyName), axis=1)
		values = []
		delimiter = ','
		for i, c in dftl.iterrows():
			dfc = dfdc[dfdc['BasicName'] == c['Venture_basic_name']]
			val = dict()
			if len(dfc) > 0:
				self.db.execute(enum.SQL.sql_target_list_update.value.format(dfc.CompanyID.values[0], c.ID))
				val['Batch'] = c.BatchID
				val['CompanyID'] = delimiter.join(str(i) for i in dfc.CompanyID.values)
				val['Company Name'] = delimiter.join(list(dfc.CompanyName.values))
				val['Venture Name'] = c.Venture_name
				val['DC Basic Name'] = delimiter.join(list(dfc.BasicName.values))
				val['TL Basic Name'] = c.Venture_basic_name
				values.append(val)
			else:
				val['Batch'] = c.BatchID
				val['CompanyID'] = None
				val['Company Name'] = None
				val['Venture Name'] = c.Venture_name
				val['DC Basic Name'] = None
				val['TL Basic Name'] = c.Venture_basic_name
				values.append(val)
				self.db.bulk_insert(enum.SQL.sql_dim_company_insert.value, None)
		df = pd.DataFrame(values, columns=values[0].keys())
		os.chdir('./00 Matching')
		self.file.save_as_csv(df, 'Target list Company V Existing Company Matching.xlsx', os.getcwd(), 'Target list Company ID')

	def target_list_comapny_id_update(self):
		dftl = self.db.pandas_read('SELECT ID, BatchID, CompanyID,Venture_name, Venture_basic_name FROM SURVEY.Targetlist WHERE CompanyID IS NULL')
		dfdc = self.db.pandas_read('SELECT ID, [Name], BasicName FROM MDCDW.dbo.DimVenture')
		# dfdc['BasicName'] = dfdc.apply(lambda dfs: common.update_cb_basic_name(dfs.CompanyName), axis=1)
		for i, c in dftl.iterrows():
			dfc = dfdc[dfdc['BasicName'] == c['Venture_basic_name']]
			val = dict()
			if len(dfc) > 0:
				pass
				print(enum.SQL.sql_target_list_update.value.format(dfc.ID.values[0], c.ID))
				self.db.execute(enum.SQL.sql_target_list_update.value.format(dfc.ID.values[0], c.ID))
			else:
				# print('{}\t<---------------------------------NOT MATCHED-------------------------->\t{}'.format(c.Venture_name, c.Venture_basic_name))
				new_com_id = self.batch.get_table_seed('MDCDW.dbo.DimVenture', 'ID') + 1
				val['ID'] = new_com_id
				val['Name'] = c.Venture_name
				val['BasicName'] = c.Venture_basic_name
				val['BatchID'] = c.BatchID
				val['DateFounded'] = None
				val['DateOfIncorporation'] = None
				val['VentureType'] = None
				val['Description'] = None
				val['Website'] = None
				val['Email'] = None
				val['Phone'] = c.BatchID
				val['Fax'] = None
				val['VentureStatus'] = c.BatchID
				val['ModifiedDate'] = str(dt.datetime.utcnow())[:-3]
				val['CreatedDate'] = str(dt.datetime.utcnow())[:-3]
				df = pd.DataFrame([val], columns=val.keys())
				values = common.df_list(df)
				print(enum.SQL.sql_dim_venture_insert.value, values)
				print(enum.SQL.sql_target_list_update.value.format(new_com_id, c.ID))
				self.db.bulk_insert(enum.SQL.sql_dim_venture_insert.value, values)
				self.db.execute(enum.SQL.sql_target_list_update.value.format(new_com_id, c.ID))

	def create_target_list_batch(self):
		print(os.getcwd())
		os.chdir('./00 Matching')
		df = pd.read_excel('Target_List_Batch.xlsx')
		self.batch.create_batch(df)

	def get_invalid_email(self):
		df = self.db.pandas_read('SELECT ID, Invite_first_name + \' \' + Invite_last_name AS Full_Name, Email,Venture_name, RIC_organization_name FROM SURVEY.Targetlist')
		j=0
		values = []
		for i, c in df.iterrows():
			if not self.common.verify_email_exists(c.Email.lower()):
				j = j + 1
				d = dict()
				d['ID'] = c.ID
				d['RIC'] = c.RIC_organization_name
				d['FullName'] = c.Full_Name
				d['Venture'] = c.Venture_name
				d['Email'] = c.Email
				values.append(d)
				print('{}. {}\t\t{}\t\t{}\t\t{}'.format(j, c.RIC_organization_name, c.Full_Name, c.Venture_name, c.Email))
		dfs = pd.DataFrame(values, columns=values[0].keys())
		os.chdir('./00 Matching')
		self.file.save_as_csv(dfs, 'Targetlist with Inactive Email.xlsx', os.getcwd(), 'Targetlist Bad Email')

	def get_name_and_email_issue(self):
		dataset = []
		sql_list = ['SELECT FileName, Email, Invite_first_name, Invite_last_name, Venture_name, RIC_organization_name FROM SURVEY.Targetlist WHERE Email LIKE \'%,%\'',
					'SELECT FileName, Email, Invite_first_name, Invite_last_name, Venture_name, RIC_organization_name FROM SURVEY.Targetlist WHERE Invite_first_name IS NULL AND Invite_last_name IS NULL',
					'SELECT FileName, Email, Invite_first_name, Invite_last_name, Venture_name, RIC_organization_name FROM SURVEY.Targetlist WHERE Targetlist.Invite_last_name IS NULL AND Invite_first_name IS NOT NULL',
					'SELECT FileName, Email, Invite_first_name, Invite_last_name, Venture_name, RIC_organization_name FROM SURVEY.Targetlist WHERE Invite_first_name IS NULL']
		for q in sql_list:
			dataset.append(self.db.pandas_read(q))
		try:
			dfs = pd.concat(dataset)
			dfs = dfs.reset_index(drop=True)
			os.chdir('./00 Matching')
			print(os.getcwd())
			print(dfs.head(10))
			dfs.loc[3]['Email'] = 'ashley@memoryandcomopany.com ||||||||'
			self.file.save_as_csv(dfs, 'Target_list_Name_Email_Issues.xlsx', os.getcwd(), 'Name_and_Email')
		except Exception as ex:
			print(ex)
		print('Done')

	def set_primary_ric_for_target_list(self):
		path = '/Users/mnadew/Box Sync/Workbench/BAP/Annual Survey FY2018/Response Statuses'
		working_dir = common.change_working_directory(path)
		df = pd.read_excel('ResponseStatuses-Mar-16-2018-1134AM.xlsx')
		sql_select = 'SELECT CompanyID, RIC_organization_name FROM MDCRaw.SURVEY.Targetlist WHERE Primary_RIC IS NULL'
		dfi = self.db.pandas_read(sql_select)
		ric = ['Haltech','IION','Innovate Niagara','Innovation Factory','Innovation Guelph','Spark Commercialization and Innovation Centre',
				'Invest Ottawa','Launch Lab','MaRS', 'MaRS Discovery District','NORCAT','NWO Innovation Centre', 'Northwestern Ontario Innovation Centre',
				'Haltech Regional Innovation Centre ','RIC Centre','Sault Ste. Marie Innovation Centre','TechAlliance','ventureLAB','WEtech']
		c_id = list(dfi.CompanyID.values)
		dfs = df[df.primary_RIC.isin(ric)]
		# dfl = dfs[dfs.venture_id.isin(c_id)]
		dframe = dfs[['venture_id', 'company_name', 'primary_RIC','email']]
		# dframe = dfs.loc['venture_id', 'company_name', 'primary_RIC','email']
		data_source = []
		for i, r in dframe.iterrows():
			dsource = common.set_datasource(r['primary_RIC'])
			if dsource is not None:
				data_source.append(int(dsource))
			else:
				data_source.append(None)
		dframe['DataSource'] = data_source
		df = dframe[dframe['DataSource'] > 0]
		try:
			for j, l in df.iterrows():
				company_name = common.sql_friendly(str(l['company_name']))
				sql_update = 'UPDATE MDCRaw.SURVEY.Targetlist SET Primary_RIC = {} WHERE RIC_organization_name LIKE \'{}\' ' \
							 'AND CompanyID = {} AND Venture_name LIKE \'%{}%\' AND Email LIKE \'{}\' '.format(l['DataSource'],l['primary_RIC'], l['venture_id'],company_name, l['email'])
				print(sql_update)
				self.db.execute(sql_update)
		except Exception as ex:
			print(ex, str(l['company_name']))

	def match_mars_final_target_list(self):
		i, j = 0, 0
		values = []
		common.change_working_directory('Box Sync/Innovation Economy/Projects/Survey FY18 Planning/Templates/RIC target lists completed/RIC target list corrections')
		print(os.getcwd())
		targetlist = self.db.pandas_read(enum.SQL.sql_mars_target_list.value)
		mars = pd.read_excel('MDD_AnnualSurveyFY18 target list prePopulationTemplate_Final_20180323.xlsx', )
		mars['BasicName'] = mars.apply(lambda df: self.common.update_cb_basic_name(df['Venture_Name']), axis=1)
		# sql_update = '''UPDATE T SET T.Email = \'{}\', T.Invite_first_name = \'{}\', T.Invite_last_name = \'{}\', T.RIC_first_name = \'{}\', T.RIC_last_name = \'{}\', T.RIC_person_email = \'{}\', T.RIC_person_title = \'{}\' FROM MDCRaw.SURVEY.Targetlist T WHERE T.DataSource = 7 AND T.Venture_basic_name = \'{}\''''
		# for _, ven in mars.iterrows():
		# 	df = targetlist[targetlist.Venture_basic_name == ven['BasicName']]
		# 	if len(df) > 0:
		# 		i = i + 1
		# 		update = sql_update.format(ven.Email, ven.Invite_First_Name, ven.Invite_Last_Name, ven.RIC_first_Name, ven.RIC_last_Name, ven.RIC_person_Email, ven.RIC_person_Title, ven.BasicName)
		# 		self.db.execute(update)
		# 		print('{}. {}'.format(i,update))
		for _, venture in mars.iterrows():
			df = targetlist[targetlist.Venture_basic_name == venture['BasicName']]
			val = dict()
			if len(df) > 0:
				i = i + 1
				val['SS_Company'] = venture['Venture_Name']
				val['DD_Company'] = df['Venture_name'].values[0]
				val['SS_Email'] = venture['Email']
				val['DD_Email'] = df['Email'].values[0]
				val['SS_FirstName'] = venture['Invite_First_Name']
				val['DD_FirstName'] = df['Invite_first_name'].values[0]
				val['SS_LastName'] = venture['Invite_Last_Name']
				val['DD_LastName'] = df['Invite_last_name'].values[0]
				val['SS_RIC_FirstName'] = venture['RIC_first_Name']
				val['DD_RIC_FirstName'] = df['RIC_first_name'].values[0]
				val['SS_RIC_LastName'] = venture['RIC_last_Name']
				val['DD_RIC_LastName'] = df['RIC_last_name'].values[0]
				val['SS_RIC_person_email'] = venture['RIC_person_Email']
				val['DD_RIC_person_email'] = df['RIC_person_email'].values[0]
				val['SS_RIC_person_title'] = venture['RIC_person_Title']
				val['DD_RIC_person_title'] = df['RIC_person_title'].values[0]
				values.append(val)
			else:
				j= j + 1
				val['SS_Company'] = venture['Venture_Name']
				val['DD_Company'] = ''
				val['SS_Email'] = venture['Email']
				val['DD_Email'] = ''
				val['SS_FirstName'] = venture['Invite_First_Name']
				val['DD_FirstName'] = ''
				val['SS_LastName'] = venture['Invite_Last_Name']
				val['DD_LastName'] = ''
				val['SS_RIC_FirstName'] = venture['RIC_first_Name']
				val['DD_RIC_FirstName'] = ''
				val['SS_RIC_LastName'] = venture['RIC_last_Name']
				val['DD_RIC_LastName'] = ''
				val['SS_RIC_person_email'] = venture['RIC_person_Email']
				val['DD_RIC_person_email'] = ''
				val['SS_RIC_person_title'] = venture['RIC_person_Title']
				val['DD_RIC_person_title'] = ''
				values.append(val)
				print('{}. {}'.format(j,venture['Venture_Name']))

		print('{}: Matched\n{}: Not Matched'.format(i,j))
		df_match = pd.DataFrame(values, columns= values[0].keys())
		# new_col = ['SS_Company', 'DD_Company', 'SS_Email', 'DD_Email', 'SS_FirstName', 'DD_FirstName', 'SS_LastName',
		# 		   'DD_LastName','SS_RIC_FirstName', 'DD_RIC_FirstName', 'SS_RIC_LastName', 'DD_RIC_LastName', 'SS_RIC_person_email',
		# 		   'DD_RIC_person_email','SS_RIC_person_title', 'DD_RIC_person_title']
		# df_match.columns = new_col
		print(df_match.head())

		print('{} Matches target lsit and {} does not'.format(i, j))
		self.file.save_as_csv(df_match, 'MDD_Final_Matching.xlsx', os.getcwd(), 'MaRS Final Vs Target list')

	def match_invest_ottawa_final_target_list(self):
		i, j = 0, 0
		values = []
		valuesd = []
		common.change_working_directory('Box Sync/Innovation Economy/Projects/Survey FY18 Planning/Templates/RIC target lists completed/RIC target list corrections')
		print(os.getcwd())
		targetlist = self.db.pandas_read(enum.SQL.sql_invest_ottawa_target_list.value)
		ottawa = pd.read_excel('2018-03-28 InvestOttawa_Amalgamated Survey Recipients.xlsx', )
		ottawa['BasicName'] = ottawa.apply(lambda df: self.common.update_cb_basic_name(df['Venture_Name']), axis=1)
		sql_update = '''UPDATE T SET T.Email = \'{}\', T.Invite_first_name = \'{}\', T.Invite_last_name = \'{}\' FROM MDCRaw.SURVEY.Targetlist T WHERE T.DataSource = 16 AND T.Venture_basic_name = \'{}\''''
		for _, ven in ottawa.iterrows():
			df = targetlist[targetlist.Venture_basic_name == ven['BasicName']]
			if len(df) > 0:
				i = i + 1
				update = sql_update.format(ven.Email, ven.Invite_First_Name, ven.Invite_Last_Name,ven.BasicName)
				self.db.execute(update)
				print('{}. {}'.format(i, update))
		print('----------------Done------------------')

		# for _, venture in ottawa.iterrows():
		# 	df = targetlist[targetlist.Venture_basic_name == venture['BasicName']]
		# 	val = dict()
		# 	if len(df) > 0:
		# 		i = i + 1
		# 		val['SS_Company'] = venture['Venture_Name']
		# 		val['DD_Company'] = df['Venture_name'].values[0]
		# 		val['SS_Email'] = venture['Email']
		# 		val['DD_Email'] = df['Email'].values[0]
		# 		val['SS_FirstName'] = venture['Invite_First_Name']
		# 		val['DD_FirstName'] = df['Invite_first_name'].values[0]
		# 		val['SS_LastName'] = venture['Invite_Last_Name']
		# 		val['DD_LastName'] = df['Invite_last_name'].values[0]
		# 		val['Type'] = 'Existing'
		# 		values.append(val)
		# 	else:
		# 		j= j + 1
		# 		val['SS_Company'] = venture['Venture_Name']
		# 		val['DD_Company'] = ''
		# 		val['SS_Email'] = venture['Email']
		# 		val['DD_Email'] = ''
		# 		val['SS_FirstName'] = venture['Invite_First_Name']
		# 		val['DD_FirstName'] = ''
		# 		val['SS_LastName'] = venture['Invite_Last_Name']
		# 		val['DD_LastName'] = ''
		# 		val['Type'] = 'NEW'
		# 		values.append(val)
		# 		print('{}. {}'.format(j,venture['Venture_Name']))
		#
		# # Target list V Final doc
		#
		# for _, venture in targetlist.iterrows():
		# 	df = ottawa[ottawa.BasicName == venture['Venture_basic_name']]
		# 	vald = dict()
		# 	if len(df) > 0:
		# 		i = i + 1
		# 		vald['DD_Company'] = venture['Venture_name']
		# 		vald['SS_Company'] = df['Venture_Name'].values[0]
		# 		vald['DD_Email'] = venture['Email']
		# 		vald['SS_Email'] = df['Email'].values[0]
		# 		vald['DD_FirstName'] = venture['Invite_first_name']
		# 		vald['SS_FirstName'] = df['Invite_First_Name'].values[0]
		# 		vald['DD_LastName'] = venture['Invite_last_name']
		# 		vald['SS_LastName'] = df['Invite_Last_Name'].values[0]
		# 		vald['Type'] = 'Existing'
		# 		valuesd.append(vald)
		# 	else:
		# 		j = j + 1
		# 		vald['DD_Company'] = venture['Venture_name']
		# 		vald['SS_Company'] = ''
		# 		vald['DD_Email'] = venture['Email']
		# 		vald['SS_Email'] = ''
		# 		vald['DD_FirstName'] = venture['Invite_first_name']
		# 		vald['SS_FirstName'] = ''
		# 		vald['DD_LastName'] = venture['Invite_last_name']
		# 		vald['SS_LastName'] = ''
		# 		vald['Type'] = 'Dropped'
		# 		valuesd.append(vald)
		# 		print('{}. {}'.format(j, venture['Venture_name']))
		#
		# print('{}: Matched\n{}: Not Matched'.format(i,j))
		# df_match = pd.DataFrame(values, columns= values[0].keys())
		# dfd_match = pd.DataFrame(valuesd, columns=valuesd[0].keys())
		# self.file.save_as_csv(df_match, 'INVEST_OTTAWA_Final_Matching.xlsx', os.getcwd(), 'Sheet Vs Target list')
		# self.file.save_as_csv(dfd_match, 'INVEST_OTTAWA_Final_Matching_1.xlsx', os.getcwd(), 'Target list Vs Sheet')
		print('This do two three things')

	def duplicate_ventures_for_second_pair_of_eye(self):
		venture = self.db.pandas_read(enum.SQL.sql_duplicate_venture_select.value)
		venture = venture.set_index(['CompanyID','BasicName'])
		path = self.common.change_working_directory('Box Sync/Workbench/IAF')
		self.file.save_as_csv(venture, 'Duplicate Ventures for QA.xlsx',path, 'Duplicate Ventures')
		print(venture.head(50))

	def final_mars_csv_tl_comparision(self):
		path = 'Box Sync/Innovation Economy/Projects/Survey FY18 Planning/FINAL target lists for QA'
		self.common.change_working_directory(path)
		self.data = pd.read_excel('MDD_AnnualSurveyFY18 target list prePopulationTemplate_Final_20180323.xlsx', 'new-Target List')
		self.data['BasicName'] = self.data.apply(lambda df: self.common.update_cb_basic_name(df.Venture_Name), axis=1)
		df = self.db.pandas_read('SELECT * FROM MDCRaw.SURVEY.Targetlist WHERE DataSource = 7')
		print('Target list: {}\t MaRS Final: {}'.format(len(df), len(self.data)))
		values = []
		for _, r in df.iterrows():
			dfm = self.data[self.data['BasicName'] == r['Venture_basic_name']]
			val = dict()
			if len(dfm) > 0:

				val['M_Company'] = dfm.Venture_Name.values[0]
				val['T_Company'] = r['Venture_name']
				val['M_BasicName'] = dfm.BasicName.values[0]
				val['T_BasicName'] = r['Venture_basic_name']
				values.append(val)
				print(r['Venture_name'])
			else:
				val['M_Company'] = None
				val['T_Company'] = r['Venture_name']
				val['M_BasicName'] = None
				val['T_BasicName'] = r['Venture_basic_name']
				values.append(val)
				print('\t\t\t{}'.format(r['Venture_name']))
		df_result = pd.DataFrame(values, columns=values[0].keys())
		self.file.save_as_csv(df_result, 'MaRS Target List Final v Target List.xlsx', os.getcwd(),
							  'MaRS Vs TL')

	def communitech_shared_ventures(self):
		col = ['CompanyID', 'CompanyName', 'BasicName']
		self.common.change_working_directory(self.enum.FilePath.path_communitech_shared.value)
		df_shared = pd.read_excel('Communitech_DataEnrichment_201802.xlsx', sheet_name='Shared')
		df_unique = pd.read_excel('Communitech_DataEnrichment_201802.xlsx', sheet_name='Unique')
		df_unique.columns = ['CompanyName','BracketName']
		df_unique = df_unique.drop('BracketName', axis=1)
		df_shared.columns = ['CompanyName']
		df = pd.concat([df_unique, df_shared])
		df['BasicName'] = df.apply(lambda dfs: self.common.get_basic_name(dfs['CompanyName']), axis=1)
		df['CompanyID'] = None
		df = df[col]
		values = self.common.df_list(df)
		self.db.bulk_insert(self.enum.SQL.sql_communitech_venture_insert.value,values)
		print(df.head(25))
		print(len(df))

	def fact_ric_company_hours_rollup_ar(self):
		df = self.db.pandas_read('SELECT DISTINCT AnnualRevenue FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp')
		update = 'UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = {} WHERE AnnualRevenue = \'{}\''
		for i, row in df.iterrows():
			if row[0] is not None:
				update_sql= update.format(float(row[0]), row[0])
				print(update_sql)
				self.db.execute(update_sql)

	def fact_ric_company_hours_rollup_fundingToDate(self):
		df = self.db.pandas_read('SELECT DISTINCT FundingToDate FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp')
		update = 'UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET FundingToDate = {} WHERE FundingToDate = \'{}\''
		for i, row in df.iterrows():
			if row[0] is not None:
				update_sql = update.format(float(row[0]), row[0])
				print(update_sql)
				self.db.execute(update_sql)

	def fact_ric_company_hours_rollup_intakeDate(self):
		df = self.db.pandas_read('SELECT DISTINCT TOP 64IntakeDate FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp ORDER BY 1 DESC')
		update = 'UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET IntakeDate = \'{}\' WHERE IntakeDate = \'{}\''
		# dfs = df['IntakeDate'].astype(float)
		for i, row in df.iterrows():
			if row[0] is not None:
				new_date = str(parser.parse(row[0]))[:10] #-- Aug 1 2010 12:00AM
				# new_date = str(parse(row[0]))
				update_sql = update.format(new_date, row[0])
				print(update_sql)
				# self.db.execute(update_sql)

	def fact_ric_company_hours_rollup_float_intakeDate(self):
		update = 'UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET IntakeDate = \'{}\' WHERE IntakeDate = \'{}\''
		intake_dates = [
			[42714, '2016-12-10'],
			[42713, '2016-12-09'],
			[42712, '2016-12-08'],
			[42676.667361, '2016-11-02'],
			[42675.502083, '2016-11-01'],
			[42589, '2016-08-07'],
			[42585.710417, '2016-08-03'],
			[42560, '2016-07-09'],
			[42552.523611, '2016-07-01'],
			[42524.582639, '2016-06-03'],
			[42495.611111, '2016-05-05'],
			[42492.7375, '2016-05-02'],
			[42467.48125, '2016-04-07'],
			[42465.46875, '2016-04-05'],
			[42464.513889, '2016-04-04'],
			[42461.480556, '2016-04-01'],
			[42434.70625, '2016-03-05'],
			[42434.393056, '2016-03-05'],
			[42408, '2016-02-08'],
			[42371.729167, '2016-01-02'],
			[42371.423611, '2016-01-02'],
			[42341.614583, '2015-12-03'],
			[42339.480556, '2015-12-01'],
			[42318.119444, '2015-11-10'],
			[42316.084028, '2015-11-08'],
			[42285.594444, '2015-10-08'],
			[42259.616667, '2015-09-12'],
			[42228.845139, '2015-08-12'],
			[42225.724306, '2015-08-09'],
			[42225.352778, '2015-08-09'],
			[42192.71875, '2015-07-07'],
			[42167.490278, '2015-06-12'],
			[42162.664583, '2015-06-07'],
			[42160.629861, '2015-06-05'],
			[42132.594444, '2015-05-08'],
			[42131.002083, '2015-05-07'],
			[42126.740972, '2015-05-02'],
			[42106.840972, '2015-04-12'],
			[42066.573611, '2015-03-03'],
			[41982.586111, '2014-12-09'],
			[41950.452083, '2014-11-07'],
			[41947.626389, '2014-11-04'],
			[41921.417361, '2014-10-09'],
			[41888.641667, '2014-09-06'],
			[41830.394444, '2014-07-10'],
			[41791.551389, '2014-06-01'],
			[41791.514583, '2014-06-01'],
			[41710.553472, '2014-03-12'],
			[41705.751389, '2014-03-07'],
			[41649.579167, '2014-01-10'],
			[41649.438194, '2014-01-10'],
			[41581.5875, '2013-11-03'],
			[41400.626389, '2013-05-06'],
			[41400.595833, '2013-05-06'],
			[41345.622222, '2013-03-12'],
			[41345.621528, '2013-03-12'],
			[41255.415972, '2012-12-12'],
			[41009.918056, '2012-04-10'],
			[40912.365278, '2012-01-04'],
			[40795.654861, '2011-09-09'],
			[40759.447917, '2011-08-04'],
			[40704.756944, '2011-06-10'],
			[40548.638889, '2011-01-05'],
			[40157.973611, '2009-12-1']]
		for date in intake_dates:
				update_sql = update.format(date[1], date[0])
				print(update_sql)
				self.db.execute(update_sql)

	def communitech_bap_annual_sheet_update(self):
		path = 'Box Sync/mnadew/BAP Related'
		cols = ['id','Inv. Federal',	'Inv. Provincial',	'Inv. VC',	'Inv. Angel',	'Inv. Other']
		self.common.change_working_directory(path)
		self.data = pd.read_excel('Portfolio BAP-Q3-Rob-Pulled-July10-2018.xlsx', 'Sheet1')
		self.data = self.data[cols]
		self.data = self.data.where(pd.notnull(self.data), None)
		print(self.data.head(5))
		print(len(self.data))
		print('....')
		self.data.columns = ['Company','Federal', 'Provincial', 'VC', 'Angel', 'Other']
		print(self.data.head(5))
		for j, row in self.data.iterrows():
			print(row['Company'])
			if row['Federal'] is not None:
				print('Federal: {}'.format(row['Federal']))
			if row['Provincial'] is not None:
				print('Provincial: {}'.format(row['Provincial']))
			if row['VC'] is not None:
				print('VC: {}'.format(row['VC']))
			if row['Angel'] is not None:
				print('Angel: {}'.format(row['Angel']))
			if row['Other'] is not None:
				print('Other: {}'.format(row['Other']))
			print('-------------------------------------')

	def get_comm_company_id(self, company_name, df):
		company_id = df[df['Reference ID'] == company_name]['CompanyID'].values[0]
		if company_id is not None:
			return company_id
		else:
			print(company_name)


	def communitech_bap_annual_reupload(self):
		path = 'Box Sync/Workbench/BAP/BAP_FY18/FY18_Q3/Communitech_New_Annual_Data_20180712'
		self.common.change_working_directory(path)
		self.data = pd.read_excel('Portfolio BAP-Q3-Annual-Tab-Rob-Pulled-July10-2018.xlsx', 'Sheet1')
		self.data['CompanyID'] = None
		self.data['QuarterCollected'] = 3
		self.data['Year'] = 2018
		self.data['BatchID'] = 3918
		self.data['FileID'] = str(uuid.uuid4())
		self.data['FileName'] = 'Portfolio BAP-Q3-Annual-Tab-Rob-Pulled-July10-2018.xlsx'
		self.data['Path'] = path
		self.data['DataSource'] = 4
		self.data['SourceSystem'] = 49
		self.data['Reference ID'] = self.data['id']
		self.data['Company Name'] = self.data['id']
		df = self.db.pandas_read('SELECT CompanyID, [Reference ID] FROM MDCRaw.BAP.AnnualCompanyData WHERE DataSource = 4')
		print(self.data.columns)
		cols = ['CompanyID', 'BatchID','FileID','FileName','Path','DataSource','SourceSystem','Reference ID','Company Name',  'Website', 'Revenue?', 'Revenue Can', 'Revenue Int',
				'Full Time Start Cal Year', 'Full Time End Cal Year', 'Part Time Start Cal Year', 'Part Time End Cal Year', 'Total Payroll',
				'Inv. Federal', 'Inv. Provincial', 'Inv. VC', 'Inv. Angel', 'Inv. Other', 'QuarterCollected','Year']
		self.data = self.data[cols]
		print(self.data.head(25))
		# self.file.save_as_csv(self.data, 'Communitech Annual Reupload Generated.xlsx', os.getcwd(),
		# 			  'Communitech Annual Reupload')
		# i = 0
		# print('id,name,companyid')
		# for j, row in self.data.iterrows():
		# 	i=i+1
		# 	if len(df[df['Reference ID'] == row['Company Name']]) >= 1:
		# 		companyID = df[df['Reference ID'] == row['Company Name']]['CompanyID'].values[0]
		# 		if companyID is not None:
		# 			print('{},{},{}'.format(i,row['Company Name'], companyID))
		# 		else:
		# 			print('{},MissingI,{}'.format(i,row['Company Name']))
		# 	else:
		# 		print('{},MissingII,{}'.format(i,row['Company Name']))


		values = self.common.df_list(self.data)
		self.db.bulk_insert(enum.SQL.sql_bap_annual_insert.value, values)

	def update_communitech_reupload_companyID(self):
		path = 'Box Sync/Workbench/BAP/BAP_FY18/FY18_Q3/Communitech_New_Annual_Data_20180712'
		self.common.change_working_directory(path)
		self.data = pd.read_csv('Communitech_Annual_Company_IDs.csv')
		print(self.data.head(10))
		print('Done')
		update = 'UPDATE MDCRaw.BAP.AnnualCompanyData SET CompanyID = {} WHERE [Company Name] = \'{}\''
		i = 0
		for _, row in self.data.iterrows():
			if 'Missing' not in row['name']:
				i+=1
				print(update.format(row['companyid'], row['name']))
				self.db.execute(update.format(row['companyid'], row['name']))

	def communitech_bap_annual_reupload_funding(self):
		path = 'Box Sync/Workbench/BAP/BAP_FY18/FY18_Q3/Communitech_New_Annual_Data_20180712'
		self.common.change_working_directory(path)
		self.data = pd.read_excel('Portfolio BAP-Q3-Rob-Pulled-July10-2018.xlsx', 'Sheet1')
		cols = ['Company Name', 'id', 'Alternate', 'CRA Number', 'Street', 'City', 'Province', 'Postal',
				'Website', 'Stage', 'Revenue (new clients)', 'Employees (new clients)',
				'Funding (to date - new clients)', 'Incorporated', 'Inc Month', 'Inc Year',
				'Founders', 'Founders Youth', 'Founders Canadian Born', 'Founders First Venture',
				'Founders Female', 'HiPo', 'Intake Date', 'Industry', 'Advisory Hours', 'Volunteer Hours',
				'Youth', 'Social', 'Inv. Raised?', 'Seeking Amount',
				'Inv. Federal', 'Inv. Provincial', 'Inv. VC', 'Inv. Angel', 'Inv. Other', 'Valuation']
		new_cols = ['id','Inv. Federal', 'Inv. Provincial', 'Inv. VC', 'Inv. Angel', 'Inv. Other']
		self.data = self.data[new_cols]
		readable_cols = ['Name', 'Federal', 'Provincial', 'VC', 'Angel', 'Other']
		self.data.columns = readable_cols
		self.data = self.data.where(pd.notnull(self.data), None)
		print(self.data.head(5))
		for _, row in self.data.iterrows():
			if row['Federal'] is not None:
				update = 'UPDATE MDCRaw.BAP.AnnualCompanyData SET FederalInvestment = {} WHERE [Company Name] LIKE \'{}\''
				# self.db.execute(update.format(row['Federal'], row['Name']))
				print(update.format(float(row['Federal']), row['Name']))
			if row['Provincial'] is not None:
				update = 'UPDATE MDCRaw.BAP.AnnualCompanyData SET ProvincialInvestment = {} WHERE [Company Name] LIKE \'{}\''
				# self.db.execute(update.format(row['Provincial'], row['Name']))
				print(update.format(float(row['Provincial']), row['Name']))
			if row['VC'] is not None:
				update = 'UPDATE MDCRaw.BAP.AnnualCompanyData SET VCInvestment = {} WHERE [Company Name] LIKE \'{}\''
				# self.db.execute(update.format(row['VC'], row['Name']))
				print(update.format(float(row['VC']), row['Name']))
			if row['Angel'] is not None:
				update = 'UPDATE MDCRaw.BAP.AnnualCompanyData SET AngelInvestment = {} WHERE [Company Name] LIKE \'{}\''
				# self.db.execute(update.format(row['Angel'], row['Name']))
				print(update.format(float(row['Angel']), row['Name']))
			if row['Other'] is not None:
				update = 'UPDATE MDCRaw.BAP.AnnualCompanyData SET OtherInvestment = {} WHERE [Company Name] LIKE \'{}\''
				# self.db.execute(update.format(row['Other'], row['Name']))
				print(update.format(float(row['Other']), row['Name']))

	def re_insert_old_communitech_annual_reupload(self):
		try:
			path = 'Box Sync/Workbench/BAP/BAP_FY18/FY18_Q3/CommunitechAnnual_OLD_DB'
			self.common.change_working_directory(path)
			self.data = pd.read_csv('Communitech_Annual_from_db.csv')
		except Exception as ex:
			print(ex)
		cols =  ['ID','CompanyID',                                               'BatchID',
				 'FileID',                                                  'FileName',
				 'Path',                                                    'DataSource',
				 'SourceSystem',                                            '[Company Name]',
				 '[Reference ID]',                                        'Website',
				 '[Was there sales revenue generated this calendar year?]', '[Sales revenue from Canadian sources $CAN]',
				 '[Sales revenue from international sources $CAN]',        '[Full-time employees at start of calendar year]',
				 '[Full-time employees at end of calendar year]',         '[Part-time employees at start of year]',
				 '[Part-time employees at end of year]',                  '[Total payroll for calendar year $CAN]',
				 'FederalInvestment',                                      'ProvincialInvestment',
				 'VCInvestment',                                           'AngelInvestment',
				 'OtherInvestment',                                        'QuarterCollected',
				 'Year' ]
		self.data.columns = cols
		val=[]
		df = self.db.pandas_read(
			'SELECT ID, [Company Name] FROM MDCRaw.BAP.AnnualCompanyData WHERE DataSource = 4')
		for _, row in self.data.iterrows():
			if len(df[df['Company Name'] == row['[Company Name]']]) == 0:
				for i, j in enumerate(row.values):
					if str(j) == 'nan':
						row[i] = None
				val.append(list(row.values[1:]))
				print(list(row.values[1:]))
		print('..'*280)
		print(len(val[0]))
		self.db.bulk_insert(enum.SQL.sql_bap_annual_insert.value, val)


class MaRS(ds.DataSource):

	def __init__(self):
		super().__init__('','', enum.DataSourceType.MDC_SANDBOX_SURVEY)
		self.columns = ['CompanyID', 'MaRSProgram', 'MaRSSector', 'MaRSPriority', 'CAIPStatus',
						'Organization Name', 'Venture Start Date', 'Stage', 'Business Model Tags',
						'Technology Tag', 'Cluster', 'Sub-cluster', 'CAIP Enrolment Date', 'CAIP Graduation Date']

		self.columns_db = ['BatchID', 'CompanyID', 'Program', 'Sector', 'MaRSPriority', 'CAIP',
						'Organization Name','BasicName', 'Venture Start Date', 'RIC_Stage', 'Business Model Tags',
						'Technology Tag', 'Cluster', 'Sub-cluster', 'CAIP Enrolment Date', 'CAIP Graduation Date']

		self.supplemental_columns = ['Batch', 'CompanyID', 'Company Name', 'Date Submitted',
									 'Funding Federal Government $CAN', 'Funding Provincial Government $CAN',
									 'Funding Venture Capital $CAN', 'Funding Angel $CAN', 'Funding Private Other $CAN',
									 'Funding Other - not Private $CAN',
									 'Sales revenue from Canadian sources $CAN', 'Sales revenue from USA $CAN',
									 'Sales revenue from international sources $CAN', 'Full-time employees at start of calendar year',
									 'Full-time employees at end of calendar year', 'Part-time employees at start of year',
									 'Part-time employees at end of year', 'Total payroll for calendar year $CAN', 'Year']

	def load_mars_metadata(self):
		self.common.change_working_directory(enum.FilePath.path_mars_metadata.value)
		self.data = pd.read_excel('MaRS-venture-categorizations.xlsx', sheet_name='Sheet2')
		print(list(self.data.columns))
		print(self.data[['MaRSProgram','MaRSSector','CAIPStatus']])


		self.data['BatchID'] = None # Generate the batch here and assign it.
		self.data['Program'] = self.data.apply(lambda df: self.common.get_mars_program(df.MaRSProgram), axis=1)
		self.data['Sector'] = self.data.apply(lambda df: self.common.get_mars_sector(df.MaRSSector), axis=1)
		self.data['CAIP'] = self.data.apply(lambda df: self.common.get_caip_status(df.CAIPStatus), axis=1)
		self.data['RIC_Stage'] = self.data.apply(lambda df: self.common.get_stage_level(df.Stage), axis=1)
		self.data['BasicName'] = self.data.apply(lambda df: self.common.get_basic_name(df['Organization Name']), axis=1)

		self.data = self.data[self.columns_db]
		values = self.common.df_list(self.data)
		self.db.bulk_insert(self.enum.SQL.sql_mars_meta_data_insert.value, values)

		print(self.data.head())

	def load_five_missing_marsMetaData(self):
		self.common.change_working_directory(enum.FilePath.path_mars_metadata.value)
		self.data = pd.read_excel('MaRS-venture-categorizations.xlsx', sheet_name='FiveMissingVentures')

		self.data['BatchID'] = 3865
		self.data['CompanyID'] = None
		self.data['Program'] = self.data.apply(lambda df: self.common.get_mars_program(df.MaRSProgram), axis=1)
		self.data['Sector'] = self.data.apply(lambda df: self.common.get_mars_sector(df.Sector), axis=1)
		self.data['CAIP'] = self.data.apply(lambda df: self.common.get_caip_status(df.CAIPStatus), axis=1)
		self.data['RIC_Stage'] = self.data.apply(lambda df: self.common.get_stage_level(df.Stage), axis=1)
		self.data['BasicName'] = self.data.apply(lambda df: self.common.get_basic_name(df['Organization Name']), axis=1)

		self.data = self.data[self.columns_db]
		values = self.common.df_list(self.data)

		self.db.bulk_insert(self.enum.SQL.sql_mars_meta_data_insert.value, values)
		print(self.data.head())

	def load_mars_supplemental(self):
		self.common.change_working_directory(enum.FilePath.path_mars_supplemental.value)
		self.data = pd.read_excel('MaRS_Additional_Venture_Data_2018-05-11.xlsx', sheet_name='ReadyForETL')
		self.data['Batch'] = 3912
		self.data['Year'] = 2017
		self.data = self.data[self.supplemental_columns]
		print(self.data.head(6))
		values = self.common.df_list(self.data)
		self.db.bulk_insert(enum.SQL.sql_mars_supplemental_insert.value, values)


if __name__ == '__main__':
	tl = TargetList()
	# mm = MaRS()
	# tl.duplicate_ventures_for_second_pair_of_eye()
	# tl.push_data_to_db()
	# tl.final_mars_csv_tl_comparision()
	# tl.update_targetlist_basic_name()
	# mm.load_mars_metadata()
	# tl.communitech_shared_ventures()
	# mm.load_five_missing_marsMetaData()
	# mm.load_mars_supplemental()
	# tl.fact_ric_company_hours_rollup()
	# tl.fact_ric_company_hours_rollup_fundingToDate()
	# tl.fact_ric_company_hours_rollup_float_intakeDate()
	# tl.communitech_bap_annual_sheet_update()
	# tl.communitech_bap_annual_reupload()
	# tl.communitech_bap_annual_reupload_funding()
	tl.re_insert_old_communitech_annual_reupload()

