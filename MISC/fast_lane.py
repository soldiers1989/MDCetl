import Shared.datasource as ds
import Shared.enums as enum
from Shared.file_service import FileService
from Shared.common import Common as common
import os
import pandas as pd
import datetime as dt

from datetime import datetime
from dateutil import parser

import turtle


class TargetList(ds.DataSource):
	def __init__(self):
		super().__init__('box_file_path', 'path_other', enum.DataSourceType.DATA_CATALYST)

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


class MaRSMetadata(ds.DataSource):

	def __init__(self):
		super().__init__('','', enum.DataSourceType.MDC_SANDBOX_SURVEY)
		self.columns = ['CompanyID', 'MaRSProgram', 'MaRSSector', 'MaRSPriority', 'CAIPStatus',
						'Organization Name', 'Venture Start Date', 'Stage', 'Business Model Tags',
						'Technology Tag', 'Cluster', 'Sub-cluster', 'CAIP Enrolment Date', 'CAIP Graduation Date']

		self.columns_db = ['BatchID', 'CompanyID', 'Program', 'Sector', 'MaRSPriority', 'CAIP',
						'Organization Name','BasicName', 'Venture Start Date', 'RIC_Stage', 'Business Model Tags',
						'Technology Tag', 'Cluster', 'Sub-cluster', 'CAIP Enrolment Date', 'CAIP Graduation Date']

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


if __name__ == '__main__':
	tl = TargetList()
	mm = MaRSMetadata()
	# tl.duplicate_ventures_for_second_pair_of_eye()
	# tl.push_data_to_db()
	# tl.final_mars_csv_tl_comparision()
	# tl.update_targetlist_basic_name()
	# mm.load_mars_metadata()
	# tl.communitech_shared_ventures()
	mm.load_five_missing_marsMetaData()


