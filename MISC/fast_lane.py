import Shared.datasource as ds
import Shared.enums as enum
from Shared.file_service import FileService
from Shared.common import Common as common
import os
import pandas as pd
import datetime as dt


class TargetList(ds.DataSource):
	def __init__(self):
		super().__init__('box_file_path', 'path_other', enum.DataSourceType.DATA_CATALYST)

	def read_misc_file(self):
		self.data = self.file.read_source_file(enum.FileType.SPREAD_SHEET, enum.MDCDataSource.OTHER, enum.Combine.FOR_NONE.value)

	def push_data_to_db(self):
		self.read_misc_file()
		self.data['Venture_basic_name'] = self.data.apply(lambda df: self.common.get_basic_name(df.Venture_name), axis=1)
		# self.data['CompanyID'] = None
		print(os.getcwd())
		# self.file.save_as_csv(self.data, '00 Annual Survey Target List.xlsx', os.getcwd(), 'Target List')
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
		self.common.change_location(enum.PATH.FASTLANE)
		targetlist = self.db.pandas_read(enum.SQL.sql_target_list.value)
		communitech = pd.read_excel('Communitech-portfolio-full-year.xlsx')
		communitech['BasicName'] = communitech.apply(lambda df: self.common.get_basic_name(df['Company Name']), axis=1)
		for _, venture in communitech.iterrows():
			venture['BasicName']
			df = targetlist[targetlist.Venture_basic_name == venture['BasicName']]
			val = dict()
			if len(df) > 0:
				i = i + 1
				val['Communitech Company'] = venture['Company Name']
				val['Target list Company'] = df['Venture_name'].values[0]
				values.append(val)
				print('{} - MATCHED'.format(venture['BasicName']))
			else:
				j= j + 1
				val['Communitech Company'] = venture['Company Name']
				val['Target list Company'] = '-'
				values.append(val)
				print('XXXXXXXX')
		df_match = pd.DataFrame.from_dict(values, orient='columns')
		print(df_match.head())
		print('{} Matches target lsit and {} does not'.format(i, j))
		self.file.save_as_csv(df_match, 'Communitech full portfolio Venture Matchings.xlsx', os.getcwd(), 'Communitech Vs Target list')

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
		self.file.save_as_csv(dfs, 'Targetlist Matching Result.xlsx', os.getcwd(), 'Targetlist DE-dupe')

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
		dfdc = self.db.pandas_read('SELECT CompanyID, CompanyName FROM MaRSDataCatalyst.Reporting.DimCompany')
		dfdc['BasicName'] = dfdc.apply(lambda dfs: common.get_basic_name(dfs.CompanyName), axis=1)
		for i, c in dftl.iterrows():
			dfc = dfdc[dfdc['BasicName'] == c['Venture_basic_name']]
			val = dict()
			if len(dfc) > 0:
				self.db.execute(enum.SQL.sql_target_list_update.value.format(dfc.CompanyID.values[0], c.ID))
			else:
				new_com_id = self.batch.get_table_seed('MaRSDataCatalyst.Reporting.DimCompany', 'CompanyID') + 1
				val['CompanyID'] = new_com_id
				val['Company Name'] = c.Venture_name
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
				values = common.df_list(df)
				self.db.bulk_insert(enum.SQL.sql_dim_company_insert.value, values)
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


if __name__ == '__main__':
	tl = TargetList()
	tl.match_communitech_ventures()
	# tl.push_data_to_db()
	# tl.split_fullname()
	# tl.match_targetlist_ventures()
	# tl.create_target_list_batch()
	# tl.target_list_comapny_id_update()
	# tl.get_invalid_email()
	# tl.get_name_and_email_issue()