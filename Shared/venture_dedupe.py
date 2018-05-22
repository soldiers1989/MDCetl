from Shared.datasource import DataSource
import Shared.enums as enum
import pandas as pd
import os
from datetime import datetime


class VentureDedupe(DataSource):

	def __init__(self):
		super().__init__('box_file_path', 'path_iaf', enum.DataSourceType.IAF)
		self.venture_col = ['ID', 'Name', 'BasicName', 'BatchID', 'DateFounded', 'DateOfIncorporation', 'VentureType', 'Description',
							'Website', 'Email', 'Phone', 'Fax', 'VentureStatus', 'ModifiedDate', 'CreateDate']

	def update_ventures_basic_name_new(self):
		self.data = self.db.pandas_read(self.enum.SQL.sql_update_ventures_basic_name.value)
		for _, r in self.data.iterrows():
			basicName = self.common.get_basic_name(r.Name)
			print('{}\t\t{} ---> {}'.format(r.Name, r.BasicName, basicName))

	def get_verified_duplicate(self):
		self.common.change_working_directory(self.enum.FilePath.path_venture_dedupe.value)
		self.data = pd.read_excel('Duplicate Ventures for QA.xlsx')
		self.data = self.data[self.data.Comment == 'OK to merge']
		for _, dt in self.data.iterrows():
			update = enum.SQL.sql_dbo_duplicate_venture_update.value.format(int(dt.CompanyID))
			self.db.execute(update)
			print('*' * int(dt.CompanyID/10000), update)

	def update_target_list(self):
		tl = self.db.pandas_read('')
		print(tl)

	def inset_new_ventures(self):
		# df = self.db.pandas_read(self.enum.SQL.sql_cvca_exits_new_ventures.value) # CVCA Exits
		# df = self.db.pandas_read(self.enum.SQL.sql_cvca_deals_new_ventures.value)  # CVCA Deals
		# df = self.db.pandas_read(self.enum.SQL.sql_iaf_new_ventures.value)  # IAF
		# df = self.db.pandas_read(self.enum.SQL.sql_cb_new_ventures.value)  # CB
		df = self.db.pandas_read(self.enum.SQL.sql_marsmetadata_new_ventures.value)  # MaRSMetadata
		v_id = self.db.get_table_seed('MDCRaw.dbo.Venture', 'ID')
		ven_list = []
		for _, r in df.iterrows():
			value = dict()
			v_id = v_id + 1
			value['ID'] = v_id
			value['Name'] = r[0]
			value['BasicName'] = r[1]
			value['BatchID'] = r[2] # 3865
			for i in range(11):
				i = i+4
				value[self.venture_col[i]] = None
			ven_list.append(value)
		dfi = pd.DataFrame(ven_list, columns=self.venture_col)
		print(dfi.head(20))
		values = self.common.df_list(dfi)
		print(len(values))
		self.db.bulk_insert(self.enum.SQL.sql_venture_insert.value, values)

	def update_ventures_basic_name(self):
		self.data = self.db.pandas_read(self.enum.SQL.sql_venture_basic_name.value)
		for _, r in self.data.iterrows():
			basicname = self.common.sql_compliant(self.common.get_basic_name(r.Name))
			self.db.execute(self.enum.SQL.sql_venture_basic_name_update.value.format(basicname, r.ID))
			# print(self.enum.SQL.sql_venture_basic_name_update.value.format(basicname, r.ID))
			print('{}\t\t\t\t\t\t\t\t---->\t\t\t\t\t\t\t\t{}'.format(r.Name, basicname))

	def duplicate_venture_table_processing(self):
		path = 'Box Sync/mnadew/IE/Data/Ventures'
		file_name = 'Duplicate Venture Former All Dupes_20180504.xlsx'
		self.common.change_working_directory(self.enum.FilePath.path_venture_dedupe.value)
		print(os.getcwd())
		# venture = self.db.pandas_read(enum.SQL.sql_duplicate_venture_list.value)
		venture = self.db.pandas_read(enum.SQL.sql_duplicate_ventures_with_former_name.value)
		distinct_venture = list(venture.BasicName.unique())
		db_values = []
		values_II = []
		values_III = []
		values_IV = []
		values_V = []
		for v in distinct_venture:
			df = venture[venture.BasicName == v]
			ID = df.ID.values
			Name = df.Name.values
			BasicName = df.BasicName.values
			m = len(df)
			# print('({}){}\t({}){}'.format(ID[0],Name[0],ID[1],Name[1]))
			for i in range(len(df)):

				val = dict()
				for j in range(len(df)):
					val['ID-{}'.format(j)] = ID[j]
					val['Venture Name-{}'.format(j)] = Name[j]
					val['Basic Name-{}'.format(j)] = BasicName[j]
					l = j + 1
					db = dict()
					if l < len(df):
						db['CompanyID'] = ID[0]
						db['DuplicaeCompanyID'] = ID[l]
						db['Name'] = Name[0]
						db['DuplicateName'] = Name[l]
						db['BasicName'] = BasicName[0]
						if db not in db_values:
							db_values.append(db)
				# print(val)
				keys_len = len(val.keys())
				if keys_len == 6:
					if val not in values_II:
						values_II.append(val)
				elif keys_len == 9:
					if val not in values_III:
						values_III.append(val)
				elif keys_len == 12:
					if val not in values_IV:
						values_IV.append(val)
				else:
					if val not in values_V:
						values_V.append(val)
		df_db = pd.DataFrame(db_values, columns=db_values[0].keys())
		df_db['CreatedDate'] = datetime.utcnow()
		df_db['ModifiedDate'] = datetime.utcnow()
		df_db['Deduped'] = 2
		df_db['Verified'] = 0
		values = self.common.df_list(df_db)
		self.db.bulk_insert(enum.SQL.sql_duplicate_venture_insert.value, values)
		print(df_db.head(25))
		# try:
		# 	writer = pd.ExcelWriter(file_name)
		# 	# dfV2 = pd.DataFrame(values_II, columns=values_II[0].keys())
		# 	# dfV3 = pd.DataFrame(values_III, columns=values_III[0].keys())
		# 	# dfV4 = pd.DataFrame(values_IV, columns=values_IV[0].keys())
		# 	# dfVx = pd.DataFrame(values_V, columns=values_V[0].keys())
		# 	#
		# 	# dfV2.to_excel(writer, sheet_name='Two Duplicates', index=False)
		# 	# dfV3.to_excel(writer, sheet_name='Three Duplicates', index=False)
		# 	# dfV4.to_excel(writer, sheet_name='Four Duplicates', index=False)
		# 	# dfVx.to_excel(writer, sheet_name='More than four Duplicates', index=False)
		# 	df_db.to_excel(writer, index=False)
		#
		# 	writer.save()
		# except Exception as ex:
		# 	print(ex)
		self.file.save_as_csv(df_db, file_name, os.getcwd(), 'Duplicates after Former names')
		print('Duplicate files created for ventures.')

	def duplicate_venture_insert(self):
		# vlist = [48267, 302113,22724, 302757,7810, 303224,22863, 24423,4832, 24272,1367, 303259,24779,
		# 		 302557,11439, 21359,1494, 303275,20806, 22230,49338, 49473,47303, 302848,1578, 22087,
		# 		 24812, 304013,60371, 304228,17748, 23157,1621, 304042,22378, 302230,13588, 21329,13731,
		# 		 303196,6810, 48529,7136, 50043,302181, 303443,24819, 303298,11582, 26310,1224, 302868,
		# 		 10069, 302906,22641, 22764,10436, 304233,47348, 303986,1771, 302678,7098, 22585,48755,
		# 		 304110,16032, 303305,1822, 304230,26369, 49515,49470, 302591,7116, 22132,1395, 302689,19023, 21053]
		# vtups = [(48267, 302113), (22724, 302757), (7810, 303224), (22863, 24423), (4832, 24272), (1367, 303259),
		# 		 (24779, 302557), (11439, 21359), (1494, 303275), (20806, 22230), (49338, 49473), (47303, 302848), (1578, 22087),
		# 		 (24812, 304013), (60371, 304228), (17748, 23157), (1621, 304042), (22378, 302230), (13588, 21329), (13731, 303196),
		# 		 (6810, 48529), (7136, 50043), (302181, 303443), (24819, 303298), (11582, 26310), (1224, 302868), (10069, 302906),
		# 		 (22641, 22764), (10436, 304233), (47348, 303986), (1771, 302678), (7098, 22585), (48755, 304110), (16032, 303305),
		# 		 (1822, 304230), (26369, 49515), (49470, 302591), (7116, 22132), (1395, 302689), (19023, 21053)]

		# vlist = [1319 , 9556,303163 , 303251,20362 , 304016,7134 , 49549,6999 , 302654,48271 , 22388,46189, 4832]
		# vtups = [(1319 , 9556),(303163 , 303251),(20362 , 304016),(7134 , 49549),(6999 , 302654),(48271 , 22388),(46189, 4832)]

		vlist = [850,302754,24797,303215,9981,54765,9583,22611,
				 22868,26299,49474,302897,23128,302289,1497,24448,
				 24870,301772,48048,49386,17744,22373,20529,302127,304153,304119]
		vtups = [(850, 302754),(24797, 303215),(9981, 54765),(9583, 22611),
				 (22868, 26299),(49474, 302897),(23128, 302289),(1497, 24448),
				 (24870, 301772),(48048, 49386),(17744, 22373),(20529, 302127),(304119,304153)]

		for i in range(len(vtups)):
			sql_statement = 'SELECT ID, Name, AlternateName, BasicName FROM MDCRaw.dbo.Venture WHERE ID IN {} ORDER BY ID'.format(str(vtups[i]))
			print(sql_statement)
			self.data = self.db.pandas_read(sql_statement)
			venture = self.data[self.data['ID'] == vtups[i][0]]
			dventure = self.data[self.data['ID'] == vtups[i][1]]
			#(CompanyID, DuplicateCompanyID, Name, DuplicateName, BasicName, ModifiedDate, CreateDate, Deduped, Verified)
			insert_sql = 'INSERT INTO MDCRaw.CONFIG.DuplicateVenture VALUES ({},{},\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',{},{})'.format(
				venture.ID.values[0], dventure.ID.values[0], venture.Name.values[0], dventure.Name.values[0], venture.BasicName.values[0],
				str(datetime.utcnow())[:23], str(datetime.utcnow())[:23], 0, 1)
			# print(insert_sql)
			self.db.execute(insert_sql)

	def venture_with_former_name(self):
		# self.common.change_working_directory(self.enum.FilePath.path_venture_dedupe.value)


		self.data = self.db.pandas_read(self.enum.SQL.sql_venture_former_name.value)
		val = []
		for _, r in self.data.iterrows():
			a_name = self.common.sql_compliant('[' + r.Name +' | ' + r.BasicName +']')
			sname = r.Name.split(sep='(')
			usql = self.enum.SQL.sql_venture_other_name_update.value.format(self.common.sql_compliant(sname[0]),
																			self.common.sql_compliant(self.common.get_basic_name(sname[0])),
																			self.common.sql_compliant(a_name),
																			r.ID)
			self.db.execute(usql)
			print(usql)
			# d = dict() '\[(.*?)\]'
			# d['ID'] = r.ID
			# d['Name'] = sname[0]
			# d['AdditionalName'] = sname[1].replace(')', '')
			# d['BasicName'] = r.BasicName
			# d['NewBasicName'] = self.common.get_basic_name(sname[0])
			# val.append(d)
			# df = pd.DataFrame(val, columns=val[0].keys())
			# self.file.save_as_csv(df, 'Venrure with multiple name.xlsx', os.getcwd(), 'Ventures with Multiple name')

	def ventures_with_double_name(self):
		self.data = self.db.pandas_read(self.enum.SQL.sql_marsmetadata_double_name.value)
		for _, r in self.data.iterrows():
			sname = r.VentureName.split(sep='(')
			usql = self.enum.SQL.sql_marsmetadata_double_name_update.value.format(self.common.sql_compliant(sname[0]),
									 self.common.sql_compliant(self.common.get_basic_name(sname[0])),
									 r.ID)
			self.db.execute(usql)
			print(usql)

	def survey_previous_name_for_ventures(self):
		self.common.change_working_directory(self.enum.FilePath.path_venture_dedupe.value)
		cols = ['PreviousName','Similar_Pre','ConfirmName','Similar_Confirm', 'Name', 'IssueFound']
		self.data = pd.read_excel('Venture_Previous_Name_From_Survey.xlsx')
		temp = ['PreviousName', 'Other companies in dbo.Venture with names similar to PreviousName: ',
				'ConfirmName', 'Other companies in dbo.Venture with names similar to ConfirmName: ',
				'Name', 'Issue Found?']
		self.data = self.data[temp]
		self.data.columns = cols
		print(self.data.columns)
		print(self.data.head(25))

	def update_tdw_basic_name(self):
		self.db.update_basic_name(self.enum.SQL.sql_tdw_basic_company.value,
								  'org_uuid',
								  'name',
								  self.enum.SQL.sql_tdw_basic_company_update.balue)

	def update_cbInsight_basic_name(self):
		self.db.update_basic_name(self.enum.SQL.sql_cbinsights_basic_name.value,
								  'ID',
								  'CompanyName',
								  self.enum.SQL.sql_cbinsights_basic_name_update.value)


if __name__ == '__main__':
	vd = VentureDedupe()
	# vd.get_verified_duplicate()
	# vd.inset_new_ventures()
	# vd.update_ventures_basic_name()
	# vd.duplicate_venture_table_processing()
	# vd.venture_with_former_name()
	# vd.survey_previous_name_for_ventures()
	# vd.ventures_with_double_name()
	# vd.update_ventures_basic_name_new()
	# vd.update_tdw_basic_name()
	# vd.update_cbInsight_basic_name()
	vd.duplicate_venture_insert()
