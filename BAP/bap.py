import datetime
import os
import pandas as pd

from Shared.qa import BapQA
from Shared.match import CompanyService
from Shared.file_service import FileService
from Shared.match import CompanyService
from Shared.common import Common as COM
from Shared.enums import MDCDataSource as DS, WorkSheet as WS, \
	FileName as FN, SQL as sql, FileType, \
	SourceSystemType as ss, Table as tbl, \
	Columns as clm, PATH as pth, Combine, FilePath as fp
from Shared.db import DB as db
from Shared.batch import BatchService
from pypostalcode import PostalCodeDatabase
from dateutil import parser


class BapQuarterly:
	desired_width = 420
	pd.set_option('display.width', desired_width)

	year, quarter = COM.fiscal_year_quarter(datetime.datetime.utcnow())
	quarter = quarter - 1
	batch = BatchService()
	bap_path_source = COM.get_config('config.ini', 'box_file_path', 'path_bap_source')
	bap_path_etl = COM.get_config('config.ini', 'box_file_path', 'path_bap_etl')
	file = FileService(bap_path_source)
	qa = BapQA()
	season = '19_Q1'
	company = CompanyService()

	@staticmethod
	def show_bap_quarterly_template():
		BapQuarterly.file.show_source_file()

	'''
	Checks if all the RICs send the right template with all the columns exists.
	'''
	@staticmethod
	def qa_bap_spread_sheet_by_ric():
		BapQuarterly.qa.check_rics_file(fp.path_bap_etl, fp.path_bap_qa)

	@staticmethod
	def combine_rics_bap_quarterly(combine_for):
		program, program_youth, company_quarterly, company_annually = BapQuarterly.file.read_source_file(
			FileType.SPREAD_SHEET.value, DS.BAP, combine_for, current_path=fp.path_bap_qa.value)
		file_name = '{}'.format(FN.bap_combined.value.format('19','1'))#(str(BapQuarterly.year - 1)[-2:], BapQuarterly.quarter))
		if combine_for == Combine.FOR_QA:
			file_name = 'QA_' + file_name
		else:
			file_name = 'ETL_' + file_name

		print('\nSave spreadsheet file named: {}'.format(file_name))

		save_location = COM.change_working_directory(fp.path_bap_combined.value)
		print(str(save_location))

		writer = pd.ExcelWriter(file_name)
		
		program.to_excel(writer, WS.bap_program.value, index=False)
		program_youth.to_excel(writer, WS.bap_program_youth.value, index=False)
		company_quarterly.to_excel(writer, WS.bap_company.value, index=False)
		if BapQuarterly.quarter == 3:
			company_annually.to_excel(writer, WS.bap_company_annual.value, index=False)
		writer.save()
		
		print('rics_spreasheet_combined.')

	@staticmethod
	def qa_bap_ric_combined(combined=False):
		BapQuarterly.qa.check_rics_file(fp.path_bap_combined, fp.path_bap_combined_dest, combined)

	@staticmethod
	def transfer_csv_program(dataframe):
		val = COM.df_list(dataframe)
		db.bulk_insert(sql.sql_bap_ric_program_insert.value, val)
	
	@staticmethod
	def transfer_csv_program_youth(dataframe):
		val = COM.df_list(dataframe)
		db.bulk_insert(sql.sql_bap_ric_program_youth_insert.value, val)
	
	@staticmethod
	def bulk_insert_quarterly_data(dataframe):
		val = COM.df_list(dataframe)
		db.bulk_insert(sql.sql_bap_ric_venture_quarterly_insert.value, val)

	@staticmethod
	def bulk_insert_annual_data(dataframe):
		val = COM.df_list(dataframe)
		db.bulk_insert(sql.sql_bap_ric_venture_annual_insert.value, val)

	@staticmethod
	def push_bap_quarterly_to_database():
		COM.change_working_directory(fp.path_bap_combined.value)

		bap = pd.read_excel('ETL_RICS_BAP_COMBINED_FY19Q1.xlsx', sheet_name=None)

		# BapQuarterly.transfer_csv_program(bap['csv_program16'])
		# BapQuarterly.transfer_csv_program_youth(bap['csv_program16_youth'])
		BapQuarterly.bulk_insert_quarterly_data(bap['Quarterly Company Data'])
		if BapQuarterly.quarter == 3:
			BapQuarterly.bulk_insert_annual_data(bap['Annual Company data'])

	@staticmethod
	def create_bap_batch():
		batch = BatchService()
		program = db.pandas_read(sql.sql_bap_distinct_batch.value.format(tbl.ric_program.value,BapQuarterly.year,BapQuarterly.quarter))
		program_youth = db.pandas_read(sql.sql_bap_distinct_batch.value.format(tbl.ric_program_youth.value, BapQuarterly.year,BapQuarterly.quarter))
		company = db.pandas_read(sql.sql_bap_distinct_batch.value.format(tbl.venture_data.value, BapQuarterly.year, BapQuarterly.quarter))
		comapny_annual = db.pandas_read(sql.sql_annual_bap_distinct_batch.value.format(tbl.venture_annual.value,BapQuarterly.year))

		# batch.create_bap_batch(program, BapQuarterly.year, BapQuarterly.quarter, tbl.ric_program.value, WS.bap_program.value, ss.RICPD_bap.value)
		# batch.create_bap_batch(program_youth, BapQuarterly.year, BapQuarterly.quarter, tbl.ric_program_youth.value, WS.bap_program_youth.value, ss.RICPDY_bap.value)
		batch.create_bap_batch(company, BapQuarterly.year, BapQuarterly.quarter, tbl.venture_data.value, WS.bap_company.value, ss.RICCD_bap.value)
		if BapQuarterly.quarter == 3:
			batch.create_bap_batch(comapny_annual, BapQuarterly.year, BapQuarterly.quarter, tbl.venture_annual.value, WS.bap_company_annual.value, ss.RICACD_bap.value)
	
	@staticmethod
	def transfer_bap_company():
		cs = CompanyService()
		cs.move_company_data()

	@staticmethod
	def get_proper_values(df):
		df['StageLevelID'] = df.apply(lambda dfs: COM.get_stage_level(dfs.Stage), axis=1)
		df['High Potential y/n'] = df.apply(lambda dfs: COM.get_yes_no(dfs['High Potential y/n']), axis=1)
		df['Social Enterprise y/n'] = df.apply(lambda dfs: COM.get_yes_no(dfs['Social Enterprise y/n']), axis=1)
		df['Youth'] = df.apply(lambda dfs: COM.get_yes_no(dfs['Youth']), axis=1)
		# df['Funding Raised to Date $CAN'] = df.apply(lambda dfs: BapQuarterly.split_funding_range(dfs['Funding Raised to Date $CAN']), axis=1)
		return df

	@staticmethod
	def transfer_fact_ric_company_data():
		df = db.pandas_read(sql.sql_bap_fact_ric_data_fyq4.value)
		df_frc = BapQuarterly.get_proper_values(df)
		# BapQuarterly.update_month_year(df_frc)
		# df_frc['IntakeDate'] = pd.to_datetime(df_frc['IntakeDate'])
		df_frc['Age'] = None
		# df_frc['Date of Incorporation'] = pd.to_datetime(df_frc['Date of Incorporation'])
		# df_ric = df_frc.drop(columns=['ID', 'Incorporate year (YYYY)', 'Incorporation month (MM)'])
		# BapQuarterly.file.save_as_csv(df_frc, '00 FactRICCompany.xlsx', os.getcwd(), 'FactRICCompany')
		values_list = COM.df_list(df_frc)

		db.bulk_insert(sql.sql_bap_fact_ric_company_insert.value, values_list)

	@staticmethod
	def split_funding_range(funding):
		funding_value = 0
		if funding == '$100-149k':
			funding_value = '100000'
		elif funding == '$10-24k':
			funding_value = '10000'
		elif funding == '$150-249k':
			funding_value = '150000'
		elif funding == '$1M-1.9M':
			funding_value = '1000000'
		elif funding == '$250-499k':
			funding_value = '250000'
		elif funding == '$25-49k':
			funding_value = '25000'
		elif funding == '$2-5M':
			funding_value = '2000000'
		elif funding == '$2M-5M':
			funding_value = '2000000'
		elif funding == '$500-999k':
			funding_value = '500000'
		elif funding == '$50-99k':
			funding_value = '50000'
		elif funding == '<$10k':
			funding_value = '1000'
		elif funding == '>$5M':
			funding_value = '5000000'

		return funding_value

	@staticmethod
	def update_month_year(df):
		i = 0
		for index, row in df.iterrows():
			if row['Incorporate year (YYYY)'] is not None and row['Incorporation month (MM)'] is not None:
				row['Date of Incorporation'] = parser.parse('{}-{}-15'.format(row['Incorporate year (YYYY)'], row['Incorporation month (MM)']))
				i+=1
				print('{}. {}'.format(i, row['Date of Incorporation']))
		# for index, row in df.iterrows():
		# 	if row['Incorporate year (YYYY)'] is not None and len(row['Incorporate year (YYYY)']) > 4:
		# 		update = 'UPDATE BAP.QuarterlyCompanyData SET [Incorporate year (YYYY)] = {} WHERE ID = {} -- {}'.format(parser.parse(row['Incorporate year (YYYY)']).year, row['ID'], parser.parse(row['Incorporate year (YYYY)']))
		# 		print(update)
		# dfs = df[df['Incorporate year (YYYY)'].isnull()]
		# for index, row in dfs.iterrows():
		# 	if row['Incorporation month (MM)'] is not None and len(row['Incorporation month (MM)']) > 2:
		# 		update = 'UPDATE BAP.QuarterlyCompanyData SET [Incorporate year (YYYY)] = {} WHERE ID = {}'.format(parser.parse(row['Incorporation month (MM)']).year, row['ID'])
		# 		print(update)
		# i= 0
		# for index, row in df.iterrows():
		# 	if row['Incorporation month (MM)'] is not None and len(row['Incorporation month (MM)']) > 2:
		# 		i += 1
		# 		update = 'UPDATE BAP.QuarterlyCompanyData SET [Incorporation month (MM)] = {} WHERE ID = {}'.format(parser.parse(row['Incorporation month (MM)']).month, row['ID'])
		# 		print(update)
		print('')

	@staticmethod
	def transfer_fact_ric_aggregation():
		date_id = COM.get_dateid(datevalue=None)
		metric_prg = [130, 132, 133, 129, 134, 63, 77, 60, 68, 67, 135, 136, 137]
		metric_prg_youth = [134, 138]
		
		df_program = db.pandas_read(sql.sql_company_aggregate_program.value.format(2018, 4))#(BapQuarterly.year, BapQuarterly.quarter))
		df_program_youth = db.pandas_read(sql.sql_company_aggregate_program_youth.value.format(2018, 4))#(BapQuarterly.year, BapQuarterly.quarter))
		
		values = []
		
		for _, row in df_program.iterrows():
			i = 7
			while i < 20:
				m = i - 7
				val = []
				val.append(int(row['DataSource']))  # DataSource
				val.append(int(date_id))  # RICDateID
				val.append(int(metric_prg[m]))  # MetricID
				val.append(int(row['BatchID']))  # BatchID
				
				if str(row[i]) in ['no data', 'n\\a', '-', 'n/a', 'nan']:
					val.append(-1.0)
					print(row[i])
				else:
					val.append(round(float(row[i]), 2))  # AggregateNumber
				val.append(str(datetime.datetime.today())[:23])  # ModifiedDate
				val.append(str(datetime.datetime.today())[:23])  # CreatedDate
				val.append(row['Youth'])  # Youth
				values.append(val)
				i = i + 1
				# db.execute(sql.sql_bap_fra_insert.value.format(tuple(val)))
		
		for _, row in df_program_youth.iterrows():
			
			j = 7
			while j < 9:
				m = j - 7
				val = []
				val.append(int(row['DataSource']))  # DataSource
				val.append(int(date_id))  # RICDateID
				val.append(int(metric_prg_youth[m]))  # MetricID
				val.append(int(row['BatchID']))  # BatchID
				if str(row[j]) in ['no data', 'n\\a', '-', 'n/a', 'nan']:
					val.append(-1.0)
					print(row[j])
				else:
					val.append(round(float(row[j]), 2))  # AggregateNumber
				val.append(str(datetime.datetime.today())[:23])  # ModifiedDate
				val.append(str(datetime.datetime.today())[:23])  # CreatedDate
				val.append(row['Youth'])  # Youth
				
				values.append(val)
				j = j + 1
				# db.execute(sql.sql_bap_fra_insert.value.format(tuple(val)))
		for val in range(len(values)):
			print('{}. {}'.format(val,values[val]))
			# print('{}. {}'.format(val,values[val][1]))
		db.bulk_insert(sql.sql_bap_fact_ric_aggregation_insert.value, values)
	
	@staticmethod
	def generate_bap_rolled_up():
		company = []
		i = 0
		df_frcd = db.pandas_read(sql.sql_bap_fact_ric_company.value.format(BapQuarterly.year))
		print('Number of record to process {} '.format(len(df_frcd)))
		df_fact_ds_quarter = db.pandas_read(sql.sql_bap_report_company_ds_quarter.value.format(BapQuarterly.year))
		df_FactRICRolledUp = pd.DataFrame(columns=clm.clmn_fact_ric_rolled_up.value)
		df_industry = db.pandas_read(sql.sql_industry_list_table.value)
		cq = BapQuarterly.quarter
		total = 0
		if not df_frcd.empty:
			for _, row in df_fact_ds_quarter.iterrows():
				company_id = row['CompanyID']
				data_source_id = row['DataSourceID']
				
				i = i + 1
				print('{}. {}'.format(i, company_id))
				# ['Q1', 'Q2']
				ls_q = []
				ls_quarters = \
				df_fact_ds_quarter.query('CompanyID == {} & DataSourceID == {}'.format(company_id, data_source_id))[
					'MinFQ'].tolist()
				ls = df_frcd.query('CompanyID == {}'.format(company_id))['FiscalQuarter'].tolist()
				
				for itm in ls_quarters:
					ls_q.append(itm[-1:])
				# if str(cq) not in ls_q:
				# 	ls_q.append(cq)
				df_agg = df_frcd.query('CompanyID == {} & DataSourceID == {}'.format(company_id, data_source_id))
				print(ls_q)
				for quarter in ls_q:
					if int(quarter) == cq and len(df_agg) > 1:
						current_quarter = 'FiscalQuarter == \'Q{}\''.format(quarter)
						previous_quarter = 'FiscalQuarter == \'Q{}\''.format(int(quarter) - 1)
					elif int(quarter) == cq and len(df_agg) == 1:
						current_quarter = 'FiscalQuarter == \'Q{}\''.format(quarter)
					elif int(quarter) < cq:
						current_quarter = 'FiscalQuarter == \'Q{}\''.format(quarter)
					
					try:
						batch_id = df_agg.query(current_quarter)['BatchID'].values[0] if not df_agg.query(
							current_quarter).empty else None
						min_date = -1
						current_date = -1
						vhs = df_agg.query(current_quarter)['VolunteerMentorHours'].values[0] if not df_agg.query(
							current_quarter).empty else None
						adv = df_agg.query(current_quarter)['AdvisoryServicesHours'].values[0] if not df_agg.query(
							current_quarter).empty else None
						
						if int(quarter) == cq:
							vhs_agg = df_agg['VolunteerMentorHours'].sum()
							adv_agg = df_agg['AdvisoryServicesHours'].sum()
							funding_agg = df_agg['FundingCurrentQuarter'].sum()
						else:
							vhs_agg = float(df_agg['VolunteerMentorHours'].sum()) - float(
								df_agg.query('FiscalQuarter == \'Q{}\''.format(cq))['VolunteerMentorHours'].values[
									0]) if not df_agg.query('FiscalQuarter == \'Q{}\''.format(cq)).empty else float(
								df_agg.query('FiscalQuarter == \'Q{}\''.format(cq - 1))['VolunteerMentorHours'].values[
									0])
							adv_agg = float(df_agg['AdvisoryServicesHours'].sum()) - float(
								df_agg.query('FiscalQuarter == \'Q{}\''.format(cq))['AdvisoryServicesHours'].values[
									0]) if not df_agg.query('FiscalQuarter == \'Q{}\''.format(cq)).empty else float(
								df_agg.query('FiscalQuarter == \'Q{}\''.format(cq - 1))['AdvisoryServicesHours'].values[
									0])
							funding_agg = float(df_agg['FundingCurrentQuarter'].sum()) - float(
								df_agg.query('FiscalQuarter == \'Q{}\''.format(cq))['FundingCurrentQuarter'].values[
									0]) if not df_agg.query('FiscalQuarter == \'Q{}\''.format(cq)).empty else float(
								df_agg.query('FiscalQuarter == \'Q{}\''.format(cq - 1))['FundingCurrentQuarter'].values[
									0])
						
						modified_date = datetime.datetime.utcnow().__str__()[:23]
						
						stage = df_agg.query(current_quarter)['Stage'].values[0] if not df_agg.query(
							current_quarter).empty else df_agg.query(previous_quarter)['Stage'].values[0]
						industry_sector = df_agg.query(current_quarter)['IndustrySector'].values[0] if not df_agg.query(
							current_quarter).empty else df_agg.query(previous_quarter)['IndustrySector'].values[0]
						socialEnterprise = df_agg.query(current_quarter)['SocialEnterprise'].values[
							0] if not df_agg.query(current_quarter).empty else \
						df_agg.query(previous_quarter)['SocialEnterprise'].values[0]
						highPotential = df_agg.query(current_quarter)['HighPotential'].values[0] if not df_agg.query(
							current_quarter).empty else df_agg.query(previous_quarter)['HighPotential'].values[0]
						youth = df_agg.query(current_quarter)['Youth'].values[0] if not df_agg.query(
							current_quarter).empty else df_agg.query(previous_quarter)['Youth'].values[0]
						dateOfIncorporation = df_agg.query(current_quarter)['DateOfIncorporation'].values[
							0] if not df_agg.query(current_quarter).empty else \
						df_agg.query(previous_quarter)['DateOfIncorporation'].values[0]
						
						annual_revenue = df_agg.query(current_quarter)['AnnualRevenue'].values[0] if not df_agg.query(
							current_quarter).empty else None
						funding_current_quarter = df_agg.query(current_quarter)['FundingCurrentQuarter'].values[
							0] if not df_agg.query(current_quarter).empty else None
						
						number_of_employees = df_agg.query(current_quarter)['NumberEmployees'].values[
							0] if not df_agg.query(current_quarter).empty else None
						intake_date = df_agg.query(current_quarter)['IntakeDate'].values[0] if not df_agg.query(
							current_quarter).empty else None
						lvl2_industry_name = df_industry.query('Industry_Sector == \'{}\''.format(industry_sector))[
							'Lvl2IndustryName'].values[0] if not df_industry.query(
							'Industry_Sector == \'{}\''.format(industry_sector)).empty else None
						dd = {'DataSourceID': data_source_id,
							  'CompanyID': company_id,
							  'MinDate': min_date,
							  'CurrentDate': current_date,
							  'VolunteerYTD': vhs_agg,
							  'AdvisoryHoursYTD': adv_agg,
							  'VolunteerThisQuarter': vhs,
							  'AdvisoryThisQuarter': adv,
							  'FiscalQuarter': quarter,
							  'BatchID': batch_id,
							  'ModifiedDate': modified_date,
							  'SocialEnterprise': socialEnterprise,
							  'Stage': stage,
							  'HighPotential': highPotential,
							  'Lvl2IndustryName': lvl2_industry_name,
							  'FiscalYear': BapQuarterly.year,
							  'Youth': youth,
							  'DateOfIncorporation': dateOfIncorporation,
							  'AnnualRevenue': annual_revenue,
							  'NumberEmployees': number_of_employees,
							  'FundingToDate': funding_current_quarter,
							  'IndustrySector': industry_sector,
							  'IntakeDate': intake_date,
							  'FundingCurrentQuarter': funding_agg
							  }
						print(dd.values())
						df = pd.DataFrame([dd], columns=clm.clmn_fact_ric_rolled_up.value)
						df_FactRICRolledUp = pd.concat([df_FactRICRolledUp, df])
					except Exception as ex:
						total = total + 1
						company.append(company_id)
						print(ex)
			df_FactRICRolledUp = df_FactRICRolledUp[clm.clmn_fact_ric_rolled_up.value]
			BapQuarterly.file.save_as_csv(df_FactRICRolledUp,
										  'BAP_Rolled_UP_{}.xlsx'.format(str(datetime.datetime.today())),
										  '/Users/mnadew/Box Sync/mnadew/IE/data/ETL/BAP')
			print(company)
			print('{} + {} = {}/ 6236 '.format(len(df_FactRICRolledUp), total, len(df_FactRICRolledUp) + total))
	
	# @staticmethod
	# def generate_bap_report():
	# 	pass
	
	@staticmethod
	def create_postal_code_list():
		pcdb = PostalCodeDatabase()
		results = pcdb.get_postalcodes_around_radius('T3Z', 2500)
		print(type(results))
		cl = ['postalcode', 'city', 'province', 'longitude', 'latitude', 'timezone', 'dst']
		dfs = pd.DataFrame(columns=cl)
		for r in results:
			df = pd.DataFrame([r.__dict__], columns=cl)
			dfs = pd.concat([dfs, df])
		dfs
		
	@staticmethod
	def read_postal_code():
		path = '/Users/mnadew/Box Sync/mnadew/PRD_DB_REVIEW'
		print(os.getcwd())
		os.chdir(path)
		print(os.getcwd())
		columns = ['FSALDU', 'LATITUDE', 'LONGITUDE', 'COMMNAME', 'CSDNAMEE', 'CSDNAMEF', 'CSDTYPENE', 'PRABB']
		df = pd.read_csv('postal_code_utf8.csv')
		df = df[columns]
		print(len(df))
		i = 846000
		j = 847000
		while j < 847001:
			print('From {} to {}'.format(i, j))
			df_ins = df.iloc[i:j]
			BapQuarterly.insert(df_ins)
			print(len(df_ins))
			i, j = i + 1000, j + 1000
			print('From {} to {}'.format(i, j))

	@staticmethod
	def bap_insert(df):
		values_list = COM.df_list(df)
		db.bulk_insert(sql.sql_postal_code_insert.value, values_list)

	@staticmethod
	def main():
		while True:
			fy, fq = COM.fiscal_year_quarter()
			print('_'*100)
			print('| WELCOME TO BAP QUARTERLY ETL\n| FISCAL YEAR:     {}\n| FISCAL QUARTER:     {}'.format(fy, fq - 1))
			print('_' * 100)
			menu = '''
			1: Show Source File for BAP quarterly FY18-Q3
			1a: CHECK Columns Completeness
			2: QA spreadsheet by RIC
			3: Combine RICs BQ spreadsheet
			4: QA RICs BQ combined spreadsheet
			5: Push RICs data ro the database
			6: Generate Batch for RICs FY18 -Q3
			7: Match Company name
			8: Push Company data to DIM COMPANY and DIM COMPANY SOURCE
			9: Push quarterly company data to FACT RIC COMPANY DATA
			10: Push Annual company data to FACT RIC COMPANY DATA
			11: push Program and Program youth data to FACT RIC Aggregation
			'''
			print(menu)

			option = input('\nChoose your option:\t')
			if str(option) == '1':
				BapQuarterly.show_bap_quarterly_template()
			if str(option) == '1a':
				BapQuarterly.qa.check_columns_completeness()
			if str(option) == '2':
				BapQuarterly.qa.check_rics_file()
			if str(option) == '3':
				pass
			if str(option) == '4':
				pass
			if str(option) == '5':
				pass
			if str(option) == '6':
				pass
			if str(option) == '7':
				pass
			if str(option) == '8':
				pass
			if str(option) == '9':
				pass
			if str(option) == '10':
				pass
			if str(option) == '11':
				pass
			if str(option) == '12':
				pass

	@staticmethod
	def tech_alliance_intake_date_TEMP():
		# update = 'UPDATE BAP.QuarterlyCompanyData SET [Date of Intake] = \'{}\' WHERE [Company Name] = \'{}\' AND DataSource = 6'
		update = ' SELECT * FROM BAP.QuarterlyCompanyData WHERE [Company Name] = \'{}\' AND DataSource = 6 UNION'
		current_path = os.path.join(os.path.expanduser("~"), '/Users/mnadew/Box Sync/Workbench/BAP/BAP_FY18/FY18_Q3/for ETL/Missing data Reports')
		os.chdir(current_path)
		df = pd.read_excel('01 TechAlliance_BAP_qtrly_perCompany_MISSING DATA(2).xlsx', 'Quarterly Company data')
		# df['BasicName'] = df.apply(lambda dfs: COM.update_cb_basic_name(dfs['Company Name']), axis=1)
		i = 0
		for i, r in df.iterrows():
			if r[2] is not None or r[2]== 'nan':
				# print(r[2])
				year = r[2][-4:]
				month = r[2][3:5]
				date = r[2][:2]
				i = i + 1
				# print('{}. {} ---> {}-{}-{}'.format(i, r[2], year, month, date))
				d = '{}-{}-{}'.format(year, month, date)
				# print(update.format(d, r[0]))
				print(update.format(r[0]))

	@staticmethod
	def combine_missing_data():
		quarterly_missing = BapQuarterly.file.combine_bap_missing_source_file(
			current_path=fp.path_missing_bap_etl.value)
		quarterly_missing = quarterly_missing.where(pd.notnull(quarterly_missing), None)
		quarterly_missing['BasicName'] = quarterly_missing.apply(lambda dfs: COM.get_basic_name(dfs.CompanyName),
																 axis=1)
		df = quarterly_missing.where(pd.notnull(quarterly_missing), None)
		print(df.columns)
		dfs = df[['CompanyName', 'BasicName', 'Website', 'AnnualRevenue', 'NumberOfEmployees', 'FundingToDate',
				  'DataSource']]
		BapQuarterly.file.save_as_csv(dfs, '00 BAP Missing data Combined.xlsx', os.getcwd(), 'BAP Missing data')
		print(dfs.head())

	@staticmethod
	def push_bap_missing_data_to_temp_table():
		 current_path = os.path.join(os.path.expanduser("~"), '/Users/mnadew/Box Sync/Workbench/BAP/BAP_FY18/FY18_Q3/for ETL/Missing data Reports')
		 os.chdir(current_path)
		 df = pd.read_excel('00 BAP Missing data Combined.xlsx', 'BAP Missing data')
		 df['CompanyID'] = 0
		 new_col = ['CompanyID','CompanyName','BasicName','Website','AnnualRevenue','NumberOfEmployees','FundingToDate','DataSource']
		 dfs = df[new_col]
		 sql = 'INSERT INTO BAP.BAP_FY18Q3_Missing_Data VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
		 values = COM.df_list(dfs)
		 db.bulk_insert(sql, values)

	@staticmethod
	def bap_company_basic_name():
		db.update_basic_name(sql.sql_bap_basic_name.value,
							 'ID',
							 'CompanyName',
							 sql.sql_bap_basic_name_update.value)


if __name__ == '__main__':
	# BapQuarterly.qa.check_columns_completeness()
	# BapQuarterly.combine_rics_bap_quarterly(Combine.FOR_QA)
	# BapQuarterly.combine_rics_bap_quarterly(Combine.FOR_ETL)
	# BapQuarterly.qa_bap_spread_sheet_by_ric()
	# BapQuarterly.qa_bap_ric_combined(True)
	# BapQuarterly.file.read_source_file('', '')
	# BapQuarterly.push_bap_quarterly_to_database()
	# BapQuarterly.create_bap_batch()
	# BapQuarterly.company.move_company_data()
	# BapQuarterly.bap_company_basic_name()
	# BapQuarterly.company.update_raw_company()
	# BapQuarterly.transfer_fact_ric_company_data()
	# BapQuarterly.transfer_fact_ric_aggregation()
	# BapQuarterly.generate_bap_rolled_up()
	# BapQuarterly.tech_alliance_intake_date_TEMP()
	# BapQuarterly.combine_missing_data()
	# BapQuarterly.push_bap_missing_data_to_temp_table()

	# BapQuarterly.show_bap_quarterly_template()
	# BapQuarterly.qa.check_columns_completeness()
	# BapQuarterly.qa_bap_spread_sheet_by_ric()
	# BapQuarterly.combine_rics_bap_quarterly(Combine.FOR_QA)
	# BapQuarterly.combine_rics_bap_quarterly(Combine.FOR_ETL)
	# BapQuarterly.push_bap_quarterly_to_database()
	# BapQuarterly.create_bap_batch()
	BapQuarterly.bap_company_basic_name()
	## BapQuarterly.company.move_company_data()