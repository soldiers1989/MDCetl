import datetime
import os
import pandas as pd

from Shared.qa import BapQA
from Shared.file_service import FileService
from Shared.match import CompanyService
from Shared.common import Common as COM
from Shared.enums import DataSource as DS, WorkSheet as WS, \
	FileName as FN, SQL as sql, FileType, \
	SourceSystemType as ss, Table as tbl, \
	Columns as clm
from Shared.db import DB
from Shared.batch import BatchService
from pypostalcode import PostalCodeDatabase


class BapQuarterly:
	year, quarter = COM.fiscal_year_quarter(datetime.datetime.utcnow())
	batch = BatchService()
	bap_path = COM.get_config('config.ini', 'box_file_path', 'path_bap')
	file = FileService(bap_path)
	qa = BapQA()
	
	@staticmethod
	def show_bap_quarterly_template():
		BapQuarterly.file.show_source_file()

	@staticmethod
	def combine_rics_bap_quarterly():
		program, program_youth, company_quarterly, company_annually = BapQuarterly.file.read_source_file(FileType.SPREAD_SHEET.value, DS.BAP)
		file_name = FN.bap_combined.value.format(str(BapQuarterly.year)[-2:], BapQuarterly.quarter - 1)
		print('Save spreadsheet file named: {}'.format(file_name))

		writer = pd.ExcelWriter(file_name)
		
		program.to_excel(writer, WS.bap_program_final.value, index=False)
		program_youth.to_excel(writer, WS.bap_program_youth_final.value, index=False)
		company_quarterly.to_excel(writer, WS.bap_company.value, index=False)
		company_annually.to_excel(writer, WS.bap_company_annual.value, index=False)
		writer.save()
		
		print('rics_spreasheet_combined.')
	
	@staticmethod
	def transfer_csv_program(dataframe):
		val = COM.df_list(dataframe)
		DB.bulk_insert(sql.sql_program_insert, val)
	
	@staticmethod
	def transfer_csv_program_youth(dataframe):
		val = COM.df_list(dataframe)
		DB.bulk_insert(sql.sql_program_youth_insert, val)
	
	@staticmethod
	def bulk_insert_quarterly_data(dataframe):
		val = COM.df_list(dataframe)
		DB.bulk_insert(sql.sql_bap_company_insert, val)

	@staticmethod
	def bulk_insert_annual_data(dataframe):
		val = COM.df_list(dataframe)
		DB.bulk_insert(sql.sql_bap_company_annual_insert, val)

	@staticmethod
	def transfer_bap_data():
		program = pd.read_excel('ALL_RICS_BAP_FY18Q3_FINAL.xlsx', WS.bap_program.value)
		program_youth = pd.read_excel('ALL_RICS_BAP_FY18Q3_FINAL.xlsx', WS.bap_program_youth.value)
		quarterly_data = pd.read_excel('ALL_RICS_BAP_FY18Q3_FINAL.xlsx', WS.bap_company.value)
		annual_data = pd.read_excel('ALL_RICS_BAP_FY18Q3_FINAL.xlsx', WS.bap_company_annual.value)

		BapQuarterly.transfer_csv_program(program)
		BapQuarterly.transfer_csv_program_youth(program_youth)
		BapQuarterly.bulk_insert_quarterly_data(quarterly_data)
		BapQuarterly.bulk_insert_annual_data(annual_data)

	@staticmethod
	def create_bap_batch():
		batch = BatchService()
		program = DB.pandas_read(sql.sql_bap_distict_batch.format(tbl.company_program,
		                                                          BapQuarterly.year,
		                                                          BapQuarterly.quarter))
		program_youth = DB.pandas_read(sql.sql_bap_distict_batch.format(tbl.company_program_youth,
		                                                                BapQuarterly.year,
		                                                                BapQuarterly.quarter))
		company = DB.pandas_read(sql.sql_bap_distict_batch.format(tbl.company_data,
		                                                          BapQuarterly.year,
		                                                          BapQuarterly.quarter))
		
		batch.create_batch(program, BapQuarterly.year, BapQuarterly.quarter - 1, tbl.company_program)
		batch.create_batch(program_youth, BapQuarterly.year, BapQuarterly.quarter - 1, tbl.company_program_youth)
		batch.create_batch(company, BapQuarterly.year, BapQuarterly.quarter - 1, tbl.company_data)
	
	@staticmethod
	def transfer_bap_company():
		cs = CompanyService()
		cs.move_company_data(BapQuarterly.year,
		                     BapQuarterly.quarter - 1,
		                     ss.RICPD_bap,
		                     tbl.company_data
		                     )
	
	@staticmethod
	def transfer_fact_ric_company_data():
		batch = BatchService()
		batches = batch.get_batch(BapQuarterly.year, BapQuarterly.quarter, ss.RICCD_bap)
		sq = sql.sql_bap_fact_ric_company_data_source.format(tuple(batches))
		df = DB.pandas_read(sq)
		values_list = COM.df_list(df)
		DB.bulk_insert(sql.sql_bap_fact_ric_company_insert, values_list)
	
	@staticmethod
	def transfer_fact_ric_aggregation():
		date_id = COM.get_dateid(datevalue=None)
		fact_rig_aggregaton_id = COM.get_table_seed(tbl.fact_ric_aggregation, clm.ric_aggregation_id)
		metric_prg = [130, 132, 133, 129, 134, 63, 77, 60, 68, 67, 135, 136, 137]
		metric_prg_youth = [134, 138]
		batches_prg = BapQuarterly.batch.get_batch(BapQuarterly.year,
		                                           BapQuarterly.quarter,
		                                           ss.RICPD_bap)
		
		batches_prg_y = BapQuarterly.batch.get_batch(BapQuarterly.year,
		                                             BapQuarterly.quarter,
		                                             ss.RICPDY_bap)
		
		df_program = DB.pandas_read(sql.sql_company_aggregate_program.format(tuple(batches_prg)))
		df_program_youth = DB.pandas_read(sql.sql_company_aggregate_program_youth.format(batches_prg_y))
		
		values = []
		
		for _, row in df_program.iterrows():
			i = 1
			while i < 13:
				fact_rig_aggregaton_id = fact_rig_aggregaton_id + 1
				m = i - 1
				val = []
				val.append(fact_rig_aggregaton_id)
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
		
		for _, row in df_program_youth.iterrows():
			
			i = 1
			while i < 2:
				fact_rig_aggregaton_id = fact_rig_aggregaton_id + 1
				m = i - 1
				val = []
				val.append(fact_rig_aggregaton_id)
				val.append(int(row['DataSource']))  # DataSource
				val.append(int(date_id))  # RICDateID
				val.append(int(metric_prg_youth[m]))  # MetricID
				val.append(int(row['BatchID']))  # BatchID
				if str(row[i]) in ['no data', 'n\\a', '-', 'n/a', 'nan']:
					val.append(-1.0)
					print(row[i])
				else:
					val.append(round(float(row[i]), 2))  # AggregateNumber
				val.append(None)  # ModifiedDate
				val.append(None)  # CreatedDate
				val.append(row['Youth'])  # Youth
				
				values.append(val)
				i = i + 1
		
		DB.bulk_insert(tbl.fact_ric_aggregation, values)
	
	@staticmethod
	def generate_bap_rolled_up():
		company = []
		i = 0
		df_frcd = DB.pandas_read(sql.sql_bap_fact_ric_company.value.format(BapQuarterly.year))
		print('Number of record to process {} '.format(len(df_frcd)))
		df_fact_ds_quarter = DB.pandas_read(sql.sql_bap_report_company_ds_quarter.value.format(BapQuarterly.year))
		df_FactRICRolledUp = pd.DataFrame(columns=clm.clmn_fact_ric_rolled_up.value)
		df_industry = DB.pandas_read(sql.sql_industry_list_table.value)
		cq = BapQuarterly.quarter - 1
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
				if str(cq) not in ls_q:
					ls_q.append(cq)
				df_agg = df_frcd.query('CompanyID == {} & DataSourceID == {}'.format(company_id, data_source_id))
				
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
			                              '/Users/mnadew/Box Sync/mnadew/IE/Data/ETL/BAP')
			print(company)
			print('{} + {} = {}/ 6236 '.format(len(df_FactRICRolledUp), total, len(df_FactRICRolledUp) + total))
	
	@staticmethod
	def generate_bap_report():
		pass
	
	@staticmethod
	def test_runs():
		return 'Test your code'
	
	@staticmethod
	def create_postal_code_list(self):
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
	def insert(df):
		values_list = COM.df_list(df)
		DB.bulk_insert(sql.sql_postal_code_insert.value, values_list)


if __name__ == '__main__':
	desired_width = 420
	pd.set_option('display.width', desired_width)
	BapQuarterly.read_bap_data()
