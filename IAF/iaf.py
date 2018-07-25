import Shared.datasource as ds
import Shared.enums as enum
import pandas as pd
import os


class IAF(ds.DataSource):

	def __init__(self):
		# super().__init__('box_file_path', 'path_iaf', enum.DataSourceType.IAF,new_path='')
		super().__init__('', '', enum.DataSourceType.IAF, new_path='')
		self.iaf_columns = ['Date_supplied','Firm_name','Entrepreneur_name', 'Sector', 'Number_of_employees', 'Board_observer_rights','New_non_spin_off_or_existing_firm',
							'Angel', 'VC', 'Amount_of_other_dilutive_financing','Total_investment_leverage','Follow_on_closed_cumulative',
							'Term_loan','IRAP', 'FedDev','SRED', 'OCE', 'Non_Dilutive_Other','Total_non_dilutive','Non_dilutive_cumulative',
							'Number_of_business_plans_developed', 'Number_of_prototypes_developed','Number_of_new_patents_applied_for', 'Number_of_new_patents_reviewed', 'Other_non_patent_IP',
							'Number_of_finance_accounting','Number_of_marketing_sales', 'Number_of_technical_scientific', 'CEO_COO', 'Customer_service_support', 'Talent_Attracted_Others','Total_talent_attracted',
							'Current_year_estimated_sales','Quarterly_Revenue', 'Revenue_year_to_date', 'Networking_capital', 'Burn_rate',
							'Annual_revenue_growth', 'Revenue_out_side_of_canada','R_and_D_expenditure','EBITDA',
							'Number_of_new_products_marketed', 'Number_of_new_service_marketed', 'Number_of_new_processes_commercialized',
							'Number_of_new_international_exported_products_services', 'CompanyID']

		self.iaf_cols = ['Batch','CompanyID', 'Firm_name', 'Date_supplied', 'Entrepreneur_name', 'Sector', 'Number_of_employees', 'Board_observer_rights',
						 'New_non_spin_off_or_existing_firm', 'Angel', 'VC', 'Amount_of_other_dilutive_financing', 'Total_investment_leverage',
						 'Follow_on_closed_cumulative', 'Term_loan', 'IRAP', 'FedDev', 'SRED', 'OCE', 'Non_Dilutive_Other', 'Total_non_dilutive',
						 'Non_dilutive_cumulative', 'Number_of_business_plans_developed', 'Number_of_prototypes_developed',
						 'Number_of_new_patents_applied_for', 'Number_of_new_patents_reviewed', 'Other_non_patent_IP', 'Number_of_finance_accounting',
						 'Number_of_marketing_sales', 'Number_of_technical_scientific', 'CEO_COO', 'Customer_service_support', 'Talent_Attracted_Others',
						 'Total_talent_attracted', 'Current_year_estimated_sales', 'Quarterly_Revenue', 'Revenue_year_to_date', 'Networking_capital',
						 'Burn_rate', 'Annual_revenue_growth', 'Revenue_out_side_of_canada', 'R_and_D_expenditure', 'EBITDA', 'Number_of_new_products_marketed',
						 'Number_of_new_service_marketed', 'Number_of_new_processes_commercialized', 'Number_of_new_international_exported_products_services']

		self.summary_columns = ['Company_name','Sector','Prime','Investment_manager', 'Closing_date', 'Committed', 'Disbursed', 'Net_working_capital',
								'Burn_rate', 'Runway_monthly', 'Current_number_of_employees',
								'Number_of_employees_at_time_of_investment','Follow_on_closed_and_non_dilutive',
								'Follow_on_closed',	'Board_Observer_rights','Date_of_last_filed_report', 'Investment_type']

		self.sum_cols =  ['CompanyID','Sector', 'Prime', 'Investment_manager','Investment_type','Closing_date',
						 'Committed', 'Disbursed', 'Net_working_capital', 'Burn_rate', 'Runway_monthly',
						 'Current_number_of_employees', 'Number_of_employees_at_time_of_investment',
						 'Follow_on_closed_and_non_dilutive', 'Follow_on_closed', 'Board_Observer_rights',
						 'Date_of_last_filed_report', 'Company_name']

		self.iaf_metadata_columns = ['BatchID', 'MDC Company ID', 'Venture Name', 'MaRS Program (Start/ Growth/ Scale)', 'MaRS Sector', 'Notes']
		self.iaf_metadata_cols =  ['BatchID','CompanyID','Name','MaRSProgram', 'MaRSSector','Note']

	def read_iaf_summary_files(self):
		_, summary = self.file.read_source_file(enum.FileType.SPREAD_SHEET, enum.MDCDataSource.IAF, enum.Combine.FOR_NONE.value)
		print(summary.columns)
		summary.columns = self.summary_columns
		summary = summary.drop([0,40,71,75,95,113])
		print(summary.head(25))
		self.file.save_as_csv(summary, 'SUMMARY_FOR_ETL_2017.xlsx', os.getcwd() + '/ETL', 'IAF Summary 2017')


	def read_iaf_per_company_files(self):
		self.data, _ = self.file.read_source_file(enum.FileType.SPREAD_SHEET, enum.MDCDataSource.IAF, enum.Combine.FOR_NONE.value)
		self.data.reset_index()
		self.file.save_as_csv(self.data, 'IAF_2017_ORIGINAL.xlsx', os.getcwd() + '/ETL/QA', 'IAF Companies - 2017')
		self.data= self.data.drop(columns= [7,8,9,10,16,25,26,32,33,41,42]) #[9,10,11,12,16,17,18,25,26,27,28,34,35,42,43,44])
		self.data['CompanyID'] = None
		print(self.data.columns)
		self.data.columns = list(self.iaf_columns)
		self.data = self.data[list(self.iaf_cols)]
		self.file.save_as_csv(self.data, 'IAF_2017_PROCESSED.xlsx', os.getcwd(), 'IAF Companies - 2017')

	def push_iaf_summary_db(self):
		self.common.change_working_directory(self.enum.FilePath.path_iaf.value)
		dfsummary = pd.read_excel('SUMMARY_FOR_ETL_2017.xlsx')
		dfsummary['CompanyID'] = None
		dfsummary = dfsummary[self.sum_cols]
		dfsummary['BasicName'] = None
		values = self.common.df_list(dfsummary)
		self.db.bulk_insert(enum.SQL.sql_iaf_summary_insert.value,values)

	def generate_iaf_company_basic_name(self):
		df = self.db.pandas_read(enum.SQL.sql_iaf_summary_basic_name.value)
		df = self.common.generate_basic_name(df, 'Venture_Name')
		# print(df.head(25))
		for _, val in df.iterrows():
			sql_update = enum.SQL.sql_iaf_summary_update.value.format(val.BasicName, val.ID)
			self.db.execute(sql_update)
			print(sql_update)
		print('Check here...')

	def push_iaf_detail_db(self):
		self.common.change_working_directory(self.enum.FilePath.path_iaf.value)
		dfiafdetail = pd.read_excel('IAF_2017_PROCESSED.xlsx')
		dfiafdetail['Batch'] = 3861
		dfiafdetail['CompanyID'] = None
		dfiafdetail = dfiafdetail[self.iaf_cols]
		values = self.common.df_list(dfiafdetail)
		self.db.bulk_insert(enum.SQL.sql_iaf_detail_insert.value,values)

	def getpath(self):
		self.common.get_path()


	def push_iaf_metadata_db(self):
		self.common.change_working_directory('MDC')
		dfMetaData = pd.read_excel('IAF-only-ventures confidential 20180615 IG_Final.xlsx')
		dfMetaData['BatchID'] = 3916
		print(dfMetaData.columns)
		dfMetaData =  dfMetaData[self.iaf_metadata_columns]
		dfMetaData.columns = self.iaf_metadata_cols
		dfMetaData['Program'] = dfMetaData.apply(lambda dfs: self.common.convert_mars_program(dfs['MaRSProgram']),
												 axis=1)
		dfMetaData['Sector'] = dfMetaData.apply(lambda dfs: self.common.convert_mars_sector(dfs['MaRSSector']),
												 axis=1)
		dfMetaData = dfMetaData[['BatchID', 'CompanyID', 'Name', 'Program', 'Sector', 'Note']]
		print(dfMetaData.head(75))
		values = self.common.df_list(dfMetaData)
		self.db.bulk_insert(self.enum.SQL.sql_iaf_metadata_insert.value, values)



if __name__ == '__main__':
	iaf = IAF()
	# iaf.read_iaf_summary_files()
	# iaf.push_iaf_summary_db()
	# iaf.generate_iaf_company_basic_name()
	# iaf.read_iaf_per_company_files()
	# iaf.push_iaf_detail_db()
	# iaf.getpath()
	# iaf.push_iaf_detail_db()
	iaf.push_iaf_metadata_db()