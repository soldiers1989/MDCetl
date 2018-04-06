import Shared.datasource as ds
import Shared.enums as enum
import pandas as pd
import os


class IAF(ds.DataSource):

	def __init__(self):
		super().__init__('box_file_path', 'path_iaf', enum.DataSourceType.IAF)
		self.iaf_columns = ['Date_supplied','Firm_name','Entrepreneur_name', 'Sector', 'Number_of_Employees', 'Board_observer_rights','New_non_spin_off_or_existing_firm',
							'Angel', 'VC', 'Amount_of_other_dilutive_financing',
							'Term_loan','IRAP', 'FedDev','SR&ED', 'OCE', 'Other',
							'Number_of_Business_Plans_Developed', 'Number_of_prototypes_developed','Number_of_new_patents_applied_For', 'Number_of_New_Patents_Received', 'Other_non_patent_IP',
							'Number_of_finance_accounting','Number_of_marketing_sales', 'Number_of_technical_scientific', 'CEO_COO', 'Customer_service_support', 'Others',

							'Current_year_estimated_sales','Quarterly Revenue', 'Revenue_year_to_date', 'Net_working_capital', 'Burn_rate',
							'Annual_revenue_growth', 'Revenue_out_side_of_canada','R_and_D_expenditure','EBITDA',
							'Number_of_new_products_marketed', 'Number_of_new_services_marketed', 'Number_of_new_processes_commercialized',
							'Number_of_new_international_exported_products_services']

		self.iaf_cols = ['CompanyID','Date_supplied', 'Entrepreneur_name', 'Sector', 'Number_of_Employees',
							'Board_observer_rights', 'New_non_spin_off_or_existing_firm',
							'Angel', 'VC', 'Amount_of_other_dilutive_financing',
							'Term_loan', 'IRAP', 'FedDev', 'SR&ED', 'OCE', 'Other',
							'Number_of_Business_Plans_Developed', 'Number_of_prototypes_developed',
							'Number_of_new_patents_applied_For', 'Number_of_New_Patents_Received',
							'Other_non_patent_IP',
							'Number_of_finance_accounting', 'Number_of_marketing_sales',
							'Number_of_technical_scientific', 'CEO_COO', 'Customer_service_support', 'Others',

							'Current_year_estimated_sales', 'Quarterly Revenue', 'Revenue_year_to_date',
							'Net_working_capital', 'Burn_rate',
							'Annual_revenue_growth', 'Revenue_out_side_of_canada', 'R_and_D_expenditure', 'EBITDA',
							'Number_of_new_products_marketed', 'Number_of_new_services_marketed',
							'Number_of_new_processes_commercialized',
							'Number_of_new_international_exported_products_services', 'Firm_name']

		self.summary_columns = ['Company_name','Sector','Prime','Investment_manager', 'Closing_date', 'Committed', 'Disbursed', 'Net_working_capital',
								'Burn_rate', 'Runway_monthly', 'Current_number_of_employees',
								'Number_of_employees_at_time_of_investment','Follow_on_closed_and_non_dilutive',
								'Follow_on_closed',	'Board_Observer_rights','Date_of_last_filed_report', 'Investment_type']

		self.sum_cols =  ['CompanyID','Sector', 'Prime', 'Investment_manager','Investment_type','Closing_date',
						 'Committed', 'Disbursed', 'Net_working_capital', 'Burn_rate', 'Runway_monthly',
						 'Current_number_of_employees', 'Number_of_employees_at_time_of_investment',
						 'Follow_on_closed_and_non_dilutive', 'Follow_on_closed', 'Board_Observer_rights',
						 'Date_of_last_filed_report', 'Company_name']

	def read_iaf_summary_files(self):
		_, summary = self.file.read_source_file(enum.FileType.SPREAD_SHEET, enum.MDCDataSource.IAF, enum.Combine.FOR_NONE.value)
		print(summary.columns)
		summary.columns = self.summary_columns
		summary = summary.drop([0,40,71,75,95,113])
		print(summary.head(25))
		self.file.save_as_csv(summary, 'SUMMARY_FOR_ETL_2017.xlsx', os.getcwd() + '/ETL', 'IAF Summary 2017')


	def read_iaf_per_company_files(self):
		self.data, _ = self.file.read_source_file(enum.FileType.SPREAD_SHEET, enum.MDCDataSource.IAF, enum.Combine.FOR_NONE.value)

		self.data= self.data.drop(columns=[9,10,11,12,16,17,18,25,26,27,28,34,35,42,43,44])
		print(self.data.columns)
		self.data.columns = self.iaf_columns
		self.file.save_as_csv(self.data, 'DETAILS_FOR_ETL_2017.xlsx', os.getcwd() + '/ETL', 'IAF Summary 2017')

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
		dfiafdetail = pd.read_excel('DETAILS_FOR_ETL_2017.xlsx')
		dfiafdetail['CompanyID'] = None
		dfiafdetail = dfiafdetail[self.iaf_cols]
		values = self.common.df_list(dfiafdetail)
		self.db.bulk_insert(enum.SQL.sql_iaf_detail_insert.value,values)

	def getpath(self):
		self.common.get_path()


if __name__ == '__main__':
	iaf = IAF()
	# iaf.read_iaf_summary_files()
	# iaf.push_iaf_summary_db()
	# iaf.generate_iaf_company_basic_name()
	# iaf.read_iaf_per_company_files()
	# iaf.push_iaf_detail_db()
	iaf.getpath()