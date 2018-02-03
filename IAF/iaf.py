import Shared.datasource as ds
import Shared.enums as enum
import os


class IAF(ds.DataSource):

	def __init__(self):
		super().__init__('box_file_path', 'path_other', enum.DataSourceType.IAF)
		self.iaf_columns = ['Date_supplied', 'Firm_name', 'Entrepreneur_name', 'Sector', 'Number_of_Employees', 'Board_observer_rights',
						'New_non_spin_off_or_existing_firm', 'New_existing', '--', 'Investment_leverage', "Follow_on",
						'Angel', 'VC', 'Amount_of_other_dilutive_financing', 'Total', 'Follow_on_closed_cumulative', 'Non_dilutive', 'Term_loan',
						'IRAP', 'FedDev', 'SR&ED', 'OCE', 'Other', 'Total', 'Non-dilutive(cumulative)', '--',
						'Business_Advisory_Intellectual_Property', 'Number_of_Business_Plans_Developed', 'Number_of_prototypes_developed3',
						'Number_of_new_patents_applied_For4', 'Number_of_New_Patents_Received', 'Other_non_patent_IP5', '--', 'Talent_Attracted', 'Number_of_finance_accounting',
						'Number_of_marketing_sales', 'Number_of_technical_scientific', 'CEO_COO', 'Customer_service_support', 'Others', '  Total', '--',
						'Economic_impact, Products, Services', 'Current_year_estimated_sales', 'Revenue_year_to_date', 'Net_working_capital', 'Burn_rate',
						'Number_of_new_products_marketed', 'Number_of_new_services_marketed', 'Number_of_new_processes_commercialized',
						'Number_of_new_international_exported_products_services']
		self.summary_columns = ['Company_name', 'Closing_date', 'Committed', 'Disbursed', 'Net_working_capital',
								'Burn_rate', 'Runway(monthly)', 'Current_number_of_employees',
								'Number_of_employees_at_time_of_investment','Follow_on_closed_and_non_dilutive',
								'Follow_on_closed',	'Board_Observer_rights']

	def read_iaf_files(self):
		self.data, summary = self.file.read_source_file(enum.FileType.SPREAD_SHEET, enum.MDCDataSource.IAF, enum.Combine.FOR_NONE.value)
		self.data.columns = self.iaf_columns
		summary.columns = self.summary_columns
		self.data = self.data.drop(columns=['--', 'Total'])
		self.file.save_as_csv(self.data, 'IAF_FOR_ETL_CSV.xlsx', str(os.getcwd()) + '/IAF_DIGEST')
		self.file.save_as_csv(summary, 'IAF_FOR_ETL_CSV_SUMMARY.xlsx', os.getcwd())


if __name__ == '__main__':
	iaf = IAF()
	iaf.read_iaf_files()