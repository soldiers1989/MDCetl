import pandas as pd
import Shared.datasource as ds
from Shared.common import Common as common
from Shared.file_service import FileService
import os
import Shared.enums as enum
import decimal
from dateutil.parser import parse


class DataWareHouse(ds.DataSource):

	def __init__(self):
		super().__init__('box_file_path', 'path_dw', enum.DataSourceType.DATA_CATALYST)

	def fact_ric_company(self):

		self.data = self.db.pandas_read(enum.SQL.sql_dw_fact_ric_company_data.value)
		# self.data['FundingToDate'] = self.data.apply(lambda df: common.scientific_to_decimal(df['FundingToDate']), axis=1)
		# self.data['FundingCurrentQuarter'] = self.data.apply(lambda df: common.scientific_to_decimal(df['FundingCurrentQuarter']), axis=1)
		# self.data['Intake_Date_New'] = pd.to_datetime(self.data['IntakeDate'])
		print(self.data.head(25))
		self.file.save_as_csv(self.data, 'FACTRICCOMPANY.xlsx', os.getcwd(), 'FACT RIC COMPANY')

	def fact_ric_aggregation(self):
		pass

	def fact_ric_roll_up(self):
		pass

	def get_float_values(self):
		lsts = [2.2e+006, 1e+006, 2.55e+006, 3e+006, 1.57e+006, 1.43e+006, 1.6e+006, 6.62e+006, 2.102e+006, 5.8e+006, 1.7e+007,
				 7.5e+006, 3.2e+006, 3.095e+006, 1.125e+006, 1.872e+006, 2.35e+006, 7.49148e+006, 1.06e+007, 5.3e+006, 2.8378e+006,
				 3.1e+006, 1.8e+006, 1.4e+006, 2.05e+006, 5.383e+006, 2e+006, 1e+007, 1.05e+006, 1.22709e+006, 6e+006, 1.36262e+006,
				 9.066e+006, 4.39e+006, 1.47911e+007, 2.95e+007, 8.6e+006, 2.7315e+006, 4.58417e+006, 1.7e+006, 1.064e+007,
				 2.85368e+006, 5.91e+006, 3.2e+007, 1.275e+006, 1.97e+007, 2.39e+006, 6.5235e+006, 1.12e+007, 1.575e+006, 5e+006,
				 5.38e+006, 1.2e+006, 1.133e+006, 1.25e+007, 1.25e+006, 7.95e+006, 1.72e+006, 5.29e+006, 9.5e+006, 1.19e+006,
				 6.3e+006, 1.03e+006, 1.08e+006, 2.58619e+006, 3.24104e+006, 4e+006, 3.55e+006, 7e+007, 3.5e+006, 7.41196e+006,
				 4.5e+006, 2.0309e+006, 2.5016e+007, 2e+007, 2.27961e+006, 2.5e+006, 1.3e+006, 1.14e+007, 1.78315e+006,
				 8.00348e+006, 3.75e+006, 1.5e+006, 5e+007, 5.7e+006]

		lst = ['2.2e+006', '1e+006', '2.55e+006', '3e+006', '1.57e+006', '1.43e+006', '1.6e+006', '6.62e+006', '2.102e+006',
			   '5.8e+006', '1.7e+007', '7.5e+006', '3.2e+006', '3.095e+006', '1.125e+006', '1.872e+006', '2.35e+006', '7.49148e+006',
			   '1.06e+007', '5.3e+006', '2.8378e+006', '3.1e+006', '1.8e+006', '1.4e+006', '2.05e+006', '5.383e+006', '2e+006',
			   '1e+007', '1.05e+006', '1.22709e+006', '6e+006', '1.36262e+006', '9.066e+006', '4.39e+006', '1.47911e+007',
			   '2.95e+007', '8.6e+006', '2.7315e+006', '4.58417e+006', '1.7e+006', '1.064e+007', '2.85368e+006', '5.91e+006',
			   '3.2e+007', '1.275e+006', '1.97e+007', '2.39e+006', '6.5235e+006', '1.12e+007', '1.575e+006', '5e+006', '5.38e+006',
			   '1.2e+006', '1.133e+006', '1.25e+007', '1.25e+006', '7.95e+006', '1.72e+006', '5.29e+006', '9.5e+006', '1.19e+006',
			   '6.3e+006', '1.03e+006', '1.08e+006', '2.58619e+006', '3.24104e+006', '4e+006', '3.55e+006', '7e+007', '3.5e+006',
			   '7.41196e+006', '4.5e+006', '2.0309e+006', '2.5016e+007', '2e+007', '2.27961e+006', '2.5e+006', '1.3e+006', '1.14e+007',
			   '1.78315e+006', '8.00348e+006', '3.75e+006', '1.5e+006', '5e+007', '5.7e+006']

		for i in range(len(lst)):
			update = 'UPDATE MDC_DEV.Reporting.FactRICCompanyData SET FundingToDate = {} WHERE FundingToDate = \'{}\''.format(float(lst[i]), lst[i])
			print(update)

	def new_ventures(self):
		sql = '''SELECT CompanyID,CompanyName,BasicName,BatchID,
			NULL as DateOfIncorporation, NULL AS VentureType, Description, Website,
			Email, ISNULL(Phone, Phone2),Fax, ModifiedDate, CreateDate
			FROM MDC_DEV.Reporting.DimCompany
			WHERE BatchID NOT IN (118,227, 297,374, 395, 561, 3496, 3497,3498, 3499)
			ORDER BY CompanyID'''
		self.data = self.db.pandas_read(sql)
		print(self.data.head())

	def intake_date_conversion(self):
		sql_select = 'SELECT DISTINCT IntakeDate FROM MDC_DEV.Reporting.FactRICCompanyHoursRolledUp ' \
					 'WHERE FactRICCompanyHoursRolledUp.IntakeDate LIKE \'%/%\' ' \
					 'OR FactRICCompanyHoursRolledUp.IntakeDate LIKE \'%A%\''
		sql_select_second = 'SELECT Distinct  TOP 115 IntakeDate FROM MDC_DEV.Reporting.FactRICCompanyHoursRolledUp WHERE IntakeDate IS NOT NULL ORDER BY 1'
		sql_update = 'UPDATE R SET R.IntakeDate = \'{}\' FROM MDC_DEV.Reporting.FactRICCompanyHoursRolledUp R WHERE R.IntakeDate = \'{}\';'

		sql_flaot_dates = [40157.973611,  40548.638889,  40704.756944,
						   40759.447917,  40795.654861,  40912.365278,
						   41009.918056,  41255.415972,  41345.621528,
						   41345.622222,  41400.595833,  41400.626389,
						   41581.5875,    41649.438194,  41649.579167,
						   41705.751389,  41710.553472,  41791.514583,
						   41791.551389,  41830.394444,  41888.641667,
						   41921.417361,  41947.626389,  41950.452083,
						   41982.586111,  42066.573611,  42106.840972,
						   42126.740972,  42131.002083,  42132.594444,
						   42160.629861,  42162.664583,  42167.490278,
						   42192.71875,   42225.352778,  42225.724306,
						   42228.845139,  42259.616667,  42285.594444,
						   42316.084028,  42318.119444,  42339.480556,
						   42341.614583,  42371.423611,  42371.729167,
						   42408,         42434.393056,  42434.70625,
						   42461.480556,  42464.513889,  42465.46875,
						   42467.48125,   42492.7375,    42495.611111,
						   42524.582639,  42552.523611,  42560,
						   42585.710417,  42589,         42675.502083,
						   42676.667361,  42712,         42713,
						   42714,                                             ]

		self.data = self.db.pandas_read(sql_select_second)
		for k, l in self.data.iterrows():
			update = sql_update.format(str(parse(l[0]))[:10], l[0])
			# self.db.execute(sql_update)
			print(update)

	def change_scientific_format_to_float(self):
		annual_revenue = ['1.034e+006', '1.05e+006', '1.11649e+006', '1.18369e+006', '1.1e+006', '1.209e+006', '1.239e+006', '1.24802e+006', '1.25451e+006', '1.25e+006',
				'1.2e+006', '1.2e+007', '1.35e+006', '1.3e+006', '1.4425e+006', '1.45e+006', '1.465e+006', '1.4e+006','1.4e+007','1.54e+006','1.575e+006','1.5e+006',
				'1.955e+006','1e+006','1e+007','2.1e+006','2.25e+006','2.36e+007','2.3e+006','2.5e+006','2.7e+006','2.9918e+006','2e+006', '3.2e+006', '3.32818e+006',
				'3.482e+006', '3.52e+006', '3.5e+006', '3.6e+007', '3.915e+006', '3e+006', '4.25e+006', '4.52157e+006', '4e+006', '5.25358e+006', '5.4e+006',
				'6.10978e+006', '6e+006']

		funding_quarter = ['1.061e+006', '1.08e+006', '1.105e+006', '1.1e+006',
					   '1.2e+007', '1e+006', '1e+007', '2.1e+006', '2.4e+006',
					   '2e+006', '4.25096e+006', '6.55e+006']
		funding_roll_up = ['1e+006', '1e+006        ', '1e+006        ', '1.2e+006      ', '1.2e+006      ',
					   '1.2e+006      ', '3.5e+006      ', '2.5e+006      ',
					   '2.5e+006      ', '4.52157e+006  ', '4.52157e+006  ', '4.52157e+006  ', '2.5e+006      ',
					   '2.5e+006      ', '2.5e+006      ', '2.9918e+006   ',
					   '2.9918e+006   ', '2.9918e+006   ', '2.9918e+006   ', '1.2e+006      ', '1.2e+006      ',
					   '1.2e+006      ', '1.2e+006      ', '1.2e+006      ',
					   '1.2e+007      ', '1.2e+007      ', '1.2e+007      ', '1.2e+007      ', '2.7e+006      ',
					   '2.7e+006      ', '1.5e+006      ', '1.5e+006      ',
					   '1.5e+006      ', '1.5e+006      ', '1e+006        ', '1e+006        ', '1e+006        ',
					   '1.5e+006      ', '1.5e+006      ', '1.5e+006      ',
					   '1.2e+006      ', '1.2e+006      ', '1.2e+006      ', '3.2e+006      ', '3.2e+006      ',
					   '3.2e+006      ', '1.4425e+006   ', '1.4425e+006   ',
					   '1.4425e+006   ', '3.5e+006      ', '3.5e+006      ', '4.52157e+006  ', '4.52157e+006  ',
					   '4e+006        ', '4e+006        ', '2.5e+006      ',
					   '2.5e+006      ', '6.10978e+006  ', '6.10978e+006  ', '2e+006        ', '2e+006        ',
					   '1e+007        ', '1e+007        ', '1.5e+006      ',
					   '1.5e+006      ', '1e+006        ', '1e+006        ', '3.5e+006      ', '3.5e+006      ',
					   '4.52157e+006  ', '4.52157e+006  ', '4e+006        ',
					   '4e+006        ', '2.5e+006      ', '2.5e+006      ', '6.10978e+006  ', '6.10978e+006  ',
					   '2e+006        ', '2e+006        ', '1e+007        ',
					   '1e+007        ', '1.5e+006      ', '1.5e+006      ', '1e+006        ', '1e+006        ',
					   '3.5e+006      ', '3.5e+006      ', '4.52157e+006  ',
					   '4.52157e+006  ', '4e+006        ', '4e+006        ', '2.5e+006      ', '2.5e+006      ',
					   '6.10978e+006  ', '6.10978e+006  ', '2e+006        ',
					   '2e+006        ', '1e+007        ', '1e+007        ', '1.5e+006      ', '1.5e+006      ',
					   '1e+006        ', '1e+006        ', '3.5e+006      ',
					   '3.5e+006      ', '4.52157e+006  ', '4.52157e+006  ', '4e+006        ', '4e+006        ',
					   '2.5e+006      ', '2.5e+006      ', '6.10978e+006  ',
					   '6.10978e+006  ', '2e+006        ', '2e+006        ', '1e+007        ', '1e+007        ',
					   '1.5e+006      ', '1.5e+006      ', '1e+006        ',
					   '1e+006        ', '3.5e+006      ', '3.5e+006      ', '3.5e+006      ', '4.52157e+006  ',
					   '4.52157e+006  ', '4.52157e+006  ', '4e+006        ',
					   '4e+006        ', '4e+006        ', '2.5e+006      ', '2.5e+006      ', '2.5e+006      ',
					   '6.10978e+006  ', '6.10978e+006  ', '6.10978e+006  ',
					   '2e+006        ', '2e+006        ', '2e+006        ', '1e+007        ', '1e+007        ',
					   '1e+007        ', '1.5e+006      ', '1.5e+006      ',
					   '1.5e+006      ', '1e+006        ', '1e+006        ', '1e+006        ']
		funding_to_date_roll_up = ['1.03e+006', '1.05e+006', '1.064e+007',
							   '1.06e+007', '1.08e+006', '1.125e+006',
							   '1.12e+007', '1.133e+006', '1.14e+007',
							   '1.19e+006', '1.22709e+006', '1.25e+006',
							   '1.25e+007', '1.275e+006', '1.2e+006',
							   '1.36262e+006', '1.3e+006', '1.43e+006',
							   '1.47911e+007', '1.4e+006', '1.575e+006',
							   '1.57e+006', '1.5e+006', '1.6e+006',
							   '1.72e+006', '1.78315e+006', '1.7e+006',
							   '1.7e+007', '1.872e+006', '1.8e+006',
							   '1.97e+007', '1e+006', '1e+007',
							   '2.0309e+006', '2.05e+006', '2.102e+006',
							   '2.27961e+006', '2.2e+006', '2.35e+006',
							   '2.39e+006', '2.5016e+007', '2.55e+006',
							   '2.58619e+006', '2.5e+006', '2.7315e+006',
							   '2.8378e+006', '2.85368e+006', '2.95e+007',
							   '2e+006', '2e+007', '3.095e+006',
							   '3.1e+006', '3.24104e+006', '3.2e+006',
							   '3.2e+007', '3.55e+006', '3.5e+006',
							   '3.75e+006', '3e+006', '4.39e+006',
							   '4.58417e+006', '4.5e+006', '4e+006',
							   '5.29e+006', '5.383e+006', '5.38e+006',
							   '5.3e+006', '5.7e+006', '5.8e+006',
							   '5.91e+006', '5e+006', '5e+007',
							   '6.3e+006', '6.5235e+006', '6.62e+006',
							   '6e+006', '7.41196e+006', '7.49148e+006',
							   '7.5e+006', '7.95e+006', '7e+007',
							   '8.00348e+006', '8.6e+006', '9.066e+006',
							   '9.5e+006']
		funding_this_quarter = ['1.08e+006',
							'1.105e+006',
							'1.2e+007',
							'1e+006',
							'1e+007',
							'2.1e+006',
							'2.4e+006',
							'2e+006',
							'4.25096e+006',
							'6.55e+006']

		annual_revenue_roll_up = ['1.034e+006', '1.11649e+006', '1.1e+006', '1.209e+006', '1.239e+006', '1.24802e+006',
							  '1.25451e+006', '1.25e+006', '1.2e+006', '1.2e+007', '1.35e+006', '1.3e+006',
							  '1.4425e+006', '1.45e+006', '1.54e+006', '1.5e+006', '1.955e+006', '1e+006', '1e+007',
							  '2.25e+006', '2.5e+006', '2.7e+006', '2.9918e+006', '2e+006', '3.2e+006', '3.32818e+006',
							  '3.482e+006', '3.52e+006', '3.5e+006', '3.6e+007', '3e+006', '4.25e+006', '4.52157e+006',
							  '4e+006', '5.4e+006', '6.10978e+006', '6e+006']


		funding_current_quarter = ['1.08e+006', '1.105e+006', '1.2e+007', '1e+006', '1e+007', '2.1e+006', '2.4e+006', '2e+006','4.25096e+006', '6.55e+006']

		funding_to_date_mdcreport = [
			'1.03e+006',
			'1.05e+006',
			'1.064e+007',
			'1.06e+007',
			'1.08e+006',
			'1.125e+006',
			'1.12e+007',
			'1.133e+006',
			'1.14e+007',
			'1.19e+006',
			'1.22709e+006',
			'1.25e+006',
			'1.25e+007',
			'1.275e+006',
			'1.2e+006',
			'1.36262e+006',
			'1.3e+006',
			'1.43e+006',
			'1.47911e+007',
			'1.4e+006',
			'1.575e+006',
			'1.57e+006',
			'1.5e+006',
			'1.6e+006',
			'1.72e+006',
			'1.78315e+006',
			'1.7e+006',
			'1.7e+007',
			'1.872e+006',
			'1.8e+006',
			'1.97e+007',
			'1e+006',
			'1e+007',
			'2.0309e+006',
			'2.05e+006',
			'2.102e+006',
			'2.27961e+006',
			'2.2e+006',
			'2.35e+006',
			'2.39e+006',
			'2.5016e+007',
			'2.55e+006',
			'2.58619e+006',
			'2.5e+006',
			'2.7315e+006',
			'2.8378e+006',
			'2.85368e+006',
			'2.95e+007',
			'2e+006',
			'2e+007',
			'3.095e+006',
			'3.1e+006',
			'3.24104e+006',
			'3.2e+006',
			'3.2e+007',
			'3.55e+006',
			'3.5e+006',
			'3.75e+006',
			'3e+006',
			'4.39e+006',
			'4.58417e+006',
			'4.5e+006',
			'4e+006',
			'5.29e+006',
			'5.383e+006',
			'5.38e+006',
			'5.3e+006',
			'5.7e+006',
			'5.8e+006',
			'5.91e+006',
			'5e+006',
			'5e+007',
			'6.3e+006',
			'6.5235e+006',
			'6.62e+006',
			'6e+006',
			'7.41196e+006',
			'7.49148e+006',
			'7.5e+006',
			'7.95e+006',
			'7e+007',
			'8.00348e+006',
			'8.6e+006',
			'9.066e+006',
			'9.5e+006'
			]

		for ls in funding_to_date_mdcreport:
			# print('SELECT FundingToDate, * FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp WHERE FundingToDate =\'{}\''.format(ls))
			print(' UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET FundingToDate = \'{}\' WHERE FundingToDate  = \'{}\''.format(float(ls), ls))


	def fact_ric_company_intake_date(self):
		k = 0
		# df = self.db.pandas_read('SELECT RICCompanyDataID, IntakeDate FROM MDC_DEV.Reporting.FactRICCompanyData WHERE IntakeDate IS NOT NULL AND IntakeDate <> \'\'')
		t1 = [18862, 18933, 18937, 18855, 19004, 18985, 18901, 18926, 18995, 18976,
			 18983, 18858, 18961, 19005, 18909, 18939, 18879, 18984, 18981, 18990,
			 18877, 18854, 18968, 19002, 18962, 18925, 18874, 18880, 18994, 19007, 18987]
		t2 = [ 18917, 18876, 18912, 18856, 18989, 18884, 18860, 18861, 18975, 18889,
			   18953, 19000, 18882, 19008, 18897, 18978, 18935, 18887, 18967, 18970,
			   18966, 18927, 18866, 18906, 18969, 18946, 18956, 18922, 18890, 18900, 18942, 18965, 19006, 18863]
		# dft = self.db.pandas_read('SELECT RICCompanyDataID, IntakeDate FROM MDC_DEV.Reporting.FactRICCompanyData WHERE RICCompanyDataID IN {}'.format(tuple(t2)))
		# df_string = self.db.pandas_read('''SELECT  RICCompanyDataID, IntakeDate FROM MDC_DEV.Reporting.FactRICCompanyData
		# 								   WHERE IntakeDate IS NOT NULL AND IntakeDate <> '' AND IntakeDate NOT LIKE '%-%' AND IntakeDate NOT LIKE \'%/%\'''')
		dft = self.db.pandas_read('''SELECT F.RICCompanyDataID, F.IntakeDate FROM MDC_DEV.Reporting.FactRICCompanyData F
									 INNER JOIN MDCDW.dbo.FactRICCompany N ON F.RICCompanyDataID = N.RICCompanyDataID
									 WHERE N.IntakeDate IS NULL AND F.IntakeDate IS NOT NULL''')
		update = 'UPDATE MDCDW.dbo.FactRICCompany SET IntakeDate = \'{}\' WHERE RICCompanyDataID = {} -- {}'
		for i, j in dft.iterrows():
			k=k+1
			# seconds = (float(j[1]) - 25569) * 86400
			# intake_date = datetime.utcfromtimestamp(seconds)
			# print('{} >>> {}'.format(j[1], intake_date))
			print(str(j[1]))
			try:
				intake_date = parser.parse(j[1])
			except:
				intake_date = None
			update_sql = update.format(intake_date, j[0], j[1])
			print(update_sql)
			try:
				self.db.execute(update_sql)
			except Exception as ex:
				print('-' * 300)
				print(ex)
				print('-' * 300)
				print(df_string.head())


if __name__ == '__main__':
	dw = DataWareHouse()
	# dw.change_scientific_format_to_float()
	dw.intake_date_conversion()