import pandas as pd
import Shared.datasource as ds
from Shared.common import Common as common
from Shared.file_service import FileService
import os
import Shared.enums as enum
import decimal


class DataWareHouse(ds.DataSource):

	def __init__(self):
		super().__init__('box_file_path', 'path_dw', enum.DataSourceType.DATA_CATALYST)

	def fact_ric_company(self):
		sql_select = '''
						SELECT
						RICCompanyDataID   AS ID,
						CompanyID ,
						DataSourceID,
						BatchID ,
						DateID,
						IntakeDate,
						AdvisoryServicesHours ,
						VolunteerMentorHours,
						AnnualRevenue,
						NumberEmployees,
						FundingToDate,
						FundingCurrentQuarter ,
						CASE
						WHEN Stage LIKE '%Idea%' OR Stage LIKE '%0%'
						  THEN 1
						WHEN Stage LIKE '%Discovery%' OR Stage LIKE '%1%'
						  THEN 2
						WHEN Stage LIKE '%Validation%' OR Stage LIKE '%2%'
						  THEN 3
						WHEN Stage LIKE '%Efficiency%' OR Stage LIKE '%3%'
						  THEN 4
						WHEN Stage LIKE '%scale%' OR Stage LIKE '%4%'
						  THEN 5
						WHEN Stage IS NULL OR Stage = '0' OR Stage = ''
						  THEN 6
						ELSE NULL END                            AS [Stage],
						CASE
						WHEN IndustrySector LIKE '%Advanced Manufacturing%'
							 OR IndustrySector LIKE '%Adv. Materials%'
							 OR IndustrySector LIKE '%materials%'
							 OR IndustrySector LIKE '%Manufactur%'
						  THEN 1
						WHEN IndustrySector LIKE '%agricult%'
							 OR IndustrySector LIKE '%agro%'
						  THEN 2
						WHEN IndustrySector LIKE '%Clean%Tech%'
							 OR IndustrySector LIKE '%energy%'
							 OR IndustrySector LIKE '%recycl%'
							 OR IndustrySector LIKE '%water%'
							 OR IndustrySector LIKE '%green%energy%'
						  THEN 3
						WHEN IndustrySector LIKE '%ICT%'
							 OR IndustrySector LIKE '%Digital%Media%'
							 OR IndustrySector LIKE '%app%'
							 OR IndustrySector LIKE '%entertainment%'
							 OR IndustrySector LIKE '%hardware%'
							 OR IndustrySector LIKE '%software%'
						  THEN 4
						WHEN IndustrySector LIKE '%Education%'
						  THEN 5
						WHEN IndustrySector LIKE '%Financial%'
						  THEN 6
						WHEN IndustrySector LIKE '%Food%' OR IndustrySector LIKE '%Beverage%'
						  THEN 7
						WHEN IndustrySector LIKE '%Forestry%'
						  THEN 8
						WHEN IndustrySector LIKE '%Life%Science%'
							 OR IndustrySector LIKE '%health%'
							 OR IndustrySector LIKE '%wellness%'
							 OR IndustrySector LIKE '%medical%'
							 OR IndustrySector LIKE '%pharma%'
						  THEN 9
						WHEN IndustrySector LIKE '%Mining%'
						  THEN 10
						WHEN IndustrySector LIKE '%Other%'
						  THEN 11
						WHEN IndustrySector LIKE '%Tourism%' OR IndustrySector LIKE '%culture%'
						  THEN 12
						WHEN IndustrySector IS NULL
						  THEN NULL
						ELSE 11 END                              AS IndustrySector,
						CASE
						WHEN Youth IN ('0', 'n', 'No', '')
						  THEN 0
						WHEN Youth IN ('1', 'y', 'Yes')
						  THEN 1
						ELSE NULL END                            AS [Youth],
						CASE
						WHEN HighPotential IN ('0', 'n', 'No', '')
						  THEN 0
						WHEN HighPotential IN ('1', 'y', 'Yes', 'High')
						  THEN 1
						ELSE NULL END                            AS [HighPotential],
						CASE
						WHEN SocialEnterprise IN ('N', 'No', '')
						  THEN 0
						WHEN SocialEnterprise IN ('Y', 'Yes')
						  THEN 1
						ELSE NULL END                            AS [SocialEnterprise],
						CONVERT(int,RIGHT(FiscalQuarter,1)) AS FiscalQuarter,
						CONVERT(int, FiscalYear) AS FiscalYear,
						CreateDate,
						ModifiedDate
						FROM MDC_DEV.Reporting.FactRICCompanyData
				'''
		self.data = self.db.pandas_read(sql_select)
		self.data['FundingToDate'] = self.data.apply(lambda df: common.scientific_to_decimal(df['FundingToDate']), axis=1)
		self.data['FundingCurrentQuarter'] = self.data.apply(lambda df: common.scientific_to_decimal(df['FundingCurrentQuarter']), axis=1)
		self.data['NID'] = pd.to_datetime(self.data['IntakeDate'])
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


if __name__ == '__main__':
	dw = DataWareHouse()
	dw.get_float_values()