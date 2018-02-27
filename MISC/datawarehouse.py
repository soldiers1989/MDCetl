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


if __name__ == '__main__':
	dw = DataWareHouse()
	dw.fact_ric_company()