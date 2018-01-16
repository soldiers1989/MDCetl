from Shared.enums import FileType, WorkSheet, DataSourceType, SourceSystemType
from Shared.file_service import FileService as file
from Shared.common import Common
from Shared.db import DB as db
import os
import pandas as pd
import time


class BAPValidate:

	def __init__(self):
		self._path1 = Common.get_config('config.ini', 'box_file_path', 'path_validI')
		self._path2 = Common.get_config('config.ini', 'box_file_path', 'path_validII')
		self.pathQ1 = os.path.join(os.path.expanduser(self._path1))
		self.pathQ2 = os.path.join(os.path.expanduser(self._path2))

		self.year = 2018
		self.Q1 = '\'Q1\''
		self.Q2 = '\'Q2\''

		self.Q1CompanyData_sheet = None
		self.Q2CompanyData_sheet = None

		self.Q1CompanyData = None
		self.Q2CompanyData = None

		self.Q1CompanyData_dc = None
		self.Q2CompanyData_dc = None

		self.Q1CompanyData_fact_ric = None
		self.Q2CompanyData_fact_ric = None

		self.Q1CompanyData_rollup = None
		self.Q2CompanyData_rollup = None

		self.source_file = None
		self.file_list = []

		self.haltech = 'Haltech'

		self.batch = 'SELECT * FROM Config.ImportBatch WHERE Year = {} AND Quarter = {} ' \
					 'AND DataSourceID = {} AND SourceSystemId = {} AND ImportStatusID = 5'
		self.select = 'SELECT * FROM {} WHERE BatchID = {}'
		self.selectQ1 = '''SELECT  [CompanyName]  AS  [Company Name],[ReferenceID] AS [Reference ID],FormerCompanyName AS 
						[Former / Alternate Names],CRABusinessNumber AS [CRA Business Number],Address1  AS  StreetAddress,City,Province,
						Postalcode,Website,StageName AS Stage,RevenueRange AS [Annual Revenue $CAN],CompanyNumberofEmployees AS 
						[Number of Employees],FundingRaisedToDate AS [Funding Raised to Date $CAN],FundingRaisedCurrentQuarter AS 
						[Funding Raised in Current Quarter $CAN],IncorporationDate AS [Date of Incorporation],IntakeDate AS [Date of Intake],
						[High Potential y/n],[Industry_Sector] AS [Industry Sector],[AdvisoryServicesHrs] AS 
						[Number of advisory service hours provided],[VolunteerMentorHrs] AS [Volunteer mentor hours],
						[Youth] AS [Youth y/n],SocialEnterprise AS [Social Enterprise y/n],Quarter,Year AS [Fiscal Year]
						FROM Config.MaRSMaster WHERE BatchID = {}'''

		self.rollup_select = '''
							SELECT DISTINCT 
							DimCompany.CompanyName, 
							DimCompany.CompanyId, 
							FactRICCompanyHoursRolledUp.FiscalQuarter, 
							FactRICCompanyHoursRolledUp.FiscalYear, 
							FactRICCompanyHoursRolledUp.AdvisoryHoursYTD,
					
							LOWER(LEFT(FactRICCompanyHoursRolledUp.HighPotential,1)) AS HighPotential,
							FactRICCompanyHoursRolledUp.DateOfIncorporation,
							DATEDIFF(m, FactRICCompanyHoursRolledUp.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2017-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END AS Age,
							FactRICCompanyHoursRolledUp.AdvisoryThisQuarter AS AdvisoryHours_thisQuarter,
							FactRICCompanyHoursRolledUp.VolunteerThisQuarter AS VolunteerHours_thisQuarter,
							
							CASE WHEN 
								TRY_CAST(FactRICCompanyHoursRolledUp.DateOfIncorporation AS date) IS NOT NULL THEN 
									CASE 
										WHEN
										DATEDIFF(m, FactRICCompanyHoursRolledUp.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2017-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END
										< 7 THEN '0-6 months/pre-startup'
										WHEN
										DATEDIFF(m, FactRICCompanyHoursRolledUp.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2017-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END
										BETWEEN 7 AND 12 THEN '07-12 months'
										WHEN
										DATEDIFF(m, FactRICCompanyHoursRolledUp.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2017-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END
										 BETWEEN 13 AND 24 THEN '13-24 months'
										 WHEN
										DATEDIFF(m, FactRICCompanyHoursRolledUp.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2017-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END
										BETWEEN 25 AND 36 THEN '25-36 months' 
										WHEN
										DATEDIFF(m, FactRICCompanyHoursRolledUp.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2017-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END
										BETWEEN 37 AND 60 THEN '37-60 months'
										WHEN
										DATEDIFF(m, FactRICCompanyHoursRolledUp.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2017-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END
										 > 60 THEN 'Over 60 months'
									ELSE 'older'
									END
								ELSE '0-6 months/pre-startup'
								END AS AgeRangeFriendly,	
				
					        CASE WHEN 
					            DimStagelevel.StageFriendly IS NULL THEN 'Stage 0 - Idea'
					            ELSE StageFriendly
					            END AS StageFriendly,
							tPartnerRIC.Friendly AS RICFriendlyName,
				
							---- Annual Revenue ----
				
							FactRICCompanyHoursRolledup.AnnualRevenue,
						
							CASE WHEN
								TRY_CAST(FactRICCompanyHoursRolledUp.AnnualRevenue AS float) IS NOT NULL THEN
							CASE
								WHEN CAST(FactRICCompanyHoursRolledUp.AnnualRevenue AS float) = 0 OR FactRICCompanyHoursRolledUp.AnnualRevenue IS NULL THEN '0. 0'
								WHEN CAST(FactRICCompanyHoursRolledUp.AnnualRevenue AS float) BETWEEN 100000 AND 499000 THEN '2. 100K-499K'
								WHEN CAST(FactRICCompanyHoursRolledUp.AnnualRevenue AS float) BETWEEN 10000000 AND 49999999 THEN '5. 10M-49.9M'
								WHEN CAST(FactRICCompanyHoursRolledUp.AnnualRevenue AS float) BETWEEN 1 AND 99999 THEN '1. 1-99K'
								WHEN CAST(FactRICCompanyHoursRolledUp.AnnualRevenue AS float) BETWEEN 2000000 AND 9999999 THEN '4. 2M-9.9M'
								WHEN CAST(FactRICCompanyHoursRolledUp.AnnualRevenue AS float) BETWEEN 500000 AND 1999999 THEN '3. 500K-1.9M'
								WHEN CAST(FactRICCompanyHoursRolledUp.AnnualRevenue AS float) >= 500000000 THEN '6. 50M+'
							END
								WHEN LEN(FactRICCompanyHoursRolledUp.AnnualRevenue) < 2 THEN '0. 0'
							ELSE
								CASE WHEN MRevenue.Friendly IS NULL THEN '0. 0'
									ELSE MRevenue.Friendly
							END
							END	AS RevenueRange,
				
							-- Employee Range ---
							FactRICCompanyHoursRolledUp.NumberEmployees,
							--TRY_CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS numeric),
										CASE 
								WHEN TRY_CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) IS NOT NULL THEN 
									CASE
					                    WHEN CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) = 0 THEN 'Employee Range of 0'
					                    WHEN CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) BETWEEN 1 AND 4 THEN 'Employee Range of 001-4'
					                    WHEN CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) BETWEEN 5 AND 9 THEN 'Employee Range of 005-9'
					                    WHEN CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) BETWEEN 10 AND 19 THEN 'Employee Range of 010-19'
					                    WHEN CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) BETWEEN 20 AND 49 THEN 'Employee Range of 020-49'
					                    WHEN CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) BETWEEN 50 AND 99 THEN 'Employee Range of 050-99'
					                    WHEN CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) BETWEEN 100 AND 199 THEN 'Employee Range of 100-199'
					                    WHEN CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) BETWEEN 200 AND 499 THEN 'Employee Range of 200-499'
					                    WHEN CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) >= 500 THEN 'Employee Range of 500 or over'
									END
				                WHEN TRY_CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS float) IS NULL AND FactRICCompanyHoursRolledUp.NumberEmployees != '' THEN MEmploy.Friendly  
				                ELSE 'Employee Range of 0'
							END
							AS EmployeeRange,
						
							-- New Clients Funding ----
				
							FactRICCompanyHoursRolledUp.FundingToDate,
							CASE WHEN TRY_CAST(FactRICCompanyHoursRolledUp.FundingToDate AS float) IS NOT NULL THEN
								CASE
									WHEN CAST(FactRICCompanyHoursRolledUp.FundingToDate AS float) < 1000 OR FactRICCompanyHoursRolledUp.FundingToDate IS NULL THEN '0. 0'
									WHEN CAST(FactRICCompanyHoursRolledUp.FundingToDate AS float) BETWEEN 1000 AND 19999 THEN '1. 1-19K'
									WHEN CAST(FactRICCompanyHoursRolledUp.FundingToDate AS float) BETWEEN 20000 AND 49999 THEN '2. 20K-49K'
									WHEN CAST(FactRICCompanyHoursRolledUp.FundingToDate AS float) BETWEEN 50000 AND 199999 THEN '3. 50K-199K'
									WHEN CAST(FactRICCompanyHoursRolledUp.FundingToDate AS float) BETWEEN 200000 AND 499999 THEN '4. 200K-499K'
									WHEN CAST(FactRICCompanyHoursRolledUp.FundingToDate AS float) BETWEEN 500000 AND 1999999 THEN '5. 500K-1.9M'
									WHEN CAST(FactRICCompanyHoursRolledUp.FundingToDate AS float) BETWEEN 2000000 AND 4999999 THEN '6. 2M-4.9M'
									WHEN CAST(FactRICCompanyHoursRolledUp.FundingToDate AS float) >= 5000000 THEN '7. 5M+'
								END
								WHEN LEN(FactRICCompanyHoursRolledUp.FundingToDate) < 2 THEN '0. 0'
							ELSE
								CASE WHEN MFundToDate.Friendly IS NULL THEN '0. 0'
									ELSE MFundToDate.Friendly
							END
							
							END AS Funding_ToDate, 
				
							TRY_CAST(FactRICCompanyHoursRolledUp.FundingCurrentQuarter AS float) AS Funding_ThisQuarter, 
						
							FactRICCompanyHoursRolledUp.IndustrySector,
							CASE WHEN RICSurvey2016Industry.Lvl2IndustryName IS NOT NULL THEN RICSurvey2016Industry.Lvl2IndustryName
								 ELSE 'Other'
							END AS Lvl2IndustryName
							,
							FactRICCompanyHoursRolledUp.IntakeDate,
				
							---- Intake Fiscal Year ----	
								
						CASE WHEN FactRICCompanyHoursRolledUp.IntakeDate IS NULL OR FactRICCompanyHoursRolledUp.IntakeDate = '' THEN NULL
								ELSE 
							CASE WHEN TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime) IS NOT NULL THEN  --Handle the case where date has format "42034"
								CASE WHEN MONTH(TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime)) BETWEEN 1 AND 3 THEN YEAR(TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime))
						             WHEN MONTH(TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime)) BETWEEN 4 AND 12 THEN YEAR(TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime)) + 1
								END
							ELSE
							CASE WHEN TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE) IS NULL THEN 
								CASE WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 1 AND 3 THEN RIGHT(RTRIM(FactRICCompanyHoursRolledUp.IntakeDate), 4)  
										WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 4 AND 12 THEN RIGHT(RTRIM(FactRICCompanyHoursRolledUp.IntakeDate), 4) + 1
								END
							ELSE
							CASE WHEN TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE) IS NOT NULL THEN
								CASE WHEN TRY_CAST(LEFT(FactRICCompanyHoursRolledUp.IntakeDate,3) AS INT) IS NULL AND TRY_CAST(LEFT(FactRICCompanyHoursRolledUp.IntakeDate,2) AS INT) IS NULL THEN
									CASE WHEN MONTH(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE)) BETWEEN 1 AND 3 THEN YEAR(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE))
						                    WHEN MONTH(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE)) BETWEEN 4 AND 12 THEN YEAR(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE)) + 1
						            END
								ELSE
								CASE WHEN LEFT(IntakeDate, 4) != LEFT(TRY_CAST(INTAKEDATE AS date), 4) THEN 
									CASE WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 1 AND 3 THEN YEAR(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE)) 
									        WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 4 AND 12 THEN YEAR(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE)) + 1
						            END
								ELSE
								CASE WHEN LEFT(IntakeDate, 4) = LEFT(TRY_CAST(INTAKEDATE AS date), 4) OR YEAR(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE)) = LEFT(TRY_CAST(INTAKEDATE AS date), 4) THEN 
						            CASE WHEN MONTH(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE)) BETWEEN 1 AND 3 THEN YEAR(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE))
						                    WHEN MONTH(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE)) BETWEEN 4 AND 12 THEN YEAR(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS DATE)) + 1
						            END
								END
								END
								END
								END
								END
								END
						        END AS IntakeFiscalYear,
				       
				       
						   ---- Intake Fiscal Quarter ----
				
						     CASE WHEN FactRICCompanyHoursRolledUp.IntakeDate IS NULL OR FactRICCompanyHoursRolledUp.IntakeDate = '' THEN NULL
								ELSE
				
								CASE WHEN TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime) IS NOT NULL THEN 
									CASE WHEN MONTH(TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime)) BETWEEN 1 AND 3 THEN '4'
						                 WHEN MONTH(TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime)) BETWEEN 4 AND 6 THEN '1'
						                 WHEN MONTH(TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime)) BETWEEN 7 AND 9 THEN '2'
						                 WHEN MONTH(TRY_CAST(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS float) AS datetime)) BETWEEN 10 AND 12 THEN '3'
						            END
								ELSE
								CASE WHEN TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS date) IS NULL THEN 
										CASE WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 1 AND 3 THEN '4'
						                     WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 4 AND 6 THEN '1'
						                     WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 7 AND 9 THEN '2'
						                     WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 10 AND 12 THEN '3'
									END
								ELSE
								CASE WHEN TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS date) IS NOT NULL THEN
						            CASE WHEN TRY_CAST(LEFT(FactRICCompanyHoursRolledUp.IntakeDate,3) AS INT) IS NOT NULL THEN 
						                CASE WHEN MONTH(FactRICCompanyHoursRolledUp.IntakeDate) BETWEEN 1 AND 3 THEN '4'
						                     WHEN MONTH(FactRICCompanyHoursRolledUp.IntakeDate) BETWEEN 4 AND 6 THEN '1'
						                     WHEN MONTH(FactRICCompanyHoursRolledUp.IntakeDate) BETWEEN 7 AND 9 THEN '2'
						                     WHEN MONTH(FactRICCompanyHoursRolledUp.IntakeDate) BETWEEN 10 AND 12 THEN '3'
						                END
						            ELSE
									CASE WHEN TRY_CAST(LEFT(FactRICCompanyHoursRolledUp.IntakeDate,3) AS INT) IS NULL THEN
										CASE WHEN RIGHT(RTRIM(FactRICCompanyHoursRolledUp.IntakeDate),2) = 'AM' THEN 
											CASE WHEN MONTH(FactRICCompanyHoursRolledUp.IntakeDate) BETWEEN 1 AND 3 THEN '4'
						                         WHEN MONTH(FactRICCompanyHoursRolledUp.IntakeDate) BETWEEN 4 AND 6 THEN '1'
						                         WHEN MONTH(FactRICCompanyHoursRolledUp.IntakeDate) BETWEEN 7 AND 9 THEN '2'
						                         WHEN MONTH(FactRICCompanyHoursRolledUp.IntakeDate) BETWEEN 10 AND 12 THEN '3'
						                    END
										ELSE 
											CASE WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 1 AND 3 THEN '4'
						                         WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 4 AND 6 THEN '1'
						                         WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 7 AND 9 THEN '2'
						                         WHEN TRY_CAST(SUBSTRING(FactRICCompanyHoursRolledUp.IntakeDate,4,2) AS INT) BETWEEN 10 AND 12 THEN '3'
				                            END	
										END
									END	
								END
								END
								END
								END
								END AS IntakeFiscalQuarter,
				
							FactRICCompanyHoursRolledUp.Youth,
							tPartnerRIC.DataSourceID,
							FactRICCompanyHoursRolledUp.VolunteerYTD AS VolunteerHoursYTD,
							LOWER(LEFT(FactRICCompanyHoursRolledUp.SocialEnterprise,1)) AS SocialEnterprise
				
							
				
					    FROM MaRSDataCatalyst.Reporting.FactRICCompanyHoursRolledUp
						LEFT JOIN MaRS.tPartnerRIC ON tPartnerRIC.DataSourceID = FactRICCompanyHoursRolledUp.DataSourceID
						LEFT JOIN Reporting.DimCompany ON FactRICCompanyHoursRolledUp.CompanyID = DimCompany.CompanyID
						LEFT JOIN RICSurveyFlat.RICSurvey2016Industry ON FactRICCompanyHoursRolledUp.IndustrySector = RICSurvey2016Industry.Industry_Sector
						LEFT JOIN Reporting.DimStagelevel ON DimStagelevel.StageLevelName = FactRICCompanyHoursRolledUp.stage
				
						-- Revenue Range ----
						LEFT JOIN Reporting.DimRevenueRange ON FactRICCompanyHoursRolledUp.AnnualRevenue = DimRevenueRange.RevenueRange
						LEFT JOIN Reporting.DimEmployeeRange ON FactRICCompanyHoursRolledUp.NumberEmployees = DimEmployeeRange.Range
				
						-- Employee Range ----
						LEFT JOIN Reporting.DimMetric MRevenue ON DimRevenueRange.MetricID = MRevenue.MetricID
						LEFT JOIN Reporting.DimMetric MEmploy ON DimEmployeeRange.MetricID = MEmploy.MetricID
				
						-- Funding to date ----
						LEFT JOIN Reporting.DimFunding FundToDate ON FactRICCompanyHoursRolledUp.FundingToDate = FundToDate.FundingName
						LEFT JOIN Reporting.DimMetric MFundToDate ON FundToDate.MetricId = MFundToDate.MetricID
				
						-- Funding this quarter ----
						LEFT JOIN Reporting.DimFunding FundThisQ ON FactRICCompanyHoursRolledUp.FundingCurrentQuarter = FundThisQ.FundingName
						LEFT JOIN Reporting.DimMetric MFundThisQ ON FundThisQ.MetricId = MFundThisQ.MetricID
				
				
				WHERE
				
				FactRICCompanyHoursRolledUp.FiscalYear = {} AND FactRICCompanyHoursRolledUp.FiscalQuarter = {} AND FactRICCompanyHoursRolledUp.DataSourceID = 8 

		'''

		self.getDBData()
		self.getSheetData()

	def readFileSource(self, path, fname):
		os.chdir(path)
		self.source_file = os.listdir(path)
		self.file_list = [f for f in self.source_file if f[0:2] != '~$' and (f[-3:] in FileType.SPREAD_SHEET.value or f[-4:] in FileType.SPREAD_SHEET.value)]
		for file in self.file_list:
			if fname in file:
				test = pd.read_excel(file, WorkSheet.bap_company_old.value)
				return test

	def getDBData(self):
		batch1 = db.pandas_read(self.batch.format(self.year, self.Q1, DataSourceType.HAL_TECH.value, SourceSystemType.RICCD_bap.value))['BatchID']
		batch2 = db.pandas_read(self.batch.format(self.year, self.Q2, DataSourceType.HAL_TECH.value, SourceSystemType.RICCD_bap.value))['BatchID']

		self.Q1CompanyData = db.pandas_read(self.selectQ1.format(str(batch1[0]) + ' ORDER BY CompanyName'))
		self.Q2CompanyData = db.pandas_read(self.select.format('Config.CompanyDataRaw', str(batch2[0]) + ' ORDER BY CompanyName'))

		self.Q1CompanyData_fact_ric = db.pandas_read(self.rollup_select.format(self.year, self.Q1))
		self.Q2CompanyData_fact_ric = db.pandas_read(self.rollup_select.format(self.year, self.Q2))

		self.Q1CompanyData_rollup = db.pandas_read(self.select.format('Reporting.FactRICCompanyHoursRolledUp', batch1[0]))
		self.Q2CompanyData_rollup = db.pandas_read(self.select.format('Reporting.FactRICCompanyHoursRolledUp', batch2[0]))

		self.Q1CompanyData_fact_ric = db.pandas_read(self.select.format('Reporting.FactRICCompanyData', batch1[0]))
		self.Q2CompanyData_fact_ric = db.pandas_read(self.select.format('Reporting.FactRICCompanyData', batch2[0]))

		self.Q1CompanyData_rollup = db.pandas_read(self.select.format('Reporting.FactRICCompanyHoursRolledUp', batch1[0]))
		self.Q2CompanyData_rollup = db.pandas_read(self.select.format('Reporting.FactRICCompanyHoursRolledUp', batch2[0]))

	def getSheetData(self):
		self.Q1CompanyData_sheet = self.readFileSource(self.pathQ1, 'Haltech Q1-')
		self.Q2CompanyData_sheet = self.readFileSource(self.pathQ2, 'Haltech Q2-')

	def compareSheetAndMaster(self):
		sheet_columns_one = list(map(lambda x: str(x) + '_sheet', self.Q1CompanyData_sheet.columns))
		self.Q1CompanyData_sheet.columns = sheet_columns_one

		db_columns_one = list(map(lambda x: str(x) + '_db', self.Q1CompanyData.columns))
		self.Q1CompanyData.columns = db_columns_one

		df_one = pd.concat([self.Q1CompanyData.sort_values('Company Name_db'),
							self.Q1CompanyData_sheet.sort_values('Company Name_sheet')], axis=1)
		df_one = df_one[sorted(df_one.columns)]

		sheet_columns_two = list(map(lambda x: str(x) + '_sheet', self.Q2CompanyData_sheet.columns))
		self.Q2CompanyData_sheet.columns = sheet_columns_two

		self.Q2CompanyData = self.Q2CompanyData.iloc[:, 10:]
		db_columns_two = list(map(lambda x: str(x) + '_db', self.Q2CompanyData.columns))
		self.Q2CompanyData.columns = db_columns_two

		df_two = pd.concat([self.Q2CompanyData.sort_values('CompanyName_db'),
							self.Q2CompanyData_sheet.sort_values('Company Name_sheet')], axis=1)
		df_two = df_two[sorted(df_two.columns)]

		self.save_to_csv(df_one, df_two)

	def save_to_csv(self, df_one, df_two):
		os.chdir(self.pathQ2)
		writer = pd.ExcelWriter('HT_Sheet_db_comparison_{}.xlsx'.format(str(time.time())[:-8]))
		self.Q1CompanyData.to_excel(writer, 'Quarter 1 from database', index=False)
		self.Q1CompanyData_sheet.to_excel(writer, 'Quarter 1 from sheet', index=False)
		self.Q2CompanyData.to_excel(writer, 'Quarter 2 from database', index=False)
		self.Q2CompanyData_sheet.to_excel(writer, 'Quarter 2 from sheet', index=False)
		df_one.to_excel(writer, 'Quarter 1 Combined', index=False)
		df_two.to_excel(writer, 'Quarter 2 Combined', index=False)
		self.Q1CompanyData_fact_ric.to_excel(writer, 'FactRICCompany_Q1', index=False)
		self.Q2CompanyData_fact_ric.to_excel(writer, 'FactRICCompany_Q2', index=False)
		self.Q1CompanyData_rollup.to_excel(writer, 'Rollup_Q1', index=False)
		self.Q2CompanyData_rollup.to_excel(writer, 'Rollup_Q2', index=False)
		writer.save()


if __name__ == '__main__':
	bapv = BAPValidate()
	bapv.compareSheetAndMaster()