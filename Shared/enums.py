from enum import Enum


class DataSourceType(Enum):
	NORCAT = 1
	WE_TECH = 2
	SSMIC = 3
	COMMUNI_TECH = 4
	IION = 5
	TECH_ALLIANCE = 6
	MaRS = 7
	HAL_TECH = 8
	RIC_CENTER = 9
	SPARK_CENTER = 10
	VENTURE_LAB = 11
	INNOVATION_FACTORY = 12
	LAUNCH_LAB = 13
	NWOIC = 14
	INNOVATION_GUELPH = 15
	INVEST_OTTAWA = 16
	INNOVATE_NIAGARA = 17
	DATA_CATALYST = 18
	IAF = 19
	CRUNCH_BASE = 20
	CVCA = 21
	GUST = 22
	OCE = 23
	CB_INSIGHTS = 24
	ANGEL_LIST = 25
	JLABS_TORONTO = 26
	MARKET_INTEL = 27
	BAP = 30
	EDUCATION = 31
	IRAP = 32
	ENDEAVOR = 33
	FEDERAL = 34
	THOMSON_REUTERS = 35


class SourceSystemType(Enum):
	UNK = 0
	SF = 1
	RICCD = 2
	RICPD = 3
	RICAGG_TC = 4
	RICAGG_CP = 5
	RICAGG_AS = 6
	JW = 7
	TR_D = 8
	TR_C = 9
	SG = 10
	IRAP = 11
	SURVEY_FLUID14 = 12
	RICAGG_TC15 = 13
	RICAGG_CP15 = 14
	RICAGG_AS15 = 15
	RICCD15 = 16
	RICPD15 = 17
	SURVEY_FLUID16 = 18
	CB = 19
	CVCA = 20
	Gust_Orgs = 21
	Gust_Reltshps = 22
	CB_Insights = 23
	AngelList = 24
	RICAGG_AS16 = 25
	RICAGG_AS16Y = 26
	RICAGG_CP16 = 27
	RICAGG_CP16Y = 28
	RICAGG_TC16 = 29
	RICAGG_TC16Y = 30
	RICPD_bap = 31
	RICPDY_bap = 32
	RICCD_bap = 33
	MARKET_INTELLIGENCE = 34
	JLABS = 35
	BAP = 36
	EDUCATION = 37
	SURVEY_TARGET = 38
	CONNENTFOUNDED = 39
	CONNENTPERSON = 40
	CONNENTPROJECTSORG = 41
	OVERLAP = 42
	FEDERAL_EMPLOYMENT = 43
	FEDERAL_REVENUE = 44
	SURVEYNORMSFY17 = 45
	SURVEY_FLAT_FY17 = 46
	FEDERAL_FUNDING = 47
	OSVP = 48
	RICACD_bap = 49


class CompanyStage(Enum):
	UNKNOWN = 1
	IDEATION = 4
	DISCOVERY = 2
	VALIDATION = 6
	EFFICIENCY = 3
	SCALE = 5


class CompanyIndustry(Enum):
	ADVANCED = 1
	MATERIALS_AND_MANUFACTURING = 2
	AGRICULTURE = 3
	CLEAN = 4
	TECHNOLOGIES = 5
	DIGITAL = 6
	MEDIA_AND_ICT = 7
	EDUCATION = 8
	FINANCIAL = 9
	SERVICES = 10
	FOOD_AND_BEVERAGE = 11
	FORESTRY = 12
	LIFE = 13
	SCIENCES_AND_ADVANCED = 14
	HEALTH = 15
	MINING = 16
	OTHER = 17
	TOURISM_AND_CULTURE = 18


class ImportStatus(Enum):
	STARTED = 1
	IN_PROGRESS = 2
	STAGED = 3
	READY_TO_LOAD = 4
	COMPLETED = 5
	FAILED = 6
	DELETED = 7
	STAGING_IN_PROGRESS = 8
	DW_LOAD_IN_PROGRESS = 9
	LOADED = 10
	IMPORTING = 11
	IMPORTED = 12
	DELETED_FROM_STAGING = 13
	DELETED_FROM_REPORTING = 14


class FileType(Enum):
	SPREAD_SHEET = ['xls', 'xlsx']
	CSV = ['csv']
	PDF = ['pdf']
	WORD = ['doc', 'docx']


class DataSource(Enum):
	BAP = 1
	CBINSIGHT = 2
	CRUNCHBASE = 3
	CVCA = 4
	IAF = 5
	OSVP = 6
	SURVEY = 7
	TDW = 8
	TR = 9
	WS = 10
	OTHER = 11


class WorkSheet(Enum):
	bap_program = 'csv_program16'
	bap_program_youth = 'csv_program16_youth'
	bap_company = 'Quarterly Company Data'
	bap_company_annual = 'Annual Company Data'
	bap_program_final = 'Program'
	bap_program_youth_final = 'Program Youth'
	bap_company_old = 'Company Data'


class FileName(Enum):
	bap_combined = 'ALL_RICS_BAP_FY{}Q{}.xlsx'


class Table(Enum):
	company_program = 'Config.CompanyProgram'
	company_program_youth = 'Config.CompanyProgramAgg'
	company_data = 'Config.CompanyDataRaw'

	batch = 'Config.ImportBatch'
	batch_log = 'Config.ImportBatchLog'

	fact_ric_aggregation = 'Reporting.FactRICAggregation'


class SQL(Enum):
	sql_program_insert = 'INSERT INTO[Config].[CompanyAggProgram] ' \
						 'Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_program_youth_insert = 'INSERT INTO[Config].[CompanyAggProgramYouth] Values (?,?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_bap_company_insert = 'INSERT INTO [Config].[CompanyDataRaw] ' \
							 'Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_bap_company_annual_insert = 'INSERT INTO [BAP].[AnnualCompanyData] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_bap_distict_batch = 'SELECT DISTINCT FileName,Path, SourceSystem, DataSource,WorkSheetName ' \
							'FROM {} Year = {} AND Quarter = \'{}\''

	sql_bap_fact_ric_company_data_source = '''SELECT CompanyID, DataSource, BatchID,'20170930' AS DateID,DateOfIntake,IntakeDate, 
				NULL AS [StageLevelID],NULL AS [SizeID], 'NULL' AS Age,HighPotential, NULL AS [DevelopmentID], 
				NumberOfAdvisoryServiceHoursProvided,VolunteerMentorHours, GETDATE() AS [Modified Date], 
				GETDATE() AS [CreatedDate], Youth,StreetAddress, City, Province, PostalCode, Website,Stage,
				AnualRevenueCAN,NumberOfEmployees,FundingRaisedToDateCAN,FundingRaisedInCurrentQuarterCAN,
				DateOfIncorporation,IndustrySector, SocialEnterprise, [Quarter], [Year] 
				FROM [Config].[CompanyDataRaw] WHERE CompanyID IS NOT NULL  AND BatchID IN {}'''
	sql_bap_fact_ric_company_insert = 'INSERT INTO [Reporting].[FactRICCompanyData] ' \
									  'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

	sql_bap_fact_ric_aggregation_insert = 'INSERT INTO [Reporting].[FactRICAggregation] VALUES (?,?,?,?,?,?,?,?)'

	sql_company_aggregate_program = 'SELECT  * FROM [Config].[CompanyAggProgram] WHERE BatchID IN {}'
	sql_company_aggregate_program_youth = 'SELECT  * FROM [Config].[CompanyAggProgramYouth] WHERE BatchID IN {}'

	sql_postal_code_insert = 'INSERT INTO [dbo].[DimPostalCode] VALUES (?,?,?,?,?,?,?,?)'

	sql_bap_fact_ric_company = ''' 
	SELECT [RICCompanyDataID]
	  ,[CompanyID]
	  ,[DataSourceID]
	  ,[BatchID]
	  ,[DateID]
	  ,[IntakeDate]
	  ,[StageLevelID]
	  ,[SizeID]
	  ,[Age]
	  ,[HighPotential]
	  ,[DevelopmentID]
	  ,ISNULL([AdvisoryServicesHours],0) AS AdvisoryServicesHours
	  ,ISNULL([VolunteerMentorHours],0) AS VolunteerMentorHours
	  ,[ModifiedDate]
	  ,[CreateDate]
	  ,[Youth]
	  ,[StreetAddress]
	  ,[City]
	  ,[Province]
	  ,[PostalCode]
	  ,[Website]
	  ,[Stage]
	  ,[AnnualRevenue]
	  ,[NumberEmployees]
	  ,[FundingToDate]
	  ,ISNULL([FundingCurrentQuarter],0) AS FundingCurrentQuarter
	  ,[DateOfIncorporation]
	  ,[IndustrySector]
	  ,[SocialEnterprise]
	  ,[FiscalQuarter]
	  ,[FiscalYear]
	FROM [Reporting].[FactRICCompanyData]
	WHERE FiscalYear = {}'''

	sql_bap_report_company_ds_quarter = '''SELECT CompanyID,DataSourceID,  MIN(RIGHT(FiscalQuarter,1)) AS MinFQ 
											FROM Reporting.FactRICCompanyData 
											WHERE FISCALYEAR = {}
											GROUP BY CompanyID, DataSourceID
											ORDER BY CompanyID, min(RIGHT(FiscalQuarter, 1)) '''

	sql_bap_report_all_quarter = '''
				SELECT DISTINCT 
				RIGHT(FiscalQuarter, 1) AS FiscalQuarter,
				FiscalYear
				FROM Reporting.FactRICCompanyData
				WHERE FiscalYear = {} AND RIGHT(FiscalQuarter, 1) <= {}
				ORDER BY RIGHT(FiscalQuarter, 1) ASC
				'''
	sql_bap_distict_company = '''SELECT DISTINCT CompanyID FROM Reporting.FactRICCompanyData WHERE BatchID IN {}'''
	sql_industry_list_table = 'SELECT [Industry_Sector],[Lvl2IndustryName] FROM [RICSurveyFlat].[RICSurvey2016Industry]'

	sql_columns = 'SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE Table_Schema = \'{}\''

	sql_rollup_select = '''
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


class Columns(Enum):
	ric_aggregation_id = 'RICAggregationID'
	clmn_fact_ric_rolled_up = ['DataSourceID',
							   'CompanyID',
							   'MinDate',
							   'CurrentDate',
							   'VolunteerYTD',
							   'AdvisoryHoursYTD',
							   'VolunteerThisQuarter',
							   'AdvisoryThisQuarter',
							   'FiscalQuartecr',
							   'BatchID',
							   'ModifiedDate',
							   'SocialEnterprise',
							   'Stage',
							   'HighPotential',
							   'Lvl2IndustryName',
							   'FiscalYear',
							   'Youth',
							   'DateOfIncorporation',
							   'AnnualRevenue',
							   'NumberEmployees',
							   'FundingToDate',
							   'IndustrySector',
							   'IntakeDate',
							   'FundingCurrentQuarter']


class VAR(Enum):
	data = 'data'
	properties = 'properties'
	items = 'items'
	item = 'item'
	paging = 'paging'
	uuid = 'uuid'
	total_items= 'total_items'
	number_of_pages = 'number_of_pages'
	relationships = 'relationships'
	type = 'type'
	cardinality = 'cardinality'
	primary_image = 'primary_image'
	founders = 'founders'
	featured_team = 'featured_team'
	current_team = 'current_team'
	past_team = 'past_team'
	bmad = 'board_members_and_advisors'
	investors = 'investors'
	partners = 'partners'
	owned_by = 'owned_by'
	sub_orgs = 'sub_organizations'
	headquarters = 'headquarters'
	offices = 'offices'
	products = 'products'
	categories = 'categories'
	customers = 'customers'
	competitors = 'competitors'
	members = 'members'
	memberships = 'memberships'
	funding_rounds = 'funding_rounds'
	investments = 'investments'
	acuisitions = 'acquisitions'
	acquired_by = 'acquired_by'
	ipo = 'ipo'
	funds = 'funds'
	websites = 'websites'
	images = 'images'
	videos = 'videos'
	news = 'news'
	person = 'person'
	invested_in = 'invested_in'


class CONSTANTS(Enum):
	organization_summary = 'ORGANIZATION_SUMMARY'
	people_summary = 'PEOPLE_SUMMARY'
	categories = 'CATEGORIES'
	locations = 'LOCATIONS'
	get = 'GET'


class PATH(Enum):
	DATA = 0
	QA = 1
	COMBINED = 2
	ETL = 3


class TeamStatus(Enum):
	Featured = 'Featured Team'
	Past = 'Past Team'
	Current = 'Current Team'
	Board = 'Board Member and Adviser'


class Schema(Enum):
	bap = 'BAP'
	crunchbase = 'CRUNCHBASE'
	config = 'Config'


class BapSummary(Enum):
	new_clients = 'Number of New Clients'
	social_enterprise = 'Social Enterprises'
	clients_helped = 'Number of Clients who got help'
	stage_zero = 'Stage 0'
	stage_one = 'Stage 1'
	stage_two = 'Stage 2'
	stage_three = 'Stage 3'
	stage_four = 'Stage 4'

