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
	ANNUAL_SURVEY = 36
	NEW_SURVEY = 37
	STUDIO_Y_C5_S4 = 38
	COMPREHENSIVE_TEST_SURVEY = 39
	MDC_TOOLS_SURVEY = 40
	SGIZMO_TEST_SURVEY = 41
	STUDIO_Y_C5_S3 = 42
	STUDIO_Y_C5_S2 = 43
	SPARKING_INTEREST_SURVEY = 44
	STUDIO_Y_C5_S1 = 45
	OSVP_METRICS_TEST_SURVEY = 46
	MDC_SANDBOX_SURVEY = 47


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
	SURVEY_GIZMO = 50


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


class StageLevel(Enum):
	IDEA = 4
	DISCOVERY = 2
	VALIDATION = 6
	EFFICIENCY = 3
	SCALE = 5


class Stage(Enum):
	IDEA = 1
	DISCOVERY = 2
	VALIDATION = 3
	EFFICIENCY = 4
	SCALE = 5


class FileType(Enum):
	SPREAD_SHEET = ['xls', 'xlsx']
	CSV = ['csv']
	PDF = ['pdf']
	WORD = ['doc', 'docx']


class MDCDataSource(Enum):
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
	target_list = 'target_list'


class FileName(Enum):
	bap_combined = 'RICS_BAP_COMBINED_FY{}Q{}.xlsx'


class Table(Enum):
	company_program =       'MaRSDataCatalyst.BAP.ProgramData'
	company_program_youth = 'MaRSDataCatalyst.BAP.ProgramDataYouth'
	company_data =          'MaRSDataCatalyst.BAP.QuarterlyCompanyData'
	company_annual =        'MaRSDataCatalyst.BAP.AnnualCompanyData'

	batch = 'Config.ImportBatch'
	batch_log = 'Config.ImportBatchLog'

	fact_ric_aggregation = 'Reporting.FactRICAggregation'


class SQL(Enum):
	sql_entity_exists = '''SELECT * FROM {} WHERE {} = \'{}\' '''
	sql_annual_comapny_data_update = 'UPDATE BAP.AnnualCompanyData SET CompanyID = {} WHERE ID = {}'
	sql_target_list_update = 'UPDATE MDCRaw.SURVEY.Targetlist SET CompanyID = {} WHERE ID = {}'
	sql_batch_insert = 'INSERT INTO Config.ImportBatch VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_update = 'UPDATE {} SET {} = {} WHERE {} = {}'
	sql_get_max_id = 'SELECT MAX({}) AS MaxID FROM {}'
	sql_bap_quarterly_company = 'SELECT ID, [Company Name] as Name, [FileName], BatchID, Website, DataSource FROM BAP.QuarterlyCompanyData WHERE CompanyID = \'0\''
		# 'SELECT [Company Name] as Name, [Former / Alternate Names], [Street Address], City, Province, [Postal Code],Website FROM BAP.QuarterlyCompanyData'
	# sql_dim_company_insert = 'INSERT INTO[Reporting].[DimCompany] VALUES({},\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',\'{}\',{},\'{}\',\'{}\')'
	# sql_dim_company_source_insert = 'INSERT INTO[Reporting].[DimCompanySource] VALUES({}, {},\'{}\',\'{}\',{},{},\'{}\',\'{}\',\'{}\''
	sql_dim_company_source_update = 'UPDATE [Reporting].[DimCompanySource] SET CompanyID = {} WHERE Name = \'{}\''
	sql_dim_company = 'SELECT CompanyID, [CompanyName] FROM {} WHERE CompanyName IS NOT NULL'
	sql_dim_company_source = 'SELECT CompanyID, [Name] as [CompanyName] FROM {} WHERE[Name] IS NOT NULL'
	sql_update_company_source ='UPDATE [Config].[CompanyDataRaw] SET CompanyID = {} WHERE ID = {}'

	sql_batch_update = 'Update {} SET BatchId = {} WHERE SourceSystem = {} AND DataSource = {}'
	sql_batch_select = 'SELECT DISTINCT DataSourceId, BatchID FROM Config.ImportBatch WHERE Year = {} AND Quarter = \'Q{}\' AND SourceSystemID = {}'
	sql_program_insert = 'INSERT INTO MaRSDataCatalyst.[BAP].[ProgramData] Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_program_youth_insert = 'INSERT INTO MaRSDataCatalyst.[BAP].[ProgramDataYouth] Values (?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_bap_company_insert = 'INSERT INTO MaRSDataCatalyst.[BAP].[QuarterlyCompanyData] Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_bap_company_annual_insert = 'INSERT INTO MaRSDataCatalyst.[BAP].[AnnualCompanyData] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_bap_distinct_batch = 'SELECT DISTINCT FileName,Path, SourceSystem, DataSource FROM {} WHERE Year = \'{}\' AND Quarter = \'Q{}\''
	sql_annual_bap_distinct_batch = 'SELECT DISTINCT FileName,Path, SourceSystem, DataSource FROM {} WHERE Year = \'{}\''

	sql_target_list_insert = 'INSERT INTO [SURVEY].[Targetlist] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

	sql_dim_company_insert = 'INSERT INTO MaRSDataCatalyst.[Reporting].[DimCompany] VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_dim_venture_insert = 'INSERT INTO MDCDW.dbo.DimVenture VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
	sql_dim_company_source_insert = 'INSERT INTO [Reporting].[DimCompanySource] VALUES (?,?,?,?,?,?,?,?,?)'

	'''
				SELECT CompanyID, DataSource, BatchID,'20171231' AS DateID,[Date of Intake] AS IntakeDate,
				NULL AS [StageLevelID],NULL AS [SizeID], 'NULL' AS Age,[High Potential y/n], NULL AS [DevelopmentID],
				[Number of advisory service hours provided] AS AdvisoryServiceHours, [Volunteer mentor hours] , GETDATE() AS [Modified Date],
				GETDATE() AS [CreatedDate], Youth,[Street Address], City, Province, [Postal Code], Website,Stage,
				'' as AnnualRevenue ,NULL as NumberOfEmployees,[Funding Raised to Date $CAN], NULL AS FundingCurrentQuarter,
				[Incorporate year (YYYY)]+'-'+ [Incorporation month (MM)] + '-15' AS [Date of Incorporation],
				[Industry Sector], [Social Enterprise y/n], [Quarter], [Year]
				FROM BAP.QuarterlyCompanyData WHERE CompanyID IS NOT NULL AND Year = 2018 AND Quarter = 'Q3'
	'''
	sql_bap_fact_ric_data_fyq4 = '''
		SELECT CompanyID, DataSource, BatchID,'20180331' AS DateID,[Date of Intake] AS IntakeDate,
		NULL AS [StageLevelID],NULL AS [SizeID], 'NULL' AS Age,[High Potential y/n], NULL AS [DevelopmentID],
		[Number of advisory service hours provided] AS AdvisoryServiceHours, [Volunteer mentor hours] , GETDATE() AS [Modified Date],
		GETDATE() AS [CreatedDate], Youth,[Street Address], City, Province, [Postal Code], Website,Stage,
		[AnnualRevenue(CAN)],NumberOfEmployees,[Funding Raised to Date $CAN], NULL AS FundingCurrentQuarter,
		Date_of_Incorporation AS [Date of Incorporation],[Industry Sector], [Social Enterprise y/n], [Quarter], [Year]
		FROM MaRSDataCatalyst.BAP.QuarterlyCompanyData WHERE CompanyID IS NOT NULL AND Year = 2018 AND Quarter = 'Q4'
	'''

	sql_bap_fact_ric_company_data_source = '''
		SELECT CompanyID, DataSource, BatchID,'20171231' AS DateID,[Date of Intake] AS IntakeDate,
		NULL AS [StageLevelID],NULL AS [SizeID], 'NULL' AS Age,[High Potential y/n], NULL AS [DevelopmentID],
		[Number of advisory service hours provided] AS AdvisoryServiceHours, [Volunteer mentor hours] , GETDATE() AS [Modified Date],
		GETDATE() AS [CreatedDate], Youth,[Street Address], City, Province, [Postal Code], Website,Stage,
		[AnnualRevenue(CAN)],NumberOfEmployees,[Funding Raised to Date $CAN], NULL AS FundingCurrentQuarter,
		 Date_of_Incorporation AS [Date of Incorporation],[Industry Sector], [Social Enterprise y/n], [Quarter], [Year]
		FROM BAP.QuarterlyCompanyData WHERE CompanyID IS NOT NULL AND Year = 2018 AND Quarter = 'Q3' '''

	sql_bap_ric_company_quarterly_data = '''
				SELECT CompanyID, DataSource, BatchID,'20171231' AS DateID,[Date Of Intake],'INTAKE DATE' AS IntakeDate,
				NULL AS [StageLevelID],NULL AS [SizeID], 'NULL' AS Age, [High Potential y/n], NULL AS [DevelopmentID], 
				[Number of advisory service hours provided], [Volunteer mentor hours], GETDATE() AS [Modified Date],
				GETDATE() AS [CreatedDate], Youth, [Street Address], City, Province, [Postal Code], Website,Stage,
				NULL AS AnualRevenueCAN,NULL AS NumberOfEmployees,[Funding Raised to Date $CAN],NULL AS FundingRaisedInCurrentQuarterCAN,
				[Incorporate year (YYYY)]+'-'+[Incorporation month (MM)]+'-'+'15' AS DateOfIncorporation,[Industry Sector], [Social Enterprise y/n], [Quarter], [Year] 
				FROM MaRSDataCatalyst.BAP.QuarterlyCompanyData WHERE Year = {} AND Quarter = \'{}\'
	'''
	sql_bap_fact_ric_company_insert = 'INSERT INTO MaRSDataCatalyst.[Reporting].[FactRICCompanyData] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

	sql_bap_fact_ric_aggregation_insert = 'INSERT INTO MaRSDataCatalyst.[Reporting].[FactRICAggregation] VALUES (?,?,?,?,?,?,?,?)'
	sql_bap_fra_insert = 'INSERT INTO MaRSDataCatalyst.[Reporting].[FactRICAggregation] VALUES {}'

	sql_company_aggregate_program = 'SELECT * FROM MaRSDataCatalyst.BAP.ProgramData WHERE Year = {} AND Quarter = \'Q{}\''
	sql_company_aggregate_program_youth = 'SELECT * FROM MaRSDataCatalyst.BAP.ProgramDataYouth WHERE Year = {} AND Quarter = \'Q{}\''

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
		WHERE FiscalYear = {}
		'''

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

	sql_columns = 'SELECT * FROM MaRSDataCatalyst.INFORMATION_SCHEMA.COLUMNS WHERE Table_Schema = \'{}\' ORDER BY ORDINAL_POSITION'
	
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
								--FactRICCompanyHoursRolledUp.IntakeDate,
								ISNULL(TRY_CAST(FactRICCompanyHoursRolledUp.IntakeDate AS date), '') as IntakeDate,

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

					FactRICCompanyHoursRolledUp.FiscalYear = {} AND FactRICCompanyHoursRolledUp.FiscalQuarter = {} AND FactRICCompanyHoursRolledUp.DataSourceID = {} 

			'''

	sql_target_list = 'SELECT ID,Invite_first_name, Invite_last_name, Venture_name, Venture_basic_name, Email, Datasource,RIC_organization_name FROM SURVEY.Targetlist WHERE Status = 1'
	sql_mars_target_list = '''SELECT Venture_name, Venture_basic_name, Email, Invite_first_name, Invite_last_name,
							  RIC_first_name,RIC_last_name, RIC_person_email,RIC_person_title
							  FROM MDCRaw.SURVEY.Targetlist WHERE DataSource = 7'''
	sql_target_list_basic_name = '''SELECT ID, Venture_name  FROM MDCRaw.SURVEY.Targetlist'''
	sql_target_list_basic_name_update = '''UPDATE MDCRaw.SURVEY.Targetlist SET Venture_basic_name = \'{}\' WHERE ID = {}'''

	sql_invest_ottawa_target_list = '''SELECT Venture_name, Venture_basic_name, Email, Invite_first_name, Invite_last_name
									   FROM MDCRaw.SURVEY.Targetlist WHERE DataSource = 16'''

	sql_duplicate_venture_list = '''SELECT ID,Name, BasicName FROM Venture WHERE BasicName IN (SELECT BasicName FROM Venture GROUP BY BasicName HAVING Count(BasicName) > 1) AND BasicName <> '' ORDER BY 2'''
	sql_duplicate_venture_insert = '''INSERT INTO MDCRaw.CONFIG.DuplicateVenture VALUES (?,?,?,?,?,?,?,?,?)'''
	sql_duplicate_venture_truncate = '''TRUNCATE TABLE MDCRaw.CONFIG.DuplicateVenture'''
	sql_duplicate_venture_select = '''SELECT CompanyID, DuplicateCompanyID, Name as [Company Name], DuplicaateName as [Duplicate Company Name], BasicName FROM MDCRaw.CONFIG.DuplicateVenture'''
	sql_duplicate_ventures_with_former_name = '''SELECT ID, Name, BasicName FROM Venture WHERE BasicName IN ( SELECT BasicName FROM dbo.Venture GROUP BY BasicName HAVING COUNT(BasicName) > 1) ORDER BY ID'''

	sql_venture_basic_name = 'SELECT ID, Name FROM MDCRaw.dbo.Venture ORDER BY 1'
	sql_update_venture_duplicates = ''' UPDATE MDCRaw.dbo.Venture SET Duplicate = {} WHERE ID = {}'''
	sql_venture_basic_name_update = '''UPDATE MDCRaw.dbo.Venture SET BasicName = \'{}\' WHERE ID = {}'''

	sql_venture_former_name = '''SELECT ID, Name, OtherNames, BasicName FROM MDCRaw.dbo.Venture WHERE Name LIKE \'%(%\' '''
	sql_venture_insert = '''INSERT INTO MDCRaw.dbo.Venture VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_venture_insert_short = '''INSERT INTO MDCRaw.dbo.Venture VALUES {}'''
	sql_venture_other_name_update = '''UPDATE MDCRaw.dbo.Venture SET Name = \'{}\', BasicName = \'{}\',OtherNames = \'{}\' WHERE ID = {}'''

	sql_dw_fact_ric_company_data = '''
		SELECT
		CompanyID ,
		DataSourceID,
		BatchID ,
		DateID,
		Convert(varchar,IntakeDate,111) AS IntakeDate,--CAST(IntakeDate AS date) AS Intake,
		CAST(AdvisoryServicesHours AS Decimal(18,4)) AS [Advisory],
		CAST(VolunteerMentorHours AS Decimal(18,4)) AS [Volunteer],
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
		CONVERT(int,RIGHT(FiscalQuarter,1)) AS [Fiscal Quarter],
		CONVERT(int, FiscalYear) AS [Fiscal Year],
		CreateDate,
		ModifiedDate
		FROM MDC_DEV.Reporting.FactRICCompanyData
	'''

	sql_iaf_summary_insert = 'INSERT INTO MDCRaw.IAF.IAFSummary VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
	sql_iaf_summary_basic_name = 'SELECT ID, Venture_Name FROM IAF.IAFSummary'
	sql_iaf_summary_update = 'UPDATE MDCRaw.IAF.IAFSummary SET BasicName = \'{}\' WHERE ID = {}'
	sql_iaf_detail_insert = 'INSERT INTO MDCRaw.IAF.IAFDetail VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'

	sql_cvca_deals = 'INSERT INTO MDCRaw.CVCA.VCPEDeals VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
	sql_cvca_exits = 'INSERT INTO MDCRaw.CVCA.Exits VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'

	sql_dbo_duplicate_venture_update = '''UPDATE MDCRaw.CONFIG.DuplicateVenture SET Verified = 1 WHERE CompanyID = {}'''

	sql_cbinsights_select = '''SELECT ID, CompanyName FROM MDCRaw.CBINSIGHTS.Funding'''
	sql_cbinsights_update = '''UPDATE MDCRaw.CBINSIGHTS.Funding SET BasicName = \'{}\' WHERE Id = {}'''

	sql_cvca_select = '''SELECT ID, CompanyName FROM MDCRaw.CVCA.VCPEDeals'''
	sql_cvca_update = '''UPDATE MDCRaw.CVCA.VCPEDeals SET BasicName = \'{}\' WHERE ID = {}'''

	sql_orgs_summary_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[OrganizationsSummary] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_people_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[People] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_category_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Category] VALUES (?,?,?,?,?,?,?,?,?)'''
	sql_location_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Location] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

	sql_orgs_summary = '''SELECT api_url, fetched, uuid FROM MDCRaw.Crunchbase.OrganizationsSummary WHERE [permalink] LIKE \'wattpad\''''

	sql_orgnization_insert = '''INSERT INTO MDCRaw.CRUNCHBASE.Organization VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_org_short_insert = 'INSERT INTO MDCRaw.CRUNCHBASE.Organization VALUES {}'

	sql_orgs_summary_update = '''UPDATE [CRUNCHBASE].[OrganizationsSummary] SET data_fetched = 1 WHERE uuid = \'{}\''''
	sql_orgs_detail_update = '''UPDATE [CRUNCHBASE].[Organization] SET data_fetched = 1 WHERE org_uuid = \'{}\''''

	sql_orgs_summary_select = 'SELECT uuid, api_url, name, fetched FROM MDCRaw.CRUNCHBASE.OrganizationsSummary'
	sql_acquired_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Acquired_by] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_acquiree_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Acquiree] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_acquisition_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Acquisition] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_org_category_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Org_Category] VALUES (?,?,?,?,?,?,?)'''
	sql_founders_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Founders] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_funding_rounds_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Funding_Rounds] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_funds_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Funds] VALUES (?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_image_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Image] VALUES (?,?,?,?,?,?,?,?,?,?)'''
	sql_investments_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Investments] VALUES (?,?,?,?,?,?,?,?,?,?)'''
	sql_investors_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Investments] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_ipo_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[IPO] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_job_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Job] VALUES (?,?,?,?,?,?,?,?,?,?)'''
	sql_news_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[News] VALUES (?,?,?,?,?,?,?,?,?)'''
	sql_offices_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Offices] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_partners_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Partners] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_sub_organization_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[SubOrganization] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_team_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Team] VALUES (?,?,?,?,?,?,?,?,?,?,?)'''
	sql_websites_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Websites] VALUES (?,?,?,?,?,?,?)'''
	sql_person_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Person] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_invested_in_insert = '''INSERT INTO MDCRaw.[CRUNCHBASE].[Person] VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''

	sql_org_detail_exists = '''SELECT * FROM MDCRaw.CRUNCHBASE.Organization WHERE org_uuid LIKE \'{}\''''
	sql_funding_exists = '''SELECT * FROM MDCRaw.CRUNCHBASE.Funding_Rounds WHERE fun_uuid LIKE \'{}\''''
	sql_offices_exists = '''SELECT * FROM MDCRaw.CRUNCHBASE.Offices WHERE office_uuid LIKE \'{}\''''
	sql_org_category_exists = '''SELECT * FROM MDCRaw.CRUNCHBASE.Org_Category WHERE category_uuid LIKE \'{}\''''

	sql_tdw_companies_insert = '''INSERT INTO MDCRaw.TDW.Companies VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_tdw_companies_single_insert = '''INSERT INTO MDCRaw.TDW.Companies VALUES {}'''


	sql_tdw_basic_company = '''SELECT DISTINCT ID, legal_name FROM MDCRaw.TDW.Companies WHERE BasicName = '' '''
	sql_tdw_basic_company_update = '''UPDATE MDCRaw.TDW.Companies SET BasicName = \'{}\' WHERE ID = {}'''
	sql_cb_basic_company = '''SELECT org_uuid, name FROM MDCRaw.CRUNCHBASE.Organization '''
	sql_cb_basic_company_update = '''UPDATE MDCRaw.CRUNCHBASE.Organization SET BasicName = \'{}\' WHERE org_uuid = \'{}\''''
	sql_cvca_basic_company = '''SELECT ID, CompanyName FROM MDCRaw.CVCA.Exits WHERE BasicName = '' '''
	sql_cvca_basic_company_update = '''UPDATE MDCRaw.CVCA.Exits SET BasicName = \'{}\' WHERE ID = {}'''

	sql_cvca_exits_new_ventures = '''SELECT E.CompanyName, E.BasicName, E.Batch FROM CVCA.Exits E LEFT JOIN dbo.Venture V ON E.BasicName = V.BasicName WHERE V.Name IS NULL'''
	sql_iaf_new_ventures = '''SELECT E.Venture_Name, E.BasicName, E.Batch FROM IAF.IAFSummary E LEFT JOIN dbo.Venture V ON E.BasicName = V.BasicName WHERE V.Name IS NULL'''
	sql_cvca_deals_new_ventures = '''SELECT E.CompanyName, E.BasicName, E.Batch FROM CVCA.VCPEDeals E LEFT JOIN dbo.Venture V ON E.BasicName = V.BasicName WHERE V.Name IS NULL'''
	sql_cb_new_ventures = '''SELECT E.name, E.BasicName, E.Batch FROM CRUNCHBASE.Organization E LEFT JOIN dbo.Venture V ON E.BasicName = V.BasicName WHERE V.Name IS NULL'''
	sql_cvca_type_update = '''UPDATE MDCRaw.CVCA.VCPEDeals SET Type = {} WHERE ID = {}'''

	sql_bap_basic_name = '''SELECT ID, CompanyName FROM MaRSDataCatalyst.BAp.QuarterlyCompanyData WHERE CompanyID = 0 AND BasicName IS NULL'''
	sql_bap_basic_name_update = '''UPDATE MaRSDataCatalyst.BAp.QuarterlyCompanyData SET BasicName = \'{}\' WHERE ID = {}'''

	#sql_batch_selects = 'SELECT DISTINCT BatchID FROM Config.ImportBatch WHERE Year = {} AND Quarter = \'Q{}\' AND SourceSystemID = {}'
	#sql_batch_insert = 'INSERT INTO CONFIG.ImportBATCH Values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
	#sql_batch_single_insert = 'INSERT INTO {} Values {}'
	#sql_batch_update = 'Update {} SET BatchId = {} WHERE SourceSytemID = {} AND DataSource = {}'
	#sql_batch_delete = 'DELETE FROM {} WHERE BatchID IN {}'
	#sql_batch_table = 'SELECT BatchID FROM {} WHERE BatchID IN {}'
	#sql_batch_count = 'SELECT COUNT(*) AS Total FROM {} WHERE BatchID IN {}'
	#sql_batch_search = 'SELECT BatchID FROM {} WHERE {}'

	sql_bap_new_company = '''
			SELECT CompanyName, BasicName, BatchID , NULl as DateFounded, Date_of_Incorporation,NULL as VentureType,
			NULL AS DescriptionL,Website, NULL as Email, NULL as Phone, NULL as Fax,
			1 AS VentureStatus, GETDATE() as ModifiedDate, GETDATE() AS CreatedDate
			FROM MaRSDataCatalyst.BAP.QuarterlyCompanyData
			WHERE CompanyID = 0 '''

	sql_mars_meta_data_insert = '''INSERT INTO MDCReport.BD.MaRSMetadata VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'''
	sql_marsmetadata_new_ventures = '''SELECT VentureName as Name, BasicName, BatchID FROM MDCReport.BD.MaRSMetadata WHERE CompanyID IS NULL'''


class Columns(Enum):
	ric_aggregation_id = 'RICAggregationID'
	clmn_fact_ric_rolled_up = ['DataSourceID', 'CompanyID', 'MinDate', 'CurrentDate', 'VolunteerYTD', 'AdvisoryHoursYTD',
							   'VolunteerThisQuarter', 'AdvisoryThisQuarter', 'FiscalQuartecr', 'BatchID', 'ModifiedDate',
							   'SocialEnterprise', 'Stage', 'HighPotential', 'Lvl2IndustryName', 'FiscalYear', 'Youth',
							   'DateOfIncorporation', 'AnnualRevenue', 'NumberEmployees', 'FundingToDate', 'IndustrySector',
							   'IntakeDate', 'FundingCurrentQuarter']


class CBDict(Enum):
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
	MATCH = 4
	FASTLANE = 5
	MaRS_FIX = 6


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


class Combine(Enum):
	FOR_TEST = 'TEST'
	FOR_QA = 'QA'
	FOR_ETL = 'ETL'
	FOR_NONE = 'COMMON'


class FilePath(Enum):
	path_iaf_source = 'Box Sync/IAF-MDC_Shared'
	path_cbinsight_source = ''
	path_bap_source = ''
	path_missing_bap_etl = 'Box Sync/WorkBench/BAP/BAP_FY18/FY18_Q3/for ETL/Missing Data Reports'
	path_iaf = 'Box Sync/Workbench/IAF/ETL Prep/2017/ETL'
	path_cvca = 'Box Sync/Workbench/CVCA/ETL/2017'
	path_venture_dedupe = 'Box Sync/Workbench/Venture_Dedupe'
	path_namara = 'Box Sync/Workbench/Think Data Works/Namara'
	path_bap_qa = 'Box Sync/WorkBench/BAP/BAP_FY18/FY18_Q4/ETL/00 QA'
	path_bap_etl = 'Box Sync/WorkBench/BAP/BAP_FY18/FY18_Q4/ETL'
	path_bap_combined = 'Box Sync/WorkBench/BAP/BAP_FY18/FY18_Q4/ETL/01 Combined'
	path_bap_combined_dest = 'Box Sync/WorkBench/BAP/BAP_FY18/FY18_Q4/ETL/01 Combined/00 QA'
	path_mars_metadata = 'Box Sync/WorkBench/BAP/Annual Survey FY2018/MaRS Metadata'


class DealType(Enum):
	Bridge_VC = 1
	Early_Stage_VC = 2
	Exits_VC = 3
	Later_Stage_VC = 4
	Other_VC = 5
	PE_Add_on = 6
	PE_Buyout = 7
	PE_Debt = 8
	PE_Follow_on = 9
	PE_Growth = 10
	PE_Infrastructure = 11
	PE_Private_Placemen = 12
	PE_Privatization = 13
	PE_Recap = 14
	PE_Secondary_Buyout = 15
	PE_Secondary_Sale = 16
	PIPE = 17
	Seed_VC = 18
	Venture_Debt_VC  = 19
	Exits_PE = 20
	PE_backed_IPO_RTO = 21
	VC_backed_IPO_RTO = 22


class Province(Enum):
	pass


class APIUrl(Enum):
	Crunchbase = ''
	CVCA = ''
	TDW = 'https://api.namara.io/v0/data_sets/162cb2bb-54b5-45d9-a61e-b2cbb80758bf/data/en-5?geometry_format=wkt&api_key=295a4ce3b8a56f1150e941b79b2a990033f3248eb9b5551ef58db0fc3b029988&organization_id=58b5dfbb6bf6b80009000229&project_id=58b5dfcd2ce34d000a000237'


class MaRSProgram(Enum):
	Start = 1
	Growth = 2
	Scale = 3


class MaRSSector(Enum):
	Cleantech = 1
	Fiance_and_Commerce = 2
	Health = 3
	Work_and_Learning = 4


class CAIPStatus(Enum):
	IsCAIP = 1
	WasCAIP = 2
	NeverCAIP = 3


