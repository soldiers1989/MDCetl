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
	DATA = 0,
	QA = 1


class TeamStatus(Enum):
	Featured = 'Featured Team'
	Past = 'Past Team'
	Current = 'Current Team'
	Board = 'Board Member and Adviser'


