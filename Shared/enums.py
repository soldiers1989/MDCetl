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
    RICPD16 = 31
    RICPD16Y = 32
    RICCD16Y = 33
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

