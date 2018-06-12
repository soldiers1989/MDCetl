INSERT INTO MDCDW.dbo.FactEmployment
--IAF
SELECT
	Batch,
	CompanyID,
	19                     AS DataSource,
	'MDCRaw.IAF.IAFDetail' AS TableSource,
	NULL                   AS TimeType,
	NULL                   AS EmploymentType, -- verify w Joseph; what is default type?
	NULL                   AS TimeOfYear,
	Cast(Number_of_employees AS INT)    AS Value,
	NULL                   AS AssocDate, --verify w Joseph; What this date is about from old db
	6                      AS Zindex,
	2017                   AS FiscalYear
FROM MDCRaw.IAF.IAFDetail
-- WHERE Number_of_employees > 0

UNION
--CRUNCHBASE
SELECT
	Batch,
	company_id AS CompanyID,
	20 AS DataSource,
	'MDCRaw.CRUNCHBASE.Organization' AS TableSource,
	NULL                   AS TimeType,
	NULL                   AS EmploymentType, -- verify w Joseph; what is default type?
	NULL                   AS TimeOfYear,
	CAST(num_employees_min AS INT)		 AS Value,-- Picked the min value
	NULL                   AS AssocDate,
	2 AS Zindex,
	2017 AS FiscalYear
FROM MDCRaw.CRUNCHBASE.Organization
-- WHERE role_company = 1
-- AND num_employees_min > 0

UNION

--ANNUAL SURVEY
SELECT DISTINCT
	3842 AS Batch,
	Company_ID AS CompanyID,
	36 AS DataSource,
	'MDCReport.BD.AnnualSurveyResult' AS TableSource,
	CASE WHEN QuestionID IN (5002132749, 5002132750)  THEN 1
		WHEN QuestionID IN (5002132751, 5002132752) THEN 2
		ELSE NULL END AS TimeType,
	CASE WHEN QuestionID IN (50021327274, 50021327273)  THEN 1
		ELSE NULL END AS EmploymentType,
	CASE WHEN QuestionID IN (5002132749, 5002132751, 50021327273)  THEN 2
		WHEN QuestionID IN (5002132750, 5002132752, 50021327274) THEN 1
		ELSE NULL END AS TimeOfYear,
	CAST(ROUND(CAST(Answer AS FLOAT), 0)AS INT) AS Value,
	NULL                   AS AssocDate, -- END of FY18 date - March 31,2018
	5 AS Zindex,
	2017 AS FiscalYear
FROM MDCReport.BD.AnnualSurveyResult
WHERE QuestionID IN (
	5002132749
	,5002132750
	,5002132751
	,5002132752
	,50021327274
	,50021327274
)
AND Answer NOT LIKE 'N/A'

UNION
--BAP Quarterly Annual Data

SELECT E.BatchID, E.CompanyID, E.DataSource, E.TableSource,
	CASE WHEN E.EmploymentTimeType = 'Full-time employees at end of calendar year' THEN 1
		WHEN E.EmploymentTimeType = 'Part-time employees at end of year' THEN 2
			ELSE NULL END AS TimeTYPE,
	E.EmploymentType,
	E.TimeOfYear, E.Value, E.AssocDate, E.ZIndex, E.Year
FROM (
	SELECT
		BatchID,
		CompanyID,
		DataSource,
		'MDCRaw.BAP.AnnualCompanyData' AS TableSource,
		EmploymentTimeType AS EmploymentTimeType,
		NULL         EmploymentType,
		1            TimeOfYear,
		[Value] AS  Value,
		'2017-12-31' AssocDate,
		7           ZIndex,
		2017         Year
	FROM MDCRaw.BAP.AnnualCompanyData
			 UNPIVOT
			 (
					 [Value]
			 FOR [EmploymentTimeType]
			 IN (
				 [Full-time employees at end of calendar year],
				 [Part-time employees at end of year]
				 )
			 ) un
) E

UNION

--MaRSSuppliment

SELECT F.BatchID, F.CompanyID, F.DataSource,
	F.TableSource, F.TimeType,F.EmploymentType,F.TimeOfYear, F.Amount,
	F.AssocDate, F.Zindex, F.FiscalYear
FROM (
	SELECT
		BatchID,
		CompanyID,
		37                             DataSource,
		'MDCRaw.MaRS.MaRSSupplemental' TableSource,
		CASE WHEN [EmploymentTimeType]='EmploymentFullTimeEOY' THEN 1
				 WHEN [EmploymentTimeType]='EmploymentPartTimeEOY' THEN 2
				 ELSE NULL END AS TimeType,
		NULL            AS             EmploymentType,
		1            AS             TimeOfYear,
		CASE WHEN [DateSubmitted]='0001-01-01 00:00:00.0000000' THEN NULL
			ELSE SUBSTRING(DateSubmitted,1,10) END AS AssocDate,
		[Amount]        AS             [Amount],
		8               AS             Zindex,
		2017            AS             FiscalYear
	FROM MDCRaw.MaRS.MaRSSupplemental
			 UNPIVOT
			 (
					 [Amount]
			 FOR [EmploymentTimeType]
			 IN (
				 EmploymentFullTimeEOY,
				 EmploymentPartTimeEOY
			 )
			 ) un
) F
