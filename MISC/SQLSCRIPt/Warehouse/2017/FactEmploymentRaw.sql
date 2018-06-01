/*IAF*/
SELECT
	Batch,
	CompanyID,
	19                     AS DataSource,
	'MDCRaw.IAF.IAFDetail' AS TableSource,
	NULL                   AS TimeType,
	NULL                   AS EmploymentType, -- verify w Joseph; what is default type?
	NULL                   AS TimeOfYear,
	Number_of_employees    AS Value,
	NULL                   AS AssocDate, --verify w Joseph; What this date is about from old db
	6                      AS Zindex,
	2017                   AS FiscalYear
FROM MDCRaw.IAF.IAFDetail
WHERE Number_of_employees > 0


UNION

 /*CRUNCHBASE*/
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
WHERE role_company = 1
AND num_employees_min > 0

UNION

 /*AnnualSurvey2017*/
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
	NULL                   AS AssocDate,
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
AND Answer NOT LIKE 'N/A'-- Should we convert this to NULL
AND CAST(ROUND(CAST(Answer AS FLOAT), 0)AS INT) > 0
