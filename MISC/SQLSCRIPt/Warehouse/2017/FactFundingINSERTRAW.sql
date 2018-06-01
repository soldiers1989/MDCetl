/*
GENERAL
Assumptions:
	- Amount > 0
	- Year(Not Fiscal Year) = 2017
	- Amount is Rounded to 0 and AS INT
QA:
	- Are these records unique? Determinant[FundingType, Funding Source, Funding Territory and Amount(to the lesser extent)
*/

/*IAF*/
SELECT
	F.Batch,
	F.CompanyID,
	F.DataSource,
	F.TableSource,
	CASE WHEN F.FundingSource IN ('FedDev', 'IRAP', 'SRED', 'Non_Dilutive_Other') THEN 3
		WHEN F.FundingSource LIKE 'Angel' THEN 1
		WHEN F.FundingSource LIKE 'VC' THEN 12
		WHEN F.FundingSource LIKE 'OCE' THEN 9
		WHEN F.FundingSource IN ('Term_loan', 'OtherDilutiveFinancing') THEN 6
		ELSE NULL END AS FundingSource,
	CASE WHEN F.FundingSource IN ('FedDev', 'IRAP', 'SRED', 'Non_Dilutive_Other', 'OCE', 'Term_loan') THEN 5
		WHEN F.FundingSource IN ('Angel', 'VC', 'OtherDilutiveFinancing') THEN 3
		ELSE NULL
		END AS FundingType,
	F.FundingTerritory,
	F.FundingDate,
	F.Amount,
	F.Zindex,
	F.FiscalYear
FROM (
	SELECT
		Batch,
		CompanyID,
		19         AS          DataSource,
		'MDCRaw.IAF.IAFDetail' TableSource,
		[FundingSource],
		NULL       AS          FundingType,
		NULL       AS          FundingTerritory,
		[Date]     AS          FundingDate,
		CAST([Amount] AS INT)  [Amount],
		6          AS          Zindex,
		YEAR(Date) AS          FiscalYear
	FROM MDCRaw.IAF.IAFDetail
			 UNPIVOT
			 (
					 [Amount]
			 FOR [FundingSource]
			 IN ([Angel], [VC], [IRAP], [Term_loan], [FedDev], [OCE], [SRED], [Non_Dilutive_Other])
			 ) un
	WHERE [Amount] > 0.0
				AND YEAR([Date]) = 2017
) F

UNION

/*CVCA*/
SELECT  Batch, CompanyID, 21 AS DataSource, 'MDCRaw.CVCA.VCPEDeals', 3 AS FundingType, 12 AS FundingSource, 39 AS FundingTerritory, Closed_date AS FundingDate, CAST(Amount AS INT) [Amount], 4 AS Zindex, 2017 AS FiscalYear
FROM MDCRaw.CVCA.VCPEDeals
WHERE YEAR(Closed_date) = 2017

UNION

/*CBInsights
Funding Amount: x * 1,000,000
*/
SELECT BatchID AS Batch, CompanyID, 20 AS DataSource, 'MDCRaw.CBINSIGHTS.Funding' AS TableSource, NULL AS FundingSource,
	CASE WHEN Round IN ('Grant') THEN 4
		WHEN Round IN ('Debt') THEN 2
			ELSE NULL END AS FundingType,
	39 AS FundingTerritory,
	[Date] AS FundingDate,
	CAST(ROUND(Amount, 0) AS INT) * 1000000 AS Amount,
	2 AS Zindex,
	2017 AS FiscalYear
FROM MDCRaw.CBINSIGHTS.Funding
WHERE YEAR(Date) = 2017
AND CAST(ROUND(Amount, 0) AS INT) * 1000000 > 0.0

UNION

/*AnnualSurvey2017*/
SELECT DISTINCT
	3842 AS Batch,
	Company_ID AS CompanyID,
	36 AS DataSource,
	'MDCReport.BD.AnnualSurveyResult' AS TableSource,
	CASE WHEN Question LIKE 'Federal Government' THEN 3
		WHEN Question LIKE 'Provincial Government' THEN 9
		WHEN Question LIKE 'Private - Angel' THEN 1
		WHEN Question LIKE 'Private - Other' THEN 8
		WHEN Question LIKE 'Private - Venture Capital' THEN 12
		WHEN Question LIKE 'Other (not private)' THEN 6
		ELSE NULL END AS FundingSource,
	NULL AS FundingType,
	NULL AS FundingTerritory,
	NULL AS FundingDate,
	CAST(ROUND(CAST(Answer AS FLOAT), 0)AS INT) AS Amount,
	5 AS Zindex,
	2017 AS FiscalYear
FROM MDCReport.BD.AnnualSurveyResult
WHERE QuestionID IN (
	50021327241,
	50021327242,
	50021327243,
	50021327244,
	50021327287,
	50021327288
)
AND CAST(ROUND(CAST(Answer AS FLOAT), 0)AS INT) > 0

UNION

/*CRUNCHBASE*/
SELECT
	O.Batch,
	O.company_id AS CompanyID,
	20 AS DataSource,
	'MDCRaw.CRUNCHBASE.Funding_Rounds' AS TableSource,
	NULL AS FundingSource,
	CASE WHEN funding_type LIKE 'grant' THEN 4
	WHEN funding_type IN ('debt_financing', 'post_ipo_debt') THEN 2
	WHEN funding_type IN ('post_ipo_equity', 'private_equity', 'equity_crowdfunding') THEN 3
	WHEN funding_type LIKE 'non_equity_assistance' THEN 5
	ELSE NULL
	END AS FundingType,
	NULL AS FundingTerritory, -- Joseph: if currency == CAD, then Canada as territory?
	announced_on AS FundingDate,
	money_raised_usd AS Amount, --CURRENTLY USD; NEED TO CONVERT TO CAD
	2 AS Zindex,
	2017 AS FiscalYear
FROM MDCRaw.CRUNCHBASE.Organization O INNER JOIN
	MDCRaw.CRUNCHBASE.Funding_Rounds F ON O.org_uuid = F.org_uuid
WHERE O.role_company = 1 -- Should we move all type of FUnding raising or just the company ones [DON'T]
AND YEAR(announced_on) = 2017
AND money_raised_usd > 0
