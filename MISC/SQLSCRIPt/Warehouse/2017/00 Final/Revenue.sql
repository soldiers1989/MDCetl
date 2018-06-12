INSERT INTO MDCDW.dbo.FactRevenue
SELECT
	R.Batch,
	R.CompanyID,
	R.DataSource,
	R.TableSource,
	CASE WHEN R.Territory LIKE 'Revenue_year_to_date' THEN 1
		WHEN R.Territory LIKE 'Revenue_out_side_canada' THEN 2
		ELSE NULL
	END AS Territory,
	R.Revenue,
	R.Zindex,
	R.FiscalYear,
	42 AS Currency
FROM (

SELECT
		Batch,
		CompanyID,
		19                     AS DataSource,
		'MDCRaw.IAF.IAFDetail' AS TableSource,
		[Territory],
		CAST([Revenue] AS INT) [Revenue],
		6                      AS Zindex,
		2017                   AS FiscalYear
FROM  (SELECT
				 Batch,
				 CompanyID,
				 CAST(Revenue_year_to_date AS INT) [Revenue_year_to_date],
				 CAST(Revenue_out_side_canada AS INT)  * 1000000 [Revenue_out_side_canada]
			 FROM MDCRaw.IAF.IAFDetail
			WHERE YEAR(Date) = 2017) T
UNPIVOT
( [Revenue]
FOR [Territory]
IN ([Revenue_year_to_date], [Revenue_out_side_canada])
) un
-- WHERE Revenue > 0

) R

UNION

--SURVEY

SELECT DISTINCT
	3842 AS Batch,
	Company_ID AS CompanyID,
	36 AS DataSource,
	'MDCReport.BD.AnnualSurveyResult' AS TableSource,
	CASE WHEN QuestionID = 5002132744 THEN 1
		WHEN QuestionID = 5002132745 THEN 2
		ELSE NULL
	END AS [Territory],
	CAST(Answer AS Float) Amount,
	5 AS Zindex,
	2017 AS FiscalYear,
	42 AS Currency -- Change this currency to ID
	FROM MDCReport.BD.AnnualSurveyResult
	WHERE QuestionID IN (
5002132744,
5002132745)
AND CAST(Answer AS Float) < 100000000000

UNION

--BAP Quarterly Annual Data

SELECT E.BatchID, E.CompanyID, E.DataSource, E.TableSource,
	CASE WHEN E.Territory = 'Sales revenue from Canadian sources $CAN' THEN 1
		WHEN E.Territory = 'Sales revenue from international sources $CAN' THEN 2
			ELSE NULL END AS Territory,
CAST(E.Value AS Float) AS Value
	, E.ZIndex, E.Year, Currency
FROM (
	SELECT
		BatchID,
		CompanyID,
		DataSource,
		'MDCRaw.BAP.AnnualCompanyData' AS TableSource,
		[RevenueTerritory] AS Territory,
		[Value] AS  Value,
		7           ZIndex,
		2017         Year,
		42 AS Currency
	FROM MDCRaw.BAP.AnnualCompanyData
			 UNPIVOT
			 (
					 [Value]
			 FOR [RevenueTerritory]
			 IN (
				 [Sales revenue from Canadian sources $CAN],
			   [Sales revenue from international sources $CAN]
				 )
			 ) un
) E

UNION

--MaRS Suppliment


SELECT F.BatchID, F.CompanyID, F.DataSource,
	F.TableSource, F.Territory, F.Amount,
	F.Zindex, F.FiscalYear, F.Currency
FROM (
	SELECT
		BatchID,
		CompanyID,
		37                             DataSource,
		'MDCRaw.MaRS.MaRSSupplemental' TableSource,
		CASE WHEN [RevenueSource]='RevenueCanadianSource' THEN 40
				 WHEN [RevenueSource]='RevenueUSASource' THEN 235
				 WHEN [RevenueSource]='RevenueInternational' THEN 248
				 ELSE NULL END AS Territory,
		[Amount]        AS             [Amount],
		8               AS             Zindex,
		2017            AS             FiscalYear,
		CASE WHEN [RevenueSource]='RevenueCanadianSource' THEN 42
				 WHEN [RevenueSource]='RevenueUSASource' THEN 5
				 WHEN [RevenueSource]='RevenueInternational' THEN 5
				 ELSE NULL END AS Currency
	FROM MDCRaw.MaRS.MaRSSupplemental
			 UNPIVOT
			 (
					 [Amount]
			 FOR [RevenueSource]
			 IN (
				 RevenueCanadianSource,
				 RevenueUSASource,
				 RevenueInternational
			 )
			 ) un
) F





