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





