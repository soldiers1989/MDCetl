WITH CTX AS(
			SELECT DISTINCT
			V.Name AS CompanyName,
			V.ID AS CompanyID,
			F.FiscalQuarter,
			F.FiscalYear,
			F.AdvisoryHoursYTD,
			Y.Name HighPotential,
			F.DateOfIncorporation,
			DATEDIFF(m, F.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2018-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END AS Age,
			F.AdvisoryThisQuarter AS AdvisoryHours_thisQuarter,
			F.VolunteerThisQuarter AS VolunteerHours_thisQuarter,
			CASE
				WHEN F.DateOfIncorporation IS NOT NULL THEN
					CASE
						WHEN DATEDIFF(m, F.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2018-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END < 7 THEN '0-6 months/pre-startup'
						WHEN DATEDIFF(m, F.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2018-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END BETWEEN 7 AND 12 THEN '07-12 months'
						WHEN DATEDIFF(m, F.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2018-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END BETWEEN 13 AND 24 THEN '13-24 months'
						WHEN DATEDIFF(m, F.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2018-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END BETWEEN 25 AND 36 THEN '25-36 months'
						WHEN DATEDIFF(m, F.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2018-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END BETWEEN 37 AND 60 THEN '37-60 months'
						WHEN DATEDIFF(m, F.DateOfIncorporation, GETDATE()) - CASE WHEN DAY('2018-03-31') > DAY(GETDATE()) THEN 1 ELSE 0 END > 60 THEN 'Over 60 months'
						ELSE 'Older'
					END
				ELSE '0-6 months/pre-startup'
			END AS AgeRangeFriendly,

	    CASE WHEN F.Stage IS NULL THEN 'Stage 0 - Idea' ELSE S.Name END AS StageFriendly,
			D.Name RICFriendlyName,
			F.AnnualRevenue,
			CASE
				WHEN F.AnnualRevenue IS NOT NULL THEN
					CASE
						WHEN F.AnnualRevenue = 0 OR F.AnnualRevenue IS NULL THEN '0. 0'
						WHEN F.AnnualRevenue BETWEEN 100000 AND 499000 THEN '2. 100K-499K'
						WHEN F.AnnualRevenue BETWEEN 10000000 AND 49999999 THEN '5. 10M-49.9M'
						WHEN F.AnnualRevenue BETWEEN 1 AND 99999 THEN '1. 1-99K'
						WHEN F.AnnualRevenue BETWEEN 2000000 AND 9999999 THEN '4. 2M-9.9M'
						WHEN F.AnnualRevenue BETWEEN 500000 AND 1999999 THEN '3. 500K-1.9M'
						WHEN F.AnnualRevenue >= 500000000 THEN '6. 50M+'
					END
				WHEN LEN(F.AnnualRevenue) < 2 THEN '0.0'
					ELSE
						CASE WHEN F.AnnualRevenue IS NULL THEN '0.0' END
			END	AS RevenueRange,
			F.NumberEmployees,
			CASE
				WHEN F.NumberEmployees IS NOT NULL THEN
					CASE
	    			 	WHEN F.NumberEmployees = 0 THEN 'Employee Range of 0'
	    			 	WHEN F.NumberEmployees BETWEEN 1 AND 4 THEN 'Employee Range of 001-4'
	    			 	WHEN F.NumberEmployees BETWEEN 5 AND 9 THEN 'Employee Range of 005-9'
	    			 	WHEN F.NumberEmployees BETWEEN 10 AND 19 THEN 'Employee Range of 010-19'
	    			 	WHEN F.NumberEmployees BETWEEN 20 AND 49 THEN 'Employee Range of 020-49'
	    			 	WHEN F.NumberEmployees BETWEEN 50 AND 99 THEN 'Employee Range of 050-99'
	    			 	WHEN F.NumberEmployees BETWEEN 100 AND 199 THEN 'Employee Range of 100-199'
	    			 	WHEN F.NumberEmployees BETWEEN 200 AND 499 THEN 'Employee Range of 200-499'
	    			 	WHEN F.NumberEmployees >= 500 THEN 'Employee Range of 500 or over'
					END
  				WHEN F.NumberEmployees IS NULL THEN 'Employee Range of 0'
			END AS EmployeeRange,

			-- New Clients Funding ----

			F.FundingToDate,
			CASE
				WHEN F.FundingToDate IS NOT NULL THEN
					CASE
						WHEN F.FundingToDate < 1000 OR F.FundingToDate IS NULL THEN '0. 0'
						WHEN F.FundingToDate BETWEEN 1000 AND 19999 THEN '1. 1-19K'
						WHEN F.FundingToDate BETWEEN 20000 AND 49999 THEN '2. 20K-49K'
						WHEN F.FundingToDate BETWEEN 50000 AND 199999 THEN '3. 50K-199K'
						WHEN F.FundingToDate BETWEEN 200000 AND 499999 THEN '4. 200K-499K'
						WHEN F.FundingToDate BETWEEN 500000 AND 1999999 THEN '5. 500K-1.9M'
						WHEN F.FundingToDate BETWEEN 2000000 AND 4999999 THEN '6. 2M-4.9M'
						WHEN F.FundingToDate >= 5000000 THEN '7. 5M+'
					END
				WHEN F.FundingToDate IS NULL THEN '0. 0'
			END AS Funding_ToDate,
			F.FundingCurrentQuarter AS Funding_ThisQuarter,
			CASE WHEN F.Industry IS NOT NULL THEN I.Name ELSE 'Other' END AS Lvl2IndustryName,
			F.IntakeDate,

			---- Intake Fiscal Year ----

			CASE WHEN F.IntakeDate IS NULL  THEN NULL
				ELSE
					CASE WHEN MONTH(F.IntakeDate) BETWEEN 1 AND 3 THEN YEAR(F.IntakeDate)
		    			     WHEN MONTH(F.IntakeDate) BETWEEN 4 AND 12 THEN YEAR(F.IntakeDate) + 1

					END
			END AS IntakeFiscalYear,

		   ---- Intake Fiscal Quarter ----

			CASE
					WHEN F.IntakeDate IS NULL THEN NULL
						ELSE
								CASE 	WHEN MONTH(F.IntakeDate) BETWEEN 1 AND 3 THEN 4
		   	    			 		WHEN MONTH(F.IntakeDate) BETWEEN 4 AND 6 THEN 1
		   					 			WHEN MONTH(F.IntakeDate) BETWEEN 7 AND 9 THEN 2
		   					 			WHEN MONTH(F.IntakeDate) BETWEEN 10 AND 12 THEN 3
								END
			END AS IntakeFiscalQuarter,
			Y.Name AS Youth,
			D.ID AS DataSource,
			F.VolunteerYTD AS VolunteerHoursYTD,
			SY.Name AS SocialEnterprise

	  FROM MDCReport.BAPQ.FactRICVentureRollUp F
      LEFT JOIN MDCDim.dbo.DimDataSource D ON D.ID = F.DataSource
			LEFT JOIN MDCRaw.dbo.Venture V ON V.ID = F.CompanyID
			LEFT JOIN MDCDim.dbo.DimIndustry I ON I.ID = F.Industry
			LEFT JOIN MDCDim.dbo.DimStage S ON S.ID = F.Stage
			LEFT JOIN MDCDim.dbo.DimYesNo Y ON Y.ID = F.HighPotential
			LEFT JOIN MDCDim.dbo.DimYesNo SY ON F.SocialEnterprise = SY.ID

		WHERE F.FiscalYear = 2018 AND F.FiscalQuarter = 4 )


-------------------------------------------------------------
-------- //// AGGREGATES YOUTH AND NON-YOUTH //// -----------
-------------------------------------------------------------

  SELECT DISTINCT
	 A.AggregateNumber AS Value
	, M.FriendlyName
	, M.GroupName
	, D.Name as RICFriendlyName
	, M.ParentGroup
	, RIGHT(B.FiscalQuarter,1) as FiscalQuarter
	, B.FiscalYear as FiscalYear
	, NULL as LongIndustryName
	, NULL as ShortIndustryName
	, CASE WHEN A.Youth = 1 THEN 'Youth' ELSE 'All Including Youth' END AS Youth
	, A.DataSource
 FROM MDCReport.BAPQ.FactRICAggregation A
   LEFT JOIN MDCRaw.CONFIG.Batch B ON A.BatchID = B.BatchID
   LEFT JOIN MDCDim.dbo.DimMetric M ON A.MetricID = M.ID
   LEFT JOIN MDCDim.dbo.DimDataSource D ON A.DataSource = D.ID
 WHERE B.FiscalYear = 2018 AND B.FiscalQuarter = 'Q4'

-------------------------------------------------------------
-------------- //// COMPANY-LEVEL DATA //// -----------------
-------------------------------------------------------------


 ----------- **** Advisory Services ***** -------------

 ------------------------------------------------------
 ---------------- ADVISORY CLIENTS --------------------
 ------------------------------------------------------

 UNION
 SELECT
	  COUNT(DISTINCT CompanyID) AS 'Value'
 	, 'Number of clients assisted' AS Friendly
	, 'Advisory Services' AS GroupName
	, [RICFriendlyName]
	, 'Client Service' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE AdvisoryHours_thisQuarter > 0 AND FiscalYear = 2018 AND FiscalQuarter = 4
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


 ------------------------------------------------------
 --------- ADVISORY HOURS THIS QUARTER ----------------
 ------------------------------------------------------

 UNION
 SELECT
	  SUM([AdvisoryHours_thisQuarter]) Value
    , 'Number of advisory hours this quarter' AS Friendly
		, 'Advisory Services' AS GroupName
		, RICFriendlyName
		, 'Client Service' AS ParentGroup
		, FiscalQuarter
		, FiscalYear
		, NULL AS 'LongIndustryName'
		, NULL AS 'ShortIndustryName'
		, 'All including youth' AS Youth
		, DataSource
 FROM CTX
 WHERE FiscalYear = 2018 AND FiscalQuarter = 4
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource



 -------- **** Client Service Activity ***** --------

 ----------------------------------------------------
 ------------ CLIENTS THIS QUARTER ------------------
 ----------------------------------------------------

 UNION
 SELECT
	  COUNT(DISTINCT CompanyID) AS 'Value'
	, 'Clients receiving advisory services this quarter' AS Friendly
	, 'Client Service Activity' AS GroupName
	, RICFriendlyName
	, 'Client Service' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE (AdvisoryHours_thisQuarter > 0 or VolunteerHours_thisQuarter > 0)
			 AND FiscalYear = 2018 AND FiscalQuarter = 4
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource



----------------------------------------------------
-----------  HIGH POTENTIAL COMPANIES  -------------
----------------------------------------------------

 UNION
 SELECT
	  COUNT(DISTINCT CompanyID) AS 'Value'
	, 'High potential companies (YTD)' AS Friendly
	, 'Client Service Activity' AS GroupName
	, RICFriendlyName
	, 'Client Service' AS ParentGroup
  , FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE HighPotential = 'Yes' AND FiscalYear = 2018
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource



 ----------------------------------------------------
 ------------ NUMBER OF SOCIAL ENTERPRISE -----------
 ----------------------------------------------------

 UNION
 SELECT
	  COUNT(SocialEnterprise) AS Value
    , 'Social enterprises (YTD)' AS Friendly
    , 'Client Service Activity' AS GroupName
		, RICFriendlyName --COLLATE Latin1_General_CI_AS [RIC Friendly Name]
    , 'Client Service' AS [ParentGroup]
		, FiscalQuarter
		, FiscalYear
		, NULL AS LongIndustryName
		, NULL AS ShortIndustryName
		, 'All including youth' AS Youth
		, DataSource
 FROM CTX
 WHERE SocialEnterprise = 'Yes' AND FiscalYear = 2018
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource

 ----------------------------------------------------
 ----------------- NEW CLIENTS ----------------------
 ----------------------------------------------------

 UNION
 SELECT
	  COUNT(DISTINCT CompanyID) AS Value
    , 'Total new clients (QTD)' AS Friendly
    , 'Client Service Activity' AS GroupName
	  , RICFriendlyName
    , 'Client Service' AS ParentGroup
	  , FiscalQuarter
	  , FiscalYear
	  , NULL AS 'LongIndustryName'
	  , NULL AS 'ShortIndustryName'
	  , 'All including youth' AS Youth
	  , DataSource
 FROM CTX
 WHERE IntakeFiscalYear = 2018 AND IntakeFiscalQuarter = 4
			 AND (AdvisoryHours_thisQuarter > 0 OR VolunteerHours_thisQuarter > 0)
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource

-----------------------------------------------------
------------------ CLIENTS YTD ---------------------- ** starting here, below are YTD metrics till new clients section
-----------------------------------------------------

UNION
SELECT
	COUNT(DISTINCT CompanyID) AS Value
  , 'Total unique clients (YTD)' AS Friendly
  , 'Client Service Activity' AS GroupName
	, RICFriendlyName
  , 'Client Service' AS ParentGroup
	, 4 AS FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
FROM CTX
WHERE (AdvisoryHoursYTD > 0  OR VolunteerHoursYTD > 0) AND FiscalYear = 2018
GROUP BY RICFriendlyName, FiscalYear, DataSource

 ----------------------------------------------------
 --------- *****  FIRM AGE RANGE  ***** -------------
 ----------------------------------------------------

 UNION
 SELECT
 	COUNT(DISTINCT CompanyID) AS Value
 		, AgeRangeFriendly AS Friendly
 		, 'Firm Age' AS GroupName
 		, RICFriendlyName
 		, 'Firm Demographics' AS ParentGroup
 		, 4 AS FiscalQuarter
 		, FiscalYear
 		, NULL AS 'LongIndustryName'
 		, NULL AS 'ShortIndustryName'
 		, 'All including youth' AS Youth
 		, DataSource
 FROM CTX
 WHERE (AdvisoryHoursYTD > 0 OR VolunteerHoursYTD > 0) AND FiscalYear = 2018
 GROUP BY AgeRangeFriendly, RICFriendlyName, FiscalYear, DataSource

 ----------------------------------------------------
 ---------- *****  FIRM INDUSTRY  ***** -------------
 ----------------------------------------------------

 UNION
 SELECT
      COUNT(Lvl2IndustryName) AS Value
	, Lvl2IndustryName
	, 'Firm Industry' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, '' AS FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE (AdvisoryHoursYTD > 0  OR VolunteerHoursYTD > 0)
			 AND FiscalYear = 2018 AND Lvl2IndustryName IS NOT NULL
 GROUP BY Lvl2IndustryName, RICFriendlyName, FiscalYear, DataSource


 -----------------------------------------------------
 ------------ ***** FIRM STAGE ***** -----------------
 -----------------------------------------------------

 UNION
 SELECT
 	  COUNT(DISTINCT CompanyID) AS Value
 	, StageFriendly
 	, 'Firm Stage' AS GroupName
 	, RICFriendlyName
 	, 'Firm Demographics' AS ParentGroup
 	, '' AS FiscalQuarter
 	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE ([AdvisoryHoursYTD] > 0 OR [VolunteerHoursYTD] > 0) AND FiscalYear = 2018
 GROUP BY StageFriendly, FiscalYear,  RICFriendlyName,  DataSource


 -------------------------------------------------------------
 --------- ***** NEW CLIENTS EMPLOYEE RANGE ***** ------------
 -------------------------------------------------------------

 UNION
 SELECT
	COUNT(CompanyId) AS 'Value'
	, EmployeeRange AS Friendly
	, 'New Clients Employees' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE IntakeFiscalQuarter =4 AND IntakeFiscalYear = 2018 AND FiscalYear = 2018
			 AND FiscalQuarter = 4 AND (AdvisoryHours_thisQuarter > 0 OR VolunteerHours_thisQuarter > 0)
 GROUP BY EmployeeRange, RICFriendlyName, FiscalQuarter, FiscalYear, DataSource



 --------------------------------------------------------------
 ---------- ***** NEW CLIENTS REVENUE RANGE ***** -------------
 --------------------------------------------------------------

 UNION
 SELECT
	  COUNT(CompanyID) AS Value
	, RevenueRange AS Friendly
	, 'New Clients Revenue' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, 4 AS FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE IntakeFiscalQuarter = 4 AND IntakeFiscalYear = 2018
			 AND FiscalYear = 2018 AND FiscalQuarter = 4
			 AND (AdvisoryHours_thisQuarter > 0 OR VolunteerHours_thisQuarter > 0)
 GROUP BY RevenueRange, RICFriendlyName, FiscalQuarter, FiscalYear, DataSource




 --------------------------------------------------------------
 ---------- ***** NEW CLIENTS FUNDING TO-DATE ***** -----------
 --------------------------------------------------------------

 UNION
 SELECT
	  COUNT(CompanyID) AS Value
	, Funding_ToDate AS Friendly
	, 'New Clients Funding' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE IntakeFiscalQuarter = 4 AND IntakeFiscalYear = 2018
			 AND FiscalYear = 2018 AND FiscalQuarter = 4
			 AND (AdvisoryHours_thisQuarter > 0 OR VolunteerHours_thisQuarter > 0)
 GROUP BY Funding_ToDate, RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


 -------------------------------------------------------------
 -------------- FUNDING THIS QUARTER (ALL) -------------------
 -------------------------------------------------------------

 UNION
 SELECT
	   SUM(Funding_ThisQuarter) AS Value
	, 'Total funding raised by clients in the quarter' AS Friendly
	, 'All Clients Funding' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE FiscalYear = 2018 AND FiscalQuarter = 4
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource



  ------ ***** VOLUNTEER MENTOR NETWORK ***** ---------

 ------------------------------------------------------
 ----------- VOLUNTEER MENTOR CLIENTS -----------------
 ------------------------------------------------------

 UNION
 SELECT
 	  COUNT(DISTINCT CompanyID) AS Value
 	, 'Volunteer mentor clients' AS Friendly
 	, 'Volunteer Mentor Network' AS GroupName
 	, RICFriendlyName
	, 'Client Service' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE VolunteerHours_thisQuarter > 0 AND FiscalYear = 2018 AND FiscalQuarter = 4
 GROUP BY FiscalQuarter, FiscalYear, RICFriendlyName, DataSource


 ----------------------------------------------------------
 ------------- VOLUNTEER MENTOR HOURS ---------------------
 ----------------------------------------------------------

 UNION
 SELECT
	 SUM(VolunteerHours_thisQuarter) + 0
	, 'Volunteer hours' AS Friendly
	, 'Volunteer Mentor Network' AS GroupName
	, RICFriendlyName
	, 'Client Service' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'All including youth' AS Youth
	, DataSource
 FROM CTX
 WHERE FiscalYear = 2018 AND FiscalQuarter = 4
 GROUP BY FiscalQuarter, FiscalYear, RICFriendlyName, DataSource


 ---------- ########### YOUTH ########## ----------


 ---------- **** Advisory Services ***** -----------

 ------------------------------------------------------
 ------------------ NUMBER OF CLIENTS -----------------
 ------------------------------------------------------

 UNION
 SELECT
	  COUNT(DISTINCT CompanyID) AS Value
 	, 'Number of clients assisted' AS Friendly
	, 'Advisory Services' AS GroupName
	, RICFriendlyName
	, 'Client Service' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS LongIndustryName
	, NULL AS ShortIndustryName
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE AdvisoryHours_thisQuarter > 0 AND FiscalYear = 2018
			 AND FiscalQuarter = 4 AND Youth = 'Yes'
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


 ------------------------------------------------------
 --------- ADVISORY HOURS THIS QUARTER ----------------
 ------------------------------------------------------

 UNION
 SELECT
	 SUM(AdvisoryHours_thisQuarter) AS Value
   , 'Number of advisory hours this quarter' AS Friendly
	 , 'Advisory Services' AS GroupName
	 , RICFriendlyName
	 , 'Client Service' AS ParentGroup
	 , FiscalQuarter
	 , FiscalYear
	 , NULL AS 'LongIndustryName'
	 , NULL AS 'ShortIndustryName'
	 , 'Youth' AS Youth
	 , DataSource
 FROM CTX
 WHERE Youth = 'Yes' AND
	 FiscalYear = 2018 AND FiscalQuarter = 4
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


 ------- **** Client Service Activity ***** -------

 ----------------------------------------------------
 ------------ CLIENTS THIS QUARTER ------------------
 ----------------------------------------------------

 UNION
 SELECT
	  COUNT(DISTINCT CompanyID) AS Value
	  , 'Clients receiving advisory services this quarter' AS Friendly
	  , 'Client Service Activity' AS GroupName
	  , RICFriendlyName
	  , 'Client Service' AS ParentGroup
	  , FiscalQuarter
	  , FiscalYear
	  , NULL AS 'LongIndustryName'
	  , NULL AS 'ShortIndustryName'
	  , 'Youth' AS Youth
	  , DataSource
 FROM CTX
 WHERE (AdvisoryHours_thisQuarter > 0  OR VolunteerHours_thisQuarter > 0)
			 AND FiscalYear = 2018 AND FiscalQuarter = 4 AND Youth = 'Yes'
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource



----------------------------------------------------
----------- HIGH POTENTIAL COMPANIES ---------------
----------------------------------------------------

 UNION
 SELECT
	  COUNT(DISTINCT CompanyId) AS Value
	, 'High potential companies (YTD)' AS Friendly
	, 'Client Service Activity' AS GroupName
	, RICFriendlyName
	, 'Client Service' AS ParentGroup
  , FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE HighPotential = 'Yes'
			 AND FiscalYear = 2018
			  AND Youth = 'Yes'
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


 ----------------------------------------------------
 ----------------- NEW CLIENTS ----------------------
 ----------------------------------------------------

 UNION
 SELECT
	  COUNT(DISTINCT CompanyID) AS Value
    , 'Total new clients (QTD)' AS [Friendly]
    , 'Client Service Activity' AS [GroupName]
	, RICFriendlyName COLLATE Latin1_General_CI_AS [RIC Friendly Name]
    , 'Client Service' AS [ParentGroup]
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE IntakeFiscalYear = 2018 AND IntakeFiscalQuarter = 4
			 AND Youth = 'Yes'
			 AND (AdvisoryHours_thisQuarter > 0 OR VolunteerHours_thisQuarter > 0)
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


-----------------------------------------------------
------------------ CLIENTS YTD ----------------------
-----------------------------------------------------

UNION
SELECT
	 COUNT(DISTINCT CompanyID) AS Value
  , 'Total unique clients (YTD)' AS Friendly
  , 'Client Service Activity' AS GroupName
	, RICFriendlyName --COLLATE Latin1_General_CI_AS [RIC Friendly Name]
  , 'Client Service' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
FROM CTX
WHERE (AdvisoryHoursYTD > 0  OR VolunteerHoursYTD > 0)
			AND FiscalYear = 2018 AND Youth = 'Yes'
GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


-----------------------------------------------------
------------------ SOCIAL ENTERPRISE ----------------
-----------------------------------------------------


UNION
 SELECT
	  COUNT(SocialEnterprise) AS Value
    , 'Social enterprises (YTD)' AS Friendly
    , 'Client Service Activity' AS GroupName
	, RICFriendlyName --COLLATE Latin1_General_CI_AS [RIC Friendly Name]
    , 'Client Service' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE FiscalYear = 2018
			 AND SocialEnterprise = 'Yes'
			 AND Youth = 'Yes'
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


 ----------------------------------------------------
 ------------ *****  AGE RANGE  ***** ---------------
 ----------------------------------------------------

 UNION
 SELECT
 	  COUNT(DISTINCT CompanyID) AS Value
 	, AgeRangeFriendly AS Friendly
 	, 'Firm Age' AS GroupName
 	, RICFriendlyName
 	, 'Firm Demographics' AS ParentGroup
 	, FiscalQuarter
 	, FiscalYear
 	, NULL AS 'LongIndustryName'
 	, NULL AS 'ShortIndustryName'
 	, 'Youth' AS Youth
 	, DataSource

 FROM CTX
 WHERE (AdvisoryHoursYTD > 0 OR VolunteerHoursYTD > 0)
			 AND FiscalYear = 2018 AND Youth = 'Yes'
 GROUP BY AgeRangeFriendly, RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


 ----------------------------------------------------
 ------------ *****  INDUSTRY  ***** ----------------
 ----------------------------------------------------

 UNION
 SELECT
    COUNT(Lvl2IndustryName) AS Value
	, Lvl2IndustryName AS Friendly
	, 'Firm Industry' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE (AdvisoryHoursYTD > 0  OR VolunteerHoursYTD > 0)
			 AND FiscalYear = 2018 AND Youth = 'Yes' AND Lvl2IndustryName IS NOT NULL
 GROUP BY Lvl2IndustryName, RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


 -----------------------------------------------------
 ------------ ***** FIRM STAGE ***** -----------------
 -----------------------------------------------------

 UNION
 SELECT
 	  COUNT(DISTINCT CompanyID) AS Value
 	, StageFriendly AS Friendly
 	, 'Firm Stage' AS GroupName
 	, RICFriendlyName
 	, 'Firm Demographics' AS ParentGroup
 	, FiscalQuarter
 	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE (AdvisoryHoursYTD > 0 OR [VolunteerHoursYTD] > 0)
			 AND FiscalYear = 2018 AND Youth = 'Yes'
 GROUP BY FiscalYear, FiscalQuarter, StageFRIENDLY, RICFriendlyName,  DataSource


 -------------------------------------------------------------
 --------- ***** NEW CLIENTS EMPLOYEE RANGE ***** ------------
 -------------------------------------------------------------

 UNION
 SELECT
	COUNT(CompanyId) AS Value
	, EmployeeRange AS Friendly
	, 'New Clients Employees' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE IntakeFiscalQuarter =4 AND IntakeFiscalYear = 2018
			 AND FiscalYear = 2018 AND FiscalQuarter = 4
			 AND Youth = 'Yes'
			 AND (AdvisoryHours_thisQuarter > 0 OR VolunteerHours_thisQuarter > 0)
 GROUP BY EmployeeRange, RICFriendlyName, FiscalQuarter, FiscalYear, DataSource



 --------------------------------------------------------------
 ---------- ***** NEW CLIENTS REVENUE RANGE ***** -------------
 --------------------------------------------------------------

 UNION
 SELECT
	  COUNT(CompanyID) AS Value
	, RevenueRange AS Friendly
	, 'New Clients Revenue' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE IntakeFiscalQuarter = 4 AND IntakeFiscalYear = 2018
			 AND FiscalYear = 2018 AND FiscalQuarter = 4 AND Youth = 'Yes'
			 AND (AdvisoryHours_thisQuarter > 0 OR VolunteerHours_thisQuarter > 0)
 GROUP BY RevenueRange, RICFriendlyName, FiscalQuarter, FiscalYear, DataSource



 --------------------------------------------------------------
 ---------- ***** NEW CLIENTS FUNDING TO-DATE ***** -----------
 --------------------------------------------------------------

 UNION
 SELECT
	  COUNT(CompanyID) AS Value
	, Funding_ToDate AS Friendly
	, 'New Clients Funding' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE IntakeFiscalQuarter = 4 AND IntakeFiscalYear = 2018
			 AND FiscalYear = 2018 AND FiscalQuarter = 4 AND Youth = 'Yes'
			 AND (AdvisoryHours_thisQuarter > 0 OR VolunteerHours_thisQuarter > 0)
 GROUP BY Funding_ToDate, RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


 -------------------------------------------------------------
 ----------------- FUNDING THIS QUARTER ----------------------
 -------------------------------------------------------------

 UNION
 SELECT
	  SUM(Funding_ThisQuarter) AS Value
	, 'Total funding raised by clients in the quarter' AS Friendly
	, 'All Clients Funding' AS GroupName
	, RICFriendlyName
	, 'Firm Demographics' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE FiscalYear = 2018 AND FiscalQuarter = 4 AND Youth = 'Yes'
 GROUP BY RICFriendlyName, FiscalQuarter, FiscalYear, DataSource


  ------ ***** VOLUNTEER MENTOR NETWORK ***** ---------

 ------------------------------------------------------
 ----------- VOLUNTEER MENTOR CLIENTS -----------------
 ------------------------------------------------------

 UNION
 SELECT
 	  COUNT(DISTINCT CompanyID) AS Value
 	, 'Volunteer mentor clients' AS Friendly
 	, 'Volunteer Mentor Network' AS GroupName
 	, RICFriendlyName
	, 'Client Service' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE [VolunteerHours_thisQuarter] > 0 AND FiscalYear = 2018
			 AND FiscalQuarter = 4 AND Youth = 'Yes'
 GROUP BY FiscalQuarter, FiscalYear, RICFriendlyName, DataSource


 ----------------------------------------------------------
 ------------- VOLUNTEER MENTOR HOURS ---------------------
 ----------------------------------------------------------

 UNION
 SELECT
	 SUM(VolunteerHours_thisQuarter) + 0 AS Value
	, 'Volunteer hours' AS Friendly
	, 'Volunteer Mentor Network' AS GroupName
	, RICFriendlyName
	, 'Client Service' AS ParentGroup
	, FiscalQuarter
	, FiscalYear
	, NULL AS 'LongIndustryName'
	, NULL AS 'ShortIndustryName'
	, 'Youth' AS Youth
	, DataSource
 FROM CTX
 WHERE FiscalYear = 2018 AND FiscalQuarter = 4 AND Youth = 'Yes'
 GROUP BY FiscalQuarter, FiscalYear, RICFriendlyName, DataSource
