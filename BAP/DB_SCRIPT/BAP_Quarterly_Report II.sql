SELECT --DISTINCT
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

		WHERE F.FiscalYear = 2018 AND F.FiscalQuarter = 4 ORDER BY F.CompanyID


--====================================================================================================================================================================================

SELECT DISTINCT
			V.Name,
			V.ID,
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
						WHEN CAST(F.AnnualRevenue AS float) = 0 OR F.AnnualRevenue IS NULL THEN 	'0.0'
						WHEN CAST(F.AnnualRevenue AS float) BETWEEN 100000 AND 499000 THEN 				'2. 100K-499K'
						WHEN CAST(F.AnnualRevenue AS float) BETWEEN 10000000 AND 49999999 THEN 		'5. 10M-49.9M'
						WHEN CAST(F.AnnualRevenue AS float) BETWEEN 1 AND 99999 THEN 							'1. 1-99K'
						WHEN CAST(F.AnnualRevenue AS float) BETWEEN 2000000 AND 9999999 THEN 			'4. 2M-9.9M'
						WHEN CAST(F.AnnualRevenue AS float) BETWEEN 500000 AND 1999999 THEN 			'3. 500K-1.9M'
						WHEN CAST(F.AnnualRevenue AS float) >= 500000000 THEN 										'6. 50M+'
					END
				WHEN LEN(F.AnnualRevenue) < 2 THEN '0.0'
					ELSE
						CASE WHEN F.AnnualRevenue IS NULL THEN '0.0' END
			END	AS RevenueRange,

			-- Employee Range ---
			F.NumberEmployees,
			--TRY_CAST(FactRICCompanyHoursRolledUp.NumberEmployees AS numeric),
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
						WHEN F.FundingToDate < 1000 OR F.FundingToDate IS NULL THEN '0.0'
						WHEN F.FundingToDate BETWEEN 1000 AND 19999 THEN '1. 1-19K'
						WHEN F.FundingToDate BETWEEN 20000 AND 49999 THEN '2. 20K-49K'
						WHEN F.FundingToDate BETWEEN 50000 AND 199999 THEN '3. 50K-199K'
						WHEN F.FundingToDate BETWEEN 200000 AND 499999 THEN '4. 200K-499K'
						WHEN F.FundingToDate BETWEEN 500000 AND 1999999 THEN '5. 500K-1.9M'
						WHEN F.FundingToDate BETWEEN 2000000 AND 4999999 THEN '6. 2M-4.9M'
						WHEN F.FundingToDate >= 5000000 THEN '7. 5M+'
					END
				WHEN F.FundingToDate IS NULL THEN '0.0'
			END AS Funding_ToDate,

			TRY_CAST(F.FundingCurrentQuarter AS float) AS Funding_ThisQuarter,
			--FactRICCompanyHoursRolledUp.IndustrySector,
			CASE WHEN F.Industry IS NOT NULL THEN I.Name ELSE 'Other' END AS Lvl2IndustryName,
			F.IntakeDate,

			---- Intake Fiscal Year ----

-- 		CASE WHEN F.IntakeDate IS NULL  THEN NULL
-- 				ELSE
-- 					CASE WHEN TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime) IS NOT NULL THEN  --Handle the case where date has format "42034"
-- 							CASE WHEN MONTH(TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime)) BETWEEN 1 AND 3 THEN YEAR(TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime))
-- 		    			     WHEN MONTH(TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime)) BETWEEN 4 AND 12 THEN YEAR(TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime)) + 1
-- 							END
-- 							ELSE
-- 								CASE WHEN TRY_CAST(F.IntakeDate AS DATE) IS NULL THEN
-- 									CASE WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 1 AND 3 THEN RIGHT(RTRIM(F.IntakeDate), 4)
-- 										WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 4 AND 12 THEN RIGHT(RTRIM(F.IntakeDate), 4) + 1
-- 									END
-- 										ELSE
-- 											CASE WHEN TRY_CAST(F.IntakeDate AS DATE) IS NOT NULL THEN
-- 												CASE WHEN TRY_CAST(LEFT(F.IntakeDate,3) AS INT) IS NULL AND TRY_CAST(LEFT(F.IntakeDate,2) AS INT) IS NULL THEN
-- 													CASE WHEN MONTH(TRY_CAST(F.IntakeDate AS DATE)) BETWEEN 1 AND 3 THEN YEAR(TRY_CAST(F.IntakeDate AS DATE))
-- 		    										   WHEN MONTH(TRY_CAST(F.IntakeDate AS DATE)) BETWEEN 4 AND 12 THEN YEAR(TRY_CAST(F.IntakeDate AS DATE)) + 1
-- 		    									END
-- 													ELSE
-- 														CASE WHEN LEFT(IntakeDate, 4) != LEFT(TRY_CAST(INTAKEDATE AS date), 4) THEN
-- 															CASE WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 1 AND 3 THEN YEAR(TRY_CAST(F.IntakeDate AS DATE))
-- 					  								 		WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 4 AND 12 THEN YEAR(TRY_CAST(F.IntakeDate AS DATE)) + 1
-- 		    											END
-- 																ELSE
-- 																CASE WHEN LEFT(IntakeDate, 4) = LEFT(TRY_CAST(INTAKEDATE AS date), 4) OR YEAR(TRY_CAST(F.IntakeDate AS DATE)) = LEFT(TRY_CAST(INTAKEDATE AS date), 4) THEN
-- 		    													CASE WHEN MONTH(TRY_CAST(F.IntakeDate AS DATE)) BETWEEN 1 AND 3 THEN YEAR(TRY_CAST(F.IntakeDate AS DATE))
-- 		    														   	WHEN MONTH(TRY_CAST(F.IntakeDate AS DATE)) BETWEEN 4 AND 12 THEN YEAR(TRY_CAST(F.IntakeDate AS DATE)) + 1
-- 		    													END
-- 																END
-- 														END
-- 												END
-- 											END
-- 								END
-- 					END
-- 		END AS IntakeFiscalYear,

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

-- 		    CASE
-- 					WHEN F.IntakeDate IS NULL THEN NULL
-- 						ELSE
-- 							CASE WHEN TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime) IS NOT NULL THEN
-- 								CASE 	WHEN MONTH(TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime)) BETWEEN 1 AND 3 THEN '4'
-- 		   	    			 		WHEN MONTH(TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime)) BETWEEN 4 AND 6 THEN '1'
-- 		   					 			WHEN MONTH(TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime)) BETWEEN 7 AND 9 THEN '2'
-- 		   					 			WHEN MONTH(TRY_CAST(TRY_CAST(F.IntakeDate AS float) AS datetime)) BETWEEN 10 AND 12 THEN '3'
-- 		   					END
-- 								ELSE
-- 									CASE
-- 										WHEN TRY_CAST(F.IntakeDate AS date) IS NULL THEN
-- 											CASE
-- 												WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 1 AND 3 THEN '4'
-- 		   								 	WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 4 AND 6 THEN '1'
-- 		   								 	WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 7 AND 9 THEN '2'
-- 		   								 	WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 10 AND 12 THEN '3'
-- 											END
-- 											ELSE
-- 												CASE
-- 													WHEN TRY_CAST(F.IntakeDate AS date) IS NOT NULL THEN
-- 		   											CASE WHEN TRY_CAST(LEFT(F.IntakeDate,3) AS INT) IS NOT NULL THEN
-- 		    											CASE
-- 																WHEN MONTH(F.IntakeDate) BETWEEN 1 AND 3 THEN '4'
-- 		   	  						  			 	WHEN MONTH(F.IntakeDate) BETWEEN 4 AND 6 THEN '1'
-- 		   											 		WHEN MONTH(F.IntakeDate) BETWEEN 7 AND 9 THEN '2'
-- 		   											 		WHEN MONTH(F.IntakeDate) BETWEEN 10 AND 12 THEN '3'
-- 		   												END
-- 		   	  										ELSE
-- 																CASE
-- 																	WHEN TRY_CAST(LEFT(F.IntakeDate,3) AS INT) IS NULL THEN
-- 																		CASE
-- 																			WHEN RIGHT(RTRIM(F.IntakeDate),2) = 'AM' THEN
-- 																			CASE
-- 																				WHEN MONTH(F.IntakeDate) BETWEEN 1 AND 3 THEN '4'
-- 		   	  										  				WHEN MONTH(F.IntakeDate) BETWEEN 4 AND 6 THEN '1'
-- 		   																 	WHEN MONTH(F.IntakeDate) BETWEEN 7 AND 9 THEN '2'
-- 		   																 	WHEN MONTH(F.IntakeDate) BETWEEN 10 AND 12 THEN '3'
-- 		   																END
-- 																			ELSE
-- 																				CASE
-- 																					WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 1 AND 3 THEN '4'
-- 		   	  																WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 4 AND 6 THEN '1'
-- 		   	  																WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 7 AND 9 THEN '2'
-- 		   	  																WHEN TRY_CAST(SUBSTRING(F.IntakeDate,4,2) AS INT) BETWEEN 10 AND 12 THEN '3'
--    																			END
-- 																		END
-- 																END
-- 														END
-- 												END
-- 									END
-- 							END
-- 				END AS IntakeFiscalQuarter,

			Y.Name,
			D.ID,
			F.VolunteerYTD AS VolunteerHoursYTD,
			SY.Name AS SocialEnterprise



	  FROM MDCReport.BAPQ.FactRICVentureRollUp F
      LEFT JOIN MDCDim.dbo.DimDataSource D ON D.ID = F.DataSource
			LEFT JOIN MDCRaw.dbo.Venture V ON V.ID = F.CompanyID
			LEFT JOIN MDCDim.dbo.DimIndustry I ON I.ID = F.Industry
			LEFT JOIN MDCDim.dbo.DimStage S ON S.ID = F.Stage
			LEFT JOIN MDCDim.dbo.DimYesNo Y ON Y.ID = F.HighPotential
			LEFT JOIN MDCDim.dbo.DimYesNo SY ON F.SocialEnterprise = SY.ID

--
-- 		-- Revenue Range ----
-- 		LEFT JOIN Reporting.DimRevenueRange ON FactRICCompanyHoursRolledUp.AnnualRevenue = DimRevenueRange.RevenueRange
-- 		LEFT JOIN Reporting.DimEmployeeRange ON FactRICCompanyHoursRolledUp.NumberEmployees = DimEmployeeRange.Range
--
-- 		-- Employee Range ----
-- 		LEFT JOIN Reporting.DimMetric MRevenue ON DimRevenueRange.MetricID = MRevenue.MetricID
-- 		LEFT JOIN Reporting.DimMetric MEmploy ON DimEmployeeRange.MetricID = MEmploy.MetricID
--
-- 		-- Funding to date ----
-- 		LEFT JOIN Reporting.DimFunding FundToDate ON FactRICCompanyHoursRolledUp.FundingToDate = FundToDate.FundingName
-- 		LEFT JOIN Reporting.DimMetric MFundToDate ON FundToDate.MetricId = MFundToDate.MetricID
--
-- 		-- Funding this quarter ----
-- 		LEFT JOIN Reporting.DimFunding FundThisQ ON FactRICCompanyHoursRolledUp.FundingCurrentQuarter = FundThisQ.FundingName
-- 		LEFT JOIN Reporting.DimMetric MFundThisQ ON FundThisQ.MetricId = MFundThisQ.MetricID


		WHERE F.FiscalYear = 2018 AND FactRICCompanyHoursRolledUp.FiscalQuarter = 4


