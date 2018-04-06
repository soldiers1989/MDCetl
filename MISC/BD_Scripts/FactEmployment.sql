-- SELECT * FROM MDCReport.dbo.FactEmployment
-- TRUNCATE TABLE MDCReport.dbo.FactEmployment
--INSERT INTO MDCReport.dbo.FactEmployment
SELECT BatchId, CompanyID, DataSourceID, CurrentFormer,
    CASE WHEN DataSource = 'Crunchbase.Organizations' THEN 14
         WHEN DataSource = 'IAF.FactActivityByQuarter' THEN 15
         WHEN DataSource = 'IAF.IAFactivity2014' THEN 16
         WHEN DataSource = 'IAFThomsonReuters2014and2013activity' THEN 17
         WHEN DataSource = 'JLABS.FactSurveyNorm' THEN 18
         WHEN DataSource = 'Reporting.FactRICCompanyData' THEN 5
         WHEN DataSource = 'Reporting.vRICsurveyEmploymentAgeRange_viz' THEN 19
         WHEN DataSource = 'RICSurveyFlat.RICSurvey2015' THEN 8
         WHEN DataSource = 'RICSurveyFlat.RICSurvey2015Supplement' THEN 20
         WHEN DataSource = 'RICSurveyFlat.RICSurvey2015Triphase' THEN 9
         WHEN DataSource = 'RICSurveyFlat.RICSurvey2016MaRSSupplement' THEN 10
         WHEN DataSource = 'RICSurveyFlat.RICSurvey2016Norm' THEN 11
         WHEN DataSource = 'ThomsonReuters.DealsDetailedView' THEN 21 END as TableSource,
    CASE WHEN TimeType = 'FullTime' THEN 5
         WHEN TimeType = 'PartTime' THEN 6
         WHEN TimeType = 'Unknown' THEN 7 END as TimeType,
    CASE WHEN Contract = 'Contract' THEN 8
         WHEN Contract = 'Non_Contract' THEN 9
         WHEN Contract = 'Unknown' THEN 10 END as Contract,
   CASE  WHEN TimeOfYear = 'Beginning_of_year' THEN 1
         WHEN TimeOfYear = 'End_of_year' THEN 2
         WHEN TimeOfYear = 'Founders' THEN 3
         WHEN TimeOfYear = 'Unknown' THEN 4 END as TimeOfYear,
    CASE WHEN IndustryLvl1 = 'Advanced Health' THEN 1
         WHEN IndustryLvl1 = 'Advanced Manuf.' THEN 2
         WHEN IndustryLvl1 = 'Agriculture' THEN 3
         WHEN IndustryLvl1 = 'Cleantech' THEN 4
         WHEN IndustryLvl1 = 'Education' THEN 5
         WHEN IndustryLvl1 = 'Fintech' THEN 6
         WHEN IndustryLvl1 = 'Food & Beverage' THEN 7
         WHEN IndustryLvl1 = 'Forestry' THEN 8
         WHEN IndustryLvl1 = 'ICT' THEN 9
         WHEN IndustryLvl1 = 'Other' THEN 10
         WHEN IndustryLvl1 = 'Tourism' THEN 11 END as IndustryLI,
    CASE WHEN IndustryLvl2 = 'Agriculture - Biological Agriculture' THEN 12
         WHEN IndustryLvl2 = 'Agriculture - IT' THEN 13
         WHEN IndustryLvl2 = 'Agriculture - Other' THEN 14
         WHEN IndustryLvl2 = 'Food & Beverage - IT' THEN 15
         WHEN IndustryLvl2 = 'Food & Beverage - Other' THEN 16
         WHEN IndustryLvl2 = 'Forestry - IT' THEN 17
         WHEN IndustryLvl2 = 'Forestry - Other' THEN 18
         WHEN IndustryLvl2 = 'Mining - IT' THEN 19
         WHEN IndustryLvl2 = 'Mining - Other' THEN 20
         WHEN IndustryLvl2 = 'Financial Services - IT' THEN 21
         WHEN IndustryLvl2 = 'Financial Services - Other' THEN 22
         WHEN IndustryLvl2 = 'Education - IT' THEN 23
         WHEN IndustryLvl2 = 'Education - Other' THEN 24
         WHEN IndustryLvl2 = 'Digital Media & ICT - Apps and Software' THEN 25
         WHEN IndustryLvl2 = 'Digital Media & ICT - Hardware' THEN 26
         WHEN IndustryLvl2 = 'Digital Media & ICT - Other' THEN 27
         WHEN IndustryLvl2 = 'Clean Technologies - Water Technologies' THEN 28
         WHEN IndustryLvl2 = 'Clean Technologies - Recycling and Waste Management' THEN 29
         WHEN IndustryLvl2 = 'Clean Technologies - Energy and Power Technologies' THEN 30
         WHEN IndustryLvl2 = 'Clean Technologies - Other' THEN 31
         WHEN IndustryLvl2 = 'Advanced Materials & Manufacturing - Materials and Chemicals' THEN 32
         WHEN IndustryLvl2 = 'Advanced Materials & Manufacturing - Manufactured Goods' THEN 33
         WHEN IndustryLvl2 = 'Advanced Materials & Manufacturing - Manufacturing Processes' THEN 34
         WHEN IndustryLvl2 = 'Advanced Materials & Manufacturing - Other' THEN 35
         WHEN IndustryLvl2 = 'Healthcare - Pharmaceuticals' THEN 36
         WHEN IndustryLvl2 = 'Healthcare - Medical Devices' THEN 37
         WHEN IndustryLvl2 = 'Healthcare - IT' THEN 38
         WHEN IndustryLvl2 = 'Healthcare - Wellness & Nutrition' THEN 39
         WHEN IndustryLvl2 = 'Healthcare - Other' THEN 40
         WHEN IndustryLvl2 = 'Tourism and Culture - IT' THEN 41
         WHEN IndustryLvl2 = 'Tourism and Culture - Other' THEN 42  END as IndustryLII,
    Amount, AssocDate,
    CASE WHEN GenderMix = 'All Female' THEN 1
         WHEN GenderMix = 'All Male' THEN 2
         WHEN GenderMix = 'Mix Male and Female' THEN 3 END AS GenderMIX,
    CASE WHEN YouthMix = 'All Not Youth' THEN 4
         WHEN YouthMix = 'All Youth' THEN 5
         WHEN YouthMix = 'Mix Youth and Not Youth' THEN 6 END AS YouthMix,
    CASE WHEN FirstVentureMix = 'All First Venture' THEN 7
         WHEN FirstVentureMix = 'Extremely High' THEN 8
         WHEN FirstVentureMix = 'Low' THEN 9
         WHEN FirstVentureMix = 'Mix First and Not First Venture' THEN 10
         WHEN FirstVentureMix = 'Moderate' THEN 11
         WHEN FirstVentureMix = 'None' THEN 12
         WHEN FirstVentureMix = 'None First Venture' THEN 13
         WHEN FirstVentureMix = 'Very High' THEN 14 END AS FirstVentureMix,
    CASE WHEN CountryOfOriginMix = 'All Canadian Origin' THEN 15
         WHEN CountryOfOriginMix = 'All Foreign Origin' THEN 16
         WHEN CountryOfOriginMix = 'Mix Canadian-Foreign Origin' THEN 17 END AS CountryOfOriginMix,
    CASE WHEN MaRS_sector = 'Cleantech' THEN 1
         WHEN MaRS_sector = 'Finance & Commerce' THEN 2
         WHEN MaRS_sector = 'Health' THEN 3
         WHEN MaRS_sector = 'Non-Profit/ Social Enterprise/ Charity' THEN 4
         WHEN MaRS_sector = 'Professional Services' THEN 5
         WHEN MaRS_sector = 'Retail and Consumer Products' THEN 6
         WHEN MaRS_sector = 'Social Purpose' THEN 7
         WHEN MaRS_sector = 'Work & Learning' THEN 8 END AS MaRSSector,
    CASE WHEN MaRS_program = 'Growth' THEN 1
         WHEN MaRS_program = 'Scale' THEN 2
         WHEN MaRS_program = 'Start' THEN 3 END AS MaRSProgram,
   MaRS_client_indicator, IAF_indicator, CII_indicator, Z_index, SurveyYear, CompanyName

FROM MaRSDataCatalyst.RICSurveyFlat.vEmploymentBySupportOrg
