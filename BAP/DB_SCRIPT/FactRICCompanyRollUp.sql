
SELECT F.BatchID, F.DataSourceID, F.CompanyID,F.CurrentDate as DateID, F.MinDate,
  F.VolunteerThisQuarter, F.VolunteerYTD, F.AdvisoryThisQuarter, F.AdvisoryHoursYTD,
  CASE
  WHEN SocialEnterprise IN ('N', 'No', '')
    THEN 0
  WHEN SocialEnterprise IN ('Y', 'Yes')
    THEN 1
  ELSE NULL END                            AS [SocialEnterprise],
   CASE
  WHEN Stage LIKE '%Idea%' OR Stage LIKE '%0%' OR Stage LIKE '%ideation%'
    THEN 1
  WHEN Stage LIKE '%Discovery%' OR Stage LIKE '%1%'
    THEN 2
  WHEN Stage LIKE '%Validation%' OR Stage LIKE '%2%'
    THEN 3
  WHEN Stage LIKE '%Efficiency%' OR Stage LIKE '%3%'
    THEN 4
--   WHEN Stage LIKE '%scale%' OR Stage LIKE '%4%' OR Stage LIKE '%Scale%'
--     THEN 5
  WHEN Stage IS NULL OR Stage = '0' OR Stage = ''
    THEN 6
  ELSE NULL END                            AS [Stage],
  CASE
  WHEN HighPotential IN ('0', 'n', 'No', '')
    THEN 0
  WHEN HighPotential IN ('1', 'y', 'Yes', 'High')
    THEN 1
  ELSE NULL END  AS HighPotential,
 CASE
  WHEN Lvl2IndustryName LIKE '%Advanced Manufacturing%'
       OR Lvl2IndustryName LIKE '%Adv. Materials%'
       OR Lvl2IndustryName LIKE '%materials%'
       OR Lvl2IndustryName LIKE '%Manufactur%'
    THEN 1
  WHEN Lvl2IndustryName LIKE '%agricult%'
       OR Lvl2IndustryName LIKE '%agro%'
    THEN 2
  WHEN Lvl2IndustryName LIKE '%Clean%Tech%'
       OR Lvl2IndustryName LIKE '%energy%'
       OR Lvl2IndustryName LIKE '%recycl%'
       OR Lvl2IndustryName LIKE '%water%'
       OR Lvl2IndustryName LIKE '%green%energy%'
    THEN 3
  WHEN Lvl2IndustryName LIKE '%ICT%'
       OR Lvl2IndustryName LIKE '%Digital%Media%'
       OR Lvl2IndustryName LIKE '%app%'
       OR Lvl2IndustryName LIKE '%entertainment%'
       OR Lvl2IndustryName LIKE '%hardware%'
       OR Lvl2IndustryName LIKE '%software%'
    THEN 4
  WHEN Lvl2IndustryName LIKE '%Education%'
    THEN 5
  WHEN Lvl2IndustryName LIKE '%Financial%'
    THEN 6
  WHEN Lvl2IndustryName LIKE '%Food%' OR Lvl2IndustryName LIKE '%Beverage%'
    THEN 7
  WHEN Lvl2IndustryName LIKE '%Forestry%'
    THEN 8
  WHEN Lvl2IndustryName LIKE '%Life%Science%'
       OR Lvl2IndustryName LIKE '%health%'
       OR Lvl2IndustryName LIKE '%wellness%'
       OR Lvl2IndustryName LIKE '%medical%'
       OR Lvl2IndustryName LIKE '%pharma%'
    THEN 9
  WHEN Lvl2IndustryName LIKE '%Mining%'
    THEN 10
  WHEN Lvl2IndustryName LIKE '%Other%'
    THEN 11
  WHEN Lvl2IndustryName LIKE '%Tourism%' OR Lvl2IndustryName LIKE '%culture%'
    THEN 12
  WHEN Lvl2IndustryName IS NULL
    THEN NULL
  ELSE 11 END                              AS Lvl2IndustryName,
  NULL AS Lvl2IndustryName, NULL AS DateOfIncorporation, F.AnnualRevenue,
  F.NumberEmployees, F.FundingCurrentQuarter, F.FundingToDate, NULL AS IntakeDate,
   CASE
  WHEN Youth IN ('0', 'n', 'No', '')
    THEN 0
  WHEN Youth IN ('1', 'y', 'Yes')
    THEN 1
  ELSE NULL END                            AS [Youth],
  NULL AS CreatedDate,
  F.ModifiedDate, F.FiscalQuarter, F.FiscalYear, F.CompanyHoursRolledUpId
FROM Reporting.FactRICCompanyHoursRolledUp F
WHERE F.Stage NOT IN (1,2,3,4,5)




--==========================================================================================================================================


-- -----------------------------------------------------------------------------------

UPDATE F
  SET Stage = 1
--   SELECT Stage
  FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp F
WHERE F.Stage IN ('Stage 0 - Stage 0 - stage 0 - idea',
'Stage 0 - stage 0 - idea',
'Stage 0 - Idea',
'Stage 0',
'Ideation',
'Idea,Validation',
'Idea,Scale',
'Idea,Prototype',
'Idea,Discovery,Validation',
'Idea'
)
-- -----------------------------------------------------------------------------------
UPDATE F
  SET Stage = 2
--   SELECT Stage
  FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp F
WHERE F.Stage IN (
    'Discovery (Market Fit)',
    'Discovery','Stage 1 - Discovery'
)
-- -----------------------------------------------------------------------------------

UPDATE F
  SET Stage = 3
--   SELECT Stage
  FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp F
WHERE F.Stage IN (
'Validation (Product Fit)',
'Validation',
'Stage 2 - Validation',
'Stage 2 - Stage 2 - Validation',
'Stage 2')
-- -----------------------------------------------------------------------------------
UPDATE F
  SET Stage = 4
--   SELECT Stage
  FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp F
WHERE F.Stage IN (
'Stage 3 - Stage 3 - Efficiency',
'Stage 3 - Efficiency',
'Stage 3',
'Efficientcy',
'Efficiency,Validation,Discovery',
'Efficiency'
)
-- -----------------------------------------------------------------------------------
UPDATE F
  SET Stage = 5
--   SELECT Stage
  FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp F
WHERE F.Stage IN (
'Stage 4 - Stage 4 - Scale',
'Stage 4 - Scale',
'Stage 4',
'Scaling',
'Scale,Discovery',
'Scale (Growth)',
'Scale')

-- -----------------------------------------------------------------------------------
UPDATE F
  SET Stage = 1
--   SELECT Stage
  FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp F
WHERE F.Stage IN (
'Intake',
'Educate'
)
-- -----------------------------------------------------------------------------------

UPDATE F
  SET Stage = 4
--   SELECT Stage
  FROM MDCReport.BAPQ.FactRICCompanyHoursRollUp F
WHERE F.Stage IN (
'Traction'
)

--==========================================================================================================================================

UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 10000.0 WHERE AnnualRevenue = '10000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 230000.0 WHERE AnnualRevenue = '230000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 36000000.0 WHERE AnnualRevenue = '36000000.0                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 30000.0 WHERE AnnualRevenue = '30000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 600.0 WHERE AnnualRevenue = '600                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 15.93 WHERE AnnualRevenue = '15.93                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3328180.0 WHERE AnnualRevenue = '3328180.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 30.0 WHERE AnnualRevenue = '30.00                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 138000.0 WHERE AnnualRevenue = '138000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 74000.0 WHERE AnnualRevenue = '74000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 800000.0 WHERE AnnualRevenue = '800000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 300820.0 WHERE AnnualRevenue = '300820                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 170000.0 WHERE AnnualRevenue = '170000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1400.0 WHERE AnnualRevenue = '1400                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 66247.0 WHERE AnnualRevenue = '66247                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 430000.0 WHERE AnnualRevenue = '430000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 450000.0 WHERE AnnualRevenue = '450000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 43412.0 WHERE AnnualRevenue = '43412                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 8500.0 WHERE AnnualRevenue = '8500                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 240000.0 WHERE AnnualRevenue = '240000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 900000.0 WHERE AnnualRevenue = '900000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2150000.0 WHERE AnnualRevenue = '2150000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4146309.0 WHERE AnnualRevenue = '4146309                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4100000.0 WHERE AnnualRevenue = '4100000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 6000000.0 WHERE AnnualRevenue = '6000000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 140000.0 WHERE AnnualRevenue = '140000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 6000000.0 WHERE AnnualRevenue = '6000000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 7224.0 WHERE AnnualRevenue = '7224                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 160000.0 WHERE AnnualRevenue = '160000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 45000.0 WHERE AnnualRevenue = '45000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 650000.0 WHERE AnnualRevenue = '650000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1500.0 WHERE AnnualRevenue = '1500                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 376893.0 WHERE AnnualRevenue = '376893                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 40000.0 WHERE AnnualRevenue = '40000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 10000000.0 WHERE AnnualRevenue = '10000000.00                                       '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 888.0 WHERE AnnualRevenue = '888                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 7600000.0 WHERE AnnualRevenue = '7600000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 542000.0 WHERE AnnualRevenue = '542000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1034000.0 WHERE AnnualRevenue = '1034000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 700000.0 WHERE AnnualRevenue = '700000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 642000.0 WHERE AnnualRevenue = '642000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 365200.0 WHERE AnnualRevenue = '365200                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3000.0 WHERE AnnualRevenue = '3000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 407000.0 WHERE AnnualRevenue = '407000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4000000.0 WHERE AnnualRevenue = '4000000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 366720.0 WHERE AnnualRevenue = '366720                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2400.0 WHERE AnnualRevenue = '2400                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 20000000.0 WHERE AnnualRevenue = '20000000.00                                       '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 45000.0 WHERE AnnualRevenue = '45000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 29100.0 WHERE AnnualRevenue = '29100                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 450001.0 WHERE AnnualRevenue = '450001                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 307000.0 WHERE AnnualRevenue = '307000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 87773.0 WHERE AnnualRevenue = '87773                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 25000000.0 WHERE AnnualRevenue = '25000000                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 171500.0 WHERE AnnualRevenue = '171500                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 225000.0 WHERE AnnualRevenue = '225000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4000.0 WHERE AnnualRevenue = '4000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2250000.0 WHERE AnnualRevenue = '2250000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 16800.0 WHERE AnnualRevenue = '16800                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 700.0 WHERE AnnualRevenue = '700                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 85000.0 WHERE AnnualRevenue = '85000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 18000.0 WHERE AnnualRevenue = '18000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1350000.0 WHERE AnnualRevenue = '1350000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 13000.0 WHERE AnnualRevenue = '13000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1100000.0 WHERE AnnualRevenue = '1100000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2700000.0 WHERE AnnualRevenue = '2700000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1600.0 WHERE AnnualRevenue = '1600                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 98000.0 WHERE AnnualRevenue = '98000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 675000.0 WHERE AnnualRevenue = '675000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 542550.0 WHERE AnnualRevenue = '542550                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 504420.0 WHERE AnnualRevenue = '504420.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3200000.0 WHERE AnnualRevenue = '3200000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1442500.0 WHERE AnnualRevenue = '1442500.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 34000.0 WHERE AnnualRevenue = '34000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 163803.0 WHERE AnnualRevenue = '163803                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2060.0 WHERE AnnualRevenue = '2060                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 900.0 WHERE AnnualRevenue = '900                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1000000.0 WHERE AnnualRevenue = '1000000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1239000.0 WHERE AnnualRevenue = '1239000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 550000.0 WHERE AnnualRevenue = '550000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 80000.0 WHERE AnnualRevenue = '80000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3000000.0 WHERE AnnualRevenue = '3000000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 614000.0 WHERE AnnualRevenue = '614000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1750.0 WHERE AnnualRevenue = '1750                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 24000.0 WHERE AnnualRevenue = '24000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1000.0 WHERE AnnualRevenue = '1000.00                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 263576.0 WHERE AnnualRevenue = '263576                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 9000.0 WHERE AnnualRevenue = '9000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 15.0 WHERE AnnualRevenue = '15                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 960000.0 WHERE AnnualRevenue = '960000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 14500000.0 WHERE AnnualRevenue = '14500000.00                                       '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 127000.0 WHERE AnnualRevenue = '127000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 150000.0 WHERE AnnualRevenue = '150000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 11000.0 WHERE AnnualRevenue = '11000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 103000.0 WHERE AnnualRevenue = '103000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 375000.0 WHERE AnnualRevenue = '375000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4000000.0 WHERE AnnualRevenue = '4000000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 21000.0 WHERE AnnualRevenue = '21000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 337789.0 WHERE AnnualRevenue = '337789                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4250000.0 WHERE AnnualRevenue = '4250000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 5000.0 WHERE AnnualRevenue = '5000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = -5000.0 WHERE AnnualRevenue = '-5000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2500.0 WHERE AnnualRevenue = '2500                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 46370.0 WHERE AnnualRevenue = '46370                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 80000.0 WHERE AnnualRevenue = '80000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 31060.0 WHERE AnnualRevenue = '31060                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 917000.0 WHERE AnnualRevenue = '917000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2000000.0 WHERE AnnualRevenue = '2000000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 10000.0 WHERE AnnualRevenue = '10000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = -10000.0 WHERE AnnualRevenue = '-10000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 115000.0 WHERE AnnualRevenue = '115000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2720000.0 WHERE AnnualRevenue = '2720000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2000000.0 WHERE AnnualRevenue = '2000000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 184000.0 WHERE AnnualRevenue = '184000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 60000.0 WHERE AnnualRevenue = '60000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 77000.0 WHERE AnnualRevenue = '77000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4100000.0 WHERE AnnualRevenue = '4100000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 33000.0 WHERE AnnualRevenue = '33000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4615.0 WHERE AnnualRevenue = '4615                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1200000.0 WHERE AnnualRevenue = '1200000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2332641.0 WHERE AnnualRevenue = '2332641                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 200035.0 WHERE AnnualRevenue = '200035                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1300000.0 WHERE AnnualRevenue = '1300000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 50000.0 WHERE AnnualRevenue = '50000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1100000.0 WHERE AnnualRevenue = '1100000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 45292.0 WHERE AnnualRevenue = '45292                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 25001.0 WHERE AnnualRevenue = '25001                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 49000.0 WHERE AnnualRevenue = '49000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3200000.0 WHERE AnnualRevenue = '3200000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 10296.0 WHERE AnnualRevenue = '10296                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3482000.0 WHERE AnnualRevenue = '3482000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 25000.0 WHERE AnnualRevenue = '25000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 12000000.0 WHERE AnnualRevenue = '12000000.0                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 65000.0 WHERE AnnualRevenue = '65000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 56.0 WHERE AnnualRevenue = '56                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 80969.0 WHERE AnnualRevenue = '80969                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 8300000.0 WHERE AnnualRevenue = '8300000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 17000.0 WHERE AnnualRevenue = '17000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 81.0 WHERE AnnualRevenue = '81                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3200.0 WHERE AnnualRevenue = '3200                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 340000.0 WHERE AnnualRevenue = '340000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 55000.0 WHERE AnnualRevenue = '55000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 250000.0 WHERE AnnualRevenue = '250000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 5500000.0 WHERE AnnualRevenue = '5500000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 380000.0 WHERE AnnualRevenue = '380000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 152000.0 WHERE AnnualRevenue = '152000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 15000.0 WHERE AnnualRevenue = '15000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 352148.0 WHERE AnnualRevenue = '352148                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 498000.0 WHERE AnnualRevenue = '498000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 90000.0 WHERE AnnualRevenue = '90000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 300.0 WHERE AnnualRevenue = '300                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 977300.0 WHERE AnnualRevenue = '977300                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 270000.0 WHERE AnnualRevenue = '270000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 92000.0 WHERE AnnualRevenue = '92000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3000.0 WHERE AnnualRevenue = '3000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3150.0 WHERE AnnualRevenue = '3150                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 60000.0 WHERE AnnualRevenue = '60000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 9164.0 WHERE AnnualRevenue = '9164                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 276000.0 WHERE AnnualRevenue = '276000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4139.0 WHERE AnnualRevenue = '4139                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 850000.0 WHERE AnnualRevenue = '850000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 165000.0 WHERE AnnualRevenue = '165000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 79000.0 WHERE AnnualRevenue = '79000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 295.0 WHERE AnnualRevenue = '295                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4400.0 WHERE AnnualRevenue = '4400.00                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3390.0 WHERE AnnualRevenue = '3390                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1500000.0 WHERE AnnualRevenue = '1500000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3520000.0 WHERE AnnualRevenue = '3520000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 300.0 WHERE AnnualRevenue = '300.00                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 9500.0 WHERE AnnualRevenue = '9500                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 28000.0 WHERE AnnualRevenue = '28000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 500.0 WHERE AnnualRevenue = '500                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 600000.0 WHERE AnnualRevenue = '600000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 43300000.0 WHERE AnnualRevenue = '43300000.00                                       '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 275000.0 WHERE AnnualRevenue = '275000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 125000.0 WHERE AnnualRevenue = '125000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 930000.0 WHERE AnnualRevenue = '930000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4640.25 WHERE AnnualRevenue = '4640.25                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 20000000.0 WHERE AnnualRevenue = '20000000                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 10300.0 WHERE AnnualRevenue = '10300.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 7800.0 WHERE AnnualRevenue = '7800                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 420000.0 WHERE AnnualRevenue = '420000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 23705.0 WHERE AnnualRevenue = '23705                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 58000.0 WHERE AnnualRevenue = '58000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1248020.0 WHERE AnnualRevenue = '1248020.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 14500000.0 WHERE AnnualRevenue = '14500000                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 32000.0 WHERE AnnualRevenue = '32000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 15000000.0 WHERE AnnualRevenue = '15000000.00                                       '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 5200000.0 WHERE AnnualRevenue = '5200000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 10000000.0 WHERE AnnualRevenue = '10000000                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2000.0 WHERE AnnualRevenue = '2000.00                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1200.0 WHERE AnnualRevenue = '1200                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 265000.0 WHERE AnnualRevenue = '265000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 27500.0 WHERE AnnualRevenue = '27500                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 40000.0 WHERE AnnualRevenue = '40000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 100.0 WHERE AnnualRevenue = '100                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 100000.0 WHERE AnnualRevenue = '100000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1200000.0 WHERE AnnualRevenue = '1200000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1250000.0 WHERE AnnualRevenue = '1250000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 10000000.0 WHERE AnnualRevenue = '10000000.0                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 7000.0 WHERE AnnualRevenue = '7000.00                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 163000.0 WHERE AnnualRevenue = '163000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = -190000.0 WHERE AnnualRevenue = '-190000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 54182.0 WHERE AnnualRevenue = '54182                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1000000.0 WHERE AnnualRevenue = '1000000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 132000.0 WHERE AnnualRevenue = '132000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 400000.0 WHERE AnnualRevenue = '400000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1000.0 WHERE AnnualRevenue = '1000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 750000.0 WHERE AnnualRevenue = '750000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 90.0 WHERE AnnualRevenue = '90                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 259000.0 WHERE AnnualRevenue = '259000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 110000.0 WHERE AnnualRevenue = '110000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2300000.0 WHERE AnnualRevenue = '2300000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 72860.0 WHERE AnnualRevenue = '72860                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 600.0 WHERE AnnualRevenue = '600.00                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4521570.0 WHERE AnnualRevenue = '4521570.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 166000.0 WHERE AnnualRevenue = '166000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 867000.0 WHERE AnnualRevenue = '867000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 11000000.0 WHERE AnnualRevenue = '11000000                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3000000.0 WHERE AnnualRevenue = '3000000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 218000.0 WHERE AnnualRevenue = '218000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 130000.0 WHERE AnnualRevenue = '130000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2000.0 WHERE AnnualRevenue = '2000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 10.0 WHERE AnnualRevenue = '10                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 26000.0 WHERE AnnualRevenue = '26000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 100000.0 WHERE AnnualRevenue = '100000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1450.0 WHERE AnnualRevenue = '1450.00                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 36000.0 WHERE AnnualRevenue = '36000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 292978.0 WHERE AnnualRevenue = '292978                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 200000.0 WHERE AnnualRevenue = '200000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 552252.0 WHERE AnnualRevenue = '552252                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1540000.0 WHERE AnnualRevenue = '1540000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 800.0 WHERE AnnualRevenue = '800                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 135000.0 WHERE AnnualRevenue = '135000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 100800.0 WHERE AnnualRevenue = '100800                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 44702.0 WHERE AnnualRevenue = '44702                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3500.0 WHERE AnnualRevenue = '3500                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 6000000.0 WHERE AnnualRevenue = '6000000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 395000.0 WHERE AnnualRevenue = '395000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = -395000.0 WHERE AnnualRevenue = '-395000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2000000.0 WHERE AnnualRevenue = '2000000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 35307.0 WHERE AnnualRevenue = '35307                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 35000.0 WHERE AnnualRevenue = '35000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 400.0 WHERE AnnualRevenue = '400.00                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1209000.0 WHERE AnnualRevenue = '1209000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 250.0 WHERE AnnualRevenue = '250                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 6109780.0 WHERE AnnualRevenue = '6109780.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 670000.0 WHERE AnnualRevenue = '670000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 750000.0 WHERE AnnualRevenue = '750000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 985000.0 WHERE AnnualRevenue = '985000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 50000.0 WHERE AnnualRevenue = '50000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2991800.0 WHERE AnnualRevenue = '2991800.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 63680.0 WHERE AnnualRevenue = '63680                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 7000.0 WHERE AnnualRevenue = '7000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 150000.0 WHERE AnnualRevenue = '150000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 381.0 WHERE AnnualRevenue = '381                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 800000.0 WHERE AnnualRevenue = '800000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 10350.0 WHERE AnnualRevenue = '10350                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 5800000.0 WHERE AnnualRevenue = '5800000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 175000.0 WHERE AnnualRevenue = '175000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 300000.0 WHERE AnnualRevenue = '300000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3680.0 WHERE AnnualRevenue = '3680                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 13000000.0 WHERE AnnualRevenue = '13000000                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1450000.0 WHERE AnnualRevenue = '1450000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3500000.0 WHERE AnnualRevenue = '3500000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 181942.0 WHERE AnnualRevenue = '181942                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 6200.0 WHERE AnnualRevenue = '6200                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1400000.0 WHERE AnnualRevenue = '1400000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 28000.0 WHERE AnnualRevenue = '28000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3000000.0 WHERE AnnualRevenue = '3000000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 950000.0 WHERE AnnualRevenue = '950000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 20.0 WHERE AnnualRevenue = '20                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 25.0 WHERE AnnualRevenue = '25.00                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2500000.0 WHERE AnnualRevenue = '2500000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 260000.0 WHERE AnnualRevenue = '260000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 12.0 WHERE AnnualRevenue = '12                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 76000.0 WHERE AnnualRevenue = '76000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 19000000.0 WHERE AnnualRevenue = '19000000.00                                       '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 178000.0 WHERE AnnualRevenue = '178000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 550000.0 WHERE AnnualRevenue = '550000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 99000.0 WHERE AnnualRevenue = '99000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 200.0 WHERE AnnualRevenue = '200                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 18661597.0 WHERE AnnualRevenue = '18661597                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 350000.0 WHERE AnnualRevenue = '350000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 225.0 WHERE AnnualRevenue = '225                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 201000.0 WHERE AnnualRevenue = '201000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 284715.0 WHERE AnnualRevenue = '284715                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1955000.0 WHERE AnnualRevenue = '1955000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 378.0 WHERE AnnualRevenue = '378                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 276094.0 WHERE AnnualRevenue = '276094                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 131000.0 WHERE AnnualRevenue = '131000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 12000000.0 WHERE AnnualRevenue = '12000000                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 510000.0 WHERE AnnualRevenue = '510000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 6000.0 WHERE AnnualRevenue = '6000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 21426.0 WHERE AnnualRevenue = '21426                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 33639.0 WHERE AnnualRevenue = '33639                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 860000.0 WHERE AnnualRevenue = '860000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 240000.0 WHERE AnnualRevenue = '240000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 6623.0 WHERE AnnualRevenue = '6623                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 7500.0 WHERE AnnualRevenue = '7500                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2621045.0 WHERE AnnualRevenue = '2621045                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 2700000.0 WHERE AnnualRevenue = '2700000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 28871.0 WHERE AnnualRevenue = '28871                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 120000.0 WHERE AnnualRevenue = '120000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 439000.0 WHERE AnnualRevenue = '439000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1610000.0 WHERE AnnualRevenue = '1610000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 120000.0 WHERE AnnualRevenue = '120000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 12000.0 WHERE AnnualRevenue = '12000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = -12000.0 WHERE AnnualRevenue = '-12000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 900000.0 WHERE AnnualRevenue = '900000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 52145.0 WHERE AnnualRevenue = '52145                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 61750.0 WHERE AnnualRevenue = '61750                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 5600000.0 WHERE AnnualRevenue = '5600000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 90000.0 WHERE AnnualRevenue = '90000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 11157.0 WHERE AnnualRevenue = '11157                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 22000.0 WHERE AnnualRevenue = '22000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 96000.0 WHERE AnnualRevenue = '96000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 37.0 WHERE AnnualRevenue = '37                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1000000.0 WHERE AnnualRevenue = '1000000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 8000000.0 WHERE AnnualRevenue = '8000000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 70000.0 WHERE AnnualRevenue = '70000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 850000.0 WHERE AnnualRevenue = '850000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1600.0 WHERE AnnualRevenue = '1600.00                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 15000.0 WHERE AnnualRevenue = '15000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 8.0 WHERE AnnualRevenue = '8                                                 '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 250000.0 WHERE AnnualRevenue = '250000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 20000.0 WHERE AnnualRevenue = '20000.00                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 25.0 WHERE AnnualRevenue = '25                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 326618.0 WHERE AnnualRevenue = '326618.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 310088.0 WHERE AnnualRevenue = '310088                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 430000.0 WHERE AnnualRevenue = '430000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 173000.0 WHERE AnnualRevenue = '173000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 30000.0 WHERE AnnualRevenue = '30000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 55.0 WHERE AnnualRevenue = '55                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 231363.0 WHERE AnnualRevenue = '231363                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 25000.0 WHERE AnnualRevenue = '25000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 850000.0 WHERE AnnualRevenue = '850000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 467000.0 WHERE AnnualRevenue = '467000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 279624.0 WHERE AnnualRevenue = '279624                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 177000.0 WHERE AnnualRevenue = '177000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 5162286.0 WHERE AnnualRevenue = '5162286                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 310000.0 WHERE AnnualRevenue = '310000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 695000.0 WHERE AnnualRevenue = '695000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 185000.0 WHERE AnnualRevenue = '185000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 7600.0 WHERE AnnualRevenue = '7600                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 38000.0 WHERE AnnualRevenue = '38000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 62510.0 WHERE AnnualRevenue = '62510                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 5100.0 WHERE AnnualRevenue = '5100                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 464000.0 WHERE AnnualRevenue = '464000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1500000.0 WHERE AnnualRevenue = '1500000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1300.0 WHERE AnnualRevenue = '1300                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 275000.0 WHERE AnnualRevenue = '275000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 20000.0 WHERE AnnualRevenue = '20000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 42000.0 WHERE AnnualRevenue = '42000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = -20000.0 WHERE AnnualRevenue = '-20000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 552000.0 WHERE AnnualRevenue = '552000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1.3 WHERE AnnualRevenue = '1.3                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 94000000.0 WHERE AnnualRevenue = '94000000                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1.0 WHERE AnnualRevenue = '1                                                 '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3000.0 WHERE AnnualRevenue = '3000.00                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 79333.0 WHERE AnnualRevenue = '79333                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 475000.0 WHERE AnnualRevenue = '475000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = -258.0 WHERE AnnualRevenue = '-258                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 235000.0 WHERE AnnualRevenue = '235000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 870000.0 WHERE AnnualRevenue = '870000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 400000.0 WHERE AnnualRevenue = '400000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 350000.0 WHERE AnnualRevenue = '350000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 50.0 WHERE AnnualRevenue = '50.00                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1254510.0 WHERE AnnualRevenue = '1254510.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 5400000.0 WHERE AnnualRevenue = '5400000.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = -125849.0 WHERE AnnualRevenue = '-125849                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 8000.0 WHERE AnnualRevenue = '8000                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 149.0 WHERE AnnualRevenue = '149                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 0.0 WHERE AnnualRevenue = '0                                                 '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 70000.0 WHERE AnnualRevenue = '70000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 22741361.0 WHERE AnnualRevenue = '22741361                                          '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 75000.0 WHERE AnnualRevenue = '75000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 320000.0 WHERE AnnualRevenue = '320000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 700000.0 WHERE AnnualRevenue = '700000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 610000.0 WHERE AnnualRevenue = '610000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 426000.0 WHERE AnnualRevenue = '426000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 80.0 WHERE AnnualRevenue = '80                                                '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1116490.0 WHERE AnnualRevenue = '1116490.0                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 750.0 WHERE AnnualRevenue = '750                                               '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 85000.0 WHERE AnnualRevenue = '85000                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 500000.0 WHERE AnnualRevenue = '500000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = -500000.0 WHERE AnnualRevenue = '-500000                                           '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 180000.0 WHERE AnnualRevenue = '180000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 130000.0 WHERE AnnualRevenue = '130000.00                                         '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 825000.0 WHERE AnnualRevenue = '825000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 450000.0 WHERE AnnualRevenue = '450000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 45465.0 WHERE AnnualRevenue = '45465                                             '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 308112.0 WHERE AnnualRevenue = '308112                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 200000.0 WHERE AnnualRevenue = '200000                                            '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 1500000.0 WHERE AnnualRevenue = '1500000.00                                        '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 4600.0 WHERE AnnualRevenue = '4600                                              '
UPDATE MDCReport.BAPQ.FactRICCompanyHoursRollUp SET AnnualRevenue = 3750000.0 WHERE AnnualRevenue = '3750000                                           '


