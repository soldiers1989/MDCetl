
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
