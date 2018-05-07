--INSERT INTO MDCDW.dbo.FactRICCompany
SELECT
  CompanyID ,
  DataSourceID,
  BatchID ,
  DateID,
  CAST(IntakeDate AS date) AS Intake,--SUBSTRING(Convert(varchar,IntakeDate,103),1,11) AS IntakeDate,
  CAST(AdvisoryServicesHours AS Decimal(18,4)) AS [Advisory],
  CAST(VolunteerMentorHours AS Decimal(18,4)) AS [Volunteer],
  AnnualRevenue,
  NumberEmployees,
  FundingToDate,
  FundingCurrentQuarter ,
  CASE
  WHEN Stage LIKE '%Idea%' OR Stage LIKE '%0%'
    THEN 1
  WHEN Stage LIKE '%Discovery%' OR Stage LIKE '%1%'
    THEN 2
  WHEN Stage LIKE '%Validation%' OR Stage LIKE '%2%'
    THEN 3
  WHEN Stage LIKE '%Efficiency%' OR Stage LIKE '%3%'
    THEN 4
  WHEN Stage LIKE '%scale%' OR Stage LIKE '%4%'
    THEN 5
  WHEN Stage IS NULL OR Stage = '0' OR Stage = ''
    THEN 6
  ELSE NULL END                            AS [Stage],
  CASE
  WHEN IndustrySector LIKE '%Advanced Manufacturing%'
       OR IndustrySector LIKE '%Adv. Materials%'
       OR IndustrySector LIKE '%materials%'
       OR IndustrySector LIKE '%Manufactur%'
    THEN 1
  WHEN IndustrySector LIKE '%agricult%'
       OR IndustrySector LIKE '%agro%'
    THEN 2
  WHEN IndustrySector LIKE '%Clean%Tech%'
       OR IndustrySector LIKE '%energy%'
       OR IndustrySector LIKE '%recycl%'
       OR IndustrySector LIKE '%water%'
       OR IndustrySector LIKE '%green%energy%'
    THEN 3
  WHEN IndustrySector LIKE '%ICT%'
       OR IndustrySector LIKE '%Digital%Media%'
       OR IndustrySector LIKE '%app%'
       OR IndustrySector LIKE '%entertainment%'
       OR IndustrySector LIKE '%hardware%'
       OR IndustrySector LIKE '%software%'
    THEN 4
  WHEN IndustrySector LIKE '%Education%'
    THEN 5
  WHEN IndustrySector LIKE '%Financial%'
    THEN 6
  WHEN IndustrySector LIKE '%Food%' OR IndustrySector LIKE '%Beverage%'
    THEN 7
  WHEN IndustrySector LIKE '%Forestry%'
    THEN 8
  WHEN IndustrySector LIKE '%Life%Science%'
       OR IndustrySector LIKE '%health%'
       OR IndustrySector LIKE '%wellness%'
       OR IndustrySector LIKE '%medical%'
       OR IndustrySector LIKE '%pharma%'
    THEN 9
  WHEN IndustrySector LIKE '%Mining%'
    THEN 10
  WHEN IndustrySector LIKE '%Other%'
    THEN 11
  WHEN IndustrySector LIKE '%Tourism%' OR IndustrySector LIKE '%culture%'
    THEN 12
  WHEN IndustrySector IS NULL
    THEN NULL
  ELSE 11 END                              AS IndustrySector,
  CASE
  WHEN Youth IN ('0', 'n', 'No', '')
    THEN 0
  WHEN Youth IN ('1', 'y', 'Yes')
    THEN 1
  ELSE NULL END                            AS [Youth],
  CASE
  WHEN HighPotential IN ('0', 'n', 'No', '')
    THEN 0
  WHEN HighPotential IN ('1', 'y', 'Yes', 'High')
    THEN 1
  ELSE NULL END                            AS [HighPotential],
  CASE
  WHEN SocialEnterprise IN ('N', 'No', '')
    THEN 0
  WHEN SocialEnterprise IN ('Y', 'Yes')
    THEN 1
  ELSE NULL END                            AS [SocialEnterprise],
  CONVERT(int,RIGHT(FiscalQuarter,1)) AS [Fiscal Quarter],
  CONVERT(int, FiscalYear) AS [Fiscal Year],
  CreateDate,
  ModifiedDate
FROM MDC_DEV.Reporting.FactRICCompanyData

-- --
-- SELECT F.IntakeDate, CAST(F.IntakeDate AS date) AS [Proper]
-- FROM MaRSDataCatalyst.Reporting.FactRICCompanyData F WHERE F.IntakeDate LIKE '%Ju%'


---------------------------------------------------------------------------------------------------------------------

INSERT INTO MDCDW.dbo.FactRICCompany
SELECT
  CompanyID ,
  DataSourceID,
  BatchID ,
  DateID,
  NULL AS IntakeDate,-- CAST(IntakeDate AS date) AS Intake,--SUBSTRING(Convert(varchar,IntakeDate,103),1,11) AS IntakeDate,
NULL,--   CASE WHEN AdvisoryServicesHours = '' THEN NULL ELSE CAST(AdvisoryServicesHours AS Decimal(18,4)) END AS [Advisory],
NULL,--   CASE WHEN VolunteerMentorHours = '' THEN  NULL ELSE CAST(VolunteerMentorHours AS Decimal(18,4)) END AS [Volunteer],
NULL,--   CASE WHEN AnnualRevenue = '' THEN NULL ELSE CAST(AnnualRevenue AS Decimal(18,4)) END AS [Revenue],
NULL,--   CASE WHEN NumberEmployees = '' THEN NULL ELSE CAST(NumberEmployees AS Int) END AS [Emps],
NULL,--   CASE WHEN FundingToDate = '' THEN NULL ELSE CAST(FundingToDate AS Decimal(18,4)) END AS [FundingTD],
NULL,--   CASE WHEN FundingCurrentQuarter = '' THEN NULL ELSE CAST(FundingCurrentQuarter AS Decimal(18,4)) END AS [FundingTQ],
  CASE
  WHEN Stage LIKE '%Idea%' OR Stage LIKE '%0%'
    THEN 1
  WHEN Stage LIKE '%Discovery%' OR Stage LIKE '%1%'
    THEN 2
  WHEN Stage LIKE '%Validation%' OR Stage LIKE '%2%'
    THEN 3
  WHEN Stage LIKE '%Efficiency%' OR Stage LIKE '%3%'
    THEN 4
  WHEN Stage LIKE '%scale%' OR Stage LIKE '%4%'
    THEN 5
  WHEN Stage IS NULL OR Stage = '0' OR Stage = ''
    THEN 6
  ELSE NULL END                            AS [Stage],
  CASE
  WHEN IndustrySector LIKE '%Advanced Manufacturing%'
       OR IndustrySector LIKE '%Adv. Materials%'
       OR IndustrySector LIKE '%materials%'
       OR IndustrySector LIKE '%Manufactur%'
    THEN 1
  WHEN IndustrySector LIKE '%agricult%'
       OR IndustrySector LIKE '%agro%'
    THEN 2
  WHEN IndustrySector LIKE '%Clean%Tech%'
       OR IndustrySector LIKE '%energy%'
       OR IndustrySector LIKE '%recycl%'
       OR IndustrySector LIKE '%water%'
       OR IndustrySector LIKE '%green%energy%'
    THEN 3
  WHEN IndustrySector LIKE '%ICT%'
       OR IndustrySector LIKE '%Digital%Media%'
       OR IndustrySector LIKE '%app%'
       OR IndustrySector LIKE '%entertainment%'
       OR IndustrySector LIKE '%hardware%'
       OR IndustrySector LIKE '%software%'
    THEN 4
  WHEN IndustrySector LIKE '%Education%'
    THEN 5
  WHEN IndustrySector LIKE '%Financial%'
    THEN 6
  WHEN IndustrySector LIKE '%Food%' OR IndustrySector LIKE '%Beverage%'
    THEN 7
  WHEN IndustrySector LIKE '%Forestry%'
    THEN 8
  WHEN IndustrySector LIKE '%Life%Science%'
       OR IndustrySector LIKE '%health%'
       OR IndustrySector LIKE '%wellness%'
       OR IndustrySector LIKE '%medical%'
       OR IndustrySector LIKE '%pharma%'
    THEN 9
  WHEN IndustrySector LIKE '%Mining%'
    THEN 10
  WHEN IndustrySector LIKE '%Other%'
    THEN 11
  WHEN IndustrySector LIKE '%Tourism%' OR IndustrySector LIKE '%culture%'
    THEN 12
  WHEN IndustrySector IS NULL
    THEN NULL
  ELSE 11 END                              AS IndustrySector,
  CASE
  WHEN Youth IN ('0', 'n', 'No', '')
    THEN 0
  WHEN Youth IN ('1', 'y', 'Yes')
    THEN 1
  ELSE NULL END                            AS [Youth],
  CASE
  WHEN HighPotential IN ('0', 'n', 'No', '')
    THEN 0
  WHEN HighPotential IN ('1', 'y', 'Yes', 'High')
    THEN 1
  ELSE NULL END                            AS [HighPotential],
  CASE
  WHEN SocialEnterprise IN ('N', 'No', '')
    THEN 0
  WHEN SocialEnterprise IN ('Y', 'Yes')
    THEN 1
  ELSE NULL END                            AS [SocialEnterprise],
  CONVERT(int,RIGHT(FiscalQuarter,1)) AS [Fiscal Quarter],
  CONVERT(int, FiscalYear) AS [Fiscal Year],
  CreateDate,
  ModifiedDate
FROM MDC_DEV.Reporting.FactRICCompanyData
