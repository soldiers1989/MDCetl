SELECT DISTINCT FundingToDate
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE FundingToDate LIKE '%e%' OR FundingToDate LIKE '%n%'
      OR FundingToDate LIKE '%y%'  OR FundingToDate LIKE '%-%' OR FundingToDate LIKE ''

SELECT DISTINCT FundingCurrentQuarter
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE FundingCurrentQuarter LIKE '%to%'  OR FundingCurrentQuarter LIKE '%n%'
      OR FundingCurrentQuarter LIKE '%y%'  OR FundingCurrentQuarter LIKE '%to%'

SELECT DISTINCT AnnualRevenue
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE AnnualRevenue LIKE '%$%' OR AnnualRevenue LIKE '%n%' OR AnnualRevenue LIKE ''


SELECT DISTINCT NumberEmployees
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE NumberEmployees LIKE '%to%'
      OR NumberEmployees LIKE '%n%'
      OR NumberEmployees LIKE '%y%'
      OR NumberEmployees LIKE ''

SELECT DISTINCT AdvisoryServicesHours
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE AdvisoryServicesHours LIKE '%-%'
      OR AdvisoryServicesHours LIKE '%n%'
      OR AdvisoryServicesHours LIKE '%y%'
      OR AdvisoryServicesHours LIKE ''

SELECT DISTINCT VolunteerMentorHours
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE VolunteerMentorHours LIKE '%-%'
      OR VolunteerMentorHours LIKE '%n%'
      OR VolunteerMentorHours LIKE '%y%'
OR VolunteerMentorHours LIKE ''


SELECT DISTINCT AnnualRevenue
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE (AnnualRevenue LIKE '%-%'
      OR AnnualRevenue LIKE '%n%'
   OR AnnualRevenue LIKE ''
      OR AnnualRevenue LIKE '%y%') AND AnnualRevenue > 0

SELECT DISTINCT FiscalYear
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE FiscalYear LIKE '%-%'
      OR FiscalYear LIKE '%n%'
      OR FiscalYear LIKE '%y%'
      OR FiscalYear LIKE '%%'

SELECT DISTINCT FiscalQuarter
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE FiscalQuarter LIKE '%-%'
      OR FiscalQuarter LIKE '%n%'
      OR FiscalQuarter LIKE '%y%'
      OR FiscalQuarter LIKE '%%'
 OR FiscalQuarter LIKE ''

SELECT DISTINCT IntakeDate
FROM MDC_DEV.Reporting.FactRICCompanyData
WHERE IntakeDate LIKE '0000-00-00'
      OR IntakeDate LIKE '%n%'
      OR IntakeDate LIKE '%y%'
 OR IntakeDate LIKE '%-%'


SELECT COUNT(*)
FROM MDC_DEV.Reporting.FactRICCompanyData




----------------------------------------------------


INSERT INTO MDCDW.dbo.FactRICCompany
SELECT
  RICCompanyDataID   AS ID,
  CompanyID ,
  DataSourceID,
  BatchID ,
  DateID,
  NULL, --Convert(varchar(30),IntakeDate,103),
  AdvisoryServicesHours ,
  VolunteerMentorHours,
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
  CONVERT(int,RIGHT(FiscalQuarter,1)),
  CONVERT(int, FiscalYear),
  CreateDate,
  ModifiedDate
FROM MDC_DEV.Reporting.FactRICCompanyData





