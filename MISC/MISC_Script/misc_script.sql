EXEC Config.DedupeCompanyIDCol  'MDCRaw.dbo.Venture', 'ID'
EXEC Config.DedupeCompanyIDCol  'MDCRaw.IAF.IAFDetail', 'CompanyID'
EXEC Config.DedupeCompanyIDCol  'MDCRaw.IAF.IAFSummary', 'CompanyID'
EXEC Config.DedupeCompanyIDCol  'MDCRaw.CVCA.VCPEDeals', 'CompanyID'
EXEC Config.DedupeCompanyIDCol  'MDCRaw.CVCA.Exits', 'CompanyID'
EXEC Config.DedupeCompanyIDCol  'MDCRaw.CRUNCHBASE.Organization', 'company_id'
EXEC Config.DedupeCompanyIDCol  'MDCRaw.SURVEY.Targetlist', 'CompanyID'
EXEC Config.DedupeCompanyIDCol  'MDCRaw.SURVEY.Question_Answers', 'answer', 50021327348
EXEC Config.DedupeCompanyIDCol  'MDCReport.BD.AnnualSurveyResult', 'Company_ID'
EXEC Config.DedupeCompanyIDCol  'MDCReport.BD.ResponseStatus', 'venture_id'

ALTER DATABASE MDCDim
SET MULTI_USER
WITH NO_WAIT;
GO

SELECT V.Name, V.BasicName,Q.CompanyName, Q.BasicName, Q.CompanyID, Q.Quarter, Q.Year
-- UPDATE Q
--     SET Q.CompanyID = V.ID
FROM MaRSDataCatalyst.BAp.QuarterlyCompanyData Q INNER JOIN
  MDCRaw.dbo.Venture V ON V.BasicName COLLATE DATABASE_DEFAULT = Q.BasicName COLLATE DATABASE_DEFAULT
WHERE  Q.Year = 2018 AND Q.Quarter = 'Q4' AND CompanyID = 0


SELECT V.ID, T.CompanyID,V.Duplicate, V.Name, T.Venture_name
FROM Venture V INNER JOIN MDCRaw.SURVEY.Targetlist T
  ON V.ID = T.CompanyID WHERE V.Duplicate IS NOT NULL


SELECT V.ID, T.CompanyID,V.Duplicate, V.Name, T.Venture_name
FROM Venture V INNER JOIN MDCRaw.SURVEY.Targetlist T
  ON V.Duplicate = T.CompanyID WHERE V.Duplicate IS NOT NULL
AND T.Status = 1


SELECT CompanyID, Venture_name FROM SURVEY.Targetlist WHERE CompanyID IN (
  SELECT V.ID
FROM Venture V INNER JOIN MDCRaw.SURVEY.Targetlist T
  ON V.Duplicate = T.CompanyID WHERE V.Duplicate IS NOT NULL
)


SELECT ID,Name, BasicName
FROM Venture
WHERE BasicName IN (SELECT BasicName FROM Venture GROUP BY BasicName HAVING Count(BasicName) > 1)
      AND BasicName <> '' ORDER BY 2

SELECT TOP 10 * FROM MDCRaw.dbo.Venture
SELECT TOP 10 * FROM MDCRaw.CONFIG.DuplicateVenture

SELECT V.ID, D.CompanyID,V.Duplicate, D.DuplicateCompanyID FROM MDCRaw.dbo.Venture V
  INNER JOIN MDCRaw.CONFIG.DuplicateVenture D ON V.ID = D.CompanyID
WHERE D.Verified = 1


SELECT * FROM Venture WHERE BasicName IN (
  SELECT BasicName FROM Venture
GROUP BY BasicName
HAVING Count(BasicName) > 1
)
ORDER BY 3

SELECT BasicName, Count(BasicName) FROM Venture
GROUP BY BasicName
HAVING Count(BasicName) > 1

SELECT ID,Name, BasicName FROM Venture WHERE BasicName IN (
  SELECT BasicName FROM Venture
GROUP BY BasicName
HAVING Count(BasicName) > 1
)
ORDER BY 3

-- -----------------------------------------------------------------------------------

SELECT * FROM CONFIG.DuplicateVenture WHERE DuplicateCompanyID = 299614 OR CompanyID = 299614
SELECT * FROM Venture WHERE  ID = 299614
SELECT * FROM MaRSDataCatalyst.Reporting.DimCompany WHERE CompanyID = 299614
SELECT * FROM Venture WHERE Name LIKE 'Signority%'

SELECT CompanyID, Venture_name,Venture_basic_name
FROM SURVEY.Targetlist WHERE CompanyID IN (
 166743 ,94461 ,162243 ,201270 ,94312 ,61080 ,179855 ,153646 ,107465 ,215554 ,299614)

SELECT ID, Name,BasicName
FROM dbo.Venture WHERE ID IN (
 166743 ,94461 ,162243 ,201270 ,94312 ,61080 ,179855 ,153646 ,107465 ,215554 ,299614)


SELECT * FROM dbo.Venture WHERE ID IN (166743,3518,94461,7425,299614,1129)

-- 3518-166743-Signority AKA Green Signatures,signorityakagreensignatures
-- 7425-94461-Career Pages INC,careerpagesinc
-- 1129-299614-WipWare Incorporated,wipwarerporated