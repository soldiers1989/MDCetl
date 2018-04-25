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