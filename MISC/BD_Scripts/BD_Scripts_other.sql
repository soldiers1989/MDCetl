
/*
Script for vAddedByRIC
*/
---------------------------------------------------------------------------------------

--INSERT INTO MDCReport.dbo.FactSurveyResponse
    SELECT V.DataSourceId,V.CompanyId,S.ID, V.Answer,V.SurveyYear
    FROM MaRSDataCatalyst.RICSurveyFlat.vAddedByRIC V INNER JOIN
    MDCReport.dbo.DimSurveyQuestion S ON V.CleanQuestion = S.Question

---------------------------------------------------------------------------------------
/*
Script for vThreeMostImportant_viz
*/

--INSERT INTO MDCReport.dbo.FactSurveyResponse
SELECT DataSourceId, CompanyId, 87, Response, Ranking, SurveyYear --87 is question ID
FROM MaRSDataCatalyst.RICSurveyFlat.vThreeMostImportant_viz

------------------------------VENTURE DE-DUP PROCESS------------------------------------

SELECT * FROM Venture WHERE BasicName IN (
  SELECT BasicName FROM Venture
GROUP BY BasicName
HAVING Count(BasicName) > 1
)
ORDER BY 3