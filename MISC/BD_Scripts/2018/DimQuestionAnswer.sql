INSERT INTO MDCReport.BD.DimSurveyQuestion
SELECT
  base_type,
  type,
  id,
  survey_id,
  parent_id,
  title,
  2018 as SY
FROM MDCRaw.SURVEY.Questions WHERE survey_id IN (50021327, 50021197)