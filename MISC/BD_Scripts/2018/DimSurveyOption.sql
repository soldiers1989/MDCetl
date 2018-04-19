INSERT INTO MDCReport.BD.DimSurveyOption
SELECT O.id,
  O.question_id,
  O.title,
  O.value

FROM  MDCRaw.SURVEY.Question_Options O
INNER JOIN MDCRaw.SURVEY.Questions Q ON O.question_id = Q.id
WHERE Q.survey_id IN (50021327, 50021197)