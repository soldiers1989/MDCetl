INSERT INTO MDCReport.BD.FactSurveyAnswer
SELECT DISTINCT
   QA.AnswerID,
  NULL AS DataSource,
  RA.Company_ID,
   QA.QuestionID,
  QA.OptionID,
  QA.Answer,
  NULL AS Rank,
  2018 AS SurveyYear
    FROM (SELECT DISTINCT
        V.resp_id,
        CAST(V.answer AS INT) Company_ID,
        A.answer              [RIC_Program]
      FROM SURVEY.Questions Q INNER JOIN SURVEY.Question_Answers A ON Q.id = A.question_id
        INNER JOIN SURVEY.Survey_Responses R ON R.id = A.survey_response_id
        RIGHT JOIN (SELECT
                      R.id resp_id,
                      A.answer
                    FROM SURVEY.Question_Answers A INNER JOIN SURVEY.Survey_Responses R ON A.survey_response_id = R.id
                      INNER JOIN SURVEY.Questions Q ON A.question_id = Q.id
                    WHERE Q.id = 50021327348 AND R.survey_id IN (50021327, 50021197)) V ON V.resp_id = R.id
      WHERE Q.id IN
            (50021327301, 50021327302, 50021327305, 50021327306, 50021327307, 50021327308, 50021327309, 50021327310, 50021327311, 50021327312,
            50021327313, 50021327314, 50021327315, 50021327316, 50021327317, 50021327318, 50021327319, 50021327329, 50021327330)) RA LEFT JOIN
  (SELECT DISTINCT
     V.Resp_id,
     V.answer        Company_ID,
     A.id            AnswerID,
     Q.id            QuestionID,
     Q.title         Question,
     Q.shortname,
     Q.has_options,
     Q.[type]        QuestionType,
     OV.oid           OptionID,
     OV.title        [OptionLabel],
     OV.[value]      OptionValue,
     A.Answer,
     Q.parent_id,
     Q.Question_Type [Core/Non-core],
     Q.[RIC_Program],
     Q.[Cap/Rev/Emp]
   FROM (SELECT
           R.id resp_id,
           A.answer
         FROM SURVEY.Question_Answers A INNER JOIN SURVEY.Survey_Responses R ON A.survey_response_id = R.id
           INNER JOIN SURVEY.Questions Q ON A.question_id = Q.id
         WHERE Q.id = 50021327348 AND R.survey_id IN (50021327, 50021197)) V LEFT JOIN SURVEY.Survey_Responses R
       ON V.resp_id = r.id
     INNER JOIN SURVEY.Question_Answers A ON R.id = A.survey_response_id
     FULL OUTER JOIN SURVEY.Ann_Survey_2018_Qs_Metadata Q ON A.question_id = Q.id
     LEFT JOIN (SELECT
                  Q.id qid,
                  O.id oid,
                  O.[value],
                  O.title,
                  O.data_type,
                  O.option_type,
                  O.scale_range,
                  O.scale_type
                FROM SURVEY.Questions Q INNER JOIN SURVEY.Question_Options O ON Q.id = O.question_id
                WHERE Q.survey_id IN (50021327, 50021197)) OV
       ON Q.id = OV.qid AND A.answer = OV.[value] OR A.option_id = OV.oid
   WHERE Q.[type] NOT IN ('LOGIC', 'HIDDEN', 'JAVASCRIPT') AND resp_id IS NOT NULL) QA ON RA.resp_id = QA.resp_id
