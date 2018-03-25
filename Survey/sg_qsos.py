import pandas as pd
import requests
import json
import urllib.request as rq
import urllib
from time import sleep

class sg_qsos:

    @classmethod
    def sg_create_questionsURL(self, surveyID, api_token):
        '''Takes str id of SurveyGizmo survey, returns str URL
        for Questions SubObject
        str -> str'''

        base = "https://restapica.surveygizmo.com/v5/survey/"
        obj = "/surveyquestion/"
        URL = base + str(surveyID) + obj + "?" + api_token
        return URL

    @classmethod
    def sg_get_api_output(self, URL, attempts=10, wait_sec=3):
        '''Takes (in future) tokens, preferences, returns JSON file if successful
        after specified # of attempts.
        ints -> dict
        '''
        from time import sleep
        attempt_count = 0

        for i in range(0, attempts):
            try:
                attempt_count += 1
                output = requests.get(URL)
                if output.ok:
                    output = output.json()
                    print("Success. Stored API output in json dict.")
                    return output
            except KeyboardInterrupt:
                pass
            except:
                if attempt_count >= attempts:
                    print("All attempts failed")
                    return
                print("Likely SSLError. Trying again in", wait_sec, "second(s)...")
                sleep(wait_sec)

    @classmethod
    def remove_HTML(self, text):
        '''Removes common HTML tags from text.
        str -> str
        '''
        text = text.replace("<strong>", "")\
            .replace("</strong>", "")\
            .replace("<span>", "")\
            .replace("</span>", "")\
            .replace("\xa0", " ")\
            .replace("<b>", "")\
            .replace("</b>", "")\
            .replace("<br />", "")\
            .replace("&amp;", "&")\
            .replace("\n ", "")\
            .replace("</a>", "")\
            .replace("<a class=\"tip\" href=\"#\">", " ")\
            .replace("<div style=\"text-align: justify;\">", "")\
            .replace("</div>", "")\
            .replace("<span style=\"font-size:12pt;\">", "")\
            .strip()
        return text

    @classmethod
    def qs_to_dataframe(self, surveyID, questions):
        '''Takes SurveyGizmo-formatted Questions object and extracts to
        list of lists.
        dict -> dataframe
        '''

        surveyID = int(surveyID)
        qs = []
        sub_qs = []
        qids = []
        subqids = []
        subqid_qid = {}
        headers = ["id", "survey_id", "title", "base_type", "type", "subtype", \
                   "comment", "description", "required", "soft_required", \
                   "hidden", "piped_from", "question_description", \
                   "force_numeric", "force_percent", "force_currency", \
                   "min_number", "max_number", "only_whole_num", \
                   "defaulttext", "parent_id", "has_sub_questions", \
                   "has_options", "shortname"]
        parent_id = "NULL"
        # get questions

        ##gather question and sub-question ids
        for data in questions["data"]:
            if data["base_type"] == "Question" or data["type"] == "HIDDEN":
                qid = int(str(surveyID) + str(data["id"]))
                if qid not in qids:
                    qids.append(qid)
            try:
                if (data["base_type"] == "Question" and data["type"] == "GROUP") or len(data["sub_questions"]) > 0:
                    if type(data["sub_questions"]) == dict:
                        subq_dict = data["sub_questions"]
                        for subq in subq_dict:
                            if subq_dict[subq]["base_type"] == "Question":
                                subqid = int(str(surveyID) + str(subq_dict[subq]["id"]))
                                subqids.append(subqid)
                                subqid_qid[str(subqid)] = qid
                    else:
                        for subq in data["sub_questions"]:
                            if subq["base_type"] == "Question":
                                subqid = int(str(surveyID) + str(subq["id"]))
                                subqids.append(subqid)
                                subqid_qid[str(subqid)] = qid
            except KeyError:
                continue

        # get questions
        for data in questions["data"]:
            if data["base_type"] == "Question" or data['base_type'] == "Action":
                qid = int(str(surveyID) + str(data["id"]))
                try:
                    if data["options"] != []:
                        options = 1
                    else:
                        options = 0
                except KeyError:
                    pass

                clean_q = self.remove_HTML(data["title"]["English"])
                if data["type"] == "GROUP":
                    clean_q = "ParentQuestion : " + clean_q
                if clean_q == "":
                    clean_q = "NULL"
                if qid in subqids:
                    clean_q = "Sub-question : " + clean_q

                q_type = data["type"]
                if q_type == "":
                    q_type = "NULL"

                try:
                    if data["properties"]["force_numeric"] == True:
                        d_type = "INT"
                except KeyError:
                    d_type = "VARCHAR"
                if options == "has options" and d_type != "INT":
                    d_type = "NULL"

                if "Sub-question : " in clean_q:
                    parent_id = subqid_qid[str(int(str(surveyID) + str(data["id"])))]
                    #             try:
                base_type = data["base_type"]
                comment = data["comment"]
                descr = "NULL"  # needs further investigation
                req = int(data["properties"]["required"])
                try:
                    soft_req = int(data["properties"]["soft-required"])
                except KeyError:
                    soft_req = 0
                    pass
                hidden = int(data["properties"]["hidden"])
                piped_from = "NULL"  # implement later
                try:
                    q_descr = data["properties"]["question_description"]["English"]
                except KeyError:
                    q_descr = "NULL"
                    pass
                subtype = "NULL"
                force_num = 0
                force_pct = 0
                force_c = 0
                min_num = "NULL"
                max_num = "NULL"
                only_whole = 0
                try:
                    force_num = int(data["properties"]["force_numeric"])
                except KeyError:
                    pass
                try:
                    force_pct = int(data["properties"]["force_percent"])
                except KeyError:
                    pass
                try:
                    force_c = int(data["properties"]["force_currency"])
                except KeyError:
                    pass
                try:
                    min_num = data["properties"]["min_number"]
                except KeyError:
                    pass
                try:
                    max_num = data["properties"]["max_number"]
                except KeyError:
                    pass
                try:
                    only_whole = int(data["properties"]["only_whole_num"])
                except KeyError:
                    pass
                defaulttext = "NULL"  # needs further investigation
                if data["type"] == "GROUP":
                    has_sub_q = 1
                else:
                    has_sub_q = 0
                try:
                    subtype = data["properties"]["subtype"]
                except KeyError:
                    pass
                try:
                    shortname = data["shortname"]
                except KeyError:
                    pass
                qs.append([qid, surveyID, clean_q, base_type, \
                           q_type, subtype, comment, descr, req, \
                           soft_req, hidden, piped_from, q_descr, \
                           force_num, force_pct, force_c, min_num, \
                           max_num, only_whole, defaulttext, \
                           parent_id, has_sub_q, options, shortname])
                parent_id = "NULL"

        final_df = pd.DataFrame(qs, columns=headers)
        return final_df

    @classmethod
    def sg_options_to_dataframe(self, surveyID, questions):
        '''Takes SurveyGizmo-formatted questions and returns options, where applicable.
        dict -> dataframe
        '''

        opts = []
        scale_range = "NULL"
        category = "NULL"
        headers = ["id", "question_id", "title", "value", "data_type", "option_type", "scale_type",
                   "scale_range"]
        for question in questions["data"]:
            if question["base_type"] == "Question":
                try:
                    if question["options"] != []:
                        for option in question["options"]:
                            question_id = int(str(surveyID) + str(question["id"]))
                            try:
                                x = int(option['id'])
                            except ValueError:
                                continue
                            oid = int(str(question_id) + str(option["id"]))
                            opt_name = self.remove_HTML(option["title"]["English"])
                            opt_val = self.remove_HTML(option["value"])
                            opt_data_type = "NULL"
                            opt_type = question["type"]
                            scale_type = "NULL"
                            values = []
                            if question["type"] == "NPS":
                                for scale_value in question["options"]:
                                    values.append(int(scale_value["value"]))
                                scale_range = str(min(values)) + "-" + str(max(values))
                            opts.append(
                                [oid, question_id, opt_name, opt_val, opt_data_type, opt_type, scale_type,
                                 scale_range])
                except KeyError:
                    print("KeyError")
                    continue

        final_df = pd.DataFrame(opts, columns=headers)
        return final_df

    @classmethod
    def sg_get_qs_os(self, surveyID, api_token):

        surveyID = str(surveyID)
        URL = self.sg_create_questionsURL(surveyID, api_token)
        jsons = self.sg_get_api_output(URL)
        qs = self.qs_to_dataframe(surveyID, jsons)
        os = self.sg_options_to_dataframe(surveyID, jsons)
        return qs, os