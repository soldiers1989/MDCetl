import pandas as pd
import requests
import json
import urllib.request as rq
import urllib
from time import sleep
from Shared.common import Common as common


class sg_responses:

    @classmethod
    def create_response_API_URL(self, surveyID, api_token, resultsperpage=500, page=""):
        base = "https://restapica.surveygizmo.com/v5/survey/"
        obj = "/surveyresponse/"
        if page != "":
            page = "page=" + str(page)
        resultsperpage = "resultsperpage=" + str(resultsperpage)
        URL = base + str(surveyID) + obj + "?" + page + "&" + resultsperpage + "&" + api_token
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
                output = requests.get(URL, verify=common.get_cert_path())
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
    def sg_get_api_output_answers(self, attempts, wait_sec, surveyID, api_token):
        '''Takes (in future) tokens, preferences, returns JSON file if successful
        after specified # of attempts.
        ints -> dict
        '''
        from time import sleep

        attempt_count = 0
        page_cnt = 1
        which_page = 1
        result_pages = []
        URL = sg_responses.create_response_API_URL(surveyID, api_token, resultsperpage=200, page=which_page)
        for i in range(0, attempts):
            try:

                output = requests.get(URL, verify=common.get_cert_path())
                if output.ok:
                    output = output.json()
                    result_pages.append(output)
                    print("Success. Stored API output in json dict.")
                    resultsperpage = int(output["results_per_page"])
                    totalresults = int(output["total_count"])
                    if totalresults > resultsperpage:
                        print("total exceeds results_per_page")
                    page_cnt = output["total_pages"]
                    print("Final page count will be", page_cnt)
                    if page_cnt > 1:
                        for i in range(which_page, page_cnt):
                            print("Making call to API for another page")
                            which_page += 1
                            URL = sg_responses.create_response_API_URL(surveyID, api_token, resultsperpage=200, page=which_page)
                            resultpage = sg_responses.sg_get_api_output(URL, 10, 3)
                            result_pages.append(resultpage)

                    print("Success. All results stored in dict(s).")
                    if len(result_pages) == 1:
                        print("Output: Single dict")
                        return output
                    elif len(result_pages) > 1:
                        print("Output: List of dicts")
                        return result_pages
            except KeyboardInterrupt:
                pass
            except:
                attempt_count += 1
                if attempt_count >= attempts:
                    print("All attempts failed")
                    return
                print("Likely SSLError. Trying again in", wait_sec, "second(s)...")
                sleep(wait_sec)

    @classmethod
    def sg_get_responses(self, surveyID, api_token, dfs=0):
        '''Gets response record attribute data (date started, status, etc.) and puts into df.
        str -> dataframe
        '''
        headers = ["id", "contact_id", "survey_id", "date_started", "date_submitted", "resp_status", "is_test_data",
                   "referer", "language", "ip_address", "longitude", "latitude", "country", "city", "region", "postal"
                   ]
        if dfs == 0:
            dfs = sg_responses.sg_get_api_output_answers(10, 3, surveyID, api_token)
        print("got api output. Is type:", type(dfs), "length", len(dfs))
        if type(dfs) == list:
            print("test: list 1 same as list 2?", str(dfs[0] == dfs[1]))
        records = []
        sgid = 0
        survey_id = surveyID
        if type(dfs) == list:
            print("multiple pages will be added to final_dataframe")
            for resultspage in dfs:
                for response in resultspage["data"]:
                    sgid += 1
                    contact_id = -1
                    try:
                        sg_response_id = int(str(surveyID) + str(response["id"]))
                        if response["contact_id"] != '':
                            contact_id = response["contact_id"]
                        date_started = response["date_started"][:-4]  # removes ' GMT'
                        date_submitted = response["date_submitted"][:-4]
                        status = response["status"]
                        is_test_data = response["is_test_data"]
                        referer = response["referer"]
                        language = response["language"]
                        ip_address = response["ip_address"]
                        long = response["longitude"]
                        lat = response["latitude"]
                        country = response["country"]
                        city = response["city"]
                        region = response["region"]
                        postal = response["postal"]
                        records.append([sg_response_id, contact_id, survey_id,
                                        date_started, date_submitted, status, is_test_data,
                                        referer, language, ip_address, long, lat, country,
                                        city, region, postal])

                    except KeyError:
                        print("KeyError")
                    except:
                        print("Some other error")
        elif type(dfs) != list:
            for response in dfs["data"]:
                sgid += 1
                contact_id = -1
                try:
                    sg_response_id = int(str(surveyID) + str(response["id"]))
                    if response["contact_id"] != '':
                        contact_id = response["contact_id"]
                    date_started = response["date_started"][:-4]
                    date_submitted = response["date_submitted"][:-4]
                    status = response["status"]
                    is_test_data = response["is_test_data"]
                    referer = response["referer"]
                    language = response["language"]
                    ip_address = response["ip_address"]
                    long = response["longitude"]
                    lat = response["latitude"]
                    country = response["country"]
                    city = response["city"]

                    region = response["region"]
                    postal = response["postal"]
                    records.append([sg_response_id, contact_id, survey_id,
                                    date_started, date_submitted, status, is_test_data,
                                    referer, language, ip_address, long, lat, country,
                                    city, region, postal])

                except KeyError:
                    print("KeyError")
        final_df = pd.DataFrame(records, columns=headers)
        return final_df

    @classmethod
    def answers_to_dataframe(self, responses, surveyID):
        '''Takes dict that contains SurveyGizmo responses and returns a dataframe.
        dict/list of dicts -> dataframe
        '''

        rid = 0
        oid = "NULL"
        resps = []
        headers = ["id", "question_id", "option_id", "survey_response_id", "answer"]
        bad_qs = []

        for resp in responses["data"]:
            survey_resp_id = int(str(surveyID) + str(resp["id"]))
            for key in resp["survey_data"]:

                try:
                    # get answers to regular questions without options
                    if resp["survey_data"][key]["type"] != "parent":
                        sg_qid = str(resp["survey_data"][key]["id"])
                        if len(sg_qid) == 1:  # add leading zero to 1-digit question_id
                            sg_qid = "0" + sg_qid
                        rid = str(int(sg_qid + str(surveyID) + str(resp["id"])))
                        qid = int(str(surveyID) + str(resp["survey_data"][key]["id"]))
                        answer = resp["survey_data"][key]["answer"]

                        # account for drag & drop (rank) answer dicts
                        if type(answer) == dict and resp['survey_data'][key]['type'] == 'RANK':
                            for opt_key in answer.keys():
                                if answer[opt_key]['rank']:
                                    oid = int(str(qid) + str(opt_key))
                                    rid = str(int(str(sg_qid) + str(surveyID) + str(resp["id"] + str(opt_key))))
                                    rank_answer = answer[opt_key]['rank']
                                    resps.append([rid, qid, oid, survey_resp_id, rank_answer])
                                    oid = "NULL"
                                oid = "NULL"
                        else:
                            resps.append([rid, qid, oid, survey_resp_id, answer])
                    # get option answers
                    if resp["survey_data"][key]["type"] == "parent":
                        for key2 in resp["survey_data"][key]["options"]:
                            sg_qid = str(resp["survey_data"][key]["id"])
                            if len(sg_qid) == 1:  # add leading zero to 1-digit question_id
                                sg_qid = "0" + sg_qid
                            rid = str(int(sg_qid + str(surveyID) + str(resp["id"]) + str(resp["survey_data"][key]["options"][key2]["id"])))
                            qid = int(str(surveyID) + str(resp["survey_data"][key]["id"]))
                            oid = int(str(qid) + str(resp["survey_data"][key]["options"][key2]["id"]))
                            answer = resp["survey_data"][key]["options"][key2]["answer"]
                            resps.append([rid, qid, oid, survey_resp_id, answer])
                            qid = "NULL"
                            oid = "NULL"
                            rid = "NULL"
                except KeyError:
                    try:
                        if resp["survey_data"][key]["id"] not in bad_qs:
                            bad_qs.append(resp["survey_data"][key]["id"])
                    except KeyError:
                        pass

                # get answers to subquestions without options
                # TO-DO: GET OPTIONS FROM SUBQUESTIONS
                try:
                    if resp["survey_data"][key]["type"] == "parent":
                        for key3 in resp["survey_data"][key]["subquestions"]:
                            sg_qid = str(resp["survey_data"][key]["subquestions"][key3]["id"])
                            if len(sg_qid) == 1:  # add leading zero to 1-digit question_id
                                sg_qid = "0" + sg_qid
                            rid = str(int(sg_qid + str(surveyID) + str(resp["id"])))
                            qid = int(str(surveyID) + str(resp["survey_data"][key]["subquestions"][key3]["id"]))
                            answer = resp["survey_data"][key]["subquestions"][key3]["answer"]
                            resps.append([rid, qid, oid, survey_resp_id, answer])

                            # UNTESTED: GET OPTIONS OF SUBQUESTIONS
                            try:
                                for key4 in resp["survey_data"][key]["subquestions"][key3]["options"]:
                                    rid = str(int(str(surveyID) + str(resp["id"]) + str(
                                        resp["survey_data"][key]["subquestions"][key3]["options"][key4]["id"])))
                                    qid = int(str(surveyID) + str(resp["survey_data"][key]["subquestions"][key3]["id"]))
                                    oid = int(str(qid) + str(
                                        resp["survey_data"][key]["subquestions"][key3]["options"][key4]["id"]))
                                    answer = resp["survey_data"][key]["subquestions"][key3]["options"][key4]["answer"]
                                    resps.append([rid, qid, oid, survey_resp_id, answer])
                                    oid = "NULL"
                                    print("TESTING: Added subquestion option answers")
                            except KeyError:
                                pass
                except KeyError:
                    pass
        final_df = pd.DataFrame(resps, columns=headers)
        return final_df

    @classmethod
    def sg_answers_df(self, surveyID, api_token):
        '''Gets each page of results as a dataframe and concatenates all into one dataframe
        dict/list of dicts -> dataframe
        '''

        ans_dfs = []
        resp_dfs = []
        answers = self.sg_get_api_output_answers(10, 3, surveyID, api_token)
        if type(answers) == list:
            print("Number of pages:", len(answers))
            for i in answers:
                ans_page = self.answers_to_dataframe(i, surveyID)
                ans_dfs.append(ans_page)
                resp_page = self.sg_get_responses(surveyID, api_token, dfs=i)
                resp_dfs.append(resp_page)
        else:
            ans_df = self.answers_to_dataframe(answers, surveyID)
            resp_df = self.sg_get_responses(surveyID, api_token, dfs=answers)
            print("No concat required, returning single df.")
            return ans_df, resp_df

        ans_df = pd.concat(ans_dfs)
        resp_df = pd.concat(resp_dfs)
        print("Returning multiple dfs concatenated into one")
        return ans_df, resp_df
