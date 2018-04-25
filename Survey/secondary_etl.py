import sgizmo as sg
import json
import os
import pandas as pd
import requests
from Shared.common import Common as CM

API_TOKEN = "api_token=3918099598ee3da7e79c1add7f4b8ae392b5b543c5fe7f9d88&api_token_secret=A9XYpy0QvtH.o"
survey_id = 50021327


def pretty_dict(d):
    print(json.dumps(d, indent=1))


class Process:

    def __init__(self, api_token, domain, version, obj, obj_id, subobj1, params=''):
        if not params:
            self.params = {'page': 1, 'resultsperpage': 200}
        self.api_token = api_token
        self.domain = domain
        self.version = version
        self.obj = obj
        self.obj_id = obj_id
        self.subobj1 = subobj1
        self.params = params


class Answer(Process):

    def __init__(self, qid, srid, answer, surveyid, api_token, domain, version, obj, obj_id, subobj1, oid='',
                 page_pipe=''):
        super().__init__(api_token, domain, version, obj, obj_id, subobj1)
        self.AID = str(qid) + str(surveyid) + str(srid) + str(oid) + str(page_pipe)
        self.QID = str(qid)
        self.OID = str(oid)
        self.SRID = str(srid)
        self.Answer = str(answer)
        self.page_pipe = str(page_pipe)
        self.SurveyID = str(surveyid)

    def lst(self):
        lst = [self.AID, self.QID, self.OID, self.SRID, self.Answer, self.page_pipe]
        return lst

    @staticmethod
    def cols():
        x = ['id', 'question_id', 'option_id', 'survey_response_id', 'answer', 'page_pipe']
        return x

    def to_df(self):



class API(Process):

    def __init__(self, api_token, domain, version, obj, obj_id, subobj1, params=''):
        super().__init__(api_token, domain, version, obj, obj_id, subobj1, params)

    @staticmethod
    def get_cert_path():
        path = os.path.join('', '/Users/gcree/MDCetl/MDCetl/Shared/MaRSDD-Root.pem')
        return path

    def call(self, url):
        api_data = requests.get(url, verify=API.get_cert_path())
        api_data = api_data.json()
        return api_data

    def make_url(self):
        url = sg.make_url(api_token=self.api_token,
                          domain=self.domain,
                          version=self.version,
                          obj=self.obj,
                          obj_id=self.obj_id,
                          subobj1=self.subobj1,
                          params=self.params)
        return url

    def set_params(self, param_key, new_val):
        self.params[str(param_key)] = new_val

    def get_data(self, test=True):
        all_results = []
        output = self.call(self.make_url())
        pagenum = int(output['page'])
        all_results.extend(output['data'])
        for i in range(1, output['total_pages']):
            pagenum += 1
            api.set_params('page', pagenum)
            output = self.call(self.make_url())
            all_results.extend(output['data'])
            # TEMPORARY
            if test:
                if pagenum > 1:
                    break
            # TEMPORARY ^
        return all_results


class Json():

    keep_qids = CM.get_config('config.ini', 'secondary_etl', 'sg_del_qids')

    def __init__(self, json):
        self.json = json

    def filter_out(self):
        keeps = self.get_full_keys('question')
        filtered_dicts = []
        for dic in self.json:
            filtered_dic = {}
            for key in keeps:
                if dic[key] != '':
                    filtered_dic[key] = dic[key]
            filtered_dicts.append(filtered_dic)

        return filtered_dicts

    @staticmethod
    def extract_id(string):
        x = string.find("(") + 1
        y = string.find(")")
        return string[x:y]

    def get_full_keys(self, key_str):
        d = self.json[0]
        keeps = self.keep_qids.split(',')
        full_keys = []
        keys = list(d.keys())
        full_keys.extend(keys[:11])
        for key in keys[12:]:
            small_key = key[:18]
            if Json.extract_id(small_key) in keeps and key_str in small_key:
                full_keys.append(key)
        return full_keys

    # TODO: replace this with metadata shelve read
    # del_qids = '5002132769,5002132770,5002132771,5002132772,5002132773,5002132774,5002132775,5002132776,5002132777,5002132778,5002132779,50021327326,50021327326,50021327327,50021327283'

    # get del_vals

    # get rep_vals

    # delete del_vals from DB
    # write rep_vals to DB
    # check for completeness (read DB rep_vals and c.f. current rep_vals)
    # rollback (if necessary)
    # error processing (rollback)


if __name__ == '__main__':
    select_qs = CM.get_config("config_sql.ini", "ann_survey_18", "select_ans_by_qids")
    domain = 'restapica'
    v = '4'
    survey = 'survey'
    surveyid = '50021327'
    resp = 'surveyresponse'
    params = {'resultsperpage': 200,
              "filter[field][0]=status&filter[operator][0]=!=&filter[value][0]": 'deleted',
              'page': 1}
    api = API(API_TOKEN, domain, v, survey, surveyid, resp, params)
    data = api.get_data()
    j = Json(data)
    filtered_jsons = j.filter_out()
    ans = Answer()

    # TODO: WHEN DONE W/ TESTING, REMOVE PAGENUM LOOP LIMITER IN API.GET_DATA() METHOD
