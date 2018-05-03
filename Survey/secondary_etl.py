import sgizmo as sg
import json
import os
import pandas as pd
import requests
from Shared.common import Common as CM
from Survey.sg_misc import misc_funcs as misc
from Shared.db import DB
import numpy as np

API_TOKEN = "api_token=3918099598ee3da7e79c1add7f4b8ae392b5b543c5fe7f9d88&api_token_secret=A9XYpy0QvtH.o"
survey_id = 50021327


def pretty_dict(d):
    print(json.dumps(d, indent=1))


class Answer:

    def __init__(self, qid, srid, answer, surveyid, oid='', page_pipe=''):
        self.AID = str(qid) + str(surveyid) + str(srid) + str(oid) + str(page_pipe)
        self.QID = str(surveyid) + str(qid)
        if oid != '':
            self.OID = self.QID + str(oid)
        else:
            self.OID = oid
        self.SRID = str(surveyid) + str(srid)
        self.Answer = str(answer)
        self.page_pipe = str(page_pipe)
        self.SurveyID = str(surveyid)

    @staticmethod
    def cols():
        x = ['id', 'question_id', 'option_id', 'survey_response_id', 'answer', 'page_pipe']
        return x

    def record(self):
        return [self.AID, self.QID, self.OID, self.SRID, self.Answer, self.page_pipe]


class API:

    def __init__(self, api_token, domain, version, obj, obj_id, subobj1, params=''):

        self.api_token = api_token
        self.domain = domain
        self.version = version
        self.obj = obj
        self.obj_id = obj_id
        self.subobj1 = subobj1
        self.params = params

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

    def get_data(self, test=False):
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
                if pagenum > 0:
                    break
            # TEMPORARY ^
        return all_results


class Json:

    keep_qids = CM.get_config('config.ini', 'secondary_etl', 'sg_del_qids')

    def __init__(self, json, surveyid):
        self.json = json
        self.surveyid = surveyid

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

    def to_df(self):
        data = self.filter_out()
        all_ans = []
        for resp in data:
            srid = resp['id']
            for key in list(resp.keys())[11:]:
                qid = Json.extract_id(key[:18])
                page_pipe = Json.extract_id(key[15:])
                answer_str = str(resp[key])
                ans = Answer(qid=qid, srid=srid, answer=answer_str, surveyid=self.surveyid, page_pipe=page_pipe)
                answer = ans.record()
                all_ans.append(answer)
        all_ans = pd.DataFrame(all_ans, columns=Answer.cols())
        return all_ans


class DBInteractions:

    def __init__(self, data):
        self.data = data

    def clean_df(self):
        df = self.data
        df = df.where(df != '', None)
        self.data = df

    @staticmethod
    def store_df(df, filename):
        path = '/Users/gcree/Box Sync/Workbench/BAP/Annual Survey FY2018/DEV - Results to RICs/'
        misc.write_to_xl(df, filename, out_path=path)

    def load(self):
        df = self.data
        DBInteractions.store_df(df, '_NEW_PIPE_ANS')
        sql = CM.get_config('config.ini', 'sql_queries', 'insert_as')
        sql = sql.replace('WHAT_HEADERS', 'id, question_id, option_id, survey_response_id, answer, page_pipe')
        sql = sql.replace('WHAT_VALUES', '?,?,?,?,?,?')

        insert_vals = []

        for index, row in df.iterrows():
            vals = []
            for header in df.columns:
                vals.append(row[header])
            if len(df) == 1:
                t = tuple(vals)
                insert_vals.append(t)
            else:
                insert_vals.append(vals)

        DB.bulk_insert(sql, insert_vals, dev=False)

    @staticmethod
    def delete_old_ans():
        # delete old ans using answer ids
        #   store old ans in xl file
        old_ans_sql = CM.get_config('config.ini', 'secondary_etl', 'old_ans')
        old_ans_df = DB.pandas_read(old_ans_sql)
        # DBInteractions.store_df(old_ans_df, '_OLD_PIPE_ANS')
        #   run sql to delete old ans
        del_old_ans_sql = CM.get_config('config.ini', 'secondary_etl', 'del_old_ans')
        DB.execute(del_old_ans_sql)

    def etl(self):
        # clean
        self.clean_df()
        # delete old ans
        DBInteractions.delete_old_ans()
        # load
        self.load()


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
    data = api.get_data(test=False)
    j = Json(data, surveyid)
    all_ans = j.to_df()
    db_interactions = DBInteractions(all_ans)
    db_interactions.etl()
