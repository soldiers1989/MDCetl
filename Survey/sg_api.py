import sgizmo as sg
import json
import os
import pandas as pd
import requests
from Shared.common import Common as CM
from Survey.sg_misc import misc_funcs as misc
from Shared.db import DB
import numpy as np


class API:

    def __init__(self, api_token, domain, version, obj, obj_id, subobj1, subobj1_id = '', params=''):

        self.api_token = api_token
        self.domain = domain
        self.version = version
        self.obj = obj
        self.obj_id = obj_id
        self.subobj1 = subobj1
        self.subobj1_id = subobj1_id
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
