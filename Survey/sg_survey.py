import pandas as pd
import requests
import json
from time import sleep
from fake_useragent import UserAgent

API_TOKEN = "api_token=3918099598ee3da7e79c1add7f4b8ae392b5b543c5fe7f9d88&api_token_secret=A9XYpy0QvtH.o"

class sg_survey:

    def __init__(self):
        pass

    @classmethod
    def get_list_json(self, api_token, attempts=10, wait_sec=3):
        """
        Takes str api token and returns all surveys
        associated with account (in json-like dict).
        str -> dict
        """
        
        attempt_count = 0
        URL = "https://restapica.surveygizmo.com/v5/survey/?resultsperpage=500&" + str(api_token)
        for i in range(0, attempts):
            try:
                attempt_count += 1
                ua = UserAgent()
                headers = {"User-Agent": ua.chrome}
                output = requests.get(URL, headers=headers)
                if output.ok:
                    output = output.json()
                    print("Success. Stored API output in json dict.")
                    return output
            except KeyboardInterrupt:
                pass
            except Exception as ex:
                if attempt_count >= attempts:
                    print("All attempts failed")
                    return
                print("Likely SSLError. Trying again in", wait_sec, "second(s)...", ex)
                sleep(wait_sec)

    @classmethod
    def get_list_df(self, api_token, with_stats=False):
        """
        Takes str api token and returns SurveyGizmo surveys
        associated with account as a dataframe
        str -> dataframe
        """

        headers = ["id", "title", "created_on", "modified_on", "survey_status", "survey_type"]
        json = self.get_list_json(api_token)
        df = pd.DataFrame(json["data"])
        if with_stats:
            headers = ["id", "title", "created_on", "modified_on", "survey_status", "survey_type", "resp_statistics"]
            df = df.filter(items=["id", "title", "created_on", "modified_on", "status", "type", "statistics"])
        else:
            headers = ["id", "title", "created_on", "modified_on", "survey_status", "survey_type"]
            df = df.filter(items=["id", "title", "created_on", "modified_on", "status", "type"])
        df.columns = headers
        df = df.query("survey_type != 'QuestionLibrary'")
        return df

    @classmethod
    def sg_list_surveyIDs(self, df=0, api_token=0):

        lst = []
        if api_token == 0:
            lst = df.query("survey_type != 'QuestionLibrary'")
            lst = lst["id"]
            return lst
        elif df == 0:
            df = self.get_list_df(api_token)
            lst = df.query("survey_type != 'QuestionLibrary'")
            lst = lst["id"]
            return lst
