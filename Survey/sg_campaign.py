import pandas as pd
import requests
import json
from time import sleep


class sg_campaign:

    @classmethod
    def sg_campaigns_json(self, surveyID, api_token, attempts=10, wait_sec=3):
        '''Takes Sgizmo surveyID, api token and returns
        campaigns as dataframe.
        int, str, -> dict
        '''

        attempt_count = 0
        URL = "https://restapica.surveygizmo.com/v5/survey/" + str(surveyID) + "/surveycampaign/?resultsperpage=500&" + api_token
        for i in range(0, attempts):
            try:
                attempt_count +=1
                output = requests.get(URL, verify=False)
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
    def sg_campaigns_df(self, surveyID, api_token):
        '''Takes surveyID and json and returns
        dataframe.
        int, dict -> dataframe
        '''

        json = self.sg_campaigns_json(surveyID, api_token)
        l=[]
        headers = ["id", "survey_id", "invite_id", "campaign_type",
                   "link_type", "subtype", "campaign_status", "campaign_name",
                   "uri", "ssl", "close_message", "link_open_date",
                   "link_close_date", "language", "date_created",
                   "date_modified"]
        for j in json["data"]:
            cid = j["id"]
            inv_id = j["invite_id"]
            typ = j["type"]
            link_typ = j["link_type"]
            subtype = j["subtype"]
            status = j["status"]
            name = j["name"]
            uri = j["uri"]
            ssl = j["SSL"]
            close = j["close_message"]
            open_date = j["link_open_date"]
            close_date = j["link_close_date"]
            lang = j["language"]
            date_created = j["date_created"]
            date_mod = j["date_modified"]
            l.append([cid, surveyID, inv_id, typ, link_typ, subtype, status, name,
                      uri, ssl, close, open_date, close_date, lang,
                      date_created, date_mod])
        df = pd.DataFrame(l, columns=headers)
        return df