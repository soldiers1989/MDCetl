import pandas as pd
import requests
import json
from time import sleep
from Shared.common import Common as common


class sg_emails:

    @classmethod
    def sg_emails_json(self, surveyID, campaign_id, api_token, attempts=10, wait_sec=3):
        '''Takes  campaign id and api tokens and returns
        json-formatted dict with email messages.
        int, str, -> dict
        '''

        from time import sleep
        attempt_count = 0
        URL = "https://restapica.surveygizmo.com/v5/survey/" + str(surveyID) + "/surveycampaign/" + str(
            campaign_id) + "/emailmessage/" + "?" + api_token
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
    def sg_emails_df(self, surveyID, campaign_id, api_token):
        '''Takes campaign id and json-formatted dict and
        returns dataframe with email message data.
        int, dict -> dataframe
        '''

        json = self.sg_emails_json(surveyID, campaign_id, api_token)
        l = []
        headers = ["id", "campaign_id", "invite_identity", "subtype",
                   "message_type", "medium", "msg_status",
                   "from_email", "from_name", "subject",
                   "body_text", "body_html", "footer",
                   "date_created", "date_modified"]
        for i in json["data"]:
            eid = int(str(i["id"]) + str(campaign_id))
            inv_id = i["invite_identity"]
            subtype = i["subtype"]
            msg_type = i["message_type"]
            med = i["medium"]
            status = i["status"]
            emailfrom = i["from"]["email"]
            namefrom = i["from"]["name"]
            subj = i["subject"]
            bod_txt = i["body"]["text"]
            bod_html = i["body"]["html"]
            foot = i["footer"]
            date_created = i["date_created"]
            date_mod = i["date_modified"]
            l.append([eid, campaign_id, inv_id, subtype, msg_type, med,
                      status, emailfrom, namefrom, subj, bod_txt,
                      bod_html, foot, date_created, date_mod])
        emails_df = pd.DataFrame(l, columns=headers)

        return emails_df
