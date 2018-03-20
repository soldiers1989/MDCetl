import pandas as pd
import requests
import json
import time
import datetime

class sg_contact_status:

    @classmethod
    def format_time(self):
        t = datetime.datetime.now()
        s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
        return s[:-3]

    @classmethod
    def sg_status_json(self, surveyID, campaignID, api_token, attempts=10, wait_sec=3):

        attempt_count = 0
        URL = "https://restapica.surveygizmo.com/v5/survey/" \
              + str(surveyID) \
              + "/surveycampaign/" \
              + str(campaignID) \
              + "/surveycontact"\
              + "/?resultsperpage=500&" \
              + api_token
        for i in range(0, attempts):
            try:
                attempt_count += 1
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
                time.sleep(wait_sec)

    @classmethod
    def sg_status_df(self, surveyID, campaignID, api_token):

        # create df for reports
        reports_headers = ["id", "campaign_id", "date_generated"]
        reports_lst = []
        report_id = int(round(time.time() * 1000))
        date_generated = self.format_time()
        reports_lst.append([report_id, campaignID, date_generated])
        reports_df = pd.DataFrame(reports_lst, columns=reports_headers)

        # insert stat reports into DB


        # create df for statuses
        json = self.sg_status_json(surveyID, campaignID, api_token)
        resp_statuses = []
        headers = ["report_id", "venture_id", "company_name", "primary_RIC", "first_name", "last_name", "email", "contact_status", "response_status",
                   "date_last_sent", "invite_link"]
        try:
            x = json["data"]
        except KeyError:
            return [], []

        for contact in json["data"]:
            fname = contact["first_name"]
            lname = contact["last_name"]
            email = contact["email_address"]
            contact_status = contact["status"]
            resp_status = contact["subscriber_status"]
            date_last_sent = contact["date_last_sent"]
            invite_link = contact["invitelink"]
            try:
                primary_RIC = contact["department"]
                company_name = contact["organization"]
                venture_id = contact["venture_id"]
            except KeyError:
                continue
            resp_statuses.append([report_id, venture_id, company_name, primary_RIC, fname, lname, email, contact_status, resp_status, date_last_sent, invite_link])
        resp_stat_df = pd.DataFrame(resp_statuses, columns=headers)
        return reports_df, resp_stat_df
