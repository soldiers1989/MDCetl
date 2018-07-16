import pandas as pd
import requests
import json
import time
import datetime
from Shared.common import Common as common


class sg_contact_status:

    @classmethod
    def format_time(self):
        t = datetime.datetime.now()
        s = t.strftime('%Y-%m-%d %H:%M:%S.%f')
        return s[:-3]

    @classmethod
    def sg_status_json(self, surveyID, campaignID, api_token, attempts=5, wait_sec=3):

        attempt_count = 0
        which_page = 1
        pg_cnt = 1
        URL = "https://restapica.surveygizmo.com/v5/survey/" \
              + str(surveyID) \
              + "/surveycampaign/" \
              + str(campaignID) \
              + "/surveycontact"\
              + "/?resultsperpage=300&" \
              + "page=" + str(pg_cnt) + "&"\
              + api_token
        pages = []

        for i in range(0, attempts):
            try:
                attempt_count += 1
                output = requests.get(URL, verify=common.get_cert_path())
                if output.ok:
                    output = output.json()
                    pages.append(output)
                    pg_cnt = output["total_pages"]
                    print("Success. Stored API output in json dict.")

                    if pg_cnt > 1:
                        for i in range(which_page, pg_cnt):
                            which_page += 1
                            last_page = which_page - 1
                            replace_this = "page=" + str(last_page) + "&"
                            with_this = "page=" + str(which_page) + "&"
                            URL = URL.replace(replace_this, with_this)
                            output = requests.get(URL, verify=common.get_cert_path())
                            if output.ok:
                                output = output.json()
                            pages.append(output)

                    print("Success. All results stored in dict(s).")
                    if len(pages) == 1:
                        print("Output: Single dict")
                        return output
                    elif len(pages) > 1:
                        print("Output: List of dicts")
                        return pages

                    return output

                # if output.ok:
                #     output = output.json()
                #     result_pages.append(output)
                #     print("Success. Stored API output in json dict.")
                #     resultsperpage = int(output["results_per_page"])
                #     totalresults = int(output["total_count"])
                #     if totalresults > resultsperpage:
                #         print("total exceeds results_per_page")
                #     page_cnt = output["total_pages"]
                #     print("Final page count will be", page_cnt)
                #     if page_cnt > 1:
                #         for i in range(which_page, page_cnt):
                #             print("Making call to API for another page")
                #             which_page += 1
                #             URL = sg_responses.create_response_API_URL(surveyID, api_token, resultsperpage=100, page=which_page)
                #             resultpage = sg_responses.sg_get_api_output(URL, 10, 3)
                #             result_pages.append(resultpage)







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

        if json is None:
            return [], []

        dfs = []
        if len(json) > 1 and type(json) == list:
            for j in json:
                df = self.stats_json_to_df(j, report_id)
                dfs.append(df)
            resp_stat_df = pd.concat(dfs)
            dfs = []
        else:
            resp_stat_df = self.stats_json_to_df(json, report_id)

        # for contact in json["data"]:
        #     fname = contact["first_name"]
        #     lname = contact["last_name"]
        #     email = contact["email_address"]
        #     contact_status = contact["status"]
        #     resp_status = contact["subscriber_status"]
        #     date_last_sent = contact["date_last_sent"]
        #     invite_link = contact["invitelink"]
        #     try:
        #         primary_RIC = contact["department"]
        #         company_name = contact["organization"]
        #         venture_id = contact["venture_id"]
        #     except KeyError:
        #         continue
        #     resp_statuses.append([report_id, venture_id, company_name, primary_RIC, fname, lname, email, contact_status, resp_status, date_last_sent, invite_link])
        # resp_stat_df = pd.DataFrame(resp_statuses, columns=headers)
        return reports_df, resp_stat_df

    @classmethod
    def stats_json_to_df(self, json, report_id):

        resp_statuses = []
        headers = ["report_id", "venture_id", "company_name", "primary_RIC", "first_name", "last_name", "email", "phone",
                   "contact_status", "response_status",
                   "date_last_sent", "invite_link"]

        try:
            x = json["data"]
        except KeyError:
            return
        except TypeError:
            return

        for contact in json["data"]:
            fname = contact["first_name"]
            lname = contact["last_name"]
            email = contact["email_address"]
            phone = contact['business_phone']
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
            resp_statuses.append(
                [report_id, venture_id, company_name, primary_RIC, fname, lname, email, phone, contact_status, resp_status,
                 date_last_sent, invite_link])

        df = pd.DataFrame(resp_statuses, columns=headers)

        return df
