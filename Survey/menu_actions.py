from time import sleep
from sg_survey import sg_survey
from sg_campaign import sg_campaign
from sg_emails import sg_emails
from sg_contact_lists import sg_contact_lists
from sg_responses import sg_responses
from sg_qsos import sg_qsos
from sg_contact_status import sg_contact_status
from Shared.db import DB
from time import sleep
import pandas as pd
from sg_db_interactions import sg_get_tables
from sg_misc import misc_funcs as misc
from Shared.common import Common as CM
import numpy as np


class menu_actions():

    def __init__(self):
        pass

    @classmethod
    def return_to_main(self, user_input):

        if user_input == "-1":
            print("\nReturning to main menu.\n")
            sleep(1.15)
            return 1
        else:
            return 0

    @classmethod
    def construct_menu(self):

        menu_list = {1: "Get list of surveys and write to DB",
                     2: "Write current survey to DB (Q's & O's)",
                     3: "Write campaigns to DB",
                     4: "Write current survey answers to DB",
                     5: "Write emails to DB",
                     6: "Generate response status report and write it to DB",
                     7: "Write all components of current survey to DB",
                     8: "Write all components for all surveys to DB",
                     9: "Display contact lists on account",
                     10: "Display contacts on list",
                     11: "Display survey respondents",
                     12: "Display questions for survey",
                     13: "Display question options for survey",
                     14: "Display survey answers",
                     15: "Read all DB tables from schema into dataframes",
                     16: "Display schema dependencies (TEST)",
                     17: "Set survey ID for this session"
                     }

        return menu_list

    @classmethod
    def get_surveys(self, api_token, with_stats=False):

        if with_stats:
            surveys_df = sg_survey.get_list_df(api_token, with_stats=True)
        else:
            surveys_df = sg_survey.get_list_df(api_token, with_stats=False)
        print("\n", surveys_df)

        return surveys_df

    @classmethod
    def get_campaigns(self, api_token, survey_id, session_variables, surveys_df):

        if survey_id == 'w':
            while type(survey_id) != int:
                try:
                    survey_id = int(input("Enter ID of survey that you would like to retrieve campaign data for: "))
                    if self.return_to_main(survey_id) == 1:
                        return
                    survey_id = self.validate_survey_id(survey_id, session_variables, api_token, surveys_df)
                    survey_id = self.validate_survey_id(survey_id, session_variables, api_token, surveys_df)
                except ValueError:
                    continue

        campaigns_df = sg_campaign.sg_campaigns_df(survey_id, api_token)
        print(campaigns_df)
        campaigns_df["id"] = campaigns_df["id"].apply(pd.to_numeric, errors='ignore')

        # remove campaigns from df that are already in DB
        c_sql = CM.get_config("config.ini", "sql_queries", "campaigns_for_survey")
        c_sql = c_sql.replace("WHAT_SURVEY_ID", str(survey_id))
        db_cmpgns = DB.pandas_read(c_sql)
        if db_cmpgns is not None:
            db_cmpgns = db_cmpgns.apply(pd.to_numeric, errors='ignore')

        cmpgns_not_in_db = pd.merge(campaigns_df, db_cmpgns, how='left', indicator=True, on="id")
        cmpgns_not_in_db2 = cmpgns_not_in_db[cmpgns_not_in_db['_merge'] == 'left_only'].drop("_merge", axis=1)
        # cmpgns_not_in_db2 = cmpgns_not_in_db2.apply(pd.to_numeric, errors='ignore')

        # insert campaigns into DB
        if len(cmpgns_not_in_db2) > 0:
            insert_cmpgns_sql = "insert_campaigns"

            self.df_to_db(cmpgns_not_in_db2, insert_cmpgns_sql, remove_single_quotes=False, clean_numeric_cols=True)

        return campaigns_df

    @classmethod
    def get_emails(self, survey_id, api_token, session_variables, surveys_df, campaign_id='w'):

        if survey_id == 'w':
            while type(survey_id) != int:
                try:
                    survey_id = int(input("Enter ID of survey that you would like to retrieve campaign data for: "))
                    if menu_actions.return_to_main(survey_id) == 1:
                        return
                    survey_id = self.validate_survey_id(survey_id, session_variables, api_token, surveys_df)
                except ValueError:
                    continue

        while type(campaign_id) != int:
            try:
                campaign_id = int(input("Enter ID of campaign that you would like to retrieve email msg data for: "))
                if menu_actions.return_to_main(campaign_id) == 1:
                    return
            except ValueError:
                continue
        emails_df = sg_emails.sg_emails_df(survey_id, campaign_id, api_token)
        emails_df["id"] = emails_df["id"].apply(pd.to_numeric, errors='ignore')

        print(emails_df)

        # remove campaigns from df that are already in DB
        e_sql = CM.get_config("config.ini", "sql_queries", "emails_for_campaign")
        e_sql = e_sql.replace("WHAT_CAMPAIGN", str(campaign_id))
        db_em = DB.pandas_read(e_sql)

        em_not_in_db = pd.merge(emails_df, db_em, how='left', indicator=True, on="id")
        em_not_in_db2 = em_not_in_db[em_not_in_db['_merge'] == 'left_only'].drop("_merge", axis=1)

        # insert campaigns into DB
        if len(em_not_in_db2) > 0:
            insert_em_sql = "insert_emails"

            self.df_to_db(em_not_in_db2, insert_em_sql, remove_single_quotes=False, clean_numeric_cols=True)

            # em_headers, em_qmarks, em_vals = self.get_sql_params(em_not_in_db2, remove_single_quotes=False)
            # em_header_str = self.get_header_str(em_headers)
            # em_sql = CM.get_config("config.ini", "sql_queries", "insert_emails")
            # em_sql = em_sql.replace("WHAT_HEADERS", em_header_str).replace("WHAT_VALUES", em_qmarks)
            # for lst in em_vals:
            #     for i in range(len(lst)):
            #         element = lst[i]
            #         try:
            #             if str(element).lower() == "nan":
            #                 lst[i] = None
            #         except AttributeError:
            #             continue
            #         except TypeError:
            #             continue
            # DB.bulk_insert(em_sql, em_vals)

        return emails_df

    @classmethod
    def validate_survey_id(self, survey_id, session_variables, api_token, surveys_df, mandatory=False):

        if mandatory and survey_id == str(-1):
            print("You must enter a survey ID.")
        elif survey_id == str(-1):
            print("You have expressed the desire to return to main.")
            return None

        invalid_survey = True
        if 1 in session_variables:
            # check for survey id in surveys df
            for item in surveys_df["id"]:
                if item == str(survey_id):
                    invalid_survey = False

        elif 1 not in session_variables:
            # download surveys df and then check
            print("Downloading list of surveys on account.")
            surveys_df = sg_survey.get_list_df(api_token)
            session_variables.append(1)
            invalid_survey = True
            for item in surveys_df["id"]:
                if item == str(survey_id):
                    invalid_survey = False

        while invalid_survey:
            try:
                survey_id = int(input("Invalid survey ID. Please enter a valid survey_id: "))
                if not mandatory and menu_actions.return_to_main(survey_id) == 1:
                    print("Survey ID not set. Returning to main menu.")
                    sleep(0.5)
                    return None

                for item in surveys_df["id"]:
                    if item == str(survey_id):
                        invalid_survey = False
            except TypeError:
                continue
            except ValueError:
                continue

        # print("Valid ID entered.")
        return int(survey_id)

    @classmethod
    def get_contact_lists(self, survey_id, api_token):

        contact_list_df = sg_contact_lists.sg_contactlists_df(api_token)
        # print(contact_list_df)

        return contact_list_df

    @classmethod
    def get_contacts(self, api_token, list_id, survey_id=None, campaign_id=None):

        if self.return_to_main(list_id) == 1:
            return

        if survey_id is None:

            contacts_df = sg_contact_lists.sg_single_contactlist_df(list_id, api_token)
            print(contacts_df)

        elif survey_id is not None:
            contacts_df = sg_contact_lists.sg_campaign_contacts_df(survey_id, campaign_id, api_token)

        return contacts_df

    @classmethod
    def get_resps(self, survey_id, api_token):

        if survey_id == 'w':
            while type(survey_id) != int:
                try:
                    survey_id = int(input("Enter ID of survey that you would like to retrieve response data for: "))
                    if menu_actions.return_to_main(survey_id) == 1:
                        return
                except ValueError:
                    continue
        resps_df = sg_responses.sg_get_responses(survey_id, api_token)
        print(resps_df)

        return resps_df

    @classmethod
    def get_qsos(self, survey_id, api_token):

        if survey_id == 'w':
            while type(survey_id) != int:
                try:
                    survey_id = int(input("Enter ID of survey that you would like to retrieve question data for: "))
                except ValueError:
                    continue
        qs_df, os_df = sg_qsos.sg_get_qs_os(survey_id, api_token)

        return qs_df, os_df

    @classmethod
    def get_ans(self, survey_id, api_token):

        if survey_id == 'w':
            while type(survey_id) != int:
                try:
                    survey_id = int(input("Enter ID of survey that you would like to retrieve answer data for: "))
                    if menu_actions.return_to_main(survey_id) == 1:
                        return
                except ValueError:
                    continue
        answers_df, resp_df = sg_responses.sg_answers_df(survey_id, api_token)
        # print(answers_df)

        return answers_df, resp_df

    @classmethod
    def get_resp_stats(self, survey_id, api_token):

        campaign_id = 'w'
        if survey_id == 'w':
            while type(survey_id) != int:
                try:
                    survey_id = int(input("Enter ID of survey that you would like to retrieve status data for: "))
                    if menu_actions.return_to_main(survey_id) == 1:
                        return
                except ValueError:
                    continue
        while type(campaign_id) != int:
            try:
                campaign_id = int(input("Enter ID of campaign that you would like to retrieve status data for: "))
                if menu_actions.return_to_main(campaign_id) == 1:
                    return
            except ValueError:
                continue
        if survey_id != 'w':
            reports_df, status_df = sg_contact_status.sg_status_df(survey_id, campaign_id, api_token)
        else:
            df = sg_contact_status.sg_status_df(survey_id, campaign_id, api_token)
        print("\n", "Report record generated:")
        print("\n", reports_df, "\n")
        print("Response statuses below:")
        print("\n", status_df, "\n")

        # insert stat reports into DB
        if len(reports_df) > 0:
            insert_report_sql = "insert_resp_reports"

            self.df_to_db(reports_df, insert_report_sql, remove_single_quotes=False, clean_numeric_cols=True)

            # statreps_headers, statreps_qmarks, statreps_vals = self.get_sql_params(reports_df)
            #
            # statreps_header_str = self.get_header_str(statreps_headers)
            #
            # statreps_sql = CM.get_config("config.ini", "sql_queries", "insert_resp_reports")
            # statreps_sql = statreps_sql.replace("WHAT_HEADERS", statreps_header_str).replace("WHAT_VALUES", statreps_qmarks)
            # for lst in statreps_vals:
            #     for i in range(len(lst)):
            #         element = lst[i]
            #         try:
            #             if str(element).lower() == "nan":
            #                 lst[i] = None
            #         except AttributeError:
            #             continue
            #         except TypeError:
            #             continue
            # DB.bulk_insert(statreps_sql, statreps_vals)

        # insert resp statuses into DB
        if len(status_df) > 0:
            insert_stat_sql = "insert_resp_stats"

            self.df_to_db(status_df, insert_stat_sql, remove_single_quotes=False, clean_numeric_cols=True)

            # respstats_headers, respstats_qmarks, respstats_vals = self.get_sql_params(status_df)
            #
            # respstats_header_str = self.get_header_str(respstats_headers)
            #
            # respstats_sql = CM.get_config("config.ini", "sql_queries", "insert_resp_stats")
            # respstats_sql = respstats_sql.replace("WHAT_HEADERS", respstats_header_str).replace("WHAT_VALUES", respstats_qmarks)
            # for lst in respstats_vals:
            #     for i in range(len(lst)):
            #         element = lst[i]
            #         try:
            #             if str(element).lower() == "nan":
            #                 lst[i] = None
            #         except AttributeError:
            #             continue
            #         except TypeError:
            #             continue
            # DB.bulk_insert(respstats_sql, respstats_vals)

        return reports_df, status_df

    @classmethod
    def get_db_tables(self, schema, which_tables="all", printout=False):

        if self.return_to_main(schema) == 1:
            return

        table_dict = {}
        table_dict = sg_get_tables.schema_to_dfs(schema, which_tables)

        if len(table_dict) == 0:
            print("No tables were returned (empty dictionary)")
        elif len(table_dict) > 0:
            for key in table_dict:
                if printout:
                    print("\n", key, "\n", table_dict[key])

        if printout:
            print("\n\nNumber of tables returned: ", len(table_dict))
            print("Tables:")
            for key in table_dict:
                print(key)

        return table_dict

    @classmethod
    def get_load_order(self, schema, printout=False):

        if self.return_to_main(schema) == 1:
            return

        load_order = sg_get_tables.get_load_order(schema)
        if printout:
            print("\n\nLoad order dataframe:\n")
            print(load_order)

        ordered_tables = []
        for table in load_order["TableName"]:
            ordered_tables.append(table)
        if printout:
            print("\n\nDependency-Ordered Table List:\n")
            print(ordered_tables)

        return ordered_tables

    @classmethod
    def get_dependencies(self, schema, printout=False):

        if menu_actions.return_to_main(schema) == 1:
            return

        dependencies = sg_get_tables.get_dependencies(schema)
        if printout:
            print("\n", dependencies)

        dependency_dict = {}
        for i in range(0, len(dependencies)):
            fkt = dependencies.iloc[:, 0][i]
            reft = dependencies.iloc[:, 1][i]
            if fkt not in dependency_dict.keys():
                dependency_dict[fkt] = []
            dependency_dict[fkt].append(reft)

        if printout:
            print("\nDependency dict: \n")
            for key in dependency_dict.keys():
                print(key, ":", dependency_dict[key])

        return dependency_dict

    @classmethod
    def load_survey_entry(self, surveys_df, survey_id):

        tgt_survey = surveys_df.loc[surveys_df["id"] == str(survey_id)]
        tgt_survey = tgt_survey[["id", "title", "created_on", "modified_on", "survey_status", "survey_type"]]
        insert_survey_sql = "insert_survey_entry"

        self.df_to_db(tgt_survey, insert_survey_sql, remove_single_quotes=False)

        # headers, question_marks, insert_vals = self.get_sql_params(tgt_survey)
        #
        # header_str = self.get_header_str(headers)
        #
        # surveys_sql = CM.get_config("config.ini", "sql_queries", "insert_survey_entry")
        # surveys_sql = surveys_sql.replace("WHAT_HEADERS", header_str).replace("WHAT_VALUES", question_marks)
        # DB.bulk_insert(surveys_sql, insert_vals)

    @classmethod
    def load_qsos(self, survey_id, api_token):

        print("\nDownloading Questions & Options: \n")
        # get qs and os dataframes
        qs, os = self.get_qsos(survey_id, api_token)

        insert_qs_sql = "insert_qs"
        insert_os_sql = "insert_os"

        print("\nInserting Questions: \n")
        self.df_to_db(qs, insert_qs_sql, remove_single_quotes=False, clean_numeric_cols=True)

        print("\nInserting Options: \n")
        self.df_to_db(os, insert_os_sql, remove_single_quotes=False, clean_numeric_cols=True)

        # # get Questions headers, values, and question marks for pyodbc query
        # qs_headers, qs_qmarks, qs_insert_vals = self.get_sql_params(qs)
        # qs_header_str = self.get_header_str(qs_headers)
        #
        # # get Options headers, values, and question marks for pyodbc query
        # os_headers, os_qmarks, os_insert_vals = self.get_sql_params(os)
        # os_header_str = self.get_header_str(os)
        #
        # print("\nInserting Questions: \n")
        # # load Questions to DB
        # qs_sql = CM.get_config("config.ini", "sql_queries", "insert_qs")
        # qs_sql = qs_sql.replace("WHAT_HEADERS", qs_header_str).replace("WHAT_VALUES", qs_qmarks)
        # DB.bulk_insert(qs_sql, qs_insert_vals)
        #
        # print("\nInserting Options: \n")
        # # load Options to DB
        # os_sql = CM.get_config("config.ini", "sql_queries", "insert_os")
        # os_sql = os_sql.replace("WHAT_HEADERS", os_header_str).replace("WHAT_VALUES", os_qmarks)
        # DB.bulk_insert(os_sql, os_insert_vals)

    @classmethod
    def get_sql_params(self, df, remove_single_quotes=True):

        df_headers = []
        for header in df.columns:
            df_headers.append(str(header))

        insert_vals = []

        for index, row in df.iterrows():
            vals = []
            for header in df.columns:
                vals.append(row[header])
            if len(df) == 1:
                for i in range(0, len(vals)):
                    element = vals[i]
                    try:
                        if element == "NULL" or str(element.lower()) == "nan":
                            vals[i] = None
                    except AttributeError:
                        continue
                t = tuple(vals)
                insert_vals.append(t)
            else:
                insert_vals.append(vals)
        for record in insert_vals:
            for i in range(0, len(record)):
                element = record[i]
                try:
                    if element == "NULL" or str(element.lower()) == "nan":
                        record[i] = None
                except AttributeError:
                    continue
                try:
                    if "'" in element and remove_single_quotes == True:
                        record[i] = record[i].replace("'", "''")
                except TypeError:
                    continue

        question_marks = ""
        for header in df.columns:
            question_marks = question_marks + str("?, ")
        question_marks = question_marks[:-2]

        return df_headers, question_marks, insert_vals

    @classmethod
    def get_header_str(self, headers):

        header_str = ""
        for header in headers:
            header_str = header_str + "[" + header + "], "
        header_str = header_str[:-2]

        return header_str

    @classmethod
    def check_qs_exist(self, survey_id):

        sql = CM.get_config("config.ini", "sql_queries", "check_questions_exist")
        sql = sql.replace("WHAT_SURVEY_ID", str(survey_id))
        check = DB.pandas_read(sql)

        if check.iloc[0][0]:
            return True
        else:
            return False

    @classmethod
    def load_all_contacts_to_DB(self, survey_id, api_token):
        pass

    @classmethod
    def load_resps_ans_contacts__lists(self, survey_id, api_token):

        # get api resps
        print("\nGetting API responses (respondents)")
        api_ans, api_resps = self.get_ans(survey_id, api_token)

        # print("\nGetting API responses (respondents)")
        # api_resps = self.get_resps(survey_id, api_token)
        # api_resps["id"] = api_resps["id"].apply(pd.to_numeric, errors='ignore')

        # get api contacts where contacts.id = resps.contact_id:
        #   for each list in api contact_lists, download contacts in list
        #   put merge all api contacts into single dataframe
        #   discard where contacts.id != resps.contact_id
        print("\nGetting contact lists on account")
        contact_lists = self.get_contact_lists(survey_id, api_token)
        contact_lists = contact_lists.apply(pd.to_numeric, errors='ignore')

        print("\nGetting contact lists from DB")
        lists_in_db_sql = CM.get_config("config.ini", "sql_queries", "contact_lists")
        lists_in_db = DB.pandas_read(lists_in_db_sql)

        print("\nChecking for diffs b/t API contact lists and DB contact lists")
        lists_not_in_db = pd.merge(contact_lists, lists_in_db, how='outer', indicator=True, on="id")
        lists_not_in_db2 = lists_not_in_db[lists_not_in_db['_merge'] == 'left_only'].drop("_merge", axis=1)

        if len(lists_not_in_db2) > 0:
            print("\nOne or more new contact lists detected on acct. Loading into DB now")
            insert_lists_sql = "insert_contactlists"

            self.df_to_db(lists_not_in_db2, insert_lists_sql, remove_single_quotes=False)

            # list_headers, list_qmarks, list_vals = self.get_sql_params(lists_not_in_db2)
            # list_header_str = self.get_header_str(list_headers)
            # lists_sql = CM.get_config("config.ini", "sql_queries", "insert_contactlists")
            # lists_sql = lists_sql.replace("WHAT_HEADERS", list_header_str).replace("WHAT_VALUES", list_qmarks)
            # DB.bulk_insert(lists_sql, list_vals)

        print("\nGathering all contacts from all lists on acct into single dataframe")
        all_contacts = []
        for list_id in contact_lists["id"]:
            contact_list = self.get_contacts(api_token, list_id)
            all_contacts.append(contact_list)

        # gather all contacts from current survey
        all_campaigns = self.get_campaigns(api_token, survey_id, 0, 0)
        for campaign_id in all_campaigns['id']:
            campaign_contacts = self.get_contacts(api_token, list_id=0, survey_id=survey_id, campaign_id=campaign_id)
            if type(campaign_contacts) == int:
                continue
            else:
                all_contacts.append(campaign_contacts)

        all_contacts = pd.concat(all_contacts)
        all_contacts = all_contacts.apply(pd.to_numeric, errors='ignore')
        all_contacts['email_address'] = all_contacts['email_address'].str.lower()

        print("\nGathering all contacts from DB")
        all_contacts_sql = CM.get_config("config.ini", "sql_queries", "all_contacts")
        all_db_contacts = DB.pandas_read(all_contacts_sql)
        all_db_contacts = all_db_contacts.apply(pd.to_numeric, errors='ignore')
        all_db_contacts['email_address'] = all_db_contacts['email_address'].str.lower()

        contact_merge = pd.merge(
            all_contacts[["id", "mdc_contact_id", "contact_list_id", "email_address", "firstname", "lastname"]],
            all_db_contacts, how='left', on='email_address', indicator=True)

        new_contacts = contact_merge[["id_x", "email_address", "firstname_x", "lastname_x"]][
            contact_merge['_merge'] == 'left_only']
        new_contacts.columns = ["id", "email_address", "firstname", "lastname"]

        if len(new_contacts) > 0:
            print("Writing new contacts to DB.")
            insert_cs_sql = "insert_contacts"

            self.df_to_db(new_contacts, insert_cs_sql, clean_numeric_cols=True)

        updated_db_contacts = DB.pandas_read(all_contacts_sql)
        updated_db_contacts = updated_db_contacts.apply(pd.to_numeric, errors='ignore')
        updated_db_contacts['email_address'] = updated_db_contacts['email_address'].str.lower()

        updated_contact_merge = pd.merge(
            all_contacts[["id", "mdc_contact_id", "contact_list_id", "email_address", "firstname", "lastname"]],
            updated_db_contacts, how='left', on='email_address', indicator=True)
        api_contacts_lists_df = updated_contact_merge[["id_x", "id_y", "contact_list_id"]]
        api_contacts_lists_df = api_contacts_lists_df.apply(pd.to_numeric, errors='ignore')
        api_contacts_lists_df.columns = ["sg_cid", "mdc_contact_id", "contact_list_id"]

        print("\nGetting Contacts__Lists table from DB.")
        db_cl_sql = CM.get_config("config.ini", "sql_queries", "all_contacts__lists")
        db_contacts_lists_df = DB.pandas_read(db_cl_sql)
        db_contacts_lists_df = db_contacts_lists_df.apply(pd.to_numeric, errors='ignore')

        cl_merge = pd.merge(api_contacts_lists_df, db_contacts_lists_df, how='left', indicator=True,
                            on=["sg_cid", "mdc_contact_id", "contact_list_id"])
        new_cl = cl_merge[["sg_cid", "mdc_contact_id", "contact_list_id"]][cl_merge["_merge"] == 'left_only']
        new_cl = new_cl.apply(pd.to_numeric, errors='ignore')

        # INITIAL INSERT OF contacts__lists
        # if len(new_cl) > 0:
        #     print("Writing new entries to Contacts__Lists")
        #     insert_cl_sql = "insert_contacts_lists"
        #
        #     self.df_to_db(new_cl, insert_cl_sql, clean_numeric_cols=True)

        # get api answers where response_id = resps.id

        # get db resps where resps.survey_id = survey_id
        print("\nGetting all responses for this survey from DB.")
        r_sql = CM.get_config("config.ini", "sql_queries", "all_resps_for_survey")
        r_sql = r_sql.replace("WHAT_SURVEY_ID", str(survey_id))
        db_resps = DB.pandas_read(r_sql)
        db_resps["date_submitted"] = db_resps["date_submitted"].astype(str)

        print(
            "\nDetecting responses that have changed (looking for discrepancy between DB date_submitted and API date_submitted")
        # changed_resps = []
        i = 0
        changed_resps = pd.merge(api_resps[["id", "date_submitted"]], db_resps[["id", "date_submitted"]], how='left',
                                 indicator=True, on=["id", "date_submitted"])
        changed_resps = changed_resps[["id"]][changed_resps["_merge"] == 'left_only']
        changed_resps = changed_resps["id"].tolist()
        # for api_index, api_row in api_resps.iterrows():
        #     for db_index, db_row in db_resps.iterrows():
        #         i += 1
        #         print(str(i) + ".", api_row["id"], api_row["date_submitted"], "compared to", db_row["id"], db_row["date_submitted"])
        #         if api_row["id"] == db_row["id"] and api_row["date_submitted"] != db_row["date_submitted"]:
        #             changed_resps.append(api_row["id"])

        print("\nDetecting responses in API that are not in DB at all.")
        resps_not_in_db = pd.merge(api_resps, db_resps[["id"]], how='outer', indicator=True, on="id")
        resps_not_in_db2 = resps_not_in_db[resps_not_in_db['_merge'] == 'left_only'].drop("_merge", axis=1)

        inserted_resps = []

        # check if resps.contact_id contains any contact ids not found in contacts__lists
        # resp_contact_ids_not_in_contacts__lists = pd.merge(resps_not_in_db2[['contact_id']],
        #                                                    db_contacts_lists_df[['sg_cid']],
        #                                                    how='left',
        #                                                    indicator=True,
        #                                                    left_on='contact_id',
        #                                                    right_on='sg_cid')
        # resp_contact_ids_not_in_contacts__lists2 = resp_contact_ids_not_in_contacts__lists[
        #     resp_contact_ids_not_in_contacts__lists['_merge'] == 'left_only'].drop("_merge", axis=1)


        # SECOND INSERT OF contacts__lists
        new_cl = pd.merge(new_cl, db_contacts_lists_df, how='left', indicator=True, on=["sg_cid"])
        new_cl = new_cl[new_cl["_merge"] == 'left_only']
        new_cl = new_cl[["sg_cid", "mdc_contact_id_x", "contact_list_id_x"]]
        new_cl.columns = ["sg_cid", "mdc_contact_id", "contact_list_id"]


        if len(new_cl) > 0:
            print("Writing new entries to Contacts__Lists")
            insert_cl_sql = "insert_contacts_lists"

            self.df_to_db(new_cl, insert_cl_sql, clean_numeric_cols=True)

        # update Survey_Responses where date_submitted has changed for existing response
        if len(changed_resps) > 0:
            print("\nUpdating DB respondent entries that have changed (have diff date_submitted)")
            resp_headers, resp_qmarks, resp_vals = self.get_sql_params(api_resps)
            resp_header_str = self.get_header_str(resp_headers)

            update_r_sql = CM.get_config("config.ini", "sql_queries", "update_rs")
            for id in changed_resps:
                j = changed_resps.index(id)
                where_sql = "WHERE id = " + str(id)
                set_strs = ""
                for i in range(len(resp_headers)):
                    header = resp_headers[i]
                    val = resp_vals[j][i]
                    set_str = "[" + header + "]" + " = '" + str(val) + "', "
                    set_strs = set_strs + set_str
                final_update_sql = update_r_sql + set_strs[:-2] + " " + where_sql
                DB.execute(final_update_sql)

        # insert resps that aren't db at all
        if len(resps_not_in_db2) > 0:
            print("\nInserting new responses that aren't in DB at all")
            insert_resp_sql = "insert_rs"

            self.df_to_db(resps_not_in_db2, insert_resp_sql, remove_single_quotes=False)

            # resp_headers, resp_qmarks, resp_vals = self.get_sql_params(resps_not_in_db2)
            # resp_header_str = self.get_header_str(resp_headers)
            # resp_Sql = CM.get_config("config.ini", "sql_queries", "insert_rs")
            # resp_Sql = resp_Sql.replace("WHAT_HEADERS", resp_header_str).replace("WHAT_VALUES", resp_qmarks)
            # CM.bulk_insert(resp_Sql, resp_vals)

            for id in resps_not_in_db2["id"]:
                inserted_resps.append(id)

        # get api answers
        # print("\nGetting survey answers from API")
        # api_ans = self.get_ans(survey_id, api_token)
        # api_ans = api_ans.apply(pd.to_numeric, errors='ignore')

        # write to db only answers where answers.response_id is in list of response ids written to db above

        # del where id in changed_resps, then insert
        if len(changed_resps) > 0:
            print("\nDeleting answers of respondents who updated their response.")
            update_a_sql = CM.get_config("config.ini", "sql_queries", "update_a_sql")
            changed_ans_df = api_ans[api_ans["survey_response_id"].isin(changed_resps)]
            ans_headers, ans_qmarks, ans_vals = self.get_sql_params(changed_ans_df)
            ans_header_str = self.get_header_str(ans_headers)

            del_ans_sql = CM.get_config("config.ini", "sql_queries", "del_ans")
            for id in changed_resps:
                del_ans_sql_for_id = del_ans_sql.replace("WHAT_RESP_ID", str(id))
                DB.execute(del_ans_sql_for_id)
                inserted_resps.append(id)

        # insert ans where id in inserted_resps
        if len(inserted_resps) > 0:
            print("\nInserting answers into DB (includes updated responses and new responses)")
            # for id in inserted_resps:

            ans_insert_df = api_ans[api_ans["survey_response_id"].isin(inserted_resps)]
            inserts_ans_sql = "insert_as"

            ans_vals = self.df_to_db(ans_insert_df, inserts_ans_sql, remove_single_quotes=False, return_vals=True)

            # ans_headers, ans_qmarks, ans_vals = self.get_sql_params(ans_insert_df, remove_single_quotes=False)
            # ans_header_str = self.get_header_str(ans_headers)
            # ans_sql = CM.get_config("config.ini", "sql_queries", "insert_as")
            # ans_sql = ans_sql.replace("WHAT_HEADERS", ans_header_str).replace("WHAT_VALUES", ans_qmarks)
            # misc.write_to_xl(pd.DataFrame(ans_vals), "Survey Answer Load", "/Users/gcree/Box Sync/gcree/TESTING/")
            # DB.bulk_insert(ans_sql, ans_vals)

        elif len(inserted_resps) == 0:
            print("\nNo new answers to insert or update.")
            return

        print("\nChecking that all answers were inserted")
        check_ans_sql = CM.get_config("config.ini", "sql_queries", "check_ans")
        inserted_resp_ids_str = ''
        for id in inserted_resps:
            inserted_resp_ids_str = inserted_resp_ids_str + str(id) + ", "
        inserted_resp_ids_str = inserted_resp_ids_str[:-2]
        check_ans_sql = check_ans_sql.replace("WHAT_RESP_IDS", inserted_resp_ids_str)
        ans_inserted_this_session = DB.pandas_read(check_ans_sql)

        if len(ans_inserted_this_session) != len(ans_vals):

            print("\nNot all answers were loaded. Rolling back insert operation "
                  "(deleting answers and responses inserted into DB)")
            # del ans inserted this session, if any
            del_ans_sql = CM.get_config("config.ini", "sql_queries", "del_ans_by_respids")
            del_ans_sql = del_ans_sql.replace("WHAT_RESP_IDS", inserted_resp_ids_str)
            DB.execute(del_ans_sql)

            # del resps inserted this session, if any
            del_resps_sql = CM.get_config("config.ini", "sql_queries", "del_resps_by_list")
            del_resps_sql = del_resps_sql.replace("WHAT_RESP_IDS", inserted_resp_ids_str)
            DB.execute(del_resps_sql)

        elif len(ans_inserted_this_session) == len(ans_vals):
            print("All answers successfully inserted. This means that all the responses that were inserted during this "
                  "session have all their respective answers in the DB now.")
        return

    @classmethod
    def df_to_db(self, df, sql_config_header, remove_single_quotes=True, return_vals=False, clean_numeric_cols=False):

        df_headers, df_qmarks, df_vals = self.get_sql_params(df, remove_single_quotes=remove_single_quotes)
        df_header_str = self.get_header_str(df_headers)
        df_sql = CM.get_config("config.ini", "sql_queries", sql_config_header)
        df_sql = df_sql.replace("WHAT_HEADERS", df_header_str).replace("WHAT_VALUES", df_qmarks)

        if clean_numeric_cols:
            for lst in df_vals:
                for i in range(len(lst)):
                    element = lst[i]
                    try:
                        if str(element).lower() == "nan" or str(element) == "0000-00-00 00:00:00" or str(element) == '':
                            lst[i] = None
                        if np.dtype(element) == 'int64':
                            lst[i] = int(lst[i])
                    except AttributeError:
                        continue
                    except TypeError:
                        continue
                    except ValueError:
                        continue

        DB.bulk_insert(df_sql, df_vals)

        if return_vals:
            return df_vals

    @classmethod
    def write_all_survey_components_to_db(self, session_variables, surveys_df, survey_id, api_token):

        self.load_survey_entry(surveys_df, survey_id)
        self.load_qsos(survey_id, api_token)
        self.load_resps_ans_contacts__lists(survey_id, api_token)
        campaigns = self.get_campaigns(api_token, survey_id, session_variables, surveys_df)
        for c_id in campaigns["id"]:
            self.get_emails(survey_id, api_token, session_variables, surveys_df, campaign_id=c_id)

        return

    @classmethod
    def do_everything_for_all_surveys(self, session_variables, surveys_df, api_token):

        for survey_id in surveys_df["id"]:
            if surveys_df[surveys_df["id"] == survey_id]["survey_status"].iloc[0] != 'Closed':
                print(survey_id)
                self.write_all_survey_components_to_db(session_variables, surveys_df, survey_id, api_token)

            return
