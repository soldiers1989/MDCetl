from menu_actions import menu_actions
from Shared.db import DB
from time import sleep
import pandas as pd
from sg_db_interactions import sg_get_tables
from sg_contact_lists import sg_contact_lists
from sg_misc import misc_funcs as misc
from Shared.batch import BatchService
from Shared.common import Common as CM
import datetime
import urllib3


API_TOKEN = "api_token=3918099598ee3da7e79c1add7f4b8ae392b5b543c5fe7f9d88&api_token_secret=A9XYpy0QvtH.o"

# set pandas dataframe width
desired_width = 320
pd.set_option('display.width', desired_width)


def _main_():
    ''' PELASE INSTALL CERTIFICATE AND REMOVE THIS, WHERE EVER THE CERTIFICATE IS '''
    # urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    """Menu-selected actions for SGizmo API
    """

    menu_actions.write_survey_entries(API_TOKEN)

    menu = menu_actions.construct_menu()

    survey_id = 'w'
    selection = 0
    campaign_id = 'w'
    table_dict = 'w'
    surveys_df = None
    session_variables = []

    print("\nYou must enter a survey ID for this session.")
    # sleep(1)

    print("Downloading list of surveys now.")
    # sleep(1)

    surveys_df = menu_actions.get_surveys(API_TOKEN, with_stats=True)

    if surveys_df is not None:
        session_variables.append(1)

        # while type(survey_id) != int:
        #     try:
    survey_id = input("Enter a survey ID from above to use for this session: ")
    survey_id = menu_actions.validate_survey_id(survey_id, session_variables, API_TOKEN, surveys_df, mandatory=True)
        # except ValueError:
        #     continue

    menu_title = "\nMenu\n Quit: 99\n Back to main menu: -1\n============================================"

    # write all surveys from SG to DB, including BatchID


    while selection == 0:

        # main menu
        print(menu_title)

        # get survey title from survey_df
        survey_title = surveys_df.loc[surveys_df["id"] == str(survey_id), ["title"]]
        try:
            survey_title = survey_title.values[0][0]
        except IndexError:
            survey_title = None

        for key in menu:
            if survey_id != 'w' and key == 17:
                # specify current session survey
                print(str(key) + ".\t" + str(menu[key]) + " (Set as " + str(survey_id) + " : " + str(survey_title) + ")")
            elif key in session_variables:
                # strike through if action has been completed this session
                print('\u0336'.join(str(key) + ". " + menu[key]) + '\u0336' + "   DONE")
            else:
                print(str(key) + ".\t" + menu[key])

        while type(selection) != int or selection not in range(1, len(menu) + 1):
            try:
                selection = int(input("\nEnter a valid option number: "))
                if selection == 99:
                    print("You entered 99.\nI can't believe you've done this.")
                    print('Farewell...')
                    break
            except ValueError:
                continue

        # get surveys
        if selection == 1:

            if 1 not in session_variables:
                surveys_df = menu_actions.get_surveys(API_TOKEN)
                if surveys_df is not None:
                    session_variables.append(1)

            elif 1 in session_variables:
                print(surveys_df)
                print("Surveys already downloaded from SGizmo API.")
                sleep(0.75)
                print("Returning to main menu.")
                sleep(1)

        # get campaigns
        elif selection == 3:

            campaigns_df = menu_actions.get_campaigns(API_TOKEN, survey_id, session_variables, surveys_df)
            session_variables.append(3)

        # get email msgs
        elif selection == 5:

            if 3 not in session_variables:
                print("You must download campaign data first.")
                get_campaigns = input("Get campaign data now? (y/n): ")
                if get_campaigns.lower() ==  "y":
                    campaigns_df = menu_actions.get_campaigns(API_TOKEN, survey_id, session_variables, surveys_df)
                    if campaigns_df is not None:
                        session_variables.append(5)
                else:
                    print("Returning to main menu.")
                    sleep(1)

            elif 2 in session_variables:
                emails_df = menu_actions.get_emails(survey_id, API_TOKEN, session_variables, surveys_df)
                if emails_df is not None:
                    session_variables.append(5)

        # get contact lists
        elif selection == 9:

            if 9 not in session_variables:
                contact_list_df = menu_actions.get_contact_lists(survey_id, API_TOKEN)
                # contact_list_df = sg_contact_lists.sg_contactlists_df(API_TOKEN)
                print(contact_list_df)
                if contact_list_df is not None:
                    session_variables.append(9)
            elif 9 in session_variables:
                print(contact_list_df)
                print("Contact lists already downloaded. Returning to main menu.")
                sleep(1.5)

        # get contacts on list
        elif selection == 10:

            list_id = 'w'
            while type(list_id) != int:
                try:
                    list_id = int(input("Enter ID of contact list that you would like to retrieve: "))
                except ValueError:
                    continue
            if list_id != -1:
                contacts_df = menu_actions.get_contacts(API_TOKEN, list_id)

        # get respondents
        elif selection == 11:

            if 11 not in session_variables:
                resps_df = menu_actions.get_resps(survey_id, API_TOKEN)
                if resps_df is not None:
                    session_variables.append(11)
            elif 11 in session_variables:
                print("Already downloaded responses. Returning to main menu")
                sleep(1.5)

        # get questions or options
        elif selection in [12, 13]:

            if 12 not in session_variables and 13 not in session_variables:
                qs_df, os_df = menu_actions.get_qsos(survey_id, API_TOKEN)
                if qs_df is not None:
                    session_variables.append(12)
                if os_df is not None:
                    session_variables.append(13)

            if selection == 12 or (selection == 12 and 12 in session_variables):
                print(qs_df)

            elif selection == 13 or (selection == 13 and 13 in session_variables):
                print(os_df)

        # get answers
        elif selection == 14:

            if 14 not in session_variables:
                answers_df, resps_df = menu_actions.get_ans(survey_id, API_TOKEN)
                if answers_df is not None:
                    session_variables.append(14)

            elif 14 in session_variables:
                print("Already downloaded answers. Returning to main menu.")
                sleep(1.5)

        # get response statuses
        elif selection == 6:

            try:
                reports_df, status_df = menu_actions.get_resp_stats(survey_id, API_TOKEN)
            except TypeError:
                selection = 0
            if "JLAB" in survey_title:
                path = "/Users/gcree/Box Sync/MaRS DataCatalyst 2017 CONFIDENTIAL/JLABS Toronto Annual Survey 2017/Response_Status_Reports/"
                misc.write_to_xl(status_df.drop("invite_link", axis=1), "ResponseStatuses",
                                 out_path=path,
                                 sheetname="response_statuses")
            elif "annual" in survey_title.lower() and "2018" in survey_title:
                path = CM.get_config("config.ini", "paths", "survey2018_response_stats")
                misc.write_to_xl(status_df, 'ResponseStatuses' + survey_title,
                                 out_path=path,
                                 sheetname="response_statuses")

        # get resp stats for all campaigns
        elif selection == 19:

            campaigns_df = menu_actions.get_campaigns(API_TOKEN, survey_id, session_variables, surveys_df)
            campaigns_df = campaigns_df[['id', 'campaign_name', 'link_type', 'campaign_status']]
            reports_list = []
            status_list = []
            campaigns_df = campaigns_df[campaigns_df['campaign_status'] != 'Deleted']
            campaigns_df = campaigns_df[campaigns_df['link_type'] != 'link']
            for cid in campaigns_df["id"]:
                print('Downloading data for campaign id: {}'.format(cid))
                reports_df, status_df = menu_actions.get_resp_stats(survey_id, API_TOKEN, campaign_id=int(cid))
                if len(reports_df) > 0:
                    reports_list.append(reports_df)
                    status_list.append(status_df)

            # concat all reports dfs and concat all status dfs
            if len(reports_list) == 0:
                pass
            elif len(reports_list) == 1:
                reports_df = reports_list[0]
                status_df = status_list[0]
            else:
                reports_df = pd.concat(reports_list)
                status_df = pd.concat(status_list)

            # left join campaigns <- reports <- statuses dfs
            df1 = pd.merge(campaigns_df, reports_df, how='inner', left_on=["id"], right_on=["campaign_id"])
            all_resp_stats = pd.merge(df1, status_df, how='left', left_on='id_y', right_on='report_id')
            all_resp_stats = all_resp_stats.drop('id_y', axis=1).drop('campaign_id', axis=1).drop('report_id', axis=1).drop('primary_RIC', axis=1).drop('venture_id', axis=1)
            all_resp_stats = all_resp_stats.rename(columns={'id_x': "campaign_id"})

            path_ini = CM.get_config("config.ini", "paths", "sandbox")
            path = CM.change_working_directory(path_ini)
            print(path)
            misc.write_to_xl(all_resp_stats, 'ResponseStatuses - {}'.format(survey_title), out_path=path, sheetname="response_statuses")

            # stat_table = 'MDCReport.Fact_Response_Status'
            # print('Truncating and writing to ' + stat_table)
            # trunc_sql = CM.get_config('config.ini', 'sql_queries', 'trunc_stat_rep')
            # DB.execute(trunc_sql)
            # ins_sql = CM.get_config('config.ini', 'sql_queries', 'insert_stat_rep_f')
            # ins_sql = ins_sql.replace('(WHAT_HEADERS) ', '')
            # values = CM.df_list(all_resp_stats)
            # val_num = len(values)
            # for i in range(val_num):
            #     try:
            #         val = []
            #         for l, j in enumerate(values[i]):
            #             if isinstance(values[i][l], list):
            #                 val.append(''.join(str(x) for x in values[i][l]))
            #             elif isinstance(values[i][l], str):
            #                 val.append(CM.sql_compliant(values[i][l]))
            #             else:
            #                 val.append(values[i][l])
            #         tup = tuple(val)
            #         ins_sql_final = ins_sql.format(tup)
            #         ins_sql_final = ins_sql_final.replace('"', '\'')
            #         DB.execute(ins_sql_final)
            #         print("Record {} of {} : SUCCESS".format(i, val_num))
            #     except Exception as e:
            #         print("Record number {} of {} : ERROR: {}".format(i, val_num, e))
            #         print("ERROR VALUES : {}".format(ins_sql))
            #         continue


        # set survey ID
        elif selection == 17:

            # survey_id_choice = 1
            r_u_sure = 0
            # if type(survey_id) == int:
            while str(r_u_sure).lower() not in ['n', 'y']:
                try:
                    r_u_sure = input("""
                    Warning: changing the surveyID for this session will 
                    clear the data downloaded for the previous survey during this session. 
                    Do you still want to change the survey ID? (y/n): """)
                    if str(r_u_sure).lower() == 'y':
                        session_variables[:] = [y for y in session_variables if y in [1, 15]]
                        survey_id_choice = input("Survey ID has been reset. Enter new ID: ")
                        survey_id_choice = menu_actions.validate_survey_id(survey_id_choice, session_variables, API_TOKEN, surveys_df)
                        if survey_id_choice is not None:
                            survey_id = survey_id_choice
                    elif str(r_u_sure).lower() in ['n', str(-1)]:
                        print('Returning to main menu')
                        sleep(0.75)
                        selection = 0
                        break

                except ValueError:
                    continue

        # get all tables from schema into dfs
        elif selection == 15:

            # schema = "JLABS"
            # schema = str(input("Enter name of schema for which you would like to load all tables into dataframes: "))
            #
            # table_dict = menu_actions.get_db_tables(schema, printout=True)
            #
            # session_variables.append(15)
            #
            # # ========= Dependency query and dict ==========
            #
            # dependency_dict = menu_actions.get_dependencies(schema, printout=True)
            #
            # load_ordered_tables = menu_actions.get_load_order(schema, printout=True)
            pass

        # test get dependencies
        elif selection == 16:

            schema = str(input("Enter name of schema you would like to get dependencies for: "))

            skipped = False
            if menu_actions.return_to_main(schema) == 1:
                print("skipped")
                skipped = True
                sleep(0.5)

            if not skipped:
                dependencies = sg_get_tables.get_dependencies(schema)
                print("\n", dependencies)

            dependency_dict = {}
            for i in range(0, len(dependencies)):
                fkt = dependencies.iloc[:, 0][i]
                reft = dependencies.iloc[:, 1][i]
                if fkt not in dependency_dict.keys():
                    dependency_dict[fkt] = []
                dependency_dict[fkt].append(reft)

            print("\nDependency dict: \n")
            for key in dependency_dict.keys():
                print(key, ":", dependency_dict[key])

            load_order = sg_get_tables.get_load_order(schema)
            print("\nLOAD ORDER:\n", load_order)

        # load survey into DB
        elif selection == 2:

            # if 12 not in session_variables:
            #     print("Pull in tables from DB before loading survey data into DB")
            #     print("Execute menu item 12.")
            #     sleep(1)
            #
            # elif 12 in session_variables:
            #     # check if surveyID selected is in survey DB table
            #     surveys_table = table_dict["Surveys"]
            #     survey_ids = []
            #     for id in surveys_table["id"]:
            #         survey_ids.append(id)

                # if survey_id in survey_ids:
                #     print("Survey already exists in database")
                # elif survey_id not in survey_ids:
                #     print("Survey does not exist in DB. Loading survey data now")

            print("\nLoading survey entry into DB")
            menu_actions.load_survey_entry(surveys_df, survey_id)

            print("\nLoading survey questions & options into DB")
            menu_actions.load_qsos(survey_id, API_TOKEN)

            session_variables.append(2)

        # load responses, answers, contacts, contact lists, and contacts__lists entries
        elif selection == 4:

            exist = menu_actions.check_qs_exist(survey_id)
            if exist:
                print("At least one question for this survey exists in DB. Proceeding to load answers into DB")
                print("Loading Responses first...")

                menu_actions.load_resps_ans_contacts__lists(survey_id, API_TOKEN)

            else:
                print("No questions for this survey exist in DB. Load questions before loading answers.")

        # write all current survey to DB
        elif selection == 7:

            menu_actions.write_all_survey_components_to_db(session_variables, surveys_df, survey_id, API_TOKEN)
            session_variables.append(7)

        # write all components of all surveys to DB
        elif selection == 8:

            menu_actions.do_everything_for_all_surveys(session_variables, surveys_df, API_TOKEN)
            session_variables.append(8)

        elif selection == 18:

            sure = input(
                "\nAre you sure you wish to delete all components of current survey from the database? (y/n): ")
            if str(sure).lower() == "y":
                menu_actions.del_survey_components(survey_id)

        # quit program
        elif selection == 99:
            break

        selection = 0


if __name__ == '__main__':
    _main_()
