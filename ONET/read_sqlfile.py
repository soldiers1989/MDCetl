import pandas as pd
import os
from Shared.db import DB
import pyodbc
import json

file_table_map = {
    '11_abilities.sql': 'abilities',
    '35_alternate_titles.sql': 'alternate_titles',
    '31_career_changers_matrix.sql': 'career_changers_matrix',
    '32_career_starters_matrix.sql': 'career_starters_matrix',
    '01_content_model_reference.sql': 'content_model_reference',
    '26_dwa_reference.sql': 'dwa_reference',
    '05_ete_categories.sql': 'ete_categories',
    '12_education_training_experience.sql': 'education_training_experience',
    '30_emerging_tasks.sql': 'emerging_tasks',
    '28_green_dwa_reference.sql': 'green_dwa_reference',
    '23_green_occupations.sql': 'green_occupations',
    '24_green_task_statements.sql': 'green_task_statements',
    '25_iwa_reference.sql': 'iwa_reference',
    '13_interests.sql': 'interests',
    '02_job_zone_reference.sql': 'job_zone_reference',
    '14_job_zones.sql': 'job_zones',
    '15_knowledge.sql': 'knowledge',
    '06_level_scale_anchors.sql': 'level_scale_anchors',
    '03_occupation_data.sql': 'occupation_data',
    '07_occupation_level_metadata.sql': 'occupation_level_metadata',
    '36_sample_of_reported_titles.sql': 'sample_of_reported_titles',
    '04_scales_reference.sql': 'scales_reference',
    '16_skills.sql': 'skills',
    '08_survey_booklet_locations.sql': 'survey_booklet_locations',
    '09_task_categories.sql': 'task_categories',
    '18_task_ratings.sql': 'task_ratings',
    '17_task_statements.sql': 'task_statements',
    '27_tasks_to_dwas.sql': 'tasks_to_dwas',
    '29_tasks_to_green_dwas.sql': 'tasks_to_green_dwas',
    '34_tools_and_technology.sql': 'tools_and_technology',
    '33_unspsc_reference.sql': 'unspsc_reference',
    '19_work_activities.sql': 'work_activities',
    '10_work_context_categories.sql': 'work_context_categories',
    '20_work_context.sql': 'work_context',
    '21_work_styles.sql': 'work_styles',
    '22_work_values.sql': 'work_values'
}

conn = DB.connect(dev=True)

server = '10.101.2.74'
database = 'MDC_DEV'
username = 'gcree'
password = 'GccSQL1'
driver = '{/usr/local/lib/libmsodbcsql.17.dylib}'
cs = 'DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password


def make_connection():
    cx = pyodbc.connect(cs)
    return cx


def execute(sql):
    try:
        con = DB.connect()
        cursor = con.cursor()
        cursor.execute(sql)
        cursor.commit()
        msg = 'Query executed sucessfully.'
        # print(msg)
        return 'Success', msg
    except Exception as ex:
        msg = 'Executing Exception: {}'.format(ex)
        # print(msg)
        # print(sql)
        return 'Error', msg


def get_brackets(string, keyword, count_vals=False, list_vals=False):
    keyword = keyword.lower()
    s_ix = string.lower().index(keyword)
    short_str = string[s_ix:]
    strings = False
    val_ct = 0
    comma_pos = [0]
    for i in range(len(short_str)):
        if short_str[i] == "'" and not (short_str[i + 1] == "'" or short_str[i - 1] == "'"):
            strings = not strings
        if short_str[i - 2:i + 1] == "'''" and short_str[
            i + 1] != "'":  # in case of 3 single quotes in a row, toggle strings again
            strings = not strings
        if not strings:
            if short_str[i] == "(" and short_str[i - 1] == " ":
                open_ix = i + 1
            if i < len(short_str) - 1:
                if short_str[i] == ")" and short_str[i + 1] == " ":
                    end_ix = i
                    break
            elif i == len(short_str) - 1:
                if short_str[i] == ")":
                    end_ix = i
            if short_str[i] == ",":
                val_ct += 1

    bracket_vals = short_str[open_ix:end_ix]

    if len(bracket_vals) > 0:
        val_ct = val_ct + 1
        for i in range(len(bracket_vals)):
            #         if bracket_vals[i] == "'" and not (bracket_vals[i+1] == "'" or bracket_vals[i-1] == "'"):
            #             strings = not strings
            #         if bracket_vals[i-2:i+1] == "'''" and bracket_vals[i+1] != "'": # in case of 3 single quotes in a row, toggle strings again
            #             strings = not strings
            if bracket_vals[i] == "'":
                strings = not strings
            if not strings:
                if bracket_vals[i] == ",":
                    comma_pos.append(i)

        comma_pos.append(None)
        val_lst = []
        for i in range(len(comma_pos)):
            if comma_pos[i] == 0:
                continue
            start = comma_pos[i - 1]
            end = comma_pos[i]
            if start == 0:
                val = bracket_vals[start:end]
                val = val.strip()
                if val[0] == "'":
                    val = val[1:]
                if val[-1] == "'":
                    val = val[:-1]
                val_lst.append(val)
            else:
                val = bracket_vals[start + 1:end]
                val = val.strip()
                if val[0] == "'":
                    val = val[1:]
                if val[-1] == "'":
                    val = val[:-1]
                val_lst.append(val.strip())

    elif len(bracket_vals) == 0:
        raise ValueError('No characters found in parentheses')

    if count_vals:
        return bracket_vals, val_ct
    if list_vals:
        return bracket_vals, val_lst
    return bracket_vals

fill_these = ['career_changers_matrix'
                    ,'career_starters_matrix'
                    ,'dwa_reference'
                    ,'emerging_tasks'
                    ,'green_dwa_reference'
                    ,'green_occupations'
                    ,'green_task_statements'
                    ,'iwa_reference'
                    ,'sample_of_reported_titles'
                    # ,'task_statements'
                    ,'tasks_to_dwas'
                    ,'tasks_to_green_dwas'
                    ,'tools_and_technology'
                    ,'unspsc_reference'
                    ,'work_context'
                    ,'work_styles'
                    ,'work_values']


def _main_():
    slow_insert = True
    failed_cmds = {}
    dfs = []
    sheets_dir = os.path.expanduser('~/Box Sync/Innovation Economy/Projects/Employment Pathway - Google.org/Interactive Market Review and Report/EPP_Data collection/ONET Data/db_22_3_mssql/')
    os.chdir(sheets_dir)
    file_list = [x for x in os.listdir('.') if x.endswith('.sql')]
    file_list.sort()
    for file in file_list:
        # if True:
        if file_table_map[file] in fill_these:
            print("\n======\nLOADING FILE {}\n======\n".format(file))
            filename = file
            tablename = file_table_map[filename]

            # Open and read the file as a single buffer
            fd = open(filename, 'r')
            sqlFile = fd.read()
            fd.close()
    
            # all SQL commands (split on ';')
            sqlCommands = sqlFile.split(';')

            # clean commands
            print("Clening SQL commands")
            x = 0
            for i in range(len(sqlCommands)):
                x += 1
                if 'go' in sqlCommands[i].lower()[:5]:
                    sqlCommands[i] = sqlCommands[i][4:].strip('\n')
                else:
                    sqlCommands[i] = sqlCommands[i].strip('\n')
                print("\tFixed command number {}.".format(x))
                print("\tNow looks like: {}".format(sqlCommands[i]))

            cmd_cnt = len(sqlCommands)
            y = 0
            if slow_insert:
                failed_cmds[file] = []
                print('\nBeginning slow insert procedure for file: {}\n'.format(file))
                for cmd in sqlCommands[1:]:
                    y += 1
                    prcnt_cmplete = round((y / cmd_cnt) * 100, 3)
                    prog_msg = '\t\t\t\t\t\t\t\t\tCompletion: {}%  ({} of {})'.format(prcnt_cmplete, y, cmd_cnt)
                    cmd = cmd.replace('INSERT INTO ', 'INSERT INTO MDC_DEV.ONET.')
                    try:
                        exec_result, msg = execute(cmd)
                        if exec_result == 'Success':
                            print('\t' + msg)
                            print(prog_msg)
                        elif exec_result == 'Error':
                            failed_cmds[file].append(cmd)
                            print('\tError: {}\n\tSkipped and appended command to dict of failed commands')
                            print('\tFailed command: ' + str(cmd))
                            print(prog_msg)
                    except Exception as e:
                        failed_cmds[file].append(cmd)
                        print('\tError: {}\n\tSkipped and appended command to dict of failed commands'.format(e))
                        print('\tFailed command: ' + str(cmd))
                        print(prog_msg)
            else:

                print('\nBeginning bulk insert procedure for file: {}\n'.format(file))
                # parse one of the commands to get cloumn names and num question marks need for bulk insert sql construction
                cols = get_brackets(sqlCommands[2], "INTO ", )
                _, val_ct = get_brackets(sqlCommands[2], "VALUES ", count_vals=True)
                q_marks = ''
                for i in range(val_ct):  # construct question marks string for use in sql construction
                    q_marks = q_marks + "?, "
                q_marks = q_marks[:-2]

                # construct sql insert for bulk insert
                sql = "INSERT INTO MDC_DEV.ONET." + tablename + " (" + cols + ") VALUES (" + q_marks + ");"
                print("Constructing SQL INSERT statement for BulkInsert operation.\nSQL: " + str(sql))

                # add values of commands to df, excluding create table and emptyspace
                print('Adding values to list of lists for use in BulkInsert operation.')
                l = []
                for k in range(1, len(sqlCommands)):
                    try:
                        # st_ix = sqlCommands[k].index("VALUES (")
                        # open_bracket = sqlCommands[k][st_ix:].index("(")
                        vals_str, vals_lst = get_brackets(sqlCommands[k], "VALUES ", list_vals=True)

                        print('\tCleaning NULLs')
                        cnt = 0
                        for i in range(len(vals_lst)):
                            if vals_lst[i].strip().lower() == 'null':
                                vals_lst[i] = None
                                cnt += 1
                        print('\tCleaned {} NULLs'.format(cnt))

                        l.append(vals_lst)

                    except ValueError:
                        print('\tValueError for command at index {}.\n\tThe command {} does not contain the string "VALUES (".'.format(k, sqlCommands[k]))
                        print("\tNo values from this command were added to the list of values.")

                # check quality of l
                print("Checking to ensure correct number of parameters.")
                k = 0
                for element in l:
                    if len(element) != val_ct:
                        print("\tWarning: element {} is wrong len. Should be len {}, is actually len {}".format(k, val_ct, len(element)))
                        print("\tElement in question: " + str(element))
                        print("\tOriginal command at index {}: {}".format(k, sqlCommands[k+1]))
                        raise ValueError('Incorrect count of parameters')
                    k += 1

                print('Inserting values into table {}'.format(file_table_map[file]))
                DB.bulk_insert(sql=sql, values=l)

            print('Finished with file {}\n'.format(file))
    print('Dumping dict of failed cmds to json file')
    out_path = os.path.expanduser("~/MDCetl/MDCetl/ONET/Failed_cmds/failed_cmds.json")
    with open(out_path, 'w') as jsonFile:
        json.dump(failed_cmds, jsonFile)


if __name__ == '__main__':
    _main_()
