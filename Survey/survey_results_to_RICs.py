from Shared.db import DB
from Shared.common import Common as CM
from Shared.file_service import FileService as fileservice
import datetime
import os
import shelve
import pandas as pd
import numpy as np

desired_width = 520
pd.set_option('display.width', desired_width)
q_meta_name = 'qs_metadata'


def save_xls(list_dfs, xls_path, sheetnames=[]):
    writer = pd.ExcelWriter(xls_path)
    for n, df in enumerate(list_dfs):
        if len(sheetnames) == len(list_dfs):
            df.to_excel(writer, '%s' % sheetnames[n], index=False)
        else:
            df.to_excel(writer, 'sheet%s' % n, index=False)
    writer.save()


def include_list(ric):
    """Returns list of qids that should be included in provided RIC's data dictionary.
    str -> list
    """
    with shelve.open(q_meta_name, 'r') as sh:
        qids = list(sh['id'].keys())
        include = []
        for qid in qids:
            if (qid in sh['core/noncore']['core'] or qid in sh['addedby'][ric]) \
                    and qid in sh['which_survey']['EVERYONE ELSE']:
                include.append(qid)
    return include


def multi_options(q_type):
    if q_type in ['ESSAY', 'TABLE', 'TEXTBOX', 'MENU', 'RADIO', 'LOGIC', 'HIDDEN', 'ACTION', 'JAVASCRIPT', 'EMAIL']:
        return 0
    else:
        return 1


def opt_col_title(row):
    d = {1: str(row['col_title']) + ' - Option: ' + str(row['o_label']),
         0: row['col_title']}
    return d[row['multi_options']]


def concat_cols(row, cols):
    """Concats 2 columns of row. Returns concatted valued
    series, list -> str
    """
    return str(row[cols[0]]) + "-" + str(row[cols[1]])


def path_xl(user_path, path_extension, sep="/", filename='file.xlsx'):

    path = sep.join([user_path, path_extension, filename])
    return path


def replacements(row):
    """Replace values with other values based on business logic.
    """
    r = {'5002132717': {'1': "Active", '2': "Ceased operations", '3': "Acquired"},
         '5002132768': {'99': 'n/a'},
         '5002132769': {'99': 'n/a'},
         '5002132770': {'99': 'n/a'},
         '5002132771': {'99': 'n/a'},
         '5002132772': {'99': 'n/a'},
         '5002132773': {'99': 'n/a'},
         '5002132719': {'1': 'Jan',
                        '2': 'Feb',
                        '3': 'Mar',
                        '4': 'Apr',
                        '5': 'May',
                        '6': 'Jun',
                        '7': 'Jul',
                        '8': 'Aug',
                        '9': 'Sep',
                        '10': 'Oct',
                        '11': 'Nov',
                        '12': 'Dec'},
         '5002132715': {'1': 'Jan',
                        '2': 'Feb',
                        '3': 'Mar',
                        '4': 'Apr',
                        '5': 'May',
                        '6': 'Jun',
                        '7': 'Jul',
                        '8': 'Aug',
                        '9': 'Sep',
                        '10': 'Oct',
                        '11': 'Nov',
                        '12': 'Dec'},
         '5002132724': {'1': 'Jan',
                        '2': 'Feb',
                        '3': 'Mar',
                        '4': 'Apr',
                        '5': 'May',
                        '6': 'Jun',
                        '7': 'Jul',
                        '8': 'Aug',
                        '9': 'Sep',
                        '10': 'Oct',
                        '11': 'Nov',
                        '12': 'Dec'}
         }
    qid = str(round(row['QuestionID']))
    ans = str(row['Answer'])
    if qid in list(r.keys()):
        if ans in list(r[qid].keys()):
            return r[qid][ans]
        else:
            return ans
    else:
        return ans


def _main_():
    # make the damn ric dict: ricname: datasourceID (except CII & OSVP, number is not datasourceid)
    rics = {
        'MaRS Discovery District': {'db_name': 'MaRS Discovery District', 'code': 7},
        'RIC Centre': {'db_name': 'RIC Centre', 'code': 9},
        'Innovation Factory': {'db_name': 'Innovation Factory', 'code': 12},
        'NWOIC': {'db_name': 'NWO Innovation Centre', 'code': 14},
        'Invest Ottawa': {'db_name': 'Invest Ottawa', 'code': 16},
        'IION': {'db_name': 'IION', 'code': 5},
        'CII': {'db_name': 'MaRS Centre for Impact Investing', 'code': -1},
        'OSVP': {'db_name': 'Ontario Scale-Up Voucher Program', 'code': -1},
        'Innovation Guelph': {'db_name': 'Innovation Guelph', 'code': 15},
        'WEtech': {'db_name': 'WEtech', 'code': 2},
        'SSMIC': {'db_name': 'SSMIC', 'code': 3},
        'TechAlliance': {'db_name': 'TechAlliance', 'code': 6},
        'Haltech': {'db_name': 'Haltech', 'code': 8},
        'Spark Centre': {'db_name': 'Spark Centre', 'code': 10},
        'NORCAT': {'db_name': 'NORCAT', 'code': 1},
        'VentureLAB': {'db_name': 'ventureLAB', 'code': 11},
        'Innovate Niagara': {'db_name': 'Innovate Niagara', 'code': 17},
        'Launch Lab': {'db_name': 'Launch Lab', 'code': 13}
    }

    with shelve.open(q_meta_name, 'r') as qs_metadata:

        print("Creating ric_qs dict")
        ric_qs = {}
        for ric in rics:
            if ric in list(qs_metadata['addedby'].keys()):
                ric_qids = include_list(ric)
                ric_qs[ric] = ric_qids
            elif ric.lower() == 'communitech':
                ric_qs[ric] = qs_metadata['which_survey']['COMMUNITECH']
            else:
                ric_qs[ric] = qs_metadata['core/noncore']['core']

    print("Reading qs_metadata.xlsx to df")
    cwd = os.getcwd()
    user_path = os.path.expanduser("~")
    filename = '/qs_metadata.xlsx'
    meta_dfs = CM.xl_to_dfs(cwd, filename)
    sheetname = 'Sheet1'
    meta_df = meta_dfs[sheetname]

    # create master data dict with qid: concatted name (i.e., <survey_section - readable_name>)
    print("Creating master data dict")
    meta_df = meta_df.sort_values(by=['q_num'], ascending=[True])
    meta_df['col_title'] = meta_df['survey_section'].astype(str) + ' - ' + meta_df['readable_name']
    data_dict = meta_df[['id', 'col_title', 'title', 'q_num']]

    # split master data dict into one for each ric
    print("Splitting master data dict into 1 per RIC")
    ric_data_dicts = {}
    for ric in ric_qs.keys():
        qids_df = pd.DataFrame(ric_qs[ric], columns=['id'])
        ric_data_dict = pd.merge(qids_df, data_dict, how='inner', on=['id'])
        ric_data_dict.sort_values(by='q_num', inplace=True)
        ric_data_dicts[ric] = ric_data_dict

    # read questions and options from DB
    print("Reading questions and options from DB into qsos df")
    qsos_sql = CM.get_config("config_sql.ini", "ann_survey_18", "all_qsos")
    qsos = DB.pandas_read(qsos_sql)

    # add col_title column to qsos df
    qsos = pd.merge(qsos, meta_df[['id', 'col_title', 'q_num']],
                    how='left',
                    left_on='qid',
                    right_on='id')
    qsos.drop('id', inplace=True, axis=1)
    print("Transforming qsos df")

    # put flag on 'ESSAY', 'TABLE', 'TEXTBOX', 'MENU', 'RADIO' so that their col_title does not change in next step
    qsos['multi_options'] = qsos.q_type.apply(multi_options)

    # for options, make col_title = col_title + "Option: " + [o_label]
    qsos['col_title'] = qsos.apply(opt_col_title, axis=1)
    qsos = qsos[qsos['q_num'] > 0]

    # capture correct order for columns for use later in formatting pivoted datasheets
    col_title_order = pd.Series(qsos.q_num.values, index=qsos.col_title).to_dict()

    # read answers from DB, clean ans
    print("Reading answers from DB into ans df")
    ans_sql = CM.get_config("config_sql.ini", "ann_survey_18", "sel_ann_survey_res")
    ans = DB.pandas_read(ans_sql)
    print("Cleaning ans df")
    ans.dropna(subset=['Answer'], inplace=True)
    ans['Answer'] = ans.apply(replacements, axis=1)
    ans['page_pipe'] = ans['page_pipe'].fillna('')

    # for each RIC
    print("\nPer RIC df datasheet creation:")
    for ric in ric_qs:

        # turn that RIC's qid list into df
        print("\nRIC: {}".format(ric))
        print("Creating df of questions for {}".format(ric))
        qs_df = pd.DataFrame(ric_qs[ric], columns=['qid'])
        qs_df['ric'] = rics[ric]['db_name']

        # left join that df with qsos df on qid
        qs_df = pd.merge(qs_df, qsos, how='left', on='qid')

        # left join resulting df with ans df
        print("Left join qs with ans")
        ric_survey_results = pd.merge(qs_df, ans,
                                      how='left',
                                      left_on=['qid', 'oid', 'ric'],
                                      right_on=['QuestionID', 'OptionID', 'RIC_Program'])

        # drop empty answers and sort
        print("Clean ans")
        ric_survey_results = ric_survey_results[pd.notnull(ric_survey_results['Answer'])]
        ric_survey_results.sort_values(by='q_num', inplace=True)

        # ric_survey_results.dropna(subset=['Answer'])
        print("Pivot into datasheet for {}".format(ric))
        ric_datasheet = ric_survey_results[['resp_id', 'Company_ID', 'col_title', 'Answer', 'page_pipe']].drop_duplicates()
        ric_datasheet['col_title'] = ric_datasheet['col_title'] + ' ' + ric_datasheet['page_pipe'].astype(str)
        ric_datasheet['rid_cid'] = ric_datasheet['resp_id'].astype(float).astype(str) + '-' + ric_datasheet['Company_ID'].astype(str)
        ric_datasheet = ric_datasheet[['rid_cid', 'col_title', 'Answer']]

        try:
            ric_datasheet = ric_datasheet.pivot(index='rid_cid', columns='col_title', values='Answer')
            # ric_datasheet = pd.pivot_table(ric_datasheet, values='Answer', columns='col_title', index='rid_cid')

            ric_datasheet.reset_index(inplace=True)

            ric_datasheet['resp_id'], ric_datasheet['Company_ID'] = ric_datasheet['rid_cid'].str.split('-', 1).str
            ric_datasheet.drop('rid_cid', axis=1, inplace=True)
            ric_datasheet = ric_datasheet.apply(pd.to_numeric, errors='ignore')

            # remove non-consenting responses
            for val in list(ric_datasheet):
                if 'consent' in str(val.lower()):
                    consent_col = val
                    break
            ric_datasheet[consent_col] = ric_datasheet[consent_col].str.replace(u"\u2019", "'")
            ric_datasheet = ric_datasheet[ric_datasheet[consent_col] != "I don't give consent"]

            # re-order columns to reflect q_num ordering
            cols = list(ric_datasheet)
            rid_cid = cols[-2:]
            q_cols = cols[:-2]
            ordered_q_cols = []
            for q in q_cols:
                if q[-2:] == '.0':
                    ordered_q_cols.append([col_title_order[q[:-8]], q])
                else:
                    ordered_q_cols.append([col_title_order[q.strip()], q])
            ordered_q_cols.sort()
            for i in range(len(ordered_q_cols)):
                ordered_q_cols[i] = ordered_q_cols[i][1]
            cols = rid_cid + ordered_q_cols
            ric_datasheet = ric_datasheet[cols]

            # save to disc
            save_path = path_xl(
                                 user_path=user_path,
                                 path_extension="Box Sync/Workbench/BAP/Annual Survey FY2018/DEV - Results to RICs/",
                                 filename=ric + '.xlsx')
            results_sheets = [ric_datasheet, ric_data_dicts[ric]]
            sheetnames = ['SurveyData', 'DataDictionary']
            save_xls(results_sheets, save_path, sheetnames)
            print("Wrote to {}".format(save_path))
        except ValueError as ex:
            print("!\nERROR FOR {}: {}\n!\n".format(ric, ex))
            continue
        pass


if __name__ == '__main__':
    _main_()
