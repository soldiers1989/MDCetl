from Shared.db import DB
from Shared.common import Common as CM
import pandas as pd
from Shared.file_service import FileService as fileservice
import datetime
import os

desired_width = 520
pd.set_option('display.width', desired_width)


def write_to_xl(df, filename, out_path, sheetname="Sheet1"):

    filename = out_path + filename
    writer = pd.ExcelWriter(filename + ".xlsx", engine='xlsxwriter')
    df.to_excel(writer, sheet_name=sheetname, index=False)  # send df to writer
    writer.save()


def partition_by(df, col_name):
    """ Splits df into multiple dfs, using values in col_name
    # df, str -> dict
    """
    sql = CM.get_config("config_sql.ini", "ann_survey_18", "distinct_RICs")
    split_by = DB.pandas_read(sql)
    split_by = split_by['RIC_Program'].tolist()
    frame_dict = {elem: '' for elem in split_by}

    for key in frame_dict.keys():
        query = '{} == \"{}\"'.format(str(col_name), str(key))
        frame_dict[key] = df.query(query)

    return frame_dict


def spread(df, index, columns, values):
    """Transforms df into wide format
    df, str, str, str -> df
    """
    df = df.pivot(index=index, columns=columns, values=values)
    return df


def _main_():
    print("Getting SQL query")
    sql = CM.get_config("config_sql.ini", "ann_survey_18", "survey_res_by_ric")
    print("SQL: {}".format(sql))
    print("Executing SQL to get dataframe of results")
    all_results = DB.pandas_read(sql)

    print("Creating column names")
    all_results['ConcatQ'] = all_results[['Cap/Rev/Emp', 'Question']].apply(lambda x: ' - '.join(x), axis=1)
    print("Splitting dataframe into one per RIC")
    split_frames = partition_by(all_results, "RIC_Program")
    print("Getting write path")
    user_path = os.path.expanduser("~")
    path = user_path + "/Box Sync/Workbench/BAP/Annual Survey FY2018/Results by RIC/"
    print("Path: {}".format(path))

    print("Writing files to disc:")
    for ric in split_frames.keys():
        x = split_frames[ric]
        x['rid_cid'] = x['resp_id'].astype(str) + '_' + x['Company_ID'].astype(str)
        x = spread(x, 'rid_cid', 'ConcatQ', 'Answer')
        x['rid_cid'] = x.index
        x['_resp_id'], x['_Company_ID'] = x['rid_cid'].str.split('_', 1).str
        x = x.apply(pd.to_numeric, errors='ignore')
        cols = x.columns.tolist()
        cols = cols[-2:] + cols[:-2]
        x = x[cols]
        for i in range(len(cols)):
            if str(cols[i])[0] == '_':
                cols[i] = cols[i][1:]
        x.columns = cols
        x = x.drop('rid_cid', axis=1)
        filename = "{} Survey Results".format(ric)
        write_to_xl(x, filename, path, 'Results')
        print("Wrote {} to path: {}".format(filename, path))


if __name__ == '__main__':
    _main_()
