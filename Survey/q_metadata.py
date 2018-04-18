import shelve
import pandas as pd
import os
from Shared.common import Common as CM


def create_write_shelve(d, name):
    """Store dict in permanent .db file.
    dict, str -> .db file saved to disc
    """
    with shelve.open(name, 'c') as sh:
        for key in d.keys():
            sh[key] = d[key]
    return


def ids_to_list(df, id_col, filter_col, filter_val):
    """Returns list of ids based on filtering logic.
    DataFrame, str, str, str -> list
    """
    id_list = df[id_col][df[filter_col] == filter_val]
    return list(id_list)


def _main_():

    # set pandas viewing options
    desired_width = 320
    pd.set_option('display.width', desired_width)

    # open qs_metadata.xlsx and create df from first sheet
    path = os.getcwd()
    filename = '/qs_metadata.xlsx'
    meta_dfs = CM.xl_to_dfs(path, filename)
    sheetname = 'Sheet1'
    meta_df = meta_dfs[sheetname]

    # identify which columns have relevant metadata in them (might change this get all column names)
    meta_cols = list(meta_df)

    # build dict in format: 'column_name': [filter_values]
    q_meta_dict = {}
    for col in meta_cols:
        q_meta_dict[col] = CM.distinct_from_df(meta_df, col)

    # build dicts in format: 'filter_value': [question_ids]. Nest inside q_meta_dict
    q_meta_shelve = {}
    for key in q_meta_dict.keys():
        if len(q_meta_dict[key]) > 0:
            col_dict = {}
            for filter_val in q_meta_dict[key]:
                id_list = ids_to_list(meta_df, 'id', str(key), filter_val)
                col_dict[filter_val] = id_list
                q_meta_shelve[key] = col_dict

    # create shelve dict
    q_meta_name = 'qs_metadata'
    create_write_shelve(q_meta_shelve, q_meta_name)


if __name__ == '__main__':
    _main_()
