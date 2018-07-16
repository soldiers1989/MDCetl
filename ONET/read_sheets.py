import os
import pandas as pd
from Shared.db import DB
import pyodbc
import sqlalchemy as sa
import shelve

done_file = 'done'
done = ['Content Model Reference.xlsx',
        'Job Zone Reference.xlsx',
        'Occupation Data.xlsx',
        'Scales Reference.xlsx',
        'Education, Training, and Experience Categories.xlsx',
        'Level Scale Anchors.xlsx',
        'Occupation Level Metadata.xlsx',
        'Survey Booklet Locations.xlsx',
        'Task Categories.xlsx',
        'Work Context Categories.xlsx',


        'Abilities.xlsx',
        'Alternate Titles.xlsx',
        'Career Changers Matrix.xlsx',
        'Career Starters Matrix.xlsx',
        'Content Model Reference.xlsx',
        'DWA Reference.xlsx',
        'Education, Training, and Experience.xlsx',
        'Emerging Tasks.xlsx',
        'Green DWA Reference.xlsx',
        'Green Task Statements.xlsx',
        'IWA Reference.xlsx',
        'Interests.xlsx',
        'Job Zones.xlsx',
        'Knowledge.xlsx',
        'Sample of Reported Titles.xlsx',
        'Scales Reference.xlsx',
        'Skills.xlsx',
        'Task Ratings.xlsx',
        'Task Statements.xlsx',
        'Tasks to DWAs.xlsx',
        'Tasks to Green DWAs.xlsx',
        'Tools and Technology.xlsx',
        'UNSPSC Reference.xlsx',
        'Work Activities.xlsx'
        ]

this_one = ['Green Occupations.xlsx']

file_table_map = {
    'Abilities.xlsx': 'abilities',
    'Alternate Titles.xlsx': 'alternate_titles',
    'Career Changers Matrix.xlsx': 'career_changers_matrix',
    'Career Starters Matrix.xlsx': 'career_starters_matrix',
    'Content Model Reference.xlsx': 'content_model_reference',
    'DWA Reference.xlsx': 'dwa_reference',
    'Education, Training, and Experience Categories.xlsx': 'ete_categories',
    'Education, Training, and Experience.xlsx': 'education_training_experience',
    'Emerging Tasks.xlsx': 'emerging_tasks',
    'Green DWA Reference.xlsx': 'green_dwa_reference',
    'Green Occupations.xlsx': 'green_occupations',
    'Green Task Statements.xlsx': 'green_task_statements',
    'IWA Reference.xlsx': 'iwa_reference',
    'Interests.xlsx': 'interests',
    'Job Zone Reference.xlsx': 'job_zone_reference',
    'Job Zones.xlsx': 'job_zones',
    'Knowledge.xlsx': 'knowledge',
    'Level Scale Anchors.xlsx': 'level_scale_anchors',
    'Occupation Data.xlsx': 'occupation_data',
    'Occupation Level Metadata.xlsx': 'occupation_level_metadata',
    'Sample of Reported Titles.xlsx': 'sample_of_reported_titles',
    'Scales Reference.xlsx': 'scales_reference',
    'Skills.xlsx': 'skills',
    'Survey Booklet Locations.xlsx': 'survey_booklet_locations',
    'Task Categories.xlsx': 'task_categories',
    'Task Ratings.xlsx': 'task_ratings',
    'Task Statements.xlsx': 'task_statements',
    'Tasks to DWAs.xlsx': 'tasks_to_dwas',
    'Tasks to Green DWAs.xlsx': 'tasks_to_green_dwas',
    'Tools and Technology.xlsx': 'tools_and_technology',
    'UNSPSC Reference.xlsx': 'unspsc_reference',
    'Work Activities.xlsx': 'work_activities',
    'Work Context Categories.xlsx': 'work_context_categories',
    'Work Context.xlsx': 'work_context',
    'Work Styles.xlsx': 'work_styles',
    'Work Values.xlsx': 'work_values'
}

conn = DB.connect(dev=True)
xl_char_lmt = 31

server = '10.101.2.74'
database = 'MDC_DEV'
username = 'gcree'
password = 'GccSQL1'
driver = '{/usr/local/lib/libmsodbcsql.13.dylib}'
cs = 'DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password


def make_connection():
    cx = pyodbc.connect(cs)
    return cx


def create_write_shelve(d, name):
    """Store dict in permanent .db file.
    dict, str -> .db file saved to disc
    """
    with shelve.open(name, 'c') as sh:
        for key in d.keys():
            sh[key] = d[key]
    return


def _main_():

    conn = sa.create_engine('mssql://', creator=make_connection)
    sheets_dir = os.path.expanduser('~/Box Sync/Innovation Economy/Projects/Employment Pathway - Google.org/Interactive Market Review and Report/EPP_Data collection/ONET Data/db_22_3_excel/')
    os.chdir(sheets_dir)
    file_list = [x for x in os.listdir('.') if x.endswith('.xlsx')]
    file_list.sort()

    for file in file_list:

        if file not in done:
            print('Running {}'.format(file))
            try:
                print("Reading to pandas 'ExcelFile'")
                xl = pd.ExcelFile(file)
                tb_name = 'ONET' + file_table_map[file]
                print('Table name: {}'.format(tb_name))
                # full_tb_name = 'ONET.' + tb_name
                sheetname = file[:-5]
                if len(sheetname) > xl_char_lmt:
                    sheetname = sheetname[:xl_char_lmt]
                print("Sheet name: {}".format(sheetname))
                data = xl.parse(sheetname)
                print("Data extracted from {}, sheet '{}'. Number: {}".format(file, sheetname, len(data)))
                print("writing to DB")
                data.to_sql(tb_name, conn, if_exists='append', index=False)
                print("{} loaded to DB".format(file))
                done.append(file)
            except Exception as e:
                print("ERROR FOR {}".format(file))
                print('Error: {}. Skipped.'.format(e))
                continue
        else:
            print("{} already loaded. Skipping".format(file))


if __name__ == '__main__':
    _main_()
