from Shared.datasource import DataSource
import Shared.enums as enum
# import zipfile
import pandas as pd
from Shared.common import Common
import os


def save_as_excel(dfs, file_name, path_key):
    print(os.getcwd())
    print(len(dfs))
    path = Common.get_config('config.ini', 'box_file_path', path_key)
    box_path = os.path.join(os.path.expanduser("~"), path)
    os.chdir(box_path)
    try:
        writer = pd.ExcelWriter(file_name)
        j = 0
        for df in dfs:
            j += j
            sheet_name = 'SHEET {}'.format(j)
            df.to_excel(writer, sheet_name, index=False)
        writer.save()
    except Exception as ex:
        print(ex)


class Census(DataSource):

    def __init__(self):
        super().__init__('', '', enum.MDCDataSource.EPP, enum.FilePath.path_statscan_census.value)
        self.stat_col = ['Geography', 'Industry - Nor ', 'Occupation - N', 'Age by 5-year',
                         'Total - Population', 'Total - Sex', 'Male', 'Female']
        self.csv_files = [
                            # 'EO2908 - Table1.csv',
                            'EO2908 - Table2.csv',
                            'EO2908 - Table3.csv',
                            'EO2908 - Table4.csv'
        ]

        self.ontario_cma = ('Kingston', 'Ottawa', 'Gatineau',  'Peterborough', 'Oshawa',
                            'Toronto', 'Hamilton', 'St. Catharines', 'Niagara',
                            'Kitchener', 'Cambridge', 'Waterloo', 'Brantford',
                            'Guelph', 'London', 'Windsor', 'Barrie', 'Sudbury', 'Thunder Bay')

    def read_zipped_file(self):
        fail_path_key = 'path_statscan_failed_chunks'
        try:
            for file in self.csv_files:
                print(file.upper())
                print('\tReading file')
                self.data = pd.read_csv(file, encoding='ISO-8859-1')
                print('\tSetting columns')
                self.data.columns = self.stat_col
                print('\tExcluding header row')
                self.data = self.data[1:]
                print('\tFiltering for population data')
                data_pop = self.data[self.data['Total - Population'] == 'Total - Population 15 years and over']
                print('\tFiltering for income data')
                data_income = self.data[self.data['Total - Population'] == 'Median employment income ($)']
                print('\tDropping column from population dataset')
                data_pop = data_pop.drop(columns=['Total - Population'])
                print('\tDropping column from income dataset')
                data_income = data_income.drop(columns=['Total - Population'])
                print('\tFiltering population data to Ontario only')
                data_pop = data_pop[data_pop['Geography'].str.startswith(self.ontario_cma)]
                print('\tFiltering income data to Ontario only')
                data_income = data_income[data_income['Geography'].str.startswith(self.ontario_cma)]
                print('\tVacating superset data variable')
                self.data = None
                print('\tDATA PREVIEWS:')
                print(data_pop.head(25))
                print(data_income.head(25))
                print('\tWriting population data')
                # self.db.save_data_chunk(data_pop, self.enum.SQL.sql_census_population_insert.value, chunk_size=100000, rtrn_msg=True, fail_path_key=fail_path_key)
                self.db.save_data_chunk(data_pop, self.enum.SQL.sql_statscan_test_pop.value, chunk_size=100000, capture_fails=True, fail_path_key=fail_path_key)
                print('\tWriting income data')
                # self.db.save_data_chunk(data_income, self.enum.SQL.sql_census_median_income_insert.value, chunk_size=100000)
                self.db.save_data_chunk(data_income, self.enum.SQL.sql_statscan_test_income.value, chunk_size=100000, capture_fails=True, fail_path_key=fail_path_key)
                print('-' * 150)

        except Exception as ex:
            print(ex)


if __name__ == '__main__':
    census = Census()
    census.read_zipped_file()
