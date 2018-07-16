from Shared.datasource import DataSource
import Shared.enums as enum
# import zipfile
import pandas as pd
# import os


class Census(DataSource):

    def __init__(self):
        super().__init__('','', enum.MDCDataSource.EPP, enum.FilePath.path_statscan_census.value)
        self.stat_col = ['Geography', 'Industry - Nor ', 'Occupation - N', 'Age by 5-year',
                         'Total - Population', 'Total - Sex', 'Male', 'Female']
        self.csv_files = ['EO2908 - Table1.csv', 'EO2908 - Table2.csv', 'EO2908 - Table3.csv', 'EO2908 - Table4.csv']

        self.ontario_cma = ('Kingston','Ottawa', 'Gatineau',  'Peterborough', 'Oshawa',
                        'Toronto', 'Hamilton', 'St. Catharines', 'Niagara',
                        'Kitchener', 'Cambridge', 'Waterloo', 'Brantford',
                        'Guelph', 'London', 'Windsor', 'Barrie', 'Sudbury', 'Thunder Bay')

    def read_zipped_file(self):
        try:
            for file in self.csv_files:
                print(file.upper())
                self.data = pd.read_csv(file, encoding='ISO-8859-1')
                self.data.columns = self.stat_col
                self.data = self.data[1:]
                data_pop = self.data[self.data['Total - Population'] == 'Total - Population 15 years and over']
                data_income = self.data[self.data['Total - Population'] == 'Median employment income ($)']
                data_pop = data_pop.drop(columns=['Total - Population'])
                data_income = data_income.drop(columns=['Total - Population'])
                data_pop = data_pop[data_pop['Geography'].str.startswith(self.ontario_cma)]
                data_income = data_income[data_income['Geography'].str.startswith(self.ontario_cma)]
                self.data = None
                print(data_pop.head(25))
                print(data_income.head(25))
                self.db.save_data_chunk(data_pop, self.enum.SQL.sql_census_population_insert.value, chunk_size=100000)
                self.db.save_data_chunk(data_income, self.enum.SQL.sql_census_median_income_insert.value, chunk_size=100000)
                print('-' * 150)

        except Exception as ex:
            print(ex)


if __name__ == '__main__':
    census = Census()
    census.read_zipped_file()