from Shared.datasource import DataSource
import Shared.enums as enum
import pandas as pd


class VentureDedupe(DataSource):

    def __init__(self):
        super().__init__('box_file_path', 'path_iaf', enum.DataSourceType.IAF)

    def get_verified_duplicate(self):
        self.common.change_working_directory(self.enum.FilePath.path_venture_dedupe.value)
        self.data = pd.read_excel('Duplicate Ventures for QA.xlsx')
        self.data = self.data[self.data.Comment == 'OK to merge']
        for _, dt in self.data.iterrows():
            update = enum.SQL.sql_dbo_duplicate_venture_update.value.format(int(dt.CompanyID))
            self.db.execute(update)
            print('*' * int(dt.CompanyID/10000), update)

    def update_target_list(self):
        tl = self.db.pandas_read('')


if __name__ == '__main__':
    vd = VentureDedupe()
    vd.get_verified_duplicate()