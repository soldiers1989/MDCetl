from Shared.datasource import DataSource
import Shared.enums as enum
import pandas as pd


class VentureDedupe(DataSource):

    def __init__(self):
        super().__init__('box_file_path', 'path_iaf', enum.DataSourceType.IAF)
        self.venture_col = ['ID', 'Name', 'BasicName', 'BatchID', 'DateFounded', 'DateOfIncorporation', 'VentureType', 'Description',
                            'Website', 'Email', 'Phone', 'Fax', 'VentureStatus', 'ModifiedDate', 'CreateDate']

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
        print(tl)

    def inset_new_ventures(self):
        # df = self.db.pandas_read(self.enum.SQL.sql_cvca_exits_new_ventures.value) # CVCA Exits
        # df = self.db.pandas_read(self.enum.SQL.sql_cvca_deals_new_ventures.value)  # CVCA Deals
        # df = self.db.pandas_read(self.enum.SQL.sql_iaf_new_ventures.value)  # IAF
        df = self.db.pandas_read(self.enum.SQL.sql_cb_new_ventures.value)  # CB
        v_id = self.db.get_table_seed('MDCRaw.dbo.Venture', 'ID')
        ven_list = []
        for _, r in df.iterrows():
            value = dict()
            v_id = v_id + 1
            value['ID'] = v_id
            value['Name'] = r[0]
            value['BasicName'] = r[1]
            value['BatchID'] = r[2]
            for i in range(11):
                i = i+4
                value[self.venture_col[i]] = None
            ven_list.append(value)
        dfi = pd.DataFrame(ven_list, columns=self.venture_col)
        print(dfi.head(20))
        values = self.common.df_list(dfi)
        print(values)
        self.db.bulk_insert(self.enum.SQL.sql_venture_insert.value, values)


if __name__ == '__main__':
    vd = VentureDedupe()
    # vd.get_verified_duplicate()
    vd.inset_new_ventures()

#     '''
# UPDATE D SET D.BasicName = S.BasicName
# FROM IAF.IAFSummary S
# INNER JOIN IAF.IAFDetail D ON S.Venture_Name = D.CompanyName'''