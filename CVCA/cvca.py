import Shared.datasource as ds
import Shared.enums as enum
import pandas as pd
import os


class CVCA(ds.DataSource):
    def __init__(self):
        super().__init__('box_file_path', 'path_cvca', enum.DataSourceType.CVCA)

        self.cvca_columns_db = ['CompanyID','investeefirm_name', 'Disclosure', 'deal_province_country', 'Province',
                        'deal_investors', 'AnnounceDate', 'deal_close_date', 'deal_status', 'deal_sector_name',
                        'Deal', 'deal_amount', 'deal_currency', 'deal_exrate', 'Year']

        self.cvca_exit_db = ['CompanyID', 'investeefirm_name', 'deal_title', 'Disclosure', 'deal_province_country',
                             'Province', 'deal_close_date', 'deal_status', 'deal_sector_name',
                             'Deal', 'TotalTotal (Cdn$ mil)', 'Year']

    def read_cvca_file(self):
        self.common.change_working_directory(self.enum.FilePath.path_cvca.value)
        cvls = pd.read_excel('2017 VC and PE data extract_MaRS data Catalyst.xlsx', sheet_name=None)
        for i in range(len(list(cvls.items()))):
            if i == 0:
                df_deals = list(cvls.items())[i][1]
                df_deals = df_deals.drop('TotalTotal (Cdn$ mil)', axis=1)
                df_deals['Deal'] = df_deals.apply(lambda dfs: self.common.get_cvca_deal_types(dfs.deal_type_name), axis=1)
                df_deals['Disclosure'] = df_deals.apply(lambda dfs: self.common.convert_yes_no(dfs.deal_disclosure), axis=1)
                df_deals['Province'] = df_deals.apply(lambda dfs: self.common.convert_province_to_num(dfs.deal_province_name), axis=1)
                df_deals['CompanyID'] = None
                df_deals['Year'] = 2017
                print(df_deals.head())
                df_deals = df_deals.drop(['deal_type_name', 'deal_disclosure'], axis=1)
                df_deals = df_deals[self.cvca_columns_db]
                print(df_deals.head())
                values = self.common.df_list(df_deals)
                self.db.bulk_insert(enum.SQL.sql_cvca_deals.value, values)
                print('CVCA Moved to database.')
            elif i == 1:
                df_exits = list(cvls.items())[i][1]
                df_exits['CompanyID'] = None
                df_exits['Deal'] = df_exits.apply(lambda dfs: self.common.get_cvca_deal_types(dfs.deal_type_name), axis=1)
                df_exits['Disclosure'] = df_exits.apply(lambda dfs: self.common.convert_yes_no(dfs.deal_disclosure), axis=1)
                df_exits['Province'] = df_exits.apply(lambda dfs: self.common.convert_province_to_num(dfs.deal_province_name), axis=1)
                df_exits['Year'] = 2017
                print(df_exits.head())
                df_exits = df_exits.drop(['deal_province_name', 'deal_disclosure', 'deal_type_name'], axis=1)
                df_exits = df_exits[self.cvca_exit_db]
                print(df_exits.head())
                print(list(df_exits.columns))
                values = self.common.df_list(df_exits)
                self.db.bulk_insert(enum.SQL.sql_cvca_exits.value, values)
                print('All done...?')
            else:
                print('No more sheet to process.')

    def cvca_venture_basic_name(self):
        self.data = self.db.pandas_read(self.enum.SQL.sql_cvca_select.value)
        for _, cb in self.data.iterrows():
            sql_update = self.enum.SQL.sql_cvca_update.value.format(self.common.update_cb_basic_name(cb.CompanyName), cb.ID)
            # print(sql_update)
            self.db.execute(sql_update)
            print(cb.CompanyName)

    def get_basic_name(self):
        self.db.update_basic_name(self.enum.SQL.sql_cvca_basic_company.value,
                                  'ID',
                                  'CompanyName',
                                  self.enum.SQL.sql_cvca_basic_company_update.value)

    def cvca_type_qa_result(self):
        self.common.change_working_directory(self.enum.FilePath.path_cvca.value)
        print(os.getcwd())
        dfc = pd.read_excel('UA_QA_Results_CompaniesWrongDealTypes - 2017 VC and PE data extract_MaRS data Catalyst.xlsx', sheet_name='Companies with Incorrect Deals')
        for _, r in dfc.iterrows():
            self.db.execute(self.enum.SQL.sql_cvca_type_update.value.format(r.Deal_Type_ID, r.ID))
            print(self.enum.SQL.sql_cvca_type_update.value.format(r.Deal_Type_ID, r.ID))


if __name__ == '__main__':
    cv = CVCA()
    # cv.read_cvca_file()
    # cv.cvca_venture_basic_name()
    # cv.update_cb_basic_name()
    cv.cvca_type_qa_result()