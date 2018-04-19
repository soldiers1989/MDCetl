import pandas as pd
import Shared.datasource as ds
import Shared.enums as enum


class TDW(ds.DataSource):

    def __init__(self):
        super().__init__('', '', datasource=enum.MDCDataSource.TDW)
        self.company_col = ['establishment_name', 'CompanyID', 'legal_name', 'operating_name', 'alternate_names', 'BasicName', 'industry', 'sector',
                            'description', 'dun_and_bradstreet_number', 'full_address_mailing_source', 'street_address_mailing',
                            'neighborhood_mailing', 'city_mailing', 'administrative_area_level_3_mailing',
                            'administrative_area_level_2_mailing', 'administrative_area_level_1_mailing', 'postal_code_mailing',
                            'country_mailing', 'formatted_address_mailing', 'geometry_mailing', 'full_address_location_source',
                            'street_address_location', 'neighborhood_location', 'city_location', 'administrative_area_level_3_location',
                            'administrative_area_level_2_location', 'administrative_area_level_1_location', 'postal_code_location',
                            'country_location', 'formatted_address_location', 'geometry_location', 'telephones', 'faxes', 'website',
                            'email', 'country_of_ownership', 'year_established', 'number_of_employees', 'exporting', 'total_sales',
                            'export_sales', 'primary_industry', 'alternate_industries', 'primary_business_activity', 'aboriginal_firm',
                            'quality_certificates', 'contacts_name', 'contacts_title', 'contacts_area_of_responsibility',
                            'contacts_telephone', 'contacts_extension', 'contacts_fax', 'contacts_email', 'updated_at', 'retrieved_at',
                            'source_url']

    def get_namara_companies(self):
        self.common.change_working_directory(self.enum.FilePath.path_namara.value)
        self.data = pd.read_csv('canadian-companies.csv')
        self.data['CompanyID'] = None
        self.data['BasicName'] = None
        self.data = self.data[self.company_col]
        values = self.common.df_list(self.data)
        for i in range(len(values)):
            val = []
            for j in range(len(values[i])):
                if isinstance(values[i][j], list):
                    val.append(''.join(str(x) for x in values[i][j]))
                elif isinstance(values[i][j], str):
                    val.append(self.common.sql_compliant(values[i][j]))
                else:
                    val.append(values[i][j])
            print('{}...'.format(val[2]))
            sql_insert = enum.SQL.sql_tdw_companies_single_insert.value.format(tuple(val)).replace('"', '\'')
            self.db.execute(sql_insert)

    def get_basic_name(self):
        self.db.update_basic_name(self.enum.SQL.sql_tdw_basic_company.value, 'ID', 'legal_name', self.enum.SQL.sql_tdw_basic_company_update.value)


if __name__ == '__main__':
    tdw = TDW()
    # tdw.get_namara_companies()
    tdw.get_basic_name()