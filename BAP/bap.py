import datetime
import os
import pandas as pd

from Shared.file_service import FileService
from Shared.match import CompanyService
from Shared.common import Common as COM
from Shared.enums import DataSource as DS, WorkSheet as WS, \
    FileName as FN, SQL as sql, FileType, SourceSystemType as ss, Table as tbl, Columns as clm
from Shared.db import DB
from Shared.batch import BatchService


'''
Box Sync/MDC Shared/Innovation Economy/Data/BAP/Quarterly Data/000-BAPQ-ETL/RIC Files
'''


class BapQuarterly:

    year, quarter = COM.fiscal_year_quarter(datetime.datetime.utcnow())
    batch = BatchService()
    file = FileService('Box Sync/mnadew/IE/Data/ETL/BAP')

    @staticmethod
    def show_bap_quarterly():
        response = input('\nPLEASE ENTER THE FOLDER: ')
        files = FileService(response)
        files.show_source_file(FileType.SPREAD_SHEET.value)

    @staticmethod
    def combine_rics_bap_quarterly():
        response = input('\nPLEASE ENTER THE FOLDER: ')
        fl = FileService(response)
        program, program_youth, company_data = fl.read_source_file(FileType.SPREAD_SHEET.value, DS.BAP)
        file_name = FN.bap_combined.value.format(str(BapQuarterly.year)[-2:], BapQuarterly.quarter - 1)
        print('Save spreadsheet file named: {}'.format(file_name))
        path = COM.get_config('config.ini', 'box_file_path', 'path_bap')
        box_path = os.path.join(os.path.expanduser("~"), path)
        os.chdir(box_path)
        writer = pd.ExcelWriter(file_name)

        program.to_excel(writer, WS.bap_program_final.value, index=False)
        program_youth.to_excel(writer, WS.bap_program_youth_final.value, index=False)
        company_data.to_excel(writer, WS.bap_company.value, index=False)
        writer.save()

        print('rics_spreasheet_combined.')

    @staticmethod
    def transfer_csv_program(dataframe):
        val = COM.df_list(dataframe)
        DB.bulk_insert(sql.sql_program_insert, val)

    @staticmethod
    def transfer_csv_program_youth(dataframe):
        val = COM.df_list(dataframe)
        DB.bulk_insert(sql.sql_program_youth_insert, val)

    @staticmethod
    def bulk_insert_company_data(dataframe):
        val = COM.df_list(dataframe)
        DB.bulk_insert(sql.sql_bap_company_insert, val)

    @staticmethod
    def create_bap_batch():
        batch = BatchService()
        program = DB.pandas_read(sql.sql_bap_distict_batch.format(tbl.company_program,
                                                                  BapQuarterly.year,
                                                                  BapQuarterly.quarter))
        program_youth = DB.pandas_read(sql.sql_bap_distict_batch.format(tbl.company_program_youth,
                                                                        BapQuarterly.year,
                                                                        BapQuarterly.quarter))
        company = DB.pandas_read(sql.sql_bap_distict_batch.format(tbl.company_data,
                                                                  BapQuarterly.year,
                                                                  BapQuarterly.quarter))

        batch.create_batch(program, BapQuarterly.year, BapQuarterly.quarter - 1, tbl.company_program)
        batch.create_batch(program_youth, BapQuarterly.year, BapQuarterly.quarter - 1, tbl.company_program_youth)
        batch.create_batch(company, BapQuarterly.year, BapQuarterly.quarter - 1, tbl.company_data)

    @staticmethod
    def transfer_bap_company():
        cs = CompanyService()
        cs.move_company_data(BapQuarterly.year,
                             BapQuarterly.quarter - 1,
                             ss.RICPD_bap,
                             tbl.company_data
                             )

    @staticmethod
    def transfer_fact_ric_company_data():
        batch = BatchService()
        batches = batch.get_batch(BapQuarterly.year, BapQuarterly.quarter, ss.RICCD_bap)
        sq = sql.sql_bap_fact_ric_company_data_source.format(tuple(batches))
        df = DB.pandas_read(sq)
        values_list = COM.df_list(df)
        DB.bulk_insert(sql.sql_bap_fact_ric_company_insert, values_list)

    @staticmethod
    def transfer_fact_ric_aggregation():
        date_id = COM.get_dateid(datevalue=None)
        fact_rig_aggregaton_id = COM.get_table_seed(tbl.fact_ric_aggregation, clm.ric_aggregation_id)
        metric_prg = [130, 132, 133, 129, 134, 63, 77, 60, 68, 67, 135, 136, 137]
        metric_prg_youth = [134, 138]
        batches_prg = BapQuarterly.batch.get_batch(BapQuarterly.year,
                                               BapQuarterly.quarter,
                                               ss.RICPD_bap)

        batches_prg_y = BapQuarterly.batch.get_batch(BapQuarterly.year,
                                                   BapQuarterly.quarter,
                                                   ss.RICPDY_bap)

        df_program = DB.pandas_read(sql.sql_company_aggregate_program.format(tuple(batches_prg)))
        df_program_youth = DB.pandas_read(sql.sql_company_aggregate_program_youth.format(batches_prg_y))

        values = []

        for _, row in df_program.iterrows():
            i = 1
            while i < 13:
                fact_rig_aggregaton_id = fact_rig_aggregaton_id + 1
                m = i - 1
                val = []
                val.append(fact_rig_aggregaton_id)
                val.append(int(row['DataSource']))  # DataSource
                val.append(int(date_id))  # RICDateID
                val.append(int(metric_prg[m]))  # MetricID
                val.append(int(row['BatchID']))  # BatchID

                if str(row[i]) in ['no data', 'n\\a', '-', 'n/a', 'nan']:
                    val.append(-1.0)
                    print(row[i])
                else:
                    val.append(round(float(row[i]), 2))  # AggregateNumber
                val.append(str(datetime.datetime.today())[:23])  # ModifiedDate
                val.append(str(datetime.datetime.today())[:23])  # CreatedDate
                val.append(row['Youth'])  # Youth
                values.append(val)
                i = i + 1

        for _, row in df_program_youth.iterrows():

            i = 1
            while i < 2:
                fact_rig_aggregaton_id = fact_rig_aggregaton_id + 1
                m = i - 1
                val = []
                val.append(fact_rig_aggregaton_id)
                val.append(int(row['DataSource']))  # DataSource
                val.append(int(date_id))  # RICDateID
                val.append(int(metric_prg_youth[m]))  # MetricID
                val.append(int(row['BatchID']))  # BatchID
                if str(row[i]) in ['no data', 'n\\a', '-', 'n/a', 'nan']:
                    val.append(-1.0)
                    print(row[i])
                else:
                    val.append(round(float(row[i]), 2))  # AggregateNumber
                val.append(None)  # ModifiedDate
                val.append(None)  # CreatedDate
                val.append(row['Youth'])  # Youth

                values.append(val)
                i = i + 1

        DB.bulk_insert(tbl.fact_ric_aggregation, values)

    @staticmethod
    def generate_bap_rolled_up():
        i = 0
        df_frcd = DB.pandas_read(sql.sql_bap_fact_ric_company.value.format(BapQuarterly.year))
        print('Number of record to process {} '.format(len(df_frcd)))
        df_fact_ds_quarter = DB.pandas_read(sql.sql_bap_report_company_ds_quarter.value.format(BapQuarterly.year))
        df_FactRICRolledUp = pd.DataFrame(columns=clm.clmn_fact_ric_rolled_up.value)
        df_industry = DB.pandas_read(sql.sql_industry_list_table.value)
        cq = BapQuarterly.quarter - 1

        if not df_frcd.empty:
            for _, row in df_fact_ds_quarter.iterrows():
                company_id = row['CompanyID']
                data_source_id = row['DataSourceID']

                i = i + 1
                print('{}. {}'.format(i, company_id))

                ls_quarters = df_fact_ds_quarter.query('CompanyID == {}'.format(company_id))['MinFQ'].tolist()
                if str(cq) not in ls_quarters:
                    ls_quarters.append(cq)
                df_agg = df_frcd.query('CompanyID == {} & DataSourceID == {}'.format(company_id, data_source_id))

                for quarter in ls_quarters:
                    if quarter == cq:
                        current_quarter = 'FiscalQuarter == \'Q{}\''.format(quarter)
                        previous_quarter = 'FiscalQuarter == \'Q{}\''.format(quarter - 1)
                    elif quarter < cq:
                        current_quarter = 'FiscalQuarter == \'Q{}\''.format(quarter)

                    try:
                        batch_id = df_agg.query(current_quarter)['BatchID'].values[0] if df_agg.query(current_quarter) else None
                        min_date = -1
                        current_date = -1
                        vhs = df_agg.query(current_quarter)['VolunteerMentorHours'].values[0] if df_agg.query(current_quarter) else None
                        adv = df_agg.query(current_quarter)['AdvisoryServicesHours'].values[0] if df_agg.query(current_quarter) else None

                        vhs_agg = df_agg['VolunteerMentorHours'].sum() if quarter == cq else None
                        adv_agg = df_agg['AdvisoryServicesHours'].sum() if quarter == cq else None

                        modified_date = datetime.datetime.utcnow().__str__()[:23]

                        stage = df_agg.query(current_quarter)['Stage'].values[0] if df_agg.query(current_quarter) else df_agg.query(previous_quarter)['Stage'].values[0]
                        industry_sector = df_agg.query(current_quarter)['IndustrySector'].values[0] if df_agg.query(current_quarter) else df_agg.query(previous_quarter)['IndustrySector'].values[0]
                        socialEnterprise = df_agg.query(current_quarter)['SocialEnterprise'].values[0] if df_agg.query(current_quarter) else df_agg.query(previous_quarter)['SocialEnterprise'].values[0]
                        highPotential = df_agg.query(current_quarter)['HighPotential'].values[0] if df_agg.query(current_quarter) else df_agg.query(previous_quarter)['HighPotential'].values[0]
                        youth = df_agg.query(current_quarter)['Youth'].values[0] if df_agg.query(current_quarter) else df_agg.query(previous_quarter)['Youth'].values[0]
                        dateOfIncorporation = df_agg.query(current_quarter)['DateOfIncorporation'].values[0] if df_agg.query(current_quarter) else df_agg.query(previous_quarter)['DateOfIncorporation'].values[0]
                        lvl2_industry_name = df_industry.query('Industry_Sector == {}'.format(industry_sector))
                        annual_revenue = df_agg.query(current_quarter)['AnnualRevenue'].values[0] if df_agg.query(current_quarter) else None
                        funding_current_quarter = df_agg.query(current_quarter)['FundingCurrentQuarter'].values[0] if df_agg.query(current_quarter) else None
                        funding_to_date = df_agg['FundingToDate'].sum() if quarter == cq else None
                        number_of_employees = df_agg.query(current_quarter)['NumberEmployees'].values[0] if df_agg.query(current_quarter) else None
                        intake_date = df_agg.query(current_quarter)['IntakeDate'].values[0]  if df_agg.query(current_quarter) else None
                        dd = {'DataSourceID': data_source_id,
                              'CompanyID': company_id,
                              'MinDate': min_date,
                              'CurrentDate': current_date,
                              'VolunteerYTD': vhs_agg,
                              'AdvisoryHoursYTD': adv_agg,
                              'VolunteerThisQuarter': vhs,
                              'AdvisoryThisQuarter': adv,
                              'FiscalQuarter': quarter,
                              'BatchID': batch_id,
                              'ModifiedDate': modified_date,
                              'SocialEnterprise': socialEnterprise,
                              'Stage': stage,
                              'HighPotential': highPotential,
                              'Lvl2IndustryName': lvl2_industry_name,
                              'FiscalYear': BapQuarterly.year,
                              'Youth': youth,
                              'DateOfIncorporation': dateOfIncorporation,
                              'AnnualRevenue': annual_revenue,
                              'NumberEmployees': number_of_employees,
                              'FundingToDate': funding_current_quarter,
                              'IndustrySector': industry_sector,
                              'IntakeDate': intake_date,
                              'FundingCurrentQuarter': funding_to_date
                              }
                        print(dd.values())
                        df = pd.DataFrame([dd], columns=clm.clmn_fact_ric_rolled_up.value)
                        df_FactRICRolledUp = pd.concat([df_FactRICRolledUp, df])
                    except Exception as ex:
                        print(ex)
            df_FactRICRolledUp = df_FactRICRolledUp[clm.clmn_fact_ric_rolled_up.value]
            BapQuarterly.file.save_as_csv(df_FactRICRolledUp, 'BAP_Rolled_UP_{}.xlsx'.format(str(datetime.date.today())), '/Users/mnadew/Box Sync/mnadew/IE/Data/ETL/BAP')

    @staticmethod
    def generate_bap_report():
        pass

    @staticmethod
    def test_runs():
        df_frcd = DB.pandas_read(sql.sql_bap_fact_ric_company.value.format(BapQuarterly.year))
        print('Number of record to process {} '.format(len(df_frcd)))
        df_fact_ds_quarter = DB.pandas_read(sql.sql_bap_report_company_ds_quarter.value.format(BapQuarterly.year))
        df_quarters = DB.pandas_read(sql.sql_bap_report_all_quarter.value.format(BapQuarterly.year, BapQuarterly.quarter - 1))['FiscalQuarter'].tolist()
        df_FactRICRolledUp = pd.DataFrame(columns=clm.clmn_fact_ric_rolled_up.value)
        print(df_quarters)


if __name__ == '__main__':
    BapQuarterly.generate_bap_rolled_up()