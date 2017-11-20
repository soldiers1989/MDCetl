from Shared.enums import ImportStatus
from Shared.common import Common as CM
from Shared.db import DB as db
import datetime as dt


class BatchService:

    def __init__(self, path):
        self.path = path
        self.sql_batch_count = CM.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_count')
        self.sql_batch_select = CM.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_select')
        self.sql_batch_table = CM.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_table')
        self.sql_batch_delete = CM.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_delete')

    def get_batch(self, year, quarter, source_system):
        select_stmt = CM.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_select')
        sql = select_stmt.format(year, quarter, tuple(source_system))
        batches = self.dal.pandas_read(sql)
        return batches

    def create_batch(self, data_dict, year, quarter, table):

        '''
        <UserId, int,>
        ,<ImportStatusId, int,>
        ,<FILENAME, varchar(300),>
        ,<FullPath, varchar(512),>
        ,<SourceSystemId, int,>
        ,<WorksheetName, varchar(50),>
        ,<FileModifiedDate, datetime,>
        ,<DataSourceID, int,>
        ,<StartDate, datetime,>
        ,<EndDate, datetime,>
        ,<Records, bigint,>
        ,<StgRecords, bigint,>
        ,<DWRecords, bigint,>
        ,<IsDeleted, bit,>
        ,<DateCreated, datetime,>
        ,<DateUpdated, datetime,>
        ,<YEAR, int,>
        ,<QUARTER, varchar(50),>
        '''

        print('creating batches...')
        values = []
        for sheet, df in data_dict:
            for _, row in df.iterrows():

                val.append(0)
                val.append(ImportStatus.Started.value)
                val.append(row['FileName'])
                val.append(row['Path'])
                val.append(row['SourceSystem'])
                val.append(sheet)
                val.append(dt.datetime.now())
                val.append(row['DataSource'])
                val.append(dt.datetime.now())
                val.append(dt.datetime.now())
                val.append(len(df))
                val.append(len(df))
                val.append(len(df))
                val.append(0)
                val.append(dt.datetime.now())
                val.append(dt.datetime.now())
                val.append(year)
                val.append('Q{}'.format(quarter))

                values.append(val)
                val = []
            insert_stmt = CM.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_insert')
            sql = insert_stmt.format(self.import_batch)
            print(sql)
            self.dal.bulk_insert(sql, values)
            table = table + '_batch_created'

    def update_source_batch(self, table, year, quarter, source_system):
        df = self.get_batch(source_system, year, quarter)
        if df is not None:
            print('Updating BatchID for table {}...'.format(table))
            update_stmt = CM.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_update')
            for index, row in df.iterrows():
                sql = update_stmt.format(table, row['BatchID'], source_system)
                self.dal.execute(sql)
        print('Batch updated for table: {}'.format(table))

    def can_push_data(self, table, year, quarter, source_system):
        batch = self.get_batch(year, quarter, source_system)
        sql = CM.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_table')
        if len(batch) > 0:
            sql = sql.format(table, tuple(batch))
            df = self.dal.pandas_read(sql)
            if len(df) > 0:
                return False
            else:
                return True
        else:
            return False

    def rollback_data(self, year, quarter, source_system, table_list):
        batches = self.get_batche(year, quarter, source_system)
        print(batches)
        if len(batches) > 0:
            self.get_tables_stat(batches, table_list)
            print(batches)
            are_you_sure = input(
                'Are you sure you want to rollback all this year and quarter data from the database?\t')
            if are_you_sure in CM.user_response_yes:
                for table in table_list:
                    select_sql = self.sql_batch_count.format(table, batches)
                    delete_sql = self.sql_batch_delete.format(table, batches)
                    df = self.dal.pandas_read(select_sql)
                    print(df.head())
                    self.dal.execute(delete_sql)
                    print('{} deleted.'.format(table))
        else:
            print('No batch was found for the year and quarter specified.')

    def get_tables_stat(self, batches, table_list):
        sql_stm = CM.get_config('sql_statement.ini', 'db_sql_batch', 'sql_batch_count')
        for tbl in table_list:
            sql_stm = sql_stm.format(tbl, batches)
            print('{} : {}'.format(tbl, db.pandas_read(sql_stm)['Total'].values))

    def get_batch_stat(self, year, quarter, source_system, data_source):
        pass

