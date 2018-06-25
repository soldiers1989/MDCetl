from Shared.db import DB as db
from Shared.common import Common as common

class update_etl:
    def __init__(self):
        self.source_table = common.df_list(db.pandas_read('SELECT SourceID, ID, Name FROM MDC_DEV.dbo.SourceTable'))

    def update(self, table_name, source_id_col, company_id_col):
        etl = common.df_list(db.pandas_read('SELECT ' + source_id_col + ',' + company_id_col + ' FROM ' + table_name))
        for index1, val1 in enumerate(self.source_table):
            for index2, val2 in enumerate(etl):
                if val1[0] == str(val2[0]):
                    db.execute('UPDATE ' + table_name + ' SET ' + company_id_col + ' = ' + str(val1[1]) + ' WHERE ' + source_id_col + ' = ' + str(val2[0]))
                    break
