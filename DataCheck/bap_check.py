from Shared.db import DB as db
from Shared.common import Common as common


class BAP_check:
    def __init__(self):
        self.MDCReport = common.df_list(
            db.pandas_read('SELECT ID, DataSource,DateID,MetricID,BatchID,AggregateNumber,Youth '
                           'FROM MDCReport.BAPQ.FactRICAggregation'))
        self.MaRSDataCatalyst = common.df_list(
            db.pandas_read('SELECT RICAggregationID, DataSourceID,RICDateID,MetricID,BatchID,AggregateNumber,Youth '
                           'FROM MaRSDataCatalyst.Reporting.FactRICAggregation'))

    @staticmethod
    def row_match(mdcreport_row, mars_row):
        if mdcreport_row == mars_row:
            print('RIC Aggregation ID: ', mdcreport_row[0], 'cleared')
            return
        else:
            i = 0
            while i < len(mdcreport_row):
                if mdcreport_row[i] != mars_row[i]:
                    print('\n', mdcreport_row, '\n', mars_row, '\n')
                    return
                i += 1

    def main(self):
        for i, v in enumerate(self.MaRSDataCatalyst):
            if v[6] == 'Youth':
                v[6] = '1'
            elif v[6] is None:
                v[6] = None
            else:
                v[6] = '2'

        for ind1, row1 in enumerate(self.MDCReport):
            for ind2, row2 in enumerate(self.MaRSDataCatalyst):
                if row1[0] == row2[0]:
                    self.row_match(row1, row2)
                    break
