from Shared.db import DB as db
from Shared.common import Common as common
import csv


class BAP_check:
    def __init__(self):
        self.MDCReport = common.df_list(
            db.pandas_read('SELECT RICCompanyDataID, CompanyID,DataSource,BatchID,DateID,AdvisoryServicesHours,'
                           'VolunteerMentorHours, AnnualRevenue, NumberEmployees,FundingToDate, FundingCurrentQuarter, '
                           'HighPotential,SocialEnterprise '
                           'FROM MDCReport.BAPQ.FactRICCompany'))
        self.MaRSDataCatalyst = common.df_list(
            db.pandas_read('SELECT RICCompanyDataID, CompanyID,DataSourceID,BatchID,DateID,AdvisoryServicesHours,'
                           'VolunteerMentorHours, AnnualRevenue, NumberEmployees,FundingToDate, FundingCurrentQuarter, '
                           'HighPotential,SocialEnterprise FROM MaRSDataCatalyst.Reporting.FactRICCompanyData'))
        self.records = []


    def row_match(self, mdcreport_row, mars_row):
        if mdcreport_row == mars_row:
            # print('RIC Aggregation ID: ', mdcreport_row[0], 'cleared')
            return
        else:
            i = 0
            while i < len(mdcreport_row):
                if mdcreport_row[i] != mars_row[i]:
                    self.records.append(mdcreport_row)
                    self.records.append(mars_row)
                    self.records.append(['','','','','','','','','','','',''])
                    return
                i += 1

    def main(self):
        for i, v in enumerate(self.MaRSDataCatalyst):
            if v[11] == 'n' or v[11] == 'N' or v[11] == 'No'or v[11] == 'no' or v[11] == '0':
                v[11] = False
            elif v[11] is None:
                v[11] = None
            elif v[11] == 'y' or v[11] == 'Y' or v[11] == 'Yes' or v[11] == 'yes' or v[11] == '1':
                v[11] = True

        for i, v in enumerate(self.MaRSDataCatalyst):
            if v[12] == 'n' or v[12] == 'N' or v[12] == 'No' or v[12] == 'no' or v[12] == '0':
                v[12] = False
            elif v[12] is None:
                v[12] = None
            elif v[12] == 'y' or v[12] == 'Y' or v[12] == 'Yes' or v[12] == 'yes' or v[12] == '1':
                v[12] = True
            for i, val in enumerate(v):
                if val == '' or val == ' ':
                    v[i] = None

        for i, v in enumerate(self.MDCReport):
            if v[7] != None:
                v[7] = str(int(v[7]))
            if v[8] != None:
                v[8] = str(int(v[8]))
            if v[9] != None:
                v[9] = str(int(v[9]))

        for ind1, row1 in enumerate(self.MDCReport):
            for ind2, row2 in enumerate(self.MaRSDataCatalyst):
                if row1[0] == row2[0]:
                    self.row_match(row1, row2)
                    break
        with open("output.csv", "w",newline="") as f:
            writer = csv.writer(f)
            writer.writerows(self.records)
