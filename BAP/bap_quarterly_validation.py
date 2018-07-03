import os

from Shared.common import Common as common


#  # Guide to RICs
# 1: NORCAT
# 2: WEtech
# 3: SSMIC
# 4:Communitech
# 5: IION
# 6: TechAlliance
# 7: MaRS Discovery District
# 8: HalTech
# 9: RIC Centre
# 10: Spark Centre
# 11: ventureLAB
# 12: Innovation Factory
# 13: Launch Lab
# 14: NWOIC
# 15: Innovation Guelph
# 16: Invest Ottawa
# 17: Innovate Niagara

class BAP_Quarterly_Validation:
    def __init__(self):
        self.quarter = 'Q3'
        self.year = '2018'
        self.ric = None

        # Update quarter paths and file names each quarter
        self.first_quarter_xl_path = '/Users/ssimmons/Documents/'
        self.first_quarter_xl_file = 'BAP_Q1_Test.xlsx'
        self.second_quarter_xl_path = '/Users/ssimmons/Documents/'
        self.second_quarter_xl_file = None#'BAP_Q2_Test.xlsx'
        self.third_quarter_xl_path = '/Users/ssimmons/Documents/'
        self.third_quarter_xl_file = None#'BAP_Q3_Test.xlsx'
        self.forth_quarter_xl_path = None#'/Users/ssimmons/Documents/'
        self.forth_quarter_xl_file = None

        self.test_dict = {
            'Advisory Services 0': 'Test: Number of clients assisted > 0 then Number of advisors<br>assisting youth '
                                   'clients > 0',
            'Advisory Services 1': 'Test: If Number of advisory hours this quarter > 0 then Number of<br>advisors '
                                   'assisting youth clients > 0',
            'Advisory Services 2': 'Test: If Number of advisors assisting youth clients > 0 then<br>Number of advisory '
                                   'hours this quarter > 0 ',
            'Advisory Services 3': 'Test: If Number of clients assisted > 0 then Number of advisory hours<br>this'
                                   'quarter > 0',
            'Advisory Services 4': 'Test: If Number of advisors assisting youth clients > 0 then Number<br>of clients'
                                   'assisted > 0',
            'Advisory Services 5': 'Test: If Number of advisory hours this quarter > 0 then Number of clients '
                                   'assisted > 0',
            'Client Service Activity 0': 'Test: Clients receiving advisory services this quarter <= total<br>unique'
                                         'clients (YTD)',
            'Client Service Activity 1': 'Test: Total new clients (QTD) > 0',
            'Client Service Activity 2': 'Clients receiving advisory services this quarter > 0',
            'Firm Age 0': 'Test: Total firms = total unique clients (YTD)',
            'Firm Industry 0': 'Test: Total firms = total unique clients (YTD)',
            'Firm Stage 0': 'Test: Total firms = total unique clients (YTD)',
            'New Clients Employees 0': 'Test: Total firms = total new clients (QTD)',
            'New Clients Funding 0': 'Test: Total firms = total new clients (QTD)',
            'New Clients Revenue 0': 'Test: Total firms = total new clients (QTD)',
            'Client Outreach 0': 'Test: If Number of events > 0 then Number of event attendees > 0',
            'Client Outreach 1': 'Test: Number of events >= Number of events co-hosted<br>with community partners',
            'Client Outreach 2': 'Test: Number of events >= Number of events co-hosted with ONE partners',
            'Client Outreach 3': 'Test: Number of events > 0',
            'Volunteer Mentor Network 0': 'Test: If Volunteer mentors > 0 then Volunteer hours > 0',
            'Volunteer Mentor Network 1': 'Test: If Volunteer mentor clients > 0 then Volunteer hours > 0',
            'Volunteer Mentor Network 2': 'Test: If Volunteer hours > 0 then Volunteer mentor clients > 0',
            'Volunteer Mentor Network 3': 'Test: If Volunteer mentors assisting clients in the period > 0 then<br>'
                                          'Volunteer mentor clients > 0',
            'Volunteer Mentor Network 4': 'Test: If Volunteer hours > 0 then Volunteer mentors assisting<br>clients in '
                                          'the period > 0',
            'Volunteer Mentor Network 5': 'Test: If Volunteer mentor clients > 0 then Volunteer mentors assisting<br>'
                                          'clients in the period > 0'
        }

        self.total_unique_clients = None
        self.total_new_clients = None

        self.current_quarter_df = None
        self.last_quarter_df = None
        self.current_quarter_name = None
        self.last_quarter_name = None

        self.ric_list = ['NORCAT', 'WEtech', 'SSMIC', 'Communitech', 'IION', 'TechAlliance', 'MaRS Discovery District',
                         'HalTech', 'RIC Centre', 'Spark Centre', 'ventureLAB', 'Innovation Factory', 'Launch Lab',
                         'NWOIC', 'Innovation Guelph', 'Invest Ottawa', 'Innovate Niagara']

    def path_finder(self):
        """Set file paths for current and previous quarters"""
        if self.second_quarter_xl_file is not None:
            if self.third_quarter_xl_file is not None:
                if self.forth_quarter_xl_file is not None:
                    self.current_quarter_df = common.xl_to_dfs(self.forth_quarter_xl_path, self.forth_quarter_xl_file)[
                        'DATA']
                    self.last_quarter_df = common.xl_to_dfs(self.third_quarter_xl_path, self.third_quarter_xl_file)[
                        'DATA']
                    self.current_quarter_name = self.forth_quarter_xl_path + self.forth_quarter_xl_file
                    self.last_quarter_name = self.third_quarter_xl_path + self.third_quarter_xl_file
                    self.quarter = 'Q4'
                else:
                    self.current_quarter_df = common.xl_to_dfs(self.third_quarter_xl_path, self.third_quarter_xl_file)[
                        'DATA']
                    self.last_quarter_df = common.xl_to_dfs(self.second_quarter_xl_path, self.second_quarter_xl_file)[
                        'DATA']
                    self.current_quarter_name = self.third_quarter_xl_path + self.third_quarter_xl_file
                    self.last_quarter_name = self.second_quarter_xl_path + self.second_quarter_xl_file
                    self.quarter = 'Q3'
            else:
                self.current_quarter_df = common.xl_to_dfs(self.second_quarter_xl_path, self.second_quarter_xl_file)[
                    'DATA']
                self.last_quarter_df = common.xl_to_dfs(self.first_quarter_xl_path, self.first_quarter_xl_file)['DATA']
                self.current_quarter_name = self.second_quarter_xl_path + self.second_quarter_xl_file
                self.last_quarter_name = self.first_quarter_xl_path + self.first_quarter_xl_file
                self.quarter = 'Q2'
        else:
            self.current_quarter_df = common.xl_to_dfs(self.first_quarter_xl_path, self.first_quarter_xl_file)['DATA']
            self.current_quarter_name = self.first_quarter_xl_path + self.first_quarter_xl_file
            self.quarter = 'Q1'
            self.last_quarter_df = None
            self.last_quarter_name = 'Not Active'

        if self.ric == 'ALL':
            self.total_unique_clients = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total unique clients (YTD)')][
                    ['Value']].sum()['Value']

            self.total_new_clients = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total new clients (QTD)')][
                    ['Value']].sum()['Value']
        else:
            self.total_unique_clients = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total unique clients (YTD)') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']

            self.total_new_clients = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total new clients (QTD)') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']

    def total_clients_rollup_qa(self):
        """Ensure total number of clients does not decrease between quarters"""
        # If current quarter is Q1, no quarter-to-quarter comparisons can be made
        if self.quarter == 'Q1':
            pass
        else:
            if self.ric == 'ALL':
                for ind, each in enumerate(self.ric_list):
                    l_tuc = self.last_quarter_df[(self.last_quarter_df.Friendly == 'Total unique clients(YTD)') &
                                                 (self.last_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                    total_unique_clients = \
                        self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total unique clients (YTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                    if total_unique_clients >= l_tuc:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
                return 'Passed'
            else:
                l_tuc = self.last_quarter_df[(self.last_quarter_df.Friendly == 'Total unique clients(YTD)') &
                                             (self.last_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']

                if self.total_unique_clients >= l_tuc:
                    return 'Passed'
                else:
                    return 'Failed'

    def industry_values(self, industry):
        """Return values for each venture industry"""
        ind_vals = []
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                current_industry = self.current_quarter_df[
                    (self.current_quarter_df['Friendly'].str.contains(industry)) & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                last_industry = self.last_quarter_df[
                    (self.last_quarter_df['Friendly'].str.contains(industry)) & (
                            self.last_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                ind_vals.append(current_industry)
                ind_vals.append(last_industry)
        else:
            current_industry = self.current_quarter_df[
                (self.current_quarter_df['Friendly'].str.contains(industry)) & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            last_industry = self.last_quarter_df[
                (self.last_quarter_df['Friendly'].str.contains(industry)) & (
                        self.last_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            ind_vals.append(current_industry)
            ind_vals.append(last_industry)
        return ind_vals

    def industry_rollup_qa(self):
        """Ensure the number of ventures in each industry does not decrease quarter"""
        # If current quarter is Q1, no quarter-to-quarter comparisons can be made
        if self.quarter == 'Q1':
            pass
        else:
            results = []
            industries = ['Advanced Materials & Manufacturing', 'Agriculture', 'Clean Technologies', 'Digital Media & ICT',
                          'Education', 'Financial Services', 'Food & Beverage', 'Forestry', 'Healthcare', 'Mining', 'Other',
                          'Tourism and Culture']
            if self.ric == 'ALL':
                for i, industry in enumerate(industries):
                    ind_result = self.industry_values(industry)
                    h = 0
                    k = 1
                    while True:
                        if h == len(ind_result):
                            results.append('Passed')
                            break
                        elif ind_result[h] >= ind_result[k]:
                            h += 2
                            k += 2
                            continue
                        else:
                            if h % 2 == 0:
                                results.append(
                                    'Failed. ERROR: ' + self.ric_list[int(h / 2)])# + '<br>SEE: ' + self.current_quarter_name)
                            else:
                                results.append(
                                    'Failed. ERROR: ' + self.ric_list[int(h / 2)])# + '<br>SEE: ' + self.last_quarter_name)
                            break

            else:
                for i, industry in enumerate(industries):
                    if self.industry_values(industry)[0] < self.industry_values(industry)[1]:
                        results.append('Failed')  # , industry: ' + industry
                    else:
                        results.append('Passed')
            return results

    def advisory_services_test1(self):
        """If Number of clients assisted > 0 then Number of advisors assisting youth clients > 0 """
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                clients_assisted = \
                    self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of clients assisted') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                youth_advisors = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of advisors assisting youth clients') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']

                if clients_assisted > 0:
                    if youth_advisors > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'

        else:
            clients_assisted = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of clients assisted') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            youth_advisors = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Number of advisors assisting youth clients') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if clients_assisted > 0:
                if youth_advisors > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def advisory_services_test2(self):
        """If Number of advisory hours this quarter > 0 then Number of advisors assisting youth clients > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                advisory_hours = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of advisory hours this quarter') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                youth_advisors = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of advisors assisting youth clients') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if advisory_hours > 0:
                    if youth_advisors > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'

        else:
            advisory_hours = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Number of advisory hours this quarter') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            youth_advisors = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Number of advisors assisting youth clients') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if advisory_hours > 0:
                if youth_advisors > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def advisory_services_test3(self):
        """If Number of advisors assisting youth clients > 0 then Number of advisory hours this quarter > 0 """
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                youth_advisors = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of advisors assisting youth clients') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                advisory_hours = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of advisory hours this quarter') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if youth_advisors > 0:
                    if advisory_hours > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            youth_advisors = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Number of advisors assisting youth clients') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            advisory_hours = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Number of advisory hours this quarter') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if youth_advisors > 0:
                if advisory_hours > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def advisory_services_test4(self):
        """If Number of clients assisted > 0 then Number of advisory hours this quarter > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                clients_assisted = \
                    self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of clients assisted') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                advisory_hours = \
                    self.current_quarter_df[
                        (self.current_quarter_df.Friendly == 'Number of advisory hours this quarter') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if clients_assisted > 0:
                    if advisory_hours > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            clients_assisted = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of clients assisted') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            advisory_hours = \
                self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of advisory hours this quarter') & (
                            self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if clients_assisted > 0:
                if advisory_hours > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def advisory_services_test5(self):
        """If Number of advisors assisting youth clients > 0 then Number of clients assisted > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                youth_advisors = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of advisors assisting youth clients') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                clients_assisted = \
                    self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of clients assisted') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if youth_advisors > 0:
                    if clients_assisted > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            youth_advisors = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Number of advisors assisting youth clients') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            clients_assisted = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of clients assisted') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if youth_advisors > 0:
                if clients_assisted > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def advisory_services_test6(self):
        """If Number of advisory hours this quarter > 0 then Number of clients assisted > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                advisory_hours = \
                    self.current_quarter_df[
                        (self.current_quarter_df.Friendly == 'Number of advisory hours this quarter') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                clients_assisted = \
                    self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of clients assisted') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if advisory_hours > 0:
                    if clients_assisted > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            advisory_hours = \
                self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of advisory hours this quarter') & (
                            self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            clients_assisted = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of clients assisted') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if advisory_hours > 0:
                if clients_assisted > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def client_funding(self):
        pass

    def client_service_activity_test1(self):
        """
        If it's Q1: Clients receiving advisory services this quarter = total unique clients (YTD)
        Else Clients receiving advisory services this quarter <  total unique clients (YTD)
        """
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                client_advisory_services = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Clients receiving advisory services this quarter') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if self.quarter.lower() == 'q1':
                    if client_advisory_services == self.current_quarter_df[
                        (self.current_quarter_df.Friendly == 'Total unique clients (YTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
                elif client_advisory_services < \
                        self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total unique clients (YTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']:
                    continue
                else:
                    return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            client_advisory_services = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Clients receiving advisory services this quarter') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if self.quarter.lower() == 'q1':
                if client_advisory_services == self.total_unique_clients:
                    return 'Passed'
                else:
                    return 'Failed'
            elif client_advisory_services < self.total_unique_clients:
                return 'Passed'
            else:
                return 'Failed'

    def client_service_activity_test2(self):
        """Total new clients (QTD) > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                new_clients = \
                    self.current_quarter_df[
                        (self.current_quarter_df.Friendly == 'Total new clients (QTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if new_clients > 0:
                    continue
                else:
                    return 'Warning. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            new_clients = \
                self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Total new clients (QTD)') & (
                            self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if new_clients > 0:
                return 'Passed'
            else:
                return 'Warning'

    def client_service_activity_test3(self):
        """TClients receiving advisory services this quarter > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                clients_receiving_service = \
                    self.current_quarter_df[
                        (self.current_quarter_df.Friendly == 'Clients receiving advisory services this quarter') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if clients_receiving_service > 0:
                    continue
                else:
                    return 'Warning. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            clients_receiving_service = \
                self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Clients receiving advisory services this quarter') & (
                            self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if clients_receiving_service> 0:
                return 'Passed'
            else:
                return 'Warning'


    def firm_age(self):
        """Total firms = total unique clients (YTD)"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                total_firm_age = self.current_quarter_df[(self.current_quarter_df.GroupName == 'Firm Age') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if total_firm_age == \
                        self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total unique clients (YTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']:
                    continue
                else:
                    return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            total_firm_age = self.current_quarter_df[(self.current_quarter_df.GroupName == 'Firm Age') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if total_firm_age == self.total_unique_clients:
                return 'Passed'
            else:
                return 'Failed'

    def firm_industry(self):
        """Total firms = total unique clients (YTD)"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                total_firm_industry = self.current_quarter_df[(self.current_quarter_df.GroupName == 'Firm Industry') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if total_firm_industry == \
                        self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total unique clients (YTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']:
                    continue
                else:
                    return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            total_firm_industry = self.current_quarter_df[(self.current_quarter_df.GroupName == 'Firm Industry') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if total_firm_industry == self.total_unique_clients:
                return 'Passed'
            else:
                return 'Failed'

    def firm_stage(self):
        """Total firms = total unique clients (YTD)"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                total_firm_stage = self.current_quarter_df[(self.current_quarter_df.GroupName == 'Firm Stage') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if total_firm_stage == \
                        self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total unique clients (YTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']:
                    continue
                else:
                    return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            total_firm_stage = self.current_quarter_df[(self.current_quarter_df.GroupName == 'Firm Stage') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if total_firm_stage == self.total_unique_clients:
                return 'Passed'
            else:
                return 'Failed'

    def new_clients_employees(self):
        """Total firms = total unique clients (YTD)"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                total_new_employees = \
                    self.current_quarter_df[(self.current_quarter_df.GroupName == 'New Clients Employees') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if total_new_employees == \
                        self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total new clients (QTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']:
                    continue
                else:
                    return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            total_new_employees = \
                self.current_quarter_df[(self.current_quarter_df.GroupName == 'New Clients Employees') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if total_new_employees == self.total_new_clients:
                return 'Passed'
            else:
                return 'Failed'

    def new_clients_funding(self):
        """Total firms = total unique clients (YTD)"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                total_new_funding = \
                    self.current_quarter_df[(self.current_quarter_df.GroupName == 'New Clients Funding') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if total_new_funding == \
                        self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total new clients (QTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']:
                    continue
                else:
                    return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            total_new_funding = self.current_quarter_df[(self.current_quarter_df.GroupName == 'New Clients Funding') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if total_new_funding == self.total_new_clients:
                return 'Passed'
            else:
                return 'Failed'

    def new_clients_revenue(self):
        """Total firms = total unique clients (YTD)"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                total_new_revenue = \
                    self.current_quarter_df[(self.current_quarter_df.GroupName == 'New Clients Revenue') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if total_new_revenue == \
                        self.current_quarter_df[(self.current_quarter_df.Friendly == 'Total new clients (QTD)') & (
                                self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']:
                    continue
                else:
                    return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            total_new_revenue = self.current_quarter_df[(self.current_quarter_df.GroupName == 'New Clients Revenue') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if total_new_revenue == self.total_new_clients:
                return 'Passed'
            else:
                return 'Failed'

    def client_outreach_test1(self):
        """If Number of events > 0 then Number of event attendees > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                number_events = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of events') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                event_attendees = \
                    self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of event attendees') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if number_events > 0:
                    if event_attendees > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            number_events = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of events') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            event_attendees = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of event attendees') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if number_events > 0:
                if event_attendees > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def client_outreach_test2(self):
        """Number of events >= Number of events co-hosted with community partners"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                number_events = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of events') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                community_partners = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of events co-hosted with community partners') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if number_events >= community_partners:
                    continue
                else:
                    return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            number_events = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of events') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            community_partners = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Number of events co-hosted with community partners') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if number_events >= community_partners:
                return 'Passed'
            else:
                return 'Failed'

    def client_outreach_test3(self):
        """Number of events >= Number of events co-hosted with ONE partners"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                number_events = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of events') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                one_partners = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Number of events co-hosted with ONE partners') & (
                            self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
                if number_events >= one_partners:
                    continue
                else:
                    return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            number_events = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of events') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            one_partners = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Number of events co-hosted with ONE partners') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if number_events >= one_partners:
                return 'Passed'
            else:
                return 'Failed'

    def client_outreach_test4(self):
        """Number of events > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                number_events = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of events') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if number_events > 0:
                    continue
                else:
                    return 'Warning. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            number_events = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Number of events') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if number_events > 0:
                return 'Passed'
            else:
                return 'Warning'

    def service_provider_network(self):
        pass

    def volunteer_mentor_network_test1(self):
        """If Volunteer mentors > 0 then Volunteer hours > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                mentors_assisting = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Volunteer mentors') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                volunteer_hours = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Volunteer hours') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']

                if mentors_assisting > 0:
                    if volunteer_hours > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'

        else:
            mentors_assisting = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Volunteer mentors') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            volunteer_hours = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Volunteer hours') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if mentors_assisting > 0:
                if volunteer_hours > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def volunteer_mentor_network_test2(self):
        """If Volunteer mentor clients > 0 then Volunteer hours > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                mentor_clients = \
                    self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer mentor clients') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                volunteer_hours = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer hours') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if mentor_clients > 0:
                    if volunteer_hours > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            mentor_clients = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer mentor clients') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            volunteer_hours = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer hours') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if mentor_clients > 0:
                if volunteer_hours > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def volunteer_mentor_network_test3(self):
        """If Volunteer hours > 0 then Volunteer mentor clients > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                volunteer_hours = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer hours') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                mentor_clients = \
                    self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer mentor clients') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if volunteer_hours > 0:
                    if mentor_clients > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            volunteer_hours = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer hours') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            mentor_clients = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer mentor clients') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if volunteer_hours > 0:
                if mentor_clients > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def volunteer_mentor_network_test4(self):
        """If Volunteer mentors assisting clients in the period > 0 then Volunteer mentor clients > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                mentors_assisting = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Volunteer mentors') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                mentor_clients = \
                    self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer mentor clients') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if mentors_assisting > 0:
                    if mentor_clients > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            mentors_assisting = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Volunteer mentors') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            mentor_clients = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer mentor clients') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if mentors_assisting > 0:
                if mentor_clients > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def volunteer_mentor_network_test5(self):
        """If Volunteer hours > 0 then Volunteer mentors assisting clients in the period > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                volunteer_hours = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer hours') & (
                        self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                mentors_assisting = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Volunteer mentors') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if volunteer_hours > 0:
                    if mentors_assisting > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            volunteer_hours = self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer hours') & (
                    self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            mentors_assisting = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Volunteer mentors') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if volunteer_hours > 0:
                if mentors_assisting > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'

    def volunteer_mentor_network_test6(self):
        """If Volunteer mentor clients > 0 then Volunteer mentors assisting clients in the period > 0"""
        if self.ric == 'ALL':
            for ind, each in enumerate(self.ric_list):
                mentor_clients = \
                    self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer mentor clients') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                mentors_assisting = self.current_quarter_df[
                    (self.current_quarter_df.Friendly == 'Volunteer mentors') & (
                            self.current_quarter_df.RICFriendlyName == each)][['Value']].sum()['Value']
                if mentor_clients > 0:
                    if mentors_assisting > 0:
                        continue
                    else:
                        return 'Failed. ERROR: ' + self.ric_list[ind]
            return 'Passed'
        else:
            mentor_clients = \
                self.current_quarter_df[(self.current_quarter_df.Friendly == 'Volunteer mentor clients') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            mentors_assisting = self.current_quarter_df[
                (self.current_quarter_df.Friendly == 'Volunteer mentors') & (
                        self.current_quarter_df.RICFriendlyName == self.ric)][['Value']].sum()['Value']
            if mentor_clients > 0:
                if mentors_assisting > 0:
                    return 'Passed'
                else:
                    return 'Failed'
            else:
                return 'Passed'
