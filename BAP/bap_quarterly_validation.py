class BAP_Quarterly_Validation:
    def __init__(self):
        self.quarter = 'Q4'
        self.year = '2018'
        self.ric = 'Communitech'

        self.advisory_youth_advisors = 23
        self.advisory_hours = 1429
        self.advisory_clients_assisted = 311

        self.client_advisory_services = 311
        self.total_unique_clients = 1094

        self.total_firm_age = 1094

        self.total_firm_industry = 1094

        self.total_firm_stage = 1094

        self.total_new_employees = 1094

        self.total_new_funding = 1094

        self.total_new_revenue = 1094

        self.event_attendees = 46
        self.events = 2
        self.events_community_partners = 3
        self.events_one_partners = 0

        self.volunteer_hours = 0
        self.volunteer_mentor_clients = 0
        self.volunteer_mentors = 0
        self.volunteer_mentors_assisting = 0

    def advisory_services_test1(self):
        """If Number of clients assisted > 0 then Number of advisors assisting youth clients > 0 """
        if self.advisory_clients_assisted > 0:
            if self.advisory_youth_advisors > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def advisory_services_test2(self):
        """If Number of advisory hours this quarter > 0 then Number of advisors assisting youth clients > 0"""
        if self.advisory_hours > 0:
            if self.advisory_youth_advisors > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def advisory_services_test3(self):
        """If Number of advisors assisting youth clients > 0 then Number of advisory hours this quarter > 0 """
        if self.advisory_youth_advisors > 0:
            if self.advisory_hours > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def advisory_services_test4(self):
        """If Number of clients assisted > 0 then Number of advisory hours this quarter > 0"""
        if self.advisory_clients_assisted > 0:
            if self.advisory_hours > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def advisory_services_test5(self):
        """If Number of advisors assisting youth clients > 0 then Number of clients assisted > 0"""
        if self.advisory_youth_advisors > 0:
            if self.advisory_clients_assisted > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def advisory_services_test6(self):
        """If Number of advisory hours this quarter > 0 then Number of clients assisted > 0"""
        if self.advisory_hours > 0:
            if self.advisory_clients_assisted > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def client_funding(self):
        pass

    def client_service_activity(self):
        """
        If it's Q1: Clients receiving advisory services this quarter = total unique clients (YTD)
        Else Clients receiving advisory services this quarter <  total unique clients (YTD)
        """
        if self.quarter.lower() == 'q1':
            if self.client_advisory_services == self.total_unique_clients:
                return 'Passed'
            else:
                return 'Failed'
        elif self.client_advisory_services < self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def firm_age(self):
        """Total firms = total unique clients (YTD)"""
        if self.total_firm_age == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def firm_industry(self):
        """Total firms = total unique clients (YTD)"""
        if self.total_firm_industry == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def firm_stage(self):
        """Total firms = total unique clients (YTD)"""
        if self.total_firm_stage == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def new_clients_employees(self):
        """Total firms = total unique clients (YTD)"""
        if self.total_new_employees == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def new_clients_funding(self):
        """Total firms = total unique clients (YTD)"""
        if self.total_new_funding == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def new_clients_revenue(self):
        """Total firms = total unique clients (YTD)"""
        if self.total_new_revenue == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def client_outreach_test1(self):
        """If Number of events > 0 then Number of event attendees > 0"""
        if self.events > 0:
            if self.event_attendees > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def client_outreach_test2(self):
        """Number of events >= Number of events co-hosted with community partners"""
        if self.events >= self.events_community_partners:
            return 'Passed'
        else:
            return 'Failed'

    def client_outreach_test3(self):
        """Number of events >= Number of events co-hosted with ONE partners"""
        if self.events >= self.events_one_partners:
            return 'Passed'
        else:
            return 'Failed'

    def service_provider_network(self):
        pass

    def volunteer_mentor_network_test1(self):
        """If Volunteer mentors > 0 then Volunteer hours > 0"""
        if self.volunteer_mentors_assisting > 0:
            if self.volunteer_hours > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def volunteer_mentor_network_test2(self):
        """If Volunteer mentor clients > 0 then Volunteer hours > 0"""
        if self.volunteer_mentor_clients > 0:
            if self.volunteer_hours > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def volunteer_mentor_network_test3(self):
        """If Volunteer hours > 0 then Volunteer mentor clients > 0"""
        if self.volunteer_hours > 0:
            if self.volunteer_mentor_clients > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def volunteer_mentor_network_test4(self):
        """If Volunteer mentors assisting clients in the period > 0 then Volunteer mentor clients > 0"""
        if self.volunteer_mentors_assisting > 0:
            if self.volunteer_mentor_clients > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def volunteer_mentor_network_test5(self):
        """If Volunteer hours > 0 then Volunteer mentors assisting clients in the period > 0"""
        if self.volunteer_hours > 0:
            if self.volunteer_mentors_assisting > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def volunteer_mentor_network_test6(self):
        """If Volunteer mentor clients > 0 then Volunteer mentors assisting clients in the period > 0"""
        if self.volunteer_mentor_clients > 0:
            if self.volunteer_mentors_assisting > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'
