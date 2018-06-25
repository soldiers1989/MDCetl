
class BAP_Quarterly_Validation:
    def __init__(self):
        # self.q1 = path
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

        self.total_new_employees = 94

        self.total_new_funding = 94

        self.total_new_revenue = 94

        self.event_attendees = 46
        self.events = 2
        self.events_community_partners = 3
        self.events_one_partners = 0

        self.volunteer_hours = 0
        self.volunteer_mentor_clients = 3
        self.volunteer_mentors = 0
        self.volunteer_mentors_assisting = 0

    def advisory_services_test1(self):
        if self.advisory_clients_assisted > 0:
            if self.advisory_youth_advisors > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'

    def advisory_services_test2(self):
        if self.advisory_hours > 0:
            if self.advisory_youth_advisors > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'

    def advisory_services_test3(self):
        if self.advisory_youth_advisors > 0:
            if self.advisory_hours > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'

    def advisory_services_test4(self):
        if self.advisory_clients_assisted > 0:
            if self.advisory_hours > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'

    def advisory_services_test5(self):
        if self.advisory_youth_advisors > 0:
            if self.advisory_clients_assisted > 0:
                return 'Passed'
            else:
                return 'Failed'

    def advisory_services_test6(self):
        if self.advisory_hours > 0:
            if self.advisory_clients_assisted > 0:
                return 'Passed'
            else:
                return 'Failed'

    def client_funding(self):
        pass

    def client_service_activity(self):
        if self.quarter == 'q1':
            if self.client_advisory_services == self.total_unique_clients:
                return 'Passed'
            else:
                return 'Failed'

        elif self.client_advisory_services < self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def firm_age(self):
        if self.total_firm_age == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def firm_industry(self):
        if self.total_firm_industry == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def firm_stage(self):
        if self.total_firm_stage == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def new_clients_employees(self):
        if self.total_new_employees == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def new_clients_funding(self):
        if self.total_new_funding == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def new_clients_revenue(self):
        if self.total_new_revenue == self.total_unique_clients:
            return 'Passed'
        else:
            return 'Failed'

    def client_outreach_test1(self):
        if self.events > 0:
            if self.event_attendees > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'

    def client_outreach_test2(self):
        if self.events >= self.events_community_partners:
            return 'Passed'
        else:
            return 'Failed'

    def client_outreach_test3(self):
        if self.events >= self.events_one_partners:
            return 'Passed'
        else:
            return 'Failed'

    def service_provider_network(self):
        pass

    def volunteer_mentor_network_test1(self):
        if self.volunteer_mentors_assisting > 0:
            if self.volunteer_hours > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'

    def volunteer_mentor_network_test2(self):
        if self.volunteer_mentor_clients > 0:
            if self.volunteer_hours > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'

    def volunteer_mentor_network_test3(self):
        if self.volunteer_hours > 0:
            if self.volunteer_mentor_clients > 0:
                return 'Passed'
            else:
                return 'Failed'
        else:
            return 'Passed'

    def volunteer_mentor_network_test4(self):
        if self.volunteer_mentors_assisting > 0:
            if self.volunteer_mentor_clients > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'

    def volunteer_mentor_network_test5(self):
        if self.volunteer_hours > 0:
            if self.volunteer_mentors_assisting > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'

    def volunteer_mentor_network_test6(self):
        if self.volunteer_mentor_clients > 0:
            if self.volunteer_mentors_assisting > 0:
                return 'Passed'
            else:
                return 'Failed'
        else :
            return 'Passed'
