import plotly as py
import plotly.graph_objs as go
from BAP.bap_quarterly_validation import BAP_Quarterly_Validation as validate


class Validation_Viewer:
    def __init__(self):
        self.test = validate()
        self.results_dict = {
            'Advisory Services': [self.test.advisory_services_test1(), self.test.advisory_services_test2(),
                                  self.test.advisory_services_test3(), self.test.advisory_services_test4(),
                                  self.test.advisory_services_test5(), self.test.advisory_services_test6()],
            'Client Service Activity': [self.test.client_service_activity()],
            'Firm Age': [self.test.firm_age()],
            'Firm Industry': [self.test.firm_industry()],
            'Firm Stage': [self.test.firm_stage()],
            'New Clients Employees': [self.test.new_clients_employees()],
            'New Clients Funding': [self.test.new_clients_funding()],
            'New Clients Revenue': [self.test.new_clients_revenue()],
            'Client Outreach': [self.test.client_outreach_test1(), self.test.client_outreach_test2(),
                                self.test.client_outreach_test3()],
            'Volunteer Mentor Network': [self.test.volunteer_mentor_network_test1(),
                                         self.test.volunteer_mentor_network_test2(),
                                         self.test.volunteer_mentor_network_test3(),
                                         self.test.volunteer_mentor_network_test4(),
                                         self.test.volunteer_mentor_network_test5(),
                                         self.test.volunteer_mentor_network_test6()]
        }

    def plots(self):
        fig = {
            'data': [
                {
                    'name': 'Advisory Services',
                    'labels': ['Clients assisted: Youth advisors > 0', 'Advisory hours: Youth advisors > 0',
                               'Youth advisors: Advisory hours > 0', 'Clients assisted : Advisory hours > 0',
                               'Youth advisors: Clients assisted > 0', 'Advisory hours: Clients assisted > 0'],
                    'values': [1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('Advisory Services'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [0, 0.2],
                               'y': [0.2, 0.4]},
                    'hoverinfo': 'name+text',
                    'hovertext': ['If Number of clients assisted > 0 then Number of advisors assisting youth clients > 0<br>Test: '
                                  + self.results_dict['Advisory Services'][0],
                                  'If Number of advisory hours this quarter > 0 then Number of advisors assisting youth clients > 0<br>Test: '
                                  + self.results_dict['Advisory Services'][1],
                                  'If Number of advisors assisting youth clients > 0 then Number of advisory hours this quarter > 0<br>Test: '
                                  + self.results_dict['Advisory Services'][2],
                                  'If Number of clients assisted > 0 then Number of advisory hours this quarter > 0<br>Test: '
                                  + self.results_dict['Advisory Services'][3],
                                  'If Number of advisors assisting youth clients > 0 then Number of clients assisted > 0<br>Test: '
                                  + self.results_dict['Advisory Services'][4],
                                  'If Number of advisory hours this quarter > 0 then Number of clients assisted > 0<br>Test: '
                                  + self.results_dict['Advisory Services'][5]],
                    'textinfo': 'none'
                },
                {
                    'name': 'Client Service Activity',
                    'labels': ['Number of clients (YTD) vs. Client advisory services'],
                    'values': [1],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('Client Service Activity'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [0.2, 0.4],
                               'y': [0.2, 0.4]},
                    'hoverinfo': 'name+text',
                    'hovertext': ["If it's Q1: Clients receiving advisory services this quarter = total unique "
                                  "clients (YTD)<br>Else Clients receiving advisory services this quarter <  total "
                                  "unique clients (YTD)<br>Test: " + self.results_dict['Client Service Activity'][0]],
                    'textinfo': 'none'
                },
                {
                    'name': 'Firm Age',
                    'labels': ['Number of clients (YTD) vs. Firm age'],
                    'values': [1],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('Firm Age'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [0.4, 0.6],
                               'y': [0.2, 0.4]},
                    'hoverinfo': 'name+text',
                    'hovertext': ['Total firms = total unique clients (YTD)<br>Test: ' + self.results_dict['Firm Age'][0]],
                    'textinfo': 'none'
                },
                {
                    'name': 'Firm Industry',
                    'labels': ['Number of clients (YTD) vs. Firm industry'],
                    'values': [1],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('Firm Industry'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [0.6, 0.8],
                               'y': [0.2, 0.4]},
                    'hoverinfo': 'name+text',
                    'hovertext': ['Total firms = total unique clients (YTD)<br>Test: ' + self.results_dict['Firm Industry'][0]],
                    'textinfo': 'none'
                },
                {
                    'name': 'Firm Stage',
                    'labels': ['Number of clients (YTD) vs. Firm stage'],
                    'values': [1],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('Firm Stage'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [0.8, 1],
                               'y': [0.2, 0.4]},
                    'hoverinfo': 'name+text',
                    'hovertext': ['Total firms = total unique clients (YTD)<br>Test: ' + self.results_dict['Firm Stage'][0]],
                    'textinfo': 'none'
                },
                {
                    'name': 'New Clients Employees',
                    'labels': ['Number of clients (YTD) vs. Client employees'],
                    'values': [1],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('New Clients Employees'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [0, .2],
                               'y': [0.6, 0.8]},
                    'hoverinfo': 'label+name+text',
                    'hovertext': ['Total firms = total unique clients (YTD)<br>Test: ' + self.results_dict['New Clients Employees'][0]],
                    'textinfo': 'none'
                },
                {
                    'name': 'New Clients Funding',
                    'labels': ['Number of clients (YTD) vs. Client funding'],
                    'values': [1],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('New Clients Funding'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [.2, .4],
                               'y': [0.6, 0.8]},
                    'hoverinfo': 'name+text',
                    'hovertext': ['Total firms = total unique clients (YTD)<br>Test: ' + self.results_dict['New Clients Funding'][0]],
                    'textinfo': 'none'
                },
                {
                    'name': 'New Clients Revenue',
                    'labels': ['Number of clients (YTD) vs. Client revenue'],
                    'values': [1],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('New Clients Revenue'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [.4, 0.6],
                               'y': [0.6, 0.8]},
                    'hoverinfo': 'name+text',
                    'hovertext': ['Total firms = total unique clients (YTD)<br>Test: ' + self.results_dict['New Clients Revenue'][0]],
                    'textinfo': 'none'
                },
                {
                    'name': 'Client Outreach',
                    'labels': ['Number of events: Number of event attendees > 0',
                               'Number of events > Events with community partners',
                               'Number of events > Events with  ONE Partners'],
                    'values': [1 / 3, 1 / 3, 1 / 3],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('Client Outreach'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [0.6, 0.8],
                               'y': [0.6, 0.8]},
                    'hoverinfo': 'name+text',
                    'hovertext': ['If Number of events > 0 then Number of event attendees > 0<br>Test: '
                                  + self.results_dict['Client Outreach'][0],
                                  'Number of events >= Number of events co-hosted with community partners<br>Test: '
                                  + self.results_dict['Client Outreach'][1],
                                  'Number of events >= Number of events co-hosted with ONE partners<br>Test: '
                                  + self.results_dict['Client Outreach'][2]],
                    'textinfo': 'none'
                },
                {
                    'name': 'Volunteer Mentor Network',
                    'labels': ['Mentors assisting: Mentor hours > 0', 'Mentor clients: Mentor hours > 0',
                               'Mentor hours: Mentor clients > 0', 'Mentors assisting: Mentor clients > 0',
                               'Mentor hours: Mentors assisting > 0', 'Mentor clients: Mentors assisting > 0'],
                    'values': [1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6, 1 / 6],
                    'type': 'pie',
                    'marker': {'colors': self.colour_generator('Volunteer Mentor Network'),
                               'line': {'color': 'rgb(255,255,255)', 'width': 1}},
                    'domain': {'x': [.8, 1],
                               'y': [0.6, 0.8]},
                    'hoverinfo': 'name+text',
                    'hovertext': ['If Volunteer mentors > 0 then Volunteer hours > 0<br>Test: '
                                  + self.results_dict['Volunteer Mentor Network'][0],
                                  'If Volunteer mentor clients > 0 then Volunteer hours > 0<br>Test: '
                                  + self.results_dict['Volunteer Mentor Network'][1],
                                  'If Volunteer hours > 0 then Volunteer mentor clients > 0<br>Test: '
                                  + self.results_dict['Volunteer Mentor Network'][2],
                                  'If Volunteer mentors assisting clients in the period > 0 then Volunteer mentor clients > 0<br>Test: '
                                  + self.results_dict['Volunteer Mentor Network'][3],
                                  'If Volunteer hours > 0 then Volunteer mentors assisting clients in the period > 0<br>Test: '
                                  + self.results_dict['Volunteer Mentor Network'][4],
                                  'If Volunteer mentor clients > 0 then Volunteer mentors assisting clients in the period > 0<br>Test: '
                                  + self.results_dict['Volunteer Mentor Network'][5]],
                    'textinfo': 'none'
                }
            ],
            'layout': {'title': 'BAP Quarterly QA Test Results<br>' + self.test.ric + '<br>' + self.test.quarter + ' ' +
                                self.test.year, 'showlegend': False, 'annotations': [
                {'text': 'Advisory Services', 'showarrow': False, 'align': 'center', 'x': 0.05, 'y': 0.15},
                {'text': 'Client Service Activity', 'showarrow': False, 'align': 'center', 'x': 0.242, 'y': 0.15},
                {'text': 'Firm Age', 'showarrow': False, 'align': 'center', 'x': .5, 'y': 0.15},
                {'text': 'Firm Industry', 'showarrow': False, 'align': 'center', 'x': .738, 'y': 0.15},
                {'text': 'Firm Stage', 'showarrow': False, 'align': 'center', 'x': .933, 'y': 0.15},
                {'text': 'New Clients Employees', 'showarrow': False, 'align': 'center', 'x': 0.037, 'y': .57},
                {'text': 'New Clients Funding', 'showarrow': False, 'align': 'center', 'x': .242, 'y': .57},
                {'text': 'New Clients Revenue', 'showarrow': False, 'align': 'center', 'x': .5, 'y': .57},
                {'text': 'Client Outreach', 'showarrow': False, 'align': 'center', 'x': .74, 'y': .57},
                {'text': 'Volunteer Mentor Network', 'showarrow': False, 'align': 'center', 'x': .965, 'y': .57}
            ]}
        }
        return fig

    def colour_generator(self, test_name):
        colours = []
        for ind, each in enumerate(self.results_dict[test_name]):
            if each == 'Passed':
                colours.append('#96D38C') # Green
            else:
                colours.append('#F2553A') # Red
        return colours

    def plot_generator(self):
        py.offline.plot(self.plots(), filename='BAP Quarterly QA Test Results.html')
