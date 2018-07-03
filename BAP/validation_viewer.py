import plotly as py
import plotly.graph_objs as go
from BAP.bap_quarterly_validation import BAP_Quarterly_Validation as validate
from plotly import tools


class Validation_Viewer:
    def __init__(self, choice):
        self.test = validate()
        self.test.ric = choice
        self.test.path_finder()
        self.results_dict = {
            'Advisory Services': [self.test.advisory_services_test1(), self.test.advisory_services_test2(),
                                  self.test.advisory_services_test3(), self.test.advisory_services_test4(),
                                  self.test.advisory_services_test5(), self.test.advisory_services_test6()],
            'Client Service Activity': [self.test.client_service_activity_test1(),
                                        self.test.client_service_activity_test2(),
                                        self.test.client_service_activity_test3()],
            'Firm Age': [self.test.firm_age()],
            'Firm Industry': [self.test.firm_industry()],
            'Firm Stage': [self.test.firm_stage()],
            'New Clients Employees': [self.test.new_clients_employees()],
            'New Clients Funding': [self.test.new_clients_funding()],
            'New Clients Revenue': [self.test.new_clients_revenue()],
            'Client Outreach': [self.test.client_outreach_test1(), self.test.client_outreach_test2(),
                                self.test.client_outreach_test3(), self.test.client_outreach_test4()],
            'Volunteer Mentor Network': [self.test.volunteer_mentor_network_test1(),
                                         self.test.volunteer_mentor_network_test2(),
                                         self.test.volunteer_mentor_network_test3(),
                                         self.test.volunteer_mentor_network_test4(),
                                         self.test.volunteer_mentor_network_test5(),
                                         self.test.volunteer_mentor_network_test6()],
            'Industry Roll Up': self.test.industry_rollup_qa(),
            'Total Clients Roll Up': [self.test.total_clients_rollup_qa()]
        }

    def plots(self):
        """Return the test results figure"""
        if self.test.quarter != 'Q1':
            ## Bar Graph of total clients
            current_quarter_clients = self.test.total_unique_clients
            if self.test.ric == 'ALL':
                last_quarter_clients = \
                    self.test.last_quarter_df[(self.test.last_quarter_df.Friendly == 'Total unique clients (YTD)')][
                        ['Value']].sum()['Value']
            else:
                last_quarter_clients = \
                    self.test.last_quarter_df[(self.test.last_quarter_df.Friendly == 'Total unique clients (YTD)') & (
                            self.test.last_quarter_df.RICFriendlyName == self.test.ric)][['Value']].sum()['Value']
            clients_bar = go.Bar(name='Total Unique Clients', x=['Last Quarter', 'Current Quarter'],
                                 y=[last_quarter_clients, current_quarter_clients],
                                 marker=dict(color=self.colour_generator('Total Clients Roll Up')))

            ## Bar Graph of industry numbers
            advanced_materials_manufacturing = self.industry_values('Advanced Materials & Manufacturing')
            agriculture = self.industry_values('Agriculture')
            clean_tech = self.industry_values('Clean Technologies')
            digital_media = self.industry_values('Digital Media & ICT')
            education = self.industry_values('Education')
            financial_services = self.industry_values('Financial Services')
            food_beverage = self.industry_values('Food & Beverage')
            forestry = self.industry_values('Forestry')
            healthcare = self.industry_values('Healthcare')
            mining = self.industry_values('Mining')
            other = self.industry_values('Other')
            tourism_culture = self.industry_values('Tourism and Culture')
            industry_colours = self.colour_generator('Industry Roll Up')

            ind_bar0 = go.Bar(
                name='Last Quarter',
                text=[self.results_dict['Industry Roll Up'][0], self.results_dict['Industry Roll Up'][1],
                      self.results_dict['Industry Roll Up'][2], self.results_dict['Industry Roll Up'][3],
                      self.results_dict['Industry Roll Up'][4], self.results_dict['Industry Roll Up'][5],
                      self.results_dict['Industry Roll Up'][6], self.results_dict['Industry Roll Up'][7],
                      self.results_dict['Industry Roll Up'][8], self.results_dict['Industry Roll Up'][9],
                      self.results_dict['Industry Roll Up'][10], self.results_dict['Industry Roll Up'][11]],
                y=[advanced_materials_manufacturing[1], agriculture[1], clean_tech[1], digital_media[1], education[1],
                   financial_services[1], food_beverage[1], forestry[1], healthcare[1], mining[1], other[1],
                   tourism_culture[1]],
                x=['Advanced Materials & Manufacturing', 'Agriculture', 'Clean Technologies', 'Digital Media & ICT',
                   'Education', 'Financial Services', 'Food & Beverage', 'Forestry', 'Healthcare', 'Mining', 'Other',
                   'Tourism and Culture'],
                marker=dict(color=[industry_colours[0], industry_colours[2], industry_colours[4], industry_colours[6],
                                   industry_colours[8], industry_colours[10], industry_colours[12],
                                   industry_colours[14],
                                   industry_colours[16], industry_colours[18], industry_colours[20],
                                   industry_colours[22]])
            )
            ind_bar1 = go.Bar(
                name='Current Quarter',
                y=[advanced_materials_manufacturing[0], agriculture[0], clean_tech[0], digital_media[0], education[0],
                   financial_services[0], food_beverage[0], forestry[0], healthcare[0], mining[0], other[0],
                   tourism_culture[0]],
                x=['Advanced Materials & Manufacturing', 'Agriculture', 'Clean Technologies', 'Digital Media & ICT',
                   'Education', 'Financial Services', 'Food & Beverage', 'Forestry', 'Healthcare', 'Mining', 'Other',
                   'Tourism and Culture'],
                marker=dict(color=[industry_colours[1], industry_colours[3], industry_colours[5], industry_colours[7],
                                   industry_colours[9], industry_colours[11], industry_colours[13],
                                   industry_colours[15],
                                   industry_colours[17], industry_colours[19], industry_colours[21],
                                   industry_colours[23]])
            )
            # Make subplots
            fig = tools.make_subplots(rows=12, cols=3,
                                      specs=[[{}, {}, {}],
                                             [{}, {'rowspan': 4, 'colspan': 2}, {}],
                                             [{}, {}, {}],
                                             [{}, {}, {}],
                                             [{}, {}, {}],
                                             [{}, {}, {}],
                                             [{}, {'rowspan': 4, 'colspan': 2}, {}],
                                             [{}, {}, {}],
                                             [{}, {}, {}],
                                             [{}, {}, {}],
                                             [{}, {}, {}],
                                             [{}, {}, {}]])

            fig.append_trace(clients_bar, 2, 2)
            fig.append_trace(ind_bar0, 7, 2)
            fig.append_trace(ind_bar1, 7, 2)

            # Update layout
            fig['layout'].update(
                title='BAP Quarterly QA Test Results<br>' + self.test.ric + '<br>' + self.test.quarter + ' ' + self.test.year,
                showlegend=False, annotations=[
                    dict(text='<b>Current Quarter Tests</b>', showarrow=False, x=-0.02, y=1, xref='paper', yref='paper',
                         font=dict(size=16)),

                    dict(text='<b>Total Unique Clients (YTD)</b>', showarrow=False, x=.79,
                         y=.97, xref='paper', yref='paper', font=dict(size=16)),

                    dict(text='<b>Advisory Services</b><br>' + self.result_message('Advisory Services'), align='left',
                         showarrow=False, x=-0.02, y=.94, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Advisory Services')[0])),

                    dict(text='<b>Client Service Activity</b><br>' + self.result_message('Client Service Activity'),
                         align='left', showarrow=False, x=-0.02,
                         y=.8, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Client Service Activity')[0])),

                    dict(text='<b>Firm Age</b><br>' + self.result_message('Firm Age'), align='left', showarrow=False,
                         x=-0.02, y=.68, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Firm Age')[0])),

                    dict(text='<b>Firm Industry</b><br>' + self.results_dict['Firm Industry'][0] + '<br>', align='left',
                         showarrow=False, x=-0.02, y=.56, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Firm Industry')[0])),

                    dict(text='<b>Firm Stage</b><br>' + self.result_message('Firm Stage'), align='left',
                         showarrow=False, x=-0.02, y=0.48, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Firm Stage')[0])),

                    dict(text='<b>Number of Companies by Industry (YTD)</b>', showarrow=False, x=.82,
                         y=.5, xref='paper', yref='paper', font=dict(size=16)),

                    dict(text='<b>New Clients Employees</b><br>' + self.result_message('New Clients Employees'),
                         align='left', showarrow=False, x=-0.02,
                         y=.38, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('New Clients Employees')[0])),

                    dict(text='<b>New Clients Funding</b><br>' + self.result_message('New Clients Funding'),
                         align='left', showarrow=False, x=-0.02,
                         y=.24, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('New Clients Funding')[0])),

                    dict(text='<b>New Clients Revenue</b><br>' + self.result_message('New Clients Revenue'),
                         align='left', showarrow=False, x=-0.02, y=0.14, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('New Clients Revenue')[0])),

                    dict(text='<b>Client Outreach</b><br>' + self.result_message('Client Outreach'), align='left',
                         showarrow=False, x=-0.02, y=0, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Client Outreach')[0])),

                    dict(text='<b>Volunteer Mentor Network</b><br>' + self.result_message('Volunteer Mentor Network'),
                         align='left', showarrow=False, x=-0.02,
                         y=-0.1, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Volunteer Mentor Network')[0])),

                    dict(text='Current quarter path: ' + self.test.current_quarter_name, showarrow=False, x=1, y=-0.05,
                         xref='paper', yref='paper'),

                    dict(text='Last quarter path: ' + self.test.last_quarter_name, showarrow=False, x=1, y=-0.08,
                         xref='paper', yref='paper')])
        # Quarter 1
        else:

            colours = [self.colour_generator('Advisory Services')[0],
                       self.colour_generator('Client Service Activity')[0],
                       self.colour_generator('Firm Age')[0], self.colour_generator('Firm Industry')[0],
                       self.colour_generator('Firm Stage')[0], self.colour_generator('New Clients Employees')[0],
                       self.colour_generator('New Clients Funding')[0], self.colour_generator('New Clients Revenue')[0],
                       self.colour_generator('Client Outreach')[0],
                       self.colour_generator('Volunteer Mentor Network')[0]]

            trace0 = go.Bar(x=['<b>Progress</b>'], y=[1], text='Advisory Services', textposition='auto', opacity=.8,
                            textfont=dict(color='#ffffff'), xaxis='x4',yaxis='y4', marker=dict(color=colours[0],line=dict(
                                                        color='#ffffff', width=1)))
            trace1 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto', text='Client Service Activity',opacity=.8,
                            textfont=dict(color='#ffffff'), marker=dict(color=colours[1],
                                                                        line=dict(
                                                                            color='#ffffff', width=1)))
            trace2 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto', text='Firm Age',opacity=.8,
                            textfont=dict(color='#ffffff'), marker=dict(color=colours[2], line=dict(
                    color='#ffffff', width=1)))
            trace3 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto', text='Firm Industry',opacity=.8,
                            textfont=dict(color='#ffffff'), marker=dict(color=colours[3], line=dict(
                    color='#ffffff', width=1)))
            trace4 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto', text='Firm Stage',opacity=.8,
                            textfont=dict(color='#ffffff'), marker=dict(color=colours[4], line=dict(
                    color='#ffffff', width=1)))
            trace5 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto', text='New Clients Employees',opacity=.8,
                            textfont=dict(color='#ffffff'), marker=dict(color=colours[5],
                                                                        line=dict(
                                                                            color='#ffffff', width=1)))
            trace6 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto', text='New Clients Funding',opacity=.8,
                            textfont=dict(color='#ffffff'), marker=dict(color=colours[6],
                                                                        line=dict(
                                                                            color='#ffffff', width=1)))
            trace7 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto', text='New Clients Revenue',opacity=.8,
                            textfont=dict(color='#ffffff'), marker=dict(color=colours[7],
                                                                        line=dict(
                                                                            color='#ffffff', width=1)))
            trace8 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto', text='Client Outreach',opacity=.8,
                            textfont=dict(color='#ffffff'), marker=dict(color=colours[8],
                                                                        line=dict(
                                                                            color='#ffffff', width=1)))
            trace9 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto', text='Volunteer Mentor Network',opacity=.8,
                            textfont=dict(color='#ffffff'), marker=dict(color=colours[9]))

            fig = tools.make_subplots(rows=12, cols=2,
                                      specs=[[{}, {}],
                                             [{}, {'rowspan': 10}],
                                             [{}, {}],
                                             [{}, {}],
                                             [{}, {}],
                                             [{}, {}],
                                             [{}, {}],
                                             [{}, {}],
                                             [{}, {}],
                                             [{}, {}],
                                             [{}, {}],
                                             [{}, {}]])
            fig.append_trace(trace9, 2, 2)
            fig.append_trace(trace8, 2, 2)
            fig.append_trace(trace7, 2, 2)
            fig.append_trace(trace6, 2, 2)
            fig.append_trace(trace5, 2, 2)
            fig.append_trace(trace4, 2, 2)
            fig.append_trace(trace3, 2, 2)
            fig.append_trace(trace2, 2, 2)
            fig.append_trace(trace1, 2, 2)
            fig.append_trace(trace0, 2, 2)

            # Update layout
            fig['layout'].update(
                title='BAP Quarterly QA Test Results<br>' + self.test.ric + '<br>' + self.test.quarter + ' ' + self.test.year,
                showlegend=False, barmode='stack',
                yaxis4=dict(showgrid=False,
                            showline=False,
                            showticklabels=False,
                            zeroline=False, anchor='y4'),
                annotations=[
                    dict(text='<b>Current Quarter Tests</b>', showarrow=False, x=-0.02, y=1, xref='paper', yref='paper',
                         font=dict(size=16)),

                    dict(text='<b>Advisory Services</b><br>' + self.result_message('Advisory Services'), align='left',
                         showarrow=False, x=-0.02, y=.94, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Advisory Services')[0])),

                    dict(text='<b>Client Service Activity</b><br>' + self.result_message('Client Service Activity'),
                         align='left', showarrow=False, x=-0.02,
                         y=.8, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Client Service Activity')[0])),

                    dict(text='<b>Firm Age</b><br>' + self.result_message('Firm Age'), align='left', showarrow=False,
                         x=-0.02, y=.68, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Firm Age')[0])),

                    dict(text='<b>Firm Industry</b><br>' + self.results_dict['Firm Industry'][0] + '<br>', align='left',
                         showarrow=False, x=-0.02, y=.56, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Firm Industry')[0])),

                    dict(text='<b>Firm Stage</b><br>' + self.result_message('Firm Stage'), align='left',
                         showarrow=False, x=-0.02, y=0.48, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Firm Stage')[0])),

                    dict(text='<b>New Clients Employees</b><br>' + self.result_message('New Clients Employees'),
                         align='left', showarrow=False, x=-0.02,
                         y=.38, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('New Clients Employees')[0])),

                    dict(text='<b>New Clients Funding</b><br>' + self.result_message('New Clients Funding'),
                         align='left', showarrow=False, x=-0.02,
                         y=.24, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('New Clients Funding')[0])),

                    dict(text='<b>New Clients Revenue</b><br>' + self.result_message('New Clients Revenue'),
                         align='left', showarrow=False, x=-0.02, y=0.14, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('New Clients Revenue')[0])),

                    dict(text='<b>Client Outreach</b><br>' + self.result_message('Client Outreach'), align='left',
                         showarrow=False, x=-0.02, y=0, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Client Outreach')[0])),

                    dict(text='<b>Volunteer Mentor Network</b><br>' + self.result_message('Volunteer Mentor Network'),
                         align='left', showarrow=False, x=-0.02,
                         y=-0.1, xref='paper', yref='paper',
                         font=dict(color=self.colour_generator('Volunteer Mentor Network')[0])),

                    dict(text='Current quarter path: ' + self.test.current_quarter_name, showarrow=False, x=1, y=-0.05,
                         xref='paper', yref='paper'),

                    dict(text='Last quarter path: ' + self.test.last_quarter_name, showarrow=False, x=1, y=-0.08,
                         xref='paper', yref='paper')])


        return fig

    def result_message(self, test_name):
        """Generate message to display test results"""
        for ind, each in enumerate(self.results_dict[test_name]):
            if each == 'Passed':
                continue
            else:
                if self.test.ric == 'ALL':
                    return self.test.test_dict[test_name + ' ' + str(ind)] + '<br>' + \
                           self.results_dict[test_name][ind]
                else:
                    if 'Warning' in self.results_dict[test_name][ind]:
                        return 'Warning: ' + self.test.test_dict[test_name + ' ' + str(ind)]
                    else:
                        return 'Failed: ' + self.test.test_dict[test_name + ' ' + str(ind)]
        return 'Passed'

    def industry_values(self, industry):
        """Return values for each venture industry to display in graph"""
        if self.test.ric == 'ALL':
            current_industry = self.test.current_quarter_df[
                (self.test.current_quarter_df['Friendly'].str.contains(industry, na=False))][['Value']].sum()['Value']
            last_industry = self.test.last_quarter_df[
                (self.test.last_quarter_df['Friendly'].str.contains(industry, na=False))][['Value']].sum()['Value']
        else:
            current_industry = self.test.current_quarter_df[
                (self.test.current_quarter_df['Friendly'].str.contains(industry)) & (
                        self.test.current_quarter_df.RICFriendlyName == self.test.ric)][['Value']].sum()['Value']
            last_industry = self.test.last_quarter_df[
                (self.test.last_quarter_df['Friendly'].str.contains(industry)) & (
                        self.test.last_quarter_df.RICFriendlyName == self.test.ric)][['Value']].sum()['Value']
        return [current_industry, last_industry]

    def colour_generator(self, test_name):
        """Generate list of colours for test results display"""
        colours = []
        # Last quarter-current quarter graph Display
        if test_name == 'Industry Roll Up' or test_name == 'Total Clients Roll Up':
            for ind, each in enumerate(self.results_dict[test_name]):
                if each == 'Passed':
                    colours.append('#d8ef9f')  # Light green
                    colours.append('#bce458')  # Dark green
                else:
                    colours.append('#ea867b')  # Light red
                    colours.append('#e25e50')  # Dark red
        else:
            # Current quarter test results
            for ind, each in enumerate(self.results_dict[test_name]):
                if 'Failed' in each:
                    colours.append('#de4635')  # Red
                    break
                elif 'Warning' in each:
                    colours.append('#ffcc00')  # Orange
                else:
                    continue
            colours.append('#bae255')  # Green
        return colours

    def plot_generator(self):
        py.offline.plot(self.plots(), filename='BAP Quarterly QA Test Results.html')
