import plotly as py
import plotly.graph_objs as go
from BAP.bap_quarterly_validation import BAP_Quarterly_Validation as validate
from plotly import tools


class Validation_Viewer:
    def __init__(self):
        print(
            '1: NORCAT\n2: WEtech\n3: SSMIC\n4:Communitech\n5: IION\n6: TechAlliance\n'
            '7: MaRS Discovery District\n8: HalTech\n9: RIC Centre\n10: Spark Centre\n11: ventureLAB\n'
            '12: Innovation Factory\n13: Launch Lab\n14: NWOIC\n15: Innovation Guelph\n16: Invest Ottawa\n'
            '17: Innovate Niagara\nall: All RICS')
        while True:
            choice = input('What RIC would you like to see?\n')
            if choice.lower() not in (
                    '1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13', '14', '15', '16', '17', 'all'):
                continue
            else:
                break
        self.test = validate()
        self.test.ric = self.ric_selection(choice)
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

    @staticmethod
    def ric_selection(choice):
        if choice == '1':
            return 'NORCAT'
        elif choice == '2':
            return 'WEtech'
        elif choice == '3':
            return 'SSMIC'
        elif choice == '4':
            return 'Communitech'
        elif choice == '5':
            return 'IION'
        elif choice == '6':
            return 'TechAlliance'
        elif choice == '7':
            return 'MaRS Discovery District'
        elif choice == '8':
            return 'HalTech'
        elif choice == '9':
            return 'RIC Centre'
        elif choice == '10':
            return 'Spark Centre'
        elif choice == '11':
            return 'ventureLAB'
        elif choice == '12':
            return 'Innovation Factory'
        elif choice == '13':
            return 'Launch Lab'
        elif choice == '14':
            return 'NWOIC'
        elif choice == '15':
            return 'Innovation Guelph'
        elif choice == '16':
            return 'Invest Ottawa'
        elif choice == '17':
            return 'Innovate Niagara'
        else:
            return 'ALL'

    def plots(self):
        """Return the test results figure"""
        # Create figure including quarter-to-quarter testing when it's not Q1
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

            ## Bar Graph of ventures by industry
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

        # Figure that displays test results from within the first quarter only
        else:
            # Collect colours for bar chart
            colours = [self.colour_generator('Advisory Services')[0],
                       self.colour_generator('Client Service Activity')[0],
                       self.colour_generator('Firm Age')[0], self.colour_generator('Firm Industry')[0],
                       self.colour_generator('Firm Stage')[0], self.colour_generator('New Clients Employees')[0],
                       self.colour_generator('New Clients Funding')[0], self.colour_generator('New Clients Revenue')[0],
                       self.colour_generator('Client Outreach')[0],
                       self.colour_generator('Volunteer Mentor Network')[0]]

            # Create a progress bar
            trace0 = go.Bar(x=['<b>Progress</b>'], y=[1], name='Advisory Services', opacity=.8,
                            textfont=dict(color='#ffffff'), xaxis='x4', yaxis='y4',textposition='auto',
                            hoverinfo='name', marker=dict(color=colours[0], line=dict(
                    color='#ffffff', width=0.5)))
            trace1 = go.Bar(x=['<b>Progress</b>'], y=[1], hoverinfo='name', name='Client Service Activity',
                            textposition='auto', opacity=.8, textfont=dict(color='#ffffff'),
                            marker=dict(color=colours[1], line=dict(
                                color='#ffffff', width=0.5)))
            trace2 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto',
                            name='Firm Age', opacity=.8,
                            textfont=dict(color='#ffffff'), hoverinfo='name',
                            marker=dict(color=colours[2], line=dict(
                                color='#ffffff', width=1)))
            trace3 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto',
                            name='Firm Industry', opacity=.8,
                            textfont=dict(color='#ffffff'), hoverinfo='name',
                            marker=dict(color=colours[3], line=dict(
                                color='#ffffff', width=0.5)))
            trace4 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto',
                            name='Firm Stage', opacity=.8,
                            textfont=dict(color='#ffffff'), hoverinfo='name',
                            marker=dict(color=colours[4], line=dict(
                                color='#ffffff', width=0.5)))
            trace5 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto',
                            name='New Clients Employees', opacity=.8,
                            textfont=dict(color='#ffffff'), hoverinfo='name',
                            marker=dict(color=colours[5], line=dict(
                                color='#ffffff', width=0.5)))
            trace6 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto',
                            name='New Clients Funding', opacity=.8,
                            textfont=dict(color='#ffffff'), hoverinfo='name',
                            marker=dict(color=colours[6], line=dict(
                                color='#ffffff', width=0.5)))
            trace7 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto',
                            name='New Clients Revenue', opacity=.8,
                            textfont=dict(color='#ffffff'), hoverinfo='name',
                            marker=dict(color=colours[7], line=dict(
                                color='#ffffff', width=0.5)))
            trace8 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto',
                            name='Client Outreach', opacity=.8,
                            textfont=dict(color='#ffffff'), hoverinfo='name',
                            marker=dict(color=colours[8], line=dict(
                                color='#ffffff', width=0.5)))
            trace9 = go.Bar(x=['<b>Progress</b>'], y=[1], textposition='auto',
                            name='Volunteer Mentor Network',
                            opacity=.8, textfont=dict(color='#ffffff'), hoverinfo='name',
                            marker=dict(color=colours[9],
                                        line=dict(color='#ffffff', width=0.5)))
            # Create subplots
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
            # Add all the bar graph traces to the figure
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
                         font=dict(color=colours[0])),

                    dict(text='<b>Client Service Activity</b><br>' + self.result_message('Client Service Activity'),
                         align='left', showarrow=False, x=-0.02,
                         y=.8, xref='paper', yref='paper',
                         font=dict(color=colours[1])),

                    dict(text='<b>Firm Age</b><br>' + self.result_message('Firm Age'), align='left', showarrow=False,
                         x=-0.02, y=.68, xref='paper', yref='paper',
                         font=dict(color=colours[2])),

                    dict(text='<b>Firm Industry</b><br>' + self.results_dict['Firm Industry'][0] + '<br>', align='left',
                         showarrow=False, x=-0.02, y=.56, xref='paper', yref='paper',
                         font=dict(color=colours[3])),

                    dict(text='<b>Firm Stage</b><br>' + self.result_message('Firm Stage'), align='left',
                         showarrow=False, x=-0.02, y=0.48, xref='paper', yref='paper',
                         font=dict(color=colours[4])),

                    dict(text='<b>New Clients Employees</b><br>' + self.result_message('New Clients Employees'),
                         align='left', showarrow=False, x=-0.02,
                         y=.38, xref='paper', yref='paper',
                         font=dict(color=colours[5])),

                    dict(text='<b>New Clients Funding</b><br>' + self.result_message('New Clients Funding'),
                         align='left', showarrow=False, x=-0.02,
                         y=.24, xref='paper', yref='paper',
                         font=dict(color=colours[6])),

                    dict(text='<b>New Clients Revenue</b><br>' + self.result_message('New Clients Revenue'),
                         align='left', showarrow=False, x=-0.02, y=0.14, xref='paper', yref='paper',
                         font=dict(color=colours[7])),

                    dict(text='<b>Client Outreach</b><br>' + self.result_message('Client Outreach'), align='left',
                         showarrow=False, x=-0.02, y=0, xref='paper', yref='paper',
                         font=dict(color=colours[8])),

                    dict(text='<b>Volunteer Mentor Network</b><br>' + self.result_message('Volunteer Mentor Network'),
                         align='left', showarrow=False, x=-0.02,
                         y=-0.1, xref='paper', yref='paper',
                         font=dict(color=colours[9])),

                    dict(text='Current quarter path: ' + self.test.current_quarter_name, showarrow=False, x=1, y=-0.05,
                         xref='paper', yref='paper'),

                    dict(text='Last quarter path: ' + self.test.last_quarter_name, showarrow=False, x=1, y=-0.08,
                         xref='paper', yref='paper')])

        return fig

    def result_message(self, test_name):
        """Generate message to display test results"""
        # Iterate through the test results until 'Failed' or 'Warning' is found
        for ind, each in enumerate(self.results_dict[test_name]):
            if each == 'Passed':
                continue
            else:
                if self.test.ric == 'ALL':
                    # Return the test name + the result of the test with the name of the RIC that's failing the test
                    return self.test.test_dict[test_name + ' ' + str(ind)] + '<br>' + \
                           self.results_dict[test_name][ind]
                else:
                    # Return the test name + the result of the test
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
                    colours.append('#d8ef9f')  # Light green (last quarter)
                    colours.append('#bce458')  # Dark green (current quarter)
                else:
                    colours.append('#ea867b')  # Light red (last quarter)
                    colours.append('#e25e50')  # Dark red (current quarter)
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
            colours.append('#bae255')  # Green always added to the end of the colour list but only used if it's index 0
        return colours

    def plot_generator(self):
        py.offline.plot(self.plots(), filename='BAP Quarterly QA Test Results.html')
