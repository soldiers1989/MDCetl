from Dashboard.data.Data import ChartData
from Dashboard.data.constants import Category, SQL, Keys
import copy
import pandas as pd
from Shared.common import Common as common
import plotly.graph_objs as go

class Chart:



    def __init__(self):
        self.mapbox_access_token = Keys.mapbox_access_token.value
        self.chart_data = ChartData()
        self.data_frame = self.chart_data.test_data()
        self.df = self.chart_data.bap_data(Category.DataSource)

    def layout(self):
        ly = dict(
            autosize=True,
            height=500,
            font=dict(color='#CCCCCC'),
            titlefont=dict(color='#CCCCCC', size='17'),
            margin=dict(
                l=35,
                r=35,
                b=35,
                t=45
            ),
            hovermode="closest",
            plot_bgcolor="#191A1A",
            paper_bgcolor="#020202",
            legend=dict(font=dict(size=10), orientation='h'),
            title='Satellite Overview',
            mapbox=dict(
                accesstoken=self.mapbox_access_token,
                style="dark",
                center=dict(
                    lon=-78.05,
                    lat=42.54
                ),
                zoom=7,
            )
        )
        return ly

    def map(self, df):
        traces = []
        for city, val in df.iterrows():
            trace = dict(
                trace='scattermapbox',
                lon= val['Long'],
                lat=val['Lat'],
                text=val['Name'],
                customdata=val['PopEstimate2017'],
                name=val['Name'],
                marker=dict(
                    size=5,
                    opacity=0.6,
                    color='skyblue'
                )
            )
            traces.append(trace)
        lon = 79.64
        lat = 43.65
        zoom = 7

        return traces

    def bubble_chart(self, df, x_value, y_value):
        traces = []
        for ric in self.df.Name.unique():
            x = self.df[df['Name'] == ric][x_value].dropna()
            y = self.df[df['Name'] == ric][y_value].dropna()
            trace = go.Scatter(
                x=x,
                y=y,
                text = ric,
                mode='markers',
                opacity=0.7,
                marker={
                    'size': 20,
                    'line': dict(
                        width=0.5,
                        color='red'
                    )
                },
                name= ric
            )
            traces.append(trace)

        return traces

    def scatter_chart(self, mode, graph_name,
                      x_series, y_series, line_shape,
                      smoothing=2, line_width=1, line_color='#fac1b7', line_symbol='diamond', opacity=1):
        graph = dict(
                type='scatter',
                mode=mode,
                name=graph_name,
                x=x_series,
                y=y_series,
                opacity=opacity,
                line=dict(
                    shape=line_shape,
                    smoothing=smoothing,
                    width=line_width,
                    color=line_color
                ),
                marker=dict(symbol=line_symbol)
        )
        return graph

    def bar_graph(self, x, y, graph_name='Graph 1', colors=['rgb(192, 255, 245)']):
        bar = dict(
            type='bar',
            x=x,
            y=y,
            name=graph_name,
            marker=dict(
                color=colors
            ),
        )
        return bar

    def pie_graph(self, labels, values, name, text, hole, domains, colors=['#fac1b7', '#a9bb95', '#92d8d8']):
        pie = dict(
                type='pie',
                labels=labels,
                values=values,
                name=name,
                text=text,
                hoverinfo="text+value+percent",
                textinfo="label+percent+name",
                hole=hole,
                marker=dict(
                    colors=colors
                ),
                domain=domains)
        return pie

    def alpha_graph(self, x_title, y_title, x_value, y_value):

        layout_bubble = copy.deepcopy(self.layout())
        layout_bubble['xaxis'] = dict(type='log', title=x_title)
        layout_bubble['yaxis'] = dict(title=y_title)
        # layout_bubble['legend'] = dict(x=0, y=1)
        layout_bubble['hovermode'] = 'closest'

        figure = dict(data=self.bubble_chart(self.df, x_value, y_value), layout=layout_bubble)

        return figure

    def beta_graph(self):
        layout_beta = copy.deepcopy(self.layout())
        if self.df is None:
            annotation = dict(
                text='No data available',
                x=0.5,
                y=0.5,
                align='center',
                showarrow=False,
                xref='paper',
                yref='paper'
            )
        else:
            x = self.df.Name.unique()
            data = [

                self.scatter_chart('lines', 'Revenue', x,
                                   self.df.REVENUE.dropna(), 'spline', 2, 1, '#fac1b7', 'diamond'),
                self.scatter_chart('lines', 'Number of Employees', x,
                                   self.df.Employees.dropna(), 'spline', 2, 1, '#a9bb95', 'diamond'),
                self.scatter_chart('lines', 'Funding', x,
                                   self.df.FundingTODate.dropna(), 'spline', 2, 1, '#92d8d8', 'diamond')
            ]
        layout_beta['title'] = 'Individual Data'
        figure = dict(data=data, layout=layout_beta)
        return figure

    def gamma_graph(self): # Aggregate
        layout_gamma = copy.deepcopy(self.layout())
        colors = ['skyblue','skyblue']
        # for i in range(1,25):
        #     if i >= 5 and i < 12:
        #         colors.append('rgb(192, 255, 245)')
        #     else:
        #         colors.append('rgba(192, 255, 245, 0.2)')
        data = [
            self.bar_graph(self.data_frame[0], self.data_frame[3], 'MDC Bar Graph', colors='skyblue')
            ]
        layout_gamma['title'] = 'Gamma bar chart'
        layout_gamma['dragmode'] = 'select'
        layout_gamma['showlegend'] = True

        figure= dict(data=data, layout=layout_gamma)
        return figure

    def delta_graph(self): # pie
        layout_delta = copy.deepcopy(self.layout())
        data = [
            self.pie_graph(['Fire', 'Smoke', 'Water'],
                           [sum(self.data_frame[0]), sum(self.data_frame[2]), sum(self.data_frame[4])],
                           'Bap data pie chart',
                           ['Fire Burnt', 'Smoke Inhaled', 'Water Consumed'], 0.5, {"x": [0, .45], 'y': [0.2, 0.8]},
                           ['#fac1b7', '#a9bb95', '#92d8d8']),
            self.pie_graph(['Soccer', 'Football', 'Basketball'],
                           [sum(self.data_frame[0]), sum(self.data_frame[4]), sum(self.data_frame[2])],
                           'Bap data pie chart',
                           ['Soccer Goal', 'Touch Down', 'Tripple Point'], 0.5, {"x": [0.55, 1], 'y':[0.2, 0.8]},
                           ['#caf167', '#a90095', '#2aa45fd'])
        ]
        layout_delta['title'] = 'Bap Quarterly data flow'
        layout_delta['font'] = dict(color='#777777')
        layout_delta['legend'] = dict(
                                font=dict(color='#CCCCCC', size='10'),
                                orientation='h',
                                bgcolor='rgba(0,0,0,0)'
        )
        figure = dict(data=data, layout=layout_delta)
        return figure

    def epsilon_graph(self):
        layout_epsilon = copy.deepcopy(self.layout())
        data = [
            self.scatter_chart('lines', 'Fist name', self.data_frame[0],
                               self.data_frame[1], 'spline', 2, 1, '#fac1b7', 'diamond'),
            self.scatter_chart('lines', 'Forest Gamb', self.data_frame[2],
                               self.data_frame[3], 'spline', 2, 1, '#a9bb95', 'diamond'),
            self.scatter_chart('lines', 'Forest Gamb', self.data_frame[4],
                               self.data_frame[5], 'spline', 2, 1, '#92d8d8', 'diamond')
        ]
        layout_epsilon['title'] = 'Aggregate Production'
        figure = dict(data=data, layout=layout_epsilon)
        return figure
