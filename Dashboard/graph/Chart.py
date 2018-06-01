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
		self.dfRIC = self.chart_data.bap_data(Category.DataSource)
		self.dfStage = self.chart_data.bap_data(Category.Stage)

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

	def table(self, df):
		min_value=0
		max_value=200000
		rows = []
		for i in range(len(df)):
			row = []
			for col in df.columns:
				value = df.iloc[i][col]
				style = self.cell_style(value,min_value, max_value)
				row.append(html.Td(value, style=style))
			rows.append(html.Tr(row))
		table = html.Table([html.Tr([html.Th(col) for col in df.columns])] + rows)
		return table

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
		for ric in self.dfRIC.Name.unique():
			# dx = pd.Series(list(self.df.groupby(['Name'])[x_value].agg('sum')))
			# dy = pd.Series(list(self.df.groupby(['Name'])[y_value].agg('sum')))
			x = df[df['Name'] == ric][x_value].dropna()#pd.Series(df.Name.unique())#
			y = df[df['Name'] == ric][y_value].dropna()
			trace = go.Scatter(
				x=x,
				y=y,
				text = ric,
				mode='markers',
				opacity=0.5,
				showlegend=False,
				marker={
					'size': 20,
					'line': dict(
						width=0.5,
						color='skyblue'
					)
				},
				name= ric
			)
			traces.append(trace)
		return traces

	def scatter_chart(self, mode, graph_name,x_series, y_series, line_shape, smoothing=2, line_width=1, line_color='#fac1b7', line_symbol='diamond', opacity=1):
		graph = dict(
				type='scatter',
				mode=mode,
				name=graph_name,
				x=x_series,
				y=y_series,
				opacity=opacity,
				showlegend=False,
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
				textinfo="none",
				hole=hole,
				showlegend=True,
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
		layout_bubble['title'] = 'Revenue Vs Funding Rasided by RIC'
		layout_bubble['margin'] = dict(l=65,r=35,b=35,t=45)
		dff = self.dfRIC[self.dfRIC['FundingTODate'] < 10000000]

		figure = dict(data=self.bubble_chart(dff, x_value, y_value), layout=layout_bubble)

		return figure

	def beta_graph(self):
		layout_beta = copy.deepcopy(self.layout())
		if self.dfRIC is None:
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
			x = self.dfRIC.Name.unique()
			data = [

				self.scatter_chart('lines+markers', 'Revenue', x,
								   self.dfRIC.FundingTODate.dropna(), 'spline', 2, 1, '#fac1b7', 'diamond')
			]
		layout_beta['title'] = 'FUNDING'
		layout_beta['title'] = 'FUNDING'
		figure = dict(data=data, layout=layout_beta)
		return figure

	def gamma_graph(self): # Aggregate
		layout_gamma = copy.deepcopy(self.layout())
		colors = ['skyblue','skyblue']

		data = [self.bar_graph(self.dfRIC.Name.unique(),
							   self.dfRIC.REVENUE.dropna(),# self.dfRIC[self.dfRIC.REVENUE < 40000000]['REVENUE'],
							   'All RICS Revenue',
							   colors='skyblue')]
		layout_gamma['title'] = 'Revenues by RIC'
		layout_gamma['dragmode'] = 'select'
		layout_gamma['showlegend'] = False

		figure= dict(data=data, layout=layout_gamma)
		return figure

	def delta_graph(self): # pie
		layout_delta = copy.deepcopy(self.layout())
		data = [
			self.pie_graph(list(self.dfRIC.Name.unique()),
						   list(self.dfRIC.groupby(['Name'])['FundingTODate'].agg('sum')),
						   'Revenue Distribution',
						   list(self.dfRIC.Name.unique()), 0.5, {"x": [1.1, 1.55], 'y': [1.2, 0.8]},  #{"x": [0, .45], 'y': [0.2, 0.8]},
						   ['#fac1b7', '#a9bb95', '#92d8d8']),
			# self.pie_graph(list(self.df.Name.unique()),
			# 			   list(self.df.groupby(['Name'])['Employees'].agg('sum')),
			# 			   'Number of Employees Distribution',list(self.df.Name.unique())
			# 			   , 0.5, {"x": [0.55, 1], 'y':[0.2, 0.8]},
			# 			   ['#caf167', '#a90095', '#2aa45fd']),
			# self.pie_graph(list(self.df.Name.unique()),
			#                list(self.df.groupby(['Name'])['AdvisoryHours'].agg('sum')),
			#                'Advisory Hours', list(self.df.Name.unique())
			#                , 0.5, {"x": [1.1, 1.55], 'y': [1.2, 0.8]},
			#                ['#caf167', '#a90095', '#2aa45fd'])
		]
		layout_delta['title'] = 'Funding to date by RICs'
		layout_delta['font'] = dict(color='#777777')
		layout_delta['showlegend'] = False
		layout_delta['legend'] = dict(
								font=dict(color='#CCCCCC', size='10'),
								orientation='h',
								bgcolor='rgba(0,0,0,0)')
		figure = dict(data=data, layout=layout_delta)
		return figure

	def epsilon_graph(self):
		layout_epsilon = copy.deepcopy(self.layout())
		data = [
			self.scatter_chart('lines+markers', 'Funding', self.dfRIC.Name.unique(),
							   self.dfRIC.REVENUE.dropna(), 'spline', 2, 1, '#92d8d8', 'diamond')
		]
		layout_epsilon['title'] = 'REVENUE'
		figure = dict(data=data, layout=layout_epsilon)
		return figure

	def zeta_graph(self):  # Aggregate
		layout_zeta = copy.deepcopy(self.layout())
		colors = ['skyblue', 'skyblue']

		data = [self.bar_graph(self.dfStage.Name.unique(),
							   self.dfStage.REVENUE.dropna(),
							   'All RICS Revenue',
							   colors='skyblue')]
		layout_zeta['title'] = 'Gamma bar chart'
		layout_zeta['dragmode'] = 'select'
		layout_zeta['showlegend'] = False

		figure = dict(data=data, layout=layout_zeta)
		return figure

	def eta_graph(self):  # pie
		layout_eta = copy.deepcopy(self.layout())
		data = [
			self.pie_graph(list(self.dfStage.Name.unique()),
						   list(self.dfStage.groupby(['Name'])['REVENUE'].agg('sum')),
						   'Revenue Distribution',
						   list(self.dfRIC.Name.unique()), 0.5, {"x": [1.1, 1.55], 'y': [1.2, 0.8]},
						   # {"x": [0, .45], 'y': [0.2, 0.8]},
						   ['#fac1b7', '#a9bb95', '#92d8d8']),
		]
		layout_eta['title'] = 'Revenue and Employment by RICs'
		layout_eta['font'] = dict(color='#777777')
		layout_eta['showlegend'] = False
		layout_eta['legend'] = dict(
			font=dict(color='#CCCCCC', size='10'),
			orientation='h',
			bgcolor='rgba(0,0,0,0)')
		figure = dict(data=data, layout=layout_eta)
		return figure

	def theta_graph(self):
		layout_theta = copy.deepcopy(self.layout())
		if self.dfRIC is None:
			annotation = dict(
				text='No data available',
				x=0.5,
				y=0.5,
				align='center',
				showarrow=True,
				xref='paper',
				yref='paper'
			)
		else:
			x = self.dfStage.Name.unique()
			data = [
				self.scatter_chart('lines+markers', 'Number of Employees', x,
								   self.dfStage.Employees.dropna(), 'spline', 2, 1, '#a9bb95', 'diamond')
			]
		layout_theta['title'] = 'EMPLOYMENT'
		figure = dict(data=data, layout=layout_theta)
		return figure
