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
		return traces

	def bubble_chart(self, df, x_value, y_value):
		traces = []
		for ric in self.dfRIC.Name.unique():
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

	def scatter_chart(self, mode, graph_name,x_series, y_series, line_shape, smoothing=2, line_width=1, line_color='#fac1b7', line_symbol='circle', opacity=1):
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

	def threeD_scatter_cahrt(self, x_value, y_value, z_value):
		trace =  go.Scatter3d(
			x=x_value,
			y=y_value,
			z=z_value,
			mode='markers',
			marker=dict(
				size=12,
				line=dict(
					color='rgba(217, 217, 217, 0.14)',
					width=0.5
				)
			),
			opacity=0.8,
			showlegend=False
		)
		return trace


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
		layout_bubble['hovermode'] = 'closest'
		layout_bubble['title'] = '{} Vs Funding Rasided by RIC'.format(x_title)
		layout_bubble['margin'] = dict(l=65,r=35,b=35,t=45)
		dff = self.dfRIC[self.dfRIC['FundingTODate'] < 10000000]


		data = self.bubble_chart(dff, x_value, y_value)

		figure = dict(data=data, layout=layout_bubble)

		return figure

	def beta_graph(self, y_value):
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
			x = self.dfRIC['Name'].unique()
			y = self.dfRIC[y_value].dropna()
			data = [

				self.scatter_chart('lines+markers', 'Revenue', x,y,
								   'spline', 2, 1, '#fac1b7', 'diamond')
			]
		layout_beta['title'] = str(y_value)
		figure = dict(data=data, layout=layout_beta)
		return figure

	def gamma_graph(self, y_value):
		layout_gamma = copy.deepcopy(self.layout())
		colors = ['skyblue','skyblue']

		data = [self.bar_graph(self.dfRIC.Name.unique(),
							   self.dfRIC[y_value].dropna(),
							   '',
							   colors='skyblue')]
		layout_gamma['title'] = '{} by RIC'.format(str(y_value))
		layout_gamma['dragmode'] = 'select'
		layout_gamma['showlegend'] = False

		figure= dict(data=data, layout=layout_gamma)
		return figure

	def delta_graph(self, y_value):
		layout_delta = copy.deepcopy(self.layout())
		x = self.dfRIC.Name.unique()
		data = [
			self.pie_graph(list(x),
						   list(self.dfRIC.groupby(['Name'])[y_value].agg('sum')),
						   '',
						   list(self.dfRIC.Name.unique()), 0.3, {"x": [1.1, 1.55], 'y': [1.2, 0.8]},  #{"x": [0, .45], 'y': [0.2, 0.8]},
						   ['#fac1b7', '#a9bb95', '#92d8d8']),

		]
		layout_delta['title'] = '{} breakdown'.format(y_value)
		layout_delta['font'] = dict(color='#777777')
		layout_delta['showlegend'] = False
		layout_delta['legend'] = dict(
								font=dict(color='#CCCCCC', size='10'),
								orientation='h',
								bgcolor='rgba(0,0,0,0)')
		figure = dict(data=data, layout=layout_delta)
		return figure

	def epsilon_graph(self, x_value, y_value, z_value):
		layout_epsilon = copy.deepcopy(self.layout())
		data = [
			self.threeD_scatter_cahrt(self.dfRIC[x_value].unique(), self.dfRIC[y_value].dropna(), self.dfRIC[z_value].dropna())]
		layout_epsilon['title'] = '{} | {} | {}'.format(str(x_value), y_value, z_value)
		figure = dict(data=data, layout=layout_epsilon)
		return figure

	def zeta_graph(self, y_value):  # Aggregate
		layout_zeta = copy.deepcopy(self.layout())
		colors = ['skyblue', 'skyblue']

		data = [self.bar_graph(self.dfStage.Name.unique(),
							   self.dfStage[y_value].dropna(),
							   'All RICS Revenue',
							   colors='skyblue')]
		layout_zeta['title'] = '{} by RIC'.format(str(y_value))
		layout_zeta['dragmode'] = 'select'
		layout_zeta['showlegend'] = False

		figure = dict(data=data, layout=layout_zeta)
		return figure

	def eta_graph(self, y_value):  # pie
		layout_eta = copy.deepcopy(self.layout())
		data = [
			self.pie_graph(list(self.dfStage.Name.unique()),
						   list(self.dfStage.groupby(['Name'])[y_value].agg('sum')),'',
						   list(self.dfStage.Name.unique()), 0.5, {"x": [1.1, 1.55], 'y': [1.2, 0.8]},
						   ['#fac1b7', '#a9bb95', '#92d8d8']),
		]
		layout_eta['title'] = '{} breakdown'.format(y_value)
		layout_eta['font'] = dict(color='#777777')
		layout_eta['showlegend'] = True
		layout_eta['legend'] = dict(
			font=dict(color='#CCCCCC', size='10'),
			orientation='v',
			bgcolor='rgba(0,0,0,0)')
		figure = dict(data=data, layout=layout_eta)
		return figure

	def theta_graph(self, x_value, y_value, z_value):
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
			data = [self.threeD_scatter_cahrt(self.dfStage[x_value].unique(), self.dfStage[y_value].dropna(), self.dfStage[z_value].dropna())]
		layout_theta['title'] = '{} | {} | {}'.format(str(x_value), y_value, z_value)
		figure = dict(data=data, layout=layout_theta)
		return figure
