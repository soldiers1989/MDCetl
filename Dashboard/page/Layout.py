from Shared.common import Common as common
from Dashboard.graph.Chart import Chart
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import json
import pandas as pd
import datetime


class HomePage:
	def __init__(self, page_title, user_name):
		self.title = page_title
		self.user = user_name
		self.chart = Chart()
		self.COLORS = [
			{
				'background': '#fef0d9',
				'text': 'rgb(30, 30, 30)'
			},
			{
				'background': '#fdcc8a',
				'text': 'rgb(30, 30, 30)'
			},
			{
				'background': '#fc8d59',
				'text': 'rgb(30, 30, 30)'
			},
			{
				'background': '#d7301f',
				'text': 'rgb(30, 30, 30)'
			},
		]

	def index_page(self):
		index = html.Div(style={'backgroundColor': 'black'}, children=[
			# LOGO
			self.page_logo(),
			# USER DETAIL
			self.user_detail(),
			# MENU
			self.page_menu(),
			#DROPDOWN MENU
			self.dropdown(),
            # SLIDER
            self.rangeslider(),
			# First half of the graph
			self.upper_graph(),
			# Second half of the graph
			self.lower_graph(),
			#Third lower graph
			self.third_lower_graph(),
		],
		className='ten columns offset-by-one' #'ten columns offset-by-one'
		)
		return index

	def page_menu(self):
		menu = html.Div(
			[
				html.Div([
					dcc.Link('BAP Quarterly  ', href='/bap', className="tab first"),
					dcc.Link('Annual Survey   ', href='/annual-survey', className="tab"),
					dcc.Link('Crunchbase   ', href='/crunchbase', className="tab"),
					dcc.Link('EPP   ', href='/epp', className="tab"),
					dcc.Link('Think Data Works   ', href='/tdw', className="tab"),
					dcc.Link('Board Deck   ', href='/board-deck', className="tab"),

					dcc.Link('NRCAN   ', href='/bap', className="tab"),
					dcc.Link('StatsCAN   ', href='/annual-survey', className="tab"),
					dcc.Link('PitchBook   ', href='/crunchbase', className="tab"),
					dcc.Link('EPP   ', href='/epp', className="tab"),
					dcc.Link('SPARK   ', href='/aprk', className="tab"),
					dcc.Link('Energy & Environment   ', href='/energyenvt', className="tab"),

					dcc.Link('Database Overview   ', href='/dboverview', className="tab"),
					dcc.Link('Contact   ', href='/contact', className="tab"),
					dcc.Link('About   ', href='/about', className="tab"),
					], className="row ")
				],
			style={'margin-top': '5',
				   'backgroundColor': 'grey',#e6eeff',
				   'color': 'blue',
				   'font-weight': 'bold'}
		)
		return menu

	def upper_graph(self):
		u_graph = html.Div(
			[
				html.Div(
					[
						dcc.Graph(id='main_graph',
                                  figure=self.chart.alpha_graph('Revenue','Funding to Date','REVENUE', 'FundingTODate')
								  )
						# self.table('TEST FOR BAP DATA')
					],
					className='eight columns',
					style={'margin-top': '20',  'backgroundColor':'grey'}
				),
				html.Div(
					[
						dcc.Graph(
							id='individual_graph',
							figure=self.chart.beta_graph()
						)
					],
					className='four columns',
					style={'margin-top': '20'}
				),
			],
			className='row'
		)
		return u_graph

	def lower_graph(self):
		l_graph = html.Div(
			[
				html.Div(
					[
						dcc.Graph(id='count_graph',
								  figure=self.chart.gamma_graph()
								  )
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='pie_graph',
								  figure=self.chart.delta_graph()
								  )
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='aggregate_graph',
								  figure=self.chart.epsilon_graph()
						)
					],
					className='four columns',
					style={'margin-top': '10'}
				),
			],
			className='row'
		)
		return l_graph

	def third_lower_graph(self):
		tl_graph = html.Div(
			[
				html.Div(
					[
						dcc.Graph(id='tcount_graph',
								  figure=self.chart.zeta_graph()
								  )
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='tpie_graph',
								  figure=self.chart.eta_graph()
								  )
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='taggregate_graph',
								  figure=self.chart.theta_graph()
						)
					],
					className='four columns',
					style={'margin-top': '10'}
				),
			],
			className='row'
		)
		return tl_graph

	def dropdown(self):
		dd = html.Div(
			[
				html.Div(
					[

						dcc.Dropdown(
							id='well_statuses',
							options=[{'label':'BAP', 'value':'BAP'},
									 {'label': 'CRUNCHBASE', 'value': 'CB'},
									 {'label': 'CVCA', 'value': 'CV'}],
							multi=False,
							value=['BAP'],
						)# ,

					],
					className='six columns'
				),
				html.Div(
					[

						dcc.Dropdown(
							id='well_types',
							options=[{'label':'Database', 'value':'DB'},
									 {'label': 'EPP', 'value': 'EP'},
									 {'label': 'Spark', 'value': 'SP'}],
							multi=False,
							value=['DB'],
						),
					],
					className='six columns',
					style={'backgroundColor':'none'}
				),
			],
			className='row',
			style={'backgroundColor': ''}
		)
		return dd

	def rangeslider(self):
		slider = html.Div(
			[
				dcc.RangeSlider(
					id='year_slider',
					min=2000,
					max=2018,
                    step=1,
					value=[2015, 2015]
				),
			],
			style={'margin-top': '1'}
		)
		return slider

	def user_detail(self):
		user = html.Div(
			[
				html.H5(
					'[Demo Only!]',
					id='well_text',
					className='three columns'
				),
				html.H5(
					str(datetime.date.today())[:15],
					id='production_text',
					className='seven columns',
					style={'text-align': 'center'}
				),
				html.H5(
					self.user,
					id='user_text',
					className='two columns',
					style={'text-align': 'right',
						   'color': 'skyblue'}
				),
			],
			className='row',
			style={'color': 'skyblue'}
		)
		return user

	def page_logo(self):
		logo = html.Div(
			[
				html.H1(
					self.title,
					className='eight columns',
					style={'color': 'skyblue',
							'float': 'left',
							'position': 'relative',
							'padding': 10,
						   },
				),
				html.Img(
					src="https://storage.googleapis.com/general_mdc_20180525/mdcdatacatalysti.png",#"https://s3-us-west-1.amazonaws.com/plotly-tutorials/logo/new-branding/dash-logo-by-plotly-stripe.png",
					className='one columns',
					style={
						'height': '100',
						'width': '225',
						'float': 'right',
						'position': 'relative',
					},
				),
			],
			className='row'
		)
		return logo

	def cell_style(self, value, min_value, max_value):
		style = {}
		if common.is_numeric(value):
			relative_value = (value - min_value) / (max_value - min_value)
			if relative_value <= 0.25:
				style = {
					'backgroundColor': self.COLORS[0]['background'],
					'color': self.COLORS[0]['text']
				}
			elif relative_value <= 0.5:
				style = {
					'backgroundColor': self.COLORS[1]['background'],
					'color': self.COLORS[1]['text']
				}
			elif relative_value <= 0.75:
				style = {
					'backgroundColor': self.COLORS[2]['background'],
					'color': self.COLORS[2]['text']
				}
			elif relative_value <= 1:
				style = {
					'backgroundColor': self.COLORS[3]['background'],
					'color': self.COLORS[3]['text']
				}
		return style

	def table(self, title):
		common.change_working_directory('Box Sync/mnadew/IE/MDCetl/Dashboard/data')
		df = pd.read_csv('geo.csv')
		return self.generate_table(df)
		# tbl = html.Div([
		# 	html.H5(title),
		# 	dt.DataTable(
		# 		id='main_table',
		# 		rows=df.to_dict('records'),
		# 		columns=df.columns,
		# 		editable=False,
		# 		filterable=True,
		# 		sortable=True)
		# ])
		# return tbl

	def generate_table(self, df):
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

