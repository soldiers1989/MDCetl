from Shared.common import Common as common
from Dashboard.graph.Chart import Chart

import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import json
import pandas as pd
import datetime
import dash
from flask import Flask
from flask_cors import CORS


class HomePage:


	def __init__(self, page_title, user_name):
		self.title = page_title
		self.user = user_name
		self.chart = Chart()

	def index_page(self):
		index = html.Div(style={'backgroundColor': 'black'}, children=[
			# LOGO
			self.page_logo(),
			# USER DETAIL
			self.user_detail(),
			# MENU
			self.page_menu(),
			#DROPDOWN MENU
			self.chart_filter(),
			# First half of the graph
			self.upper_graph(),
			# Second half of the graph
			self.lower_graph(),
			#Third lower graph
			self.third_laayer_graph(),
		],
		className='ten columns offset-by-one' #'ten columns offset-by-one'
		)
		return index

	def page_menu(self):
		menu = html.Div(
			[
				html.Div([
					dcc.Link('BAP QUARTERLY  ', href='/bap', className="tab first"),
					dcc.Link('ANNUAL SURVEY   ', href='/annual-survey', className="tab"),
					dcc.Link('BOARD DECK   ', href='/board-deck', className="tab"),
					dcc.Link('CRUNCHBASE   ', href='/crunchbase', className="tab"),
					dcc.Link('EPP   ', href='/epp', className="tab"),
					dcc.Link('TDW   ', href='/tdw', className="tab"),
					dcc.Link('NRCAN   ', href='/bap', className="tab"),
					dcc.Link('StatsCAN   ', href='/annual-survey', className="tab"),
					# dcc.Link('PitchBook   ', href='/crunchbase', className="tab"),
					dcc.Link('EPP   ', href='/epp', className="tab"),
					dcc.Link('SPARK   ', href='/aprk', className="tab"),
					dcc.Link('ENERGY & ENVIRONMENT   ', href='/energyenvt', className="tab"),

					dcc.Link('MDC DATABASE   ', href='/dboverview', className="tab"),
					dcc.Link('CONTACT   ', href='/contact', className="tab"),
					dcc.Link('ABOUT   ', href='/about', className="tab"),
					], className="row ")
				],
			style={'margin-top': '5',
				   'backgroundColor': '#f0f0f5',
				   'color': 'blue',
				   'font-weight': 'bold'}
		)
		return menu

	def upper_graph(self):
		u_graph = html.Div(
			[
				html.Div(
					[
						dcc.Graph(id='alpha_graph')
					],
					className='eight columns',
					style={'margin-top': '20',  'backgroundColor':'grey'}
				),
				html.Div(
					[
						dcc.Graph(
							id='beta_graph')
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
						dcc.Graph(id='gamma_graph')
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='delta_graph')
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='epsilon_graph')
					],
					className='four columns',
					style={'margin-top': '10'}
				),
			],
			className='row'
		)
		return l_graph

	def third_laayer_graph(self):
		tl_graph = html.Div(
			[
				html.Div(
					[
						dcc.Graph(id='zeta_graph')
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='eta_graph')
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='theta_graph')
					],
					className='four columns',
					style={'margin-top': '10'}
				),
			],
			className='row'
		)
		return tl_graph

	def chart_filter(self):
		val = list(self.chart.dfRIC.FiscalYear.unique())
		dd = html.Div(
			[
				html.Div(
					[
						dcc.Dropdown(
							id='mdc_metrics',
							options=[{'label': 'Funding to Date', 'value': 'FundingTODate'},
									 {'label': 'Revenue', 'value': 'REVENUE'},
									 {'label': 'Employment', 'value': 'Employees'},
									 {'label': 'Advisory Hours', 'value': 'AdvisoryHours'},
									 {'label': 'Volunteer Hours', 'value': 'VolunteerHours'}],
							multi=False,
							value='REVENUE',
						),
					],
					className='six columns'
				),
				html.Div(
					[
						dcc.Dropdown(
							id='fiscal_year',
							options=[{'label': 'FY 2015', 'value': 2015},
									 {'label': 'FY 2016', 'value': 2016},
									 {'label': 'FY 2017', 'value': 2017},
									 {'label': 'FY 2018', 'value': 2018},
									 {'label': 'FY 2019', 'value': 2019}],
							multi=False,
							value=2017,
						),
					],
					className='three columns'
				),
				html.Div(
					[
						dcc.Dropdown(
							id='fiscal_quarter',
							options=[{'label': 'Q I', 'value': 1},
									 {'label': 'Q II', 'value': 2},
									 {'label': 'Q III', 'value': 4},
									 {'label': 'Q IV', 'value': 5}],
							multi=False,
							value=1,
						),
					],
					className='three columns'
				),
				# html.Div(
				# 	[
				# 		dcc.RangeSlider(
				# 			id='year_slider',
				# 			min=2015,
				# 			max=2018,
				# 			step=1,
				# 			value=val
				# 		),
				# 	],
				# 	className='six columns',
				# 	style={'backgroundColor':'none', 'padding-top':10}
				# ),
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
					'MDC eyes only',
					id='well_text',
					className='three columns',
					style={'font-style': 'italic', 'color':'white'}
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

	def temp_table(self, title):
		common.change_working_directory('Box Sync/mnadew/IE/MDCetl/Dashboard/data')
		df = pd.read_csv('geo.csv')
		return self.chart.table(df)



