from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html


class HomePage:
	def __init__(self, page_title, user_name):
		self.title = page_title
		self.user = user_name

	def page_header(self):
		header = html.Div(style={'backgroundColor': 'black'}, children=[
		    # LOGO
		    self.page_logo(),
		    # USER DETAIL
		    self.user_detail(),
		    # MENU
		    self.page_menu(),
		    #DROPDOWN MENU
		    self.dropdown(),
		    self.upper_graph(),
		    self.lower_graph(),
		],
		className='ten columns offset-by-one'
		)
		return header

	def page_logo(self):
		pass

	def page_user_section(self):
		pass

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
			style={'margin-top': '20',
				   'backgroundColor': '#e6eeff',
				   'color': 'white',
				   'font-weight': 'bold'}
		)
		return menu

	def upper_graph(self):
		u_graph = html.Div(
			[
				html.Div(
					[
						dcc.Graph(id='main_graph')
					],
					className='eight columns',
					style={'margin-top': '20'}
				),
				html.Div(
					[
						dcc.Graph(id='individual_graph')
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
						dcc.Graph(id='count_graph')
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='pie_graph')
					],
					className='four columns',
					style={'margin-top': '10'}
				),
				html.Div(
					[
						dcc.Graph(id='aggregate_graph')
					],
					className='four columns',
					style={'margin-top': '10'}
				),
			],
			className='row'
		)
		return l_graph

	def dropdown(self):
		dd = html.Div(
			[
				html.Div(
					[
						# html.P('Filter by well status:'),
						# dcc.RadioItems(
						#     id='well_status_selector',
						#     options=[
						#         {'label': 'All ', 'value': 'all'},
						#         {'label': 'Active only ', 'value': 'active'},
						#         {'label': 'Customize ', 'value': 'custom'}
						#     ],
						#     value='active',
						#     labelStyle={'display': 'inline-block'}
						# ),
						dcc.Dropdown(
							id='well_statuses',
							options=[{'label':'BAP', 'value':'BAP'},
									 {'label': 'CRUNCHBASE', 'value': 'CB'},
									 {'label': 'CVCA', 'value': 'CV'}],
							multi=False,
							value=['BAP'],
						)# ,
						# dcc.Checklist(
						#     id='lock_selector',
						#     options=[
						#         {'label': 'Lock camera', 'value': 'locked'}
						#     ],
						#     values=[],
						# )
					],
					className='six columns'
				),
				html.Div(
					[
						# html.P('Filter by well type:'),
						# dcc.RadioItems(
						#     id='well_type_selector',
						#     options=[
						#         {'label': 'All ', 'value': 'all'},
						#         {'label': 'Productive only ', 'value': 'productive'},  # noqa: E501
						#         {'label': 'Customize ', 'value': 'custom'}
						#     ],
						#     value='productive',
						#     labelStyle={'display': 'inline-block'}
						# ),
						dcc.Dropdown(
							id='well_types',
							options=[{'label':'Database', 'value':'DB'},
									 {'label': 'EPP', 'value': 'EP'},
									 {'label': 'Spark', 'value': 'SP'}],
							multi=False,
							value=['DB'],
						),
					],
					className='six columns'
				),
			],
			className='row'
		)
		return dd

	def user_detail(self):
		user = html.Div(
			[
				html.H5(
					'++++',
					id='well_text',
					className='two columns'
				),
				html.H5(
					'****',
					id='production_text',
					className='eight columns',
					style={'text-align': 'center'}
				),
				html.H5(
					self.user,
					id='year_text',
					className='two columns',
					style={'text-align': 'left',
						   'color': 'skyblue',
						   'padding-left':0,
						   'padding-bottom': 2,}
				),
			],
			className='row'
		)

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
					src="https://s3-us-west-1.amazonaws.com/plotly-tutorials/logo/new-branding/dash-logo-by-plotly-stripe.png",
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

