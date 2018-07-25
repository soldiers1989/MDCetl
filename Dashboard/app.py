import dash
from flask_cors import CORS
from Dashboard.page.Layout import HomePage
from Dashboard.graph.Chart import Chart
from dash.dependencies import Input, Output, State


page = HomePage('BAP Quarterly','Mussie N.')
chart = Chart()
app = dash.Dash()
app.title= 'MDC DashBoard'

app.css.append_css({'external_url':'https://storage.googleapis.com/general_mdc_20180525/css/items.css'})
app.css.append_css({'external_url': 'https://storage.googleapis.com/general_mdc_20180525/css/main.css'})
# app.css.append_css({'external_url': 'https://storage.googleapis.com/general_mdc_20180525/css/tables.css'})
server = app.server
CORS(server)
app.layout = page.index_page()

# app.config['suppress_callback_exceptions']=True

@app.callback(Output('alpha_graph', 'figure'),
              [Input('mdc_metrics', 'value')])
def update_apha_graph(mdc_metrics):
    return chart.alpha_graph('Funding to Date', str(mdc_metrics),  'FundingTODate', mdc_metrics)

@app.callback(Output('beta_graph', 'figure'),
              [Input('mdc_metrics', 'value')])
def update_beta_graph(mdc_metrics):
    return chart.beta_graph(mdc_metrics)

@app.callback(Output('gamma_graph', 'figure'),
              [Input('mdc_metrics', 'value')])
def update_beta_graph(mdc_metrics):
    return chart.gamma_graph(mdc_metrics)

@app.callback(Output('delta_graph', 'figure'),
              [Input('mdc_metrics', 'value')])
def update_delta_graph(mdc_metrics):
    return chart.delta_graph(mdc_metrics)

@app.callback(Output('epsilon_graph', 'figure'),
              [Input('mdc_metrics', 'value')])
def update_epsilon_graph(mdc_metrics):
    return chart.epsilon_graph(mdc_metrics, 'FundingTODate', 'REVENUE')

@app.callback(Output('zeta_graph', 'figure'),
              [Input('mdc_metrics', 'value')])
def update_zeta_graph(mdc_metrics):
    return chart.zeta_graph(mdc_metrics)

@app.callback(Output('eta_graph', 'figure'),
              [Input('mdc_metrics', 'value')])
def update_eta_graph(mdc_metrics):
    return chart.eta_graph(mdc_metrics)

@app.callback(Output('theta_graph', 'figure'),
              [Input('mdc_metrics', 'value')])
def update_theta_graph(mdc_metrics):
    return chart.theta_graph(mdc_metrics, 'FundingTODate', 'REVENUE')

if __name__ == '__main__':
    app.run_server(debug=True, threaded=True)
