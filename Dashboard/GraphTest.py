import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from Shared.common import Common
import os

app = dash.Dash()
print(os.getcwd())
Common.change_working_directory('Box Sync/mnadew/IE/MDCetl/Dashboard/data')
print(os.getcwd())
df = pd.read_csv('gdp_life_exp_2007.csv')


def b_data():
    traces = []
    for i in df.continent.unique():
        trace = go.Scatter(
                    x=df[df['continent'] == i]['gdp per capita'],
                    y=df[df['continent'] == i]['life expectancy'],
                    text=df[df['continent'] == i]['country'],
                    mode='markers',
                    opacity=0.7,
                    marker={
                        'size': 15,
                        'line': {'width': 0.5, 'color': 'white'}
                    },
                    name=i)
        traces.append(trace)
    return traces
    # data = [
    #             go.Scatter(
    #                 x=df[df['continent'] == i]['gdp per capita'],
    #                 y=df[df['continent'] == i]['life expectancy'],
    #                 text=df[df['continent'] == i]['country'],
    #                 mode='markers',
    #                 opacity=0.7,
    #                 marker={
    #                     'size': 15,
    #                     'line': {'width': 0.5, 'color': 'white'}
    #                 },
    #                 name=i
    #             ) for i in df.continent.unique()
    #         ]
    # return data


def b_layout():
    layout= go.Layout(
        xaxis={'type': 'log', 'title': 'GDP Per Capita'},
        yaxis={'title': 'Life Expectancy'},
        margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
        legend={'x': 0, 'y': 1},
        hovermode='closest')
    return layout


app.layout = html.Div([
    dcc.Graph(
        id='life-exp-vs-gdp',
        figure=dict(data=b_data(), layout=b_layout())
    )
])

if __name__ == '__main__':
    app.run_server()