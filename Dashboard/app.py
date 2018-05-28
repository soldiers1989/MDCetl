import dash
from flask_cors import CORS
from Dashboard.page.Layout import HomePage


class MdcDb:
    def __init__(self):
        self.page = HomePage('BAP Quarterly','MDC User')

        self.app = dash.Dash()
        self.app.title= 'MDC DashBoard'


    def run_app(self):
        self.app.css.append_css({'external_url': 'https://cdn.rawgit.com/plotly/dash-app-stylesheets/2d266c578d2a6e8850ebce48fdb52759b2aef506/stylesheet-oil-and-gas.css'})
        self.app.css.append_css({'external_url': 'https://codepen.io/bcd/pen/KQrXdb.css'})
        # self.app.css.append_css({'external_url': 'https://codepen.io/chriddyp/pen/bWLwgP.css'})
        self.server = self.app.server
        CORS(self.server)
        self.app_layout()
        self.app.run_server(debug=True, threaded=True)

    def app_layout(self):
        self.app.layout = self.page.index_page()


if __name__ == '__main__':
    db = MdcDb()
    db.run_app()
