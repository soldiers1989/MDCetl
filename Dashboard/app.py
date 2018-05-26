import dash
from flask_cors import CORS
from Dashboard.page.Layout import HomePage


class MdcDb:
    def __init__(self):
        self.page = HomePage('BAP Quarterly','MDC User')
        self.app = dash.Dash()
        self.app.css.append_css({'external_url': 'https://cdn.rawgit.com/plotly/dash-app-stylesheets/2d266c578d2a6e8850ebce48fdb52759b2aef506/stylesheet-oil-and-gas.css'})
        self.app.css.append_css({'external_url':'https://codepen.io/bcd/pen/KQrXdb.css'})
        self.server = self.app.server
        CORS(self.server)
        self.app_layout()
        self.app.run_server()

    def app_layout(self):
        self.app.layout = self.page.page_header()


if __name__ == '__main__':
    db = MdcDb()
