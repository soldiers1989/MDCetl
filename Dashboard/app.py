import dash
from flask_cors import CORS
from Dashboard.page.Layout import HomePage


class MdcDb:
    def __init__(self):
        self.page = HomePage('BAP Quarterly','MDC User')

        self.app = dash.Dash()
        self.app.title= 'MDC DashBoard'


    def run_app(self):
        self.app.css.append_css({'external_url':'https://storage.googleapis.com/general_mdc_20180525/css/items.css'})
        self.app.css.append_css({'external_url': 'https://storage.googleapis.com/general_mdc_20180525/css/main.css'})
        # self.app.css.append_css({'external_url': 'https://storage.googleapis.com/general_mdc_20180525/css/tables.css'})
        self.server = self.app.server
        CORS(self.server)
        self.app_layout()
        self.app.run_server(debug=True, threaded=True)

    def app_layout(self):
        self.app.layout = self.page.index_page()


if __name__ == '__main__':
    db = MdcDb()
    db.run_app()
