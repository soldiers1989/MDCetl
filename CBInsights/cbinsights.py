from Shared.db import DB
from Shared.common import Common as COM
from Shared.batch import BatchService
from Shared.file_service import FileService
from Shared.enums import FileType, DataSource
import datetime


class CBInsights:
	file_name = 'CB_Insights_Canada_2017.csv'
	year, quarter = COM.fiscal_year_quarter(datetime.datetime.utcnow())
	batch = BatchService()
	cb_path = COM.get_config('config.ini', 'box_file_path', 'path_cbinsights')
	cb_sql_insert = COM.get_config('config_sql.ini', 'db_sql_cbinsights', 'sql_cbinsights_insert')
	file = FileService(cb_path)

	def __init__(self):
		self.data = None

	def read_cbinsights_files(self):
		self.data = self.file.read_source_file(FileType.CSV, DataSource.CBINSIGHT, file_name=self.file_name)
		print(self.data.head())

	def push_cbinsights_to_db(self):
		self.read_cbinsights_files()
		values = COM.df_list(self.data)
		DB.bulk_insert(self.cb_sql_insert, values)


if __name__ == '__main__':
	cb = CBInsights()
	cb.push_cbinsights_to_db()