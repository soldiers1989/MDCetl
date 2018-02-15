import Shared.datasource as ds
import Shared.enums as enum


class OSVP(ds.DataSource):

	def __init__(self):
		super.__init__('config_sql.ini', 'db_sql_general', enum.MDCDataSource.OSVP)

	def read_osvp_files(self):
		self.data = self.file.read_source_file(enum.FileType.SPREAD_SHEET, enum.MDCDataSource.OSVP)
		print(self.data.head())