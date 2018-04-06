import pandas as pd
from Shared import db, enums, common, file_service, batch
import datetime


class DataSource:

	desired_width = 820

	def __init__(self, header, item, datasource:enums.MDCDataSource):
		pd.set_option('display.width', self.desired_width)
		self.common = common.Common
		_y, _q = self.common.fiscal_year_quarter(datetime.datetime.utcnow())
		self.year = _y
		self.quarter = _q
		self.data = None
		self.db = db.DB()
		self.batch = batch.BatchService()
		self.enum = enums

		if header is not '' and item is not '':
			self.path = self.common.get_config('config.ini', header, item)
			self.file = file_service.FileService(self.path)

		if datasource.value == enums.MDCDataSource.CBINSIGHT.value:
			self.cb_sql_insert = self.common.get_config('config_sql.ini', header, item)
		elif datasource.value == enums.MDCDataSource.OSVP.value:
			self.osvp_sql_insert = self.common.get_config('config_sql.ini', header, item)
		elif datasource.value == enums.MDCDataSource.IAF.value:
			pass

