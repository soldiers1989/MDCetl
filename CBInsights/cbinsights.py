

import Shared.enums as enum

import datetime
import pandas as pd
import Shared.datasource as ds


class CBInsights(ds.DataSource):

	def __init__(self):
		super().__init__('box_file_path', 'path_cbinsights')
		file_name = 'CB_Insights_Canada_2017.csv'

	def read_cbinsights_files(self):
		self.data = self.file.read_source_file(enum.FileType.CSV, enum.DataSource.CBINSIGHT, file_name=self.file_name)
		print(self.data.head())

	def push_cbinsights_to_db(self):
		self.read_cbinsights_files()
		# batch_id = self.bach.create_batch_for_etl(DataSource.CBINSIGHT.value, SourceSystemType.CB_Insights.value, self.year,self.quarter, file_name=self.file_name, full_path=self.cb_path, work_sheet_name='CB_Insights_Canada_2017')
		batch_id = 3678

		self.data.insert(0, 'BatchID', batch_id)
		df = self.data.where(pd.notnull(self.data), None)
		values = self.common.df_list(df)
		# db.DB.bulk_insert(self.cb_sql_insert, values)
		self.db.bulk_insert(self.cb_sql_insert, values)


if __name__ == '__main__':
	cb = CBInsights()
	cb.read_cbinsights_files()