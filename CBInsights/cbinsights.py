
import Shared.enums as enum
import pandas as pd
import Shared.datasource as ds


class CBInsights(ds.DataSource):

	def __init__(self):
		super().__init__('box_file_path', 'path_cbinsights', enum.MDCDataSource.CBINSIGHT)
		self.file_name = 'CB_Insights_Canada_2017.csv'

	def read_cbinsights_files(self):
		self.data = self.file.read_source_file(enum.FileType.CSV, enum.MDCDataSource.CBINSIGHT, file_name=self.file_name)
		print(self.data.head())

	def push_cbinsights_to_db(self):
		self.read_cbinsights_files()
		# batch_id = self.bach.create_batch_for_etl(DataSource.CBINSIGHT.value, SourceSystemType.CB_Insights.value, self.year,self.quarter, file_name=self.file_name, full_path=self.cb_path, work_sheet_name='CB_Insights_Canada_2017')
		batch_id = 3678

		self.data.insert(0, 'BatchID', batch_id)
		values = self.common.df_list(self.data)
		self.db.bulk_insert(self.cb_sql_insert, values)
		print(self.data.head())

	def cs_venture_matching(self):
		self.data = self.db.pandas_read(self.enum.SQL.sql_cbinsights_select.value)
		for _, cb in self.data.iterrows():
			sql_update = self.enum.SQL.sql_cbinsights_update.value.format(self.common.get_basic_name(cb.CompanyName), cb.ID)
			self.db.execute(sql_update)
			print(cb.CompanyName)


if __name__ == '__main__':
	cb = CBInsights()
	# cb.push_cbinsights_to_db()
	cb.cs_venture_matching()