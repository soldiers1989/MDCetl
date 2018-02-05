import Shared.datasource as ds
import Shared.enums as enum
from Shared.file_service import FileService


class FastLane(ds.DataSource):
	def __init__(self):
		super().__init__('box_file_path', 'path_other', enum.DataSourceType.DATA_CATALYST)

	def read_misc_file(self):
		self.data = self.file.read_source_file(enum.FileType.SPREAD_SHEET.value, enum.MDCDataSource.OTHER, enum.Combine.FOR_NONE.value)
		# print(self.data.head())

	def push_data_to_db(self):
		self.read_misc_file()
		self.data['Venture_basic_name'] = self.data.apply(lambda df: self.common.get_basic_name(df.Venture_name), axis=1)
		values = self.common.df_list(self.data)
		self.db.bulk_insert(self.enum.SQL.sql_other_insert.value, values=values)
		print(values[0])

	def last_minute_issue(self):
		path = '/Users/mnadew/Box Sync/WorkBench/BAP/BAP_FY18/FY18_Q3/LMI'
		fs = FileService(path)
		_, _, data = fs.read_source_file(enum.FileType.SPREAD_SHEET.value, enum.MDCDataSource.BAP, enum.Combine.FOR_NONE.value)
		# print(data.head())
		for _, row in data.iterrows():
			update = 'UPDATE BAP.QuarterlyCompanyData SET  Stage = \'{}\' WHERE [Company Name] = \'{}\''.format(row['Stage'], self.common.sql_friendly(row['Company Name']))
			print(update)


if __name__ == '__main__':
	fl = FastLane()
	fl.last_minute_issue()