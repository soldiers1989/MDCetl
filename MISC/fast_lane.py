import Shared.datasource as ds
import Shared.enums as enum


class FastLane(ds.DataSource):
	def __init__(self):
		super().__init__('box_file_path', 'path_other', enum.DataSourceType.DATA_CATALYST)

	def read_misc_file(self):
		self.data = self.file.read_source_file(enum.FileType.SPREAD_SHEET, enum.MDCDataSource.OTHER, enum.Combine.FOR_NONE.value)
		# print(self.data.head())

	def push_data_to_db(self):
		self.read_misc_file()
		self.data['Venture_basic_name'] = self.data.apply(lambda df: self.common.get_basic_name(df.Venture_name), axis=1)
		values = self.common.df_list(self.data)
		self.db.bulk_insert(self.enum.SQL.sql_other_insert.value, values=values)
		print(values[0])


if __name__ == '__main__':
	fl = FastLane()
	fl.push_data_to_db()