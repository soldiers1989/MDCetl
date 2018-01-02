import os
import pandas as pd
import time

from Shared.common import Common as CM
from Shared.enums import VAR, CONSTANTS
from Shared.file_service import FileService
from Shared.db import DB as db


class Crunchbase:
	def __init__(self):
		self.user_key = CM.get_config('config.ini', 'crunch_base', 'user_key')
		self.api_token = '&user_key=' + self.user_key + '&page={}'
		self.api_tokens = '?user_key=' + self.user_key + '&page={}'

		self.url_org = CM.get_config('config.ini', 'crunch_base', 'url_org') + self.api_token
		self.url_people = CM.get_config('config.ini', 'crunch_base', 'url_person') + self.api_token
		self.url_cat = CM.get_config('config.ini', 'crunch_base', 'url_cat') + self.api_tokens
		self.url_loc = CM.get_config('config.ini', 'crunch_base', 'url_loc') + self.api_token

		self.path = CM.get_config('config.ini', 'box_file_path', 'path_crunchbase')

		self.org_summary = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_organizations_insert')
		self.people = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_people_insert')
		self.category = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_category_insert')
		self.location = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_location_insert')

		self.data = None
		self.file_name = 'CB_{}_{}.csv'
		self.file = FileService(self.path)

	def get_organizations(self):
		self.get_data(self.url_org, CONSTANTS.organization_summary.value)

	def get_people(self):
		self.get_data(self.url_people, CONSTANTS.people_summary.value)

	def get_categories(self):
		self.get_data(self.url_cat, CONSTANTS.categories.value)

	def get_locations(self):
		self.get_data(self.url_loc, CONSTANTS.locations.value)

	def get_data(self, url, object_name):
		self.data = CM.get_crunch_data(url.format('1'))
		if self.data.ok:
			total_items = self.data.json()[VAR.data.value][VAR.paging.value][VAR.total_items.value]
			number_of_pages = self.data.json()[VAR.data.value][VAR.paging.value][VAR.number_of_pages.value]
			cols = self.data.json()[VAR.data.value][VAR.items.value][0][VAR.properties.value].keys()
			cols = list(cols)
			cols.append('uuid')
			print(cols)
			print('Total items: {}\nTotal Pages: {}'.format(total_items, number_of_pages))
			data_list = []
			for j in range(0, number_of_pages):
				self.data = CM.get_crunch_data(url.format(j + 1))
				data = self.data.json()[VAR.data.value][VAR.items.value]
				print(j, '*' * j, len(data))
				for i in range(0, len(data)):
					dt = data[i][VAR.properties.value]
					dt[VAR.uuid.value] = data[i][VAR.uuid.value]
					data_list.append(dt)
			df = pd.DataFrame(data_list, columns=cols)
			df.to_csv(self.file_name.format(object_name, str(time.time())), sep=',', columns=cols, index=False)
			print('File saved successfully!')
		else:
			print('SNAP! Something goes wrong.\nSTATUS: {}\nMESSAGE: {}'.format(self.data.json()[0]['status'], self.data.json()[0]['message']))

	def save_cb_data(self):
		try:
			print(os.getcwd(), self.path)
			#df = pd.read_csv('CB_organization_summary_1513788334.csv')
			#df = pd.read_csv('CB_people_summary_1513794895.csv')
			#df = pd.read_csv('CB_CATEGORIES_1514474648.46174.csv')
			df = pd.read_csv('CB_LOCATIONS_1514478442.280132.csv')
			print('{}\n{}'.format(len(df), df.columns))
			db.save_data_chunk(df, self.location, chunk_size=5000)
		except FileNotFoundError as f:
			print(f)


if __name__ == '__main__':
	crb = Crunchbase()
	crb.save_cb_data()
