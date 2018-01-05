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
		self.api_org_token = '?user_key=' + self.user_key

		self.url_org = CM.get_config('config.ini', 'crunch_base', 'url_org') + self.api_token
		self.url_people = CM.get_config('config.ini', 'crunch_base', 'url_person') + self.api_token
		self.url_cat = CM.get_config('config.ini', 'crunch_base', 'url_cat') + self.api_tokens
		self.url_loc = CM.get_config('config.ini', 'crunch_base', 'url_loc') + self.api_token

		self.path = CM.get_config('config.ini', 'box_file_path', 'path_crunchbase')

		self.org_summary = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_organizations_insert')
		self.people = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_people_insert')
		self.category = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_category_insert')
		self.location = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_location_insert')

		self.orgs_api_url = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_orgs_summary')
		self.orgs_detail_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_orgs_detail_insert')
		self.orgs_summary_update = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_orgs_summary_update')

		self.orgs_detail_update = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_orgs_detail_update')

		self.data = None
		self.file_name = 'CB_{}_{}.csv'
		self.file = FileService(self.path)
		self.org_uuid = None
		self.i = 0

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

	def get_organization_api_url(self):
		df = db.pandas_read(self.orgs_api_url)
		for c in df.iterrows():
			self.save_orgs_detail(c[1].api_url)

	def save_orgs_detail(self, api_url):
		self.i = self.i + 1
		url = api_url + self.api_org_token
		orgs = CM.get_crunch_data(url)
		if orgs.ok:
			uuid = orgs.json()[VAR.data.value]['uuid']
			print('{}. UUID: {}'.format(self.i, uuid))
			columns = list(orgs.json()[VAR.data.value][VAR.properties.value].keys())
			columns.append('org_uuid')
			json_properties = orgs.json()[VAR.data.value][VAR.properties.value]
			json_properties['org_uuid'] = uuid
			df_properties = pd.DataFrame([json_properties], columns=columns)
			values = CM.df_list(df_properties)
			db.bulk_insert(self.orgs_detail_insert, values)
			db.execute(self.orgs_summary_update.format(uuid))

	def get_orgs_rshp_api_url(self):
		df = db.pandas_read(self.orgs_api_url)
		for c in df.iterrows():
			self.save_orgs_relationship(c[1].api_url)

	def save_orgs_relationship(self, api_url):
		url = api_url + self.api_org_token
		print(url)
		orgs = CM.get_crunch_data(url)
		if orgs.ok:
			self.org_uuid = orgs.json()[VAR.data.value][VAR.uuid.value]
			rs_json = orgs.json()[VAR.data.value][VAR.relationships.value]
			primary_image = rs_json['primary_image']
			self.save_relational_entity(primary_image, self.org_uuid, 'insert statement')
			founders = rs_json['founders']
			self.save_relational_entity(founders, self.org_uuid, 'insert statement')
			featured_team = rs_json['featured_team']
			self.save_relational_entity(featured_team, self.org_uuid, 'insert statement')
			current_team = rs_json['current_team']
			self.save_relational_entity(current_team, self.org_uuid, 'insert statement')
			past_team = rs_json['past_team']
			self.save_relational_entity(past_team, self.org_uuid, 'insert statement')
			bmad = rs_json['board_members_and_advisors'] #Job
			self.save_relational_entity(bmad, self.org_uuid, 'insert statement')
			investors = rs_json['investors'] # Organization or Person or FundingRound
			self.save_relational_entity(investors, self.org_uuid, 'insert statement')
			owned_by = rs_json['owned_by'] # Person or Organizations
			self.save_relational_entity(owned_by, self.org_uuid, 'insert statement')
			sub_organizations = rs_json['sub_organizations']
			self.save_relational_entity(sub_organizations, self.org_uuid, 'insert statement')
			headquarters = rs_json['headquarters']
			self.save_relational_entity(headquarters, self.org_uuid, 'insert statement')
			offices = rs_json['offices']
			self.save_relational_entity(offices, self.org_uuid, 'insert statement')
			# products = rs_json['products']
			# self.save_relational_entity(products, uuid, 'insert statement')
			categories = rs_json['categories']
			self.save_relational_entity(categories, self.org_uuid, 'insert statement')
			# customers = rs_json['customers']
			# self.save_relational_entity(customers, uuid, 'insert statement')
			# competitors = rs_json['competitors']
			# self.save_relational_entity(competitors, uuid, 'insert statement')
			# members = rs_json['members']
			# self.save_relational_entity(members, uuid, 'insert statement')
			# memberships = rs_json['memberships']
			# self.save_relational_entity(memberships, uuid, 'insert statement')
			funding_rounds = rs_json['funding_rounds']
			self.save_relational_entity(funding_rounds, self.org_uuid, 'insert statement')
			investments = rs_json['investments']
			self.save_relational_entity(investments, self.org_uuid, 'insert statement')
			acquisitions = rs_json['acquisitions']
			self.save_relational_entity(acquisitions, self.org_uuid, 'insert statement')
			acquired_by = rs_json['acquired_by']
			self.save_relational_entity(acquired_by, self.org_uuid, 'insert statement')
			ipo = rs_json['ipo']
			self.save_relational_entity(ipo, self.org_uuid, 'insert statement')
			funds = rs_json['funds']
			self.save_relational_entity(funds, self.org_uuid, 'insert statement')
			websites = rs_json['websites']
			self.save_relational_entity(websites, self.org_uuid, 'insert statement')
			images = rs_json['images']
			self.save_relational_entity(images, self.org_uuid, 'insert statement')
			# videos = rs_json['videos']
			# self.save_relational_entity(videos, uuid, 'insert statement')
			news = rs_json['news']
			self.save_relational_entity(news, self.org_uuid, 'insert statement')
			print('-' * 200)

	def save_relational_entity(self, json, org_uuid, sql_insert):
		if json[VAR.cardinality.value] == 'OneToOne':
			if int(json['paging']['total_items']) > 0:
				print(json[VAR.item.value][VAR.type.value])
				uuid = json[VAR.item.value][VAR.uuid.value]
				self.push_entity_to_db(json, org_uuid, sql_insert, uuid)
		elif json[VAR.cardinality.value] == 'OneToMany':
			if int(json['paging']['total_items']) > 0:
				print(json[VAR.items.value][0][VAR.type.value])
				for i in range(int(json['paging']['total_items'])):
					uuid = json[VAR.items.value][i][VAR.uuid.value]
					self.push_entity_to_db(json, org_uuid, sql_insert, uuid)

	def push_entity_to_db(self, json, org_uuid, sql_insert, uuid):
		json_properties = json[VAR.item.value][VAR.properties.value]
		cols = list(json_properties.keys())
		cols.append('uuid')
		cols.append('org_uuid')
		json_properties['uuid'] = uuid
		json_properties['org_uuid'] = org_uuid
		print(cols)
		df_properties = pd.DataFrame([json_properties], columns=cols)
		values = CM.df_list(df_properties)
		db.bulk_insert(sql_insert, values)
		db.execute(self.orgs_detail_update.format(org_uuid))

	def generate_columns(self, prprty):
		if int(prprty['paging']['total_items']) == 0:
			print('MISSING')
		elif int(prprty['paging']['total_items']) == 1:
			if prprty[VAR.cardinality.value] == 'OneToOne':
				print('{} ----> {}'.format(prprty[VAR.item.value][VAR.type.value], list(prprty[VAR.item.value][VAR.properties.value].keys())))
			elif prprty[VAR.cardinality.value] == 'OneToMany':
				if isinstance(prprty[VAR.items.value], list):
					print('{} ---> {}'.format(prprty[VAR.items.value][0]['type'], list(prprty[VAR.items.value][0]['properties'].keys())))
				else:
					print('{} ----> {}'.format(prprty[VAR.items.value][VAR.type.value], list(prprty[VAR.items.value][VAR.properties.value].keys())))
		elif int(prprty['paging']['total_items']) > 1:
			print('{} ----> {}'.format(prprty[VAR.items.value][0][VAR.type.value], list(prprty[VAR.items.value][0][VAR.properties.value].keys())))


if __name__ == '__main__':
	crb = Crunchbase()
	crb.get_orgs_rshp_api_url()
