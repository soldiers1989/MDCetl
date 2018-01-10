import os
import pandas as pd
import time

from Shared.common import Common as CM
from Shared.enums import VAR, CONSTANTS, TeamStatus
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
		self.orgs_detail_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_orgnization_insert')
		self.orgs_summary_update = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_orgs_summary_update')

		self.orgs_detail_update = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_orgs_detail_update')

		self.data = None
		self.file_name = 'CB_{}_{}.csv'
		self.file = FileService(self.path)
		self.org_uuid = None
		self.i = 0

		self.entities_script()

	def entities_script(self):
		self.sql_acquired_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_acquired_insert')
		self.sql_acquiree_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_acquiree_insert')
		self.sql_acquisition_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_acquisition_insert')
		self.sql_category_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_category_insert')
		self.sql_org_category_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_org_category_insert')
		self.sql_founders_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_founders_insert')
		self.sql_funding_rounds_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_funding_rounds_insert')
		self.sql_funds_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_funds_insert')
		self.sql_image_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_image_insert')
		self.sql_investments_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_investments_insert')
		self.sql_investors_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_investors_insert')
		self.sql_ipo_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_ipo_insert')
		self.sql_job_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_job_insert')
		self.sql_news_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_news_insert')
		self.sql_offices_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_offices_insert')
		self.sql_partners_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_partners_insert')
		self.sql_sub_organization_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_sub_organization_insert')
		self.sql_team_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_team_insert')
		self.sql_websites_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_websites_insert')
		self.sql_person_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_person_insert')
		self.sql_invested_in_insert = CM.get_config('config_sql.ini', 'db_sql_crunchbase', 'sql_invested_in_insert')

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

	def get_org_entities(self):
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

			self.save_teams(rs_json['featured_team'], self.org_uuid, TeamStatus.Featured.value)
			self.save_teams(rs_json['current_team'], self.org_uuid, TeamStatus.Current.value)
			self.save_teams(rs_json['past_team'], self.org_uuid, TeamStatus.Past.value)
			self.save_teams(rs_json['board_members_and_advisors'], self.org_uuid, TeamStatus.Board.value)

			self.save_funding_rounds(rs_json['funding_rounds'], self.org_uuid)
			self.save_investments_invested_in(rs_json['investments'])

			self.save_relational_entity(rs_json['sub_organizations'], self.org_uuid, self.sql_sub_organization_insert)
			self.save_relational_entity(rs_json['offices'], self.org_uuid, self.sql_offices_insert)
			self.save_relational_entity(rs_json['categories'], self.org_uuid, self.sql_org_category_insert)
			self.save_relational_entity(rs_json['founders'], self.org_uuid, self.sql_founders_insert)
			self.save_relational_entity(rs_json['acquisitions'], self.org_uuid, self.sql_acquisition_insert)
			self.save_relational_entity(rs_json['acquired_by'], self.org_uuid, self.sql_acquired_insert)
			self.save_relational_entity(rs_json['ipo'], self.org_uuid, self.sql_ipo_insert)
			self.save_relational_entity(rs_json['funds'], self.org_uuid, self.sql_funds_insert)
			self.save_relational_entity(rs_json['websites'], self.org_uuid, self.sql_websites_insert)
			self.save_relational_entity(rs_json['images'], self.org_uuid, self.sql_image_insert)
			self.save_relational_entity(rs_json['news'], self.org_uuid, self.sql_news_insert)

			db.execute(self.orgs_summary_update.format(self.org_uuid))

	def save_funding_rounds(self, json, org_uuid):
		if json[VAR.paging.value][VAR.total_items.value] > 0:
			for i in range(int(json[VAR.paging.value][VAR.total_items.value])):
				self.save_relational_entity(json[i], org_uuid, self.sql_funding_rounds_insert)
				funding_rounds_uuid = json[i][VAR.uuid.value]
				investments = json[i][VAR.relationships.value][VAR.investments.value]
				for j in len(investments):
					self.push_entity_to_db(investments[j][VAR.properties.value], funding_rounds_uuid
												,self.sql_investments_insert, fk_uuid='funding_rounds_uuid')
					if investments[i][VAR.relationships.value] is not None:
						investors = investments[i][VAR.relationships.value]['investors'][VAR.properties.value]
						investment_uuid = investments[j][VAR.uuid.value]
						self.push_entity_to_db(investors, investment_uuid, self.sql_investors_insert, fk_uuid='investment_uuid')

						partners = investments[i][VAR.relationships.value]['partners'][VAR.properties.value]
						self.push_entity_to_db(partners, investment_uuid, self.sql_investors_insert, fk_uuid='investment_uuid')

	def save_investments_invested_in(self, json):
		if json[VAR.paging.value][VAR.total_items.value] > 0:
			investment_uuid = json[VAR.items.value][0][VAR.uuid.value]
			for i in range(json[VAR.paging.value][VAR.total_items.value]):
				self.push_entity_to_db(json[VAR.items.value][i][VAR.relationships.value][VAR.invested_in.value], investment_uuid, self.sql_invested_in_insert, fk_uuid='investment_uuid')

	def save_teams(self, json, org_uuid, status):
		for i in range(int(json[VAR.paging.value][VAR.total_items.value])):
			json[VAR.items.value][i][VAR.properties.value]['TeamStatus'] = status
			team_uuid = json[VAR.items.value][i][VAR.uuid.value]
			self.push_entity_to_db(json[VAR.items.value][i], org_uuid, self.sql_team_insert, team_uuid)
			person_uuid = json[VAR.items.value][i][VAR.relationships.value][VAR.person.value][VAR.uuid.value]
			self.push_entity_to_db(json[VAR.items.value][i][VAR.relationships.value][VAR.person.value], team_uuid, self.sql_person_insert, person_uuid, fk_uuid='team_uuid')

	def save_relational_entity(self, json, org_uuid, sql_insert):
		if json[VAR.cardinality.value] == 'OneToOne':
			if int(json['paging']['total_items']) > 0:
				print(json[VAR.item.value][VAR.type.value])
				uuid = json[VAR.item.value][VAR.uuid.value]
				self.push_entity_to_db(json, org_uuid, sql_insert, uuid, 0)
		elif json[VAR.cardinality.value] == 'OneToMany':
			if int(json['paging']['total_items']) > 0:
				print(json[VAR.items.value][0][VAR.type.value])
				for i in range(int(json['paging']['total_items'])):
					uuid = json[VAR.items.value][i][VAR.uuid.value]
					self.push_entity_to_db(json, org_uuid, sql_insert, uuid, i)

	def push_entity_to_db(self, json, org_uuid, sql_insert, uuid, i=0, fk_uuid='org_uuid'):
		if json[VAR.properties.value]:
			json_properties = json[VAR.properties.value]
		elif json[VAR.cardinality.value] == 'OneToOne':
			json_properties = json[VAR.item.value][VAR.properties.value]
		elif json[VAR.cardinality.value] == 'OneToMany':
			json_properties = json[VAR.items.value][i][VAR.properties.value]
		if 'uuid' not in json_properties.keys():
			json_properties['uuid'] = uuid
		if fk_uuid not in json_properties.keys():
			json_properties[fk_uuid] = org_uuid
		print(list(json_properties.keys()))
		df_properties = pd.DataFrame([json_properties], columns=json_properties.keys())
		values = CM.df_list(df_properties)
		db.bulk_insert(sql_insert, values)

	@staticmethod
	def generate_columns(prprty):
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
	crb.get_org_entities()
