import os
import pandas as pd
import time
import Shared.datasource as ds
import Shared.enums as enum
from Shared.common import Common as CM
from Shared.enums import CBDict, CONSTANTS, TeamStatus, SQL

from Shared.file_service import FileService
from Shared.db import DB as db



class Crunchbase(ds.DataSource):

	def __init__(self):
		super().__init__('', '', datasource=enum.DataSourceType.CRUNCH_BASE)
		self.file = FileService(os.getcwd())
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
		# self.file = FileService(self.path)
		self.org_uuid = None
		self.fk_uuid = 'org_uuid'
		self.one_to_one = 'OneToOne'

		self.i = 0

		self.entities_script()

		self.col_funding = ['uuid', 'org_uuid', 'permalink', 'api_path', 'web_path', 'api_url', 'funding_type', 'series',
					   'series_qualifier', 'announced_on',
					   'announced_on_trust_code', 'closed_on', 'closed_on_trust_code', 'money_raised',
					   'money_raised_currency_code',
					   'money_raised_usd', 'target_money_raised', 'target_money_raised_currency_code',
					   'target_money_raised_usd',
					   'pre_money_valuation', 'pre_money_valuation_currency_code', 'pre_money_valuation_usd', 'rank',
					   'created_at',
					   'updated_at']
		self.org_columns = ['org_uuid', 'company_id', 'permalink', 'permalink_aliases', 'api_path', 'web_path', 'api_url',
					   'name', 'BasicName',
					   'also_known_as', 'short_description', 'description', 'profile_image_url',
					   'primary_role', 'role_company', 'role_investor', 'role_group', 'role_school',
					   'investor_type', 'founded_on', 'founded_on_trust_code', 'is_closed', 'closed_on',
					   'closed_on_trust_code', 'num_employees_min', 'num_employees_max', 'stock_exchange',
					   'stock_symbol', 'total_funding_usd', 'number_of_investments', 'homepage_url',
					   'contact_email', 'phone_number', 'rank', 'created_at', 'updated_at', 'fetched']
		self.org_summary_col = ['uuid', 'permalink', 'api_path', 'web_path', 'api_url', 'name', 'stock_exchange',
						  'stock_symbol', 'primary_role', 'short_description', 'profile_image_url',
						  'domain', 'homepage_url', 'facebook_url', 'twitter_url', 'linkedin_url',
						  'city_name', 'region_name', 'country_code', 'created_at', 'updated_at']
		self.office_col = ['uuid', 'org_uuid', 'name', 'street_1', 'street_2', 'postal_code', 'city', 'region',
						   'country','city_web_path','region_code2','region_web_path', 'country_code2',
						   'country_code3', 'country_web_path', 'latitude','longitude', 'created_at', 'updated_at' ]

		self.category_col = ['uuid' ,'org_uuid', 'name',
							 'web_path', 'category_groups',
							 'created_at', 'updated_at']

	def record_exits(self, sql):
		self.data = self.db.pandas_read(sql)
		if len(self.data) > 0:
			return True
		else:
			return False

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
		self.data = CM.get_api_data(url.format('1'))
		if self.data.ok:
			total_items = self.data.json()[CBDict.data.value][CBDict.paging.value][CBDict.total_items.value]
			number_of_pages = self.data.json()[CBDict.data.value][CBDict.paging.value][CBDict.number_of_pages.value]
			cols = self.data.json()[CBDict.data.value][CBDict.items.value][0][CBDict.properties.value].keys()
			cols = list(cols)
			cols.append('uuid')
			print(cols)
			print('Total items: {}\nTotal Pages: {}'.format(total_items, number_of_pages))
			data_list = []
			for j in range(0, number_of_pages):
				self.data = CM.get_api_data(url.format(j + 1))
				data = self.data.json()[CBDict.data.value][CBDict.items.value]
				print(j, '*' * j, len(data))
				for i in range(0, len(data)):
					dt = data[i][CBDict.properties.value]
					dt[CBDict.uuid.value] = data[i][CBDict.uuid.value]
					data_list.append(dt)
			df = pd.DataFrame(data_list, columns=cols)
			df.to_csv(self.file_name.format(object_name, str(time.time())), sep=',', columns=cols, index=False)
			print('File saved successfully!')
		else:
			print('SNAP! Something goes wrong.\nSTATUS: {}\nMESSAGE: {}'.format(self.data.json()[0]['status'], self.data.json()[0]['message']))

	def save_organization_summary_data(self):
		try:
			print(os.getcwd(), self.path)
			df = pd.read_csv('CB_ORGANIZATION_SUMMARY_1523546562.095965.csv')
			#df = pd.read_csv('CB_people_summary_1513794895.csv')
			#df = pd.read_csv('CB_CATEGORIES_1514474648.46174.csv')
			# df = pd.read_csv('CB_LOCATIONS_1514478442.280132.csv')
			print('{}\n{}'.format(len(df), df.columns))
			# print(df.head(10))
			df = df[self.org_summary_col]
			print('Total number of Organizations: {}'.format(len(df)))
			# db.save_data_chunk(d, enum.SQL.sql_organizations_insert.value, chunk_size=1000)
		except FileNotFoundError as f:
			print(f)

	def get_organization_api_url(self):
		df = db.pandas_read(self.enum.SQL.sql_orgs_summary.value)# self.orgs_api_url)
		for c in df.iterrows():
			self.save_orgs_detail(c[1].api_url)

	def get_organization_relationships(self):
		df = db.pandas_read(self.enum.SQL.sql_orgs_summary_select.value)
		for _, c in df.iterrows():
			print('[{}] ....'.format(c['name'].upper()))
			self.save_orgs_relationship(c.api_url)

	def save_orgs_relationship(self, api_url):
		url = api_url + self.api_org_token
		print(url)
		orgs = CM.get_api_data(url)
		if orgs.ok:

			self.org_uuid = orgs.json()[CBDict.data.value][CBDict.uuid.value]
			# save Oraganization detail
			self.save_organization_detail(self.org_uuid, orgs.json()[CBDict.data.value][CBDict.properties.value])

			rs_json = orgs.json()[CBDict.data.value][CBDict.relationships.value]
			# save all the related entities
			# self.save_teams(rs_json['featured_team'], self.org_uuid, TeamStatus.Featured.value)
			# self.save_teams(rs_json['current_team'], self.org_uuid, TeamStatus.Current.value)
			# self.save_teams(rs_json['past_team'], self.org_uuid, TeamStatus.Past.value)
			# self.save_teams(rs_json['board_members_and_advisors'], self.org_uuid, TeamStatus.Board.value)

			self.save_funding_rounds(rs_json['funding_rounds'], self.org_uuid)
			# self.save_investments_invested_in(rs_json['investments'])

			# self.save_relational_entity(rs_json['sub_organizations'], self.org_uuid, self.sql_sub_organization_insert)
			self.save_relational_entity(rs_json[self.enum.CBDict.headquarters.value], self.org_uuid, self.enum.SQL.sql_offices_exists.value, self.enum.SQL.sql_offices_insert.value, self.office_col)
			# if rs_json[self.enum.CBDict.offices.value][self.enum.CBDict.items] is not None:
			# 	self.save_relational_entity(rs_json[self.enum.CBDict.offices.value], self.org_uuid, self.enum.SQL.sql_offices_exists, self.sql_offices_insert, self.office_col)
			self.save_relational_entity(rs_json['categories'], self.org_uuid, self.enum.SQL.sql_org_category_exists.value, self.enum.SQL.sql_org_category_insert.value, self.category_col)
			# self.save_relational_entity(rs_json['founders'], self.org_uuid, self.sql_founders_insert)
			# self.save_relational_entity(rs_json['acquisitions'], self.org_uuid, self.sql_acquisition_insert)
			# self.save_relational_entity(rs_json['acquired_by'], self.org_uuid, self.sql_acquired_insert)
			# self.save_relational_entity(rs_json['ipo'], self.org_uuid, self.sql_ipo_insert)
			# self.save_relational_entity(rs_json['funds'], self.org_uuid, self.sql_funds_insert)
			# self.save_relational_entity(rs_json['websites'], self.org_uuid, self.sql_websites_insert)
			# self.save_relational_entity(rs_json['images'], self.org_uuid, self.sql_image_insert)
			# self.save_relational_entity(rs_json['news'], self.org_uuid, self.sql_news_insert)

			db.execute(self.orgs_summary_update.format(self.org_uuid))

	def save_organization_detail(self, uuid, json_properties):

		# print('{}. UUID: {}'.format(self.i, uuid))
		df = self.db.pandas_read(self.enum.SQL.sql_org_detail_exists.value.format(uuid))
		if len(df) == 0:
			json_properties['org_uuid'] = uuid
			json_properties['company_id'] = None
			json_properties['BasicName'] = None
			json_properties['fetched'] = 0
			df_properties = pd.DataFrame([json_properties], columns=self.org_columns)
			values = CM.df_list(df_properties)
			val = []
			for l, j in enumerate(values[0]):
				if isinstance(values[0][l], list):
					val.append(''.join(str(x) for x in values[0][l]))
				elif isinstance(values[0][l], str):
					val.append(self.common.sql_compliant(values[0][l]))
				else:
					val.append(values[0][l])
			tup = tuple(val)
			# print(tup)
			ival = [val]
			sql_insert = self.enum.SQL.sql_org_short_insert.value.format(tup)
			# print(sql_insert)
			sql_insert = sql_insert.replace('"', '\'').replace('True', '1').replace('False','0')
			# print(sql_insert)
			self.db.execute(sql_insert)
		else:
			print('[{}] exists.'.format(json_properties['name']))

	def save_funding_rounds(self, json, org_uuid):
		try:
			print('Funding Rounds')
			if json[CBDict.paging.value][CBDict.total_items.value] > 0:
				for i in range(int(json[CBDict.paging.value][CBDict.total_items.value])):
					if not self.record_exits(self.enum.SQL.sql_funding_exists.value.format(json[CBDict.items.value][i]['uuid'])):
						self.save_relational_entity(json[CBDict.items.value][i], org_uuid,self.enum.SQL.sql_funding_exists.value, self.enum.SQL.sql_funding_rounds_insert.value, self.col_funding)
		except Exception as ex:
			print(ex)


				# funding_rounds_uuid = json[CBDict.items.value][i][CBDict.uuid.value]
				# # INVESTMENT
				# if CBDict.relationships.value in json[CBDict.items.value][i].keys():
				# 	investments = json[CBDict.items.value][i][CBDict.relationships.value][CBDict.investments.value]
				# 	for j in range(len(investments)):
				# 		investment_uuid = investments[j][CBDict.uuid.value]
				# 		self.push_entity_to_db(investments[j], funding_rounds_uuid, self.sql_investments_insert, investment_uuid, fk_uuid='funding_rounds_uuid')
				# 		# INVESTOR
				# 		if CBDict.relationships.value in investments[i].keys():
				# 			investors = investments[j][CBDict.relationships.value]['investors']
				# 			if CBDict.investors.value in investors.keys():
				# 				investor_uuid = investors[CBDict.uuid.value]
				# 				self.push_entity_to_db(investors, investment_uuid, self.sql_investors_insert, investor_uuid, fk_uuid='investment_uuid')
				# 			# PARTNERS
				# 			partners = investments[j][CBDict.relationships.value]['partners']
				# 			if CBDict.partners.value in partners.keys():
				# 				partner_uuid = partners[CBDict.uuid.value]
				# 				self.push_entity_to_db(partners, investment_uuid, self.sql_investors_insert, partner_uuid, fk_uuid='investment_uuid')
	def save_relational_entity(self, json, org_uuid, sql_exist, sql_insert, columns=[]):
		try:
			if CBDict.properties.value in json.keys():
				uuid = json[CBDict.uuid.value]
				if not self.record_exits(sql_exist.format(uuid)):
					self.push_entity_to_db(json, org_uuid, sql_insert, uuid, 0, self.fk_uuid, columns)
			elif json[CBDict.cardinality.value] == 'OneToOne':
				if int(json['paging']['total_items']) > 0:
					print(json[CBDict.item.value][CBDict.type.value])
					uuid = json[CBDict.item.value][CBDict.uuid.value]
					if not self.record_exits(sql_exist.format(uuid)):
						self.push_entity_to_db(json, org_uuid, sql_insert, uuid, 0, self.fk_uuid, columns)
			elif json[CBDict.cardinality.value] == 'OneToMany':
				if int(json['paging']['total_items']) > 0:
					print(json[CBDict.items.value][0][CBDict.type.value])
					for i in range(int(json['paging']['total_items'])):
						uuid = json[CBDict.items.value][i][CBDict.uuid.value]
						if not self.record_exits(sql_exist.format(uuid)):
							self.push_entity_to_db(json, org_uuid, sql_insert, uuid, i, self.fk_uuid, columns)
		except Exception as ex:
			print(ex)

	def save_investments_invested_in(self, json):
		if json[CBDict.paging.value][CBDict.total_items.value] > 0:
			investment_uuid = json[CBDict.items.value][0][CBDict.uuid.value]
			for i in range(json[CBDict.paging.value][CBDict.total_items.value]):
				self.push_entity_to_db(json[CBDict.items.value][i][CBDict.relationships.value][CBDict.invested_in.value], investment_uuid, self.sql_invested_in_insert, fk_uuid='investment_uuid')

	def save_teams(self, json, org_uuid, status):
		for i in range(int(json[CBDict.paging.value][CBDict.total_items.value])):
			json[CBDict.items.value][i][CBDict.properties.value]['TeamStatus'] = status
			team_uuid = json[CBDict.items.value][i][CBDict.uuid.value]
			self.push_entity_to_db(json[CBDict.items.value][i], org_uuid, self.sql_team_insert, team_uuid)
			person_uuid = json[CBDict.items.value][i][CBDict.relationships.value][CBDict.person.value][CBDict.uuid.value]
			self.push_entity_to_db(json[CBDict.items.value][i][CBDict.relationships.value][CBDict.person.value], team_uuid, self.sql_person_insert, person_uuid, fk_uuid='team_uuid')

	def push_entity_to_db(self, json, org_uuid, sql_insert, uuid, i=0, fk_uuid='org_uuid', columns=[]):
		try:
			json_properties = None
			if CBDict.properties.value in json.keys():
				json_properties = json[CBDict.properties.value]
			elif json[CBDict.cardinality.value] == 'OneToOne':
				json_properties = json[CBDict.item.value][CBDict.properties.value]
			elif json[CBDict.cardinality.value] == 'OneToMany':
				json_properties = json[CBDict.items.value][i][CBDict.properties.value]
			if 'uuid' not in json_properties.keys():
				json_properties['uuid'] = uuid
			if fk_uuid not in json_properties.keys():
				json_properties[fk_uuid] = org_uuid
			# print(list(json_properties.keys()))
			df_properties = pd.DataFrame([json_properties], columns=json_properties.keys())
			if len(columns) > 0:
				df_properties = df_properties[columns]
			values = CM.df_list(df_properties)
			val = []
			for l, j in enumerate(values[0]):
				if isinstance(values[0][l], list):
					val.append(' , '.join(str(x) for x in values[0][l]))
				elif isinstance(values[0][l], str):
					val.append(self.common.sql_compliant(values[0][l]))
				else:
					val.append(values[0][l])
			db.bulk_insert(sql_insert, [val])
		except Exception as ex:
			print(ex)

	@staticmethod
	def generate_columns(prprty):
		if int(prprty['paging']['total_items']) == 0:
			print('MISSING')
		elif int(prprty['paging']['total_items']) == 1:
			if prprty[CBDict.cardinality.value] == 'OneToOne':
				print('{} ----> {}'.format(prprty[CBDict.item.value][CBDict.type.value], list(prprty[CBDict.item.value][CBDict.properties.value].keys())))
			elif prprty[CBDict.cardinality.value] == 'OneToMany':
				if isinstance(prprty[CBDict.items.value], list):
					print('{} ---> {}'.format(prprty[CBDict.items.value][0]['type'], list(prprty[CBDict.items.value][0]['properties'].keys())))
				else:
					print('{} ----> {}'.format(prprty[CBDict.items.value][CBDict.type.value], list(prprty[CBDict.items.value][CBDict.properties.value].keys())))
		elif int(prprty['paging']['total_items']) > 1:
			print('{} ----> {}'.format(prprty[CBDict.items.value][0][CBDict.type.value], list(prprty[CBDict.items.value][0][CBDict.properties.value].keys())))

	def get_basic_name(self):
		self.db.update_basic_name(self.enum.SQL.sql_cb_basic_company.value,
								  'org_uuid',
								  'name',
								  self.enum.SQL.sql_cb_basic_company_update.value)

if __name__ == '__main__':
	crb = Crunchbase()
	# crb.get_organization_relationships()
	# crb.get_organizations()
	# crb.save_organization_summary_data()
	# crb.get_organization_api_url()
	# crb.get_organization_relationships()
	crb.get_basic_name()
