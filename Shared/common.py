import datetime
import os
import re
import requests

from configparser import ConfigParser
from dateutil import parser
from dateutil.parser import parse
from Shared.enums import DataSourceType, CONSTANTS, PATH
import pandas as pd


class Common:

	# sql_get_max = Common.get_config('sql_statement.ini', 'db_sql_common', 'sql_get_max')
	user_response_yes = ['y', 'yes']
	user_response_yesno = ['y', 'yes', 'n', 'no']
	Provinces = ['ON', 'QC', 'NS', 'NB', 'MB', 'BC', 'PE', 'SK', 'AB', 'NL']
	pc_pattern = '[ABCEGHJ-NPRSTVXY][0-9][ABCEGHJ-NPRSTV-Z]\s*[0-9][ABCEGHJ-NPRSTV-Z][0-9]'
	url_pattern = '^((http[s]?|ftp):\/)?\/?([^:\/\s]+)((\/\w+)*\/)([\w\-\.]+[^#?\s]+)(.*)?(#[\w\-]+)?$'
	email_pattern = '[a-zA-Z0-9+_\-\.]+@[0-9a-zA-Z][.-0-9a-zA-Z]*.[a-zA-Z]+'
	address_pattern = '[ABCEGHJKLMNPRSTVXY][0-9][ABCEGHJKLMNPRSTVWXYZ] ?[0-9][ABCEGHJKLMNPRSTVWXYZ][0-9]'
	suffix = ['Limited', 'Ltd.',  'Ltd', 'ltd', 'Inc.', 'inc', 'Inc', 'Incorporated', 'Corp',  'Corp.', 'Corporation', 'Communications', 'Technologies', 'Tech.']
	stage = []
	basic_name = ''
	temp_name = ''

	@staticmethod
	def progress(char, index, total):
		pass

	@staticmethod
	def fiscal_year_quarter(dt=datetime.date.today()):
		month = dt.today().month
		year = dt.today().year
		if month <= 3:
			fy = year
		elif month > 3:
			fy = year + 1

		if 4 <= month <= 6:
			fq = 1
		elif 7 <= month <= 9:
			fq = 2
		elif 10 <= month <= 12:
			fq = 3
		elif 1 <= month <= 3:
			fq = 4

		return fy, fq

	@staticmethod
	def get_dateid(datevalue):
		if datevalue is None:
			dat = datetime.datetime.utcnow()
			month = dat.month if dat.month >= 10 else '0{}'.format(dat.month)
			dayvalue = dat.day if dat.day >= 10 else '0{}'.format(dat.day)
			return f'{dat.year}{month}{dayvalue}'
		else:
			month = datevalue.month if datevalue.month >= 10 else '0{}'.format(datevalue.month)
			dayvalue = datevalue.day if datevalue.day >= 10 else '0{}'.format(datevalue.day)

			return f'{datevalue.year}{month}{dayvalue}'

	@staticmethod
	def is_postal_code(pc):
		pattern = re.compile(Common.pc_pattern, re.IGNORECASE)
		res = pattern.match(pc)
		if res:
			return True
		else:
			return False

	@staticmethod
	def is_url(url):
		pattern = re.compile(Common.url_pattern, re.IGNORECASE)
		res = pattern.match(url)
		if res:
			return True
		else:
			return False

	@staticmethod  # Not important. May be deprecate it after this iteration (Nov 16,2017)
	def is_canadian_address(url):
		pattern = re.compile(Common.address_pattern, re.IGNORECASE)
		res = pattern.match(url)
		if res:
			return True
		else:
			return False

	@staticmethod
	def is_date(dt):
		try:
			parse(str(dt))
			return True
		except ValueError:
			return False

	@staticmethod
	def get_basic_name(name):
		Common.temp_name = name
		if name is not None:
			for sf in Common.suffix:
				Common.temp_name = re.sub(sf, '', Common.temp_name)
			Common.basic_name = re.sub('[^A-Za-z0-9]+', '', Common.temp_name).lower()
			print(Common.basic_name)
			return Common.basic_name
		return {'error': 'No name found'}

	@staticmethod
	def get_config(config_file, header, item):
		try:
			config = ConfigParser()
			config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), config_file))
			config_value = config.get(header, item)
			return config_value
		except Exception as ex:
			print(ex)
			return None

	@staticmethod
	def get_sql(header, item):
		print(os.getcwd())
		path = Common.get_config(header, item)
		box_path = os.path.join(os.path.expanduser("~"), path)
		os.chdir(box_path)

	@staticmethod
	def get_company_age(dateofincorporation):
		if dateofincorporation is not None or dateofincorporation is not '':
			if Common.is_date(dateofincorporation):
				today = datetime.date.today()
				d_inc = datetime.datetime.strptime(dateofincorporation[:10], '%Y-%m-%d')
				return (today.year - d_inc.year) * 12 + \
					   (today.month - d_inc.month)
			else:
				return None
		else:
			return None

	@staticmethod
	def get_company_age(dateofincorporation, quarter_date):
		if dateofincorporation is not None or dateofincorporation is not '':
			if Common.is_date(dateofincorporation):
				quarter_two_end = datetime.datetime.strptime(quarter_date, '%Y-%m-%d')
				dinc = datetime.datetime.strptime(dateofincorporation[:10], '%Y-%m-%d')
				return (quarter_two_end.year - dinc.year) * 12 + \
					   (quarter_two_end.month - dinc.month)
			else:
				return None
		else:
			return None

	@staticmethod
	def change_date_format(date):
		if Common.is_date(date):
			d = parser.parse(date)
			day = d.day if d.day >= 10 else '0{}'.format(d.day)
			month = d.month if d.month >= 10 else '0{}'.format(d.month)
			return '{}-{}-{}'.format(d.year, month, day)
		else:
			return None

	@staticmethod
	def replace_it(this, that, here):
		return re.sub(this, that, here)

	@staticmethod
	def print_list(lst, delimiter='\n'):
		print('+', '-' * 100, '+\n')
		print(delimiter.join(lst))
		print('\n+', '-' * 100, '+')

	@staticmethod
	def apostrophe_name(name):
		nm = name.replace("\'", "\'\'")
		return nm

	@staticmethod
	def get_table_seed(table, id_column):
		seed = 0
		sql_dc = Common.sql_get_max.format(id_column, table)
		df = Common.dal.pandas_read(sql_dc)
		if len(df) > 0:
			seed = df[0].values
		return seed

	@staticmethod
	def sql_friendly(strs):
		lst = []
		for i, c in enumerate(strs):
			if c == '\'':
				lst.append(i)
		for i in range(len(lst)):
			p = lst[i]
			value = strs[:p] + '\'' + strs[p:]
			strs = value
			lst = [x + 1 for x in lst]
		return strs

	@staticmethod
	def set_datasource(file):
		file = re.sub('[^A-Za-z0-9]+', '', file).lower()
		d_source = None
		if 'spark' in file:
			d_source = DataSourceType.SPARK_CENTER.value
		elif 'communitech' in file:
			d_source = DataSourceType.COMMUNI_TECH.value
		elif 'venturelab' in file:
			d_source = DataSourceType.VENTURE_LAB.value
		elif 'haltech' in file:
			d_source = DataSourceType.HAL_TECH.value
		elif 'iion' in file:
			d_source = DataSourceType.IION.value
		elif 'niagara' in file:
			d_source = DataSourceType.INNOVATE_NIAGARA.value
		elif 'guelph' in file:
			d_source = DataSourceType.INNOVATION_GUELPH.value
		elif 'innovationfactory' in file:
			d_source = DataSourceType.INNOVATION_FACTORY.value
		elif 'ottawa' in file:
			d_source = DataSourceType.INVEST_OTTAWA.value
		elif 'launchlab' in file:
			d_source = DataSourceType.LAUNCH_LAB.value
		elif 'mars' in file:
			d_source = DataSourceType.MaRS.value
		elif 'norcat' in file:
			d_source = DataSourceType.NORCAT.value
		elif 'ric' in file:
			d_source = DataSourceType.RIC_CENTER.value
		elif 'ssmic' in file:
			d_source = DataSourceType.SSMIC.value
		elif 'noic' in file:
			d_source = DataSourceType.NWOIC.value
		elif 'alliance' in file:
			d_source = DataSourceType.TECH_ALLIANCE.value
		elif 'wetec' in file:
			d_source = DataSourceType.WE_TECH.value

		return d_source

	@staticmethod
	def df_list(dataframe):
		try:
			df = dataframe.where(pd.notnull(dataframe), None)
			values = df.values.tolist()
			return values
		except ValueError:
			return None

	@staticmethod
	def get_api_data(url, user_key, attempts=5):
		count = 0
		if user_key != '':
			url = url + '?user_key=' + user_key
		for i in range(0, attempts):
			try:
				count = count + 1
				data = requests.get('GET', url)
				if data.ok:
					data = data.json()
					return data
			except requests.RequestException:
				pass

	@staticmethod
	def get_crunch_data(url):
		try:
			response = requests.request(CONSTANTS.get.value, url)
			return response
		except requests.RequestException as e:
			print(e)

	@staticmethod
	def change_location(loc):
		path = Common.get_config('config.ini', 'box_file_path', 'path_bap_source')
		path_qa = Common.get_config('config.ini', 'box_file_path', 'path_bap_qa')
		path_combined = Common.get_config('config.ini', 'box_file_path', 'path_bap_combined')
		path_etl = Common.get_config('config.ini', 'box_file_path', 'path_bap_etl')
		path_match = Common.get_config('config.ini', 'box_file_path', 'path_bap_company_matching')
		path_other = Common.get_config('config.ini', 'box_file_path', 'path_other')

		if loc == PATH.DATA:
			box_path = os.path.join(os.path.expanduser('~'), path)
			os.chdir(box_path)
			return box_path
		elif loc == PATH.QA:
			qa_path = os.path.join(os.path.expanduser('~'), path_qa)
			os.chdir(qa_path)
			return qa_path
		elif loc == PATH.COMBINED:
			com_path = os.path.join(os.path.expanduser('~'), path_combined)
			os.chdir(com_path)
			return com_path
		elif loc == PATH.ETL:
			etl_path = os.path.join(os.path.expanduser('~'), path_etl)
			os.chdir(etl_path)
			return etl_path
		elif loc == PATH.MATCH:
			match_path = os.path.join(os.path.expanduser('~'), path_match)
			os.chdir(path_match)
			return match_path
		elif loc == PATH.OTHER:
			other_path = os.path.join(os.path.expanduser('~'), path_other)
			os.chdir(path_other)
			return other_path

	@staticmethod
	def change_series_type(sr, dtype):
		sr.astype(dtype)
		print(sr)

	@staticmethod
	def make_directory(directory_name):
		if not os.path.exists(directory_name):
			os.makedirs(directory_name)
