import datetime
import os
import re
import requests
from configparser import ConfigParser
from dateutil import parser
from dateutil.parser import parse
from Shared.enums import DataSourceType, CONSTANTS, PATH, \
	StageLevel as stg, DealType as dt, MaRSProgram, \
	MaRSSector, CAIPStatus, Stage
import pandas as pd
# import dns.resolver
import socket
import smtplib


class Common:

	user_response_yes = ['y', 'yes']
	user_response_yesno = ['y', 'yes', 'n', 'no']
	Provinces = ['ON', 'QC', 'NS', 'NB', 'MB', 'BC', 'PE', 'SK', 'AB', 'NL']
	pc_pattern = '[ABCEGHJ-NPRSTVXY][0-9][ABCEGHJ-NPRSTV-Z]\s*[0-9][ABCEGHJ-NPRSTV-Z][0-9]'
	url_pattern = '^((http[s]?|ftp):\/)?\/?([^:\/\s]+)((\/\w+)*\/)([\w\-\.]+[^#?\s]+)(.*)?(#[\w\-]+)?$'
	email_pattern = '[a-zA-Z0-9+_\-\.]+@[0-9a-zA-Z][.-0-9a-zA-Z]*.[a-zA-Z]+'
	address_pattern = '[ABCEGHJKLMNPRSTVXY][0-9][ABCEGHJKLMNPRSTVWXYZ] ?[0-9][ABCEGHJKLMNPRSTVWXYZ][0-9]'
	suffix = ['Limited', 'Ltd.',  'Ltd', 'ltd', 'Incorporated', 'Inc.', 'inc', 'Inc','Corporation', 'Corp.', 'Corp', 'Technologies']
			  #['Communications', 'Technologies', 'Technology','Tech.','Industry','Industries','Enterprise']

	stage = ['idea', 'discovery', 'validation', 'efficiency', 'scale']
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
	def convert_yes_no(val):
		value = val.lower() if val is not None else ''
		if value == 'yes':
			return 1
		elif value == 'no':
			return 0
		else:
			return None

	@staticmethod
	def get_basic_name(name):
		Common.temp_name = name
		if name is not None and name != '':
			for sf in Common.suffix:
				Common.temp_name = re.sub(sf, '', Common.temp_name)
			Common.basic_name = re.sub('[^A-Za-z0-9]+', '', Common.temp_name).lower()
			return Common.basic_name
		return {'error': 'No name found'}

	@staticmethod
	def generate_basic_name(df):
		df['BasicName'] = df.apply(lambda dfs: Common.get_basic_name(dfs.Name), axis=1)
		return df

	@staticmethod
	def get_cvca_deal_types(deal_types):
		delas_sector = None
		deal = deal_types.lower()# if deal_types is not None else ''
		deal = deal.replace("- ", "")
		try:
			if deal == '':
				deals_sector = None
			elif 'bridge' in deal:
				deals_sector = dt.Bridge_VC.value
			elif 'early' in deal:
				deals_sector = dt.Early_Stage_VC.value
			elif 'exits vc' in deal:
				deals_sector = dt.Exits_VC.value
			elif 'later stage' in deal:
				deals_sector = dt.Later_Stage_VC.value
			elif 'other vc' in deal:
				deals_sector = dt.Other_VC.value
			elif 'pe add on' in deal:
				deals_sector = dt.PE_Add_on.value
			elif 'pe buyout' in deal:
				deals_sector = dt.PE_Buyout.value
			elif 'pe debt' in deal:
				deals_sector = dt.PE_Debt.value
			elif 'pe follow on' in deal:
				deals_sector = dt.PE_Follow_on.value
			elif 'pe growth' in deal:
				deals_sector = dt.PE_Growth.value
			elif 'pe infrastructure' in deal:
				deals_sector = dt.PE_Infrastructure.value
			elif 'pe private placement' in deal:
				deals_sector = dt.PE_Private_Placement.value
			elif 'pe privatization' in deal:
				deals_sector = dt.PE_Privatization.value
			elif 'pe recap' in deal:
				deals_sector = dt.PE_Recap.value
			elif 'pe secondary buyout' in deal:
				deals_sector = dt.PE_Secondary_Buyout.value
			elif 'pe secondary sale' in deal:
				deals_sector = dt.PE_Secondary_Sale.value
			elif 'pipe' in deal:
				deals_sector = dt.PIPE.value
			elif 'seed vc' in deal:
				deals_sector = dt.Seed_VC.value
			elif 'venture debt vc' in deal:
				deals_sector = dt.Venture_Debt_VC.value
			elif 'exits pe' in deal:
				deals_sector = dt.Exits_PE.value
			elif 'pe-backed ipo/rto' in deal:
				deals_sector = dt.PE_backed_IPO_RTO.value
			elif 'vc-backed ipo/rto' in deal:
				deals_sector = dt.VC_backed_IPO_RTO.value
			else:
				deals_sector = None
			return deals_sector
		except Exception as ex:
			print(ex)

	@staticmethod
	def convert_province_to_id(province):
		current_province = None
		pro = province.lower() if province is not None else ''
		if pro == 'alberta':
			current_province = 1
		elif pro == 'british columbia':
			current_province = 2
		elif pro == 'manitoba':
			current_province = 3
		elif pro == 'new brunswick':
			current_province = 4
		elif pro == 'Newfoundland and Labrador':
			current_province = 5
		elif pro == 'Northwest Territories':
			current_province = 6
		elif pro == 'Nova Scotia':
			current_province = 7
		elif pro == 'Nunavut':
			current_province = 8
		elif pro == 'Ontario':
			current_province = 9
		elif pro == 'Prince Edward Island':
			current_province = 10
		elif pro == 'Quebec':
			current_province = 11
		elif pro == 'Saskatchewan':
			current_province = 12
		elif pro == 'Saskatchewan':
			current_province = 13
		else:
			current_province = None

		return current_province

	@staticmethod
	def convert_province_to_num(province):
		current_province = None
		pro = province.lower() if province is not None else ''
		if 'alberta' in pro:
			current_province = 1
		elif 'british' in pro or 'bc' in pro:
			current_province = 2
		elif 'manitoba' in pro:
			current_province = 3
		elif 'brunswick' in pro:
			current_province = 4
		elif 'newfoundland' in pro:
			current_province = 5
		elif 'northwest' in pro:
			current_province = 6
		elif 'scotia' in pro:
			current_province = 7
		elif 'nunavut' in pro:
			current_province = 8
		elif 'ontario' in pro:
			current_province = 9
		elif 'prince' in pro or 'pei' in pro or 'island' in pro:
			current_province = 10
		elif 'quebec' in pro or 'québec' in pro:
			current_province = 11
		elif 'saskatchewan' in pro:
			current_province = 12
		elif 'yukon' in pro:
			current_province = 13
		else:
			current_province = None

		return current_province

	@staticmethod
	def scientific_to_decimal(number):
		if number is not None:
			dec = float(number)
			print('{} *----> {}'.format(number, str(dec)))
			return str(dec)

	@staticmethod
	def get_stage_level(stage):
		st = str(stage).lower() if stage is not None else ''
		company_stage = None
		if st == '':
			company_stage = None
		if 'idea' in st:
			company_stage = Stage.IDEA.value
		elif 'discovery' in st:
			company_stage = Stage.DISCOVERY.value
		elif 'validation' in st:
			company_stage = Stage.VALIDATION.value
		elif 'efficiency' in st:
			company_stage = Stage.EFFICIENCY.value
		elif 'scale' in st:
			company_stage = Stage.SCALE.value

		if company_stage is not None:
			return str(company_stage)
		else:
			return company_stage

	@staticmethod
	def get_mars_program(program):
		prg = str(program).lower() if program is not None else ''
		mars_program = None
		if 'start' in prg:
			mars_program = MaRSProgram.Start.value
		elif 'growth' in prg:
			mars_program = MaRSProgram.Growth.value
		elif 'scale' in prg:
			mars_program = MaRSProgram.Scale.value

		return mars_program

	@staticmethod
	def get_mars_sector(sector):
		sec = str(sector).lower() if sector is not None else ''
		mars_sector = None
		if 'clean' in sec:
			mars_sector = MaRSSector.Cleantech.value
		elif 'finance' in sec or 'commerce' in sec:
			mars_sector = MaRSSector.Fiance_and_Commerce.value
		elif 'health' in sec:
			mars_sector = MaRSSector.Health.value
		elif 'work' in sec or 'learn' in sec:
			mars_sector = MaRSSector.Work_and_Learning.value

		return mars_sector

	@staticmethod
	def get_caip_status(caip_status):
		caip = str(caip_status).lower() if caip_status is not None else ''
		mars_caip = None
		if 'is caip' in caip:
			mars_caip = CAIPStatus.IsCAIP.value
		elif 'was caip' in caip:
			mars_caip = CAIPStatus.WasCAIP.value
		elif 'never caip' in caip:
			mars_caip = CAIPStatus.NeverCAIP.value

		return mars_caip

	@staticmethod
	def get_yes_no(val):
		v = val.lower() if val is not None else None
		yes_no = None
		if v is None:
			yes_no = None
		elif 'y' in v:
			yes_no = 'Y'
		elif 'n' in v:
			yes_no = 'N'

		if yes_no is None:
			return yes_no
		else:
			return yes_no

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

	# def get_table_seed(self, table, id_column):
	# 	seed = 0
	# 	sql_dc = sql.sql_get_max_id.value.format(id_column, table)
	# 	df = db.pandas_read(sql_dc)
	# 	if len(df) > 0:
	# 		seed = df.values[0][0]
	# 	return seed

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
	def sql_compliant(strs):
		complaint = str(strs).replace("'", "\'\'")
		complaint: str = str(complaint).replace('"', '')
		return complaint

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
		elif 'ssmic' in file or 'sault' in file:
			d_source = DataSourceType.SSMIC.value
		elif 'noic' in file or 'nwoic' in file or 'nwo' in file:
			d_source = DataSourceType.NWOIC.value
		elif 'alliance' in file:
			d_source = DataSourceType.TECH_ALLIANCE.value
		elif 'wetec' in file:
			d_source = DataSourceType.WE_TECH.value
		else:
			d_source = None

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
	def get_api_data_old(url, user_key, attempts=5):
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
	def get_api_data(url):
		try:
			response = requests.request(CONSTANTS.get.value, url)
			return response
		except requests.RequestException as e:
			print(e)
		except requests.HTTPError as h:
			print(h)
		except requests.ConnectionError as ex:
			print(ex)

	@staticmethod
	def change_location(loc):
		path = Common.get_config('config.ini', 'box_file_path', 'path_bap_source')
		path_qa = Common.get_config('config.ini', 'box_file_path', 'path_bap_qa')
		path_combined = Common.get_config('config.ini', 'box_file_path', 'path_bap_combined')
		path_etl = Common.get_config('config.ini', 'box_file_path', 'path_bap_etl')
		path_match = Common.get_config('config.ini', 'box_file_path', 'path_bap_company_matching')
		path_other = Common.get_config('config.ini', 'box_file_path', 'path_other')
		path_mars_fix = '/Users/mnadew/Box Sync/Innovation Economy/Projects/Survey FY18 Planning/Templates/RIC target lists completed/RIC target list corrections'

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
		elif loc == PATH.FASTLANE:
			other_path = os.path.join(os.path.expanduser('~'), path_other)
			os.chdir(other_path)
			return other_path
		elif loc == PATH.MaRS_FIX:
			mars_fix_path = os.path.join(os.path.expanduser('~'), path_mars_fix)
			os.chdir(mars_fix_path)

	@staticmethod
	def change_working_directory(path):
		new_dir = os.path.join(os.path.expanduser('~'), path)
		os.chdir(new_dir)
		return new_dir

	@staticmethod
	def change_series_type(sr, dtype):
		sr.astype(dtype)
		print(sr)

	@staticmethod
	def make_directory(directory_name):
		if not os.path.exists(directory_name):
			os.makedirs(directory_name)

	@staticmethod
	def generate_basic_name(df, name):
		df['BasicName'] = df.apply(lambda dfs: Common.get_basic_name(dfs[name]), axis=1)
		return df

	@staticmethod
	def valid_email(email):
		pattern = re.compile('^[_a-z0-9-]+(\.[_a-z0-9-]+)*@[a-z0-9-]+(\.[a-z0-9-]+)*(\.[a-z]{2,4})$')
		match = re.match(pattern, email)
		if match is None:
			return False
		else:
			return True

	# @staticmethod
	# def verify_email_exists(email):
	# 	try:
	# 		_, domain = email.split('@')
	# 		records = dns.resolver.query(domain, 'MX')
	# 		mxRecord = records[0].exchange
	# 		mxRecord = str(mxRecord)
	# 		#Get local server hostname
	# 		host = socket.gethostbyname(mxRecord)
	# 		#SMTP lib setup
	# 		server = smtplib.SMTP()
	# 		server.set_debuglevel(0)
	# 		#SMTP Conversation
	# 		server.connect(mxRecord)
	# 		server.helo(host)
	# 		server.mail(email)
	# 		code, message = server.rcpt(str(email))
	# 		server.quit()
	# 		if code == 250:
	# 			return True
	# 		else:
	# 			return False
	# 	except ConnectionRefusedError:
	# 		return False
	# 	except smtplib.SMTPServerDisconnected:
	# 		return False
	# 	except dns.resolver.NoAnswer:
	# 		return False
	# 	except dns.resolver.NXDOMAIN:
	# 		return False
	# 	except dns.resolver.NoNameservers:
	# 		return False
	# 	except socket.error:
	# 		return False

	@staticmethod
	def convert_float_todate(fl):# Don't use
		serial = 43111.0
		seconds = (serial - 25569) * 86400.0
		return datetime.datetime.utcfromtimestamp(seconds)

	@staticmethod
	def get_cert_path():
		path = os.path.join(os.path.dirname(__file__), 'MaRSDD-Root.pem')
		return path

	@staticmethod
	def xl_to_dfs(path, filename):
		"""Reads xlsx file and stores each sheet in df in dict."""
		xl = pd.ExcelFile(path + filename)
		sheets = xl.sheet_names
		dfs = {}
		for sheet in sheets:
			df = xl.parse(sheet)
			dfs[sheet] = df
		return dfs

	@staticmethod
	def distinct_from_df(df, col):
		"""Returns list of distinct values in given column, excluding 'nan'.
		DataFrame, str -> list
		"""
		lst = list(df[col].dropna().unique())
		return lst

	@staticmethod
	def venture_name_with_bracket_split(string):
		bracket_string = re.findall('\(.*?\)', string)[0]
		main_string = re.sub(' +',' ',string.replace(bracket_string, ''))
		return [main_string, bracket_string]

	@staticmethod
	def is_numeric(value):
		try:
			float(value)
			return True
		except ValueError:
			return False


