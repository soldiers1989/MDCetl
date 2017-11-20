import datetime
import os
import re
from configparser import ConfigParser
from dateutil import parser
from dateutil.parser import parse

#TODO: review all patterns
#TODO: re-implement get_config method.


class Common:
    def __init__(self):
        sql_get_max = Common.get_config('sql_statement.ini', 'db_sql_common', 'sql_get_max')
        user_response_yes = ['y', 'yes']
        user_response_yesno = ['y', 'yes', 'n', 'no']
        Provinces = ['ON', 'QC', 'NS', 'NB', 'MB', 'BC', 'PE', 'SK', 'AB', 'NL']
        pc_pattern = '[ABCEGHJ-NPRSTVXY][0-9][ABCEGHJ-NPRSTV-Z]\s*[0-9][ABCEGHJ-NPRSTV-Z][0-9]'
        url_pattern = '^((http[s]?|ftp):\/)?\/?([^:\/\s]+)((\/\w+)*\/)([\w\-\.]+[^#?\s]+)(.*)?(#[\w\-]+)?$'
        email_pattern = '[a-zA-Z0-9+_\-\.]+@[0-9a-zA-Z][.-0-9a-zA-Z]*.[a-zA-Z]+'
        address_pattern = '[ABCEGHJKLMNPRSTVXY][0-9][ABCEGHJKLMNPRSTVWXYZ] ?[0-9][ABCEGHJKLMNPRSTVWXYZ][0-9]'
        suffix = ['Limited', 'Ltd.',  'Ltd', 'ltd', 'Inc.', 'inc', 'Inc', 'Incorporated',
                  'Corp',  'Corp.', 'Corporation', 'Communications', 'Technologies', 'Tech.']
        stage = []
        basic_name = ''
        temp_name = ''

    @staticmethod
    def progress(total, current):
        percent = (current / total) * 100
        return percent

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

    @staticmethod # Not important. May be deprecate it after this iteration (Nov 16,2017)
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
            return Common.basic_name
        return {'error': 'No name found'}

    @staticmethod
    def get_config(config_file, header, item):
        config = ConfigParser()
        path = str(os.path.join(os.path.abspath(os.path.dirname(__file__)))) + '/Config'
        config.read(path, config_file)
        con_str = config.get(header, item)
        return con_str

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
    def print_list(lst):
        print(' | '.join(lst))

    @staticmethod
    def print_list(lst, delimiter):
        print(delimiter.join(lst))

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



