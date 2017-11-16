import datetime
import os
import re
from configparser import ConfigParser
from dateutil import parser
from dateutil.parser import parse


class Common:
    def __init__(self):
        pass

    user_response_yes = ['y', 'yes']
    user_response_yesno = ['y', 'yes', 'n', 'no']
    Provinces = ['ON', 'QC', 'NS', 'NB', 'MB', 'BC', 'PE', 'SK', 'AB', 'NL']
    pc_pattern = '[ABCEGHJ-NPRSTVXY][0-9][ABCEGHJ-NPRSTV-Z]\s*[0-9][ABCEGHJ-NPRSTV-Z][0-9]'
    url_pattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    email_pattern = '[a-zA-Z0-9+_\-\.]+@[0-9a-zA-Z][.-0-9a-zA-Z]*.[a-zA-Z]+'
    address_pattern = '[ABCEGHJKLMNPRSTVXY][0-9][ABCEGHJKLMNPRSTVWXYZ] ?[0-9][ABCEGHJKLMNPRSTVWXYZ][0-9]'
    suffix = ['Limited', 'Ltd.',  'Ltd', 'ltd', 'Inc.', 'Corp', 'Inc', 'Incorporated', 'Corp.', 'Corporation']
    stage = []
    basic_name = ''
    temp_name = ''

    @staticmethod
    def progress(total, current):
        percent = (current / total) * 100
        return percent

    @staticmethod
    def fiscal_year_quarter():
        month = datetime.date.today().month
        year = datetime.date.today().year
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
    def ispostalcode(pc):
        pattern = re.compile(Common.pc_pattern, re.IGNORECASE)
        res = pattern.match(pc)
        if res:
            return True
        else:
            return False

    @staticmethod
    def isurl(url):
        pattern = re.compile(Common.url_pattern, re.IGNORECASE)
        res = pattern.match(url)
        if res:
            return True
        else:
            return False

    @staticmethod
    def iscanadianaddress(url):
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
        return ''

    @staticmethod
    def get_config(header, item):
        config = ConfigParser()
        config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'Config.ini'))
        con_str = config.get(header, item)
        return con_str

    @staticmethod
    def get_guid():
        config = ConfigParser()
        config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'Config.ini'))
        guid = config.get('batch_guid', 'guid')
        return guid

    @staticmethod
    def get_company_age(dateofincorporation):
        if dateofincorporation is not None or dateofincorporation is not '':
            if len(dateofincorporation) >= 10:
                quarter_two_end = datetime.datetime.strptime('2017-09-30', '%Y-%m-%d')
                dinc = datetime.datetime.strptime(dateofincorporation[:10], '%Y-%m-%d')
                return (quarter_two_end.year - dinc.year) * 12 + \
                       (quarter_two_end.month - dinc.month)
            else:
                return None
        else:
            return None

    @staticmethod
    def change_date_format(idate):
        if idate is not None or idate is not '':
            if len(idate) >= 8:
                if idate[:4] != '0000':
                    d = parser.parse(idate)
                    day = d.day if d.day >= 10 else '0{}'.format(d.day)
                    month = d.month if d.month >= 10 else '0{}'.format(d.month)
                    return '{}-{}-{}'.format(d.year, month, day)
                else:
                    return None
            else:
                return None
        else:
            return None

    @staticmethod
    def replace(here, this, that):
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

