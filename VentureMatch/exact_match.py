from Shared.db import DB as db
from VentureMatch.validate import Validate as validate
from urllib.parse import urlparse


class Exact:
    def __init__(self):
        self.source_table = 'MDC_DEV.dbo.SourceTable'
        self.valid = validate()

    def match(self):
        """
        Find exact matches between Venture table and a secondary source
        :param: sourcetable: the name of the secondary source table
        :return: dictionary of secondary source ventures
        """

        data = db.pandas_read(
            "SELECT * FROM MDC_DEV.dbo.ProcessedVenture").to_dict(
            'index')  # WHERE CRANumber NOT NULL OR Email NOT NULL").to_dict()
        source = db.pandas_read('SELECT * FROM MDC_DEV.dbo.SourceTable').to_dict('index')
        while True:
            choice = input('Would you like to turn update mode on? (y)es or (n)o \n')
            if choice.lower() not in ('y', 'n'):
                continue
            else:
                break
        if choice is 'y':
            update_mode = True
        else:
            update_mode = False

        for index1, value1 in source.items():
            for index2, value2 in data.items():
                # Checks if name is exact match and if website url is exact match (ignoring scheme (http,https) and path)
                if str(value1['Name']).lower() == str(value2['Name']).lower() and value1['ID'] != value2['ID']:  # value1['Email'] == value2['Email'] or value1['CRA'] == value2['CRA'] or
                    # One record is in the database and the other is not in the database (-ve ID)
                    if value2['ID'] > value1['ID']:
                        print('\nrecord 1: ', value2, '\nrecord 2: ', value1, '\n')
                        self.valid.duplicate_existing_new(value2, value1, update_mode)
                    else:
                        print('record 1: ', value1, '\nrecord 2: ', value2, '\n')
                        self.valid.duplicate_existing_new(value1, value2, update_mode)
                    break
                elif str(value1['BasicName']) == str(value2['BasicName']) and value1['ID'] != value2['ID']:  # value1['Email'] == value2['Email'] or value1['CRA'] == value2['CRA'] or
                    # One record is in the database and the other is not in the database (-ve ID)
                    if value2['ID'] > value1['ID']:
                        print('\nrecord 1: ', value2, '\nrecord 2: ', value1, '\n')
                        self.valid.duplicate_existing_new(value2, value1, update_mode)
                    else:
                        print('record 1: ', value1, '\nrecord 2: ', value2, '\n')
                        self.valid.duplicate_existing_new(value1, value2, update_mode)
                    break
                elif value1['Website'] is not None:
                    if urlparse(value1['Website'])[1] == urlparse(value2['Website'])[1] and value1['ID'] != value2['ID']:
                        # One record is in the database and the other is not in the database (-ve ID)
                        if value2['ID'] > value1['ID']:
                            print('\nrecord 1: ', value2, '\nrecord 2: ', value1, '\n')
                            self.valid.duplicate_existing_new(value2, value1, update_mode)
                        else:
                            print('record 1: ', value1, '\nrecord 2: ', value2, '\n')
                            self.valid.duplicate_existing_new(value1, value2, update_mode)
                        break