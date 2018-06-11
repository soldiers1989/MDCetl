from Shared.db import DB as db
from VentureMatch.validate import Validate as validate



class Exact:
    def __init__(self):
        pass

    @classmethod
    def match(self, source_table):
        """
        Find exact matches between Venture table and a secondary source
        :param: sourcetable: the name of the secondary source table
        :return: dictionary of secondary source ventures
        """

        data = db.pandas_read(
            "SELECT * FROM MDC_DEV.dbo.ProcessedVenture").to_dict('index') #WHERE CRANumber NOT NULL OR Email NOT NULL").to_dict()
        sql = 'SELECT * FROM %s'%source_table
        source = db.pandas_read(sql).to_dict('index')

        for index1, value1 in data.items():
            for index2, value2 in source.items():
                if value1['Name'] == value2['Name'] and value1['ID'] != value2['ID']: #value1['Email'] == value2['Email'] or value1['CRA'] == value2['CRA'] or
                    # One record is in the database and the other is not in the database (-ve ID)
                    if value2['ID'] > value1['ID'] and value2['ID'] > 0:
                        print('\nrecord 1: ', value2, '\nrecord 2: ', value1,'\n')
                        validate.duplicate_existing_new(value2, value1, source_table)
                    else:
                        print('record 1: ', value1, '\nrecord 2: ', value2,'\n')
                        validate.duplicate_existing_new(value1, value2, source_table)
                    break
