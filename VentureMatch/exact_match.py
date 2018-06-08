from Shared.db import DB as db
from VentureMatch.reconcile import Reconcile as reconcile
from VentureMatch.update_record import Update as update


class Exact:
    def __init__(self):
        pass

    @classmethod
    def match(self):
        """
        Find exact matches between Venture table and a secondary source
        :return: dictionary of secondary source ventures
        """
        #db.execute("SELECT * FROM MDC_DEV.dbo.ProcessedVenture ORDER BY ID")
        duplicate_records = []
        secondary_source = []
        data = db.pandas_read(
            "SELECT * FROM MDC_DEV.dbo.ProcessedVenture").to_dict('index') #WHERE CRANumber NOT NULL OR Email NOT NULL").to_dict()
        source = db.pandas_read("SELECT * FROM MDC_DEV.dbo.ACTiaTargetList").to_dict('index')
        for index1, value1 in data.items():
            for index2, value2 in source.items():
                if value1['Name'] == value2['Name'] and value1['ID'] != value2['ID']: #value1['Email'] == value2['Email'] or value1['CRA'] == value2['CRA'] or
                    # # Both records are already in the database
                    # if value2['ID'] > value1['ID'] > 0:
                    #     print('\nrecord 1: ', value1, '\n', 'record 2: ', value2)
                    #     duplicate_records.append(reconcile.deduplicate_existing(value1, value2))
                    # elif value1['ID'] > value2['ID'] > 0:
                    #     print('\nrecord 1: ', value2, '\n', 'record 1: ', value2)
                    #     duplicate_records.append(reconcile.deduplicate_existing(value2, value1))
                    # One record is in the database and the other is not in the database (-ve ID)
                    if value2['ID'] > value1['ID'] and value2['ID'] > 0:
                        print('\nrecord 1: ', value2, '\n', 'record 2: ', value1)
                        secondary_source.append(reconcile.duplicate_existing_new(value2, value1))
                    #elif value1['ID'] > value2['ID'] and value1['ID'] > 0:
                    else:
                        print('record 1: ', value1, '\n', 'record 2: ', value2)
                        secondary_source.append(reconcile.duplicate_existing_new(value1, value2))
                    # # Matching ventures but both records are from new data (both have -ve ID)
                    # else:
                    #     secondary_source.append(reconcile.duplicate_new(value1, value2))
                    #
                    # # If an exact match is found, no need to include the records in the fuzzy matching process
                    # sql = 'DELETE FROM MDC_DEV.dbo.ProcessedVenture WHERE ID = (?)'
                    # values = [[value1['ID']], [value2['ID']]]
                    # db.bulk_insert(sql, values)
                    break
                    # else:
                    #     continue
                # else:
                #     continue

        # Insert duplicate ventures into VentureDedupe
        sql = 'INSERT INTO MDC_DEV.dbo.DuplicateVenture (CompanyID, DuplicateCompanyID, Name, DuplicateName) VALUES (?,?,?,?)'
        db.bulk_insert(sql, duplicate_records)

        return secondary_source
