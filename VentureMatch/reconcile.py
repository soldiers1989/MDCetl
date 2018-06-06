from Shared.db import DB as db


class Reconcile:
    @staticmethod
    def rec():
        print('output reconciling')
        db.execute("SELECT * FROM MDC_DEV.dbo.EntityMap ORDER BY CanonID")
        clustered_data = db.pandas_read("SELECT TOP 100 * FROM MDC_DEV.dbo.EntityMap WHERE ClusterScore >0.50").to_dict(
            'index')
        print('ID, CanonID, ClusterScore, Name, Description, Website, Email, Phone, Address, BatchID')

        merged_records = {}

        for row1, value1 in clustered_data.items():
            if row1 % 2 == 0:
                for row2, value2 in clustered_data.items():
                    if value1['CanonID'] == value2['CanonID'] and value1['ID'] != value2['ID']:
                        print('would you like to merge?', '\n', value1, '\n', value2)
                        while True:
                            choice = input('(y)es (n)o or (u)nsure \n')
                            if choice.lower() not in ('y', 'n', 'u'):
                                continue
                            else:
                                break
                        # If yes, add records to MDCRaw.CONFIG.DiplicateVenture for validation, remove both records
                        # from entitymap table
                        if choice == 'y':
                            merged_records[row1] = {}
                            if value1['ID'] < value2['ID']:
                                merged_records[row1]['CompanyID'] = value1['ID']
                                merged_records[row1]['DuplicateCompanyID'] = value2['ID']
                                merged_records[row1]['Name'] = value1['Name']
                                merged_records[row1]['DuplicateName'] = value2['Name']
                            else:
                                merged_records[row1]['CompanyID'] = value2['ID']
                                merged_records[row1]['DuplicateCompanyID'] = value1['ID']
                                merged_records[row1]['Name'] = value2['Name']
                                merged_records[row1]['DuplicateName'] = value1['Name']
                            sql ='DELETE FROM MDC_DEV.dbo.EntityMap WHERE ID = (?)'
                            values =[[value1['ID']], [value2['ID']]]
                            db.bulk_insert(sql,values)
                        # If no, add both records to false positives list and remove both records from the entitymap
                        # table
                        if choice == 'n':
                            values = [
                                [value1['ID'], value1['Name'], value2['ID'], value2['Name'], value1['ClusterScore']],
                                [value2['ID'], value2['Name'], value1['ID'], value1['Name'], value2['ClusterScore']]
                            ]
                            sql = 'INSERT INTO MDC_DEV.dbo.MatchingFalsePositives (ID, Name, FalseID, FalseName, ClusterScore) VALUES (?,?,?,?,?)' #\
                                  #' WHERE (?,?,?,?) NOT IN (SELECT (ID, Name, FalseID, FalseName) FROM MDC_DEV.dbo.MatchingFalsePositives)'
                            db.bulk_insert(sql,values)
                            sql ='DELETE FROM MDC_DEV.dbo.EntityMap WHERE ID = (?)'
                            values =[[value1['ID']], [value2['ID']]]
                            db.bulk_insert(sql,values)
                        # If unsure, continue loop and keep both records in entitymap table for future validation
                        else:
                            break

                    else:
                        continue
            else:
                continue

        for row, val in merged_records.items():
            print(val)