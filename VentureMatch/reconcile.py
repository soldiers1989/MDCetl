from Shared.db import DB as db
from VentureMatch.update_record import Update as update
from VentureMatch.new_venture import New as new

"""
This is where you can validate the results of the EntityMap 
    If 2 records are the same enter 'y':
        Option to update historical record
        They will then be 1) removed from the EntityMap and 2) be inserted into the DuplicateVenture table for processing 
    If 2 records are not the same enter 'n':
        They will then be 1) removed from the EntityMap and 2) added to the MatchingFalsePositive table for future reference
    If you are unsure if 2 records are the same or not enter 'u':
        They will be left in the EntityMap for future validation
"""


class Reconcile:
    def rec(self):
        """

        :return: dictionary of secondary source ventures to be loaded into the secondary source table
        """
        print('output reconciling')
        db.execute("SELECT * FROM MDC_DEV.dbo.EntityMap ORDER BY CanonID")
        clustered_data = db.pandas_read("SELECT TOP 20 * FROM MDC_DEV.dbo.EntityMap WHERE ClusterScore >0.98").to_dict(
            'index')
        print('ID, CanonID, ClusterScore, Name, Description, Website, Email, Phone, Address, BatchID')

        duplicate_records = {}
        secondary_source = {}
        for row1, value1 in clustered_data.items():
            if row1 % 2 == 0:
                for row2, value2 in clustered_data.items():
                    if value1['CanonID'] == value2['CanonID'] and value1['ID'] != value2['ID']:
                        print('\n','Are these duplicates?', '\n', value1, '\n', value2)
                        while True:
                            choice = input('(y)es (n)o or (u)nsure \n')
                            if choice.lower() not in ('y', 'n', 'u'):
                                continue
                            else:
                                break
                        # If yes, add records to MDCRaw.CONFIG.DiplicateVenture for validation, remove both records
                        # from EntityMap table
                        if choice == 'y':
                            duplicate_records[row1] = {}
                            # Both records are already in the database
                            if value2['ID'] > value1['ID'] > 0:
                                duplicate_records[row1] = self.deduplicate(value1, value2)
                            elif value1['ID'] > value2['ID'] > 0:
                                duplicate_records[row1] = self.deduplicate(value2, value1)
                            # One record is in the database and the other is not in the database (-ve ID)
                            elif value2['ID'] > value1['ID'] and value2['ID'] > 0:
                                update.update_blanks(value2, value1)
                                value1['ID'] = value2['ID']
                                secondary_source[row1] = value1
                            elif value1['ID'] > value2['ID'] and value1['ID'] > 0:
                                update.update_blanks(value1, value2)
                                value2['ID'] = value1['ID']
                                secondary_source[row1] = value2
                            # Matching ventures but both records are from new data (both have -ve ID)
                            else:
                                # Fill any missing dimensions to create a "full" record
                                value1 = update.update_blanks(value1, value2)
                                # Insert record into Venture Table
                                sql = "INSERT INTO MDC_DEV.dbo.ProcessedVenture (Name, BatchID, Description, Website, Email, Phone, Address) " \
                                      "VALUES ('{Name}',{BatchID},'{Description}','{Website}','{Email}','{Phone}','{Address}')".format(
                                      Name=str(value1['Name']), BatchID=value1['BatchID'], Description=str(value1['Description']),
                                      Website=str(value1['Website']), Email=str(value1['Email']), Phone=str(value1['Phone']), Address=str(value1['Address']))
                                db.execute(sql)
                                secondary_source[row1] = value1

                            sql = 'DELETE FROM MDC_DEV.dbo.EntityMap WHERE ID = (?)'
                            values = [[value1['ID']], [value2['ID']]]
                            db.bulk_insert(sql, values)

                        # If no, add both records to false positives list and remove both records from the EntityMap
                        # table
                        if choice == 'n':
                            values = [
                                [value1['ID'], value1['Name'], value2['ID'], value2['Name'], value1['ClusterScore']],
                                [value2['ID'], value2['Name'], value1['ID'], value1['Name'], value2['ClusterScore']]
                            ]
                            sql = 'INSERT INTO MDC_DEV.dbo.MatchingFalsePositives (ID, Name, FalseID, FalseName, ClusterScore) VALUES (?,?,?,?,?)'  # \
                            # ' WHERE (?,?,?,?) NOT IN (SELECT (ID, Name, FalseID, FalseName) FROM MDC_DEV.dbo.MatchingFalsePositives)'
                            db.bulk_insert(sql, values)
                            sql = 'DELETE FROM MDC_DEV.dbo.EntityMap WHERE ID = (?)'
                            values = [[value1['ID']], [value2['ID']]]
                            db.bulk_insert(sql, values)
                            # If a new company is found to be part of a false-positive match, add it to the venture
                            # table as a new record
                            if value1['ID'] < 0 or value2['ID'] < 0:
                                    new.fp_create_new()
                        # If unsure, continue loop and keep both records in entitymap table for future validation
                        else:
                            break
                    else:
                        continue
            else:
                continue

        for row, val in duplicate_records.items():
            print(val)
        return secondary_source

    @staticmethod
    def deduplicate(record1, record2):
        """

        :param record1: Historic record
        :param record2: Duplicate record
        :return:
        """
        duplicate_records = {}
        while True:
            choice = input('Would you like to merge? (y)es or (n)o \n')
            if choice.lower() not in ('y', 'n'):
                continue
            else:
                break
        if choice == 'y':
            # Update record and push the changes to venture table
            record1 = update.update_blanks(record1, record2)
            sql = "UPDATE MDC_DEV.dbo.ProcessedVenture SET Name = '{Name}', BatchID = {BatchID}, Description = '{Description}', Website = '{Website}', " \
                  "Email = '{Email}', Phone = '{Phone}', Address = '{Address}' WHERE ID = {ID}".format(
                      Name=str(record1['Name']), BatchID=record1['BatchID'], Description=str(record1['Description']),
                      Website=str(record1['Website']), Email=str(record1['Email']), Phone=str(record1['Phone']),
                      Address=str(record1['Address']), ID=record1['ID'])
            db.execute(sql)
        duplicate_records['CompanyID'] = record1['ID']
        duplicate_records['DuplicateCompanyID'] = record2['ID']
        duplicate_records['Name'] = record1['Name']
        duplicate_records['DuplicateName'] = record2['Name']
        return duplicate_records
