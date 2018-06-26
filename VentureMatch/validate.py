from Shared.db import DB as db
from Shared.common import Common as common
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


class Validate:
    def __init__(self):
        self.source_table = 'MDC_DEV.dbo.SourceTable'
        self.sql_delete_entitymap = 'DELETE FROM MDC_DEV.dbo.EntityMap WHERE ID = (?) AND CanonID = (?)'
        self.new = new()

    def val(self):
        """ Validate the results of the dedupe program """
        print('Starting validation')

        # Create IDs for all unmatched companies
        print('Creating IDs for new companies...')
        self.new.nomatch_create_new()

        db.execute("SELECT * FROM MDC_DEV.dbo.EntityMap ORDER BY CanonID")
        clustered_data = db.pandas_read("SELECT * FROM MDC_DEV.dbo.EntityMap WHERE ClusterScore > 0.8").to_dict(
            'index')
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

        for row1, value1 in clustered_data.items():
            if row1 % 2 == 0:
                for row2, value2 in clustered_data.items():
                    if value1['CanonID'] == value2['CanonID'] and value1['ID'] != value2['ID']:
                        c = common.df_list(db.pandas_read("SELECT COUNT(*) FROM MDC_DEV.dbo.EntityMap"))
                        count = int(c[0][0] / 2)

                        print('\nNumber of duplicates left to validate: ', count)
                        print('Are these duplicates?', '\n', value1, '\n', value2)

                        while True:
                            choice = input('(y)es (n)o or (u)nsure \n')
                            if choice.lower() not in ('y', 'n', 'u'):
                                continue
                            else:
                                break
                        # If yes, add records to MDCRaw.CONFIG.DiplicateVenture for validation, remove both records
                        # from EntityMap table
                        if choice is 'y':
                            if value2['ID'] > value1['ID'] > 0:
                                self.deduplicate_existing(value1, value2, update_mode)
                            elif value1['ID'] > value2['ID'] > 0:
                                self.deduplicate_existing(value2, value1, update_mode)
                            elif value2['ID'] > value1['ID'] and value2['ID'] > 0:
                                self.duplicate_existing_new(value2, value1, update_mode)
                            elif value1['ID'] > value2['ID'] and value1['ID'] > 0:
                                self.duplicate_existing_new(value1, value2, update_mode)
                            else:
                                # Assign record with longest name as 'main record'
                                if len(value1['Name']) > len(value2['Name']):
                                    self.duplicate_new(value1, value2)
                                else:
                                    self.duplicate_new(value2, value1)
                            values = [[value1['ID'], value1['CanonID']], [value2['ID'],value2['CanonID']]]
                            db.bulk_insert(self.sql_delete_entitymap, values)

                        # If no, add both records to false positives list and remove both records from the EntityMap
                        # table
                        if choice is 'n':
                            self.false_positive(value1, value2)
                            values = [[value1['ID'], value1['CanonID']], [value2['ID'], value2['CanonID']]]
                            db.bulk_insert(self.sql_delete_entitymap, values)
                            # If a new company is found to be part of a false-positive match, add it to the venture
                            # table as a new record
                            if value1['ID'] < 0 or value2['ID'] < 0:
                                self.new.fp_create_new()
                        # If unsure, continue loop and keep both records in entitymap table for future validation
                        else:
                            break
                    else:
                        continue
            else:
                continue

    def deduplicate_existing(self, record1, record2, update_on):
        """
        Both records are already in the database
        :param record1: Historic record
        :param record2: Duplicate record
        :return:
        """
        if update_on:
            merged = update.update(record1, record2)
            print('Both records already exist in database!\nMerged record preview:\n', merged)
            while True:
                choice = input('Would you like to merge? (y)es or (n)o \n')
                if choice.lower() not in ('y', 'n'):
                    continue
                else:
                    break
            if choice is 'y':
                record1 = merged
                self.update(record1)

        # Insert duplicates of existing records into DuplicateVenture
        duplicate_records = [
            [record1['ID'], record2['ID'], record1['ID'], record2['ID'], record1['Name'], record2['Name']]]
        sql = "IF NOT EXISTS (SELECT * FROM MDC_DEV.DBO.DuplicateVenture WHERE CompanyID = (?) AND DuplicateCompanyID = (?)) " \
              "INSERT INTO MDC_DEV.dbo.DuplicateVenture (CompanyID,DuplicateCompanyID,Name,DuplicateName) VALUES (?,?,?,?)"
        db.bulk_insert(sql, duplicate_records)

    def duplicate_existing_new(self, record1, record2, update_on):
        """
        One record is in the database and the other is not in the database (-ve ID)
        :param record1: Historic record
        :param record2: Secondary source record
        """
        if update_on:
            merged = update.update(record1, record2)
            print('One record is from secondary source!\nMerged record preview:\n', merged)
            while True:
                choice = input('Would you like to merge? (y)es or (n)o \n')
                if choice.lower() not in ('y', 'n'):
                    continue
                else:
                    break
            if choice is 'y':
                record1 = merged
                self.update(record1)

        record2['ID'] = record1['ID']
        # Update source table with new ID
        record2['Name'] = str(record2['Name']).replace("'", "''")
        sql = '''UPDATE %s SET ID = %s FROM %s WHERE Name = '%s' ''' % (self.source_table, str(record2['ID']),
                                                                        self.source_table,
                                                                        record2['Name'])  # + str(record2["Name"]) + " "
        db.execute(sql)

    def duplicate_new(self, record1, record2):
        """Matching ventures but both records are from new data (both have -ve ID)"""
        # Fill any missing dimensions to create a "full" record
        record1 = update.update(record1, record2)
        # Insert record into Venture Table
        self.new.create_new(record1, record2)

    @staticmethod
    def false_positive(record1, record2):
        """Add 2 records into MatchingFalsePositives table"""
        values = [
            [record1['ID'], record2['ID'], record1['ID'], record1['Name'], record2['ID'], record2['Name'],
             record1['ClusterScore']],
            [record2['ID'], record1['ID'], record2['ID'], record2['Name'], record1['ID'], record1['Name'],
             record2['ClusterScore']]
        ]
        sql = "IF NOT EXISTS (SELECT * FROM MDC_DEV.DBO.MatchingFalsePositives WHERE ID = (?) AND FalseID = (?)) " \
              "INSERT INTO MDC_DEV.dbo.MatchingFalsePositives VALUES (?, ?, ?, ?, ?)"
        db.bulk_insert(sql, values)

    @staticmethod
    def update(record1):
        # Update record and push the changes to venture table
        record1['Name'] = str(record1['Name']).replace("'", "''")
        record1['Description'] = str(record1['Description']).replace("'", "''")
        sql = '''UPDATE MDC_DEV.dbo.Venture SET Name = '%s',BatchID = %s,Description = '%s' ,Website = '%s',
                 Email = '%s',Phone = '%s' ,Address = '%s' WHERE ID = '%s' ''' % (str(record1['Name']),
                                                                                  record1['BatchID'],
                                                                                  str(record1['Description']),
                                                                                  record1['Website'], record1['Email'],
                                                                                  str(record1['Phone']),
                                                                                  str(record1["Address"]),
                                                                                  str(record1['ID']))
        db.execute(sql)
