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


class Validate:

    @classmethod
    def val(self, source_table):
        """ Validate the results of the dedupe program """
        print('Starting validation')

        # Create IDs for all unmatched companies
        new.nomatch_create_new(source_table)

        db.execute("SELECT * FROM MDC_DEV.dbo.EntityMap ORDER BY CanonID")
        clustered_data = db.pandas_read("SELECT * FROM MDC_DEV.dbo.EntityMap").to_dict(
            'index')

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
                        if choice is 'y':
                            if value2['ID'] > value1['ID'] > 0:
                                self.deduplicate_existing(value1, value2)
                            elif value1['ID'] > value2['ID'] > 0:
                                self.deduplicate_existing(value2, value1)
                            elif value2['ID'] > value1['ID'] and value2['ID'] > 0:
                                self.duplicate_existing_new(value2, value1, source_table)
                            elif value1['ID'] > value2['ID'] and value1['ID'] > 0:
                                self.duplicate_existing_new(value1, value2, source_table)
                            else:
                                self.duplicate_new(value1,value2, source_table)

                            sql = 'DELETE FROM MDC_DEV.dbo.EntityMap WHERE ID = (?)'
                            values = [[value1['ID']], [value2['ID']]]
                            db.bulk_insert(sql, values)

                        # If no, add both records to false positives list and remove both records from the EntityMap
                        # table
                        if choice is 'n':
                            self.false_positive(value1,value2)
                            sql = 'DELETE FROM MDC_DEV.dbo.EntityMap WHERE ID = (?)'
                            values = [[value1['ID']], [value2['ID']]]
                            db.bulk_insert(sql, values)
                            # If a new company is found to be part of a false-positive match, add it to the venture
                            # table as a new record
                            if value1['ID'] < 0 or value2['ID'] < 0:
                                    new.fp_create_new(source_table)
                        # If unsure, continue loop and keep both records in entitymap table for future validation
                        else:
                            break
                    else:
                        continue
            else:
                continue

    @staticmethod
    def deduplicate_existing(record1, record2):
        """
        Both records are already in the database
        :param record1: Historic record
        :param record2: Duplicate record
        :return:
        """
        merged = update.update_blanks(record1, record2)
        print('Both records already exist in database!\nMerged record preview:\n',merged)
        while True:
            choice = input('Would you like to merge? (y)es or (n)o \n')
            if choice.lower() not in ('y', 'n'):
                continue
            else:
                break
        if choice is 'y':
            # Update record and push the changes to venture table
            record1 = merged
            sql = 'UPDATE MDC_DEV.dbo.Venture SET Name = ' + record1['Name'] + ' BatchID = ' + record1['BatchID'] + ' Description = ' + record1['Description'] + \
                  ' Website = ' + record1['Website'] + 'Email = ' + record1['Email'] + ' Phone = ' + record1['Phone'] + ' Address = ' + record1['Address'] + \
                  ' WHERE ID = ' + record1['ID']
            db.execute(sql)

        # Insert duplicates of existing records into DuplicateVenture
        duplicate_records = [[record1['ID'],record2['ID'],record1['Name'],record2['Name']]]
        sql = 'INSERT INTO MDC_DEV.dbo.DuplicateVenture (CompanyID,DuplicateCompanyID,Name,DuplicateName) VALUES (?,?,?,?)'
        db.bulk_insert(sql, duplicate_records)


    @staticmethod
    def duplicate_existing_new(record1, record2, source_table):
        """
        One record is in the database and the other is not in the database (-ve ID)
        :param record1: Historic record
        :param record2: Secondary source record
        """
        merged = update.update_blanks(record1, record2)
        print('One record is from secondary source!\nMerged record preview:\n', merged)
        while True:
            choice = input('Would you like to update database? (y)es or (n)o \n')
            if choice.lower() not in ('y', 'n'):
                continue
            else:
                break
        if choice is 'y':
            # Update record and push the changes to venture table
            record1 = merged
            sql = 'UPDATE MDC_DEV.dbo.Venture SET Name = ' + record1['Name'] + ' BatchID = ' + record1['BatchID'] + ' Description = '+ record1['Description']+ \
             ' Website = ' + record1['Website'] + 'Email = ' + record1['Email'] + ' Phone = ' + record1['Phone'] + ' Address = ' + record1['Address'] + \
             ' WHERE ID = ' + record1['ID']
            db.execute(sql)
        record2['ID'] = record1['ID']

        # Update source table with new ID
        if source_table is not None:
            sql = "UPDATE " + source_table + " SET ID = " + str(record2['ID']) + " FROM " + source_table +\
                  " WHERE Name = '{Name}'".format(Name=str(record2['Name']))
            db.execute(sql)
        return record2

    @staticmethod
    def duplicate_new(record1, record2, source_table):
        """Matching ventures but both records are from new data (both have -ve ID)"""

        # Fill any missing dimensions to create a "full" record
        record1 = update.update_blanks(record1, record2)
        # Insert record into Venture Table
        new.create_new(source_table)
        return record1

    @staticmethod
    def false_positive(record1, record2):
        """Add 2 records into MatchingFalsePositives table"""
        values = [
            [record1['ID'], record2['ID'], record1['ID'], record1['Name'], record2['ID'], record2['Name'], record1['ClusterScore']],
            [record2['ID'], record1['ID'], record2['ID'], record2['Name'], record1['ID'], record1['Name'], record2['ClusterScore']]
        ]
        sql = 'IF NOT EXISTS (SELECT * FROM MDC_DEV.DBO.MatchingFalsePositives WHERE ID = (?) AND FalseID = (?)) ' \
              'INSERT INTO MDC_DEV.dbo.MatchingFalsePositives VALUES (?, ?, ?, ?, ?)'
        db.bulk_insert(sql, values)
