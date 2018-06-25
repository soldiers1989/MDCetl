from Shared.db import DB as db
from Shared.common import Common as common
class New:
    def __init__(self):
        self.source_table = 'MDC_DEV.dbo.SourceTable'


    def create_new(self, new_venture, second_venture):
        """new_venture: main record to be loaded into Venture table, second_venture: matching record that
        need to be updated with the same ID as new_venture"""
        db.execute('INSERT INTO MDC_DEV.dbo.Venture (AlternateName, BasicName,DateFounded,DateOfIncorporation,VentureType,'
                   'Description,Website,Email,Phone,Fax ,Address,VentureStatus,ModifiedDate,CreateDate) SELECT AlternateName, '
                   'BasicName,DateFounded,DateOfIncorporation,VentureType,Description,Website,Email,Phone,Fax ,'
                   'Address,VentureStatus,ModifiedDate,CreateDate FROM ' + self.source_table + ' AS a WHERE a.ID = ' + str(new_venture['ID']))
        # Update ID to match Venture Table in the given source table for both records
        if self.source_table is not None:
            sql = 'UPDATE ' + self.source_table + ' SET ID = b.ID FROM ' + self.source_table + ' AS a INNER JOIN MDC_DEV.dbo.Venture AS b ON a.Name = b.Name'
            db.execute(sql)
            sql = "UPDATE " + self.source_table + " SET ID = b.ID FROM " + self.source_table + " AS a INNER JOIN MDC_DEV.dbo.Venture AS b ON " \
                   "a.Name = '{Name1}' AND b.Name = '{Name2}'".format(Name1=(str(second_venture['Name'])),Name2=(str(new_venture['Name'])))
            db.execute(sql)

    def nomatch_create_new(self):
        """Add non-duplicate ventures that are new companies (-ve ID) as new ventures to the venture table """
        new_ventures = common.df_list(db.pandas_read("SELECT * FROM MDC_DEV.dbo.ProcessedVenture AS a WHERE a.ID NOT IN "
                                                     "(SELECT ID FROM MDC_DEV.dbo.EntityMap) AND a.ID < 0 "))
        sql = 'INSERT INTO MDC_DEV.dbo.Venture VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        db.bulk_insert(sql,new_ventures)

        # Update ID to match Venture Table in the given source table
        if self.source_table is not None:
            sql = 'UPDATE ' + self.source_table + ' SET ID = b.ID FROM ' + self.source_table + ' AS a INNER JOIN MDC_DEV.dbo.Venture AS b ON a.Name = b.Name'
            db.execute(sql)

    def fp_create_new(self):

        new_ventures = common.df_list(db.pandas_read("SELECT * FROM MDC_DEV.dbo.ProcessedVenture AS a WHERE a.ID IN "
                                                     "(SELECT ID FROM MDC_DEV.dbo.MatchingFalsePositives) AND a.ID < 0"))
        sql = 'INSERT INTO MDC_DEV.dbo.Venture VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        db.bulk_insert(sql, new_ventures)

        # Update MFP with the new ventures new ID
        db.execute("UPDATE MDC_DEV.dbo.MatchingFalsePositives SET ID = a.ID "
                   "FROM MDC_DEV.dbo.MatchingFalsePositives AS m INNER JOIN MDC_DEV.dbo.Venture AS a ON m.Name = a.Name")
        db.execute("UPDATE MDC_DEV.dbo.MatchingFalsePositives SET FalseID = a.ID "
                   "FROM MDC_DEV.dbo.MatchingFalsePositives AS m INNER JOIN MDC_DEV.dbo.Venture AS a ON m.FalseName = a.Name")

        # Update sourcetable with new ID
        if self.source_table is not None:
            sql = 'UPDATE ' + self.source_table + ' SET ID = b.ID FROM ' + self.source_table + 'as a INNER JOIN MDC_DEV.dbo.Venture AS b ON a.Name = b.Name'
            db.execute(sql)