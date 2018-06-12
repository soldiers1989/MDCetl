from Shared.db import DB as db
from Shared.common import Common as common
class New:
    @staticmethod
    def create_new(new_venture, source_table):
        sql = 'INSERT INTO MDC_DEV.dbo.Venture (Name, BatchID, Description, Website, Email, Phone, Address) VALUES (?,?,?,?,?,?,?)'
        db.bulk_insert(sql,new_venture)

        # Update ID to match Venture Table in the given source table
        if source_table is not None:
            sql = 'UPDATE ' + source_table + ' SET ID = b.ID FROM ' + source_table + 'as a INNER JOIN MDC_DEV.dbo.Venture AS b ON a.Name = b.Name'
            db.execute(sql)

    @staticmethod
    def nomatch_create_new(source_table):
        """Add non-duplicate ventures that are new companies (-ve ID) as new ventures to the venture table """
        new_ventures = common.df_list(db.pandas_read("SELECT * FROM MDC_DEV.dbo.ProcessedVenture AS a WHERE a.ID NOT IN "
                                                     "(SELECT ID FROM MDC_DEV.dbo.EntityMap) AND a.ID < 0 "))
        sql = 'INSERT INTO MDC_DEV.dbo.Venture VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        db.bulk_insert(sql,new_ventures)

        # Update ID to match Venture Table in the given source table
        if source_table is not None:
            sql = 'UPDATE ' + source_table + ' SET ID = b.ID FROM ' + source_table + ' AS a INNER JOIN MDC_DEV.dbo.Venture AS b ON a.Name = b.Name'
            db.execute(sql)

    @staticmethod
    def fp_create_new(source_table):

        new_ventures = common.df_list(db.pandas_read("SELECT * FROM MDC_DEV.dbo.ProcessedVenture AS a WHERE a.ID IN "
                                                     "(SELECT ID FROM MDC_DEV.dbo.MatchingFalsePositives)"))
        sql = 'INSERT INTO MDC_DEV.dbo.Venture VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        db.bulk_insert(sql, new_ventures)

        # Update MFP with the new ventures new ID
        db.execute("UPDATE MDC_DEV.dbo.MatchingFalsePositives SET ID = a.ID "
                   "FROM MDC_DEV.dbo.MatchingFalsePositives AS m INNER JOIN MDC_DEV.dbo.Venture AS a ON m.Name = a.Name")
        db.execute("UPDATE MDC_DEV.dbo.MatchingFalsePositives SET FalseID = a.ID "
                   "FROM MDC_DEV.dbo.MatchingFalsePositives AS m INNER JOIN MDC_DEV.dbo.Venture AS a ON m.FalseName = a.Name")

        # Update sourcetable with new ID
        if source_table is not None:
            sql = 'UPDATE ' + source_table + ' SET ID = b.ID FROM ' + source_table + 'as a INNER JOIN MDC_DEV.dbo.Venture AS b ON a.Name = b.Name'
            db.execute(sql)