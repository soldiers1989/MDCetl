from Shared.db import DB as db
from Shared.common import Common as common
class New:
    def create_new(self,new_venture):
        sql = 'INSERT INTO MDC_DEV.dbo.Venture VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        db.bulk_insert(sql,new_venture)

    def nomatch_create_new(self):
        """
        Add non-duplicate ventures that are new companies (-ve ID) as new ventures to the venture table
        :return:
        """
        new_ventures = common.df_list(db.pandas_read("SELECT * FROM MDC_DEV.dbo.ProcessedVenture AS a WHERE a.ID NOT IN "
                             "(SELECT ID FROM MDC_DEV.dbo.EntityMap) AND a.ID < 0 "))
        sql = 'INSERT INTO MDC_DEV.dbo.Venture VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        db.bulk_insert(sql,new_ventures)

    def fp_create_new(self):

        new_ventures = common.df_list(db.pandas_read("SELECT * FROM MDC_DEV.dbo.ProcessedVenture AS a WHERE a.ID IN "
                                                     "(SELECT ID FROM MDC_DEV.dbo.MatchingFalsePositives)"))
        sql = 'INSERT INTO MDC_DEV.dbo.Venture VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)'
        db.bulk_insert(sql, new_ventures)

        # Update MFP with the new ventures new ID
        db.execute("UPDATE MDC_DEV.dbo.MatchingFalsePositives SET ID = a.ID "
                   "FROM MDC_DEV.dbo.MatchingFalsePositives AS m INNER JOIN MDC_DEV.dbo.Venture AS a ON m.Name = a.Name")
        db.execute("UPDATE MDC_DEV.dbo.MatchingFalsePositives SET FalseID = a.ID "
                   "FROM MDC_DEV.dbo.MatchingFalsePositives AS m INNER JOIN MDC_DEV.dbo.Venture AS a ON m.FalseName = a.Name")