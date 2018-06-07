from Shared.db import DB as db
class Exact:
    def __init__(self):
        pass
    @staticmethod
    def match(records, new):
        """
        Find exact matches
        :param records: dictionary of only records with CRA numbers != NULL from exisitng database  (Venture Table)
        :param new: new entries (ie: targetlist, etc)
        :return:
        """
        records = db.pandas_read("SELECT * FROM MDC_DEV.dbo.Venture WHERE CRA NOT NULL")
