from Shared.db import DB as db

class Clean:

    @staticmethod
    def preprocess():
        # # CLEANING
        # Website Cleaning
        # Where all the fields are 'n/a' 'NA' etc need to change them to null
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture "
                   "SET Website = NULL WHERE Website ='na' OR Website = 'n/a' OR Website = '-' "
                   "OR Website = 'http://' OR Website = 'no website' OR Website = '--' "
                   "OR Website = 'http://N/A' OR Website = 'no data' OR Website = 'http://www.facebook.com' "
                   "OR Website = '0' OR Website = 'http://www.nowebsite.co'"
                   "OR Website = 'http://N/A - Development stage.' OR Website = ' -'"
                   "OR Website = 'http://NOT ON WEB' OR Website = 'http://none'"
                   "OR Website = 'http://coming soon' OR Website = 'http://not.yet' "
                   "OR Website = 'http://no website' OR Website = 'http://not yet' "
                   "OR Website = 'none' OR Website = 'http://NA'OR Website = 'tbd' "
                   "OR Website = 'https' OR Website = 'http://www.nowebsite.com' "
                   "OR Website = 'http://nowebsite.com' OR Website = 'http://Nowebsiteyet' "
                   "OR Website = 'Coming soon' OR Website = 'not set up yet' OR Website = 'http://www.yahoo.com' "
                   "OR Website = 'http://under construction' OR Website = 'http://www.nwebsite.com' "
                   "OR Website = 'http://www.google.com' OR Website = 'http://www.google.ca' OR Website = 'youtube.com'")

        # # Phone Cleaning
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET Phone = NULL WHERE LEN(Phone)<10 ")

        # # Name Cleaning
        db.execute("DELETE FROM MDC_DEV.dbo.ProcessedVenture WHERE Name LIKE '%communitech%' OR Name LIKE '%company:%' "
                   "OR Name LIKE '%nwoic%' OR Name LIKE '%riccentre%' OR Name LIKE '%sparkcentre%'OR Name LIKE '%venture%' "
                   "OR Name LIKE '%Wetch_%' OR Name LIKE '%testBAP%' OR Name LIKE '%SSMIC%' OR Name LIKE '%Techalliance%' "
                   "OR Name LIKE '%RICC_%' OR Name LIKE '%_anon_%' OR Name LIKE '%InnovationFactory_%' "
                   "OR Name LIKE '%InvestOttawa_%' OR Name LIKE '%Québec inc%' OR Name LIKE '%Haltech_%' "
                   "OR Name LIKE '%InnovNiag_survey%' OR Name LIKE '%NOIC%'")

    @staticmethod
    def set_to_none():
        # Set all blank cells to null

        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET Name = NULL WHERE Name = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET ID = NULL WHERE ID = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET AlternateName = NULL WHERE AlternateName = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET BasicName = NULL WHERE BasicName = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET BatchID = NULL WHERE BatchID = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET DateFounded = NULL WHERE DateFounded = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET DateOfIncorporation = NULL WHERE DateOfIncorporation = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET VentureType = NULL WHERE VentureType = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET Description = NULL WHERE Description = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET Website = NULL WHERE Website = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET Email = NULL WHERE Email = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET Phone = NULL WHERE Phone = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET Fax = NULL WHERE Fax = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET VentureStatus = NULL WHERE VentureStatus = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET ModifiedDate = NULL WHERE ModifiedDate = ''")
        db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET CreateDate = NULL WHERE CreateDate = ''")
