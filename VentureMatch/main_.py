from VentureMatch.match import Match as match
from Shared.db import DB as db
from VentureMatch.clean import Clean as clean
from VentureMatch.validate import Validate as validate
from Shared.common import Common as common
from VentureMatch.exact_match import Exact as exact
from VentureMatch.update_etl import update_etl
from VentureMatch.clean_website import toURL
from urllib.parse import urlparse
import time


# # EXACT MATCHING
db.execute("DELETE FROM MDC_DEV.dbo.ProcessedVenture")
db.execute("INSERT INTO MDC_DEV.dbo.ProcessedVenture SELECT * FROM MDC_DEV.dbo.Venture")

# # Insert ACTia target list xlsx into ProcessedVenture to match with database
# df = common.xl_to_dfs('/Users/ssimmons/Documents/',input)
# df = df['ACTia_targetlist_2018']
# vals_to_insert = common.df_list(df)
db.execute("DELETE FROM MDC_DEV.dbo.SourceTable")
# sql = 'INSERT INTO MDC_DEV.dbo.SourceTable (ID,Name,Email,Phone) VALUES (?,?,?,?)' ## Edit based on dataset
# db.bulk_insert(sql, vals_to_insert)
db.execute('INSERT INTO MDC_DEV.dbo.SourceTable (SourceID, Name, BasicName, Website, Description, BatchID) '
           'SELECT ID,CompanyName, BasicName, URL,CompanyDescription,BatchID FROM MDC_DEV.dbo.CBINSIGHTS_Funding')


source = common.df_list(db.pandas_read("SELECT SourceID FROM MDC_DEV.dbo.SourceTable"))


vals = []
k = -1
for i,v in enumerate(source):
    vals.append([k,v[0]])
    k -= 1

sql = 'UPDATE MDC_DEV.dbo.SourceTable SET ID = ? WHERE SourceID = ?'
db.bulk_insert(sql, vals)

print('Starting exact matching')
e = exact()
stime = time.time()
e.match()
print(time.time() - stime)

# FUZZY MATCHING
# Insert all ventures not found to be in the database
match = match()

db.execute("INSERT INTO MDC_DEV.dbo.ProcessedVenture SELECT ID,Name,AlternateName,BasicName, "
           "BatchID,DateFounded,DateOfIncorporation,VentureType,Description,Website,Email,"
           "Phone,Fax,Address,VentureStatus,ModifiedDate,CreateDate FROM MDC_DEV.dbo.SourceTable WHERE ID<0")

print('Starting dedupe program')
# Clean table
clean.preprocess()
clean.set_to_none()

settings_file = 'database_learned_settings'
training_file = 'database_training.json'

fields = [['Name', 'String', None], ['Website', 'String', True],
          ['Description', 'Text', True], ['Phone', 'Exact', True],
          ['Email', 'String', True]]
VENTURE_SELECT = "SELECT ID, Name, Description, Website, Phone, Email FROM MDC_DEV.dbo.ProcessedVenture"

# Training/deduper setup
match.deduper_setup(settings_file, training_file, fields, VENTURE_SELECT, 10000)

# Populate blocking map, filter records through processing tables
match.block(VENTURE_SELECT)
match.prematch_processing()

cluster_list = match.clustering()
print('# duplicate sets', match.write_results(cluster_list))

print('New ventures that were not matched up')
unmatched = db.pandas_read("SELECT ID, Name FROM MDC_DEV.dbo.ProcessedVenture AS a WHERE a.ID NOT IN "
                           "(SELECT ID FROM MDC_DEV.dbo.EntityMap) AND a.ID < 0").to_dict('index')

for index, val in unmatched.items():
    print(val)
# # VALIDATION

print('Removing false-positives')
match.remove_false_positives()
print('Removing discovered matches')
match.remove_discovered_matches()

valid = validate()
valid.val()

# update = update_etl()
# update.update('MDC_DEV.dbo.CBINSIGHTS_Funding', 'ID', 'CompanyID')

# ventures = common.df_list(db.pandas_read('SELECT ID, Website FROM MDC_DEV.dbo.Venture WHERE Website IS NOT NULL'))
#
# for i, v in enumerate(ventures):
#     if v[1] != '':
#         url = toURL(v[1])
#         v[1] = url.cleanURL
#
# sql = 'UPDATE MDC_DEV.dbo.SourceTable SET ID = ? WHERE SourceID = ?'
# db.bulk_insert(sql, ventures)

