import sys

from VentureMatch.match import Match as match
from Shared.db import DB as db
from VentureMatch.clean import Clean as clean
from VentureMatch.validate import Validate as validate
from VentureMatch.new_venture import New as n
from Shared.common import Common as common
from VentureMatch.exact_match import Exact as exact
from VentureMatch.new_venture import New as new



# Main matching program using Match class

input = 'ACTia_targetlist_2018.xlsx'


match = match()
db.execute("DELETE FROM MDC_DEV.dbo.ProcessedVenture")
db.execute("INSERT INTO MDC_DEV.dbo.ProcessedVenture SELECT * FROM MDC_DEV.dbo.Venture")

# Insert ACTia target list xlsx into ProcessedVenture to match with database
df = common.xl_to_dfs('/Users/ssimmons/Documents/',input)
df = df['ACTia_targetlist_2018']
vals_toinsert = common.df_list(df)
db.execute("DELETE FROM MDC_DEV.dbo.ACTiaTargetList")
sql = 'INSERT INTO MDC_DEV.dbo.ACTiaTargetList (ID,Name,Email,Phone) VALUES (?,?,?,?)'
db.bulk_insert(sql, vals_toinsert)

print('Starting exact matching')
exact.match('MDC_DEV.dbo.ACTiaTargetList')

# Insert all ventures not found to be in the database
db.execute("INSERT INTO MDC_DEV.dbo.ProcessedVenture (ID, Name, Email, Phone) "
           "SELECT * FROM MDC_DEV.dbo.ACTiaTargetList WHERE ID<0")

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
deduper = match.deduper_setup(settings_file, training_file, fields, VENTURE_SELECT, 10000)

# Populate blocking map, filter records through processing tables
match.block(deduper,VENTURE_SELECT)
match.prematch_processing()

cluster_list = match.clustering(deduper)
print('# duplicate sets', match.write_results(cluster_list))

print('New ventures that were not matched up')
unmatched = db.pandas_read("SELECT ID, Name FROM MDC_DEV.dbo.ProcessedVenture AS a WHERE a.ID NOT IN "
                           "(SELECT ID FROM MDC_DEV.dbo.EntityMap) AND a.ID < 0").to_dict('index')
for index, val in unmatched.items():
    print(val)


print('false-positive QA')
match.remove_false_positives()

valid = validate()
valid.val('MDC_DEV.dbo.ACTiaTargetList')
