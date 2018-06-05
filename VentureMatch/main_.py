import sys

from VentureMatch.match import Match as match
from Shared.db import DB as db
from VentureMatch.clean import import Clean as clean
from Shared.common import Common as common
import pandas as pd

# Main matching program using Match class
"""
input = 'ACTia_targetlist_2018.xlsx'
output = 'actia_2018_output.csv'

match = match()
db.execute("DELETE FROM MDC_DEV.dbo.ProcessedVenture")
db.execute("INSERT INTO MDC_DEV.dbo.ProcessedVenture SELECT * FROM MDC_DEV.dbo.Venture")

# Insert ACTia target list xlsx into ProcessedVenture to match with database
df = common.xl_to_dfs('/Users/ssimmons/Documents/',input)
df = df['ACTia_targetlist_2018']
vals_toinsert = common.df_list(df)

sql = 'INSERT INTO MDC_DEV.dbo.ProcessedVenture (ID,Name,Email,Phone) VALUES (?,?,?,?)'
db.bulk_insert(sql, vals_toinsert)

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

# Output ACTia matching Results to csv
df = pd.read_excel('/Users/ssimmons/Documents/ACTia_targetlist_2018.xlsx', sheet_name=0)

df.insert(2, 'HistoricalName', "")
df.insert(3, 'HistoricalID', "")
df.insert(4, 'ClusterScore', "")

xl_dict = df.to_dict('index')

# Read the matches associated with the actia targetlist from the database
actia_data = db.pandas_read("SELECT * FROM MDC_DEV.dbo.EntityMap WHERE ID < 0 OR CanonID IN "
                            "(SELECT a.CanonID FROM MDC_DEV.dbo.EntityMap AS a WHERE ID < 0)").to_dict('index')

# Insert cluster score, historical name, and historical ID into the actia target list excel dictionary
# to be loaded into a csv file

for index, field in actia_data.items():
    for xl_index, xl_field in xl_dict.items():
        if field['ID'] == xl_field['ID']:
            xl_dict[xl_index]['ClusterScore'] = field['ClusterScore']
            for old_index, old_field in actia_data.items():
                if field['CanonID'] == old_field['CanonID'] and old_field['ID'] != field['ID']:
                    xl_dict[xl_index]['HistoricalID'] = old_field['ID']
                    xl_dict[xl_index]['HistoricalName'] = old_field['Name']

# Write to the output csv file
d = pd.DataFrame.from_dict(xl_dict, orient='index')
d.to_csv(output, index=False)
"""

print('output reconciling')
db.execute("SELECT * FROM MDC_DEV.dbo.EntityMap ORDER BY CanonID")
clustered_data = db.pandas_read("SELECT TOP 100 * FROM MDC_DEV.dbo.EntityMap WHERE ClusterScore >0.90").values.tolist()
print('ID, CanonID, ClusterScore, Name, Description, Website, Email, Phone, Address, BatchID')

merged_records = []
i = 0
k = 1
choice = ''
while i < len(clustered_data):
    while k < len(clustered_data):
        if clustered_data[i][2] == clustered_data[k][2]:
            print('would you like to merge?', '\n', clustered_data[i], '\n', clustered_data[k], '\n' '(y)es or (n)o')
            choice = sys.stdin.readline()
            new_record = []
            if choice == 'y\n':
                if int(clustered_data[i][9]) < int(clustered_data[k][9]):
                    new_record = clustered_data[i]
                    for index, field in enumerate(new_record):
                        if field is None:
                            new_record[index] = clustered_data[k][index]
                else:
                    new_record = clustered_data[k]
                    for index, field in enumerate(new_record):
                        if field is None:
                            new_record[index] = clustered_data[i][index]
                merged_records.append(new_record)
            elif choice == 'n\n':
                merged_records.append(clustered_data[i])
                merged_records.append(clustered_data[k])
        k = k + 1
    i = i + 1
    k = i + 1
print('Records:')
for each in merged_records:
    print(each)
