import itertools
import sys
import time
import os
import logging
import optparse
from Shared.common import Common as common

from multiprocessing.pool import Pool

import dedupe
from hcluster import jaccard

from Shared.db import DB as db
import affinegap

# LOGGING
# Dedupe uses Python logging to show or suppress verbose output. This
# code block lets you change the level of loggin on the command
# line. You don't need it if you don't want that. To enable verbose
optp = optparse.OptionParser()
optp.add_option('-v', '--verbose', dest='verbose', action='count',
                help='Increase verbosity (specify multiple times for more)'
                )
(opts, args) = optp.parse_args()
log_level = logging.WARNING
if opts.verbose:
    if opts.verbose == 1:
        log_level = logging.INFO
    elif opts.verbose >= 2:
        log_level = logging.DEBUG
logging.getLogger().setLevel(log_level)

# SETUP
settings_file = 'learned_settings'
training_file = 'training.json'
print('importing...')
start_time = time.time()

#
# # Initialize
db.execute("DELETE FROM MDC_DEV.dbo.ProcessedVenture")
db.execute("INSERT INTO MDC_DEV.dbo.ProcessedVenture SELECT * FROM MDC_DEV.dbo.Venture")

# # CLEANING
# # WEBSITE CLEANING
# #Where all the fields are 'n/a' 'NA' etc need to change them to null
db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture "
           "SET Website = NULL WHERE Website ='na' OR Website = 'n/a' OR Website = '-' "
           "OR Website = 'http://' OR Website = 'no website' OR Website = '--' "
           "OR Website = 'http://N/A' OR Website = 'no data' "
           "OR Website = '0' OR Website = 'http://www.nowebsite.co'"
           "OR Website = 'http://N/A - Development stage.' "
           "OR Website = 'http://NOT ON WEB' OR Website = 'http://none'"
           "OR Website = 'http://coming soon' OR Website = 'http://not.yet' "
           "OR Website = 'http://no website' OR Website = 'http://not yet' "
           "OR Website = 'none' OR Website = 'http://NA'OR Website = 'tbd' "
           "OR Website = 'https' OR Website = 'http://www.nowebsite.com' "
           "OR Website = 'http://nowebsite.com' OR Website = 'http://Nowebsiteyet' "
           "OR Website = 'Coming soon' OR Website = 'not set up yet' "
           "OR Website = 'http://under construction' OR Website = 'http://www.nwebsite.com'")

# # PHONE CLEANING
# db.execute("UPDATE MDC_DEV.dbo.ProcessedVenture SET Phone = NULL WHERE Phone<10 ")
# # NAME CLEANING
db.execute("DELETE FROM MDC_DEV.dbo.ProcessedVenture WHERE Name LIKE '%communitech%' OR Name LIKE '%company:%' "
           "OR Name LIKE '%nwoic%' OR Name LIKE '%riccentre%' OR Name LIKE '%sparkcentre%'"
           "OR Name LIKE '%ontario ltd%' OR Name LIKE '%ontario inc%' OR Name LIKE '%ontario limited%' "
           "OR Name LIKE '%ontario incorporated%' OR Name LIKE '%ontario corp%' OR Name LIKE '%saskatchewan ltd%' "
           "OR Name LIKE '%BC ltd%' OR Name LIKE '%B.C. ltd%' OR Name LIKE '%BC inc%' OR NAME LIKE '%Manitoba ltd%'"
           "OR Name LIKE '%manitoba ltd%' OR Name LIKE '%NB inc%' OR Name LIKE '%NB ltd%' OR Name LIKE '%venture%' "
           "OR Name LIKE '%testBAP%' OR Name LIKE '%SSMIC%' OR Name LIKE '%Techalliance%' OR Name LIKE '%RICC_%' "
           "OR Name LIKE '%_anon_%' OR Name LIKE '%InnovationFactory_%' OR Name LIKE '%InvestOttawa_%' "
           "OR Name LIKE '%Québec inc%' OR Name LIKE '%Haltech_%' OR Name LIKE '%InnovNiag_survey%' OR Name LIKE '%NOIC%'")

VENTURE_SELECT = "SELECT ID, Name, Description, Website, Phone, Email FROM MDC_DEV.dbo.ProcessedVenture"
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

# # TRAINING

print('importing and data cleaning time: ', time.time() - start_time, 'seconds')
start_time = time.time()
if os.path.exists(settings_file):
    print('reading from ', settings_file)
    with open(settings_file, 'rb') as sf:
        deduper = dedupe.StaticDedupe(sf, num_cores=4)
else:
    # Define the fields dedupe will pay attention to
    fields = [
        {'field': 'Name', 'type': 'String'},
        {'field': 'Website', 'type': 'String', 'has missing': True},
        {'field': 'Description', 'type': 'Text', 'has missing': True},
        {'field': 'Phone', 'type': 'Exact', 'has missing': True},
        {'field': 'Email', 'type': 'String', 'has missing': True},
    ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields, num_cores=4)

    # We will sample pairs from the entire venture table for training
    data = db.pandas_read(VENTURE_SELECT).to_dict('index')
    start = time.time()
    deduper.sample(data, 8000)
    data = None

    # If we have training data saved from a previous run of dedupe,
    # look for it an load it in.
    #
    # __Note:__ if you want to train from
    # scratch, delete the training_file

    if os.path.exists(training_file):
        print('Would you like to start a new training session? (y)es or (n)o')
        if sys.stdin.readline() is 'y\n':
            del training_file
        else:
            print('reading labeled examples from ', training_file)
            with open(training_file) as tf:
                deduper.readTraining(tf)

    # ACTIVE LEARNING
    # Dedupe will find the next pair of records
    # it is least certain about and ask you to label them as duplicates
    # or not.
    # use 'y', 'n' and 'u' keys to flag duplicates
    # press 'f' when you are finished
    print('set up time: ', time.time() - start_time)
    print('starting active labeling...')

    dedupe.convenience.consoleLabel(deduper)

    # When finished, save our labeled, training pairs to disk
    with open(training_file, 'w') as tf:
        deduper.writeTraining(tf)

    # `recall` is the proportion of true dupes pairs that the learned
    # rules must cover. You may want to reduce this if your are making
    # too many blocks and too many comparisons.
    deduper.train(recall=0.90)

    with open(settings_file, 'wb') as sf:
        deduper.writeSettings(sf)

    # We can now remove some of the memory hobbing objects we used
    # for training
    deduper.cleanupTraining()

# BLOCKING

print('blocking...')

# To run blocking on such a large set of data, we create a separate table
# that contains blocking keys and venture ids

# If dedupe learned a Index Predicate, we have to take a pass
# through the data and create indices.
print('creating inverted index')

for field in deduper.blocker.index_fields:
    df = db.pandas_read("SELECT DISTINCT {field} FROM MDC_DEV.dbo.ProcessedVenture "
                        "WHERE {field} IS NOT NULL".format(field=field))
    dataset = [tuple(x) for x in df.values]
    field_data = set(row[0] for row in dataset)
    deduper.blocker.index(field_data, field)

    # Free up memory
    dataset = None
    field_data = None
    df = None

# Now we are ready to write our blocking map table by creating a
# generator that yields unique `(BlockKey, ID)` tuples.
print('writing blocking map')
db.execute("DELETE FROM MDC_DEV.dbo.BlockingMap")

df = db.pandas_read(VENTURE_SELECT).set_index('ID').to_dict('index')
b_data = deduper.blocker(df)
df = None

# Chunk the blocked data into groups of 30,000 blocks
step_size = 30000


# We will also speed up the writing of blocking map by using
# parallel database writers

def dbWriter(sql, rows):
    db.bulk_insert(sql, rows)

start = time.time()

sql = 'INSERT INTO MDC_DEV.dbo.BlockingMap values (?,?)'

print('creating blocking map... this will probably take a while')

main_list = list(b_data)

chunks = [main_list[x:x+step_size] for x in range(0, len(main_list), step_size)]
print('data chunking time: ', time.time() - start)

start = time.time()
main_list = None

for chunk in chunks:
    dbWriter(sql,chunk)

chunks = None

print('insertion into blocking map time: ', time.time() - start)
# Free up memory by removing indices we don't need anymore
deduper.blocker.resetIndices()

# Many BlockKeys will only form blocks that contain a single
# record. Since there are no comparisons possible within such a
# singleton ID we can ignore them.
#
# Additionally, if more than one BlockKey forms identifical blocks
# we will only consider one of them.

logging.info("populating transition map")

db.execute("DELETE FROM MDC_DEV.dbo.TransitionMap")
db.execute("INSERT INTO MDC_DEV.dbo.TransitionMap (BlockKey, IDs) SELECT BlockKey, "
           "STUFF((SELECT ',' + CONVERT(varchar, a.ID) FROM MDC_DEV.dbo.BlockingMap "
           "WHERE BlockKey = a.BlockKey ORDER BY ID "
           "FOR XML Path (''), TYPE).value('.','VARCHAR(MAX)'),1,1,'') AS IDs "
           "FROM MDC_DEV.dbo.BlockingMap AS a")

logging.info("calculating plural_key")

db.execute("DELETE FROM MDC_DEV.dbo.PluralKey")

# Move BlockeyKeys with multiple company IDs from TransitionMap to PluralKey
# and where the BlockKey doesn't already exist in PluralKey
db.execute("INSERT INTO MDC_DEV.dbo.PluralKey (BlockKey) SELECT a.BlockKey "
           "FROM MDC_DEV.dbo.TransitionMap AS a WHERE a.IDs LIKE '%,%' AND a.BlockKey "
           "NOT IN (SELECT BlockKey FROM MDC_DEV.dbo.PluralKey)")

logging.info("calculating plural_block")

db.execute("DELETE FROM MDC_DEV.dbo.PluralBlock")
db.execute("INSERT INTO MDC_DEV.dbo.PluralBlock SELECT a.BlockKey, a.ID "
           "FROM MDC_DEV.dbo.BlockingMap AS a WHERE a.BlockKey IN "
           "(SELECT b.BlockKey FROM MDC_DEV.dbo.PluralKey AS b)")

logging.info("adding company id index and sorting index")

# To use Kolb, et.al's Redundant Free Comparison scheme, we need to
# keep track of all the BlockKeys that are associated with a
# particular venture records.

logging.info("creating covered_blocks")

db.execute("DELETE FROM MDC_DEV.dbo.CoveredBlocks")
db.execute("INSERT INTO MDC_DEV.dbo.CoveredBlocks (ID, SortedKeys) SELECT ID, "
           "STUFF((SELECT ',' + BlockKey FROM MDC_DEV.dbo.PluralBlock "
           "WHERE ID = a.ID ORDER BY BlockKey FOR XML Path ('')),1,1,'') "
           "AS SortedKeys FROM MDC_DEV.dbo.PluralBlock AS a GROUP BY ID")

# In particular, for every block of records, we need to keep
# track of a venture records's associated BlockKeys that are SMALLER than
# the current block's id. Because we ordered the ids we can achieve this by using some string hacks.

logging.info("creating SmallerCoverage")
db.execute("DELETE FROM MDC_DEV.dbo.SmallerCoverage")
db.execute("INSERT INTO MDC_DEV.dbo.SmallerCoverage (ID, BlockKey, SmallerKeys) SELECT a.ID, a.BlockKey, "
           "REPLACE(SUBSTRING(b.SortedKeys, 0, CHARINDEX(a.BlockKey, b.SortedKeys)), ',', ' ') "
           "AS SmallerKeys FROM MDC_DEV.dbo.PluralBlock AS a "
           "INNER JOIN MDC_DEV.dbo.CoveredBlocks AS b ON a.ID = b.ID")


# # Clustering

def candidates_gen(result_set):
    lset = set

    blockKey = None
    records = []
    i = 0
    for row, value in result_set.items():
        if value['BlockKey'] != blockKey:
            if records:
                yield records

            blockKey = value['BlockKey']
            records = []
            i += 1

            if i % 10000 == 0:
                print(i, "blocks")

        smallerKeys = value['SmallerKeys']

        if smallerKeys:
            smallerKeys = lset(smallerKeys.split(','))
        else:
            smallerKeys = lset([])

        records.append((value['ID'], value, smallerKeys))

    if records:
        yield records


entity_dict = db.pandas_read("SELECT b.ID, b.Name, b.AlternateName, b.BatchID, b.DateFounded, "
                             "b.DateOfIncorporation, b.Description, b.Website, b.Email, b.Phone, "
                             "b.Address, a.BlockKey, a.SmallerKeys FROM MDC_DEV.dbo.SmallerCoverage "
                             "AS a INNER JOIN MDC_DEV.dbo.ProcessedVenture AS b "
                             "ON a.ID = b.ID ORDER BY a.BlockKey").to_dict('index')

print('clustering...')
clustered_dupes = deduper.matchBlocks(candidates_gen(entity_dict),
                                      threshold=0.5)
entity_dict = None

# matchBlocks returns a generator. Turn it into a list
clustered_dupes_list = []
for item in clustered_dupes:
    clustered_dupes_list.append(item)

# # Writing out results

# We now have a sequence of tuples of venture ids that dedupe believes
# all refer to the same entity. We write this out onto an entity map
# table
db.execute("DELETE FROM MDC_DEV.dbo.EntityMap")

print('creating entity_map database')

sql = 'INSERT INTO MDC_DEV.dbo.EntityMap (ID, CanonID, ClusterScore) VALUES (?,?,?)'
values = []
cluster_id = 1
for cluster, scores in clustered_dupes_list:
    for id, score in zip(cluster, scores):
        values.append([int(id), int(cluster_id), float(score)])
    cluster_id = cluster_id + 1

db.bulk_insert(sql, values)

db.execute("UPDATE MDC_DEV.dbo.EntityMap SET Name =  p.Name, Description = p.Description, "
           "Website = p.Website, Email = p.Email, Phone = p.Phone, Address = p.Address "
           "FROM MDC_DEV.dbo.EntityMap AS a INNER JOIN MDC_DEV.dbo.ProcessedVenture AS p "
           "ON a.ID = p.ID")

# Print out the number of duplicates found
print('# duplicate sets')
print(len(clustered_dupes_list))

print('ran in', time.time() - start_time, 'seconds')
