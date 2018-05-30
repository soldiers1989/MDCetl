import itertools
import sys
import time
import os
import logging
import optparse
from Shared.common import Common as common
from multiprocessing import pool
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

# conn = db.connect(db='MDC_DEV')
# c = conn.cursor(as_dict=True)
# conn.setencoding('utf8')####
# conn.setdecoding(pyodbc.SQL_CHAR, encoding = 'utf8', to=str)###
#
# conn2 = db.connect(db='MDC_DEV')
# c2 = conn2.cursor()
# conn2.setencoding('utf8') #latin-1
# conn2.setdecoding(pyodbc.SQL_CHAR, encoding = 'utf8', to=str)

# c.execute("SET group_concat_max_len = 10192")#######

#
# # Initialize
# def initialize():
#     print('creating processed_venture table')
#     # db.execute(' IF EXISTS CREATE TABLE MDCRaw.dbo.ProcessedVenture AS (SELECT * FROM Venture)')
#     df = db.pandas_read('SELECT TOP 10 * FROM MDC_DEV.dbo.Venture')
#
#     # c.execute("DROP TABLE IF EXISTS processed_ventures")
#     # c.execute("CREATE TABLE processed_ventures AS (SELECT * FROM Venture)")
db.execute("DELETE FROM MDC_DEV.dbo.ProcessedVenture")
db.execute("INSERT INTO MDC_DEV.dbo.ProcessedVenture SELECT * FROM MDC_DEV.dbo.Venture")

# #######################################
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
           "OR Website = 'Coming soon' OR Website = 'not set up yet'")

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
           "OR Name LIKE 'Québec inc' OR Name LIKE 'Haltech_'")

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
# update_sql = 'UPDATE MDC_DEV.dbo.ProcessedVenture SET ''? = NULL WHERE ? = '''
# vals = [('Name', 'Name'), ('ID','ID'), ('AlternateName','AlternateName'),
#         ('BasicName','BasicName'),('BatchID','BatchID'),('DateFounded','DateFounded'),
#         ('DateOfIncorporation','DateOfIncorporation'),('VentureType','VentureType'),
#         ('Description','Description'),('Website','Website'),('Email','Email'),
#         ('Phone','Phone'),('Fax','Fax'),('VentureStatus','VentureStatus'),
#         ('ModifiedDate','ModifiedDate'),('CreateDate','CreateDate')]
# db.bulk_insert(update_sql, vals)
# TRAINING
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
        {'field': 'Website', 'type': 'String'},
        {'field': 'Description', 'type': 'Text', 'has missing': True},
        {'field': 'Phone', 'type': 'Exact', 'has missing': True},
        {'field': 'Email', 'type': 'String', 'has missing': True},
    ]

    # Create a new deduper object and pass our data model to it.
    deduper = dedupe.Dedupe(fields, num_cores=4)

    # We will sample pairs from the entire venture table for training
    data = db.pandas_read(VENTURE_SELECT).to_dict('index')

    deduper.sample(data, 10000)
    # del data
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

    # Notice our the argument here
    #
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
# that contains blocking keys and record ids
print('creating blocking_map database')

# db.execute("CREATE TABLE MDC_DEV.dbo.BlockingMap "
#            "(BlockKey VARCHAR(200),"
#            " ID INTEGER) ")
#   "CHARACTER SET utf8 COLLATE utf8_unicode_ci") #is this important

# If dedupe learned a Index Predicate, we have to take a pass
# through the data and create indices.
print('creating inverted index')

for field in deduper.blocker.index_fields:
    df = db.pandas_read("SELECT DISTINCT {field} FROM MDC_DEV.dbo.ProcessedVenture "
                        "WHERE {field} IS NOT NULL".format(field=field))
    dataset = [tuple(x) for x in df.values]
    field_data = set(row[0] for row in dataset)
    deduper.blocker.index(field_data, field)

# Now we are ready to write our blocking map table by creating a
# generator that yields unique `(BlockKey, ID)` tuples.
print('writing blocking map')
# db.execute("DELETE FROM MDC_DEV.dbo.BlockingMap")
# # c.execute(VENTURE_SELECT)
# df = db.pandas_read(VENTURE_SELECT).set_index('ID').to_dict('index')
# # full_data = dict((row['ID'], row) for row in df)
# # b_data = deduper.blocker(full_data)
# b_data = deduper.blocker(df)
#
# # MySQL has a hard limit on the size of a data object that can be
# # passed to it.  To get around this, we chunk the blocked data in
# # to groups of 30,000 blocks
# step_size = 30000
#
#
# # step = step_size
#
# # We will also speed up the writing of blocking map by using
# # parallel database writers
# def dbWriter(sql, rows):
#     db.bulk_insert(sql, rows)
#
#
# pool = Pool(processes=3)
# start = time.time()
#
# done = False
# sql = 'INSERT INTO MDC_DEV.dbo.BlockingMap VALUES (?,?)'
#
# print('starting bulk insert. this may take a while')
# while not done:
#     chunks = (list(itertools.islice(b_data, step)) for step in [step_size]* 100)
#     #results = []
#     for chunk in chunks:
#         #results.append((pool.apply_async(dbWriter, (sql, chunk))))
#         dbWriter(sql,chunk)
#
#
#     # for r in results:
#     #     r.wait()
#
#     if len(chunk) < step_size:
#         done = True
#
# print('bulk insert time: ', time.time() - start)
# pool.close()
#
# # Free up memory by removing indices we don't need anymore
# deduper.blocker.resetIndices()

# Remove blocks that contain only one record, sort by block key and
# venture, key and index blocking map.

# These steps, particularly the sorting will let us quickly create
# blocks of data for comparison
print('prepare blocking table. this will probably take a while ...')

logging.info("indexing BlockKey")
# db.execute("ALTER TABLE MDC_DEV.dbo.BlockingMap "
#            "CREATE UNIQUE INDEX (BlockKey, ID)")
#db.execute("CREATE INDEX IndexedBlock ON MDC_DEV.dbo.BlockingMap (BlockKey, ID)")
#db.execute("CREATE INDEX BlockKeyIndex ON MDC_DEV.dbo.BlockingMap (BlockKey,ID)")
#
# Many BlockKeys will only form blocks that contain a single
# record. Since there are no comparisons possible within such a
# singleton block we can ignore them.
#
# Additionally, if more than one BlockKey forms identifical blocks
# we will only consider one of them.
logging.info("populating transition map")
db.execute("DELETE FROM MDC_DEV.dbo.TransitionMap")
db.execute("INSERT INTO MDC_DEV.dbo.TransitionMap SELECT BlockKey, "
           "STUFF((SELECT ',' + CONVERT(varchar, a.ID) FROM MDC_DEV.dbo.BlockingMap "
           "WHERE BlockKey = a.BlockKey ORDER BY ID "
           "FOR XML Path (''), TYPE).value('.','NVARCHAR(MAX)'),1,1,'') AS Block "
           "FROM MDC_DEV.dbo.BlockingMap AS a")

logging.info("calculating plural_key")


db.execute("DELETE FROM MDC_DEV.dbo.PluralKey")
# block_list_plural = []
# block_df = db.pandas_read("SELECT * FROM MDC_DEV.dbo.TransitionMap").set_index('BlockKey').to_dict()
# for k,v in block_df.items():
#     if len(v.split(',')) > 1:
#         block_list_plural.append(k)
#db.pandas_read("SELECT * FROM MDC_DEV.dbo.TransitionMap WHERE Block LIKE '%,%'")

#Insert BlockeyKeys from TransitionMap where there are multiple IDs (comparisions available) and the BlockKey doesn't already exist in PluralKey
db.execute("INSERT INTO MDC_DEV.dbo.PluralKey (BlockKey) SELECT a.BlockKey "
           "FROM MDC_DEV.dbo.TransitionMap AS a WHERE a.Block LIKE '%,%' AND a.BlockKey "
           "NOT IN (SELECT BlockKey FROM MDC_DEV.dbo.PluralKey)")

#db.bulk_insert(sql,block_list_plural)

# block_list_plural = None
# block_list = None
# db.pandas_read("INSERT INTO MDC_DEV.dbo.PluralKey "
#                "BlockID int IDENTITY (1,1),"
#                "BlockKey WHERE ID "
#                "GROUP BY BlockKey HAVING COUNT(*) > 1) AS Blocks "
#                "GROUP BY Block")

logging.info("creating BlockKey index")
#db.execute("CREATE INDEX BlockKeyIndex ON MDC_DEV.dbo.PluralKey (BlockKey)")

logging.info("calculating plural_block")
db.execute("INSERT INTO MDC_DEV.dbo.PluralBlock (BlockID, ID) SELECT a.BlockKey, a.ID "
           "FROM MDC_DEV.dbo.BlockingMap AS a INNER JOIN MDC_DEV.dbo.PluralKey "
           "AS b ON a.BlockKey = b.BlockKey")

logging.info("adding company id index and sorting index")
# db.execute("ALTER TABLE MDC_DEV.dbo.PluralBlock "
#            "ADD INDEX (ID), "
#            "ADD UNIQUE INDEX ON (BlockID, ID)")
db.execute("CREATE UNIQUE INDEX PluralBlockIndex ON MDC_DEV.dbo.PluralBlock (BlockID, ID)")
# To use Kolb, et.al's Redundant Free Comparison scheme, we need to
# keep track of all the BlockIDs that are associated with a
# particular venture records.

logging.info("creating covered_blocks")
# c.execute("CREATE TABLE covered_blocks "
#           "(SELECT ID, "
#           " GROUP_CONCAT(BlockID ORDER BY BlockID)"
#           " AS SortedIDs "
#           " FROM plural_block "
#           " GROUP BY ID)")

# db.execute("CREATE TABLE MDC_DEV.dbo.CoveredBlocks "
db.execute("INSERT INTO MDC_DEV.dbo.CoveredBlocks "
           "SELECT ID, STUFF((SELECT ',' + BlockID FROM MDC_DEV.dbo.PluralBlock "
           "WHERE BlockID = a.BlockID "
           "ORDER BY BlockID FOR XML Path ('')),1,1,'') AS SortedIDs "
           "FROM MDC_DEV.dbo.PluralBlock AS a "
           "GROUP BY ID")

db.execute("CREATE UNIQUE INDEX VentureIndex ON MDC_DEV.dbo.CoveredBlocks (ID)")

# In particular, for every block of records, we need to keep
# track of a venture records's associated BlockIDs that are SMALLER than
# the current block's id. Because we ordered the ids we can achieve this by using some string hacks.

logging.info("creating SmallerCoverage")
db.execute(  # "CREATE TABLE MDC_DEV.dbo.SmallerCoverage "
    "INSERT INTO MDC_DEV.dbo.SmallerCoverage SELECT ID, BlockID, "
    "TRIM(',' FROM SUBSTRING_INDEX(SortedIDs, BlockID, 1)) AS SmallerIDs "
    "FROM MDC_DEV.dbo.PluralBlock "
    "INNER JOIN MDC_DEV.dbo.CoveredBlocks USING (ID))")


## Clustering

def candidates_gen(result_set):
    lset = set

    blockID = None
    records = []
    i = 0
    for row in result_set:
        if row['BlockID'] != blockID:
            if records:
                yield records

                blockID = row['BlockID']
            records = []
            i += 1

            if i % 10000 == 0:
                print(i, "blocks")
                # print(time.time() - start_time, "seconds")

                smallerIDs = row['SmallerIDs']

        if smallerIDs:
            smallerIDs = lset(smallerIDs.split(','))
        else:
            smallerIDs = lset([])

        records.append((row['ID'], row, smallerIDs))

    if records:
        yield records


# c.execute("SELECT donor_id, city, name, "
#           "zip, state, address, "
#           "occupation, employer, person, BlockID, SmallerIDs "
#           "FROM smaller_coverage "
#           "INNER JOIN processed_donors "
#           "USING (donor_id) "
#           "ORDER BY (BlockID)")

df = db.pandas_read("SELECT * FROM MDC_DEV.dbo.SmallerCoverage "
                    "INNER JOIN MDC_DEV.dbo.ProcessedVenture "
                    "USING (ID) ORDER BY (BlockID)")
print('clustering...')
clustered_dupes = deduper.matchBlocks(candidates_gen(df),
                                      threshold=0.5)

# matchBlocks returns a generator. Turn it into a list
clustered_dupes = list(clustered_dupes)

## Writing out results

# We now have a sequence of tuples of venture ids that dedupe believes
# all refer to the same entity. We write this out onto an entity map
# table
db.execute("DROP TABLE IF EXISTS MDC_DEV.dbo.EntityMap")

print('creating entity_map database')
# db.execute("CREATE TABLE EntityMap "
#            "(ID INTEGER, CanonID INTEGER, "
#            " ClusterScore FLOAT, PRIMARY KEY(ID))")

for cluster, scores in clustered_dupes:
    cluster_id = cluster[0]
    for ID, score in zip(cluster, scores):
        db.execute('INSERT INTO EntityMap VALUES (%s, %s, %s)',
                   (ID, cluster_id, score))

# conn.commit()

db.execute("CREATE INDEX HeadIndex ON MDC_DEV.dbo.EntityMap (CanonID)")
# conn.commit()

# Print out the number of duplicates found
print('# duplicate sets')
print(len(clustered_dupes))

print('ran in', time.time() - start_time, 'seconds')
