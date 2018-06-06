import os
import logging
import optparse
import dedupe
from Shared.db import DB as db


class Match:
    def __init__(self):
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

    @staticmethod
    def deduper_setup(settings_file, training_file, field_list, selection, sample):
        """
        Trains (if training and settings files do not exist) otherwise set up deduper object
        :param settings_file: settings file name
        :param training_file: training file name
        :param field_list: list of lists (field(string), comparator(string), missing?(bool))
        :param selection: sql statement selecting all relevant columns to use in deduplication
        :param sample: sample size of data to be used for training
        :return: deduper object
        """
        if os.path.exists(settings_file):
            print('Reading from ', settings_file)
            with open(settings_file, 'rb') as sf:
                deduper = dedupe.StaticDedupe(sf, num_cores=4)
        else:
            # Define the fields dedupe will pay attention to
            fields = []
            for field in field_list:
                fields.append({'field': field[0], 'type': field[1], 'has missing': field[2]})

            # Create a new deduper object and pass our data model to it.
            deduper = dedupe.Dedupe(fields, num_cores=4)

            data = db.pandas_read(selection).to_dict('index')

            print('Collecting sample data for active learning... this may take a while.')
            deduper.sample(data, sample)

            if os.path.exists(training_file):
                print('Reading labeled examples from ', training_file)
                with open(training_file) as tf:
                    deduper.readTraining(tf)

            print('Starting active labeling...')
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

            deduper.cleanupTraining()

        return deduper

    @staticmethod
    def block(deduper, selection):
        """
        :param deduper: deduper object created in training
        :param selection: sql statement selecting all relevant columns to use in deduplication
        :return: None
        """
        # If dedupe learned a Index Predicate, we have to take a pass
        # through the data and create indices.
        for field in deduper.blocker.index_fields:
            df = db.pandas_read("SELECT DISTINCT {field} FROM MDC_DEV.dbo.ProcessedVenture "
                                "WHERE {field} IS NOT NULL".format(field=field))
            dataset = [tuple(x) for x in df.values]
            field_data = set(row[0] for row in dataset)
            deduper.blocker.index(field_data, field)

        # Now we are ready to write our blocking map table by creating a
        # generator that yields unique `(BlockKey, ID)` tuples.
        db.execute("DELETE FROM MDC_DEV.dbo.BlockingMap")

        df = db.pandas_read(selection).set_index('ID').to_dict('index')
        b_data = deduper.blocker(df)
        sql = 'INSERT INTO MDC_DEV.dbo.BlockingMap values (?,?)'

        print('Populating BlockingMap... ')
        # Chunk the blocked data into groups of 30,000 blocks to be inserted in the BlockingMap
        size = 30000
        main_list = list(b_data)
        chunks = [main_list[x:x + size] for x in range(0, len(main_list), size)]

        for chunk in chunks:
            db.bulk_insert(sql, chunk)

        deduper.blocker.resetIndices()

    @staticmethod
    def prematch_processing():
        """
        Organize the data by passing it through the processing tables
        :return: None
        """
        # Many BlockKeys will only form blocks that contain a single
        # record. Since there are no comparisons possible within such a
        # singleton ID we can ignore them.
        print("Populating TransitionMap table.")

        db.execute("DELETE FROM MDC_DEV.dbo.TransitionMap")
        db.execute("INSERT INTO MDC_DEV.dbo.TransitionMap (BlockKey, IDs) SELECT BlockKey, "
                   "STUFF((SELECT ',' + CONVERT(varchar, a.ID) FROM MDC_DEV.dbo.BlockingMap "
                   "WHERE BlockKey = a.BlockKey ORDER BY ID "
                   "FOR XML Path (''), TYPE).value('.','VARCHAR(MAX)'),1,1,'') AS IDs "
                   "FROM MDC_DEV.dbo.BlockingMap AS a")

        # Move BlockeyKeys with multiple company IDs from TransitionMap to PluralKey
        # and make sure a BlockKey isn't entered more than once
        print("Populating PluralKey table.")

        db.execute("DELETE FROM MDC_DEV.dbo.PluralKey")
        db.execute("INSERT INTO MDC_DEV.dbo.PluralKey (BlockKey) SELECT a.BlockKey "
                   "FROM MDC_DEV.dbo.TransitionMap AS a WHERE a.IDs LIKE '%,%' AND a.BlockKey "
                   "NOT IN (SELECT BlockKey FROM MDC_DEV.dbo.PluralKey)")

        # Keep track of all company IDs associated with a particular BlockKey
        print("Populating PluralBlock table.")

        db.execute("DELETE FROM MDC_DEV.dbo.PluralBlock")
        db.execute("INSERT INTO MDC_DEV.dbo.PluralBlock SELECT a.BlockKey, a.ID "
                   "FROM MDC_DEV.dbo.BlockingMap AS a WHERE a.BlockKey IN "
                   "(SELECT b.BlockKey FROM MDC_DEV.dbo.PluralKey AS b)")

        # Keep track of all the BlockKeys associated with a particular company ID
        print("Populating CoveredBlocks table.")

        db.execute("DELETE FROM MDC_DEV.dbo.CoveredBlocks")
        db.execute("INSERT INTO MDC_DEV.dbo.CoveredBlocks (ID, SortedKeys) SELECT ID, "
                   "STUFF((SELECT ',' + BlockKey FROM MDC_DEV.dbo.PluralBlock "
                   "WHERE ID = a.ID ORDER BY BlockKey FOR XML Path ('')),1,1,'') "
                   "AS SortedKeys FROM MDC_DEV.dbo.PluralBlock AS a GROUP BY ID")

        # In particular, for every block of records, we need to keep
        # track of a venture records's associated BlockKeys that are SMALLER than
        # the current block's id. Because we ordered the ids we can achieve this by using some string hacks.
        print("Populating SmallerCoverage table.")

        db.execute("DELETE FROM MDC_DEV.dbo.SmallerCoverage")
        db.execute("INSERT INTO MDC_DEV.dbo.SmallerCoverage (ID, BlockKey, SmallerKeys) SELECT a.ID, a.BlockKey, "
                   "REPLACE(SUBSTRING(b.SortedKeys, 0, CHARINDEX(a.BlockKey, b.SortedKeys)), ',', ' ') "
                   "AS SmallerKeys FROM MDC_DEV.dbo.PluralBlock AS a "
                   "INNER JOIN MDC_DEV.dbo.CoveredBlocks AS b ON a.ID = b.ID")

        return None

    @staticmethod
    def candidates_gen(result_set):
        """
        Helper method for clustering
        :return: generator of records used for clustering
        """
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

    def clustering(self, deduper):
        """
        Cluster potential record matches
        :param deduper: deduper object
        :return: list of clustered tuples ((ID1, ID2), (confidence_score1, confidence_score2))
        """
        entity_dict = db.pandas_read("SELECT b.ID, b.Name, b.AlternateName, b.BatchID, b.DateFounded, "
                                     "b.DateOfIncorporation, b.Description, b.Website, b.Email, b.Phone, "
                                     "b.Address, a.BlockKey, a.SmallerKeys FROM MDC_DEV.dbo.SmallerCoverage "
                                     "AS a INNER JOIN MDC_DEV.dbo.ProcessedVenture AS b "
                                     "ON a.ID = b.ID ORDER BY a.BlockKey").to_dict('index')

        print('Clustering...')
        clustered_dupes = deduper.matchBlocks(self.candidates_gen(entity_dict),
                                              threshold=0.5)

        # matchBlocks returns a generator. Turn it into a list
        return list(clustered_dupes)

    @staticmethod
    def write_results(clustered_dupes_list):
        """
        Load finalized clusters into EntityMap table
        :param clustered_dupes_list: list of tuples returned from clustering
        :return: number of duplicate sets
        """
        # We now have a sequence of tuples of company IDs that dedupe believes
        # all refer to the same entity. We write this out onto an entity map table
        db.execute("DELETE FROM MDC_DEV.dbo.EntityMap")

        print('Populating EntityMap table.')

        sql = 'INSERT INTO MDC_DEV.dbo.EntityMap (ID, CanonID, ClusterScore) VALUES (?,?,?)'
        values = []
        cluster_id = 1
        for cluster, scores in clustered_dupes_list:
            for id, score in zip(cluster, scores):
                values.append([int(id), int(cluster_id), float(score)])
            cluster_id = cluster_id + 1

        db.bulk_insert(sql, values)

        db.execute("UPDATE MDC_DEV.dbo.EntityMap SET Name =  p.Name, BatchID = p.BatchID, "
                   "Description = p.Description, Website = p.Website, Email = p.Email, "
                   "Phone = p.Phone, Address = p.Address "
                   "FROM MDC_DEV.dbo.EntityMap AS a INNER JOIN MDC_DEV.dbo.ProcessedVenture AS p "
                   "ON a.ID = p.ID")

        return len(clustered_dupes_list)

    @staticmethod
    def false_positives():
        """
        Consult MatchingFalsePositives and remove known false positive clusters from EntityMap
        :return: None
        """
        db.execute("SELECT * FROM MDC_DEV.dbo.EntityMap ORDER BY CanonID")
        entity_map = db.pandas_read("SELECT * FROM MDC_DEV.dbo.EntityMap").set_index('ID').to_dict('index')
        clusters = []
        for index1, val1 in entity_map.items():
            for index2, val2 in entity_map.items():
                if val1['CanonID'] == val2['CanonID'] and index1 != index2:
                    clusters.append([index1, index2])
                    break

        fal_pos = db.pandas_read("SELECT * FROM MDC_DEV.dbo.MatchingFalsePositives").to_dict('index')
        remove = []
        for cluster in clusters:
            for i, v in fal_pos.items():
                if cluster[0] == v['ID'] and cluster[1] == v['FalseID']:
                    remove.append([cluster[0]])
                    remove.append([cluster[1]])
                    break
                else:
                    continue

        sql = 'DELETE FROM MDC_DEV.dbo.EntityMap WHERE ID = (?)'
        db.bulk_insert(sql, remove)
