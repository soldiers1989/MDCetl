import logging
import optparse
import os
import time

import dedupe

from Shared.db import DB
from Shared.file_service import FileService
from Shared.common import Common as common


class Duplicate:

    file = FileService('')

    def __init__(self):
        self.db = DB()

        self.training_file = 'company_traning.json'
        self.setting_file = 'company_settings'
        self.result_set = None
        self.start_time = time.time()

        # CLUSTERING

    def candidates_gen(self):
        lset = set
        block_id = None
        records = []
        i = 0
        for row in self.result_set:
            if row['block_id'] != block_id:
                if records:
                    yield records
                block_id = row['block_id']
                records = []
                i += 1
                if i % 10000 == 0:
                    print(i, "blocks")
                    print(time.time() - self.start_time, "seconds")
            smaller_ids = row['smaller_ids']
            if smaller_ids:
                smaller_ids = lset(smaller_ids.split(','))
            else:
                smaller_ids = lset([])
            records.append((row['donor_id'], row, smaller_ids))
        if records:
            yield records

    def company_deduplicate(self):
        optp = optparse.OptionParser()
        optp.add_option('-v', '--verbose', dest='verbose', action='count',
                        help='Increase verbosity (specify multiple times for more)')
        (opts, args) = optp.parse_args()
        log_level = logging.WARNING
        if opts.verbose:
            if opts.verbose == 1:
                log_level = logging.INFO
            elif opts.verbose >= 2:
                log_level = logging.DEBUG
        logging.getLogger().setLevel(log_level)

        sql_data = 'SELECT CompanyID, CompanyName,Website,Email,Phone FROM Reporting.DimCompany'
        sql = 'SELECT C.CompanyID, C.CompanyName, C.Website,L.Address1,Y.[Name] AS [CityName],L.PostalCode ' \
              'FROM Reporting.DimCompany C LEFT JOIN [Reporting].[DimCompanyLocation] L ON L.CompanyID = C.CompanyID ' \
              'LEFT JOIN [Reporting].[DimCity] Y ON Y.CityID = L.CityID WHERE C.CompanyName IS NOT NULL'

        if os.path.exists(self.setting_file):
            print('reading from '.format(self.setting_file))
            with open(self.setting_file, 'rb') as sf:
                deduper = dedupe.StaticDedupe(sf, num_cores=4)

        fields = [{'field': 'CompanyID', 'variable name': 'CompanyID', 'type': 'String'},
                  {'field': 'CompanyName', 'variable name': 'CompanyName', 'type': 'Exists'},
                  {'field': 'Website', 'variable name': 'Website', 'type': 'String', 'has missing': True},
                  {'field': 'Email', 'variable name': 'Email', 'type': 'String', 'has missing': True},
                  {'field': 'Phone', 'variable name': 'Phone', 'type': 'String', 'has missing': True},
                  {'type': 'Interaction', 'interaction variables': ['CompanyName', 'Website']},
                  {'type': 'Interaction', 'interaction variables': ['CompanyID', 'Email']}]

        deduper = dedupe.Dedupe(fields, num_cores=4)
        data = self.db.pandas_read(sql_data)
        temp_data = dict((index, row) for index, row in enumerate(data))
        deduper.sample(temp_data, sample_size=10000)
        del temp_data

        if os.path.exists(self.training_file):
            print('reading labeled examples from {}'.format(self.training_file))
            with open(self.training_file) as tf:
                deduper.readTraining(tf)

        print('start active labeling...')
        # ACTIVE LEARNING
        dedupe.convenience.consoleLabel(deduper)

        with open(self.training_file, 'w') as tf:
            deduper.writeTraining(tf)

        deduper.train(recall=0.90)

        with open(self.setting_file, 'wb') as sf:
            deduper.writeSettings(sf)

        deduper.cleanupTraining()

        # BLOCKING
        print('blocking...')

        print('creating blocking_map database')
        self.db.execute('DROP TABLE IF EXISTS blocking_map')
        self.db.execute('CREATE TABLE blocking_map '
                         '(block_key VARCHAR(200), company_id INTEGER) '
                         'CHARACTER SET utf8 COLLATE utf8_unicode_ci')
        print('creating inverted index')

        for field in deduper.blocker.index_fields:
            df = self.db.pandas_read('SELECT DISTINCT {} FROM Staging.DimCompany WHERE {} IS NOT NULL'.format(field))
            field_data = (row[0] for row in df)
            deduper.blocker.index(field_data, field)

        print('writing blocking map')

        d_company = self.db.pandas_read(sql)
        full_data = ((row['CompanyID'], row) for row in d_company)
        b_data = deduper.blocker(full_data)

        val = b_data.toList()

        self.db.bulk_insert('INSERT INTO blocking_map VALUES (?,?)', val)

        deduper.blocker.resetIndices()

        # PREPARE BLOCKING TABLE

        print('prepare blocking table. ...this take a while')
        logging.info('indexing block_key')
        self.db.execute('ALTER TABLE blocking_map ADD UNIQUE INDEX(block_key, company_id)')

        self.db.execute('DROP TABLE IF EXISTS plural_key')
        self.db.execute('DROP TABLE IF EXISTS plural_block')
        self.db.execute('DROP TABLE IF EXISTS covered_blocks')
        self.db.execute('DROP TABLE IF EXISTS smaller_coverage')

        logging.info('calculating plural_key')
        self.db.execute('CREATE TABLE plural_key (block_key VARCHAR(200), block_id INTEGER UNSIGNED AUTO_INCREMENT, '
                         'PRIMARY KEY (block_id)) (SELECT MIN(block_key) '
                         'FROM  ('
                         'SELECT block_key,GROUP_CONCAT(donor_id ORDER BY donor_id) AS block  '
                         'FROM blocking_map  GROUP BY block_key HAVING COUNT(*) > 1'
                         ') AS blocks '
                         'GROUP BY block)')

        logging.info('creating block_key index')
        self.db.execute('CREATE UNIQUE INDEX block_key_idx ON plural_key (block_key)')

        logging.info("calculating plural_block")
        self.db.execute('CREATE TABLE plural_block ('
                         'SELECT block_id, donor_id FROM blocking_map INNER JOIN plural_key  USING (block_key))')

        logging.info("adding donor_id index and sorting index")
        self.db.execute('ALTER TABLE plural_block ADD INDEX (donor_id), ADD UNIQUE INDEX (block_id, donor_id)')

        self.db.execute('SET group_concat_max_len = 2048')

        logging.info("creating covered_blocks")
        self.db.execute('CREATE TABLE covered_blocks ('
                         'SELECT donor_id, GROUP_CONCAT(block_id ORDER BY block_id) AS sorted_ids  '
                         'FROM plural_block  GROUP BY donor_id)')

        self.db.execute("CREATE UNIQUE INDEX donor_idx ON covered_blocks (donor_id)")

        logging.info("creating smaller_coverage")
        self.db.execute('CREATE TABLE smaller_coverage ('
                         'SELECT donor_id, block_id,  TRIM(\',\' '
                         'FROM SUBSTRING_INDEX(sorted_ids, block_id, 1)) AS smaller_ids  '
                         'FROM plural_block INNER JOIN covered_blocks  USING (donor_id))')

        # CORRECT THIS SQL STATEMENT TO CORRECT THE ATTRIBUTES
        c_data = self.db.execute('SELECT company_id, city, name, zip, state, address, occupation, '
                                  'employer, person, block_id, smaller_ids '
                                  'FROM smaller_coverage INNER JOIN processed_donors '
                                  'USING (company_id) ORDER BY (block_id)')

        print('clustering...')
        clustered_dupes = deduper.matchBlocks(self.candidates_gen(c_data), threshold=0.5)

        self.db.execute('DROP TABLE IF EXISTS entity_map')

        # WRITING OUT RESULTS

        print('creating entity_map database')
        self.db.execute('CREATE TABLE entity_map ('
                         'donor_id INTEGER, canon_id INTEGER,  cluster_score FLOAT, PRIMARY KEY(donor_id))')

        for cluster, scores in clustered_dupes:
            cluster_id = cluster[0]
            for donor_id, score in zip(cluster, scores):
                self.db.execute('INSERT INTO entity_map VALUES (%s, %s, %s)',
                                 (donor_id, cluster_id, score))

        self.db.execute("CREATE INDEX head_index ON entity_map (canon_id)")

        print('# of duplicate sets are: {}'.format(len(clustered_dupes)))
        self.file.df_to_excel(clustered_dupes, 'Clustered Companies')

    def get_ventures(self):
        sql_venture = 'SELECT CompanyID, CompanyName FROM Reporting.DimCompany WHERE BasicName IS NULL AND CompanyName IS NOT NULL' #AND BatchID NOT IN (3496, 3497,3498, 3499)'
        data = self.db.pandas_read(sql_venture)
        sql_update = 'UPDATE Reporting.DimCompany SET BasicName = \'{}\' WHERE CompanyID = {}'
        for index, row in data.iterrows():
            basic_name = common.get_basic_name(row[1])
            # print(sql_update.format(basic_name, row[0]))
            self.db.execute(sql_update.format(basic_name, row[0]))


if __name__ == '__main__':
    dup = Duplicate()
    # dup.company_deduplicate()
    dup.get_ventures()