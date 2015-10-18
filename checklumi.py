import sys
import re

# probably not safe..
sys.path.append('/usr/lib/python2.6/site-packages')
import mysql.connector as mysqlc

import config
from das import dasQuery, datasetList
from localdb import dbcursor

### STEP 1 ###################################################
### Find lumisections to be processed from DAS             ###
##############################################################

recoids = {}
for reco in config.reconstructions:
    dbcursor.execute('SELECT `recoid` FROM `reconstructions` WHERE `name` LIKE %s', (reco,))
    if dbcursor.rowcount <= 0:
        # insert new reconstruction version
        dbcursor.execute('INSERT INTO `reconstructions` (name) VALUES (%s)', (reco,))
        recoids[reco] = dbcursor.lastrowid
    else:
        recoids[reco] = dbcursor.fetchall()[0][0]

dbcursor.execute('SELECT `datasetid`, `name` FROM `primarydatasets`')
knownPDs = dict([(name, datasetid) for datasetid, name in dbcursor])

# list of dataset full names (PD + reconstruction version)
# There isn't really a need to query das every time. Providing a hard-coded dataset list is another option..
datasets = datasetList()

for reco in config.reconstructions:
    print 'Checking for new lumis in', reco

    recoid = recoids[reco]

    # loop over primary datasets
    for pd, recoVersion in [ds for ds in datasets if ds[1][:ds[1].rfind('-v')] == reco]:
        if pd not in knownPDs:
            dbcursor.execute('INSERT INTO `primarydatasets` (name) VALUES (%s)', (pd,))
            knownPDs[pd] = dbcursor.lastrowid
            print ' Inserted', pd, 'to the list of primary datasets to process.'

        datasetid = knownPDs[pd]

        dbcursor.execute('SELECT `run`, `lumi` FROM `scanstatus` WHERE `recoid` = %s AND `datasetid` = %s', (recoid, datasetid))
        processedLumis = [(run, lumi) for run, lumi in dbcursor]

        # find new lumisections and inject to DB
        nInject = 0
        for row in dasQuery('run, lumi dataset=/' + pd + '/' + recoVersion + '/RECO'):
            # example output
            # [{u'das_id': [u'562374bae13918e2ff9dcb8b'], u'run': [{u'run_number': 256584}], u'lumi': [{u'number': [[3, 5], [7, 18], [20, 22]]}], u'cache_id': [u'562374bfe13918e2ff9dcb92'], u'das': {u'primary_key': u'run.run_number', u'record': 1, u'condition_keys': [u'dataset.name'], u'ts': 1445164352.67815, u'system': [u'dbs3'], u'instance': u'prod/global', u'api': [u'run_lumi4dataset'], u'expire': 1445164472, u'services': [{u'dbs3': [u'dbs3']}]}, u'qhash': u'213d57e7df3cc986dec2a81820c33679', u'_id': u'56237540e13918e4b9ffe1fc'}, {u'das_id': [u'562374bae13918e2ff9dcb8b'], u'qhash': u'213d57e7df3cc986dec2a81820c33679', u'lumi': [{u'number': [[1, 1], [3, 15], [17, 26], [28, 33], [35, 36], [38, 43], [45, 52], [55, 176], [178, 207]]}], u'cache_id': [u'562374bfe13918e2ff9dcb93'], u'das': {u'primary_key': u'run.run_number', u'record': 1, u'condition_keys': [u'dataset.name'], u'ts': 1445164352.67815, u'system': [u'dbs3'], u'instance': u'prod/global', u'api': [u'run_lumi4dataset'], u'expire': 1445164472, u'services': [{u'dbs3': [u'dbs3']}]}, u'run': [{u'run_number': 256587}], u'_id': u'56237540e13918e4b9ffe1fb'}]
            
            run = row['run'][0]['run_number']
            lumiranges = row['lumi'][0]['number']
            for first, last in lumiranges:
                for lumi in range(first, last + 1):
                    if (run, lumi) in processedLumis:
                        continue

                    dbcursor.execute('INSERT INTO `scanstatus` (recoid, datasetid, run, lumi, status) VALUES (%s, %s, %s, %s, \'new\')', (recoid, datasetid, run, lumi))
                    nInject += 1

        print ' Injected', nInject, 'lumis for dataset', pd + '/' + recoVersion
