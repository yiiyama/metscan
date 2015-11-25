import sys
import os
import re
import subprocess

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

        dbcursor.execute('SELECT `run`, `lumi` FROM `scanstatus` WHERE `datasetid` = %s AND `status` NOT LIKE \'done\'', (datasetid,))

        # find new lumisections and inject to DB
        for run, lumi in dbcursor:
            result = dasQuery('file dataset=/' + pd + '/' + recoVersion + '/RECO run=' + str(run) + ' lumi=' + str(lumi))
            try:
                lfn = result[0]['file'][0]['name']
            except:
#                print result
                lfn = pd + ' N/A'

            # example output
            # {u'das_id': [u'56466a9f6924172dcacc17ea', u'56466a9f6924172dcacc17e8'], u'qhash': u'0c0ff5c354d68314f3f00a3cab297ceb', u'cache_id': [u'56466aa06924172dcacc17ef'], u'file': [{u'name': u'/store/data/Run2015D/Tau/RECO/PromptReco-v3/000/257/400/00000/4CC2B9E3-AD64-E511-B274-02163E014308.root'}], u'das': {u'primary_key': u'file.name', u'record': 1, u'condition_keys': [u'run.run_number', u'lumi.number', u'dataset.name'], u'ts': 1447455392.3699269, u'system': [u'dbs3'], u'instance': u'prod/global', u'api': [u'file4DatasetRunLumi'], u'expire': 1447455692, u'services': [{u'dbs3': [u'dbs3']}]}, u'_id': u'56466aa06924172dcacc17f2'}

            print run, lumi, lfn
