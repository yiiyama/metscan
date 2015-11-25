import os
import sys
import re
import time
import shutil

from CRABAPI.RawCommand import crabCommand
from CRABClient.ClientExceptions import ClientException
import CRABClient.UserUtilities
from httplib import HTTPException

import config
from das import dasQuery, datasetList
from localdb import dbcursor

### STEP 2 ###################################################
### Submit ntuplizer jobs over all new lumisections        ###
##############################################################

# list of dataset full names (PD + reconstruction version)
# There isn't really a need to query das every time. Providing a hard-coded dataset list is another option..
datasets = datasetList()

dbcursor.execute('SELECT `datasetid`, `name` FROM `primarydatasets`')
knownPDs = dict([(name, datasetid) for datasetid, name in dbcursor])

timestamp = time.strftime('%y%m%d%H%M%S')

crabConfig = CRABClient.UserUtilities.config()
crabConfig.General.workArea = config.installdir + '/jobs/' + timestamp
crabConfig.JobType.pluginName = 'Analysis'
#crabConfig.JobType.outputFiles = ['tags.txt', 'eventdata.txt', 'lumis.txt']
crabConfig.Data.splitting = 'LumiBased'
#crabConfig.Data.totalUnits = 1 # TESTING
crabConfig.Data.outLFNDirBase = config.eosdir.replace('/eos/cms', '') + '/' + timestamp
crabConfig.Site.storageSite = 'T2_CH_CERN'

try:
    os.makedirs(crabConfig.General.workArea)
except:
    pass

for reco in config.reconstructions:
    print 'Creating ntuplizer jobs for', reco

    dbcursor.execute('SELECT `recoid` FROM `reconstructions` WHERE `name` LIKE %s', (reco,))
    recoid = dbcursor.fetchall()[0][0]

    crabConfig.JobType.psetName = config.installdir + '/cmssw/' + config.cmsswbases[reco][1] + '/src/ntuplize.py'

    for pd, datasetid in knownPDs.items():
        dbcursor.execute('SELECT `run`, `lumi` FROM `scanstatus` WHERE `recoid` = %s AND `datasetid` = %s AND (`status` LIKE \'new\' OR `status` LIKE \'failed\') ORDER BY `run`, `lumi`', (recoid, datasetid))
        if dbcursor.rowcount <= 0:
            print ' No job to submit for', pd
            continue

        lumis = [(run, lumi) for run, lumi in dbcursor]

        if len(lumis) == 0:
            continue

        print ' ' + pd

        for recoVersion in [ds[1] for ds in datasets if ds[0] == pd]:
            runsInDS = []
            for row in dasQuery('run dataset=/%s/%s/RECO' % (pd, recoVersion)):
                # example output
                # {u'das_id': [u'562e00e8e1391816fc88e7e3'], u'qhash': u'82989270bb85ec7e7d676d8f447a1381', u'cache_id': [u'562e00e8e1391816fc88e81b'], u'das': {u'primary_key': u'run.run_number', u'record': 1, u'condition_keys': [u'dataset.name'], u'ts': 1445855464.7480609, u'system': [u'dbs3'], u'instance': u'prod/global', u'api': [u'runs_via_dataset'], u'expire': 1445855764, u'services': [{u'dbs3': [u'dbs3']}]}, u'run': [{u'run_number': 256584}], u'_id': u'562e00e8e1391816fc88e890'}
                runsInDS.append(row['run'][0]['run_number'])

            lumisDS = [(run, lumi) for run, lumi in lumis if run in runsInDS]
            # make json
            jsonCont = []
            currentLumiRange = None
            for run, lumi in lumisDS:
                if len(jsonCont) == 0:
                    jsonCont.append((run, []))

                if run != jsonCont[-1][0]:
                    jsonCont[-1][1].append(currentLumiRange)
                    jsonCont.append((run, []))
    
                if not currentLumiRange:
                    currentLumiRange = (lumi, lumi)
                elif lumi == currentLumiRange[1] + 1:
                    currentLumiRange = (currentLumiRange[0], lumi)
                else:
                    jsonCont[-1][1].append(currentLumiRange)
                    currentLumiRange = (lumi, lumi)

            if len(jsonCont) == 0:
                continue

            jsonCont[-1][1].append(currentLumiRange)

            runBlocks = []
            for run, ranges in jsonCont:
                runBlock = '"%d": [' % run
                runBlock += ', '.join(['[%d, %d]' % lr for lr in ranges])
                runBlock += ']'
                runBlocks.append(runBlock)

            jsonText = '{' + ', '.join(runBlocks) + '}'

            lumiMaskName = crabConfig.General.workArea + '/lumiMask_' + pd + '_' + recoVersion + '.json'
            json = open(lumiMaskName, 'w')
            json.write(jsonText)
            json.close()

            crabConfig.General.requestName = pd + '_' + recoVersion
            crabConfig.Data.inputDataset = '/' + pd + '/' + recoVersion + '/RECO'
            crabConfig.Data.lumiMask = lumiMaskName
            
            if len(lumisDS) > 30:
                crabConfig.Data.unitsPerJob = 30
            else:
                crabConfig.Data.unitsPerJob = 1

            # Submit.
            nAttempt = 0
            while nAttempt < 10:
                print '  Submitting..'
                try:
                    crabCommand('submit', config = crabConfig)
                    break
                except HTTPException as hte:
                    print "   Submission for input dataset %s/%s failed: %s" % (pd, recoVersion, hte.headers)
                except ClientException as cle:
                    print "   Submission for input dataset %s/%s failed: %s" % (pd, recoVersion, cle)

                shutil.rmtree(crabConfig.General.workArea + '/crab_' + crabConfig.General.requestName)
                nAttempt += 1
    
            query = 'UPDATE `scanstatus` SET `status` = \'scanning\' WHERE `recoid` = %d AND `datasetid` = %d AND (`run`, `lumi`) IN ' % (recoid, datasetid)
            query += '(%s)' % (', '.join(['(%d, %d)' % ent for ent in lumisDS]))
            dbcursor.execute(query)
