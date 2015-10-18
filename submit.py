import os
import sys
import re
import time

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

if not os.path.exists('/afs/cern.ch/user/' + os.environ['USER'][0] + '/' + os.environ['USER'] + '/x509up_u' + str(os.getuid())):
    print 'X509 proxy does not exist. Not submitting ntuplizer jobs.'
    sys.exit(1)

# list of dataset full names (PD + reconstruction version)
# There isn't really a need to query das every time. Providing a hard-coded dataset list is another option..
datasets = datasetList()

dbcursor.execute('SELECT `datasetid`, `name` FROM `primarydatasets`')
knownPDs = dict([(name, datasetid) for datasetid, name in dbcursor])

timestamp = time.strftime('%y%m%d%H%M%S')

crabConfig = CRABClient.UserUtilities.config()
crabConfig.General.workArea = config.installdir + '/jobs/' + timestamp
crabConfig.JobType.pluginName = 'Analysis'
crabConfig.Data.splitting = 'LumiBased'
crabConfig.Data.unitsPerJob = 30
#crabConfig.Data.totalUnits = 1 # TESTING
crabConfig.Data.outLFNDirBase = config.eosdir + '/' + timestamp
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
            continue

        lumis = [(run, lumi) for run, lumi in dbcursor]

        print ' ' + pd

        # make json
        lumiMaskName = crabConfig.General.workArea + '/lumiMask_' + pd + '_' + reco + '.json'
        json = open(lumiMaskName, 'w')
        json.write('{')
        currentRun = 0
        currentLumiRange = None
        for run, lumi in lumis:
            if run != currentRun:
                if currentRun != 0:
                    json.write('[%d, %d]' % currentLumiRange)
                    json.write('], ')
                json.write('"%d": [' % run)
                currentRun = run
                currentLumiRange = None

            if not currentLumiRange:
                currentLumiRange = (lumi, lumi)
            elif lumi == currentLumiRange[1] + 1:
                currentLumiRange = (currentLumiRange[0], lumi)
            else:
                json.write('[%d, %d], ' % currentLumiRange)
                currentLumiRange = (lumi, lumi)

        json.write('[%d, %d]' % currentLumiRange)
        json.write(']}')
        json.close()

        crabConfig.Data.lumiMask = lumiMaskName

        for recoVersion in [ds[1] for ds in datasets if ds[0] == pd]:
            crabConfig.General.requestName = pd + '_' + recoVersion
            crabConfig.Data.inputDataset = '/' + pd + '/' + recoVersion + '/RECO'
            # Submit.
            try:
                print '  Submitting..'
                crabCommand('submit', config = crabConfig)
            except HTTPException as hte:
                print "   Submission for input dataset %s/%s failed: %s" % (pd, recoVersion, hte.headers)
            except ClientException as cle:
                print "   Submission for input dataset %s/%s failed: %s" % (pd, recoVersion, cle)

        for run, lumi in lumis:
            dbcursor.execute('UPDATE `scanstatus` SET `status` = \'scanning\' WHERE `recoid` = %s AND `datasetid` = %s AND `run` = %s AND `lumi` = %s', (recoid, datasetid, run, lumi))

