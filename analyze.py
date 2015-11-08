import os
import sys
import subprocess
import ROOT

import config
from localdb import dbcursor

### STEP 3 ###################################################
### Analyze the ntuples and find tagged events             ###
##############################################################

NMAX = -1

def updateScanStatus(dumper, recoid, datasetid):
    global dbcursor

    print 'Updateing scan status.'

    lumis = {}
    for run in dumper.getAnalyzedRuns():
        if run not in lumis:
            lumis[run] = []

        for lumi in dumper.getAnalyzedLumis(run):
            lumis[run].append(lumi)

    query = 'UPDATE `scanstatus` SET STATUS = \'done\' WHERE `recoid` = %d AND `datasetid` = %d AND (' % (recoid, datasetid)
    runblocks = []
    for run, ls in lumis.items():
        block  = '(`run` = %d AND `lumi` IN (%s))' % (run, ','.join(map(str, ls)))
        runblocks.append(block)

    query += ' OR '.join(runblocks)
    query += ')'

    dbcursor.execute(query)

    dumper.resetRuns()


def updateEventTags(dumper):
    dumper.closeTags()
    loadFromFile('eventtags.txt', 'eventtags')
    dumper.resetNTags()


def updateEventData(dumper):
    global config

    dumper.closeData()
    loadFromFile('eventdata.txt', 'eventdata')
    loadFromFile('datasetrel.txt', 'datasetrel')
    dumper.resetNData()


def loadFromFile(fileName, tableName):
    global config

    query = 'LOAD DATA LOCAL INFILE \'' + config.scratchdir + '/' + fileName + '\' INTO TABLE `' + tableName + '` FIELDS TERMINATED BY \',\' LINES TERMINATED BY \'\\n\''
    proc = subprocess.Popen(['mysql', '-u', config.dbuser, '-p' + config.dbpass, '-D', config.dbname, '-e', query])
    out, err = proc.communicate()


ROOT.gROOT.LoadMacro(config.installdir + '/scripts/dumpASCII.cc+')
dumper = ROOT.ASCIIDumper(config.scratchdir)

dbcursor.execute('SELECT `filterid`, `name` from `filters`')
for filterid, name in dbcursor:
    dumper.addFilter(filterid, name)

sourcePaths = {}
nFiles = 0

class MaxFiles(Exception):
    pass

try:
    for reco in os.listdir('/'.join((config.scratchdir, 'merged'))):
        sourcePaths[reco] = {}
    
        for pd in os.listdir('/'.join((config.scratchdir, 'merged', reco))):
            sourcePaths[reco][pd] = []
    
            for fname in os.listdir('/'.join((config.scratchdir, 'merged', reco, pd))):
                sourcePaths[reco][pd].append('/'.join((config.scratchdir, 'merged', reco, pd, fname)))
                nFiles += 1
    
                if NMAX > 0 and nFiles > NMAX:
                    raise MaxFiles

except MaxFiles:
    pass

for reco in sourcePaths.keys():
    dbcursor.execute('SELECT `recoid` FROM `reconstructions` WHERE `name` LIKE %s', (reco,))
    recoid = dbcursor.fetchall()[0][0]

    for pd, paths in sourcePaths[reco].items():
        dbcursor.execute('SELECT `datasetid` FROM `primarydatasets` WHERE `name` LIKE %s', (pd,))
        datasetid = dbcursor.fetchall()[0][0]

        dumper.clearLumiMask()
        dbcursor.execute('SELECT `run`, `lumi` FROM `scanstatus` WHERE `recoid` = %s AND `datasetid` = %s AND `status` LIKE \'done\'', (recoid, datasetid))
        for run, lumi in dbcursor:
            dumper.addLumiMask(run, lumi)

        for sourcePath in paths:
            print 'Analyzing', sourcePath

            status = dumper.dump(sourcePath, recoid, datasetid)

            if not status:
                continue

            if dumper.getNTags() > 1000000:
                updateEventTags(dumper)

            if dumper.getNData() > 1000000:
                updateEventData(dumper)

            if dumper.getNLumis() > 1000:
                updateScanStatus(dumper, recoid, datasetid)

            os.remove(sourcePath)
        
        # update lumi table for each dataset - reco
        if dumper.getNLumis() != 0:
            updateScanStatus(dumper, recoid, datasetid)

    if dumper.getNData() != 0:
        updateEventData(dumper)

if dumper.getNTags() != 0:
    updateEventTags(dumper)

print 'Done.'
