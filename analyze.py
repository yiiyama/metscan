import os
import sys
import subprocess
import ROOT

import config
from eos import eos, cleanup
from localdb import dbcursor
sys.path.append('/afs/cern.ch/cms/caf/python')
import cmsIO

### STEP 3 ###################################################
### Analyze the ntuples and find tagged events             ###
##############################################################

sourcedir = config.eosdir

NMAX = -1

def download(eosPath):
    global config

    localPath = config.scratchdir + '/' + os.path.basename(eosPath)

    source = cmsIO.cmsFile(eosPath, 'eos')
    dest = cmsIO.cmsFile(localPath, 'eos')

    command = cmsIO.getCommand(source.protocol, False)
    command.append(source.pfn)
    command.append(dest.pfn)
    cmsIO.executeCommand(command)

    return localPath


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
    print query
    proc = subprocess.Popen(['mysql', '-u', config.dbuser, '-p' + config.dbpass, '-D', config.dbname, '-e', query])
    out, err = proc.communicate()


ROOT.gROOT.LoadMacro(config.installdir + '/scripts/dumpASCII.cc+')
dumper = ROOT.ASCIIDumper(config.scratchdir)

dbcursor.execute('SELECT `filterid`, `name` from `filters`')
for filterid, name in dbcursor:
    dumper.addFilter(filterid, name)

sourcePaths = {}
pathParts = ['merged']
nFiles = 0

for reco in eos('ls', sourcedir + '/' + '/'.join(pathParts)):
    pathParts.append(reco)
    sourcePaths[reco] = {}

    for pd in eos('ls', sourcedir + '/' + '/'.join(pathParts)):
        pathParts.append(pd)
        sourcePaths[reco][pd] = []

        for fname in eos('ls', sourcedir + '/' + '/'.join(pathParts)):
            pathParts.append(fname)
            if fname.endswith('.root'):
                sourcePaths[reco][pd].append(sourcedir + '/' + '/'.join(pathParts))
                nFiles += 1

            pathParts.pop()
            if NMAX > 0 and nFiles > NMAX:
                break

        pathParts.pop()
        if NMAX > 0 and nFiles > NMAX:
            break

    pathParts.pop()
    if NMAX > 0 and nFiles > NMAX:
        break

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

            localPath = download(sourcePath)

            status = dumper.dump(localPath, recoid, datasetid)

            os.remove(localPath)

            if not status:
                continue

            if dumper.getNTags() > 1000000:
                updateEventTags(dumper)

            if dumper.getNData() > 1000000:
                updateEventData(dumper)

            if dumper.getNLumis() > 1000:
                updateScanStatus(dumper, recoid, datasetid)

            eos('rm', sourcePath)
        
        # update lumi table for each dataset - reco
        if dumper.getNLumis() != 0:
            updateScanStatus(dumper, recoid, datasetid)

    if dumper.getNData() != 0:
        updateEventData(dumper)

if dumper.getNTags() != 0:
    updateEventTags(dumper)

cleanup(['merged'])
