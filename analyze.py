import os
import sys
import subprocess
import ROOT

import config
from localdb import dbcursor
sys.path.append('/afs/cern.ch/cms/caf/python')
import cmsIO

### STEP 3 ###################################################
### Analyze the ntuples and find tagged events             ###
##############################################################

def eos(cmd, *args):
    proc = subprocess.Popen(['eos', cmd] + list(args), stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    out, err = proc.communicate()
    if err.strip():
        print err.strip()

    res = out.strip().split('\n')
    if len(res) == 1 and res[0] == '':
        res = []

    return res


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

eosPaths = {}

for timestamp in eos('ls', config.eosdir):
    for pd in eos('ls', config.eosdir + '/' + timestamp):
        eosPaths[pd] = {}

        for crabRecoVersion in eos('ls', config.eosdir + '/' + timestamp + '/' + pd):
            reco = crabRecoVersion.replace('crab_' + pd + '_', '')
            reco = reco[:reco.rfind('-v')]
            if reco not in eosPaths[pd]:
                eosPaths[pd][reco] = []
                
            for jobTimestamp in eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion):
                for jobBlock in eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp):
                    files = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp + '/' + jobBlock)
                    eosPaths[pd][reco] += [config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp + '/' + jobBlock + '/' + f for f in files if f.endswith('.root')]
#                    eosPaths[pd][reco] = eosPaths[pd][reco][0:10]
#                    break
#                break
#            break
#        break
#    break

for pd in eosPaths.keys():
    dbcursor.execute('SELECT `datasetid` FROM `primarydatasets` WHERE `name` LIKE %s', (pd,))
    datasetid = dbcursor.fetchall()[0][0]

    for reco, paths in eosPaths[pd].items():
        dbcursor.execute('SELECT `recoid` FROM `reconstructions` WHERE `name` LIKE %s', (reco,))
        recoid = dbcursor.fetchall()[0][0]

        for eosPath in paths:
            print 'Analyzing', eosPath

            localPath = download(eosPath)

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

            eos('rm', eosPath)
        
        # update lumi table for each dataset - reco
        if dumper.getNLumis() != 0:
            updateScanStatus(dumper, recoid, datasetid)

    if dumper.getNData() != 0:
        updateEventData(dumper)

if dumper.getNTags() != 0:
    updateEventTags(dumper)

for timestamp in eos('ls', config.eosdir):
    pds = eos('ls', config.eosdir + '/' + timestamp)
    for pd in list(pds):
        crabRecoVersions = eos('ls', config.eosdir + '/' + timestamp + '/' + pd)
        for crabRecoVersion in list(crabRecoVersions):
            jobTimestamps = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion)
            for jobTimestamp in list(jobTimestamps):
                jobBlocks = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp)
                for jobBlock in list(jobBlocks):
                    files = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp + '/' + jobBlock)
                    if len(files) == 0:
                        eos('rm', '-r', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp + '/' + jobBlock)
                        jobBlocks.remove(jobBlock)

                if len(jobBlocks) == 0:
                    eos('rmdir', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp)
                    jobTimestamps.remove(jobTimestamp)

            if len(jobTimestamps) == 0:
                eos('rmdir', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion)
                crabRecoVersions.remove(crabRecoVersion)

        if len(crabRecoVersions) == 0:
            eos('rmdir', config.eosdir + '/' + timestamp + '/' + pd)
            pds.remove(pd)

    if len(pds) == 0:
        eos('rmdir', config.eosdir + '/' + timestamp)
