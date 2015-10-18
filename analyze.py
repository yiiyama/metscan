import sys
import os
import re
import subprocess
import array
import ROOT

import config
from localdb import dbcursor

### STEP 3 ###################################################
### Analyze the ntuples and find tagged events             ###
##############################################################

def eos(cmd, path):
    proc = subprocess.Popen(['eos', cmd, path], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    out, err = proc.communicate()
    if err.strip():
        print err.strip()

    res = out.strip().split('\n')
    if len(res) == 1 and res[0] == '':
        res = []

    return res

filterids = []
for filt in config.filters:
    dbcursor.execute('SELECT `filterid` FROM `filters` WHERE `name` LIKE %s', (filt,))
    filterids.append(dbcursor.fetchall()[0][0])

xrdhead = 'root://eoscms.cern.ch//eos/cms'

xrdPaths = {}

timestamps = eos('ls', config.eosdir)
for timestamp in timestamps:
    pds = eos('ls', config.eosdir + '/' + timestamp)
    for pd in pds:
        xrdPaths[pd] = {}

        crabRecoVersions = eos('ls', config.eosdir + '/' + timestamp + '/' + pd)
        for crabRecoVersion in crabRecoVersions:
            reco = crabRecoVersion.replace('crab_' + pd + '_', '')
            reco = reco[:reco.rfind('-v')]
            if reco not in xrdPaths[pd]:
                xrdPaths[pd][reco] = []
                
            jobTimestamps = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion)
            for jobTimestamp in jobTimestamps:
                jobBlocks = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp)
                for jobBlock in jobBlocks:
                    files = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp + '/' + jobBlock)
                    xrdPaths[pd][reco] += map(lambda n: xrdhead + config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp + '/' + jobBlock + '/' + n, files)


for pd in xrdPaths.keys():
    dbcursor.execute('SELECT `datasetid` FROM `primarydatasets` WHERE `name` LIKE %s', (pd,))
    datasetid = dbcursor.fetchall()[0][0]
    
    for reco, paths in xrdPaths[pd].items():
        dbcursor.execute('SELECT `recoid` FROM `reconstructions` WHERE `name` LIKE %s', (reco,))
        recoid = dbcursor.fetchall()[0][0]
        
        for xrdPath in paths:
            source = ROOT.TFile.Open(xrdPath)

            run = array.array('I', [0])
            lumi = array.array('I', [0])
            event = array.array('I', [0])
            pfMET = array.array('f', [0.])
            results = {}
            for filt in config.filters:
                results[filt] = array.array('B', [0])

            tree = source.Get('ntuples/metfilters')
                
            tree.SetBranchAddress('run', run)
            tree.SetBranchAddress('lumi', lumi)
            tree.SetBranchAddress('event', event)
            tree.SetBranchAddress('pfMET', pfMET)
            for filt in config.filters:
                tree.SetBranchAddress('filter_' + filt, results[filt])

            iEntry = 0
            while tree.GetEntry(iEntry) > 0:
                iEntry += 1
                dbcursor.execute('SELECT `eventid` FROM `events` WHERE `run` = %s AND `lumi` = %s AND `event` = %s', (run[0], lumi[0], event[0]))
                if dbcursor.rowcount <= 0:
                    dbcursor.execute('INSERT INTO `events` (`run`, `lumi`, `event`) VALUES (%s, %s, %s)', (run[0], lumi[0], event[0]))
                    eventid = dbcursor.lastrowid
                else:
                    eventid = dbcursor.fetchall()[0][0]

                dbcursor.execute('SELECT COUNT(*) FROM `datasetrel` WHERE `eventid` = %s AND `datasetid` = %s', (eventid, datasetid))
                if dbcursor.fetchall()[0][0] == 0:
                    dbcursor.execute('INSERT INTO `datasetrel` (`eventid`, `datasetid`) VALUES (%s, %s)', (eventid, datasetid))

                dbcursor.execute('SELECT COUNT(*) FROM `eventdata` WHERE `recoid` = %s AND `eventid` = %s', (recoid, eventid))
                if dbcursor.fetchall()[0][0] != 0:
                    continue

                dbcursor.execute('INSERT INTO `eventdata` (`recoid`, `eventid`, `met`) VALUES (%s, %s, %s)', (recoid, eventid, pfMET[0]))

                for iF, filt in enumerate(config.filters):
                    if results[filt] == 0:
                        dbcursor.execute('INSERT INTO `eventtags` (`recoid`, `eventid`, `filterid`)', (recoid, eventid, filterids[iF]))

            lumis = []
            lumiTree = source.Get('ntuples/lumis')

            lumiTree.SetBranchAddress('run', run)
            lumiTree.SetBranchAddress('lumi', lumi)

            iEntry = 0
            while lumiTree.GetEntry(iEntry) > 0:
                iEntry += 1
                if (run[0], lumi[0]) not in lumis:
                    lumis.append((run[0], lumi[0]))

            for r, l in lumis:
                dbcursor.execute('UPDATE `scanstatus` SET `status` = \'done\' WHERE `recoid` = %s AND `datasetid` = %s AND `run` = %s AND `lumi` = %s', (recoid, datasetid, r, l))

            eosPath = xrdPath.replace(xrdhead, '')
            eos('rm', eosPath)
            eosPath = os.path.dirname(eosPath)
            dircont = eos('ls', eosPath)
            while len(dircont) == 0:
                eos('rmdir', eosPath)
                eosPath = os.path.dirname(eosPath)
                if os.path.basename(eosPath) == 'metscan':
                    break
                dircont = eos('ls', eosPath)
