import sys
import os
import subprocess
import array
import ROOT

### STEP 3 ###################################################
### Analyze the ntuples and find tagged events             ###
##############################################################

def eos(cmd, path):
    proc = subprocess.Popen(['/afs/cern.ch/project/eos/installation/0.3.84-aquamarine/bin/eos.select', cmd, path], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    out, err = proc.communicate()
    if err.strip():
        print err.strip()

    res = out.strip().split('\n')
    if len(res) == 1 and res[0] == '':
        res = []

    return res


# CMS default python configuration does not come with MySQL API..
def querydb(query, form = ''):
    proc = subprocess.Popen(['mysql', '-h', 'cms-metscan.cern.ch', '-u', 'cmsmet', '-pFindBSM', '-D', 'metscan', '-e', query.rstrip(';') + ';'], stdout = subprocess.PIPE, stderr = subprocess.PIPE)
    out, err = proc.communicate()

    if err.strip():
        print err.strip()
        return []

    outlines = out.strip().split('\n')[1:]
    result = []
    for line in outlines:
        words = line.strip().split()
        if form:
            iW = 0
            for f in form:
                if f == 'i':
                    words[iW] = int(words[iW])
                elif f == 'f':
                    words[iW] = float(words[iW])

                iW += 1

        result.append(tuple(words))

    return result


# pass up to crab dir: /store/user/yiiyama/metscan/<timestamp>/<pd>/crab_<pd>_<reco>
eosdir = sys.argv[1]

xrdhead = 'root://eoscms.cern.ch//eos/cms'

xrdPaths = []

crabRecoVersion = os.path.basename(eosdir)
pd = os.path.basename(os.path.dirname(eosdir))

reco = crabRecoVersion.replace('crab_' + pd + '_', '')
reco = reco[:reco.rfind('-v')]
    
jobTimestamps = eos('ls', eosdir)
for jobTimestamp in jobTimestamps:
    jobBlocks = eos('ls', eosdir + '/' + jobTimestamp)
    for jobBlock in jobBlocks:
        xrdPaths += [xrdhead + eosdir + '/' + jobTimestamp + '/' + jobBlock + '/' + f for f in eos('ls', eosdir + '/' + jobTimestamp + '/' + jobBlock) if f.endswith('.root')]


datasetids = querydb('SELECT `datasetid` FROM `primarydatasets` WHERE `name` LIKE \'%s\'' % pd, 'i')
datasetid = datasetids[0][0]

recoids = querydb('SELECT `recoid` FROM `reconstructions` WHERE `name` LIKE \'%s\'' % reco, 'i')
recoid = recoids[0][0]

filters = querydb('SELECT `filterid`, `name` FROM `filters`', 'is')
filterids = dict([(name, filterid) for filterid, name in filters])

for xrdPath in xrdPaths:
    print ' Analyzing', xrdPath

    source = ROOT.TFile.Open(xrdPath)

    runArr = array.array('I', [0])
    lumiArr = array.array('I', [0])

    tree = source.Get('ntuples/metfilters')

    if tree.GetEntries() > 0:
        eventArr = array.array('I', [0])
        pfMETArr = array.array('f', [0.])
        resultArrs = {}
            
        tree.SetBranchAddress('run', runArr)
        tree.SetBranchAddress('lumi', lumiArr)
        tree.SetBranchAddress('event', eventArr)
        tree.SetBranchAddress('pfMET', pfMETArr)
    
        branches = tree.GetListOfBranches()
        for branch in branches:
            if not branch.GetName().startswith('filter_'):
                continue
    
            filt = branch.GetName().replace('filter_', '')
            if filt not in filterids:
                continue
            
            resultArrs[filt] = array.array('B', [0])
            tree.SetBranchAddress(branch.GetName(), resultArrs[filt])
    
        runs = []
    
        iEntry = 0
        while tree.GetEntry(iEntry) > 0:
            iEntry += 1
            if int(runArr[0]) not in runs:
                runs.append(int(runArr[0]))
    
        print 'Runs in file:', runs
    
        query = 'SELECT `eventid`, `run`, `event` FROM `events`'
        query += ' WHERE %s' % (' OR '.join(['`run` = %d' % r for r in runs]))
        dbresult = querydb(query, 'iii')
        eventids = dict([((r, e), eventid) for eventid, r, e in dbresult])

        query = 'SELECT rel.`eventid` FROM `datasetrel` AS rel INNER JOIN `events` AS ev ON ev.`eventid` = rel.`eventid`'
        query += ' WHERE rel.`datasetid` = %d' % datasetid
        query += ' AND (%s)' % (' OR '.join(['ev.`run` = %d' % r for r in runs]))
        dbresult = querydb(query, 'i')
        connectedIds = [row[0] for row in dbresult]
    
        query = 'SELECT ed.`eventid` FROM `eventdata` AS ed INNER JOIN `events` AS ev ON ev.`eventid` = ed.`eventid`'
        query += ' WHERE ed.`recoid` = %d' % recoid
        query += ' AND (%s)' % (' OR '.join(['ev.`run` = %d' % r for r in runs]))
        dbresult = querydb(query, 'i')
        idsWithData = [row[0] for row in dbresult]
    
        # freeing memory
        dbresult = []

        relBuffer = []
        dataBuffer = []
        tagBuffer = []
    
        iEntry = 0
        while tree.GetEntry(iEntry) > 0:
            iEntry += 1

            run = int(runArr[0])
            lumi = int(lumiArr[0])
            event = int(eventArr[0])
            pfMET = float(pfMETArr[0])
            results = dict([(filt, bool(r[0])) for filt, r in resultArrs.items()])
    
            if (run, event) not in eventids:
                querydb('INSERT INTO `events` (`run`, `lumi`, `event`) VALUES (%d, %d, %d)' % (run, lumi, event))
                lastins = querydb('SELECT LAST_INSERT_ID()', 'i')
                eventid = lastins[0][0]
                eventids[(run, event)] = eventid
            else:
                eventid = eventids[(run, event)]
    
            if eventid not in connectedIds:
                relBuffer.append((eventid, datasetid))
                connectedIds.append(eventid)

            if len(relBuffer) > 100:
                # flush buffer
                query = 'INSERT INTO `datasetrel` (`eventid`, `datasetid`) VALUES'
                query += ' %s' % (', '.join(['(%d, %d)' % (e, d) for e, d in relBuffer]))
                querydb(query)
                relBuffer = []
    
            if eventid in idsWithData:
                continue
    
            dataBuffer.append((eventid, pfMET))
            idsWithData.append(eventid)
    
            if len(dataBuffer) > 100:
                # flush buffer
                query = 'INSERT INTO `eventdata` (`recoid`, `eventid`, `met`) VALUES'
                query += ' %s' % (', '.join(['(%d, %d, %f)' % (recoid, e, m) for e, m in dataBuffer]))
                querydb(query)
                dataBuffer = []
    
            for filt, res in results.items():
                if not res:
                    tagBuffer.append((eventid, filterids[filt]))
    
            if len(tagBuffer) > 100:
                # flush buffer
                query = 'INSERT INTO `eventtags` (`recoid`, `eventid`, `filterid`) VALUES'
                query += ' %s' % (', '.join(['(%d, %d, %d)' % (recoid, e, f) for e, f in tagBuffer]))
                querydb(query)
                tagBuffer = []

        if len(relBuffer) > 0:
            query = 'INSERT INTO `datasetrel` (`eventid`, `datasetid`) VALUES'
            query += ' %s' % (', '.join(['(%d, %d)' % (e, d) for e, d in relBuffer]))
            querydb(query)
    
        if len(dataBuffer) > 0:
            query = 'INSERT INTO `eventdata` (`recoid`, `eventid`, `met`) VALUES'
            query += ' %s' % (', '.join(['(%d, %d, %f)' % (recoid, e, m) for e, m in dataBuffer]))
            querydb(query)
    
        if len(tagBuffer) > 0:
            query = 'INSERT INTO `eventtags` (`recoid`, `eventid`, `filterid`) VALUES'
            query += ' %s' % (', '.join(['(%d, %d, %d)' % (recoid, e, f) for e, f in tagBuffer]))
            querydb(query)

    lumis = []
    lumiTree = source.Get('ntuples/lumis')

    lumiTree.SetBranchAddress('run', runArr)
    lumiTree.SetBranchAddress('lumi', lumiArr)

    iEntry = 0
    while lumiTree.GetEntry(iEntry) > 0:
        iEntry += 1
        if (int(runArr[0]), int(lumiArr[0])) not in lumis:
            lumis.append((int(runArr[0]), int(lumiArr[0])))

    if len(lumis) > 0:
        query = 'UPDATE `scanstatus` SET `status` = \'done\' WHERE'
        query += ' `recoid` = %d AND `datasetid` = %d' % (recoid, datasetid)
        query += ' AND (%s)' % (' OR '.join(['(`run` = %d AND `lumi` = %d)' % (r, l) for r, l in lumis]))
        querydb(query)

    print ' Done. Removing file.'
    eosPath = xrdPath.replace(xrdhead, '')
    eos('rm', eosPath)
    eosPath = os.path.dirname(eosPath)
    dircont = eos('ls', eosPath)
    while len(dircont) == 0:
        print ' eos rmdir', eosPath
        eos('rmdir', eosPath)
        eosPath = os.path.dirname(eosPath)
        if os.path.basename(eosPath) == 'metscan':
            break
        dircont = eos('ls', eosPath)
