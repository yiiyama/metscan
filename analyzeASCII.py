import os
import subprocess

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

eosPaths = {}

timestamps = eos('ls', config.eosdir)
for timestamp in timestamps:
    pds = eos('ls', config.eosdir + '/' + timestamp)
    for pd in pds:
        eosPaths[pd] = {}

        crabRecoVersions = eos('ls', config.eosdir + '/' + timestamp + '/' + pd)
        for crabRecoVersion in crabRecoVersions:
            reco = crabRecoVersion.replace('crab_' + pd + '_', '')
            reco = reco[:reco.rfind('-v')]
            if reco not in eosPaths[pd]:
                eosPaths[pd][reco] = []
                
            jobTimestamps = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion)
            for jobTimestamp in jobTimestamps:
                jobBlocks = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp)
                for jobBlock in jobBlocks:
                    files = eos('ls', config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp + '/' + jobBlock)
                    eosPaths[pd][reco] += [config.eosdir + '/' + timestamp + '/' + pd + '/' + crabRecoVersion + '/' + jobTimestamp + '/' + jobBlock + '/' + f for f in files if f.endswith('.txt')]


for pd in eosPaths.keys():
    dbcursor.execute('SELECT `datasetid` FROM `primarydatasets` WHERE `name` LIKE %s', (pd,))
    datasetid = dbcursor.fetchall()[0][0]
    
    for reco, paths in eosPaths[pd].items():
        dbcursor.execute('SELECT `recoid` FROM `reconstructions` WHERE `name` LIKE %s', (reco,))
        recoid = dbcursor.fetchall()[0][0]
        
        for eosPath in paths:
            fileName = os.path.basename(eosPath)
            eos('cp', eosPath, '/tmp/' + fileName)

            if fileName.startswith('tags'):
                query = 'LOAD DATA LOCAL INFILE \'/tmp/' + fileName + '\' INTO TABLE `eventtags` FIELDS TERMINATED BY \' \' LINES TERMINATED BY \'\\n\''
            elif fileName.startswith('eventdata'):
                query = 'LOAD DATA LOCAL INFILE \'/tmp/' + fileName + '\' INTO TABLE `eventdata` FIELDS TERMINATED BY \' \' LINES TERMINATED BY \'\\n\''
            elif fileName.startswith('lumis'):
                lumis = []
                with open('/tmp/' + fileName) as lumiList:
                    for line in lumiList:
                        run, lumi = map(int, line.strip().split())
                        if (run, lumi) not in lumis:
                            lumis.append((run, lumi))

                query = 'UPDATE `scanstatus` SET STATUS = \'done\' WHERE `recoid` = %d AND `datasetid` = %d' % (recoid, datasetid)
                query += ' AND (%s)' % (' OR '.join(['(run = %d AND lumi = %d)' % row for row in lumis]))

            dbcursor.execute(query)
