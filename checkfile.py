import sys
import os
import re
import subprocess

if 'CMSSW_BASE' not in os.environ:
    print 'CMSSW environment must be set'
    sys.exit(1)

# probably not safe..
sys.path.append('/usr/lib/python2.6/site-packages')
import mysql.connector as mysqlc

sys.path.append('/cvmfs/cms.cern.ch/' + os.environ['SCRAM_ARCH'] + '/cms/cmssw/' + os.environ['CMSSW_BASE'] + '/external/' + os.environ['SCRAM_ARCH'] + '/bin')
import das_client

import config
from terminal import Terminal

def dasQuery(query, limit = 0):
    global das_client
    result = das_client.get_data('https://cmsweb.cern.ch', query, 0, limit, False, 300, '', '')
    return result['data']

dbconn = mysqlc.connect(user = config.dbuser, password = config.dbpass, host = config.dbhost, database = config.dbname)
dbcursor = dbconn.cursor()

for reco in config.reconstructions:
    print 'Checking for new files in', reco

    dbcursor.execute('SELECT `recoid` FROM `reconstructions` WHERE `name` LIKE %s', (reco,))
    results = [row for row in dbcursor]
    if len(results) == 0:
        # insert new reconstruction version
        dbcursor.execute('INSERT INTO `reconstructions` (name) VALUES (%s)', (reco,))
        recoid = dbcursor.lastrowid

    else:
        recoid = results[0][0]

    # There isn't really a need to query das every time. Providing a hard-coded dataset list is another option..
    datasets = []
    
    for row in dasQuery('dataset dataset=/*/' + reco + '-*/RECO'):
        dsdata = row['dataset'][0]
        name = dsdata['primary_dataset']['name']
        for excl in config.datasetExcludePatterns:
            if re.match(excl + '$', name):
                break
        else:
            datasets.append((name, dsdata['processed_ds_name']))

    print ' Datasets:'
    print '\n'.join([data[0] for data in datasets])

    dbcursor.execute('SELECT `datasetid`, `name` FROM `primarydatasets`')
    knownDatasets = dict([(name, datasetid) for datasetid, name in dbcursor])

    for dataset, recoVersion in datasets:
        if dataset not in knownDatasets:
            dbcursor.execute('INSERT INTO `primarydatasets` (name) VALUES (%s)', (dataset,))
            knownDatasets[dataset] = dbcursor.lastrowid
            print ' Inserted', dataset, 'to the list of primary datasets to process.'

        dbcursor.execute('SELECT `path` FROM `files` WHERE `recoid` = %s AND `datasetid` = %s', (recoid, knownDatasets[dataset]))
        processedFiles = [row[0] for row in dbcursor]

        nInsert = 0
        for row in dasQuery('file dataset=/' + dataset + '/' + recoVersion + '/RECO'):
            fileName = row['file'][0]['name']
            if fileName in processedFiles:
                continue

            dbcursor.execute('INSERT INTO `files` (recoid, datasetid, path, status) VALUES (%s, %s, %s, \'new\')', (recoid, knownDatasets[dataset], fileName))
            nInsert += 1

        print ' Injected', nInsert, 'files for dataset', dataset + '/' + recoVersion

if not os.path.exists('/afs/cern.ch/user/' + os.environ['USER'][0] + '/' + os.environ['USER'] + '/x509up_u' + str(os.getuid())):
    print 'X509 proxy does not exist. Not submitting ntuplizer jobs.'
    sys.exit(1)

terminal = Terminal('lxplus.cern.ch')
terminal.communicate('scp -oStrictHostKeyChecking=no cms-metscan.cern.ch:/local/metscan/scripts/ntuplize.sh /tmp/' + os.environ['USER'] + '/')

dbcursor.execute('SELECT reco.`name`, dataset.`name`, file.`path`, file.`fileid` FROM `files` AS file LEFT JOIN `reconstructions` AS reco ON reco.`recoid` = file.`recoid` LEFT JOIN `primarydatasets` AS dataset ON dataset.`datasetid` = file.`datasetid` WHERE file.`status` LIKE \'new\' OR file.`status` LIKE \'failed\' LIMIT %s', (config.submitMax,))

nSubmit = 0
for recoName, datasetName, lfn, fileid in dbcursor:
    command = 'bsub -q 8nh -J {jobname} -o /local/metscan/logs/{logname}.log'.format(jobname = lfn, logname = os.path.basename(lfn).replace('.root', '.log'))
    command += ' "scp -oStrictHostKeyChecking=no -oLogLevel=quiet {node}:/tmp/{user}/ntuplize.sh .;./ntuplize.sh {release} {tarball} {reco} {dataset} {source} {fileid}"'.format(
        node = terminal.node,
        user = os.environ['USER'],
        release = config.cmsswbases[recoName][0],
        tarball = config.cmsswbases[recoName][1],
        reco = recoName,
        dataset = datasetName,
        source = lfn,
        fileid = fileid
    )

    terminal.communicate(command)

    nSubmit += 1

print 'Submitted', nSubmit, 'jobs.'

terminal.close()

dbcursor.close()
dbconn.close()
