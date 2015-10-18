import sys
import os
import re
import ROOT

if 'CMSSW_BASE' not in os.environ:
    print 'CMSSW environment must be set'
    sys.exit(1)

# probably not safe..
sys.path.append('/usr/lib/python2.6/site-packages')
import mysql.connector as mysqlc

import config

dbconn = mysqlc.connect(user = config.dbuser, password = config.dbpass, host = config.dbhost, database = config.dbname)
dbcursor = dbconn.cursor()

dbcursor.execute('SELECT reco.`name`, dataset.`name`, file.`path`, file.`fileid` FROM `files` AS file LEFT JOIN `reconstructions` AS reco ON reco.`recoid` = file.`recoid` LEFT JOIN `datasets` AS dataset ON dataset.`datasetid` = file.`datasetid` WHERE file.`status` LIKE \'ntuplized\'')

for recoName, datasetName, lfn, fileid in dbcursor:
    source = ROOT.TFile.Open(config.eosdir + '/' + recoName + '/' + datasetName + '/' + os.path.basename(lfn))
    tree = source.Get('tree')

    
    dbcursor.execute('UPDATE `files` SET `status` = \'analyzed\' WHERE `fileid` = %s', (fileid))
