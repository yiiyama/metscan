import os
import sys
import shutil
import re
import time
import logging

from CRABClient.Commands.status import status
from CRABClient.Commands.resubmit import resubmit
from CRABClient.Commands.report import report
from CRABClient.Commands.kill import kill
#from CRABAPI.RawCommand import crabCommand
from CRABClient.ClientExceptions import ClientException
import CRABClient.UserUtilities
from httplib import HTTPException

import config
from das import dasQuery, datasetList
from localdb import dbcursor

### STEP 2.5 ###################################################
### Manage CRAB jobs                                       #####
################################################################

KILL = ('-K' in sys.argv)

logger = logging.getLogger('CRAB3.all')
logger.setLevel(logging.CRITICAL)
logger.logfile = '/dev/null'

def cleanup(timestamp, jobdir):
    taskdir = config.installdir + '/jobs/' + timestamp + '/' + jobdir
    if os.path.exists(taskdir + '/result/missingLumiSummary.json'):
        with open(taskdir + '/result/missingLumiSummary.json') as json:
            lumiLists = eval(json.read())
    else:
        jsonName = jobdir.replace('crab_', 'lumiMask_') + '.json'
        with open(config.installdir + '/jobs/' + timestamp + '/' + jsonName) as json:
            lumiLists = eval(json.read())

    allLumis = []
    for srun, lumiRanges in lumiLists.items():
        for start, end in lumiRanges:
            allLumis += ['(%s, %d)' % (srun, l) for l in range(start, end + 1)]

    query = 'UPDATE `scanstatus` SET `status` = \'failed\' WHERE `status` LIKE \'scanning\' AND (`run`, `lumi`) IN (%s)' % (', '.join(allLumis))

    dbcursor.execute(query)
    shutil.rmtree(config.installdir + '/jobs/' + timestamp + '/' + jobdir)


timestamps = sorted(os.listdir(config.installdir + '/jobs'))
for timestamp in timestamps:
    jobdirs = [d for d in os.listdir(config.installdir + '/jobs/' + timestamp) if d.startswith('crab_')]
    for jobdir in jobdirs:
        taskdir = config.installdir + '/jobs/' + timestamp + '/' + jobdir
        shortname = timestamp + '/' + jobdir

        try:
            statusobj = status(logger, ['--dir', taskdir])
            res = statusobj()
        except:
            print ' CRAB directory ' + shortname + ' is corrupted. Deleting.'
            cleanup(timestamp, jobdir)
            continue

        print ' Task ' + shortname + ' status is ' + res['status']

        if res['status'] == 'SUBMITTED':
            if KILL:
                print ' Killing jobs..'
                try:
                    killobj = kill(logger, ['--dir', taskdir])
                    killobj()

                    try:
                        statusobj = status(logger, ['--dir', taskdir])
                        res2 = statusobj()
                        if res2['status'] == 'KILLED':
                            cleanup(timestamp, jobdir)
                    except:
                        print ' Task directory not cleaned up'

                except:
                    print ' Failed to kill ' + shortname

            else:
                print ' Resubmitting potential failed jobs..'
                try:
                    resubmitobj = resubmit(logger, ['--dir', taskdir])
                    resubmitobj()
                except:
                    print ' Failed to resubmit ' + shortname

        elif res['status'] == 'COMPLETED':
            print ' Clearing.'
            shutil.rmtree(taskdir)

        elif res['status'] in ['KILLED', 'KILLFAILED', 'FAILED', 'SUBMITFAILED', 'RESUBMITFAILED']:
            print ' Obtaining the list of lumis not analyzed.'
            try:
                reportobj = report(logger, ['--dir', taskdir])
                reportobj()
            except:
                print ' Failed to fetch the lumi list.'

            cleanup(timestamp, jobdir)

    jobdirs = [d for d in os.listdir(config.installdir + '/jobs/' + timestamp) if d.startswith('crab_')]
    if len(jobdirs) == 0:
        shutil.rmtree(config.installdir + '/jobs/' + timestamp)
