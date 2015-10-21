import os
import sys
import shutil
import re
import time
import logging

from CRABClient.Commands.status import status
from CRABClient.Commands.resubmit import resubmit
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

timestamps = os.listdir(config.installdir + '/jobs')
for timestamp in timestamps:
    jobdirs = [d for d in os.listdir(config.installdir + '/jobs/' + timestamp) if d.startswith('crab_')]
    for jobdir in jobdirs:
        taskdir = config.installdir + '/jobs/' + timestamp + '/' + jobdir
        shortname = timestamp + '/' + jobdir

        try:
            statusobj = status(logger, ['--dir', taskdir])
            res = statusobj()
#            res = crabCommand('status', dir = taskdir)
        except ClientException as cle:
            print ' CRAB directory ' + shortname + ' is corrupted. Deleting.'
            shutil.rmtree(taskdir)
            continue

        print ' Task ' + shortname + ' status is ' + res['status']

        if res['status'] == 'SUBMITTED':
            if KILL:
                try:
                    killobj = kill(logger, ['--dir', taskdir])
                    killobj()
#                    crabCommand('kill', dir = taskdir)
                except HTTPException as hte:
                    print ' Failed to kill ' + shortname
                except ClientException as cle:
                    print ' Failed to kill ' + shortname

            print ' Resubmitting potential failed jobs..'
            resubmitobj = resubmit(logger, ['--dir', taskdir])
            resubmitobj()

            continue

        elif res['status'] == 'FINISHED':
            print ' Task ' + shortname + ' is complete. Clearing.'
            shutil.rmtree(taskdir)

        elif res['status'] == 'KILLED' or res['status'] == 'KILLFAILED':
            print ' Task ' + shortname + ' is killed. Clearing.'
            shutil.rmtree(taskdir)

        elif res['status'] == 'FAILED' or res['status'] == 'SUBMITFAILED':
            if KILL:
                clear = True
            else:
                clear = False
                try:
                    resubmitobj = resubmit(logger, ['--dir', taskdir])
                    resubmitobj()
#                    crabCommand('resubmit', dir = taskdir)
                except HTTPException as hte:
                    print ' Resubmission of ' + shortname + ' failed. Deleting.'
                    clear = True
                except ClientException as cle:
                    print ' Resubmission of ' + shortname + ' failed. Deleting.'
                    clear = True

            if clear:
                shutil.rmtree(taskdir)

    jobdirs = [d for d in os.listdir(config.installdir + '/jobs/' + timestamp) if d.startswith('crab_')]
    if len(jobdirs) == 0:
        shutil.rmtree(config.installdir + '/jobs/' + timestamp)
