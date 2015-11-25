import sys
import os
import time
import subprocess
import config
from localdb import dbcursor
import xrd

sourcedir = config.eosdir

def mergeAndMove(remotePath):
    lines = xrd.ls(remotePath, lopt = True)
    filesToMerge = []
    for line in lines:
        words = line.split()
        newPath = words[-1]
        print 'Merging files in', newPath
        if words[0][0] == 'd':
            if newPath.endswith('failed'):
                xrd.cleanup(newPath, force = True)
            else:
                mergeAndMove(newPath)

        elif newPath.endswith('.root'):
            filesToMerge.append(newPath)

    if len(filesToMerge):
        pathParts = remotePath.replace(sourcedir, '')[1:].split('/') # <timestamp> <pd> crab_<pd>_<reco>-v* ...
        pd = pathParts[1]
        recov = pathParts[2]
        reco = recov.replace('crab_' + pd + '_', '')
        reco = reco[:reco.rfind('-v')]

        fileName = 'metfilters_%s.root' % time.strftime('%y%m%d%H%M%S')
        outFile = '/'.join((config.scratchdir, 'merged', reco, pd, fileName))

        proc = subprocess.Popen(['hadd', outFile + '.tmp'] + ['root://eoscms.cern.ch/' + f for f in filesToMerge])
        proc.wait()

        if proc.returncode == 0:
            os.rename(outFile + '.tmp', outFile)

            for path in filesToMerge:
                xrd.rm(path)

        else:
            os.remove(outFile + '.tmp')


if __name__ == '__main__':
    dbcursor.execute('SELECT `name` FROM `primarydatasets`')
    for reco in config.reconstructions:
        for name in [row[0] for row in dbcursor]:
            if not os.path.isdir('/'.join((config.scratchdir, 'merged', reco, name))):
                os.mkdir('/'.join((config.scratchdir, 'merged', reco, name)))

    for tsdir in xrd.ls(sourcedir):
        #temporary
        if int(os.path.basename(tsdir)) < 151108000000:
            continue

        for pddir in xrd.ls(tsdir):
            for recovdir in xrd.ls(pddir):
                mergeAndMove(recovdir)

    xrd.cleanup(sourcedir)

    print 'Done.'
