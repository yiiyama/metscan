import sys
import os
import time
import subprocess
import config
from eos import eos, cleanup

sourcedir = config.eosdir
targetdir = config.eosdir

def mergeAndMove(pathParts):
    lines = eos('ls', '-l', sourcedir + '/' + '/'.join(pathParts))
    filesToMerge = []
    for line in lines:
        words = line.split()
        newParts = pathParts + [words[-1]]
        path = '/' + '/'.join(newParts)
        print 'Merging files in', path
        if words[0][0] == 'd':
            if words[-1] == 'failed':
                eos('rm', '-r', sourcedir + path)
            else:
                mergeAndMove(newParts)

        elif words[-1].endswith('.root'):
            filesToMerge.append('root://eoscms.cern.ch//eos/cms' + sourcedir + path)

    if len(filesToMerge):
        fileName = 'metfilters_%s.root' % time.strftime('%y%m%d%H%M%S')
        tmpFile = '/data/scratch/' + fileName
        proc = subprocess.Popen(['hadd', tmpFile] + filesToMerge)
        proc.wait()

        if proc.returncode == 0:
            pd = pathParts[1]
            recov = pathParts[2]
            reco = recov.replace('crab_' + pd + '_', '')
            reco = reco[:reco.rfind('-v')]
            proc = subprocess.Popen(['xrdcp', tmpFile, 'root://eoscms.cern.ch//eos/cms' + targetdir + '/merged/' + reco + '/' + pd + '/' + fileName])
            proc.wait()

            if proc.returncode == 0:
                for path in filesToMerge:
                    eos('rm', path.replace('root://eoscms.cern.ch//eos/cms', ''))

        os.remove(tmpFile)


if __name__ == '__main__':
    recos = eos('ls', targetdir + '/merged')
    for timestamp in eos('ls', sourcedir):
        if timestamp == 'merged':
            continue

        for pd in eos('ls', sourcedir + '/' + timestamp):
            for recov in eos('ls', sourcedir + '/' + timestamp + '/' + pd):
                reco = recov.replace('crab_' + pd + '_', '')
                reco = reco[:reco.rfind('-v')]
                if reco not in recos:
                    eos('mkdir', targetdir + '/merged/' + reco)
                    
                if pd not in eos('ls', targetdir + '/merged/' + reco):
                    eos('mkdir', targetdir + '/merged/' + reco + '/' + pd)

                mergeAndMove([timestamp, pd, recov])

        cleanup([timestamp])
