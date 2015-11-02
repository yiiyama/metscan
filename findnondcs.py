import sys
import re
import subprocess

import config
from localdb import dbcursor

# DCS-only JSON mask
dcsMask = {}
for fileName in config.dcsJsons:
    with open(fileName) as dcsJson:
        maskJSON = eval(dcsJson.read())

    for runStr, lumiRanges in maskJSON.items():
        run = int(runStr)
        if run not in dcsMask:
            dcsMask[run] = []
        for begin, end in lumiRanges:
            dcsMask[run] += range(begin, end + 1)

dbcursor.execute('SELECT run, lumi FROM scanstatus')
fulllist = {}
for run, lumi in dbcursor:
    if run not in fulllist:
        fulllist[run] = []
    if lumi not in fulllist[run]:
        fulllist[run].append(lumi)

dump = open('/data/scratch/lumistodrop', 'w')
for run in fulllist.keys():
    if run not in dcsMask:
        dump.write('%d,*\n' % run)
        continue

    for lumi in fulllist[run]:
        if lumi not in dcsMask[run]:
            dump.write('%d,%d\n' % (run, lumi))
