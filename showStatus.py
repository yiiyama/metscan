import os
import sys
import re
import time
import subprocess

import config
from localdb import dbcursor

#htmlDirs = ['/var/www/html', '/afs/cern.ch/user/y/yiiyama/www/metscan']
htmlDirs = ['/var/www/html']

messages = '    <p><span style="color:red;">The system is currently re-scanning the entire dataset to apply the new muon-in-jets filter.</span></p>\n'
messages += '    <p><a href="nov2/index.html">Status as of November 2</a></p>\n'
messages += '    <p>Golden JSON used is: ' + config.goldenJson + '</p>\n'
messages += '    <p>Silver JSON used is: ' + config.silverJson + '</p>\n'
messages += '    <p>Page last updated: ' + time.asctime() + '</p>'

dbcursor.execute('SELECT `status`+0 FROM `scanstatus` WHERE `status` LIKE \'done\'')
DONE = dbcursor.fetchall()[0][0]

dbcursor.execute('SELECT `recoid`, `name` FROM `reconstructions` ORDER BY `recoid`')
recos = [(row[0], row[1]) for row in dbcursor]

dbcursor.execute('SELECT `datasetid`, `name` FROM `primarydatasets` ORDER BY `name`')
pds = []
for pdid, pdname in dbcursor:
    for pat in config.datasetExcludePatterns:
        if re.match(pat + '$', pdname):
            break
    else:
        pds.append((pdid, pdname))

status = dict([(reco[0], dict([(pdid, {}) for pdid, name in pds])) for reco in recos])
dbcursor.execute('SELECT `recoid`, `datasetid`, `run`, `lumi`, `status`+0 FROM `scanstatus`')
for recoid, pdid, run, lumi, st in dbcursor:
    if recoid not in status:
        status[recoid] = {}
    if pdid not in status[recoid]:
        status[recoid][pdid] = {}
    if run not in status[recoid][pdid]:
        status[recoid][pdid][run] = []

    status[recoid][pdid][run].append((lumi, st))

with open(config.goldenJson) as goldenSource:
    json = eval(goldenSource.read())
    golden = {}
    for srun, ranges in json.items():
        run = int(srun)
        golden[run] = []
        for begin, end in ranges:
            golden[run] += range(begin, end + 1)

with open(config.silverJson) as silverSource:
    json = eval(silverSource.read())
    silver = {}
    for srun, ranges in json.items():
        run = int(srun)
        silver[run] = []
        for begin, end in ranges:
            silver[run] += range(begin, end + 1)

header = '''  <head>
    <title>Scan status</title>
    <style type="text/css">
  body {
    font-family: Arial, Helvetica, sans-serif;
    font-size: 12px;
  }
  table {
    border-collapse: collapse;
  }
  table, th, td {
    border: 1px solid black;
  }
    </style>
  </head>
'''

mainHTML = '<html>\n' + header
mainHTML += '  <body>\n'
mainHTML += '    <div id="messages">\n' + messages + '\n    </div>\n'
mainHTML += '    <table>\n'

for recoid, reconame in recos:
    mainHTML += '      <tr><th colspan="4" style="font-weight:bold;font-size:16px;">' + reconame + '</th></tr>\n'
    mainHTML += '      <tr><th>PD</th><th>Status - Golden (%)</th><th>Status - Silver (%)</th><th>Status - DCSOnly (%)</th></tr>\n'
    for pdid, pdname in pds:
        mainHTML += '      <tr><td><a href="datasets/' + pdname + '.html">' + pdname + '</a></td>'

        dsHTML = '<html>\n' + header + '  <body>\n'
        dsHTML += '    <h1>' + pdname + '/' + reconame + '</h1>\n'
        dsHTML += '    <p>Golden JSON used is: ' + config.goldenJson + '</p>\n'
        dsHTML += '    <p>Silver JSON used is: ' + config.silverJson + '</p>\n'
        dsHTML += '    <table>\n'
        dsHTML += '      <tr><th>Run</th><th>Completion - Golden</th><th>Completion - Silver</td><td>Completion - DCSOnly</td></tr>\n'

        ndonePD = 0
        ngoldenPD = 0
        nsilverPD = 0
        totalPD = 0
        dgoldenPD = 0
        dsilverPD = 0

        for run in sorted(status[recoid][pdid].keys()):
            lumis = status[recoid][pdid][run]
            ndone = sum([1 for l, s in lumis if s == DONE])
            if run in golden:
                ngolden = sum([1 for l, s in lumis if s == DONE and l in golden[run]])
                dgolden = len(golden[run])
            else:
                ngolden = 0
                dgolden = 0
            if run in silver:
                nsilver = sum([1 for l, s in lumis if s == DONE and l in silver[run]])
                dsilver = len(silver[run])
            else:
                nsilver = 0
                dsilver = 0

            total = len(lumis)
            dsHTML += '      <tr><td>%d</td><td>%d/%d</td><td>%d/%d</td><td>%d/%d</td></tr>\n' % (run, ngolden, dgolden, nsilver, dsilver, ndone, total)
            ndonePD += ndone
            ngoldenPD += ngolden
            nsilverPD += nsilver
            totalPD += total
            dgoldenPD += dgolden
            dsilverPD += dsilver

        dsHTML += '    </table>\n'
        dsHTML += '  </body>\n'
        dsHTML += '</html>\n'

        for htmlDir in htmlDirs:
            with open(htmlDir + '/datasets/' + pdname + '.html', 'w') as htmlFile:
                htmlFile.write(dsHTML)

        if dgoldenPD:
            mainHTML += '<td>%.1f</td>' % (float(ngoldenPD) / dgoldenPD * 100.)
        else:
            mainHTML += '<td>N/A</td>'
        if dsilverPD:
            mainHTML += '<td>%.1f</td>' % (float(nsilverPD) / dsilverPD * 100.)
        else:
            mainHTML += '<td>N/A</td>'
        if totalPD:
            mainHTML += '<td>%.1f</td>' % (float(ndonePD) / totalPD * 100.)
        else:
            mainHTML += '<td>N/A</td>'

        mainHTML += '</tr>\n'

mainHTML += '''    </table>
  </body>
</html>
'''

for htmlDir in htmlDirs:
    with open(htmlDir + '/index.html', 'w') as htmlFile:
        htmlFile.write(mainHTML)
