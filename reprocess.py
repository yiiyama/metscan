import config
from localdb import dbcursor

query = 'UPDATE `scanstatus` SET STATUS = \'new\' WHERE `recoid` = 1 AND `datasetid` = %s AND `run` = %s AND `lumi` = %s'

replist = open('/data/scratch/reprocess.txt')

for line in replist:
    datasetid, run, lumi = map(int, line.split())
    dbcursor.execute(query, (datasetid, run, lumi))

dbcursor.commit()
replist.close()
