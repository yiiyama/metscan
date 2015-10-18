import sys

# probably not safe..
sys.path.append('/usr/lib/python2.6/site-packages')
import mysql.connector as mysqlc

import config

# setup connection to local DB
dbconn = mysqlc.connect(user = config.dbuser, password = config.dbpass, host = config.dbhost, database = config.dbname, buffered = True)
dbcursor = dbconn.cursor()
