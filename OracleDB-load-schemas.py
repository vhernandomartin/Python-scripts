#!/usr/bin/python

## MODULES ##
import difflib
import sys
import os
import subprocess
import time
import glob
import getopt
import smtplib
import re
import cx_Oracle
from email.mime.text import MIMEText
## END MODULES ##

## VARIABLES ##
oracle_sid = os.environ['ORACLE_SID']
dp_directory = 'DUMPPREPRO'

dp_path = '/cluster/datapump/PREPRO'
reports_path = '/cluster/datapump/PREPRO/reports'
FNULL = open(os.devnull, 'w')
## END VARIABLES ##

## FUNCTIONS ##
def printf (format,*args):
    sys.stdout.write (format % args)

def printException (exception):
    error, = exception.args
    printf ("Error code = %s\n",error.code);
    printf ("Error message = %s\n",error.message);

class DBConnection:
    global myPrintf
    global myprintException
    myPrintf = printf
    myprintException = printException

    def __init__(self, database, username, password):
        self.database = database
        self.username = username
        self.password = password

    def imprime(self):
        return self.database
        return self.username
        return self.password

    def add_connection(self):
        try:
            username = self.username
            password = self.password
	        database = self.database
            connection = cx_Oracle.connect (username,password,database)
            ver = connection.version.split(":")
	    #myPrintf ('Connected to database %s\n',database)
        except cx_Oracle.DatabaseError, exception:
            myiPrintf ('Failed to connect to %s\n',database)
            myprintException (exception)
            exit (1)
        cursor = connection.cursor()
        return cursor

    def execute_query(self, cursor, sql, program):
        try:
            #cursor.execute ('SELECT COUNT(*) FROM DBA_USERS')
            cursor.execute (sql,program = program)
        except cx_Oracle.DatabaseError, exception:
            myPrintf ('Failed to select from DBA_USERS\n')
            myprintException (exception)
            exit (1)

        count = cursor.fetchone ()[0]
	return count

    def create_user(self, cursor):
        try:
            cursor.execute ('CREATE USER DIFF0 identified by test')
	        print 'User created: DIFF0'
            cursor.execute ('CREATE USER DIFF1 identified by test')
	        print 'User created: DIFF1'
        except cx_Oracle.DatabaseError, exception:
            myPrintf ('Failed to prepare cursor\n')
            myprintException (exception)
            exit (1)

    def drop_user(self, cursor):
        try:
            cursor.execute ('DROP USER DIFF0 cascade')
            print 'User Dropped: DIFF0'
            cursor.execute ('DROP USER DIFF1 cascade')
	        print 'User Dropped: DIFF1'
        except cx_Oracle.DatabaseError, exception:
            myPrintf ('Failed to prepare cursor\n')
            myprintException (exception)
            exit (1)

    def close_cursor(self, cursor):
        cursor.close ()

def getFileNames():
    global schemas
    global pattern
    schemas = sys.argv[2]
    timestring = time.strftime("%d%m%Y")
    pattern = schemas + '_' + timestring + '_' + oracle_sid + '_METADATA'
    return pattern
    return schemas

def getUserPassword():
    global dbuser
    global dbpassword
    user = raw_input("Oracle DB user: ")
    password = raw_input("Oracle DB Password: ")
    dbuser = user
    dbpassword = password

def exportDatapump(pattern,schemas,dbuser,dbpassword):
    #schemas = sys.argv[2]
    dumpfile = pattern + '.dmp'
    logfile = pattern + '.log'
    print "++INFO: Exporting PRE metadata Datapump..."
    expdp_args = dbuser + '/' + dbpassword + '@' + oracle_sid + ' DIRECTORY=' + dp_directory + ' SCHEMAS=' + schemas + ' CONTENT=METADATA_ONLY DUMPFILE=' + dumpfile + ' LOGFILE=' + logfile + ' EXCLUDE=STATISTICS'
    expdp_process = subprocess.Popen(["expdp", expdp_args], stdout=FNULL, stderr=subprocess.PIPE)

def generateSqlFile(schemas,dbuser,dbpassword):
    dumpfiles = glob.glob(schemas + '*.dmp')

    if len(dumpfiles) != 2:
        print "ERROR: Need 2 files to compare!"
        raise Exception("Insuficient files to compare")
    else:
        print "++INFO: Importing dmp file to generate SQL File..."
        i = 0
        for dmpfile in dumpfiles:
            dest_schema = 'DIFF' + str(i)
            impdp_args_real = dbuser + '/' + dbpassword + '@' + oracle_sid + ' DIRECTORY=' + dp_directory + ' DUMPFILE=' + dmpfile + ' REMAP_SCHEMA=' + schemas + ':' + dest_schema
            impdp_process_real = subprocess.Popen(["impdp", impdp_args_real], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(30)
	        i += 1

def cleanEnvironment():
    schemas = sys.argv[2]
    print "++INFO: Deleting SQLfiles and DMPfiles..."
    for dumps in glob.glob(schemas + "*.dmp"):
        os.remove(dumps)
    for sqls in glob.glob(schemas + "*.sql"):
        os.remove(sqls)
    for sorteddmps in glob.glob(schemas + "*_SORTED"):
        os.remove(sorteddmps)

def help():
    #if len(sys.argv) != 2:
    print 'Usage: ' + sys.argv[0] + ' -s <schema>'
    #sys.exit(1)

def sendEmail():
    report = "diff_report.txt"
    fp = open(report, 'rb')
    msg = MIMEText(fp.read())
    fp.close()
    msg['Subject'] = 'Diff Report'
    msg['From'] = 'foo@bar.com'
    msg['To'] = 'vhernandomartin@gmail.com'
    s = smtplib.SMTP('localhost')
    s.sendmail('foo@bar.com', ['vhernandomartin@gmail.com'], msg.as_string())
    s.quit()

def waitImpdp(con):
    print "++INFO: Waiting to finish import PRE metadata Datapump..."
    cursor = con.add_connection()
    sql = "SELECT count(*) from v$session where program like :program"
    program = '%DW%'
    num = con.execute_query(cursor,sql,program)
    #print num
    while True:
        num = con.execute_query(cursor,sql,program)
        if num < 1:
            break
            con.close_cursor(cursor)

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:", ["help", "schema=",])
        if not opts:
            print 'No options supplied'
            help()
            sys.exit(1)
    except getopt.GetoptError as err:
        print err
        help()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            help()
            sys.exit(2)
        elif opt in ("-s", "--schema"):
            schemas = arg
            # Getting Dump filenames
            getFileNames()
	        getUserPassword()
            # Exporting PRE Datapump
            exportDatapump(pattern,schemas,dbuser,dbpassword)
            time.sleep(30)
            # Connecting to database to create new schemas
            con = DBConnection(oracle_sid,dbuser,dbpassword)
            cursor = con.add_connection()
            con.drop_user(cursor)
            con.create_user(cursor)
            #con.execute_query(cursor)
            # Importing datapump into new schemas and generate sqlfiles to compare
            generateSqlFile(schemas,dbuser,dbpassword)
            #sendEmail()
	        waitImpdp(con)
            con.close_cursor(cursor)
            cleanEnvironment()
## END FUNCTIONS ##

## MAIN ##
if __name__=='__main__':
    main()
## END MAIN ##
