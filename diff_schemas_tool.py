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
from email.mime.text import MIMEText
## END MODULES ##

## VARIABLES ##
oracle_sid = 'PCRMLBJ'
dp_directory = 'DUMPPREPRO'

dp_path = '/cluster/datapump/PREPRO'
reports_path = '/cluster/datapump/PREPRO/reports'
FNULL = open(os.devnull, 'w')
## END VARIABLES ##

## FUNCTIONS ##
def getFileNames():
    global schemas
    global objectype
    global pattern
    schemas = sys.argv[2]
    objectype = sys.argv[4]
    timestring = time.strftime("%d%m%Y")
    pattern = schemas + '_' + timestring + '_' + oracle_sid + '_METADATA'
    return pattern
    return schemas
    return objectype

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

def generateSqlFile(schemas,objectype,dbuser,dbpassword):
    #schemas = sys.argv[2]
    #dumpfiles = [name for name in os.listdir('.') if name.endswith(".dmp")]
    dumpfiles = glob.glob(schemas + '*.dmp')

    if len(dumpfiles) != 2:
        print "ERROR: Need 2 files to compare!"
        raise Exception("Insuficient files to compare")
    else:
        print "++INFO: Importing dmp file to generate SQL File..."
        sqlfilevar = {}
        i = 0
        for dmpfile in dumpfiles:
            sqlfile = dmpfile.replace("dmp","sql")
            impdp_args = dbuser + '/' + dbpassword + '@' + oracle_sid + ' DIRECTORY=' + dp_directory + ' DUMPFILE=' + dmpfile + ' SQLFILE=' + sqlfile + ' INCLUDE=' + objectype
            impdp_process = subprocess.Popen(["impdp", impdp_args], stdout=FNULL, stderr=subprocess.PIPE)
            sqlfilevar[i] = sqlfile
	    i += 1
        time.sleep(30)
        return sqlfilevar

def compareDumps(sqlFiles = [], *args):
    print "++INFO: matching differences between files:"
    print "\__" + sqlFiles[0]
    print "\__" + sqlFiles[1]
    with open(sqlFiles[0], 'r') as dump1:
        with open(sqlFiles[1], 'r') as dump2:
            global dmpsortedi
	        dmpsortedi = {}
            i = 0
            for dmp in [sqlFiles[0], sqlFiles[1]]:
		        dmpsorted = dmp + '_SORTED'
                greparg = 'CREATE ' + objectype
                file = open(dmpsorted,'w')
		        p1 = subprocess.Popen(["grep",greparg,dmp], stdout=subprocess.PIPE)
	            p2 = subprocess.Popen('sort'.split(), stdin=p1.stdout, stdout=file)
                p1.stdout.close()
                p2.communicate()[0]
		        p1.wait()
                dmpsortedi[i] = dmpsorted
            i += 1
            return dmpsortedi

def diffDumps(sortedDmps = [], *args):
    print "++INFO: Performing final Diff between files:"
    print "-->" + sortedDmps[0]
    print "-->" + sortedDmps[1]
    with open(sortedDmps[0], 'r') as sorteddmp1:
        with open(sortedDmps[1], 'r') as sorteddmp2:
            diff = difflib.unified_diff(
                sorteddmp1.readlines(),
                sorteddmp2.readlines(),
                fromfile=sortedDmps[0],
                tofile=sortedDmps[1],
            )
            orig_stdout = sys.stdout
            for line in diff:
		f = open('diff_report.txt','a')
                #sys.stdout = open('diff_report.txt','a')
                sys.stdout = f
                sys.stdout.write(line)
	    #sys.stdout.close()
	    sys.stdout = sys.__stdout__
	    f.close()

def cleanEnvironment():
    schemas = sys.argv[2]
    print "++INFO: Deleting SQLfiles and DMPfiles..."
    for dumps in glob.glob(schemas + "*.dmp"):
        os.remove(dumps)
    for sqls in glob.glob(schemas + "*.sql"):
        os.remove(sqls)
    for sorteddmps in glob.glob(schemas + "*_SORTED"):
        os.remove(sorteddmps)
    print "++INFO: Moving report diff file to report dir..."
    timestring = time.strftime("%d%m%Y%H%M%S")
    os.rename("diff_report.txt","reports/diff_report.txt_" + timestring)

def help():
    #if len(sys.argv) != 2:
    print 'Usage: ' + sys.argv[0] + ' -s <schema> -o <object_type>'
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

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:o:", ["help", "schema=", "object-type=",])
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
	elif opt in ("-o", "--object-type"):
            objectype = arg
            getFileNames()
            exportDatapump(pattern,schemas,dbuser,dbpassword)
            time.sleep(30)
            compareDumps(generateSqlFile(schemas,objectype,dbuser,dbpassword),objectype)
	        diffDumps(dmpsortedi)
            #sendEmail()
            cleanEnvironment()
## END FUNCTIONS ##

## MAIN ##
if __name__=='__main__':
    main()
## END MAIN ##
