#!/usr/bin/python

import getopt
import sys
import cx_Oracle

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
            #ver = connection.version.split(":")
            #print ver
        except cx_Oracle.DatabaseError, exception:
            myiPrintf ('Failed to connect to %s\n',database)
            myprintException (exception)
            exit (1)
        cursor = connection.cursor()
        return cursor

    def execute_query(self, cursor, diff_user1, diff_user2):
        sql = "SELECT OBJECT_NAME FROM DBA_OBJECTS WHERE OWNER = :owner order by OBJECT_NAME"
        for diff_user in [diff_user1, diff_user2]:
            print diff_user
            try:
                cursor.execute (sql, owner = diff_user)
            except cx_Oracle.DatabaseError, exception:
                myPrintf ('Failed to prepare cursor\n')
                myprintException (exception)
                exit (1)
            for object_name in cursor.fetchall():
                myPrintf(" %s\n",object_name)

    def close_cursor(self, cursor):
        cursor.close ()

def help():
    print 'Usage: ' + sys.argv[0] + ' -s <ORACLE_SID> -u <DB_ADM_USR> -p <DB_ADM_PWD> --diff_user1 <DIFF_USER1> --diff_user2 <DIFF_USER2>'

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:u:p:d1:d2:", ["help", "sid=", "db_adm_usr=", "db_adm_pwd=", "diff_user1=", "diff_user2="])
        if not opts:
            print 'No options supplied'
            help()
            sys.exit(1)
    except getopt.GetoptError as err:
        print err
        help()
        sys.exit(2)
    for opt, arg in opts:
        if len(opts) < 5:
            print 'ERROR: Missing required arguments!'
            help()
            sys.exit(2)
        if opt in ("-h", "--help"):
            help()
            sys.exit(2)
        elif opt in ("-s", "--sid"):
            sid = arg
	elif opt in ("-u", "--user"):
            user = arg
        elif opt in ("-p", "--password"):
            password = arg
        elif opt in ("-d1", "--diff_user1"):
            diff_user1 = arg
        elif opt in ("-d2", "--diff_user2"):
            diff_user2 = arg
            # Stablish new connection to database
    	    con = DBConnection(sid,user,password)
            # Open new cursor on database
    	    cursor = con.add_connection()
            # Create user function
    	    con.execute_query(cursor,diff_user1,diff_user2)
            # Closing database cursor
    	    con.close_cursor(cursor)

## MAIN ##
if __name__=='__main__':
    main()
## END MAIN ##
