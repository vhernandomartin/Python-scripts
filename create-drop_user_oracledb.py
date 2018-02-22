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

    def execute_query(self, cursor):
	#cursor = connection.cursor()
        try:
            cursor.execute ('select count(*) from dba_objects')
        except cx_Oracle.DatabaseError, exception:
            myPrintf ('Failed to select from EMP\n')
            myprintException (exception)
            exit (1)

        count = cursor.fetchone ()[0]
        print count
        #cursor.close ()
        #connection.close ()

    def create_user(self, cursor, new_user, new_user_pwd):
        sql = "CREATE USER " + new_user + " identified by " + new_user_pwd
        try:
            cursor.execute (sql)
            #cursor.execute ('CREATE USER TESTUSR identified by TESTUSR')
        except cx_Oracle.DatabaseError, exception:
            myPrintf ('Failed to prepare cursor\n')
            myprintException (exception)
            exit (1)

    def drop_user(self, cursor):
        try:
            cursor.execute ('DROP USER TEST cascade')
        except cx_Oracle.DatabaseError, exception:
            myPrintf ('Failed to prepare cursor\n')
            myprintException (exception)
            exit (1)

    def close_cursor(self, cursor):
        cursor.close ()

def help():
    print 'Usage: ' + sys.argv[0] + ' -s <ORACLE_SID> -u <DB_ADM_USR> -p <DB_ADM_PWD> -U <NEW_USER> -P <NEW_USER_PWD>'

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hs:u:p:U:P:", ["help", "sid=", "db_adm_usr=", "db_adm_pwd=", "new_user=", "new_user_pwd="])
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
        elif opt in ("-U", "--new_user"):
            new_user = arg
        elif opt in ("-P", "--new_user_pwd"):
            new_user_pwd = arg
            # Stablish new connection to database
    	    con = DBConnection(sid,user,password)
            # Open new cursor on database
    	    cursor = con.add_connection()
            # Execute query
    	    #con.execute_query(cursor)
            # Create user function
    	    con.create_user(cursor,new_user,new_user_pwd)
    	    #con.drop_user(cursor)
            # Closing database cursor
    	    con.close_cursor(cursor)

## MAIN ##
if __name__=='__main__':
    main()
## END MAIN ##
