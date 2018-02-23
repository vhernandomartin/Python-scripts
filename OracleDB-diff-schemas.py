#!/usr/bin/python

import difflib
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
        except cx_Oracle.DatabaseError, exception:
            myiPrintf ('Failed to connect to %s\n',database)
            myprintException (exception)
            exit (1)
        cursor = connection.cursor()
        return cursor

    def sql_get_object_name(self, cursor, col1, owner, object_type):
        sql = "SELECT " + col1 + " FROM DBA_OBJECTS WHERE OWNER = :owner AND OBJECT_TYPE = :object_type order by OBJECT_NAME"
        try:
            cursor.execute (sql, owner = owner, object_type = object_type)
        except cx_Oracle.DatabaseError, exception:
            myPrintf ('Failed to prepare cursor\n')
            myprintException (exception)
            exit (1)
        object_list = cursor.fetchall()
        return object_list
        #for object_name in cursor.fetchall():
        #    myPrintf(" %s;\n",object_name)

    def close_cursor(self, cursor):
        cursor.close ()

def help():
    print 'Usage: ' + sys.argv[0] + ' -s <ORACLE_SID> -u <DB_ADM_USR> -p <DB_ADM_PWD> --diff_user1 <DIFF_USER1> --diff_user2 <DIFF_USER2>'

def get_all_objects(con,diff_user1,diff_user2):
    cursor = con.add_connection()
    all_object_types = ['SEQUENCE','TABLE PARTITION','QUEUE','PROCEDURE','DATABASE LINK','LOB','PACKAGE BODY','PACKAGE','TRIGGER','MATERIALIZED VIEW','TABLE','INDEX','VIEW','FUNCTION','SYNONYM','TYPE','JOB']
    for db_object_type in all_object_types:
        print '#### ' + db_object_type + ' ####'
        objects_output = {}
        i = 0
        for db_user in [diff_user1,diff_user2]:
            #print '  \__ ' + db_user
            object_list = con.sql_get_object_name(cursor,'OBJECT_NAME',db_user,db_object_type)
            objects_output[i] = object_list
            i += 1
        diff_part1 = "\n".join(str(e) for e in objects_output[0]).replace("(","").replace(")","")
        diff_part2 = "\n".join(str(e) for e in objects_output[1]).replace("(","").replace(")","")
        diff_part1 = diff_part1.splitlines(1)
        diff_part2 = diff_part2.splitlines(1)
        diff = difflib.unified_diff(diff_part1,diff_part2, n=0)
        print (''.join(diff))

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
            get_all_objects(con,diff_user1,diff_user2)
    	    #con.execute_query(cursor,diff_user1,diff_user2)
            # Closing database cursor
    	    con.close_cursor(cursor)

## MAIN ##
if __name__=='__main__':
    main()
## END MAIN ##
