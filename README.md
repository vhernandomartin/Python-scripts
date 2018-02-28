# Python-scripts

#### OracleDB-diff-schemas.py
The following script will connect to the schemas provided as arguments and will compare all objects to match differences.
```
./OracleDB-diff-schemas.py -s TESTDB -u system -p manager --diff_user1 DIFF0 --diff_user2 DIFF1
```
Provide *ORACLE_SID*, *DBUSER* and *DBPASSWORD* with admin privileges since DBA_ views are used, and both users to compare.

#### OracleDB-load-schemas.py
Script to import two schemas into a Oracle Database.
The purpose of this data load is to be able to compare both schemas later.
A dump is exported from database *A*, the other dump comes from other environment, database *B*, PRO for example.
It's assumed that in the same directory is already placed a dump belonging the other environment (database *B*).
*SOURCE_ORACLE_SID* is the ORACLE_SID belonging to source database, where tool will be connecting to do the expdp.
*DEST_ORACLE_SID* is the database where both dumps will be imported (*A* and *B* database dumps), this database is only to store metadata.
```
./OracleDB-load-schemas.py -u TSTUSR -s SOURCE_ORACLE_SID -d DEST_ORACLE_SID
```
Provide *DBUSER* to export data from this schema.

#### create-drop_user_oracledb.py
Sample script to create or delete users on Oracle database.
```
./create-drop_user_oracledb.py -s TESTDB -u system -p manager -U TESTUSR -P TESTUSER
```
Provide *ORACLE_SID*, *DBUSER* and *DBPASSWORD* with admin privileges, and finally new user name and the password assigned to it.
