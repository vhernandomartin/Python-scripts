# Python-scripts

#### OracleDB-diff-schemas.py
The following script will connect to the schemas provided as arguments and will compare all objects to match differences.
```
./OracleDB-diff-schemas.py -s PCRMLBJ -u system -p manager --diff_user1 DIFF0 --diff_user2 DIFF1
```
Provide *ORACLE_SID*, *DBUSER* and *DBPASSWORD* with admin privileges since DBA_ views are used, and both users to compare.

#### create-drop_user_oracledb.py
Sample script to create or delete users on Oracle database.
```
./create-drop_user_oracledb.py -s PCRMLBJ -u system -p manager -U TESTUSR -P TESTUSER
```
Provide *ORACLE_SID*, *DBUSER* and *DBPASSWORD* with admin privileges, and finally new user name and the password assigned to it.
