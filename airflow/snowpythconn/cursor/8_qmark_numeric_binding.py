import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector
from datetime import datetime

connector.paramstyle='qmark'

cur= snowcnt.connect_snowflake()

cur.execute("USE ROLE ACCOUNTADMIN")

cur.execute(
    "INSERT INTO demo_db.public.test_table(col1, col2)"
    "VALUES(?, ?)", (
        '789',
        'test string3'
    ))

connector.paramstyle='numeric'

cur.execute(
    "INSERT INTO demo_db.public.test_table(col1, col2) "
    "VALUES(:1, :2)", (
        787,
        'test string31'
    ))


# Inserting datetime values.

cur.execute(
    "CREATE OR REPLACE TABLE demo_db.public.test_table2 ("
    "   col1 int, "
    "   col2 string, "
    "   col3 timestamp_ltz"
    ")"
)

cur.execute(
    "INSERT INTO demo_db.public.test_table2(col1,col2,col3) "
    "VALUES(?,?,?)", (
        987,
        'test string4',
        ("TIMESTAMP_LTZ", datetime.now())
    )
 )

# Binding data for IN operator
# But you can't see the results here
connector.paramstyle='qmark'

cur.execute(
    "SELECT col1, col2 FROM demo_db.public.test_table"
    " WHERE col2 IN (?)", (
        ['test string3', 'test string4'],
    ))

cur.close()

