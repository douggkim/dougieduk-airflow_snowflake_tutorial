from pickle import TRUE
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector
from snowflake.connector import ProgrammingError
import time
from snowflake.connector import DictCursor

cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()

dict_cur = conn.cursor(DictCursor)

# Use dict cursor
try:
    dict_cur.execute("SELECT col1, col2 FROM demo_db.public.test_table")
    for rec in dict_cur:
        print('{0}, {1}'.format(rec['COL1'], rec['COL2']))
finally:
    dict_cur.close()


# Use normal cursor

try:
    cur.execute("SELECT col1, col2 FROM demo_db.public.test_table")
    for rec in cur:
        print('{0}, {1}'.format(rec['COL1'], rec['COL2']))
finally:
    cur.close()
