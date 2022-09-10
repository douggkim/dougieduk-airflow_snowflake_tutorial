from pickle import TRUE
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector
from codecs import open


cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()


cursor_list = conn.execute_string(
    "SELECT * FROM demo_db.public.test_table WHERE col1 LIKE '7%';"
    "SELECT * FROM demo_db.public.test_table WHERE col2 LIKE 't%';"
    )

type(cursor_list)
list(cursor_list)

for cursor in cursor_list:
   for row in cursor:
      print(row[0], row[1])