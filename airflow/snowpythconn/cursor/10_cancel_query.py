from pickle import TRUE
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector
from snowflake.connector import ProgrammingError
import time

cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()

cur.execute_async('select count(*) from table(generator(timeLimit => 60))')

query_id = cur.sfqid

try:
  cur.execute(f"""SELECT SYSTEM$CANCEL_QUERY('{query_id}')""")
  result = cur.fetchall()
  print(len(result))
  print(result[0])
finally:
  cur.close()