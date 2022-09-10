from pickle import TRUE
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector
from snowflake.connector import ProgrammingError
import time

cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()



# Submit an asynchronous query for execution.
cur.execute_async('select count(*) from table(generator(timeLimit => 120))')
# Retrieve the results.
cur.get_results_from_sfqid(cur.sfqid)
results = cur.fetchall()
print(f'{results[0]}')


# Execute a long-running query asynchronously.
cur.execute_async('select count(*) from table(generator(timeLimit => 60))')
# Wait for the query to finish running.
query_id = cur.sfqid
while conn.is_still_running(conn.get_query_status(query_id)):
  #time.sleep(1)
  print('I am waiting')

cur.close()

status=conn.get_query_status(query_id).name

print(status)
type(status)
