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
cur.execute_async('select count(*) from table(generator(timeLimit => 30))')

# Get the query ID for the asynchronous query.
query_id = cur.sfqid

# Close the cursor and the connection.
cur.close()
conn.close()

# Open a new connection.
new_conn = snowcnt.return_conn_obj()

# Create a new cursor.
new_cur = snowcnt.connect_snowflake()


# Retrieve the results.
while new_conn.is_still_running(new_conn.get_query_status(query_id)):
  #results = new_cur.fetchall()
  #print(f'{results[0]}')
  print('I am waiting')


# Retrieve the results.
new_cur.get_results_from_sfqid(query_id)
results = new_cur.fetchall()
print(f'{results[0]}')


# Wait for the query to finish running and raise an error
# if a problem occurred with the execution of the query.
try:
  query_id = cur.sfqid
  while conn.is_still_running(conn.get_query_status_throw_if_error(query_id)):
    print('All good')
except ProgrammingError as err:
  print('Programming Error: {0}'.format(err))


  query_id = cur.sfqid
  if conn.is_an_error(conn.get_query_status(query_id)):
    raise Exception("Sorry something went wrong")
  else:
    print('All good')

  query_id = cur.sfqid
  if conn.is_an_error(conn.get_query_status(query_id)):
    raise Exception("Sorry something went wrong")
  else:
    print('All good')


cur.close()

