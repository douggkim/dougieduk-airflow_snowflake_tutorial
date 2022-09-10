import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector


cur= snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()

# Catching the syntax error
try:
    cur.execute("SELECT * FROM demo_db.public.test_table")
except connector.errors.ProgrammingError as e:
    # default error message
    print(e)
    # customer error message
    print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
finally:
    cur.close()


cur= snowcnt.connect_snowflake()

# Catching the syntax error
try:
    cur.execute("SELECT * FROM demo_db.public.tes_table")
except connector.errors.ProgrammingError as e:
    # default error message
    print(e)
    # customer error message
    print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
finally:
    cur.close()


# Catch timeout error

conn.cursor().execute("begin")
try:
   conn.cursor().execute("insert into demo_db.public.test_table(col1,col2) values(3, 'test3'), (4,'test4')", timeout=10) # long query

except connector.errors.ProgrammingError as e:
   if e.errno == 604:
      print("timeout")
      conn.cursor().execute("rollback")
   else:
      raise e
else:
   conn.cursor().execute("commit")




   # -- Exception clause : snowflake
# except sf.errors.ProgrammingError as e:
#         print('SQL Execu tion Error: {0}'.format(e.msg))
#         print('Snowflake Query Id: {0}'.format(e.sfqid))
#         print('Error Number: {0}'.format(e.errno))
#         print('SQL State: {0}'.format(e.sqlstate))