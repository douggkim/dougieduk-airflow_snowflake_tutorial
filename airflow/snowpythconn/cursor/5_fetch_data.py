from pickle import TRUE
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector
from snowflake.connector import ProgrammingError
import time

cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()

try:
    cur.execute("SELECT col1, col2 FROM demo_db.public.test_table ORDER BY col1")
    for (col1, col2) in cur:
        print('{0}, {1}'.format(col1, col2))
finally:
        cur.close()

#Alternatively, the Snowflake Connector for Python provides a convenient shortcut:
for (col1, col2) in conn.cursor().execute("SELECT col1, col2 FROM demo_db.public.test_table"):
    print('{0}, {1}'.format(col1, col2))

#If you need to get a single result (i.e. a single row), use the fetchone method
col1, col2 = conn.cursor().execute("SELECT col1, col2 FROM demo_db.public.test_table").fetchone()
print('{0}, {1}'.format(col1, col2))

#If you need to get the specified number of rows at a time, use the fetchmany method with the number of rows
cur = conn.cursor().execute("SELECT col1, col2 FROM demo_db.public.test_table")
ret = cur.fetchmany(10)
print(ret)
while len(ret) > 0:
    ret = cur.fetchmany(1)
    print(ret)

#If you need to get all results at once:
results = conn.cursor().execute("SELECT col1, col2 FROM demo_db.public.test_table").fetchall()
for rec in results:
    print('%s, %s' % (rec[0], rec[1]))

type(results)