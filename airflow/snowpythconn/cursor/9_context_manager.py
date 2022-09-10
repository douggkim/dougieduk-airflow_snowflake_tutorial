from pickle import TRUE
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector

#connector.connect.autocommit=TRUE
    
cur = snowcnt.connect_snowflake()

cur.execute("INSERT INTO demo_db.public.test_table VALUES(1, 'test1')")
cur.execute("INSERT INTO demo_db.public.test_table VALUES(2, 'test2')")
cur.execute("INSERT INTO demo_db.public.test_table VALUES(not numeric value, 'test3')")

cur.close()
