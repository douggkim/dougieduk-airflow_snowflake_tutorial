import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt

cur= snowcnt.connect_snowflake()
print(cur)


