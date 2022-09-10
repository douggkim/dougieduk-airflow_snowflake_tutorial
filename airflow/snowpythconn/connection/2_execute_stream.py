from pickle import TRUE
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector
from codecs import open


cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()

all_cursors=[]

with open('./snowpythconn/Sql_files/test_query.sql', 'r', encoding='utf-8') as f:
    for cur in conn.execute_stream(f,remove_comments=True):
        all_cursors.append(cur)
        for ret in cur:
            print(ret[0])

print(len(all_cursors))