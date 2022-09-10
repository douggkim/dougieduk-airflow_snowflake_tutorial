import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector

cur= snowcnt.connect_snowflake()

cur.execute("SELECT * FROM demo_db.public.test_table")
print(','.join([col[0] for col in cur.description]))


result_metadata_list = cur.describe("SELECT * FROM DEMO_DB.PUBLIC.EMP_PYTHON")
print(','.join([col.name for col in result_metadata_list]))

print(result_metadata_list)

result_metadata_list[0][3]
result_metadata_list[1][3]
result_metadata_list[2][3]
