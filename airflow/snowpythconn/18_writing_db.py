from pickle import TRUE
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector

import pandas
from snowflake.connector.pandas_tools import write_pandas

cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()

conn.close()

# Create a DataFrame containing data about test_table
df = pandas.DataFrame([('Mark', '10'), ('Luke', '20')], columns=['COL1', 'COL2'])

cur.execute('select seq4() as num from table(generator(rowcount => 100000));')

# Return a Pandas DataFrame containing all of the results.
table = cur.fetch_pandas_all()
type(table)

success, nchunks, nrows,output = write_pandas(conn, table,'SEQ','DEMO_DB','PUBLIC',2)

# Write the data from the DataFrame to the table named "test_table".
success, nchunks, nrows,output = write_pandas(conn, df,'TEST_TABLE','DEMO_DB','PUBLIC',chunk_size=10)

print(nrows)
print(success)
print(nchunks)
print(output)


write_pandas(conn,df,'demo_db.public.test_table','demo_db','public')

from snowflake.connector.pandas_tools import pd_writer

# to write the data from the DataFrame to the table named "customers"
# in the Snowflake database.
df.to_sql('TEST_TABLE', conn, index=False, method=pd_writer)