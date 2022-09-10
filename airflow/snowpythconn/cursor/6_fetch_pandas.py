from pickle import TRUE
import time
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector

cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()

sql = "SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER"
cur.execute(sql)

# Fetch the result set from the cursor and deliver it as the Pandas DataFrame.
#This method fetches all the rows in a cursor and loads them into a Pandas DataFrame.


start_time = time.time()
df = cur.fetch_pandas_all()
print("--- %s seconds ---" % (time.time() - start_time))

type(df)

#This method fetches all the rows in a cursor and loads them into a PyArrow table.
start_time = time.time()
df = cur.fetch_arrow_all()
print("--- %s seconds ---" % (time.time() - start_time))

type(df)

for df in cur.fetch_pandas_batches():
    print('****batch break*******')
    print(df)

#Datasize pyarrow  pandas
#--------------------------
#10 MB    5 sec    5 sec
#100 MB   37 sec   63 Sec
#1000 MB  360 sec 