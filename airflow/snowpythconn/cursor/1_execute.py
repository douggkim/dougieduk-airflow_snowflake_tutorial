import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt

#execute(command [, parameters][, timeout][, file_stream])

cur= snowcnt.connect_snowflake()

# Upload file to table stage
cur.execute("USE ROLE ACCOUNTADMIN")

cur.execute("PUT file:///opt/airflow/data/employee.csv @DEMO_DB.PUBLIC.%EMP_PYTHON")

cur.execute("COPY INTO DEMO_DB.PUBLIC.EMP_PYTHON FROM @DEMO_DB.PUBLIC.%EMP_PYTHON")


#using file stream parameter
f = open("/opt/airflow/data/employee.csv",'rb')

print(cur.execute("rm @DEMO_DB.PUBLIC.%EMP_PYTHON"))
# once there is a file_stream parameter, the cursor won't consider the directory behind "PUT"
print(cur.execute("PUT file:///opt/dummy/data/employee.csv @DEMO_DB.PUBLIC.%EMP_PYTHON",
file_stream=f))

#timeout parameter
cur2=cur.execute("SELECT * FROM DEMO_DB.PUBLIC.EMP_PYTHON",timeout=120)

cur3=cur.execute("SELECT * FROM DEMO_DB.PUBLIC.EMP_PYTHON",timeout=120)

cur.describe("SELECT * FROM DEMO_DB.PUBLIC.EMP_PYTHON",timeout=120)

# Get query id

print(f"cur2: {cur2.sfqid}")
print(f"cur: {cur.sfqid}")
print(f"cur3: {cur3.sfqid}")

# Get record count
print(f"cur rowcount:{cur.rowcount}")

print(f"cur description: {cur.description}")

print(f"cur arraysize: {cur.arraysize}")
print(f"cur.connection {cur.connection}")

print(f"cur.messages: {cur.messages}")

# cur.errorhandler()


cur.close()


