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

cur.execute("rm @DEMO_DB.PUBLIC.%EMP_PYTHON")
cur.execute("PUT file:///opt/dummy/data/employee.csv @DEMO_DB.PUBLIC.%EMP_PYTHON",
file_stream=f)

#timeout parameter
cur2=cur.execute("SELECT * FROM DEMO_DB.PUBLIC.EMP_PYTHON",timeout=120)

cur3=cur.execute("SELECT * FROM DEMO_DB.PUBLIC.TEST_TABLE",timeout=120)

cur.describe("SELECT * FROM DEMO_DB.PUBLIC.TEST_TABLE",timeout=120)

# Get query id

cur2.sfqid
cur.sfqid
cur3.sfqid

# Get record count
cur.rowcount

cur.description

cur.arraysize
cur.connection

cur.messages

cur.errorhandler()


cur.close()


