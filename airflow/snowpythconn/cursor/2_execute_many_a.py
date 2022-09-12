import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector
connector.paramstyle='qmark'


#executemany(command, seq_of_parameters)
cur= snowcnt.connect_snowflake()

# Useful for batch insert operation
cur.execute("USE DATABASE DEMO_DB",timeout=120)
cur.execute("CREATE OR REPLACE TABLE BATCH_INSERT(A varchar, B varchar)",timeout=120)

# A list of lists
sequence_of_parameters1 = [ ['Smith', 'Ann'], ['Jones', 'Ed'] ]

# A tuple of tuples
sequence_of_parameters2 = ( ('Cho', 'Kim'), ('Cooper', 'Pat') )

stmt2 = "insert into PUBLIC.BATCH_INSERT (A, B) values (?, ?)"
cur.executemany(stmt2, sequence_of_parameters1)
cur.executemany(stmt2, sequence_of_parameters2)

print(cur.description())

cur.close()
