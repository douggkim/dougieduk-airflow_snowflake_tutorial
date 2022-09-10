import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector

cur= snowcnt.connect_snowflake()

cur.execute("USE ROLE ACCOUNTADMIN")

cur.execute(
            "INSERT INTO demo_db.public.test_table(col1, col2)"
            "VALUES(%(col1)s, %(col2)s)", {
                'col1': '789',
                'col2': 'test string3',
                })

cur.execute(
    "INSERT INTO demo_db.public.test_table(col1, col2) "
    "VALUES(%s, %s)", (
        788,
        'test string4'
    ))

# Binding data for IN operator
# But you can't see the results here
cur.execute(
    "SELECT col1, col2 FROM demo_db.public.test_table"
    " WHERE col2 IN (%s)", (
        ['test string3', 'test string4'],
    ))


cur.close()

