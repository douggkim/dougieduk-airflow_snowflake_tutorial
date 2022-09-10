from pickle import TRUE
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector

import logging

cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()

file_name = '/tmp/snowflake_python_connector.log'

#basic logining
logging.basicConfig(
                filename=file_name,
                level=logging.INFO)

with open('./snowpythconn/Sql_files/test_query.sql', 'r', encoding='utf-8') as f:
    for cur in conn.execute_stream(f):
        for ret in cur:
            print(ret[0])



# Logging including the timestamp, thread and the source code location
import logging
for logger_name in ['snowflake.connector', 'botocore', 'boto3']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.FileHandler('/tmp/python_connector.log')
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
    logger.addHandler(ch)   

with open('./snowpythconn/Sql_files/test_query.sql', 'r', encoding='utf-8') as f:
    for cur in conn.execute_stream(f):
        for ret in cur:
            print(ret[0])


# Logging including the timestamp, thread and the source code location
import logging
from snowflake.connector.secret_detector import SecretDetector
for logger_name in ['snowflake.connector', 'botocore', 'boto3']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.FileHandler('/tmp/python_connector_secure.log')
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(SecretDetector('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
    logger.addHandler(ch)

with open('./snowpythconn/Sql_files/test_query.sql', 'r', encoding='utf-8') as f:
    for cur in conn.execute_stream(f):
        for ret in cur:
            print(ret[0])

