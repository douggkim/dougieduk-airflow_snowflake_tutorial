import sys

from marshmallow_sqlalchemy import SQLAlchemyAutoSchemaOpts
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector

import glob
import pandas as pd

cur= snowcnt.connect_snowflake()

# Read parameter files with copy commands

def pd_read_pattern(pattern):
    files = glob.glob(pattern)

    df = pd.DataFrame()
    for f in files:
        df = df.append(pd.read_csv(f))

    return df.reset_index(drop=True)

def execute_copy_cmd():

    cur= snowcnt.connect_snowflake()
    
    df = pd_read_pattern('/opt/airflow/snowpythconn/apps/parameter/copy_emp_snow_cp_v1_param.csv')


    for index, row in df.iterrows():
        
        stage_object=row['STAGE_OBJECT']
        folder_path=row['S3_FILE_PATH']
        file_format=row['FILE_FORMAT']

        database=row['DATABASE']
        schema=row['SCHEMA']
        tablename=row['TABLE_NAME']
        pattern=row['PATTERN']
        
        warehouse=row['WAREHOUSE']

        cur.execute(f"""USE DATABASE {database}""")
        cur.execute(f"""USE SCHEMA {schema}""")
        cur.execute(f"""USE WAREHOUSE {warehouse}""")


        try:

            copy_command=f"""copy into {database}.{schema}.{tablename} from @{stage_object}{folder_path} 
            FILE_FORMAT = {file_format} PATTERN = '{pattern}' ON_ERROR=CONTINUE"""

            cur.execute(copy_command)

        except connector.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

        # store query id

        query_id=cur.sfqid

        # Collect rejected records and insert into rejectd records table
        try :

            collect_rejects=f"""insert into {database}.{schema}.copy_cmd_rejects    
            select 'snowpython' job_name, '{query_id}' QUERY_ID ,'{tablename}' TABLE_NAME, CURRENT_TIMESTAMP() LOAD_DATE,
            A.* from table(validate({database}.{schema}.{tablename},job_id =>'{query_id}')) A;"""

            cur.execute(collect_rejects)
        except connector.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

        # Get reject record counts
        try:

            rej_records=f"""select count(*) from {database}.{schema}.copy_cmd_rejects where QUERY_ID = '{query_id}'"""
            
            cur.execute(rej_records)
        except connector.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))

        # store reject record count in variable
        rej_rec_cnt=cur.rowcount

        # Get query history.

        audit_copy=f"""
        INSERT INTO {database}.{schema}.COPY_AUDIT
        SELECT 
        'snowpython' JOB_NAME,
        'copy_audit' TASK_NAME,
        QUERY_ID,
        QUERY_TEXT,
        DATABASE_NAME,
        ROWS_PRODUCED,
        '{rej_rec_cnt}' ROWS_REJECTED,
        SCHEMA_NAME,
        ROLE_NAME,
        WAREHOUSE_NAME,
        WAREHOUSE_SIZE,
        EXECUTION_STATUS,
        ERROR_MESSAGE,
        EXECUTION_TIME,

        CASE 

        WHEN WAREHOUSE_SIZE='X-Small' THEN ((((EXECUTION_TIME/1000)/60)+1.5)*(CLUSTER_NUMBER*1))/60 

        WHEN WAREHOUSE_SIZE='Small' THEN ((((EXECUTION_TIME/1000)/60)+1.5)*(CLUSTER_NUMBER*2))/60 

        WHEN WAREHOUSE_SIZE='Medium' THEN ((((EXECUTION_TIME/1000)/60)+1.5)*(CLUSTER_NUMBER*4))/60 

        WHEN WAREHOUSE_SIZE='Large' THEN ((((EXECUTION_TIME/1000)/60)+1.5)*(CLUSTER_NUMBER*8))/60 

        WHEN WAREHOUSE_SIZE='X-Large' THEN ((((EXECUTION_TIME/1000)/60)+1.5)*(CLUSTER_NUMBER*16))/60   

        WHEN WAREHOUSE_SIZE='2X-Large' THEN ((((EXECUTION_TIME/1000)/60)+1.5)*(CLUSTER_NUMBER*32))/60

        WHEN WAREHOUSE_SIZE='3X-Large' THEN ((((EXECUTION_TIME/1000)/60)+1.5)*(CLUSTER_NUMBER*64))/60 

        WHEN WAREHOUSE_SIZE='4X-Large' THEN ((((EXECUTION_TIME/1000)/60)+1.5)*(CLUSTER_NUMBER*128))/60 

        END WAREHOUSE_COST,

        current_timestamp() ETL_TS

        FROM table(information_schema.query_history())
        WHERE QUERY_TYPE='COPY' AND QUERY_ID='{query_id}'"""


        try:

            cur.execute(audit_copy)

        except connector.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        
    cur.close()

#execute_copy_cmd()