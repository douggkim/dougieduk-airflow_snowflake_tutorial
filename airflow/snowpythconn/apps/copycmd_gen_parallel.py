import glob
from array import array
import sys
from snowflake import connector

import time
import pandas as pd
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt



def pd_read_pattern(pattern):
    files = glob.glob(pattern)

    df = pd.DataFrame()
    for f in files:
        df = df.append(pd.read_csv(f))

    return df.reset_index(drop=True)


def execute_copy_cmd():
    
    # Read parameter files with copy commands
    cur = snowcnt.connect_snowflake()
    conn = snowcnt.return_conn_obj()

    cur_list = []
    collect_rej = {}
    i = 0

    df = pd_read_pattern(
        '/opt/airflow/snowpythconn/apps/parameter/copy_emp_snow_cp_v1_param.csv')

    warehouse = df['WAREHOUSE'].unique()
    database = df['DATABASE'].unique()
    schema = df['SCHEMA'].unique()

    cur.execute(f"""USE WAREHOUSE {warehouse[0]}""")
    cur.execute(f"""USE DATABASE {database[0]}""")
    cur.execute(f"""USE SCHEMA {schema[0]}""")
    cur.execute("USE ROLE ACCOUNTADMIN")

    for index, row in df.iterrows():

        stage_object = row['STAGE_OBJECT']
        folder_path = row['S3_FILE_PATH']
        file_format = row['FILE_FORMAT']

        database = row['DATABASE']
        schema = row['SCHEMA']
        tablename = row['TABLE_NAME']
        pattern = row['PATTERN']

        try:

            copy_command = f"""copy into {database}.{schema}.{tablename} from @{stage_object}{folder_path}
            FILE_FORMAT = {file_format} PATTERN = '{pattern}' ON_ERROR=CONTINUE"""

            cur.execute_async(copy_command)

        except connector.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(
                e.errno, e.sqlstate, e.msg, e.sfqid))

        query_id = cur.sfqid

        cur_list.append(query_id)

        collect_rej[i] = query_id, database, schema, tablename
        i = i + 1

    get_status(cur_list)
    collect_rejects(collect_rej, cur)


def get_status(cur_list):
    conn = snowcnt.return_conn_obj()
    status = []
    df = pd.DataFrame(columns=['Query_id', 'Status'])
    arr = cur_list
    for query_id in cur_list:
        status.append(conn.get_query_status(query_id).name)
        #data = [[query_id, conn.get_query_status(query_id).name]]
        df = df.append({'Query_id': query_id,
                        'Status': conn.get_query_status(query_id).name},
                       ignore_index=True)

    if status.count('RUNNING') > 1:

        del status[:]
        print(df)
        print("\ncopy commands are still running\n")
        time.sleep(10)
        get_status(arr)

    else:
        print("\nAll copy commands execution done!!!\n")
        print(df)

    return


def collect_rejects(collect_rej, cur):

    print("\n started collecting rejectd records if any !!!\n")
    for key in collect_rej:
        # Collect rejected records an insert into rejectd records table
        try:

            collect_rejects = f"""insert into {collect_rej[key][1]}.{collect_rej[key][2]}.copy_cmd_rejects
            select 'snowpython' job_name, '{collect_rej[key][0]}' QUERY_ID ,'{collect_rej[key][3]}' TABLE_NAME, CURRENT_TIMESTAMP() LOAD_DATE,
            A.* from table(validate({collect_rej[key][1]}.{collect_rej[key][2]}.{collect_rej[key][3]},job_id =>'{collect_rej[key][0]}')) A;"""

            cur.execute(collect_rejects)

        except connector.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(
                e.errno, e.sqlstate, e.msg, e.sfqid))
        # Get reject record counts
        try:

            rej_records = f"""select count(*) from {collect_rej[key][1]}.{collect_rej[key][2]}.copy_cmd_rejects where QUERY_ID = '{collect_rej[key][0]}'"""

            cur.execute(rej_records)
        except connector.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(
                e.errno, e.sqlstate, e.msg, e.sfqid))

        # store reject record count in variable
        rej_rec_cnt = cur.rowcount

        # Get query history.
        print(
            f"""\n started auditing copy command for query Id,{collect_rej[key][0]}\n""")

        audit_copy = f"""
        INSERT INTO DEMO_DB.PUBLIC.COPY_AUDIT
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
        WHERE QUERY_TYPE='COPY' AND QUERY_ID='{collect_rej[key][0]}'"""

        try:

            cur.execute(audit_copy)

        except connector.errors.ProgrammingError as e:
            # default error message
            print(e)
            # customer error message
            print('Error {0} ({1}): {2} ({3})'.format(
                e.errno, e.sqlstate, e.msg, e.sfqid))

    cur.close()

execute_copy_cmd()