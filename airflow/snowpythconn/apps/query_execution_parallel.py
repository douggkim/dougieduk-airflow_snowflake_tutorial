import csv
import glob
import sys
import time
from marshmallow_sqlalchemy import SQLAlchemyAutoSchemaOpts
import pandas as pd
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector




def pd_read_pattern(pattern):
    files = glob.glob(pattern)

    df = pd.DataFrame()
    for f in files:
        df = df.append(pd.read_csv(f))

    return df.reset_index(drop=True)

def execute_elt_queries():

    cur = snowcnt.connect_snowflake()
    conn = snowcnt.return_conn_obj()
    cur.execute('USE DATABASE DEMO_DB')
    cur_list=[]
    i=0

    csvreader=pd.read_csv('/opt/airflow/snowpythconn/Sql_files/emp_process.sql',
    engine='python',delimiter='\n',header=None,sep=';')

    for index,row in csvreader.iterrows():
        print(row[0])
        cur.execute_async(row[0])
        query_id = cur.sfqid
        cur_list.append(query_id)

    get_status(cur_list)
    audit_queries(cur_list,cur)


def get_status(cur_list):
    conn = snowcnt.return_conn_obj()
    status=[]
    df = pd.DataFrame(columns=['Query_id','Status'])
    arr=cur_list
    for query_id in cur_list:
        status.append(conn.get_query_status(query_id).name)
        #data = [[query_id, conn.get_query_status(query_id).name]]
        df=df.append({'Query_id':query_id,'Status':conn.get_query_status(query_id).name},ignore_index=True)
    
    if status.count('RUNNING')>1:
        
        del status[:]
        print(df)
        print("\nsql commands are still running\n")
        time.sleep(10)
        get_status(arr)

    else:
        print("\nAll sql commands execution done!!!\n")
        print(df)

    return

def audit_queries(cur_list,cur):

    for query_id in cur_list:

        query_audit = f"""
        INSERT INTO DEMO_DB.PUBLIC.ELT_QUERIES_AUDIT
        SELECT 

        'snowpython' AIRFLOW_JOB_NAME , 
        'snowpythontask' AIRFLOW_TASK_NAME,

        QUERY_ID,
        QUERY_TEXT,
        DATABASE_NAME,
        ROWS_PRODUCED,

        SCHEMA_NAME,
        ROLE_NAME,
        WAREHOUSE_NAME,
        WAREHOUSE_SIZE,
        EXECUTION_STATUS,
        ERROR_MESSAGE,
        EXECUTION_TIME,

        QUEUED_PROVISIONING_TIME,
        QUEUED_OVERLOAD_TIME,
        TRANSACTION_BLOCKED_TIME,
        BYTES_SCANNED,

        CASE 

        WHEN WAREHOUSE_SIZE='X-Small' THEN ((((EXECUTION_TIME/1000)/60)+0.015)*(CLUSTER_NUMBER*1))/60 

        WHEN WAREHOUSE_SIZE='Small' THEN ((((EXECUTION_TIME/1000)/60)+0.015)*(CLUSTER_NUMBER*2))/60 

        WHEN WAREHOUSE_SIZE='Medium' THEN ((((EXECUTION_TIME/1000)/60)+0.015)*(CLUSTER_NUMBER*4))/60 

        WHEN WAREHOUSE_SIZE='Large' THEN ((((EXECUTION_TIME/1000)/60)+0.015)*(CLUSTER_NUMBER*8))/60 

        WHEN WAREHOUSE_SIZE='X-Large' THEN ((((EXECUTION_TIME/1000)/60)+0.015)*(CLUSTER_NUMBER*16))/60   

        WHEN WAREHOUSE_SIZE='2X-Large' THEN ((((EXECUTION_TIME/1000)/60)+0.015)*(CLUSTER_NUMBER*32))/60

        WHEN WAREHOUSE_SIZE='3X-Large' THEN ((((EXECUTION_TIME/1000)/60)+0.015)*(CLUSTER_NUMBER*64))/60 

        WHEN WAREHOUSE_SIZE='4X-Large' THEN ((((EXECUTION_TIME/1000)/60)+0.015)*(CLUSTER_NUMBER*128))/60 

        END WAREHOUSE_COST ,

        CURRENT_TIMESTAMP() ETL_TS

        FROM table(information_schema.query_history())
        WHERE query_id='{query_id}'
        """ 
    
    cur.execute(query_audit)


#execute_elt_queries()



