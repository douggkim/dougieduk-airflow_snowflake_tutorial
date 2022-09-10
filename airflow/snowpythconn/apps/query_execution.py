import sys
from marshmallow_sqlalchemy import SQLAlchemyAutoSchemaOpts
sys.path.append('/opt/airflow/snowpythconn/')
from creds import make_snowflake_connection as snowcnt
from snowflake import connector


cur = snowcnt.connect_snowflake()
conn = snowcnt.return_conn_obj()


cur_list=[]

with open('./snowpythconn/Sql_files/test_query.sql', 'r', encoding='utf-8') as f:
    for cur in conn.execute_stream(f,remove_comments=True):
        cur_list.append(cur.sfqid)
        for ret in cur:
            print(ret[0])

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






