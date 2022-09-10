import logging
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeOperator
)
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils.dates import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SNOWFLAKE_TEST_TABLE = 'TEST_TABLE'

SNOWFLAKE_CONN_ID = "snowflake_default"

def get_row_count(**context): 
    dwh_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    result = dwh_hook.get_first("select count(*) from public.test_table")
    logging.info("Number of rows - %s", result[0])

with DAG('airflow_snowflake_hook_operator_by_doug',
    description='airflow_snowflake_hook_operator_by_doug',
    start_date=datetime(2022,9,8),
    schedule_interval='@hourly',
    template_searchpath='/opt/airflow/sql/',
    catchup=False) as dag: 

    create_table = SnowflakeOperator(
        task_id="create_table",
        sql="{% include 'create_snowflake_table.sql' %}",
        params={"table_name": SNOWFLAKE_TEST_TABLE}
    )

    load_data = SnowflakeOperator(
        task_id="insert_query",
        sql="{% include 'load_sample_data.sql'%}",
        params={"table_name":SNOWFLAKE_TEST_TABLE}
    )

    get_count = PythonOperator(
        task_id="get_count",
        python_callable=get_row_count
    )


    delete_table = SnowflakeOperator(
        task_id="delete_table",
        sql="{% include 'delete_snowflake_table.sql' %}",
        params={"table_name":SNOWFLAKE_TEST_TABLE}
    )

    begin=DummyOperator(task_id='begin')
    end=DummyOperator(task_id='end')

    chain(
        begin,
        create_table,
        load_data,
        get_count,
        delete_table,
        end
    )

