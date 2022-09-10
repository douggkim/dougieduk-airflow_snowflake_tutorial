from airflow import DAG 
from airflow.models.baseoperator import chain 
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeOperator
)
from airflow.utils.dates import datetime 

SNOWFLAKE_TEST_TABLE = "TEST_TABLE"
SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG('airflow_snowflake_written_by_doug', description= 'Section : Snowflake airflow ELT. Rewriting the script', start_date=datetime(2022,9,8), schedule_interval="@hourly", template_searchpath="/opt/airflow/sql", catchup=False) as dag: 
    create_table = SnowflakeOperator(
        task_id="create_table",
        sql="{% include 'create_snowflake_table.sql' %}",
        params={"table_name": SNOWFLAKE_TEST_TABLE} 
    )

    load_data = SnowflakeOperator(
        task_id="insert_query",
        sql="{% include 'load_sample_data.sql' %}",
        params = {"table_name": SNOWFLAKE_TEST_TABLE}
    )

    delete_table=SnowflakeOperator(
        task_id="delete_table",
        sql="{% include 'delete_snowflake_table.sql' %}",
        params={"table_name": SNOWFLAKE_TEST_TABLE}
    )

    begin = DummyOperator(task_id='begin')
    end = DummyOperator(task_id='end')

    chain(
        begin,
        create_table,
        load_data,
        delete_table,
        end
    )




