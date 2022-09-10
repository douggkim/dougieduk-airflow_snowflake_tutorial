"""
Section : Snowflake airflow ELT.
Lecture : Load to snowflake example 1.
This job will create tables and insert data into snowflake table
https://www.udemy.com/course/snowflake-cloud-database-with-airflow-python-talend/learn/lecture/32294714#overview

#airflow dags show airflow_snowflake --save test.png

Pylint score : 5
"""

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeOperator
)
from airflow.utils.dates import datetime


SNOWFLAKE_TEST_TABLE = 'TEST_TABLE'

SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG('airflow_snowflake',
         description='Section : Snowflake airflow ELT. Lecture : Load to snowflake example 1',
         start_date=datetime(2021, 1, 1),
         schedule_interval=None,
         template_searchpath="/opt/airflow/sql/",
         catchup=False) as dag:

    """
    #### Snowflake table creation
    Create the table to store sample forest fire data.
    """
    create_table = SnowflakeOperator(
        task_id="create_table",
        sql="{% include 'create_snowflake_table.sql' %}",
        params={"table_name": SNOWFLAKE_TEST_TABLE}
    )

    """
    #### Insert data
    Insert data into the Snowflake table using an existing SQL query (stored in
    the include/sql/snowflake_examples/ directory).
    """
    load_data = SnowflakeOperator(
        task_id="insert_query",
        sql="{% include 'load_sample_data.sql' %}",
        params={"table_name": SNOWFLAKE_TEST_TABLE}
    )

    """
    #### Delete table
    Clean up the table created for the example.
    """
    delete_table = SnowflakeOperator(
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
