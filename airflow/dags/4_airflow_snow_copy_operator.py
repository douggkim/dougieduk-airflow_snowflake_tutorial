"""
Section : Snowflake airflow ELT.
Lecture : Load to snowflake example 3.
This job will copy data to snowflake and process data in snowflake.
https://www.udemy.com/course/snowflake-cloud-database-with-airflow-python-talend/learn/lecture/32294714#overview

#airflow dags show airflow_copy_frm_s3_snow --save test.png

Pylint score : 2.67
"""


from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeOperator
)
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.dates import datetime

SNOWFLAKE_CONN_ID = "snowflake_default"
with DAG('airflow_copy_frm_s3_snow',
         description='Example DAG showcasing loading data',
         start_date=datetime(2021, 1, 1),
         schedule_interval=None,
         template_searchpath="/opt/airflow/sql/",
         catchup=False) as dag:
         
    """
    #### copy data from s3
    """
    copy_data_frm_s3 = S3ToSnowflakeOperator(
    task_id='copy_into_table',    
    s3_keys=['emp_vendor1/employees01.csv'],
    table='EMPLOYEE_V1',
    schema='PUBLIC',
    stage='MY_S3_STAGE',
    file_format="(type = 'CSV',field_delimiter = ',')",
    dag=dag
)
    """
    #### Process emp data
    """
    process_data = SnowflakeOperator(
        task_id="process_emp",
        sql="{% include 'emp_process.sql' %}",
        params={"table_name": 'EMPLOYEE'}
    )
    begin = DummyOperator(task_id='begin')
    end = DummyOperator(task_id='end')
    chain(
        begin,
        copy_data_frm_s3,
        process_data,
        end
    )
