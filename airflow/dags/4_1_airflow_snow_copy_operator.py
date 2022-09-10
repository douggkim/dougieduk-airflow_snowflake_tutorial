from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.snowflake.operators.snowflake import (
    SnowflakeOperator
)
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.utils.dates import datetime

SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG('airflow_copy_frm_s3_snow_by_doug',
    description='airflow_copy_frm_s3_snow_by_doug',
    start_date=datetime(2022,9,8),
    schedule_interval='@hourly',
    template_searchpath='/opt/airflow/sql/',
    catchup=False) as dag: 

    copy_data_frm_s3 = S3ToSnowflakeOperator(
        task_id='copy_into_table',
        s3_keys=['emp_vendor1/employees01.csv'],
        table="EMPLOYEE_V1",
        schema="PUBLIC",
        stage='MY_S3_STAGE',
        file_format="(type='CSV', field_delimiter=',')",
        dag=dag
    )

    process_data = SnowflakeOperator(
        task_id='process_emp',
        sql="{% include 'emp_process.sql' %}",
        params={"table_name": "EMPLOYEE"}
    )

    begin=DummyOperator(task_id="Begin")
    end=DummyOperator(task_id="End")

    chain(
        begin,
        copy_data_frm_s3,
        process_data,
        end
    )

