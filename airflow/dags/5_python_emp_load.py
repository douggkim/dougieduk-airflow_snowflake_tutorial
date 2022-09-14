"""
Section : Snowflake python ELT.
Lecture : Schedule airflow job : For python code.

This job will copy data to snowflake and process data in snowflake. 
We will not use airflow operators. 
But we will go with pur python code.

https://www.udemy.com/course/snowflake-cloud-database-with-airflow-python-talend/learn/lecture/31833480#overview

#airflow dags show python_emp_load --save test.png

Pylint score : 2.67
"""


import sys
import functools
import airflow
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG

sys.path.append('/opt/airflow/snowpythconn/apps')
import copycmd_gen_parallel
import query_execution_parallel


# Declare the variables
JOB_NAME = 'python_emp_load'
TASK1 = 'copy_data'
TASK2 = 'process_data'

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'snowadmin',
    # 'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    # 'email': ['VALID_EMAIL_ID'],
    # 'email_on_failure': True,
    # 'email_on_success': True,
    'retries':1

}

# DAG for airflow task
dag = DAG(JOB_NAME, default_args=default_args, schedule_interval='@daily', 
description='Section : Snowflake python ELT. Lecture : Schedule airflow job : For python code.')

# t1, t2, t3, etc are tasks created by instantiating operators

t1 = PythonOperator(
task_id=TASK1,
#python_callable=copycmd_gen_parallel.execute_copy_cmd(),
python_callable=functools.partial(copycmd_gen_parallel.execute_copy_cmd),
#provide_context=False,
#op_kwargs = {"JOB_NAME" : JOB_NAME},
dag=dag
)

t2 = PythonOperator(
task_id=TASK2,
python_callable=functools.partial(query_execution_parallel.execute_elt_queries),
#op_kwargs = {"JOB_NAME" : JOB_NAME},
dag=dag
)

t1>>t2
