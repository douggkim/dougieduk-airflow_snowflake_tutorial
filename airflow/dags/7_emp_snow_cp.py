"""
Section : Create generic components.
Lecture : Schedule using airflow.

We have build generic modules. We will check our airflow job.

https://www.udemy.com/course/snowflake-cloud-database-with-airflow-python-talend/learn/lecture/30448540#overview

#airflow dags show emp_snow_cp --save test.png

Pylint score : 6.92
"""

import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG

# Declare the variables
JOB_NAME = 'emp_snow_cp'
TALENDJOB_PATH = '/opt/airflow/talendjobs'
PARAMETERS_PATH = '/opt/airflow/talendjobs/parameters'

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

# Shell script location for the respective bash command
talend_copy_job = f"java -jar {TALENDJOB_PATH}/s3snowcp.jar {PARAMETERS_PATH}/snowflake_conn1.csv {PARAMETERS_PATH}/job_parameters.csv"

talend_elt_job = f"java -jar {TALENDJOB_PATH}/eltsnowjob.jar {PARAMETERS_PATH}/snowflake_conn1.csv elt_job_parameters.csv"


# DAG for airflow task
dag = DAG(JOB_NAME, default_args=default_args, schedule_interval=None, 
description='Section : Create generic components. Lecture : Schedule using airflow.')

# t1, t2, t3, etc are tasks created by instantiating operators
t1 = BashOperator(
    task_id='copy_file_to_snowflake',
    bash_command=talend_copy_job,
    dag=dag)


t2 = BashOperator(
    task_id='process_emp_data',
    bash_command=talend_elt_job,
    dag=dag)


t1>>t2
