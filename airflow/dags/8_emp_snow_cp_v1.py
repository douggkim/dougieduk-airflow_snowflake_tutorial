"""
Section : Create generic components.
Lecture : Schedule using airflow.

We have build generic modules. We will check our airflow job.

https://www.udemy.com/course/snowflake-cloud-database-with-airflow-python-talend/learn/lecture/30448540#overview

#airflow dags show emp_snow_cp_v1 --save test.png

Pylint score : 4.29
"""

import sys
import airflow
from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG

sys.path.append('/opt/airflow/python/my_module/')
import Generic_commands


# Declare the variables
JOB_NAME = 'emp_snow_cp_v1'
TASK1 = 'copy_emp_snow_cp_v1'
TASK2 = 'elt_emp_snow_cp_v1'

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
dag = DAG(JOB_NAME, default_args=default_args, schedule_interval=None)

# t1, t2, t3, etc are tasks created by instantiating operators
t1 = BashOperator(
    task_id=TASK1,
    bash_command=Generic_commands.copy_command(JOB_NAME, TASK1),
    dag=dag)


t2 = BashOperator(
    task_id=TASK2,
    bash_command=Generic_commands.elt_command(JOB_NAME, TASK2),
    dag=dag)


t1>>t2
