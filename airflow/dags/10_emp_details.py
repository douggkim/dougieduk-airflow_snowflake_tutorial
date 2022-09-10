"""
Section : Building workflow.
Lecture : Preparing dag code.

We will see how to build end to end workflow using Airflow.

https://www.udemy.com/course/snowflake-cloud-database-with-airflow-python-talend/learn/lecture/31097384#overview

#airflow dags show emp_details --save test.png

Pylint score : 5.24
"""

import sys
import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
sys.path.append('/opt/airflow/python/my_module/')
import Task_builder


# Declare the variables
JOB_NAME = 'emp_details'

TASK6 = 'generate_prm_fls'
TASK0 = 'src_audit'
TASK1 = 'copy_emp_details_load'
TASK2 = 'elt_parse_emp_details' 
TASK3 = 'elt_merge_emp_details'
TASK4 = 'archive_create_ext_views'
TASK5 = 'elt_emp_details_scoring'

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'snowadmin',
    # 'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    # 'email': ['VALID_EMAIL_ID'],
    # 'email_on_failure': True,
    # 'email_on_success': True,
    'retries':0

}

# DAG for airflow task
dag = DAG(JOB_NAME, default_args=default_args, schedule_interval=None, 
description='Section : Building workflow. Lecture : Preparing dag code.')

# t1, t2, t3, etc are tasks created by instantiating operators

op1 = DummyOperator(task_id='Start', dag=dag)

t6 = Task_builder.generate_parm_files(JOB_NAME, TASK6, dag)

t0 = Task_builder.execute_source_audit(JOB_NAME, TASK0, dag)

t1 = Task_builder.execute_cp_command(JOB_NAME, TASK1, dag)

t2 = Task_builder.execute_elt_query(JOB_NAME, TASK2, dag)

t3 = Task_builder.execute_elt_query(JOB_NAME, TASK3, dag)

t4 = Task_builder.execute_archive(JOB_NAME, TASK4, dag)

t5 = Task_builder.execute_elt_query(JOB_NAME, TASK5, dag)

op1>>t6>>t0>>t1>>t2>>t3>>t4>>t5
