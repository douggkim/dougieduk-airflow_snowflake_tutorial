"""
Section : Nyc traffic data demo.
Lecture : Load sample data.

We will load sample Nyc data. Before doing full load.

https://www.udemy.com/course/snowflake-cloud-database-with-airflow-python-talend/learn/lecture/31101810#overview

#airflow dags show Nyc_data_processing_smp_load --save test.png

Pylint score : 5.24
"""

import sys
import airflow
#from datetime import datetime
#from datetime import timedelta
#from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy import DummyOperator
#from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
#from airflow.utils.email import send_email

sys.path.append('/opt/airflow/python/my_module/')
import Task_builder


# Declare the variables

JOB_NAME = 'Nyc_data_processing_smp_load' 
TASK1 = 'copy_nyc_taxi_zones_load'
TASK2 = 'copy_nyc_weather_data_load'
TASK3 = 'copy_nyc_yellow_taxi_load'
TASK4 = 'elt_nyc_process_data'
TASK5 = 'generate_prm_fls'                                                 
# these args will get passedstageobject on to each operator
# you can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'snowadmin',
    # 'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    # 'email': ['VALID_EMAIL_ID'],
    # 'email_on_failure': True,
    # 'email_on_success': True,
    #  'retries':1
}

# DAG for airflow task
dag = DAG(JOB_NAME, default_args=default_args, schedule_interval=None)

# Link tasks
#continue_op_1 = DummyOperator(task_id='continue_task_step1', dag=dag)

start = DummyOperator(task_id='Start', dag=dag)

gn_param = Task_builder.generate_parm_files(JOB_NAME, TASK5, dag)

Taxi_zones = Task_builder.execute_cp_command(JOB_NAME, TASK1, dag)

Load_weather_data = Task_builder.execute_cp_command(JOB_NAME, TASK2, dag)

load_taxi_data = Task_builder.execute_cp_command(JOB_NAME, TASK3, dag)

process_data = Task_builder.execute_elt_query(JOB_NAME, TASK4, dag)

start>>gn_param>>Taxi_zones>>Load_weather_data>>load_taxi_data>>process_data
