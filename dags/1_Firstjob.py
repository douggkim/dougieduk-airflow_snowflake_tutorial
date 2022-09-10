"""
Section : Airflow intro.
Lecture : First DAG.
This job will introduced you to basics of airflow DAG.
https://www.udemy.com/course/snowflake-cloud-database-with-airflow-python-talend/learn/lecture/32122764#overview

#airflow dags show First_job --save test.png

Pylint score : 5
"""


from datetime import datetime
import airflow
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'snowadmin',
    # 'depends_on_past': False,
    #'start_date': airflow.utils.dates.days_ago(2),
    'start_date': datetime(2022, 5, 2),
    # 'email': ['VALID_EMAIL_ID'],
    # 'email_on_failure': True, - email when success/ failure 
    # 'email_on_success': True,
    'retries':1 
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'sla': timedelta(hours=2), - 
    # 'execution_timeout': timedelta(seconds=300), - 
    # 'on_failure_callback': some_function, - 
    # 'on_success_callback': some_other_function, - 
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success' - all the tasks are successful 
}
def call_my_name(name):	
    print("Hi my name is :"+name)

# DAG for airflow task
#schedule_interval : how often is this going to be run (12:00 am ) <-> None 
dag = DAG('First_job', default_args=default_args, schedule_interval="@daily", 
description='Section : Airflow intro. Lecture : First DAG')

#show which tag the task belongs to 
t1 = DummyOperator(task_id='Start', dag=dag)


t2 = PythonOperator(
    task_id='call_my_name',
    python_callable=call_my_name,
    op_kwargs={"name" : "Python"},
    dag=dag
    )

t3 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5', #time.sleep(300),
    dag=dag)

#order of the tasks to be executed
t1>>t2>>t3


