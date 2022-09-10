"""
Section : Talend introduction.
Lecture : Build sample job : Schedule using airflow.

We will see how to scedule Talend job using airflow.

https://www.udemy.com/course/snowflake-cloud-database-with-airflow-python-talend/learn/lecture/30448540#overview

#airflow dags show emp_load_to_snw --save test.png

Pylint score : -4 ( intetionally it's not formated to keep things descriptive)
"""


import airflow
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.utils.email import send_email
from zipfile import ZipFile

# Declare the variables
airflow_home='/opt/airflow/talendjobs/extracts'
job_name='emp_load_to_snw'
job_version='0.1'

# these args will get passed on to each operator
# you can override them on a per-task basis during operator initialization

default_args = {
    'owner': 'snowadmin',
    # 'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    # 'email': ['VALID_EMAIL_ID'],
    # 'email_on_failure': True,
    # 'email_on_success': True,
      'retries':1,

}

def uzip_func(ds, **kwargs):
        print("Performing unzip operation")
        zf = ZipFile(airflow_home+"/"+job_name+"_"+job_version+".zip", 'r')
        zf.extractall(airflow_home+"/"+job_name+"_"+job_version+"/")
        zf.close()

# Shell script location for the respective bash command
talend_job = airflow_home+"/"+job_name+"_"+job_version+"/"+job_name+"/"+job_name+"_run.sh --context_param connection=/opt/airflow/talendjobs/parameters/snowflake_conn1.csv" 


# DAG for airflow task
dag = DAG(job_name, default_args=default_args,schedule_interval=None, description='Section : Talend introduction. Lecture : Build sample job : Schedule using airflow. ')

# t1, t2, t3, etc are tasks created by instantiating operators

un_zip = PythonOperator(
    task_id='uzip_jobs',
    provide_context=True,
    python_callable=uzip_func,
    dag=dag,
)
	
t1 = BashOperator(
    task_id='load_file_to_snowflake',
    bash_command=talend_job,
	params={'job_name':job_name,'job_version':job_version},
    dag=dag)

un_zip>>t1

