import airflow
from datetime import datetime
from datetime import timedelta
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import DAG
from airflow.utils.email import send_email
import sys
#sys.path.append('/opt/airflow/python/my_module/')
sys.path.append('/opt/airflow/python/')
from my_module import Generic_commands as generic
from my_module import Generate_param_files as param


from snowflake import audit_source_files as audit
from snowflake import archive_s3_files as archive

def execute_cp_command(job_name,task_name,dag):	
    t1 = BashOperator(
	    task_id=task_name,
	    bash_command=generic.copy_command(job_name,task_name),
	    dag=dag)
    return t1

def execute_elt_query(job_name,task_name,dag):	
    t2 = BashOperator(
    task_id=task_name,
    bash_command=generic.elt_command(job_name,task_name),
    dag=dag)
    return t2


def execute_source_audit(job_name,task_name,dag):	
    t3 = PythonOperator(
    task_id=task_name,
    #provide_context=True,
    #audit_source_files.audit_source_data(job_name)
    python_callable=audit.audit_source_data,
    op_kwargs = {"job_name" : job_name},
    dag=dag

    )
    return t3


def execute_archive(job_name,task_name,dag):	
    t4 = PythonOperator(
    task_id=task_name,
    #provide_context=True,
    #archive_s3_files.archive_s3_crt_ext_tbl(job_name),
    python_callable=archive.archive_s3_crt_ext_tbl,
    op_kwargs = {"job_name" : job_name},
    dag=dag
    )
    return t4


def generate_parm_files(job_name,task_name,dag):	
     t5 = PythonOperator(
     task_id=task_name,
     python_callable=param.generate_param_files,
     op_kwargs = {"job_name" : job_name},
     dag=dag
     )
     return t5


