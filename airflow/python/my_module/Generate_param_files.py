import pandas as pd
import os

parameters_path = '/opt/airflow/talendjobs/parameters/'
copy_df = pd.read_csv('/opt/airflow/talendjobs/job_register/copy_task_register.csv')
elt_df = pd.read_csv('/opt/airflow/talendjobs/job_register/elt_task_register.csv')

#Filter copy_df by job name

def create_copy_param_files(job_name):
    job_records = copy_df[copy_df.JOB_NAME==job_name]
    job_name = job_records['JOB_NAME'].unique()

    os.makedirs(parameters_path+job_name[0], exist_ok=True)

#Filter by task name. Group by task name. 
    task_name = job_records['TASK_NAME'].unique()
# Create param files for each task
    for i in task_name:
        task=job_records[(job_records.TASK_NAME==i) & (job_records.JOB_NAME==job_name[0])]
        param_file = task[['DATABASE','SCHEMA','TABLE_NAME','S3_BUCKET','STAGE_OBJECT','S3_FILE_PATH','WAREHOUSE','FILE_FORMAT','PATTERN','REJECTS_THRESHOLD','DATA_SOURCE']]
        param_file = param_file.astype({"REJECTS_THRESHOLD":int})
        param_file.to_csv(parameters_path+job_name[0]+'/'+i+'_param.csv', header=True, index=None, sep=',', mode='w')

#create_copy_param_files('emp_details')


def create_elt_param_files(job_name):
    job_records = elt_df[elt_df.JOB_NAME==job_name]
    job_name = job_records['JOB_NAME'].unique()

    os.makedirs(parameters_path+job_name[0], exist_ok=True)

#Filter by task name. Group by task name. 
    task_name = job_records['TASK_NAME'].unique()
# Create param files for each task
    for i in task_name:
        task=job_records[(job_records.TASK_NAME==i) & (job_records.JOB_NAME==job_name[0])]
        param_file = task[['DATABASE','SCHEMA','CONTROL_TABLE','SQL_FILE','LOAD_FLG','EXECUTE_FLG']]
        param_file = param_file.astype({"LOAD_FLG":int,"EXECUTE_FLG":int})
        param_file.to_csv(parameters_path+job_name[0]+'/'+i+'_param.csv', header=True, index=None, sep=',', mode='w')


#create_elt_param_files('emp_details')

def generate_param_files(job_name):
    create_elt_param_files(job_name)
    create_copy_param_files(job_name)


#generate_param_files('Nyc_data_processing_smp_load')
