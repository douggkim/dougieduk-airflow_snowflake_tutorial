# Declare the variables
talendjob_path = '/opt/airflow/talendjobs'
parameters_path = '/opt/airflow/talendjobs/parameters'

def copy_command(job_name,task_id) :
    
    talend_copy_job = f"java -jar {talendjob_path}/s3snowcp.jar {parameters_path}/snowflake_conn1.csv {parameters_path}/{job_name}/{task_id}_param.csv  {job_name} {task_id}" 

    return talend_copy_job


def elt_command(job_name,task_id) :
    
    talend_elt_job = f"java -jar {talendjob_path}/eltsnowjob.jar {parameters_path}/snowflake_conn1.csv {parameters_path}/{job_name}/{task_id}_param.csv {job_name} {task_id}"

    return talend_elt_job



