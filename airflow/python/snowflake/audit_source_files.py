import snowflake.connector
import sys
sys.path.append('/opt/airflow/python/snowflake')
from connection import make_snowflake_connection as snowcnt
from connection import read_hardcoded_paths as paths
#import connection.read_hardcoded_paths as paths
import glob
import pandas as pd

#import snowflake.connection.make_snowflake_connection as snowcnt


# params = paths.paths()

# cur= snowcnt.connect_snowflake()
# cur.execute("USE DATABASE DEMO_DB")
# cur.execute("USE ROLE SYSADMIN")

# Read parameter files with copy commands

def pd_read_pattern(pattern):
    files = glob.glob(pattern)

    df = pd.DataFrame()
    for f in files:
        df = df.append(pd.read_csv(f))

    return df.reset_index(drop=True)

def audit_source_data(job_name):

    params = paths.paths()

    cur= snowcnt.connect_snowflake()
    
    df = pd_read_pattern(params["parameters"]+job_name+'/copy*.csv')

    warehouse = df['WAREHOUSE'].unique()
    database  = df['DATABASE'].unique()
    schema    = df['SCHEMA'].unique()

    cur.execute(f"""USE WAREHOUSE {warehouse[0]}""")
    cur.execute(f"""USE DATABASE {database[0]}""")
    cur.execute(f"""USE SCHEMA {schema[0]}""")
    cur.execute("USE ROLE ACCOUNTADMIN")

    for index, row in df.iterrows():
        
        data_source=row['DATA_SOURCE']
        stage_object=row['STAGE_OBJECT']
        folder_path=row['S3_FILE_PATH']
        file_format=row['FILE_FORMAT']

        sql_statement=f"""insert into demo_db.public.source_file_audit select 
        distinct '{job_name}' job_name, split_part(metadata$filename ,'/',2) File_name,count(*) Rec_cnt,current_timestamp() processed_date , '{data_source}' DATA_SOURCE
        from '@{stage_object}{folder_path}' 
        (FILE_FORMAT=>'{file_format}' , PATTERN=>'.*') t
        group by split_part(metadata$filename ,'/',2)"""
        
        cur.execute(sql_statement)

    cur.close()

#audit_source_data('emp_snow_cp_v1')
