import glob
import boto3
import pandas as pd

from datetime import datetime
import sys
sys.path.append('/opt/airflow/python/snowflake')
#from connection import make_snowflake_connection as snowcnt
from connection import read_hardcoded_paths as paths
from connection import read_hardcoded_paths as creds

# read hardcoded paths from connection folder
#import connection.read_hardcoded_paths as paths
#import connection.read_hardcoded_paths as creds

import snowflake.connector
# read snowflake credentials from connection folder
#import connection.make_snowflake_connection as snowcnt
from connection import make_snowflake_connection as snowcnt



# Function to read files.
def pd_read_pattern(pattern):
    files = glob.glob(pattern)

    df = pd.DataFrame()
    for f in files:
        df = df.append(pd.read_csv(f))

    return df.reset_index(drop=True)

# Function to archive files and create external tables
def archive_s3_crt_ext_tbl(job_name):
    today = datetime.now()
    folder_date=today.strftime('%Y%m%d')

    # Initialize connection and import paths
    params = paths.paths()
    cur= snowcnt.connect_snowflake()
    cred = creds.aws_creds()
    
    #Creating Session With Boto3.
    session = boto3.Session(
    aws_access_key_id=cred["aws_access_key_id"],
    aws_secret_access_key=cred["aws_secret_access_key"]
    )

    #Creating S3 Resource From the Session.
    s3 = session.resource('s3')

    # Read copy param files from job parameter folder.
    df = pd_read_pattern(params["parameters"]+job_name+'/copy*.csv')
    
    warehouse = df['WAREHOUSE'].unique()
    database  = df['DATABASE'].unique()
    schema    = df['SCHEMA'].unique()

    # Iterate over records in parameter file.
    for index, row in df.iterrows():
        
        # Fetch column value of each record and store it in variable.
        data_source=row['DATA_SOURCE']
        stage_object=row['STAGE_OBJECT']
        folder_path=row['S3_FILE_PATH']
        file_format=row['FILE_FORMAT']
        s3_bucket=row['S3_BUCKET']

        srcbucket = s3.Bucket(s3_bucket)
        destbucket = s3.Bucket(s3_bucket)

        # Iterate over each file in s3 folder and copy the files to archive.
        for objects in srcbucket.objects.filter(Prefix=folder_path):
            copy_source = {
                'Bucket': s3_bucket,
                'Key': objects.key
                    }
            destbucket.copy(copy_source, 'archive/'+folder_date+'/'+objects.key)
    
        # After copying the files execute stored procedure to create exteranl views.
        cur.execute(f"""USE DATABASE {database[0]}""")
        cur.execute(f"""USE SCHEMA {schema[0]}""")
        cur.execute("USE ROLE ACCOUNTADMIN")
        
        cur.execute(f"""USE WAREHOUSE {warehouse[0]}""")
    
        sql_statement=f"""call demo_db.public.{job_name}_archive(to_char(to_date('{today}'),'YYYYMMDD'),'{stage_object}','archive/{folder_date}/{folder_path}','{file_format}','{data_source}')"""
        cur.execute(sql_statement)
    
    # Close snowflake connection.
    cur.close()

#archive_s3_crt_ext_tbl('emp_details')
