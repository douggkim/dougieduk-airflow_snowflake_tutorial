#!/usr/bin/env python
import sys
sys.path.append('/opt/airflow/snowpythconn/')
from lib2to3.pgen2 import parse
from pickle import FALSE, TRUE
import snowflake.connector
import creds.read_hardcoded_paths as paths

cred = paths.snow_creds()


# Gets the version
def connect_snowflake():
    
    ctx = snowflake.connector.connect(
    user=cred["userid"],
    password=cred["password"],
    account=cred["account"],
    #autocommit=False
    )
    cs = ctx.cursor()
    return cs

def return_conn_obj():
    
    ctx = snowflake.connector.connect(
    user=cred["userid"],
    password=cred["password"],
    account=cred["account"],
    database='DEMO_DB',
    schema='PUBLIC'
    #autocommit=FALSE
    )
    return ctx
