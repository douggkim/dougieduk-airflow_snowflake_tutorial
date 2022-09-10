#!/usr/bin/env python
import sys
sys.path.append('/opt/airflow/python/snowflake')
from lib2to3.pgen2 import parse
import snowflake.connector
import connection.read_hardcoded_paths as paths

cred = paths.snow_creds()


# Gets the version
def connect_snowflake():
    
    ctx = snowflake.connector.connect(
    user=cred["userid"],
    password=cred["password"],
    account=cred["account"]
    )
    cs = ctx.cursor()
    return cs
