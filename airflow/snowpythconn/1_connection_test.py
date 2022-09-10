#!/usr/bin/env python
import snowflake.connector

# Gets the version
with open('/opt/airflow/snowpythconn/Sql_files/snowflake_config.cfg','r') as config_txt:
    user = config_txt.readline().strip()
    pw = config_txt.readline().strip() 
    account = config_txt.readline().strip()
    

ctx = snowflake.connector.connect(
    user=user,
    password=pw,
    account=account
    )
cs = ctx.cursor()

try:
    cs.execute("SELECT current_version()")
    one_row = cs.fetchone()
    print(one_row[0])
finally:
    cs.close()    
    ctx.close()