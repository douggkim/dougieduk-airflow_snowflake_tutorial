import json

def snow_creds():
    with open("/opt/airflow/snowpythconn/creds/snow_creds.json","r") as f:
         cred = json.load(f)
         return cred


