from airflow import DAG
import logging
import requests

from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from datetime import datetime as dt

def ingest_csv():
    """
    Load a CSV
    """
    import pandas as pd
    logger = logging.getLogger(__name__)
    csv_url = "https://storage.cloud.google.com/trendflow-455409-trendflow-bucket/Amazon%20Sale%20Report.csv"
    csv_url = "https://storage.googleapis.com/schoolofdata-datasets/Data-Analysis.Data-Visualization/CO2_per_capita.csv"
    df = pd.read_csv(csv_url,sep=";")
    logger.log(1,msg="CSV loaded")
    df.to_csv("/tmp/co2_per_capita.csv",index=False)
    return  "/tmp/co2_per_capita.csv"


def ingest_api(**kwargs):
    url = "https://restcountries.com/v3.1/all"
    response = requests.get(url)
    ti = kwargs["ti"]
    ti.xcom_push(key="status_code",value=response.status_code)
    return response.json()

def validate(ti): 
    import pandas as pd
    status_code = ti.xcom_pull(task_ids="ingest_api", key = "status_code")
    if status_code != 200 : 
        return False
    csv_path = ti.xcom_pull(task_ids="ingest_csv", key = "return_value")
    df = pd.read_csv(csv_path)
    print(df.info())
    print(df.head())
    return True
    
def choose_next(**kwargs):
    ti = kwargs["ti"]
    print("Doesn't exist", ti.xcom_pull(task_ids='truc', key="return_value") )
    if ti.xcom_pull(task_ids='validate', key="return_value") :
        return 'merge_data'
    return 'send_alert'

def merge(**kwargs): 
    ti = kwargs["ti"]
    import pandas as pd 
    df_co2 =   pd.read_csv("/tmp/co2_per_capita.csv")      
    
    df_countries = pd.DataFrame(ti.xcom_pull(task_ids="ingest_api",key="return_value"))
    
    final_df = pd.merge(df_co2,df_countries.loc[:,["cca3","capital","currencies"]],how="inner",left_on="Country Code",right_on="cca3")
    final_df.rename(columns={"CO2 Per Capita (metric tons)":"CO2_Per_Capita"},inplace=True)
    final_df.columns = [col.replace(" ","_") for col in final_df.columns]
    final_df.to_csv("/tmp/countries.csv",index=False)
    return True 
     
    
with DAG( "etl_pipe",
         default_args = {"owner":"Yongjing"},
         schedule_interval="0 */5 * * *",
         start_date=dt(2025,3,23)
         ):
    
    ingest_csv_task = PythonOperator(task_id="ingest_csv",python_callable=ingest_csv)
    ingest_api_task = PythonOperator(task_id="ingest_api",python_callable=ingest_api)
    validate_task   = PythonOperator(task_id="validate"  ,python_callable=validate)
    
    branch_task = BranchPythonOperator(task_id="branch",python_callable=choose_next)
    
    merge_data_task = PythonOperator(task_id="merge_data",python_callable=merge)
    load_data_task = LocalFilesystemToGCSOperator(task_id="load_data",
                                                  src="/tmp/countries.csv",
                                                  dst="final.csv",
                                                  bucket="data-engenering-mars-2015",
                                                  gcp_conn_id="bq-admin"
                                                  )
    load_bq_task = GCSToBigQueryOperator(task_id="load_bq",
                                        bucket="data-engenering-mars-2015",
                                        source_objects="final.csv",
                                        destination_project_dataset_table="countries.countries_table",
                                        write_disposition="WRITE_TRUNCATE",
                                        gcp_conn_id="bq-admin"                                        
    )

    
    send_alert = EmptyOperator(task_id="send_alert")
    
    
    [ingest_api_task, ingest_csv_task] >> validate_task >> branch_task
    branch_task >> [merge_data_task, send_alert]
    
    merge_data_task >> load_data_task >> load_bq_task