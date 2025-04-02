from airflow import DAG
import logging
import requests

from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from datetime import datetime as dt

# GCS CSV file path
GCS_SALES_PATH = "gs://trendflow-455409-trendflow-bucket/Amazon Sale Report.csv"
GCS_ASIN_PRODUCT_MAPPING_PATH = "gs://trendflow-455409-trendflow-bucket/asin_to_product_mapping.csv"

# BigQuery settings
PROJECT_ID = "trendflow-455409"
DATASET_ID = "trendflow"
TABLE_ID = "sales"

# BUCKET 
BUCKET_ID = "trendflow-455409-trendflow-bucket"

def ingest_sales_csv():
    """
    Load a sales CSV
    """
    import pandas as pd
    logger = logging.getLogger(__name__)
    df_sales = pd.read_csv(GCS_SALES_PATH,sep=";")
    logger.log(1,msg="Sales CSV loaded")
    df_sales.to_csv("/tmp/Amazon Sale Report.csv",index=False)
    return  "/tmp/Amazon Sale Report.csv"

def ingest_mapping_csv():
    """
    Load a asin_to_product_mapping CSV
    """
    import pandas as pd
    logger = logging.getLogger(__name__)
    df_mapping = pd.read_csv(GCS_ASIN_PRODUCT_MAPPING_PATH,sep=";")
    logger.log(1,msg="asin_to_product_mapping CSV loaded")
    df_mapping.to_csv("/tmp/asin_to_product_mapping.csv",index=False)
    return  "/tmp/asin_to_product_mapping.csv"

def validate(ti): 
    import pandas as pd
    sales_csv_path = ti.xcom_pull(task_ids="ingest_sales_csv", key = "return_value")
    df_sales = pd.read_csv(sales_csv_path)
    print(df_sales.info())
    print(df_sales.head())
    mapping_csv_path = ti.xcom_pull(task_ids="ingest_mapping_csv", key = "return_value")
    df_mapping = pd.read_csv(mapping_csv_path)
    print(df_mapping.info())
    print(df_mapping.head())
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
    df_sales =   pd.read_csv("/tmp/Amazon Sale Report.csv")      
    df_mapping = pd.read_csv("/tmp/asin_to_product_mapping.csv") 
    
    print("🧼 Normalizing column names...")
    # Rename columns to match BigQuery schema (replace spaces with underscores)
    df_sales.rename(columns={
        "Sales Channel ": "Sales_Channel",
        "Sales Channel": "Sales_Channel",
        "Date": "Date",
        "ASIN": "ASIN",
        "Qty": "Qty",
        "Category": "Category"
    }, inplace=True)
    
    # Filter only expected columns
    expected_columns = ["Date", "ASIN", "Qty", "Sales_Channel", "Category"]
    selected_df = df_sales[expected_columns]    
    merged_df = selected_df.merge(merged_df, on="ASIN", how="right")
    
    print("🔁 Converting data types...")
    # Convert types to match BigQuery schema
    merged_df["Date"] = pd.to_datetime(merged_df["Date"], errors="coerce").dt.date
    merged_df["Qty"] = pd.to_numeric(merged_df["Qty"], errors="coerce")

    merged_df.to_csv("/tmp/sales.csv",index=False)
    return True 
     
    
with DAG("load_amazon_sales_from_gcs_to_bq",
         default_args = {"owner":"Yongjing"},
         schedule_interval="0 */5 * * *",
         start_date=dt(2025,3,23)
         ):
    
    ingest_csv_task = PythonOperator(task_id="ingest_sales_csv",python_callable=ingest_sales_csv)
    ingest_mapping_csv_task = PythonOperator(task_id="ingest_mapping_csv",python_callable=ingest_mapping_csv)
    validate_task   = PythonOperator(task_id="validate"  ,python_callable=validate)
    
    branch_task = BranchPythonOperator(task_id="branch",python_callable=choose_next)
    
    merge_data_task = PythonOperator(task_id="merge_data",python_callable=merge)
    load_data_task = LocalFilesystemToGCSOperator(task_id="load_data",
                                                  src="/tmp/sales.csv",
                                                  dst="final_sales.csv",
                                                  bucket=BUCKET_ID,
                                                  gcp_conn_id="bq-admin"
                                                  )
    load_bq_task = GCSToBigQueryOperator(task_id="load_bq",
                                        bucket=BUCKET_ID,
                                        source_objects=["final_sales.csv"],
                                        destination_project_dataset_table="trendflow.sales",
                                        write_disposition="WRITE_TRUNCATE",
                                        gcp_conn_id="bq-admin"                                        
    )

    
    send_alert = EmptyOperator(task_id="send_alert")
    
    
    [ingest_csv_task, ingest_mapping_csv_task] >> validate_task >> branch_task
    branch_task >> [merge_data_task, send_alert]
    
    merge_data_task >> load_data_task >> load_bq_task