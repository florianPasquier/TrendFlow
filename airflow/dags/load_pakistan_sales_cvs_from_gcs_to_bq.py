from airflow import DAG
import logging
import requests
import openpyxl

from airflow.operators.empty import EmptyOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from datetime import datetime as dt

# GCS CSV file path
GCS_SALES_PATH = "gs://trendflow-455409-trendflow-bucket/Pakistan Largest Ecommerce Dataset.xlsx"

# BigQuery settings
PROJECT_ID = "trendflow-455409"
DATASET_ID = "trendflow"
TABLE_ID = "sales_history"

# BUCKET 
BUCKET_ID = "trendflow-455409-trendflow-bucket"
LOCAL_TMP_RAW_PATH = "/tmp/pakistan_sales.xlsx"
LOCAL_FINAL_PATH = "/tmp/Pakistan_final_sales.csv"

def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)
    
def ingest_pakistan_sales_xlsx():
    import pandas as pd
    import os

    logger = logging.getLogger(__name__)
    
    download_from_gcs("trendflow-455409-trendflow-bucket", "Pakistan Largest Ecommerce Dataset.xlsx", LOCAL_TMP_RAW_PATH)   
    df_sales = pd.read_excel(LOCAL_TMP_RAW_PATH)
    logger.info("✅ Sales excel file loaded")
    df_sales.to_excel(LOCAL_TMP_RAW_PATH, index=False)
    return LOCAL_TMP_RAW_PATH

def validate(ti): 
    import pandas as pd
    sales_xlsx_path = ti.xcom_pull(task_ids="ingest_pakistan_sales_xlsx", key = "return_value")
    df_sales = pd.read_excel(sales_xlsx_path)
    print(df_sales.info())
    print(df_sales.head())
    return True

def clean_data(**kwargs):
    ti = kwargs["ti"]
    import pandas as pd
    pakistan_df = pd.read_excel(LOCAL_TMP_RAW_PATH)
    pakistan_df.rename(columns={
    "created_at": "sale_date",
    "item_id": "product_id",
    "sku": "product_name",
    "qty_ordered": "quantity_sold",
    "price": "sale_price",
    "category_name_1": "category",
    "discount_amount": "discount",
    }, inplace=True)
    print("🔁 Converting data types...")
    pakistan_df["product_id"] = pd.to_numeric(pakistan_df["product_id"], errors="coerce").fillna(0).astype(int).astype(str).replace("0", "")
    pakistan_df["quantity_sold"] = pd.to_numeric(pakistan_df["quantity_sold"], errors="coerce").fillna(0).astype(int).astype(str).replace("0", "")
    # Convert types to match BigQuery schema
    pakistan_df["sale_date"] = pd.to_datetime(pakistan_df["sale_date"], errors="coerce").dt.date
    pakistan_df["sale_price"] = pd.to_numeric(pakistan_df["sale_price"], errors="coerce")
    pakistan_df["region"] = "Pakistan"
    pakistan_df["currency"] = "PKR"
    pakistan_df["category_id"] = "unknown"
    pakistan_df["inventory_level"] = 100

    expected_columns = ["sale_date", "product_id", "quantity_sold", "region", "category", "sale_price", "product_name", "category_id", "discount", "inventory_level"]
    expected_df = pakistan_df[expected_columns]
    expected_df.drop_duplicates(subset=["sale_date", "product_id"], inplace=True)
    
    print(expected_df.head())
    expected_df.to_csv(LOCAL_FINAL_PATH,index=False)
    return LOCAL_FINAL_PATH
    
def choose_next(**kwargs):
    ti = kwargs["ti"]
    print("Doesn't exist", ti.xcom_pull(task_ids='truc', key="return_value") )
    if ti.xcom_pull(task_ids='validate', key="return_value") :
        return 'merge_data'
    return 'send_alert'
    
with DAG("load_pakistan_sales_from_gcs_to_bq",
         default_args = {"owner":"Yongjing"},
         schedule_interval="0 */5 * * *",
         start_date=dt(2025,4,3)
         ):
    
    ingest_xlsx_task = PythonOperator(task_id="ingest_pakistan_sales_xlsx",python_callable=ingest_pakistan_sales_xlsx)
    validate_task   = PythonOperator(task_id="validate"  ,python_callable=validate)
    
    branch_task = BranchPythonOperator(task_id="branch",python_callable=choose_next)
    clean_data_task = PythonOperator(task_id="clean_data",python_callable=clean_data)
    
    load_data_task = LocalFilesystemToGCSOperator(task_id="load_data",
                                                  src=LOCAL_FINAL_PATH,
                                                  dst="Pakistan_final_sales.csv",
                                                  bucket=BUCKET_ID,
                                                  gcp_conn_id="bq-admin"
                                                  )
    load_bq_task = GCSToBigQueryOperator(task_id="load_bq",
                                        bucket=BUCKET_ID,
                                        source_objects=["Pakistan_final_sales.csv"],
                                        source_format="CSV", 
                                        skip_leading_rows=1,
                                        destination_project_dataset_table="trendflow.sales_history",
                                        write_disposition="WRITE_APPEND",
                                        gcp_conn_id="bq-admin"                                        
    )

    
    send_alert = EmptyOperator(task_id="send_alert")
    
ingest_xlsx_task >> validate_task >> branch_task   
branch_task >> [clean_data_task, send_alert]
clean_data_task >> load_data_task >> load_bq_task