from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.email import EmailOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from datetime import datetime as dt
import pandas as pd
import logging

# CONFIG 
# BigQuery table
PROJECT_ID = "trendflow-455409"
DATASET_ID = "trendflow"
STAGING_TABLE = "ebay_sales_staging"
FINAL_TABLE = "sales_history"

# GCS path
BUCKET_ID = "trendflow-455409-trendflow-bucket"
GCS_SALES_PATH = "ebay_final_sales.csv"

# Local path
LOCAL_TMP_RAW_PATH = "/tmp/ebay_sales.xlsx"
LOCAL_FINAL_PATH = "/tmp/ebay_final_sales.csv"

default_args = {
    "owner": "Yongjing",
    "email": ["yongjing.chen@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}

default_args = {
    "owner": "Yongjing",
    "email_on_failure": True,
    "email": ["yongjing.chen@gmail.com"],
}

def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

def ingest_xlsx():
    download_from_gcs(BUCKET_ID, "eBay-OrdersReport.xlsx", LOCAL_TMP_RAW_PATH)
    logging.info("✅ Downloaded Excel file")
    return LOCAL_TMP_RAW_PATH

def validate(ti):
    import os
    path = ti.xcom_pull(task_ids="ingest_xlsx", key="return_value")
    if not os.path.exists(path):
        logging.error(f"❌ File not found: {path}")
    elif os.path.getsize(path) == 0:
        logging.error(f"❌ File is empty: {path}")
    return os.path.exists(path) and os.path.getsize(path) > 0

def clean_data():
    ebay_df = pd.read_excel(LOCAL_TMP_RAW_PATH)
    print("🧼 Normalizing column names...")
    
    # Check existence of required column before renaming
    if "Sold for" not in ebay_df.columns:
        raise ValueError("❌ Column 'Sold for' not found in the DataFrame")
    # Rename columns to match BigQuery schema
    ebay_df.rename(columns={
        "Sale date": "sale_date",
        "Item number": "product_id",
        "Item title": "product_name",
        "Quantity": "quantity_sold",
        "Sold for": "sale_price"
        }, inplace=True)

    print("🔁 Converting data types...")

    # Convert sale_date to datetime
    ebay_df["sale_date"] = pd.to_datetime(ebay_df["sale_date"], errors="coerce").dt.date

    # Ensure product_id is a clean string
    ebay_df["product_id"] = ebay_df["product_id"].astype(str).str.strip()
    ebay_df = ebay_df[ebay_df["product_id"].notna() & (ebay_df["product_id"] != "")]

    # Clean and convert sale_price: remove currency symbols like £/$/€ and convert to float
    ebay_df["sale_price"] = ebay_df["sale_price"].astype(str).str.replace(r"[^\d\.]", "", regex=True)
    ebay_df["sale_price"] = pd.to_numeric(ebay_df["sale_price"], errors="coerce")

    # Convert quantity_sold to integer
    ebay_df["quantity_sold"] = pd.to_numeric(ebay_df["quantity_sold"], errors="coerce").fillna(0).astype(int)

    # Fill in default values for BigQuery compatibility
    ebay_df["region"] = "United Kingdom"
    ebay_df["currency"] = "GBP"
    ebay_df["category"] = "High-tech Electronics"
    ebay_df["category_id"] = "unknown"
    ebay_df["discount"] = 0
    ebay_df["inventory_level"] = 100

    # Reorder and keep only expected columns
    expected_columns = [
        "sale_date", "product_id", "quantity_sold", "region", "category",
        "sale_price", "product_name", "category_id", "discount", "inventory_level"
    ]
    ebay_df = ebay_df[expected_columns]

    # Remove duplicates based on (sale_date + product_id)
    ebay_df.drop_duplicates(subset=["sale_date", "product_id"], inplace=True)        
    ebay_df.to_csv(LOCAL_FINAL_PATH, index=False)
    return LOCAL_FINAL_PATH

def choose_branch(ti):
    return "clean_data" if ti.xcom_pull(task_ids="validate") else "send_alert"

with DAG(
    "load_ebay_uk_sales_from_gcs_to_bq",
    default_args=default_args,
    schedule_interval="0 */5 * * *",
    start_date=dt(2025, 4, 3),
    catchup=False,
) as dag:
    
    ingest_xlsx_task = PythonOperator(task_id="ingest_xlsx", python_callable=ingest_xlsx)
    validate_task = PythonOperator(task_id="validate", python_callable=validate)
    branch_task = BranchPythonOperator(task_id="branch", python_callable=choose_branch)
    clean_data_task = PythonOperator(task_id="clean_data", python_callable=clean_data)
    send_alert_task = EmptyOperator(task_id="send_alert")

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=LOCAL_FINAL_PATH,
        dst=GCS_SALES_PATH,
        bucket=BUCKET_ID,
        gcp_conn_id="bq-admin",
    )

    load_to_staging = GCSToBigQueryOperator(
        task_id="load_to_staging",
        bucket=BUCKET_ID,
        source_objects=[GCS_SALES_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}",
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        source_format="CSV",
        schema_fields=[
            {"name": "sale_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
            {"name": "quantity_sold", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "region", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category", "type": "STRING", "mode": "NULLABLE"},
            {"name": "sale_price", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "product_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "category_id", "type": "STRING", "mode": "NULLABLE"},
            {"name": "discount", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "inventory_level", "type": "INTEGER", "mode": "NULLABLE"},
        ],
        gcp_conn_id="bq-admin"
    )

    merge_into_main = BigQueryInsertJobOperator(
        task_id="merge_into_main",
        configuration={
            "query": {
                "query": f"""
                    MERGE `{PROJECT_ID}.{DATASET_ID}.{FINAL_TABLE}` T
                    USING `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE}` S
                    ON T.sale_date = S.sale_date AND T.product_id = S.product_id
                    WHEN MATCHED THEN
                      UPDATE SET
                        quantity_sold = S.quantity_sold,
                        region = S.region,
                        category = S.category,
                        sale_price = S.sale_price,
                        product_name = S.product_name,
                        category_id = S.category_id,
                        discount = S.discount,
                        inventory_level = S.inventory_level
                    WHEN NOT MATCHED THEN
                      INSERT (
                        sale_date, product_id, quantity_sold, region,
                        category, sale_price, product_name, category_id,
                        discount, inventory_level
                      )
                      VALUES (
                        S.sale_date, S.product_id, S.quantity_sold, S.region,
                        S.category, S.sale_price, S.product_name, S.category_id,
                        S.discount, S.inventory_level
                      )
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="bq-admin"
    )

    send_alert = EmailOperator(
        task_id="email_alert_on_failure",
        to="yongjing.chen@gmail.com",
        subject="[Airflow Failure] Amazon DAG Failed",
        html_content="The DAG <b>load_amazon_sales_from_gcs_to_bq</b> has failed. Please check Airflow logs.",
        trigger_rule="one_failed",
        conn_id="smtp_default",
    )

    final = EmptyOperator(task_id="pipeline_done")

    ingest_xlsx_task >> validate_task >> branch_task
    branch_task >> [clean_data_task, send_alert_task]
    clean_data_task >> upload_to_gcs >> load_to_staging >> merge_into_main