from airflow import DAG
import logging
from google.cloud import bigquery
import os
import pandas as pd
import openpyxl

from datetime import datetime as dt

from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

# CONFIG
PROJECT_ID = "trendflow-455409"
DATASET_ID = "trendflow"
TABLE_ID = "sales_history"
STAGING_TABLE_ID = "sales_staging"
BUCKET_ID = "trendflow-455409-trendflow-bucket"
GCS_SALES_PATH = "Pakistan_final_sales.csv"
LOCAL_TMP_RAW_PATH = "/tmp/pakistan_sales.xlsx"
LOCAL_FINAL_PATH = "/tmp/Pakistan_final_sales.csv"

default_args = {
    "owner": "Yongjing",
    "email": ["yongjing.chen@gmail.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
}
def ensure_staging_table():
    from google.cloud import bigquery
    import logging

    schema = [
        {"name": "sale_date", "field_type": "DATE", "mode": "NULLABLE"},
        {"name": "product_id", "field_type": "STRING", "mode": "REQUIRED"},
        {"name": "quantity_sold", "field_type": "INTEGER", "mode": "NULLABLE"},
        {"name": "region", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "sale_price", "field_type": "FLOAT", "mode": "NULLABLE"},
        {"name": "product_name", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "category_id", "field_type": "STRING", "mode": "NULLABLE"},
        {"name": "discount", "field_type": "FLOAT", "mode": "NULLABLE"},
        {"name": "inventory_level", "field_type": "INTEGER", "mode": "NULLABLE"},
    ]

    client = bigquery.Client(project=PROJECT_ID)
    table_ref = bigquery.DatasetReference(PROJECT_ID, DATASET_ID).table(STAGING_TABLE_ID)

    try:
        client.get_table(table_ref)
        logging.info("✅ Staging table already exists.")
    except Exception as e:
        logging.info("⚠️ Staging table not found. Creating...")
        table = bigquery.Table(
            table_ref,
            schema=[bigquery.SchemaField(col["name"], col["field_type"], mode=col["mode"]) for col in schema]
        )
        client.create_table(table)
        logging.info("✅ Staging table created successfully.")

def download_from_gcs(bucket_name, source_blob_name, destination_file_name):
    from google.cloud import storage
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

def ingest_xlsx():
    download_from_gcs(BUCKET_ID, "Pakistan Largest Ecommerce Dataset.xlsx", LOCAL_TMP_RAW_PATH)
    logging.info("✅ Downloaded Excel file")
    return LOCAL_TMP_RAW_PATH

def validate(ti):
    path = ti.xcom_pull(task_ids="ingest_xlsx", key="return_value")
    return os.path.exists(path) and os.path.getsize(path) > 0

def clean_data():
    df = pd.read_excel(LOCAL_TMP_RAW_PATH)
    df.rename(columns={
        "created_at": "sale_date",
        "item_id": "product_id",
        "sku": "product_name",
        "qty_ordered": "quantity_sold",
        "price": "sale_price",
        "category_name_1": "category",
        "discount_amount": "discount",
    }, inplace=True)
    df = df.dropna(subset=["product_id"])
    df["product_id"] = pd.to_numeric(df["product_id"], errors="coerce").fillna(0).astype(int).astype(str).replace("0", "")
    df["quantity_sold"] = pd.to_numeric(df["quantity_sold"], errors="coerce").fillna(0).astype(int)
    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
    df["sale_price"] = pd.to_numeric(df["sale_price"], errors="coerce")
    df["region"] = "Pakistan"
    df["currency"] = "PKR"
    df["category_id"] = "unknown"
    df["inventory_level"] = 100
    expected_columns = [
        "sale_date", "product_id", "quantity_sold", "region", "category",
        "sale_price", "product_name", "category_id", "discount", "inventory_level"
    ]
    df = df[expected_columns].drop_duplicates(subset=["sale_date", "product_id"])
    df.to_csv(LOCAL_FINAL_PATH, index=False)
    return LOCAL_FINAL_PATH

def choose_branch(ti):
    return "clean_data" if ti.xcom_pull(task_ids="validate") else "send_alert"

with DAG(
    "load_pakistan_sales_from_gcs_to_bq",
    default_args=default_args,
    schedule_interval="0 */5 * * *",
    start_date=dt(2025, 4, 3),
    catchup=False,
) as dag:

    ingest_xlsx_task = PythonOperator(task_id="ingest_xlsx", python_callable=ingest_xlsx)
    validate_task = PythonOperator(task_id="validate", python_callable=validate)
    branch_task = BranchPythonOperator(task_id="branch", python_callable=choose_branch)
    clean_data_task = PythonOperator(task_id="clean_data", python_callable=clean_data)
    send_alert = EmptyOperator(task_id="send_alert")
    
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=LOCAL_FINAL_PATH,
        dst=GCS_SALES_PATH,
        bucket=BUCKET_ID,
        gcp_conn_id="bq-admin"
    )

    ensure_staging = PythonOperator(
        task_id="ensure_staging_table", 
        python_callable=ensure_staging_table
    )

    load_to_staging = GCSToBigQueryOperator(
        task_id="load_to_staging",
        bucket=BUCKET_ID,
        source_objects=[GCS_SALES_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}",
        write_disposition="WRITE_TRUNCATE",
        skip_leading_rows=1,
        source_format="CSV",
        autodetect=False,
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
                    MERGE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}` T
                    USING `{PROJECT_ID}.{DATASET_ID}.{STAGING_TABLE_ID}` S
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
                "useLegacySql": False
            }
        },
        gcp_conn_id="bq-admin"
    )

    # DAG dependencies
    ensure_staging >> ingest_xlsx_task >> validate_task >> branch_task
    branch_task >> [clean_data_task, send_alert]
    clean_data_task >> upload_to_gcs >> load_to_staging >> merge_into_main