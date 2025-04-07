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

# GCS 路径
BUCKET_ID = "trendflow-455409-trendflow-bucket"
GCS_SALES_PATH = "Amazon_Sales_Final.csv"

# BigQuery 表
PROJECT_ID = "trendflow-455409"
DATASET_ID = "trendflow"
STAGING_TABLE = "amazon_sales_staging"
FINAL_TABLE = "sales_history"

# 临时路径
LOCAL_FINAL_PATH = "/tmp/sales.csv"

default_args = {
    "owner": "Yongjing",
    "email_on_failure": True,
    "email": ["yongjing.chen@gmail.com"],
}

def ingest_and_merge(**kwargs):
    df_sales = pd.read_csv(f"gs://{BUCKET_ID}/Amazon Sale Report.csv")
    df_mapping = pd.read_csv(f"gs://{BUCKET_ID}/asin_to_product_mapping.csv")

    df_sales.rename(columns={
        "Sales Channel ": "Sales_Channel",
        "Sales Channel": "Sales_Channel",
        "Date": "Date",
        "ASIN": "ASIN",
        "Qty": "Qty",
        "Category": "Category",
        "Amount": "price",
    }, inplace=True)

    expected = ["Date", "ASIN", "Qty", "Sales_Channel", "Category", "price"]
    df_sales = df_sales[expected]
    df = df_sales.merge(df_mapping, on="ASIN", how="right")

    df.rename(columns={
        "Date": "sale_date",
        "ASIN": "product_id",
        "ProductName": "product_name",
        "Qty": "quantity_sold",
        "price": "sale_price",
        "Category": "category",
        "Sales_Channel": "region",
    }, inplace=True)

    df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
    df["quantity_sold"] = pd.to_numeric(df["quantity_sold"], errors="coerce")
    df["sale_price"] = pd.to_numeric(df["sale_price"], errors="coerce")
    df["category_id"] = "unknown"
    df["discount"] = 0.0
    df["inventory_level"] = 100
    df = df.dropna(subset=["product_id", "sale_date"])

    df = df[[
        "sale_date", "product_id", "quantity_sold", "region", "category",
        "sale_price", "product_name", "category_id", "discount", "inventory_level"
    ]].drop_duplicates(subset=["sale_date", "product_id"])

    df.to_csv(LOCAL_FINAL_PATH, index=False)
    logging.info("✅ Final sales CSV written to /tmp/sales.csv")
    return LOCAL_FINAL_PATH

with DAG("load_amazon_sales_from_gcs_to_bq",
         default_args=default_args,
         schedule_interval="0 */5 * * *",
         start_date=dt(2025, 3, 23),
         catchup=False,
         ) as dag:

    clean_merge = PythonOperator(
        task_id="clean_and_merge",
        python_callable=ingest_and_merge,
    )

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

    alert = EmailOperator(
        task_id="email_alert_on_failure",
        to="yongjing.chen@gmail.com",
        subject="[Airflow Failure] Amazon DAG Failed",
        html_content="The DAG <b>load_amazon_sales_from_gcs_to_bq</b> has failed. Please check Airflow logs.",
        trigger_rule="one_failed",
    )

    final = EmptyOperator(task_id="pipeline_done")

    clean_merge >> upload_to_gcs >> load_to_staging >> merge_into_main >> final
    [clean_merge, upload_to_gcs, load_to_staging, merge_into_main] >> alert